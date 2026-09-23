# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""MCP tools for STATE_DEPLOYMENT_DATA_MIGRATION.

The data services graded `migrate` at the review have to actually move, and
nothing in this agent can move them: the server holds no AWS credentials and
never will (DESIGN.md §10). So this step does the three things it can — say
what is owed, write the worklist down, and record what the operator reports —
and leaves the moving to them.

WHY IT IS A STATE RATHER THAN A TOOL AT THE TERMINAL. Image replication got
away with a tool hanging off the terminal because an operator learns it exists
from the state before it, which asks the question and writes a runbook, and
because a `skopeo copy` is an afternoon's work. Neither holds here: a database
migration takes weeks, so whoever reports it may be a different person in a
different session with nothing in front of them. A tool nobody is told about is
a tool nobody calls.

WHY THE GRAPH STAYS HERE UNTIL THE WORK IS DONE. A database migration takes
days or weeks, and the first instinct is to let the graph move on and report
the long tail against the terminal. That does not work, and the reason is worth
writing down: a TERMINAL state declares no step, no instructions and no
knowledge, so `build_stage_payload` returns one line and nothing else. An
operator arriving six weeks later to report that the Postgres finally landed
would be told the migration is complete and offered nothing. The tool would
exist; the only thing that could ever mention it is the state's instructions,
and those would be behind them.

So the graph parks here until nothing is owed. A state is the only mechanism in
this system that survives weeks of elapsed time and a change of operator —
every future session asks `get_next_stage`, and this state answers with what is
still outstanding. Parking costs nothing: the graphs are independent, the
deployment exports slice publishes from HERE rather than from the terminal, and
`mark_replication_complete` accepts this state too, so nothing is trapped
behind a database.

AND IT IS A HOLDING STATE, NOT A TASK. Waiting is the normal condition here,
not a failure to progress. An AGENT_TASK parks the walk and returns control to
the user, so the operator is free to do anything at all while a migration runs
— including asking for help running it, which is what the phase knowledge
document is for. The instructions tell the agent to report where things stand,
act on a completion when the user reports one, and otherwise get out of the
way. There is always an exit: a service that genuinely cannot move is recorded
`keep-in-aws` and stops being owed.
"""

import json
import logging
from datetime import datetime, timezone
from typing import Optional

from google.api_core import exceptions
from mcp.server.fastmcp import Context

from servers.dag.state_management import (
    authorize_and_rehydrate,
    get_bucket_name,
    load_dag,
    load_inventory,
)
from servers.dag.server import exports as exports_lib
from servers.phases.discovery.discovery_init_1 import overrides as overrides_lib
from servers.phases.discovery.discovery_init_1 import datastores
import servers.dag.state_management as state_mgr

from .. import datamigration as dm
from .. import runbooks as rb
# The review step owns the scan baseline; this step grades it — absent,
# unreadable, or not describing this section — because `keep-in-aws`, the exit
# from an unmovable service, is replayed over it, and the close has to say the
# same thing about the object that `_amend` does.
from servers.phases.discovery.discovery_datareview_3.tools import (
    BASELINE_ABSENT, BASELINE_UNREADABLE, SCAN_BASELINE_BLOB,
    load_scan_baseline, scan_baseline_state)

logger = logging.getLogger("migration-dag")

STATE = "STATE_DEPLOYMENT_DATA_MIGRATION"
STATE_BLOB = "platform/onboarding/state.json"
# Every tool here runs in the one state, and that is a consequence of the graph
# parking rather than a restriction on top of it: the step does not close while
# a service is unreported, so at the terminal there is by construction nothing
# left to report. An earlier draft let the graph move on and accepted reports
# against the terminal too; that is unreachable now, and unreachable code that
# looks reachable is worse than no code.
REPORTING_STATES = (STATE,)

# Below this, a "rendered runbook" is a summary of one. The shortest template
# in the set is several thousand characters and an adaptation removes
# placeholders rather than sections, so this only catches the case where the
# agent sent a paragraph instead of the document.
MIN_RENDERED_RUNBOOK = 1000


class _Refused(Exception):
    """An ERROR string a tool returns rather than raising out of the call."""


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def load_migrations(bucket) -> tuple:
    """(document, generation). (None, None) when it exists but is unreadable.

    The shape check covers the ELEMENTS, not only the container: every reader
    calls `.get` on the items, and a hand-edited object is anticipated here the
    same way it is for the corrections store. Refused rather than filtered —
    dropping a malformed record silently would lose the record that a service
    moved, and there is no way to tell a corrupted record from a deliberate one.
    """
    blob = bucket.blob(dm.MIGRATIONS_BLOB)
    try:
        # Both sibling loaders reload first, and the generation this returns
        # guards the write. Relying on the download to populate it is relying
        # on a download implementation detail — a None there would make the
        # write unconditional and lose a concurrent report with no
        # PreconditionFailed to show for it.
        blob.reload()
        document = json.loads(blob.download_as_text())
    except exceptions.NotFound:
        return dm.empty_document(), 0
    except ValueError:
        logger.warning("data_migrations.json is not valid JSON")
        return None, None
    if not isinstance(document, dict) or not isinstance(
            document.get("migrations", []), list) or not all(
                isinstance(record, dict)
                for record in document.get("migrations") or []):
        logger.warning("data_migrations.json is not a migration record set")
        return None, None
    return document, blob.generation


def _save_migrations(bucket, document: dict, generation: int) -> None:
    bucket.blob(dm.MIGRATIONS_BLOB).upload_from_string(
        json.dumps(document, indent=2), content_type="application/json",
        if_generation_match=generation)


def refresh_runbook_from_ledger(bucket) -> str:
    """`_refresh_runbook` for a caller in another package.

    Two of them: `annotate_data_dependency` in the review module, where
    `keep-in-aws` stops a service being owed, and the image tools in
    `deployment_provision_1`, where confirming or abandoning a copy settles
    the other half of what this step waits on. Each loads what it needs from
    the ledger rather than taking a context, and reports rather than raising:
    a failure to redraw the artifact must not turn a recorded, durable
    outcome into an error.

    EVERY input is re-read here, `current_state` included. A caller
    rehydrated its state at the top of its own call and may have been holding
    it while another session closed the step — and once the step has closed,
    this document is the record of what the close left behind, with a UI
    caveat promising nothing rewrites it afterwards. Redrawing it from a
    stale copy restores the live document over that record: the "has left the
    step" banner gone, the reporting calls back, and the `keep-in-aws` exit
    re-offered at a terminal where it refuses. That is the same bug once fixed by
    statement ordering, arrived at through a cached read instead.
    """
    verdict, state_dict = _step_liveness(bucket)
    if verdict == CLOSED:
        return CLOSED_UNDER_US
    if verdict == UNKNOWN:
        return _UNREADABLE_STATE
    try:
        inventory, _ = load_inventory(bucket)
        document, _ = load_migrations(bucket)
    except Exception as e:
        logger.warning("Could not reload for the runbook refresh: %r", e)
        return (f"WARNING: {dm.RUNBOOK_BLOB} could not be refreshed ({e}); "
                "it still shows the worklist as it stood before this call.")
    if document is None or inventory is None:
        # Both loads, not one. Rendering from a missing inventory overwrites
        # the primary artifact with "No data service is waiting to move", and
        # rendering from a missing outcome store reports every service as
        # unstarted — either way in the copy that outlives the session.
        missing = (dm.MIGRATIONS_BLOB if document is None
                   else "the discovery inventory")
        return (f"WARNING: {dm.RUNBOOK_BLOB} was not refreshed because "
                f"{missing} is not readable; the worklist there is "
                "as it stood before this call.")
    # NOT passed through. Two reads of one object can disagree, and here the
    # LATER one is the one that should decide: the only transition this guard
    # watches is LIVE -> CLOSED (nothing re-enters the step), and skipping the
    # write is always the safe action. Passing the first verdict through would
    # put the decision two GCS round-trips ahead of the write it guards —
    # exactly the window rounds 13, 14 and 15 each closed somewhere else. The
    # sibling callers already re-read immediately before `_save_runbook`.
    # `state_dict` from the first read is fine: it supplies `workspace_name`.
    return _refresh_runbook({
        "bucket": bucket, "state_dict": state_dict, "document": document,
        "inventory": inventory,
        "entries": (inventory or {}).get("data_dependencies") or []})


def _publish_data_gate(context) -> str:
    """The `data_gate` publish for a caller inside this step.

    Derived from the context this call already holds — the same two objects
    `_refresh_runbook` redraws the artifact from, after the same save — so
    the slice and the runbook can never describe different worklists.
    """
    return exports_lib.publish_data_exports(
        context["bucket"], context["inventory"], context["document"],
        dm.key_of, dm.status_of)


def publish_data_gate_from_ledger(bucket, inventory: dict = None) -> str:
    """Republishes the exports `data_gate` slice. "" on success, else a warning.

    The developer half of this gate (`servers/phases/workload/datagate`) runs
    in sessions that hold no read on `platform/*`, so both objects joined
    here — the inventory's `data_dependencies` and this step's outcome store
    — reach them only through exports.json. Every call that changes either
    one calls this: the discovery extraction that persists the section, the
    review's `keep-in-aws`, and this step's own reports and close.

    NO LIVENESS GUARD, unlike `refresh_runbook_from_ledger`. That artifact is
    a record whose value is that the close freezes it; this slice is a live
    fact whose whole purpose is to open developer ship gates, and the close
    is the moment the most gates open. Refusing to publish at a closed step
    would leave every component held on a migration that finished.

    `inventory` is accepted from a caller that already has it in hand and
    re-read otherwise. An unreadable inventory or outcome store skips the
    publish rather than deriving over the gap — `publish_data_exports` gives
    the reason.
    """
    try:
        if inventory is None:
            inventory, _ = load_inventory(bucket)
        document, _ = load_migrations(bucket)
    except Exception as e:
        logger.warning("Could not reload for the data_gate publish: %r", e)
        return (f"WARNING: the exports data_gate slice was not republished "
                f"({e}); developer ship gates read its last published value.")
    if inventory is None:
        return ("WARNING: the exports data_gate slice was not republished "
                "because the discovery inventory is not readable; developer "
                "ship gates read its last published value.")
    return _publish_data_gate({"bucket": bucket, "inventory": inventory,
                               "document": document})


# What `state.json` says about this step, right now. THREE outcomes, not two.
# "Could not read it" is not "it has closed": the first is a transport failure
# that clears on the next call, and folding them together made the listing
# announce that the workspace had left the step while printing two services
# still owed. Every refusal in this package splits those two for the same
# reason.
LIVE, CLOSED, UNKNOWN = "live", "closed", "unknown"


def _step_liveness(bucket) -> tuple:
    """(verdict, state_dict). `state_dict` is None when the read failed.

    Every caller holds a `state_dict` rehydrated at the top of its own call,
    and another session can close the step while it works. Redrawing the LIVE
    document from that stale view puts it over the record the close left at
    the terminal — the banner gone, the reporting calls back, `keep-in-aws`
    re-offered where it refuses — against a UI caveat promising nothing
    rewrites it afterwards. Earlier reviews hit this by statement ordering, then
    through the external callers, then through the internal two.

    Takes the bucket and nothing else: the one exemption from this check
    belongs to `_refresh_runbook`, whose docstring carries it.
    """
    try:
        state = json.loads(bucket.blob(STATE_BLOB).download_as_text())
    except Exception as e:
        logger.warning("Could not read the state for the runbook refresh: %r", e)
        return UNKNOWN, None
    return (LIVE if state.get("current_state") == STATE else CLOSED), state


# Said out loud rather than swallowed. The refresh exists so the terminal
# artifact's "written as it stood when the step closed" stays true, and a
# silent skip is the one case that makes it false again.
# The one outcome that was silent. Skipping the write is right — at the
# terminal the artifact is the record of what the close left behind — but the
# record this call just made will never appear in it, and its UI caveat says
# nothing rewrites it. The operator has to be told, because nothing else will.
# Public: the callers that append it also have to RECOGNISE it, because
# every forward-looking sentence they would otherwise add — "still owed",
# "will refuse until", "holds the step open" — is false once it has fired.
CLOSED_UNDER_US = (
    "WARNING: another session closed the data migration step while this call "
    "was running. What was just recorded is durable and stays where this tool "
    f"wrote it, but {dm.RUNBOOK_BLOB} was written at the close and is not "
    "rewritten afterwards, so it does not reflect this call.")

_UNREADABLE_STATE = (
    f"WARNING: {dm.RUNBOOK_BLOB} was not refreshed: the workspace state could "
    "not be read, so this call cannot tell whether the step is still open and "
    "will not risk overwriting the record of a close. The worklist there is as "
    "it stood before this call.")


def _refresh_runbook(context, closed: str = None) -> str:
    """Rewrites the worklist to match the section as it stands. Best effort.

    Called by everything that CHANGES what it says — the listing, every
    report, the close, the `keep-in-aws` correction from the review module,
    and `mark_replication_complete` / `abandon_image_replication` settling the
    image half — because the runbook is this state's PRIMARY artifact and is
    registered at the terminal too, where its caveat promises it was "written
    as it stood when the step closed".

    Only `list_data_migrations` wrote it at first. That made the promise false
    by however long ago the operator last listed — weeks, on a real migration
    — and the instructions tell the agent NOT to re-list, so the stale window
    is long by design. A reviewer opening the primary artifact was told two
    databases had not moved right after reporting them, and at the terminal
    right after being told the step closed with nothing outstanding.

    A caller passing `closed` is exempt from the liveness check below: it has
    just written that state itself, so the persisted value agreeing with it is
    the whole point.
    """
    if closed is None:
        # Read HERE, immediately before the write it guards. There is no
        # parameter to pass a verdict in with: the one caller that did was
        # deciding two GCS round-trips early, which is the window this check
        # exists to close.
        verdict = _step_liveness(context["bucket"])[0]
        if verdict == CLOSED:
            return CLOSED_UNDER_US
        if verdict == UNKNOWN:
            return _UNREADABLE_STATE
    try:
        _save_runbook(context["bucket"], dm.runbook(
            context["entries"], context["document"],
            context["state_dict"]["variables"].get("workspace_name")
            or "this workspace", _now(),
            copies=dm.unconfirmed_copies(context["inventory"]),
            rendered=rendered_runbooks(context["bucket"]),
            closed=closed))
        return ""
    except Exception as e:
        # Reported, not just logged. The refresh exists so the terminal's
        # artifact caveat — "written as it stood when the step closed" — is
        # true, and a silent failure is the one case that makes it false
        # again, with nothing callable at the terminal to correct it.
        logger.warning("Could not refresh the data migration runbook: %r", e)
        return (f"WARNING: {dm.RUNBOOK_BLOB} could not be refreshed ({e}), so "
                "it still shows the worklist as it stood at the last listing "
                "rather than at the close. The outcomes themselves are safe in "
                f"{dm.MIGRATIONS_BLOB}.")


def rendered_runbooks(bucket, strict: bool = False):
    """Ledger paths of the per-service runbooks already adapted and saved.

    Best effort by default. A listing failure costs the reader a "rendered"
    marker and nothing else, so it must not cost them the worklist — the same
    reason the runbook write itself is wrapped where it is called. `strict`
    is for the one caller that cannot take an empty set for an answer: the
    save tool's earlier-copy guard, where "could not list" read as "nothing
    there" and the adapted copy was written around in silence — the fault
    the guard exists for, produced by the guard's own input.
    """
    try:
        # Through the CLIENT, not the bucket, because that is the form the
        # rest of the repo uses — `servers/frontend/server.py:178`. Both forms
        # exist on the real API and on `fake_gcs` (client at :143, bucket at
        # :101), so this is convention rather than necessity; the reason to
        # keep it is that a reader comparing the two listing sites should not
        # have to wonder why they differ.
        return {blob.name for blob in state_mgr.gcs_client.list_blobs(
            bucket.name, prefix=rb.RENDERED_PREFIX)}
    except Exception as e:
        if strict:
            raise
        logger.warning("Could not list rendered runbooks: %r", e)
        # A sentinel, not an empty set: "could not tell" is not "none there".
        # An empty set made the worklist state as fact that an adapted
        # runbook did not exist and that the steps live only inside the
        # server. Not None either — that is the callers' "no listing taken".
        return dm.LISTING_UNREAD


def _save_runbook(bucket, text: str) -> None:
    bucket.blob(dm.RUNBOOK_BLOB).upload_from_string(
        text, content_type="text/markdown; charset=utf-8")


def _open(tool_name: str, states=REPORTING_STATES):
    """(context, error). Exactly one of the two is None."""
    try:
        state_dict, generation, config = authorize_and_rehydrate(None)
    except Exception as e:
        return None, f"ERROR: {e}"
    current = state_dict["current_state"]
    if current not in states:
        return None, (
            f"ERROR: Invalid state for {tool_name}: {current}. "
            # Both populations and both exits, like every sibling statement in
            # this step. Keyed on the data half alone, an estate with no
            # `migrate` service — the ordinary bare-accept shape, four copies
            # self-service — reads this as "nothing to wait for", and the
            # agent relays it verbatim from the terminal.
            + f"This tool runs at {STATE}, which the deployment segment "
              "parks in until every data service graded 'migrate' has been "
              "reported migrated or excused with 'keep-in-aws', AND every "
              "planned or failed container image copy has been confirmed or "
              "abandoned.")
    bucket = state_mgr.gcs_client.bucket(get_bucket_name(config["ledger_uri"]))
    try:
        inventory, _ = load_inventory(bucket)
    except Exception as e:
        return None, f"ERROR: Could not read the inventory: {e}"
    if inventory is None:
        return None, ("ERROR: No discovery inventory exists in this ledger; "
                      "there are no data services to report on.")
    try:
        document, doc_generation = load_migrations(bucket)
    except Exception as e:
        return None, (f"ERROR: Could not read {dm.MIGRATIONS_BLOB}, which "
                      f"holds what has already moved: {e}. Nothing was "
                      "recorded; try again.")
    if document is None:
        return None, (f"ERROR: {dm.MIGRATIONS_BLOB} exists but is not a "
                      "readable set of migration records. It is the only "
                      "record of which data services have moved — repair or "
                      "remove the object, then try again.")
    return {
        "state_dict": state_dict, "generation": generation, "config": config,
        "bucket": bucket, "inventory": inventory,
        "entries": inventory.get("data_dependencies") or [],
        "document": document, "doc_generation": doc_generation,
    }, None


def _target(context, address: str, directory: Optional[str],
            identifier: Optional[str]) -> dict:
    """The one entry a report is about, or a refusal naming the ambiguity.

    The same three axes the corrections store keys on, and the same refusal
    when they do not separate: two root modules can declare one address, and
    an `_override.tf` that renames leaves two entries sharing an address and a
    directory. Recording "this moved" against the wrong one of a dev/prod pair
    would let a component ship against a database still in AWS.
    """
    # Matched the way the outcome store matches, `arn` included: an entry the
    # operator addressed by its ARN before a later scan folded it onto its
    # declaration still answers to that ARN, and the runbook they copied it
    # from was printed before the fold.
    matches = [e for e in context["entries"]
               if e.get("address") == address
               or (e.get("arn") and e.get("arn") == address)
               # And the endpoint a queue was known by before a policy named
               # its ARN — the runbook printed then names the URL.
               or (e.get("endpoint") and e.get("endpoint") == address)]
    by_spelling = not matches
    if not matches:
        # Two passes, as `overrides.entries_at` resolves: an entry's handle is
        # one SPELLING of its ARN and the merge settles on another when the
        # set of sightings changes, so a call copied out of a runbook rendered
        # before a new sighting names a spelling no entry carries any more.
        # Refusing there was safe but left this the one reader of the handle
        # that disagreed with the others. Over-matching still refuses below.
        matches = [e for e in context["entries"]
                   if datastores.arn_spellings_agree(address, e.get("arn"))
                   or datastores.arn_spellings_agree(address, e.get("address"))]
    if directory is not None:
        matches = [e for e in matches if dm.directory_of(e) == directory]
    if identifier is not None:
        matches = [e for e in matches if e.get("identifier") == identifier]
    flagged = [e for e in matches if datastores.spelling_is_ambiguous(e)]
    if by_spelling and flagged:
        # What the replay refuses, refused here too: reached by SPELLING
        # onto an entry the scan keeps apart as ambiguous. A call copied out
        # of an earlier runbook naming the estate's own secret, no longer in
        # the section, otherwise released the any-account entry — which may
        # be the 333 or the 444 secret, the reason it was flagged — from
        # `outstanding` on a report about a different spelling.
        raise _Refused(
            f"ERROR: '{address}' matches {len(flagged)} data service"
            f"{'' if len(flagged) == 1 else 's'} by spelling only, and the scan "
            "keeps them apart as ambiguous: "
            + ", ".join(sorted(e.get("address") or "" for e in flagged))
            + ". Report against the exact address as list_data_migrations() "
            "prints it.")
    if not matches:
        known = sorted({e.get("address") for e in context["entries"]
                        if e.get("address")})
        raise _Refused(
            f"ERROR: no data service is recorded at '{address}'"
            # `is not None`, not truthiness — the same reason the review's
            # sibling refusal gives: a root-declared entry's directory is the
            # empty string, `reporting_call` emits `directory=""` for it on
            # purpose, and the one call that needs telling why it matched
            # nothing is the one that passed it. Dropping it printed "no data
            # service is recorded at X" and then listed X.
            + (f" in directory '{directory}'" if directory is not None else "")
            + (f" with identifier '{identifier}'"
               if identifier is not None else "")
            + ". The section records: "
            + (", ".join(known) if known else "nothing at all") + ".")
    if len(matches) > 1:
        # Only axes that actually separate these matches. Offering
        # "directory=<one of 'envs/prod', 'envs/prod'>" is not a disambiguator,
        # it is noise the caller has to see through.
        directories = sorted({dm.directory_of(e) for e in matches})
        identifiers = sorted({e.get("identifier") for e in matches})
        options = []
        if len(directories) > 1:
            options.append("directory=<one of "
                           + ", ".join(f"'{d}'" for d in directories) + ">")
        if len(identifiers) > 1:
            options.append("identifier=<one of "
                           + ", ".join(f"'{i}'" for i in identifiers) + ">")
        if not options:
            # Entries reached through the spelling fallback: known only from
            # their own ARNs, no declaring root module, one identifier. The
            # exact spelling is the one thing that separates them, and the
            # listing prints it — the review's resolver says the same, and
            # "should not happen; report it" was this reader disagreeing.
            addresses = sorted({e.get("address") for e in matches if e.get("address")})
            raise _Refused(
                f"ERROR: '{address}' names {len(matches)} data services. Each is "
                "known only from its own ARN and this spelling matches more than "
                "one of them — the scan could not tell whether they are one "
                "resource or several, so it kept them apart. Neither directory= "
                "nor identifier= separates them. Call again with the exact "
                "address as list_data_migrations() prints it: "
                + ", ".join(addresses) + ".")
        raise _Refused(
            f"ERROR: '{address}' names {len(matches)} data services and this "
            "call does not say which. Re-call with " + " or ".join(options) + ".")
    return matches[0]


async def _report(tool_name: str, status: str, address: str,
                  directory: Optional[str], identifier: Optional[str],
                  target: Optional[str], note: Optional[str]) -> str:
    """The shared body of the two reporting tools."""
    context, error = _open(tool_name)
    if error:
        return error
    try:
        entry = _target(context, address, directory, identifier)
    except _Refused as refusal:
        return str(refusal)

    # What `record_status` will store, so every sentence below reports the
    # value that was written rather than the one that was typed. `"   "` is
    # not a destination and must not read as one anywhere.
    if target is not None:
        target = dm.clean_value(target)
    if note is not None:
        note = dm.clean_value(note)
    if target and dm.looks_like_a_placeholder(target):
        return (f"ERROR: '{target}' is the placeholder from the printed call, "
                "not a destination. Pass where the service actually landed — "
                "a Cloud SQL instance name, a bucket URI — or omit target= "
                "entirely. It is recorded verbatim into the durable record of "
                "where customer data went, so a stand-in is worse there than "
                "nothing.")

    if entry.get("disposition") != dm.GATING_DISPOSITION:
        # Not an error worth refusing over — recording that something moved is
        # never harmful — but the operator should know it was not owed, in
        # case they are working from a stale list or meant a different entry.
        prefix = (f"Note: {dm.describe(entry)} is graded "
                  f"'{entry.get('disposition')}', not 'migrate', so it was not "
                  "holding anything up. Recorded anyway. ")
    else:
        prefix = ""

    previous = dm.status_of(context["document"], entry, context["entries"]) or {}
    _, carried, dropped = dm.record_status(
        context["document"], entry, status,
        state_mgr.get_authenticated_user_email(), _now(),
        target=target, note=note)
    try:
        _save_migrations(context["bucket"], context["document"],
                         context["doc_generation"])
    except exceptions.PreconditionFailed:
        return ("ERROR: another session recorded a data migration outcome "
                "while this one was being written. Nothing was saved — call "
                "list_data_migrations() to see the current state, then report "
                "again.")
    except Exception as e:
        return f"ERROR: Could not record the outcome: {e}"

    # The report just changed what the worklist says, and this artifact is
    # what a platform engineer opens to read it. Best-effort and after the
    # save: the outcome is already durable, and a failed redraw must not read
    # as a failed report.
    runbook_warning = _refresh_runbook(context)
    # And the developer side of the same fact. A report that a database
    # landed is the event that releases every component held on it, and the
    # only way that reaches a developer session is this slice.
    exports_warning = _publish_data_gate(context)
    owed = dm.outstanding(context["entries"], context["document"])
    copies_left = dm.unconfirmed_copies(context["inventory"])
    # Said out loud. Silence about a field that survived is how the operator
    # learns nothing was lost; silence about one that changed is how they miss
    # that it was.
    kept = (" Kept from the earlier report: "
            + ", ".join(f"{f} ({previous[f]})" for f in carried) + "."
            if carried else "")
    overwritten = sorted(
        f for f, v in (("target", target), ("note", note))
        if v and previous.get(f) and previous[f] != v)
    replaced = (" Replaced: "
                + ", ".join(f"{f} was {previous[f]}" for f in overwritten) + "."
                if overwritten else "")
    cleared = sorted(f for f, v in (("target", target), ("note", note))
                     if v == "" and previous.get(f))
    cleared_line = (" Cleared: "
                    + ", ".join(f"{f} was {previous[f]}" for f in cleared)
                    + "." if cleared else "")
    # A note describes the status it was written against, so the status change
    # left it behind. Named, because an operator who wanted it kept has to
    # know to pass it again — and one who did not would otherwise never learn
    # the runbook stopped showing it.
    lost = (" The note recorded against "
            f"'{(previous.get('status') or '').replace('_', ' ')}' was not "
            f"carried onto this one ({previous.get('note')})."
            # No "pass note= to record one" once the step has closed under
            # this call: that is an instruction to call a tool that refuses.
            + ("" if runbook_warning == CLOSED_UNDER_US else
               " Pass note= to record one for the new status.")
            if "note" in dropped else "")
    # No CLOSED_UNDER_US guard here, deliberately: the only consumer of this
    # is the innermost arm of a ternary that has already tested the same
    # condition, so a guard would be a branch that cannot be taken — the shape
    # this package removes by name in two other places.
    frozen = dm.closing_freezes_in_flight(context["entries"],
                                          context["document"])
    # The past-tense form, for the arm where the close already happened.
    was_frozen = dm.closed_over_in_flight(context["entries"],
                                          context["document"])
    return (
        prefix
        + f"Recorded: {dm.describe(entry)} is {status.replace('_', ' ')}"
        + (f" at {target}" if target else "")
        + ". "
        # Once the step has closed under this call, nothing "still owes" and
        # nothing "holds the step open" — those sentences describe a gate
        # that no longer exists, and the warning below says why.
        + ("The data migration step is no longer open, so nothing waits on "
           "this or any other service now."
           # Including, when this call recorded one, that the record was born
           # uncompletable. `CLOSED_UNDER_US` says the runbook does not
           # reflect this call, so without this the record appears in no
           # document that says so.
           + (f" {was_frozen}" if was_frozen else "")
           if runbook_warning == CLOSED_UNDER_US else
           f"{len(owed)} data service(s) still owe a move: "
           + ", ".join(dm.describe(e) for e in owed[:5])
           + ("..." if len(owed) > 5 else "") + "."
           if owed else
           # Both populations, because the close waits on both. Keyed on the
           # data alone this said "Nothing else is owed" and the very next
           # close refused, naming an image copy — and the agent relays this
           # sentence verbatim.
           (f"No data service is still owed, but {len(copies_left)} container "
            "image copy/copies are unconfirmed and hold the step open: "
            + ", ".join(copies_left) + "."
            if copies_left else
            # The one transition the step exists to reach, named at the one
            # moment it becomes available. Without this the operator settles
            # the last service and is told nothing, while the instructions
            # tell the agent to stand down — and the walk parks for good.
            #
            # With the cost, when a cutover is running: this same call is what
            # records a move IN PROGRESS, so the sentence proposing the close
            # can otherwise sit in the response that opened the cutover.
            "Nothing else is owed: complete_data_migration() closes the step."
            + (f" {frozen}" if frozen else "")))
        + kept + replaced + cleared_line + lost
        + (f"\n{runbook_warning}" if runbook_warning else "")
        + (f"\n{exports_warning}" if exports_warning else "")
)


async def list_data_migrations(ctx: Context = None) -> str:
    """
    Lists the data services that still have to move, and writes the runbook.

    Read-only apart from the runbook, which is refreshed on every call while
    the step is open — not once it has closed underneath the call, when the
    artifact is the record of the close, and not when the state cannot be
    read — so it
    describes the section as it stands rather than as it stood when this state
    was entered. Shows who is waiting for each service, what it moves to, and
    what has already been reported.
    """
    logger.info("list_data_migrations called.")
    context, error = _open("list_data_migrations")
    if error:
        return error

    entries, document = context["entries"], context["document"]
    owed = dm.outstanding(entries, document)
    done = dm.settled(entries, document)
    workspace = context["state_dict"]["variables"].get(
        "workspace_name") or "this workspace"
    rendered = rendered_runbooks(context["bucket"])
    listing_unread = rendered is dm.LISTING_UNREAD
    if listing_unread:
        rendered = set()

    # None through to the artifact when the listing failed: each procedure
    # line then says the ledger could not be read, instead of stating as
    # fact that no adapted runbook exists.
    text = dm.runbook(entries, document, workspace, _now(),
                      copies=dm.unconfirmed_copies(context["inventory"]),
                      rendered=dm.LISTING_UNREAD if listing_unread else rendered)
    verdict = _step_liveness(context["bucket"])[0]
    try:
        # Once the verdict is CLOSED, the four call lines below are all calls
        # that refuse at the terminal — the same invariant the runbook
        # renderer applies with `actionable`, and the listing carries the
        # same "the step has closed" line. UNKNOWN keeps them: a failed state
        # read is not evidence the step closed, and withholding the calls
        # from a live step is the worse error.
        closed_under_us = verdict == CLOSED
        if verdict != LIVE:
            # Either another session closed the step while this listing was
            # being assembled, or the state could not be read. The answer
            # below is still worth giving; overwriting the terminal's record
            # with it is not — and saying WHICH matters, because "the step has
            # closed" printed above a list of services still owed is a claim
            # about the workspace that a failed GET does not support.
            raise _Refused("")
        _save_runbook(context["bucket"], text)
        runbook_line = (f"The worklist is written to {dm.RUNBOOK_BLOB} in the "
                        "ledger.")
    except _Refused:
        runbook_line = (
            f"{dm.RUNBOOK_BLOB} was not rewritten: the step has closed, and at "
            "the terminal that document is the record of what the close left "
            "behind." if verdict == CLOSED else _UNREADABLE_STATE)
    except Exception as e:
        # Best effort: the listing below is the same information, so a failed
        # runbook write must not cost the operator the answer.
        logger.warning("Could not write the data migration runbook: %r", e)
        runbook_line = (f"(The runbook could not be written to the ledger: "
                        f"{e}. The list below is the same information.)")

    # Counted over one population, not two. `settled` reports every entry
    # migrated whatever its grade — deliberately, so a service kept in AWS
    # after it moved does not vanish — but subtracting that from the `migrate`
    # count produced arithmetic nonsense in the one line whose job is to say
    # how much is left.
    gating_done = [e for e in done
                   if e.get("disposition") == dm.GATING_DISPOSITION]
    extra_done = len(done) - len(gating_done)
    out = [
        # Past tense once the step has closed under this call: "still owed"
        # is a claim about a gate that no longer exists, and it is the first
        # line the agent relays.
        (f"Data services as this session saw them: "
         f"{len(dm.gating(entries))} graded 'migrate' — "
         f"{len(gating_done)} reported migrated, {len(owed)} unreported."
         if closed_under_us else
         f"Data services: {len(dm.gating(entries))} graded 'migrate' — "
         f"{len(gating_done)} reported migrated, {len(owed)} still owed.")
        + (f" ({extra_done} more moved that this step was not waiting on.)"
           if extra_done else ""),
        runbook_line,
        "",
    ]
    # Whether any address is declared twice decides whether the reporting
    # calls below need a directory to be unambiguous. Computed once over the
    # whole section rather than per entry: an address is ambiguous because
    # ANOTHER entry shares it.
    repeated_addresses, repeated_pairs = dm.ambiguity(entries)

    for entry in owed:
        record = dm.status_of(document, entry, entries) or {}
        consumers = dm.consumers_of(entry)
        target, how = dm.target_and_how(entry)
        # `directory_of`, not `entry_directory`: an entry known only from its
        # ARN has no declaring root module, and its `evidence[0]` is whichever
        # file the walk met first. Printing that told the operator the
        # resource lives in a directory that does not declare it, and offered
        # a disambiguator the outcome store no longer keys on.
        directory = dm.directory_of(entry)
        out.append(f"- {dm.describe(entry)} — {record.get('status') or 'not started'}")
        # The ADDRESS, not just the name. `rds orders-db` is how the service is
        # spoken about; `aws_db_instance.orders` is what the reporting tools
        # take, and an agent that has only the first has to invent the second.
        out.append(f"    address: {entry.get('address')}"
                   + (f"  (in {directory})" if directory else ""))
        out.append(f"    used by: " + (", ".join(consumers) if consumers
                                       else "nobody the scan could attribute "
                                            "(unattributed is not unused)"))
        out.append(f"    moves to: {target} ({how})")
        if record.get("note"):
            out.append(f"    note: {record['note']}")
        # Spelled out, because the reporting call is the whole point of this
        # listing and the arguments are not guessable from the name.
        if not closed_under_us:
            out.append("    report it: " + dm.reporting_call(
                entry, repeated_addresses, repeated_pairs))
            # The offer, per service, in the operator's terms. Printed here
            # rather than left to the instructions because the instructions
            # cannot know which of these has a procedure behind it: offering
            # help with a DynamoDB table and then declining is worse than not
            # offering, and the difference is a table in `runbooks.py` rather
            # than something an agent can reason its way to.
            call = dm.runbook_call(entry, repeated_addresses, repeated_pairs)
            # Under every handle the entry has had: the name moves across a
            # fold, the adapted file does not (`rendered_blobs`).
            saved = next((p for p in rb.rendered_blobs(
                entry, dm._matching_records(document, entry, context["entries"]))
                if p in rendered),
                rb.rendered_blob(entry))
            if saved in rendered:
                # Before the `name_for` gate, not inside it. A service with no
                # shipped template can still have a runbook somebody wrote by
                # hand — `_no_runbook` says "that plan is theirs to write" —
                # and gating this on the template made that file invisible
                # everywhere except the refusal to overwrite it.
                out.append(f"    runbook already adapted: {saved}"
                           " — read it before adapting again")
            if rb.name_for(entry) is not None:
                out.append(f'    offer help: {call}'
                           f' — "{rb.offer(entry, target)}"')
            elif rb.needs_engine(entry):
                # Not "no procedure": four RDS procedures ship and one of them
                # is this database's. What is missing is a fact the
                # declaration did not state, and the operator has it. The
                # printed call carries `engine=` because nothing else records
                # it — without the argument the same call refuses again, which
                # is the loop this line used to send an agent round.
                out.append(
                    "    offer help: ask which engine this runs ("
                    + ", ".join(rb.ENGINE_CHOICES) + ") — the four differ "
                    "fundamentally — then " + call[:-1]
                    + ', engine="<one of those>")')
    if done:
        out.append("")
        out.append("Already migrated:")
        for entry in done:
            record = dm.status_of(document, entry, entries) or {}
            grade = entry.get("disposition")
            out.append(
                f"- {dm.describe(entry)} → "
                f"{record.get('target') or 'target not recorded'}"
                # Named when it is not what the step waits on, so a reader can
                # tell "this moved and was owed" from "this moved and then the
                # customer decided to keep it" — both are true statements and
                # only the first is progress against the gate.
                + (f"  (graded '{grade}', not owed here)"
                   if grade != dm.GATING_DISPOSITION else ""))
            if record.get("note"):
                out.append(f"    note: {record['note']}")
            if not closed_under_us:
                out.append("    amend it: " + dm.reporting_call(
                    entry, repeated_addresses, repeated_pairs))

    copies = dm.unconfirmed_copies(context["inventory"])
    if copies:
        out.append("")
        out.append(
            f"{len(copies)} container image(s) were unconfirmed as this session "
            "saw it: the copies were planned or attempted and nobody had "
            "reported them."
            if closed_under_us else
            f"{len(copies)} container image(s) are also waiting on you: the "
            "copies were planned or attempted but not confirmed. Run them and "
            "report with mark_replication_complete — until then a developer's "
            "image references are left un-rewritten, because the destination "
            "is a plan rather than an address.")
        # Every one of them, uncapped. `abandon_image_replication` has no bulk
        # form on purpose — it needs each ref named — so a truncated list
        # leaves the operator with no way to spell the exit for a copy it did
        # not print, and the only reachable alternative is a bulk
        # `mark_replication_complete()` asserting they were all copied.
        for ref in copies:
            out.append(f"- {ref}")
        if not closed_under_us:
            out.append(
                '    report with: mark_replication_complete(refs=["<ref above>"])'
                ' — or abandon_image_replication(refs=["<ref above>"], '
                'reason="...") if it is not going to be copied')

    in_flight = dm.in_flight_not_owed(entries, document)
    if in_flight:
        out.append("")
        out.append(
            f"{len(in_flight)} move(s) reported in progress against a service "
            "this step does not gate — excused after the move started, or "
            "never graded 'migrate'. Nothing waits on them, but the AWS-side "
            "resource is still live:")
        for entry in in_flight:
            record = dm.status_of(document, entry, entries) or {}
            out.append(f"- {dm.describe(entry)} "
                       f"(graded '{entry.get('disposition')}')")
            if record.get("note"):
                out.append(f"    note: {record['note']}")
            if not closed_under_us:
                out.append("    report it when it lands: " + dm.reporting_call(
                    entry, repeated_addresses, repeated_pairs))

    stale = dm.stale_records(entries, document)
    if stale:
        out.append("")
        out.append(
            f"{len(stale)} recorded outcome(s) name a data service the current "
            "section does not carry — the scope may have narrowed, the "
            "resource may be gone, or it may now be kept in AWS. Kept as a "
            "record: " + ", ".join(
                f"{r.get('service')} {r.get('identifier') or r.get('address')}"
                for r in stale[:5]) + ("..." if len(stale) > 5 else ""))

    out.append("")
    if closed_under_us:
        out.append(
            "The step has closed under this call; none of the tools this "
            "listing would normally offer can be called from the terminal. "
            "What is shown is the record as this session saw it.")
    elif owed:
        # `owed`, for the same reason as in the other renderer. "It stops being
        # owed and stops gating" describes a transition nothing can undergo
        # when the line above says 0 are owed, and the listing prints an
        # address only against an owed or settled entry — so with none owed
        # this offers a call whose one argument the operator has not been
        # given, against instructions that say never to construct one.
        out.append(
            "A service that should not move after all: annotate_data_dependency("
            "address=..., disposition=\"keep-in-aws\", note=...). It stops "
            "being owed and stops gating, and costs cross-cloud connectivity, "
            "a credential path and egress.")
    if not closed_under_us and not owed and not copies:
        # Nothing is holding the step open, so say what closes it. An estate
        # with no `migrate` service and an agent-mediated replication reaches
        # this on arrival, and the instructions tell the agent to hand back
        # control rather than propose anything.
        out.append(
            "Nothing is outstanding: complete_data_migration() closes the "
            "step and moves the platform walk to its terminal.")
        # Three lines above this, the in-flight section says "report it when
        # it lands". The close ends that.
        frozen = dm.closing_freezes_in_flight(entries, document)
        if frozen:
            out.append(frozen)
    return "\n".join(out)


async def mark_data_service_migrated(address: str,
                                     directory: Optional[str] = None,
                                     identifier: Optional[str] = None,
                                     target: Optional[str] = None,
                                     note: Optional[str] = None,
                                     ctx: Context = None) -> str:
    """
    Records that a data service is live in GCP and verified.

    Your assertion, recorded with your name against it: the move runs with your
    credentials and the server never sees it happen.

    Args:
        address: the Terraform address, e.g. aws_db_instance.orders.
        directory: the root module, when two declare the same address.
        identifier: the service's name, when one directory declares two.
        target: where it landed (a Cloud SQL instance, a bucket). Recorded
            verbatim and never invented. Omit it to keep what was recorded
            before; pass "" to clear it.
        note: anything the next reader needs — a caveat, a partial cutover.
            A note recorded while the move was in progress is NOT carried onto
            the completion, because it described the progress; pass a new one
            here if the completed record needs it. Omit it to keep the note
            already recorded against THIS status; pass "" to clear it, which
            is the only way to take a wrong note back out.

    Only 'migrate' services hold a component up, but any of them can be
    recorded.
    """
    logger.info(f"mark_data_service_migrated called (address={address!r}).")
    return await _report("mark_data_service_migrated", dm.MIGRATED, address,
                         directory, identifier, target, note)


async def mark_data_service_migrating(address: str,
                                      directory: Optional[str] = None,
                                      identifier: Optional[str] = None,
                                      note: Optional[str] = None,
                                      ctx: Context = None) -> str:
    """
    Records that a data service's move is under way.

    This does NOT satisfy anything: a component must not ship against a
    database that is still copying. It is for the next person to see where
    things stand, which on a multi-week migration is most of the value.

    Args:
        address: the Terraform address, e.g. aws_db_instance.orders.
        directory: the root module, when two declare the same address.
        identifier: the service's name, when one directory declares two.
        note: where it has got to — the job name, the expected cutover.
            Omit it to keep the one already recorded against this status;
            pass "" to clear it. This is the note that survives weeks in the
            runbook and the review UI, so correcting a wrong one matters more
            here than anywhere else in the step.
    """
    logger.info(f"mark_data_service_migrating called (address={address!r}).")
    return await _report("mark_data_service_migrating", dm.IN_PROGRESS,
                         address, directory, identifier, None, note)


async def complete_data_migration(ctx: Context = None) -> str:
    """
    Closes the deployment segment. Refuses while anything is still
    unconfirmed, with no exceptions.

    Not impatience: a terminal state carries no instructions, so a workspace
    that reached it with databases outstanding would never tell anyone again
    that they are outstanding, and the tool for reporting them would be behind
    the operator with nothing to mention it. Parking here is what keeps the
    outstanding work discoverable to a session that starts weeks from now.

    "Anything" is both kinds of out-of-band work: data services graded
    `migrate` that have not been reported, and container image copies the
    server planned or attempted but did not observe. Both are settled by tools
    that only run here, so closing over either would strand it.

    A service that genuinely cannot move is not a trap: record it
    `keep-in-aws` with annotate_data_dependency and it stops being owed.

    NO EXCEPTIONS, INCLUDING A DAMAGED LEDGER. That exit replays over the
    scan's baseline, so a missing or unusable one costs it — and this refuses
    rather than closing anyway. The step stays where it is until the object is
    restored or every owed service is reported. A migration reported complete
    is a claim the whole system makes to everyone downstream; it must not rest
    on a ledger nobody can vouch for, and the damage is not something an
    operator reaches innocently (the scan writes the baseline in the same call
    that writes the section).
    """
    logger.info("complete_data_migration called.")
    context, error = _open("complete_data_migration", states=(STATE,))
    if error:
        return error

    state_dict = context["state_dict"]
    try:
        platform_dag = load_dag(context["bucket"], "platform_dag.json")
    except Exception as e:
        return f"ERROR: {e}"
    dest = platform_dag["states"][STATE]["transitions"]["on_tool_call_received"]

    owed = dm.outstanding(context["entries"], context["document"])
    done = dm.settled(context["entries"], context["document"])
    copies = dm.unconfirmed_copies(context["inventory"])

    # `keep-in-aws` — the exit for a service that cannot move — runs through
    # the data review's amend path, which refuses without a usable scan
    # baseline (`platform/discovery/data_dependencies_scan.json`). So the
    # baseline is graded here too, and every grade short of "ok" refuses: an
    # unusable baseline is a damaged ledger, and a damaged ledger does not
    # finish the workflow. The grading is what makes the refusal SAY the right
    # thing — a transport failure is retried, an object that is present and
    # wrong is replaced, an absent one has to be restored — and it has to
    # agree with `_amend`'s, or the two tools contradict each other about one
    # object.
    #
    # A failed read is kept apart from every grade: `load_scan_baseline`
    # already turns absence and corruption into a value, so an exception here
    # is a 503, a timeout, one Forbidden read — and "try again" is its answer,
    # not "the ledger is damaged".
    baseline_state, exit_check_failed = "ok", None
    if owed:
        try:
            # The live section goes in: a baseline that does not account for
            # it grades UNREADABLE, because a `keep-in-aws` recorded against
            # it would delete the entries it omits rather than excuse one.
            # `_amend` refuses on the same test, and the two grades have to
            # agree or this closes on an exit that does not work.
            baseline_state = scan_baseline_state(
                context["bucket"], section=context["entries"])
        except Exception as e:
            exit_check_failed = e

    if owed and exit_check_failed is not None:
        lines = [
            f"ERROR: could not read {SCAN_BASELINE_BLOB} to decide whether "
            f"'keep-in-aws' is still recordable: {exit_check_failed}. Nothing "
            "was changed and the step is still open — try again.",
            f"{len(owed)} data service(s) are still owed: "
            + ", ".join(dm.describe(e) for e in owed)
            # With the exit, like both sibling refusals. It matters most here:
            # the BASELINE is what failed, and `_report` never reads it, so
            # "reporting still works" is true and not obvious from a message
            # whose subject is a baseline the server could not read. The step's
            # instructions have the agent relay an ERROR verbatim and stop, so
            # an enumeration with no call in it is where the operator stops.
            + ". They can still be REPORTED: mark_data_service_migrated works "
            "normally and does not read that object, and once every owed "
            "service has moved this step closes cleanly.",
        ]
        if copies:
            # Both other refusals in this function name both populations, and
            # this one is read the same way: as the list of what is holding the
            # step open. Leaving the copies out sends an operator to settle the
            # databases and meet them only at the next refusal.
            lines.append(
                f"{len(copies)} container image copy/copies are also "
                "unconfirmed and hold the step open: " + ", ".join(copies)
                + ". Report each with mark_replication_complete, or "
                "abandon_image_replication(refs=[...], reason=...) for one "
                "that will not be copied.")
        return "\n".join(lines)

    if owed and baseline_state in (BASELINE_UNREADABLE, BASELINE_ABSENT):
        # A DAMAGED LEDGER DOES NOT GET TO FINISH. Recording `keep-in-aws`
        # replays over the scan's own output, so an unusable baseline costs
        # the exit — and an earlier revision closed anyway when the object was
        # absent, on the grounds that refusing would wedge a workspace on a
        # service it could neither report nor excuse.
        #
        # That trade assumed an operator could arrive at the damage innocently.
        # They cannot: `scan_data_dependencies` writes the baseline in the same
        # call that writes the section, so a ledger carrying data services and
        # no baseline has had the object removed by hand. Between stopping on
        # that and stamping SUCCESS over it, stopping is the only defensible
        # answer — a completed migration is a claim the whole system makes to
        # everyone downstream, and it must not rest on an artifact somebody
        # deleted.
        #
        # So both grades refuse, and the step stays where it is. The damage is
        # named, the repair is named, and reporting — which never touches this
        # object — keeps working throughout, so a workspace whose services do
        # move still closes cleanly.
        try:
            parsed = load_scan_baseline(context["bucket"])
        except Exception:
            parsed = None
        lines = [
            f"ERROR: this ledger is damaged. {SCAN_BASELINE_BLOB} "
            + ("does not exist, and the scan writes it in the same call that "
               "writes the data services below — so it has been removed"
               if baseline_state == BASELINE_ABSENT else
               "parses, but it does not describe the current section: it is "
               "missing data services the section carries, so a correction "
               "against it would delete them"
               if parsed else
               "exists but is not readable as a scan baseline")
            + ".",
            "The step will not close over that. 'keep-in-aws' is replayed "
            f"over that object, so the {len(owed)} data service(s) still owed "
            "cannot be EXCUSED: " + ", ".join(dm.describe(e) for e in owed)
            + ". Completing the migration anyway would tell every application "
            "team the platform side is finished, on the strength of a ledger "
            "nobody can vouch for.",
            # Reporting is unaffected — `_report` never reads the baseline —
            # and saying otherwise removes the ordinary way out of this state
            # in the one message standing in front of it.
            "They can still be REPORTED: mark_data_service_migrated works "
            "normally, and once every owed service has moved this step closes "
            "cleanly, without needing the baseline at all.",
            (f"Replace {SCAN_BASELINE_BLOB} with the copy the scan wrote "
             "and try again — that is the repair, and it restores every exit."
             if baseline_state == BASELINE_UNREADABLE else
             f"To restore the exit, {SCAN_BASELINE_BLOB} has to come back. If "
             "the ledger bucket has object versioning enabled, the previous "
             "generation is the copy the scan wrote. If it does not, the only "
             "mechanism this server offers is an admin "
             "`join_ledger --reconfigure`: it resets the platform walk to the "
             "start state, so the whole platform sequence is walked again and "
             "the scan rewrites this object. The ledger keeps what it has — "
             "no artifact or recorded decision is discarded — but every step "
             "from onboarding onward is redone, so say what that means before "
             "anyone reaches for it.")
            + " The step stays open and nothing was changed.",
        ]
        if copies:
            # Named here rather than left for a second refusal after the
            # databases are settled.
            lines.append(
                f"Separately, {len(copies)} container image copy/copies are "
                "unconfirmed and also hold the step open: "
                + ", ".join(copies)
                + ". Report each with mark_replication_complete, or "
                "abandon_image_replication(refs=[...], reason=...) for one "
                "that will not be copied.")
        return "\n".join(lines)

    if owed or copies:
        # Both, in one refusal. The step waits for everything the server cannot
        # observe for itself, and the tools that settle each are callable only
        # from here — so closing with either outstanding strands it.
        lines = ["ERROR: the deployment segment is not finished."]
        if owed:
            lines.append(
                f"{len(owed)} data service(s) still owe a move: "
                + ", ".join(dm.describe(e) for e in owed) + ". Report each "
                "with mark_data_service_migrated when it lands; if one is not "
                "going to move after all, annotate_data_dependency(address=..."
                ", disposition=\"keep-in-aws\", note=...) records that and it "
                "stops being owed.")
        if copies:
            # With its exit. The data clause names `keep-in-aws`; leaving the
            # image clause without `abandon_image_replication` tells an
            # operator whose upstream base image is gone to report a copy they
            # cannot make, in the one message whose job is to explain why the
            # step will not close.
            lines.append(
                f"{len(copies)} container image copy/copies are unconfirmed: "
                # All of them, for the reason the listing prints all of them:
                # `abandon_image_replication` needs each ref named, and this
                # is the message that offers it as the exit. Naming an exit
                # while withholding the argument it takes is not an exit.
                + ", ".join(copies)
                + ". Report each with mark_replication_complete when it "
                "lands; if one is not going to be copied after all, "
                "abandon_image_replication(refs=[...], reason=...) records "
                "that and it stops being owed.")
        lines.append(
            "The graph stays here on purpose. A terminal state carries no "
            "instructions and accepts no tool calls, so moving on now would "
            "leave nobody to tell the next session this work exists and "
            "nothing able to record it when it lands.")
        return "\n".join(lines)
    # No outstanding count: the refusal above returns before this line
    # whenever anything is owed, so it could only ever be zero. The stamp says
    # what was true when the step closed, which is that everything had been
    # settled one way or the other.
    state_dict["variables"]["data_migration_review"] = {
        "left_by": state_mgr.get_authenticated_user_email(),
        "left_at": _now(),
        "migrated": len(done),
    }
    state_dict["history"].append(
        f"Data migration step left by "
        f"{state_dict['variables']['data_migration_review']['left_by']} "
        f"({len(done)} migrated, nothing outstanding)")
    state_dict["history"].append(
        f"Transitioned {STATE} -> {dest} via complete_data_migration")
    state_dict["current_state"] = dest
    try:
        context["bucket"].blob(STATE_BLOB).upload_from_string(
            json.dumps(state_dict, indent=2),
            content_type="application/json",
            if_generation_match=context["generation"])
    except exceptions.PreconditionFailed:
        return ("ERROR: the workspace state changed while this call was being "
                "written. Nothing was saved — call list_data_migrations() and "
                "try again.")
    except Exception as e:
        return f"ERROR: Could not save the workspace state: {e}"

    # After the write, and marked closed. This is the path every successful
    # migration takes, and the artifact it leaves at the terminal was still
    # offering `keep-in-aws` — a call that refuses there — under a heading
    # whose "these" named an empty list.
    runbook_warning = _refresh_runbook(context, closed=dm.CLOSED_COMPLETE)
    # The close is the biggest release of developer ship gates there is:
    # nothing is owed any more, so every component held on this step's data
    # can go. Published even though the step is over — see
    # publish_data_gate_from_ledger on why this one has no liveness guard.
    exports_warning = _publish_data_gate(context)

    # What the close just cost, in the response that reports it. "Nothing
    # outstanding" is true — the step gates on neither population here — and
    # on its own it reads as "nothing left to do", which is not the same
    # thing when a cutover is still running.
    frozen = dm.closed_over_in_flight(context["entries"], context["document"])
    if frozen:
        # The runbook pointer belongs HERE and not in the shared sentence:
        # this call has just written that document from this same section, so
        # it carries them. The other two callers cannot promise that.
        frozen += (f" {dm.RUNBOOK_BLOB} lists them under the moves in "
                   "progress, as the record of what the close left behind.")
    return (
        f"SUCCESS: data migration complete — {len(done)} service(s) moved, "
        "nothing outstanding.\n"
        + (frozen + "\n" if frozen else "")
        + (runbook_warning + "\n" if runbook_warning else "")
        + (exports_warning + "\n" if exports_warning else "")
        + f"Current State: {dest}.")


async def get_data_migration_runbook(address: str,
                                     directory: Optional[str] = None,
                                     identifier: Optional[str] = None,
                                     engine: Optional[str] = None,
                                     ctx: Context = None) -> str:
    """
    Returns the migration procedure for one data service, with what the
    ledger knows about it, to be adapted before the operator sees it.

    Read-only. The procedure comes back as a TEMPLATE carrying
    `<PLACEHOLDER>` names: substitute them from the facts below, from the
    landing zone, and from what the operator tells you, then save the result
    with `save_data_migration_runbook`. Do not hand the template over raw and
    do not run any of its commands.

    `engine` answers the one question this tool asks. An RDS whose Terraform
    builds the engine from a variable reaches here with none recorded, and the
    four RDS procedures differ too much to guess between — so the refusal says
    to ask the operator, and this argument is how their answer gets back in.
    It selects the procedure and nothing else: the inventory is the scan's to
    write, and a value a human typed is not a fact about the declaration.
    """
    logger.info("get_data_migration_runbook called (address=%r, engine=%r).",
                address, engine)
    context, error = _open("get_data_migration_runbook")
    if error:
        return error
    try:
        entry = _target(context, address, directory, identifier)
    except _Refused as refusal:
        return str(refusal)

    service = entry.get("service")
    supplied_engine = (engine or "").strip()
    declared = entry
    if supplied_engine and rb.needs_engine(entry):
        # A shadow copy for SELECTION only, and `declared` keeps the entry the
        # facts block is built from. Reading the facts off the shadow printed
        # the operator's answer under "what the ledger knows … never
        # contradict them", so a half-remembered "sql server" became an
        # established fact in the document that outlives the session.
        entry = dict(entry, engine=supplied_engine)
    elif supplied_engine and service != "rds":
        return (f"ERROR: `engine` selects among the RDS procedures, and "
                f"{dm.describe(entry)} is a {service} service — there is "
                "nothing for it to choose between. Re-call without it.")
    elif supplied_engine:
        declared = entry.get("engine")
        return ("ERROR: this entry already records its engine as "
                f"'{declared}', so `engine=\"{supplied_engine}\"` is either "
                "redundant or a disagreement — and the declaration is the "
                "thing the migration is planned against. If the operator says "
                "the running instance differs from what the Terraform "
                "declares, that is a discovery finding worth raising, not "
                "something to route around here. Re-call without `engine`.")
    name = rb.name_for(entry)
    if name is None:
        refusal = _no_runbook(
            entry, service, attempted_engine=supplied_engine,
            targeting=dm.targeting_args(
                entry, *dm.ambiguity(context["entries"])))
        listing = rendered_runbooks(context["bucket"])
        unread = listing is dm.LISTING_UNREAD
        existing = set() if unread else (listing or set())
        saved = next((p for p in rb.rendered_blobs(
            entry, dm._matching_records(context["document"], entry, context["entries"]))
            if p in existing),
            rb.rendered_blob(entry))
        if unread:
            # Not silence: the agent otherwise adapts a whole runbook with the
            # operator and `save`, which lists strictly, refuses at the end.
            refusal += (" Whether a runbook for this service already exists in "
                        "the ledger could not be read this time; save checks "
                        "again before writing.")
        if saved in existing:
            # There is no template, but somebody wrote a plan anyway — which
            # this refusal's own "that plan is theirs to write" invites. Not
            # naming it here sends the next session to write a second one.
            refusal += (f" A runbook for this service already exists at "
                        f"{saved}, written by hand in an earlier session — "
                        "read it rather than starting again.")
        return refusal
    try:
        template = rb.load(name)
    except OSError as e:
        # A packaging failure, not an estate condition. Named rather than
        # papered over with a generic procedure: the operator is entitled to
        # know the file is missing rather than to receive an improvisation.
        logger.error("Runbook %r could not be read: %r", name, e)
        return (f"ERROR: the runbook for {dm.describe(entry)} ({name}.md) "
                f"could not be read: {e}. This is a defect in the server "
                "installation. Do not improvise a procedure — say the runbook "
                "is unavailable.")

    record = dm.status_of(context["document"], entry, context["entries"]) or {}
    # Through the shared builder, like every other printed call in this step.
    # Built from `address` alone, this one refused after the whole adaptation
    # had been done with the operator — over a dev/prod pair, with the
    # instructions telling the agent to report the ERROR and stop, which loses
    # the document with the session.
    save_call = ("save_data_migration_runbook("
                 + dm.targeting_args(entry, *dm.ambiguity(context["entries"]))
                 + ", content=\"...\")")
    target, how = dm.target_and_how(entry)
    known = rb.facts(declared)
    consumers = dm.consumers_of(entry)
    grade = entry.get("disposition")
    head = [
        f"Runbook for {rb.offer(entry, target)} — {how}.",
        f"Source file: {name}.md. Adapt it; do not print it verbatim.",
        "",
        "WHAT THE LEDGER KNOWS about this service. Substitute these rather "
        "than asking for them, and never contradict them:",
        f"- address: {entry.get('address')}"
        # `directory_of`: an ARN-only entry has no declaring root module, and
        # naming `evidence[0]`'s directory here told the operator the resource
        # lives somewhere that does not declare it.
        + (f" (in {dm.directory_of(entry)})"
           if dm.directory_of(entry) else ""),
    ]
    if entry.get("identifier"):
        # Omitted rather than printed as None, for `facts()`'s reason: an
        # agent told to substitute these and never contradict them will write
        # "None" into the document.
        head.append(f"- name on the AWS side: {entry.get('identifier')}")
    head += [f"- {label}: {value}" for label, value in known]
    if supplied_engine:
        # Below the harvested facts and labelled, because it is not one. The
        # declaration states no engine; this is what a human said, and the
        # adapted runbook should not present it as anything more.
        head.append(f"- engine: {supplied_engine} — SUPPLIED BY THE OPERATOR "
                    "in this call, not recorded anywhere. The declaration "
                    "states none. If the procedure turns out not to fit the "
                    "database, this is the assumption to question first.")
    head += [
        "- used by: " + (", ".join(consumers) if consumers
                         else "nobody the scan could attribute "
                              "(unattributed is not unused)"),
        f"- graded: {grade}",
    ]
    if record.get("status"):
        head.append(f"- already reported: {record['status']}"
                    + (f" — {record['note']}" if record.get("note") else ""))
    # Only what the declaration STATED is above. Everything else the template
    # needs is the operator's, and inventing it is the failure mode this whole
    # step is shaped against — an endpoint or an instance name that looks
    # plausible is a command that runs against the wrong thing.
    listing = rendered_runbooks(context["bucket"])
    unread = listing is dm.LISTING_UNREAD
    existing = set() if unread else (listing or set())
    saved = next((p for p in rb.rendered_blobs(
        entry, dm._matching_records(context["document"], entry, context["entries"]))
        if p in existing),
        rb.rendered_blob(entry))
    if unread:
        # The same line `_procedure_line` prints for the artifact: "could not
        # tell" is not "none there", and silence here sends the agent to
        # adapt a runbook `save` will then refuse.
        head += [
            "",
            "WHETHER AN ADAPTED RUNBOOK ALREADY EXISTS in the ledger could not "
            "be read this time. Ask again before adapting from scratch; save "
            "checks strictly before writing.",
        ]
    if saved in existing:
        # The template is still returned — it is what an adaptation is made
        # from — but an agent that is not told about the existing document
        # rebuilds it from nothing and `save` then refuses, which is a worse
        # way to find out than being told here.
        head += [
            "",
            f"AN ADAPTED RUNBOOK ALREADY EXISTS at {saved}, written with the "
            "operator in an earlier session. Read it before adapting again: "
            "it carries the endpoint, window and target names this template "
            "does not, and saving over it needs replace=True.",
        ]
    head += [
        "",
        "WHAT IT DOES NOT KNOW: the source endpoint, the maintenance window, "
        "and the target names the applied Terraform created. Ask the operator "
        "for those, or leave the placeholder in place and say which ones are "
        "still open — never guess one.",
        "",
        "NEVER A CREDENTIAL VALUE. A password, key or token does not go into "
        "the adapted document under any circumstances: this file is saved to "
        "the ledger and rendered in the review UI, so writing one there makes "
        "it durable and readable, in a system whose contract is that it never "
        "holds customer credentials. The procedures reference credentials by "
        "file path or leave them to a `--prompt-for-password` flag; keep it "
        "that way, and if the operator volunteers a secret, do not write it "
        "down.",
        "",
        "<TARGET_ARGS> in the template is: "
        + dm.targeting_args(entry, *dm.ambiguity(context["entries"]))
        + " — substitute it verbatim wherever the template calls a tool, so "
        "the calls in the saved document are runnable by whoever opens it "
        "weeks from now.",
        "",
        "THEN: show the adapted procedure, and save it with "
        + save_call + ". It refuses while any `<PLACEHOLDER>` is left, so "
        "resolve them with the operator first.",
        "",
        "--- TEMPLATE BEGINS ---",
    ]
    if grade != dm.GATING_DISPOSITION:
        # The tool answers anyway: a human may be moving a service the
        # harvester graded `rebuild`, which is their call. What it must not do
        # is let the agent report progress against a gate this does not touch.
        head.insert(1, f"NOTE: this service is graded '{grade}', not "
                       f"'{dm.GATING_DISPOSITION}'. Moving it is a legitimate "
                       "choice, but nothing waits on it and it does not hold "
                       "the step open.")
    return "\n".join(head) + "\n" + template + "\n--- TEMPLATE ENDS ---"


def _no_runbook(entry: dict, service: str, attempted_engine: str = "",
                targeting: str = "") -> str:
    """The refusal, saying which kind of "no" this is.

    Three kinds, and they call for different things from the operator: an
    engine the declaration did not state (ask), a service whose answer is a
    decision rather than a procedure (escalate), and a service nobody has
    written one for (say so plainly). A single "not found" would send the
    agent looking for the nearest-looking file, which is exactly the failure
    the runbook set is shaped against.
    """
    if attempted_engine:
        # BEFORE the unsupported-engine branch. The shadow entry the caller
        # built puts the supplied value on `entry`, so `unsupported_engine`
        # answers for every mistyped answer and this arm was unreachable —
        # "psql" got a policy escalation instead of the four accepted values.
        return ("ERROR: no runbook for " + dm.describe(entry)
                + f": `{attempted_engine}` is not one of the engines Cloud "
                "SQL runs. The four with procedures are "
                + ", ".join(rb.ENGINE_CHOICES)
                + " (spelling is forgiving — \"SQL Server\" works). If the "
                "operator misspoke, re-call with the right one. If the "
                "instance really runs something else — Oracle, most often — "
                "then moving it means changing engine, which this repository "
                "escalates rather than plans; say so.")
    engine = rb.unsupported_engine(entry)
    if engine:
        return ("ERROR: there is no runbook for " + dm.describe(entry)
                + f": Cloud SQL has no `{engine}` engine. Moving it means "
                "changing engine — Oracle to PostgreSQL is the usual shape — "
                "which this repository escalates by policy rather than "
                "planning. Say so; do not adapt one of the other RDS "
                "procedures, whose replication mechanics do not carry across. "
                "If it is not going to move, "
                "annotate_data_dependency("
                + (targeting or f'address="{entry.get("address")}"')
                + ", disposition=\"keep-in-aws\") records that.")
    if service == "rds":
        return ("ERROR: no runbook for " + dm.describe(entry)
                + ": the declaration does not state an engine, and the "
                "procedure depends on it — PostgreSQL and MySQL replicate "
                "continuously through Database Migration Service, MariaDB "
                "has no continuous path at all, and SQL Server seeds from "
                "backups. Ask the operator which engine this instance runs "
                "and re-call passing it: "
                "get_data_migration_runbook("
                # Through the shared targeting text, like every other printed
                # call in this step. Built from the address alone, this one
                # refuses for the dev/prod pair — and the agent, forbidden
                # from constructing an address, has nowhere to go.
                + (targeting or f'address="{entry.get("address")}"')
                + ", engine=\"postgres\"). "
                "Do not pick the common one yourself, and do not re-call "
                "without the argument — nothing else records the engine, so "
                "the same refusal comes back.")
    reason = rb.NO_RUNBOOK_REASON.get(service)
    if reason:
        return ("ERROR: there is no runbook for " + dm.describe(entry)
                + f", by design. {reason} If the team has decided to move it "
                "anyway, that plan is theirs to write; record progress with "
                "mark_data_service_migrating and the outcome with "
                "mark_data_service_migrated. If it is not going to move, "
                "annotate_data_dependency("
                + (targeting or f'address="{entry.get("address")}"')
                + ", disposition=\"keep-in-aws\") records that.")
    return ("ERROR: no runbook has been written for service "
            f"'{service}' ({dm.describe(entry)}). Say so plainly rather than "
            "adapting a procedure for a different service — the commands do "
            "not carry across. The move is still reportable with "
            "mark_data_service_migrated when it happens.")


async def save_data_migration_runbook(address: str, content: str,
                                      directory: Optional[str] = None,
                                      identifier: Optional[str] = None,
                                      replace: bool = False,
                                      ctx: Context = None) -> str:
    """
    Writes an adapted migration runbook for one data service to the ledger.

    `content` is the procedure as the operator will run it — the template from
    `get_data_migration_runbook` with every `<PLACEHOLDER>` resolved. Refuses
    while one is left: a stand-in in a document somebody runs from is a
    command that fails at best and hits the wrong resource at worst.

    Refuses to overwrite an existing adapted runbook unless `replace=True`.
    The step is parked for weeks and the "ask once" in the instructions is per
    session, so the second session to arrive is offered the same help over a
    document the first one already wrote with the operator — the endpoint, the
    agreed window, the resolved target names. None of that is recoverable from
    a session that has ended.
    """
    logger.info("save_data_migration_runbook called (address=%r).", address)
    context, error = _open("save_data_migration_runbook")
    if error:
        return error
    try:
        entry = _target(context, address, directory, identifier)
    except _Refused as refusal:
        return str(refusal)

    text = (content or "").strip()
    if len(text) < MIN_RENDERED_RUNBOOK:
        # A short answer here is an agent summarising the template rather than
        # rendering it. The artifact is what an operator opens weeks later
        # with no session behind it, so a summary is not a smaller version of
        # the right thing — it is the wrong thing.
        return (f"ERROR: that is {len(text)} characters, which is too short "
                "to be an adapted runbook — the templates are procedures, not "
                "summaries. Send the whole adapted document, including the "
                "commands, the validation gates and the rollback.")
    left = rb.unresolved(text)
    if left:
        return ("ERROR: " + ", ".join(left) + " still "
                + ("is" if len(left) == 1 else "are")
                + " unresolved. Resolve each one with the operator — the "
                "ledger knows the AWS-side names, the applied Terraform knows "
                "the GCP-side ones — or drop the step that needs it and say "
                "why. Nothing was written.")

    path = rb.rendered_blob(entry)
    blob = context["bucket"].blob(path)
    try:
        blob.reload()
        generation = blob.generation
    except exceptions.NotFound:
        generation = None
    except Exception as e:
        return (f"ERROR: could not check whether {path} already exists ({e}), "
                "and overwriting an adapted runbook nobody has read would "
                "lose it. Nothing was written; try again.")
    try:
        # Strict: "could not list" must not read as "nothing there". The
        # current-name check below refuses on a failed `reload()`; this half
        # of the same guard refused on nothing and wrote around the adapted
        # copy in silence.
        existing_runbooks = rendered_runbooks(context["bucket"], strict=True)
    except Exception as e:
        return (f"ERROR: could not list the adapted runbooks in the ledger ({e}), "
                "and writing around one nobody has read would lose it. Nothing "
                "was written; try again.")
    earlier = rb.earlier_copies(
        entry, existing_runbooks,
        dm._matching_records(context["document"], entry, context["entries"]))
    if not replace:
        # The file the entry's EARLIER handle named: its address moved when
        # the declaration folded in (or the spelling composed), the adapted
        # runbook did not. Guarded like the current name, or the orphaned
        # copy — the one carrying the endpoint and the window — is written
        # around in silence.
        if earlier:
            return (f"ERROR: {earlier[0]} already holds an adapted runbook for "
                    f"{dm.describe(entry)}, written while the service was known "
                    "by its ARN alone; the handle moved when its declaration "
                    "folded in, the file did not. Read it first — it carries "
                    "what only the operator knew. If it is genuinely superseded, "
                    "re-call with replace=True, and say in this conversation "
                    "what changed. Nothing was written.")
    if generation is not None and not replace:
        return (f"ERROR: {path} already holds an adapted runbook for "
                f"{dm.describe(entry)}. Read it first — an earlier session "
                "wrote it with the operator, and it carries what only they "
                "knew: the source endpoint, the agreed window, the target "
                "names the applied Terraform created. If it is genuinely "
                "superseded, re-call with replace=True, and say in this "
                "conversation what changed. Nothing was written.")
    try:
        # Guarded, like every other durable write in this package. Without it
        # two sessions rendering at once both succeed and the later one wins
        # silently — the same race `_save_migrations` closes for the outcome
        # store.
        blob.upload_from_string(
            text, content_type="text/markdown; charset=utf-8",
            if_generation_match=generation if generation is not None else 0)
    except exceptions.PreconditionFailed:
        return (f"ERROR: another session wrote {path} while this runbook was "
                "being adapted. Nothing was written — read what is there now "
                "before deciding whether this version supersedes it.")
    except Exception as e:
        logger.warning("Could not write %s: %r", path, e)
        return (f"ERROR: the runbook could not be written to {path}: {e}. "
                "Nothing was recorded. The adapted procedure is still in this "
                "conversation — show it to the operator and try again.")
    # A replace supersedes the copies under the entry's earlier handles, or
    # the next scan that un-folds the entry reports the OLDER file as the
    # adapted one and the newer is unreachable. Removed here, and said
    # either way: a copy that could not be removed is two files, and the
    # operator has to know which one a later reader will be shown.
    superseded, left_behind = [], []
    if replace:
        superseded, left_behind = rb.supersede_earlier(
            earlier, lambda other: context["bucket"].blob(other).delete(),
            (exceptions.NotFound,))
        for other in left_behind:
            logger.warning("Could not remove superseded runbook %s", other)
    lifecycle = ""
    if superseded:
        lifecycle += ("\nSuperseded and removed the earlier copy at "
                      + ", ".join(superseded) + ".")
    if left_behind:
        lifecycle += ("\nCould not remove the earlier copy at "
                      + ", ".join(left_behind) + ": two files now exist, and a "
                      "later scan that knows this service by its ARN alone would "
                      "report the older one as the adapted runbook — remove it by "
                      "hand.")
    # The worklist names either the shipped template or this file, so saving
    # one changes what it says — the same invariant every reporting tool
    # keeps. Without this the artifact a later reader opens still sends them
    # to the phase document for a procedure somebody already tailored.
    runbook_warning = _refresh_runbook(context)
    if runbook_warning == CLOSED_UNDER_US:
        # The contract `CLOSED_UNDER_US` states: a caller that appends it has
        # to recognise it, because every forward-looking sentence is false
        # once it has fired. "Report it when it lands" is exactly that — from
        # the terminal, the call named below refuses.
        return (f"SUCCESS: the adapted runbook for {dm.describe(entry)} is at "
                f"{path} in the ledger.\n"
                "Another session closed the data migration step while this "
                "was being written, so the move can no longer be reported: "
                "mark_data_service_migrated does not run from the terminal. "
                "The procedure is still worth having — every command in it is "
                "the operator's to run — but it is now a record rather than "
                "something this workspace can settle.\n" + runbook_warning
                + lifecycle)
    return (f"SUCCESS: the adapted runbook for {dm.describe(entry)} is at "
            f"{path} in the ledger.\n"
            "It is a plan, not a report: the move is not recorded until "
            "somebody runs it and says so with "
            + dm.reporting_call(entry, *dm.ambiguity(context["entries"]))
            + ". Every command in it is the operator's to run."
            + (f"\n{runbook_warning}" if runbook_warning else "")
            + lifecycle)


def register(mcp):
    """Register this step's tools on the FastMCP server."""
    mcp.tool()(list_data_migrations)
    mcp.tool()(get_data_migration_runbook)
    mcp.tool()(save_data_migration_runbook)
    mcp.tool()(mark_data_service_migrated)
    mcp.tool()(mark_data_service_migrating)
    mcp.tool()(complete_data_migration)
