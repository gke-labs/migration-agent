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

"""MCP tools for discovery step 3b (data review): the human sign-off on the
workload→data-service mapping.

Owns the STATE_DISCOVERY_DATA_REVIEW agent task. The reviewer iterates —
`reject_data_consumer`, `attach_data_consumer`, `annotate_data_dependency`,
none of which transition — and then signs off with
`confirm_data_dependencies`, which raises the approval elicitation in the call
(the submit_assessment / confirm_workload_scope pattern: the user's answer IS
the decision).

Two questions are settled here, not one. Who uses each data service — the
mapping `consumers.py` derives and this step corrects — and what happens to it.
The second one matters because **not moving a data service is a supported
outcome**: a customer may keep a DynamoDB table, a bucket or an RDS instance in
AWS and have the GKE workload reach it across the cloud boundary. For an
AWS-only service that is frequently the sensible answer rather than a
concession, and the scan is structurally unable to propose it — it grades what
the Terraform declares and marks such a service `escalate`, which means "a
human decides". This step is where that human is, so it offers the option
rather than waiting to be asked, with the price attached: cross-cloud
connectivity, credentials for a pod that used to get them from IRSA, and egress
on every call. DESIGN.md issue 30 records that the decision is stored and not
yet consumed by the landing zone.

Why here, and not later. `run_discovery_extraction` carries
`data_dependencies` over untouched — it is scan-owned passthrough — so
whatever `consumers` holds when extraction starts is what the assessment
grades and what the workload data gate holds an application team on at its
ship gate. The deployment data migration step reads the DISPOSITION and renders the
consumers, but nothing recomputes the mapping itself, and extraction is the
expensive LLM step — so this is both the last chance to get it right and the
cheapest one.

Why the corrections do not live in the section they correct. The scan clears
`data_dependencies` on every run (rediscovery semantics), and
`amend_discovery_scope` makes a re-scan routine, so a correction written into
the section is erased by the next scan with nothing recording it was made.
They go to `platform/discovery/data_consumer_overrides.json` instead and are
replayed by the harvest — see discovery_init_1/overrides.py. The corrections
are durable; the approval is per-scan, so a re-scan asks again.
"""

import json
import logging
from datetime import datetime, timezone
from typing import Optional

from google.api_core import exceptions
from mcp.server.fastmcp import Context
from pydantic import BaseModel, Field

from servers.dag.state_management import (
    get_bucket_name,
    load_dag,
    load_inventory,
    load_inventory_schema,
    save_inventory,
    authorize_and_rehydrate,
)
import servers.dag.state_management as state_mgr
from servers.dag.dispatch import run_elicitation

from ..discovery_init_1 import datastores as datastores_lib
from ..discovery_init_1 import inferred as inferred_lib
from ..discovery_init_1 import overrides as overrides_lib
from . import candidates as candidates_lib

logger = logging.getLogger("migration-dag")

REVIEW_STATE = "STATE_DISCOVERY_DATA_REVIEW"
OVERRIDES_BLOB = "platform/discovery/data_consumer_overrides.json"
STATE_BLOB = "platform/onboarding/state.json"


def _services() -> list:
    """The service enum, from the inventory schema."""
    return list(load_inventory_schema()["properties"]["data_dependencies"]
                ["items"]["properties"]["service"]["enum"])


def _dispositions() -> list:
    """The dispositions the inventory schema allows, read from it."""
    return (load_inventory_schema()["properties"]["data_dependencies"]
            ["items"]["properties"]["disposition"]["enum"])


class DataMappingApprovalSchema(BaseModel):
    approved: bool = Field(
        description="Approve the workload-to-data-service mapping as it "
                    "stands? Approving starts extraction and fixes the "
                    "mapping the assessment grades and a later workload gate "
                    "would enforce. Declining leaves the review open — "
                    "correct it with reject_data_consumer / "
                    "attach_data_consumer / annotate_data_dependency and "
                    "submit again."
    )


# --- ledger helpers ---------------------------------------------------------


def load_overrides(bucket) -> tuple:
    """(document, generation). A missing object is an empty document, not an
    error: no review has recorded anything yet."""
    blob = bucket.blob(OVERRIDES_BLOB)
    try:
        blob.reload()
        text = blob.download_as_text()
    except exceptions.NotFound:
        return overrides_lib.empty_document(), 0
    try:
        document = json.loads(text)
    except ValueError:
        logger.warning("data_consumer_overrides.json is not valid JSON")
        return None, None
    if not isinstance(document, dict) or not isinstance(
            document.get("overrides", []), list) or not all(
                isinstance(record, dict)
                for record in document.get("overrides") or []):
        # Valid JSON of the wrong shape — a bare list, most plausibly. Hand
        # editing this object is anticipated (the refusal below tells an
        # operator to repair it, and so does DESIGN.md issue 29), and reaching
        # `.get`/`.setdefault` on a list raised an AttributeError out of every
        # tool in the step, which its own contract says can never happen.
        #
        # The ELEMENTS too, not only the container. `null` is what a partial
        # hand-delete leaves behind and a bare string is what a hand-written
        # placeholder leaves, and every consumer of this list calls `.get` on
        # its items. Refused rather than filtered: dropping a malformed record
        # silently would lose a human decision, which is the one thing this
        # module exists to prevent, and there is no way to tell a corrupted
        # record from a deliberate one.
        logger.warning("data_consumer_overrides.json is not a corrections "
                       "document")
        return None, None
    return document, blob.generation


# What the scan derived from the Terraform alone, kept so the review can
# rebuild the corrected section from the same inputs the next scan will use.
SCAN_BASELINE_BLOB = "platform/discovery/data_dependencies_scan.json"


def save_scan_baseline(bucket, baseline: dict) -> None:
    """Records the scan's own output, unconditionally (it is rewritten whole
    by every scan, like the section it describes)."""
    bucket.blob(SCAN_BASELINE_BLOB).upload_from_string(
        json.dumps(baseline, indent=2), content_type="application/json")


# Why `load_scan_baseline` came back empty. Both grades refuse — a damaged
# ledger does not finish the workflow — but they need different repairs, and
# the refusal has to name the right one: an UNREADABLE object is replaced from
# the copy the scan wrote, while an ABSENT one has to be restored (a previous
# generation, or an admin reconfigure that re-walks the platform sequence),
# because the scan writes it in the same call that writes the section and
# nothing else can regenerate it from the deployment segment.
BASELINE_ABSENT = "absent"
BASELINE_UNREADABLE = "unreadable"


def _freeze_over_the_section(bucket, document=None, taken=False) -> str:
    """What closing the step would freeze, across the whole live section.

    Re-read rather than taken from the caller: `_amend` mutates
    `review.inventory` in place and saves that same object, so the caller's
    copy IS the written section — what the extra round trip buys is a
    CONCURRENT writer's section, which is the one the close will actually see.

    Silent on a failed read, unlike the two arms above it, which exist to say
    out loud that the outcome store could not be read. The difference is what
    each sentence is doing: those arms answer the question the operator asked
    ("is this still owed?") and must not answer it wrongly, while this one is
    an unprompted caveat about a call they have not made. Withholding a caveat
    costs less than raising out of a tool whose durable work has already
    landed — `save_inventory` succeeded before this runs.

    `taken` switches to the past tense, for the closed-under-us arm: there the
    close has already happened, and the conditional sentence would read as a
    warning the operator still has time to act on. That arm has no outcome
    store in hand, so `document` is loaded here when it is not supplied.
    """
    from servers.phases.deployment import datamigration as dm
    from servers.phases.deployment.deployment_datamigration_2.tools import (
        load_migrations)
    try:
        inventory, _ = load_inventory(bucket)
        if document is None:
            document, _ = load_migrations(bucket)
    except Exception:
        return ""
    if inventory is None or document is None:
        return ""
    render = (dm.closed_over_in_flight if taken
              else dm.closing_freezes_in_flight)
    return render(inventory.get("data_dependencies") or [], document)


def _migration_step_tail(bucket, entry: dict, was_owed: bool, entries=None) -> str:
    """What this correction changed about what the deployment step waits on.

    The gate is keyed on TWO things — the grade AND the recorded outcome,
    because `datamigration.outstanding` drops a `migrate` entry that has been
    reported migrated. A sentence keyed on the grade alone told an operator
    the close was blocked on a service that was already settled, and offered
    `keep-in-aws` as the remedy: a durable mapping change, contradicting the
    sign-off stamp, for a problem that did not exist. The agent relays this
    verbatim, so it has to describe the gate the operator will actually meet.

    Four rounds of review have found a defect in this paragraph, each time
    because it was keyed more coarsely than the thing it describes. It is a
    function now so the cases can be enumerated and tested directly.
    """
    # Function-level: the deployment step imports this module, so a top-level
    # import here would close the cycle. Nothing runs at import time.
    from servers.phases.deployment import datamigration as dm
    from servers.phases.deployment.deployment_datamigration_2.tools import (
        load_migrations)

    graded_migrate = (entry.get("disposition") or "") == dm.GATING_DISPOSITION
    # `load_migrations` returns (None, None) for an object that exists and is
    # not a record set, and raises for a failed read. They need different
    # words: one is repaired, the other is retried — and calling a corrupt
    # object "could not be read just now" sends the operator to retry a
    # repair.
    try:
        document, _ = load_migrations(bucket)
        store_broken = document is None
    except Exception:
        document, store_broken = None, False
    # None means "could not tell", which is not the same as "not reported".
    reported = (None if document is None else
                (dm.status_of(document, entry, entries) or {}).get("status")
                == dm.MIGRATED)

    head = "Back to the data migration step: "
    if graded_migrate and reported:
        return head + ("this service is graded 'migrate' and has already been "
                       "reported migrated, so it is settled — it is not what "
                       "complete_data_migration() is waiting on.")
    if graded_migrate and reported is None and store_broken:
        return head + ("this service is graded 'migrate', so the step waits "
                       f"on it unless it has been reported migrated — and "
                       f"{dm.MIGRATIONS_BLOB} is not readable as a set of "
                       "migration records, so nothing here can say. Repair or "
                       "remove that object: complete_data_migration(), "
                       "list_data_migrations() and the two reporting tools "
                       "refuse until it is. This correction "
                       # Not "keep-in-aws": this arm requires the entry to be
                       # graded `migrate` AFTER the correction, so the only
                       # ways in are a note-only annotation and an upgrade TO
                       # `migrate`. Naming the operation the operator did not
                       # perform explains the durability of the wrong thing.
                       "was recorded regardless — the corrections store is a "
                       "different object and none of this touched it.")
    if graded_migrate and reported is None:
        return head + ("this service is graded 'migrate', so the step waits "
                       "on it unless it has already been reported migrated. "
                       "The outcome store could not be read just now — call "
                       "list_data_migrations() for what is actually "
                       "outstanding.")
    if graded_migrate and was_owed:
        return head + ("this service is still graded 'migrate', so it is "
                       "still owed a move and complete_data_migration() will "
                       "keep refusing until it is reported or excused.")
    if graded_migrate:
        return head + ("this service is NOW graded 'migrate', so it is owed a "
                       "move from here on and complete_data_migration() will "
                       "refuse until it is reported migrated or excused with "
                       "'keep-in-aws'.")
    if was_owed and store_broken:
        # The same distinction the `migrate` arms draw, because this arm makes
        # the same two claims from the same store: that the close will
        # proceed (it refuses on a corrupt store, in `_open`, before anything
        # else), and that no move is in flight (which an unreadable store
        # cannot say).
        # All four of `_open`'s callers, as an earlier fix did in the `migrate`
        # arm above but did not carry here. This is the arm a `keep-in-aws`
        # over a corrupt store actually reaches, and "cannot be told from
        # here" sends the operator straight to `list_data_migrations()` — into
        # an unannounced refusal, from the one message standing in front of it.
        return head + ("this service is no longer owed a move. But "
                       f"{dm.MIGRATIONS_BLOB} is not readable as a set of "
                       "migration records: repair or remove that object, "
                       "because complete_data_migration(), "
                       "list_data_migrations() and the two reporting tools all "
                       "refuse until it is. Whether a move was already "
                       "reported in progress against this service cannot be "
                       "told from here either. This correction was recorded "
                       "regardless — the corrections store is a different "
                       "object and none of this touched it.")
    if was_owed and document is None:
        return head + ("this service is no longer owed a move. The outcome "
                       "store could not be read just now, so whether a move "
                       "was already reported in progress against it cannot "
                       "be told — call list_data_migrations().")
    if was_owed:
        # A move already reported in flight does not stop because the service
        # stopped being owed: the AWS-side resource is live and somebody is
        # mid-cutover. Saying only "no longer owed" invites the operator to
        # forget it.
        in_flight = ((dm.status_of(document, entry, entries) or {}).get("status")
                     == dm.IN_PROGRESS)
        # Two different scopes, and the sentence needs both. Whether THIS
        # record survives the correction is about this entry. What the close
        # would freeze is about the whole population — and the ordinary
        # sequence separates them: the service being excused here is the one
        # that makes the close available, while the cutover that the close
        # would strand is running on a different service. Keyed on the entry,
        # this arm went silent in exactly that case, and `list_data_migrations`
        # one call later said the opposite.
        frozen = _freeze_over_the_section(bucket, document)
        return head + ("this service is no longer owed a move, so "
                       "complete_data_migration() will close once nothing "
                       "else is outstanding."
                       + (" A move was already reported IN PROGRESS against "
                          "it — that record is kept and list_data_migrations()"
                          " still shows it, because the cutover is real "
                          "whether or not anything waits on it."
                          if in_flight else "")
                       + (f" {frozen}" if frozen else ""))
    return head + (f"this service is graded '{entry.get('disposition')}', "
                   "which the data migration step does not wait on — nothing "
                   "about what is owed changed.")


def entries_a_rebuild_would_drop(baseline: dict, section: list) -> list:
    """The live entries the baseline does not account for.

    `overrides.rebuild` starts from the baseline's own entry list and adds
    only what an `add_entry` record creates — annotate, attach, reject,
    confirm and dismiss each act on an entry that is already there — so the
    rebuilt section's entries are the baseline's plus the hand-added ones,
    which the replay puts back from the corrections document and a baseline
    is therefore entitled not to hold (`overrides.added_by_hand`). A baseline
    that omits any OTHER entry the live section has does not CORRECT that
    section: it deletes the entry, silently, under the success message of
    whatever correction triggered the rebuild.

    A baseline is normally produced by the same scan that wrote the section,
    so this is empty. It is not empty when the object has been replaced by
    hand — which this step's own refusals tell an operator to do, and the
    minimal well-formed thing to type is `{"data_dependencies": []}`, which
    passes every other check here and drops every entry in the workspace.
    """
    scanned = {(e.get("address"), overrides_lib.entry_directory(e),
                e.get("identifier"))
               for e in (baseline or {}).get("data_dependencies") or []}
    return [e for e in section or []
            if not overrides_lib.added_by_hand(e)
            and (e.get("address"), overrides_lib.entry_directory(e),
                 e.get("identifier")) not in scanned]


def scan_baseline_state(bucket, section: list = None) -> str:
    """`"ok"`, `BASELINE_ABSENT` or `BASELINE_UNREADABLE`. Raises on transport
    failure, which is neither — the caller retries.

    `section` is the live `data_dependencies`, when the caller has it. A
    baseline that would drop entries from it grades UNREADABLE rather than
    ok: it is the wrong object for this section, and like every other corrupt
    ledger object here it is repaired by replacing it with a good copy. The
    grade has to match what `_amend` does with the same baseline, or the
    close reads "ok" while `keep-in-aws` refuses — the wedge an earlier fix closed.
    """
    blob = bucket.blob(SCAN_BASELINE_BLOB)
    try:
        blob.download_as_text()
    except exceptions.NotFound:
        return BASELINE_ABSENT
    # TRUTHINESS, not `is not None`, because that is what the consumer tests:
    # `_amend` refuses on `if not review.baseline`. A baseline of `{}` passes
    # the loader's shape check and is not None, so the two predicates
    # disagreed — `keep-in-aws` refused, the close refused because the state
    # read "ok", and the escape never fired. A permanent wedge, which is the
    # outcome the escape exists to prevent, reachable by an operator following
    # the "repair or replace" refusal with something minimal.
    baseline = load_scan_baseline(bucket)
    if not baseline:
        return BASELINE_UNREADABLE
    if section is not None and entries_a_rebuild_would_drop(baseline, section):
        return BASELINE_UNREADABLE
    return "ok"


def load_scan_baseline(bucket) -> Optional[dict]:
    """The scan's output, or None when the object is absent or unreadable.

    Callers that need to tell those apart — because one is repairable and the
    other is not — use `scan_baseline_state`.
    """
    blob = bucket.blob(SCAN_BASELINE_BLOB)
    try:
        baseline = json.loads(blob.download_as_text())
    except exceptions.NotFound:
        return None
    except ValueError:
        logger.warning("data_dependencies_scan.json is not valid JSON")
        return None
    if not isinstance(baseline, dict) or not isinstance(
            baseline.get("data_dependencies", []), list) or not all(
                isinstance(entry, dict)
                for entry in baseline.get("data_dependencies") or []):
        # The shape check `load_overrides` already has. `_amend` guards on
        # falsiness, which a non-empty list passes, and the next line calls
        # `.get` on it — an AttributeError out of a tool whose contract says
        # a failure comes back as an ERROR string.
        logger.warning("data_dependencies_scan.json is not a scan baseline")
        return None
    return baseline


def _save_overrides(bucket, document: dict, generation: int) -> None:
    bucket.blob(OVERRIDES_BLOB).upload_from_string(
        json.dumps(document, indent=2), content_type="application/json",
        if_generation_match=generation)


class _Review:
    """What every tool in this step needs, loaded once."""

    def __init__(self, state_dict, generation, config, bucket,
                 inventory, inv_generation, document, doc_generation,
                 baseline=None, baseline_error=None):
        self.state_dict = state_dict
        self.generation = generation
        self.config = config
        self.bucket = bucket
        self.inventory = inventory
        self.inv_generation = inv_generation
        self.document = document
        self.doc_generation = doc_generation
        # What the scan derived, before corrections. None when the object is
        # absent or unusable — which, since the scan writes it in the same
        # call that writes the section, means it was removed or damaged after
        # the fact rather than never produced. The refusals downstream say so.
        self.baseline = baseline
        # ...or None because the READ failed, which is a different thing. A
        # transport failure is retryable; absence is not, and the two lead an
        # operator to opposite actions. `complete_data_migration` splits them
        # deliberately; `_amend` could not, and told an operator whose read
        # had blipped that the object was gone.
        self.baseline_error = baseline_error

    @property
    def entries(self) -> list:
        return self.inventory.get("data_dependencies") or []

    @property
    def pool(self) -> list:
        return self.state_dict["variables"].get("data_dependency_workloads") or []




# Where the data mapping can still be corrected. The review is the obvious
# one. The data migration step is the other, and it is not a convenience: a
# service graded `migrate` holds a component up until it moves, and the moment
# an operator discovers a move needs an engine change or a downtime window
# nobody will approve is the moment they are standing in that step. Without a
# way to record `keep-in-aws` from there the component is blocked forever —
# the only other route back is `amend_discovery_scope`, which is reachable
# from the assessment and nothing later.
#
# Only the annotate tool is opened up. Rejecting and attaching consumers is
# settling the MAPPING, which the review signed off and extraction froze;
# re-opening that here would let the attribution move under a decision the
# reviewer already approved. Changing a disposition is a decision about the
# migration, not about what the Terraform says.
CORRECTABLE_STATES = (REVIEW_STATE, "STATE_DEPLOYMENT_DATA_MIGRATION")


def _open_review(tool_name: str, states=(REVIEW_STATE,)) -> tuple:
    """(review, error). Exactly one of the two is None."""
    try:
        state_dict, generation, config = authorize_and_rehydrate(None)
    except Exception as e:
        return None, f"ERROR: {e}"
    current = state_dict["current_state"]
    if current not in states:
        return None, f"ERROR: Invalid state for {tool_name}: {current}"
    bucket = state_mgr.gcs_client.bucket(get_bucket_name(config["ledger_uri"]))
    try:
        inventory, inv_generation = load_inventory(bucket)
    except Exception as e:
        return None, f"ERROR: Could not read the inventory: {e}"
    if inventory is None:
        # The fourth message on this path to need the distinction the other
        # three already draw. `scan_data_dependencies` runs at
        # STATE_DISCOVERY_DATA_SCAN, whose only in-edges come from scoping and
        # the assessment; from the deployment step the only reachable states
        # are itself and the terminal. Naming it there is an instruction the
        # operator cannot follow, and the step's own `_open` gets this right
        # by stating the fact and naming no repair.
        return None, (
            "ERROR: No inventory found"
            + ("; run scan_data_dependencies first."
               if current == REVIEW_STATE else
               " in this ledger, so there is no data service to correct. "
               "Nothing was changed."))
    try:
        document, doc_generation = load_overrides(bucket)
    except Exception as e:
        # Never fall back to an empty document on a read failure: the tools
        # write it back, and an empty one would erase every correction
        # recorded so far.
        return None, f"ERROR: Could not read {OVERRIDES_BLOB}: {e}"
    if document is None:
        return None, (
            f"ERROR: {OVERRIDES_BLOB} exists but is not valid JSON. It holds "
            "every correction earlier reviews recorded, and overwriting it "
            "would discard them silently — repair or remove the object before "
            "continuing.")
    try:
        baseline = load_scan_baseline(bucket)
    except Exception as e:
        # Not fatal here, deliberately. Only an amend needs the baseline, and
        # it refuses without one; a transient 503 on this object must not stop
        # a reviewer reading the section or signing it off, and must never
        # escape as an exception — the step's contract is that a failure comes
        # back as an ERROR string with the state left where it is.
        logger.warning(f"Could not read {SCAN_BASELINE_BLOB}: {e}")
        baseline, baseline_error = None, e
    else:
        baseline_error = None
    return _Review(state_dict, generation, config, bucket, inventory,
                   inv_generation, document, doc_generation, baseline,
                   baseline_error), None


# --- shared amend path ------------------------------------------------------


class _Refused(Exception):
    """An `enrich` hook rejecting the call, carrying the message to return."""


def overlaps_consumer(record: dict, candidate: dict) -> bool:
    """Does a standing correction speak about this pool candidate?

    The pool is keyed (workload, kind, namespace) and a rejection may name a
    kind, so this is the same wildcard rule `overrides.overlaps` applies to
    records — matching on the name alone hid a workload the reviewer never
    ruled out.
    """
    if record.get("workload") != candidate.get("workload"):
        return False
    kind = record.get("consumer_kind")
    return not kind or kind == candidate.get("kind")


def _record_directory(entry: dict, address: str = None):
    """The directory a correction against `entry` is keyed on: the declaring
    root module for a declared entry addressed by its block, None for one
    known from its ARN or addressed by its ARN.

    A referenced entry is matched repository-wide and its `evidence[0]` is
    whichever file the walk met first, so a directory taken from it changes
    when the same ARN is sighted in an earlier-sorting file. The ARN is
    unique on its own. Every place that builds a record or a match key for a
    correction goes through this, so the store and the guards agree.
    """
    if (entry.get("detection") == "referenced"
            or datastores_lib.is_literal_handle(entry.get("address"))):
        # A guess (`s3:name`) and a hand-added entry are literal handles as
        # much as an ARN is: a directory read off a guess's `evidence[0]`
        # moved when a later scan met the value in an earlier-sorting file,
        # and the dismissal recorded against it was "satisfied" while the
        # guess stood in the section again.
        return None
    if (address and address != entry.get("address")
            and datastores_lib.arn_spellings_agree(address, entry.get("arn"))):
        # A folded entry addressed by its ARN: the ARN stays unique after the
        # declaration leaves the repo again, and a directory recorded beside
        # it is what would then break the match.
        return None
    return overrides_lib.entry_directory(entry)


def _handles(entry: dict) -> list:
    """(address, directory) pairs a standing correction may be keyed on.

    An entry known from its ARN is keyed on the ARN with no directory. When
    a later scan folds it onto its declaration the entry's address becomes
    the block address, but the corrections recorded against the ARN still
    stand and still apply (`overrides.entries_at` matches `arn` too), so
    every guard that looks for a standing correction has to look under
    both handles.
    """
    handles = [(entry.get("address"), _record_directory(entry))]
    # The ARN and the endpoint alike: a queue seen by URL and by ARN keeps
    # the ARN as its handle and the URL in `endpoint`, and a correction made
    # while the URL was the handle has to keep applying.
    # NOT the `<service>:<identifier>` handle: dev's and prod's `orders` would
    # share it, and a decision on prod's block then retired one on dev's
    # through the alias channel. That bridge lives in `entries_at` only —
    # a record made under a guess's handle lands on the fact by name at
    # replay, and one made under the fact carries the fact's handles.
    for other in (entry.get("arn"), entry.get("endpoint")):
        if other and all(other != h for h, _d in handles):
            handles.append((other, None))
    return handles


def _alias_records(entry: dict, address: str) -> list:
    """The entry's other handles, with their directories, for a record made
    under `address` — so a later record under the other handle supersedes
    this one, and only this one: a block address is unique within a root
    module, so its directory travels with it."""
    return [{"address": h, "directory": d}
            for h, d in _handles(entry) if h and h != address]


def _describe(entry: dict) -> str:
    # The declaring root module is part of the name, not decoration: it is what
    # a reviewer passes as `directory` when two of them share an address, and
    # they cannot pass what the listing never showed them. Printed as the
    # literal value the tools match on — a root-level entry's directory is the
    # empty string, and rendering it as prose ("the repository root") produced
    # a label that reads well and matches nothing.
    if (entry.get("detection") == "referenced"
            or datastores_lib.is_literal_handle(entry.get("address"))):
        # No directory: the tools discard it for a literal-handle entry — an
        # ARN, an endpoint, a guess — so advertising one would offer a
        # disambiguator that matches nothing.
        return (f"{entry.get('service')} {entry.get('identifier')} "
                f"[{entry.get('address')}]")
    return (f"{entry.get('service')} {entry.get('identifier')} "
            f"[{entry.get('address') or 'no address'}"
            f" in directory='{overrides_lib.entry_directory(entry)}']")


# Dispositions that mean "nobody has decided yet". `escalate` is the harvester
# saying there is no clean Google Cloud equivalent, which reads as a problem to
# be solved unless someone points out that not solving it is allowed.
def _why_empty(entry: dict):
    """Why this entry has no consumer: "cut", "unknown", "none", or None.

    `note_unattributed` keeps three categories apart and writes one note per
    entry to say which — a reviewer cut the link, part of the Terraform could
    not be read so the answer is unknown, or the chains ran and found nothing.
    Every reader in this step used to ask only about the first, so the middle
    one was reported as the third: the candidate note told the reviewer the
    chains had missed an entry the scan never got to, the header counted it as
    "no consumer the scan could find", and the sign-off asked them to approve
    it as unattributed. That is the claim `TRUNCATED_NOTE` exists to withhold.
    """
    from ..discovery_init_1.consumers import REJECTED_NOTE, TRUNCATED_NOTE
    if entry.get("consumers"):
        return None
    notes = entry.get("notes") or []
    if REJECTED_NOTE in notes:
        return "cut"
    if TRUNCATED_NOTE in notes:
        return "unknown"
    if overrides_lib.added_by_hand(entry):
        # The fourth category: a reviewer created it and named no consumer.
        # The scan never looked, so "no chain reached it" is not a finding.
        return "added"
    return "none"


UNSETTLED = frozenset({"escalate", "undecided", None})

# The choice the scan is structurally unable to offer. It only grades what the
# Terraform declares; whether the customer WANTS a service moved is not in the
# files, and this review is the only place it is asked. A reviewer who is never
# told the option exists reads `escalate` as a migration to plan, and the
# engagement acquires a data project nobody asked for.
KEEP_IN_AWS_OPTION = (
    "The customer may also choose to leave this in AWS and have the GKE "
    "workload reach it across the cloud boundary — for an AWS-only service "
    "that is often the sensible answer rather than a concession. Offer it "
    "alongside migrating. It costs cross-cloud connectivity (VPN or "
    "Interconnect), AWS credentials for a pod that used to get them from IRSA, "
    "and egress on every call; say so. Record it with "
    "annotate_data_dependency(address, disposition='keep-in-aws', note=<their "
    "reason>). A kept service still appears here and still needs its "
    "connectivity, but it does not hold up an application team — only "
    "'migrate' does."
)


def _keep_in_aws_prompt(entry: dict) -> str:
    """The per-entry line. Deliberately one line.

    The full argument is printed once for the section instead of here. On the
    estates this step is for — where most services are `escalate` and
    unattributed — repeating a 600-character paragraph per entry buries the
    entries themselves in near-identical prose, in a response the agent is
    asked to relay.
    """
    if entry.get("disposition") == "escalate":
        return ("no clean Google Cloud equivalent, so the plan for "
                f"{entry.get('identifier')} is the customer's to make — "
                "including leaving it in AWS (see the end of this listing).")
    return (f"no plan recorded for {entry.get('identifier')} yet — leaving it "
            "in AWS is one of the answers (see the end of this listing).")


def _target(review: _Review, address: str, identifier: Optional[str],
            directory: Optional[str]) -> tuple:
    """([the one entry], error). Resolves the address a correction names.

    The list holds exactly one entry when there is no error — callers index
    it — because the alternative is applying one correction to two resources.

    Ambiguity is refused, never split across the matches. `envs/dev` and
    `envs/prod` can each declare `aws_sqs_queue.orders`, and applying a
    dev-only rejection to prod as well is exactly the wrong-team failure this
    step exists to prevent — silently, since the reviewer named one resource
    and corrected two.

    `identifier` is offered first because it is what a reviewer reads off the
    listing, but it is not always enough: the harvester falls back to the block
    address whenever the name is built from variables, so both entries can
    carry the same identifier too. `directory` — the root module each was
    declared in — always separates them, and the refusal names it.
    """
    matched = overrides_lib.entries_at(review.inventory, address, identifier,
                                       directory)
    if matched and not overrides_lib.entries_at(review.inventory, address,
                                                identifier, directory, exact=True):
        # Reached by SPELLING. Onto an entry the scan keeps apart as
        # ambiguous that is refused, as the replay and the deployment
        # resolver refuse it: the any-account spelling beside two foreign
        # ones may be either of them, which is why it was flagged, and a
        # correction copied from an earlier listing must not land there.
        flagged = [e for e in matched if datastores_lib.spelling_is_ambiguous(e)]
        if flagged:
            return None, (
                f"ERROR: '{address}' matches {len(flagged)} data dependenc"
                f"{'y' if len(flagged) == 1 else 'ies'} by spelling only, and the "
                "scan keeps them apart as ambiguous: "
                + ", ".join(sorted(e.get("address") or "" for e in flagged))
                + ". Call again with the exact address as the listing prints it.")
    if not matched:
        known = sorted({e.get("address") for e in review.entries
                        if e.get("address")})
        return None, (
            f"ERROR: No data dependency is declared at '{address}'"
            + (f" with identifier '{identifier}'" if identifier else "")
            # `is not None`, not truthiness: a root-level entry's directory is
            # the empty string, and the one call that needs telling why it
            # matched nothing is the one that passed it.
            + (f" in directory='{directory}'" if directory is not None else "")
            + ". Call "
            + ("list_data_migrations()"
               if review.state_dict["current_state"] != REVIEW_STATE
               # At the deployment step `list_data_dependencies` refuses, so
               # naming it hands the operator an instruction they cannot
               # follow at the one step whose argument is that the exit has to
               # be reachable.
               else "list_data_dependencies()")
            + " for the current section. "
            + (f"Recorded addresses: {', '.join(known)}." if known
               else "The section is empty."))
    if len(matched) > 1:
        # Two shapes reach here and they need different advice. Block
        # addresses repeat across root modules, and the directory separates
        # them. ARN handles do not: entries reached through the spelling
        # fallback have no declaring root module and share their identifier,
        # so offering `directory='iam'` twice is not a disambiguator but
        # noise — the same lesson the deployment twin already records. There
        # the answer is the exact spelling, which the listing prints.
        addresses = sorted({e.get("address") for e in matched
                            if e.get("address")})
        # With `address`: a declared entry reached through its ARN spelling
        # has no directory a correction could be keyed on either, and
        # without it a declared-plus-referenced over-match fell through to
        # the block-address refusal below and called two secrets "the same
        # block address declared in more than one root module".
        if datastores_lib.is_inferred_handle(address):
            # A guess handle answered by several entries of that name — a
            # declaration and a foreign spelling, dev and prod: neither
            # `directory=` nor `identifier=` is what separates them, the
            # exact address the listing prints is.
            return None, (
                f"ERROR: '{address}' names {len(matched)} data dependencies by name: "
                + "; ".join(_describe(e) for e in matched)
                + ". Call again with the exact address of the one you mean, as "
                + ("list_data_migrations()"
                   if review.state_dict["current_state"] != REVIEW_STATE
                   else "list_data_dependencies()") + " prints it.")
        if all(_record_directory(e, address) is None for e in matched):
            return None, (
                f"ERROR: '{address}' names {len(matched)} data dependencies. "
                "Each is known only from its own ARN, and this spelling "
                "matches more than one of them — the scan could not tell "
                "whether they are one resource or several, so it kept them "
                "apart. Neither directory= nor identifier= separates them. "
                "Call again with the exact address as "
                + ("list_data_migrations()"
                   if review.state_dict["current_state"] != REVIEW_STATE
                   else "list_data_dependencies()")
                + f" prints it: {', '.join(addresses)}")
        options = "; ".join(
            f"identifier='{e.get('identifier')}' "
            f"directory='{overrides_lib.entry_directory(e)}'" for e in matched)
        return None, (
            f"ERROR: '{address}' names {len(matched)} data dependencies — the "
            "same block address is declared in more than one root module, and "
            "those are different resources in different accounts. This "
            "correction is not applied to all of them: name the one you mean "
            f"and call again. Candidates: {options}")
    return matched, None


async def _amend(tool_name: str, address: str, identifier: Optional[str],
                 directory: Optional[str], record: dict, applied: str,
                 states=(REVIEW_STATE,),
                 enrich=None) -> str:
    """Records one correction, replays it onto the live section, saves both.

    The override is written FIRST. If the inventory write then fails the
    correction is still durable and the next scan applies it; the other order
    would show the reviewer a corrected section that the next scan silently
    discards.

    `enrich(review, record)` is for a tool that needs the loaded ledger to
    complete its record — it runs here so the whole step is one read.
    """
    review, error = _open_review(tool_name, states=states)
    if error:
        return error
    if review.baseline_error is not None:
        # Retryable, and the only branch that says so. `load_scan_baseline`
        # turns a genuine absence and a genuine corruption into None by
        # itself, so an exception here reports a transport failure — and
        # a retry is the answer, so saying the object is gone would
        # send the operator looking for a backup of something that is there.
        return (f"ERROR: {SCAN_BASELINE_BLOB} could not be read "
                f"({review.baseline_error}), and applying a correction needs "
                "it. Nothing was recorded and nothing was changed — try "
                "again. This is a failed read, not a missing object; do not "
                "treat the service as impossible to excuse.")
    if not review.baseline:
        # Without the scan's own output the section cannot be rebuilt, and the
        # only alternative — editing the corrected section in place — is the
        # computation this step stopped doing because it kept disagreeing with
        # the scan's. Refusing is recoverable; guessing is not.
        if review.state_dict["current_state"] != REVIEW_STATE:
            # At the deployment step `scan_data_dependencies` is unreachable —
            # it needs STATE_DISCOVERY_DATA_SCAN and nothing transitions back —
            # so the review's wording sends the operator to the one instruction
            # they cannot follow, standing exactly at the wedge it describes.
            #
            # Graded, not merged. This branch used to say "missing OR
            # unreadable" and offer both repairs as a disjunction, and the two
            # need different repairs: a corrupt object is replaced from the
            # copy the scan wrote, an absent one has to be restored or
            # regenerated. Nothing else here can tell them apart — the listing
            # never mentions the baseline, the object is not a review-UI
            # artifact, and the agent may not run a CLI to look — so if this
            # message does not say which, nothing does.
            try:
                grade = scan_baseline_state(review.bucket)
            except Exception:
                # The transient branch above already caught a failed read; a
                # failure here is the same thing arriving a moment later, and
                # "try again" is the answer to both.
                grade = None
            if grade == BASELINE_ABSENT:
                return (
                    f"ERROR: this ledger is damaged. {SCAN_BASELINE_BLOB} "
                    "does not exist, and the scan writes it in the same call "
                    "that writes the data services — so it has been removed. "
                    "'keep-in-aws' is replayed over that object, so this "
                    "service cannot be excused until it is restored.\n"
                    "The service can still be REPORTED if it does move, and "
                    "the step closes cleanly once everything owed has moved "
                    "and every image copy is confirmed or abandoned. "
                    "It will NOT close over the damage: "
                    "complete_data_migration() refuses too, naming the same "
                    "object.\nRestoring it means the object's previous "
                    "generation, if the ledger bucket has object "
                    "versioning enabled. "
                    "If it does not, the only mechanism this server offers is "
                    "an admin `join_ledger --reconfigure`: it resets the "
                    "platform walk to the start state, so the whole platform "
                    "sequence is walked again and the scan rewrites this "
                    "object. The ledger keeps what it has — no artifact or "
                    "recorded decision is discarded — but every step from "
                    "onboarding onward is redone.")
            if grade is None:
                return (
                    f"ERROR: {SCAN_BASELINE_BLOB} could not be read, and "
                    "'keep-in-aws' needs it. Nothing was recorded and nothing "
                    "was changed — try again. This is a failed read, not a "
                    "missing object; do not treat the service as impossible "
                    "to excuse.")
            return (
                f"ERROR: {SCAN_BASELINE_BLOB} exists but is not readable as a "
                "scan baseline, so 'keep-in-aws' cannot be recorded from "
                "here. Re-running the scan is not an option at this point in "
                "the graph.\nRepair or replace the object with the copy the "
                "scan wrote, then try again — that is the repair, and it "
                "keeps every exit working. complete_data_migration() will "
                "refuse too until it is, naming the same object. Reporting is "
                "unaffected: mark_data_service_migrated does not read this "
                "object.")
        return ("ERROR: The scan's baseline "
                f"({SCAN_BASELINE_BLOB}) is missing or unreadable, so a "
                "correction cannot be applied to the section. Re-run "
                "scan_data_dependencies to produce it; the corrections already "
                "recorded are replayed over that run and are not lost.")
    # A baseline can be well-formed and still not be the baseline for THIS
    # section. The rebuild below takes its entry list wholesale, so one that
    # omits a live entry deletes it — the mapping a human reviewed, gone under
    # a success message, and at the deployment step there is no re-scan and no
    # second sign-off to notice. Refused before the override is written: this
    # is the same repair the corrupt case gets, and the same argument, that
    # refusing is recoverable and guessing is not.
    dropped = entries_a_rebuild_would_drop(
        review.baseline, review.inventory.get("data_dependencies") or [])
    if dropped:
        detail = (f"ERROR: The scan's baseline ({SCAN_BASELINE_BLOB}) does not "
                  f"describe the current section: {len(dropped)} of "
                  f"{len(review.inventory.get('data_dependencies') or [])} "
                  "data service(s) are missing from it, including "
                  + ", ".join(str(e.get("address")) for e in dropped[:3])
                  + ("..." if len(dropped) > 3 else "")
                  + ". Applying a correction rebuilds the section from that "
                  "object, which would delete them. Nothing was recorded.\n")
        if review.state_dict["current_state"] != REVIEW_STATE:
            # One repair, and no escape hatch. Deleting the object used to
            # be worth naming, because absence reached a close that this one
            # did not; now both refuse, so suggesting a deletion would only
            # destroy the one copy of what the scan found.
            return (detail
                    + "Replace the baseline with the copy the scan wrote and "
                    "try again — that is the repair, and it keeps every exit "
                    "working. Until then this service cannot be excused, and "
                    "complete_data_migration() refuses too, naming the same "
                    "object. Do not delete it: the step will not close over a "
                    "damaged ledger either way, and the object is the only "
                    "record of what the scan found.\nReporting is unaffected "
                    "— mark_data_service_migrated does not read it — so a "
                    "service that does move can still be reported, and the "
                    "step closes cleanly once everything owed has moved — "
                    "and every image copy is confirmed or abandoned.")
        # Not "re-run the scan": that tool needs STATE_DISCOVERY_DATA_SCAN,
        # and nothing transitions from the review back to it — the state is
        # reached again only through amend_discovery_scope at the assessment.
        return (detail
                + "Replace the baseline with the copy the scan wrote and try "
                "again; the corrections already recorded are replayed over it "
                "and are not lost. Re-running the scan is not reachable from "
                "here — the scan state is reached again only through "
                "amend_discovery_scope at the assessment.")
    if enrich:
        try:
            record = enrich(review, record)
        except _Refused as refusal:
            return str(refusal)
    if record.get("kind") == overrides_lib.ADD:
        # An addition names no entry to resolve: it creates one, or lands on
        # the entry a scan derives for the same resource (overrides.apply).
        targets = []
    else:
        targets, error = _target(review, address, identifier, directory)
        if error:
            return error

    # Typed as a guess handle (`sqs:orders`) but resolved, through the name
    # bridge, onto an entry whose handle it is not: recorded under the
    # entry's OWN address and directory — a literal handle carries no
    # directory, so a record keyed on the typed one was refused by the
    # replay ("2 entries share it") and then landed on the other root
    # module's queue once this one left the scope. The typed handle stays on
    # the record as an alias, so guess-era records still meet it.
    typed = address
    if (targets and datastores_lib.is_inferred_handle(address)
            and address != targets[0].get("address")):
        if overrides_lib._placed_elsewhere(targets[0]):
            # The guess this handle names was answered by a spelling the
            # scan places in another account, region or partition, or cannot
            # place among several: that is not the resource the reviewer
            # means by the guess's name, and the replay refuses the same hop.
            # Naming the exact address is the way to mean THAT entry.
            return (f"ERROR: '{typed}' now names {_describe(targets[0])}, which the scan "
                    + overrides_lib._verdict_phrase([targets[0]]).replace("the scan ", "")
                    + ". If you mean that entry, call again with its exact address; "
                    "if you mean this estate's own resource, add_data_dependency it.")
        address = targets[0]["address"]
    record = dict(record, address=address)
    if targets:
        if any(datastores_lib.TWINS_NOTE_MARK in note
               for note in targets[0].get("notes") or []):
            # This entry is known from its ARN and did NOT fold, because two
            # declarations share its name. A correction against it is about
            # the ARN alone, and must not be retired by — or retire — one
            # recorded against either of those blocks.
            record["twinned"] = True
        if datastores_lib.spelling_is_ambiguous(targets[0]):
            # Several spellings of this name stand in the section because the
            # scan could not reconcile their accounts or regions. An
            # under-specified ARN agrees with every one of them, so the
            # spelling tolerance must not run across the group: it deleted a
            # customer's `keep-in-aws` on the us-east-1 queue for a decision
            # about the eu-west-1 one, and reported success. The same stamp
            # the outcome store reads, recorded here because the entry that
            # carried the note may be gone by the time this record is
            # replayed.
            record["ambiguous_spelling"] = True
        # The entry's other handles, so a later record made under the other
        # one supersedes this one rather than standing beside it. `_target`
        # returns a one-entry list or an error, so there is always exactly
        # one.
        aliases = _alias_records(targets[0], address)
        if typed != address:
            aliases.append({"address": typed, "directory": None})
        if aliases:
            record["aliases"] = aliases
        # The identifier goes on the record whenever the entry's BLOCK
        # address has siblings in its root module — under whichever handle
        # the reviewer used, so records made under the ARN and under the
        # block agree about it.
        block_address = targets[0].get("address")
        if block_address and not datastores_lib.is_literal_handle(block_address):
            siblings = overrides_lib.entries_at(
                review.inventory, block_address, None,
                overrides_lib.entry_directory(targets[0]))
            if len(siblings) > 1:
                record["identifier"] = targets[0].get("identifier")
        if _record_directory(targets[0], address) is None:
            record["directory"] = None
        else:
            # The declaring root module, always captured — `_target` has
            # resolved the correction to exactly one entry by now, so the
            # replay can aim at the same one however many blocks later share
            # its address.
            record["directory"] = overrides_lib.entry_directory(targets[0])
    else:
        # An addition: no entry yet, so no block and no directory.
        record["directory"] = ""
    record["author"] = state_mgr.get_authenticated_user_email()
    record["recorded_at"] = datetime.now(timezone.utc).isoformat()

    # Captured before the rebuild: the tail below distinguishes "stopped
    # being owed" from "was never owed", and only the pre-correction entry
    # knows which.
    # An addition has no pre-correction entry: nothing was owed before it.
    was_owed = bool(targets) and (targets[0].get("disposition") or "") == "migrate"
    # A dismissal of an addition that had landed on the scan's own guess must
    # stand: the rebuild puts the guess straight back as an open question.
    keep_dismissal = bool(
        targets and record.get("kind") == overrides_lib.DISMISS
        and overrides_lib.added_by_hand(targets[0])
        and any(not str(e).startswith(overrides_lib.ADDED_EVIDENCE_PREFIX)
                for e in targets[0].get("evidence") or []))
    superseded = list(overrides_lib.record_override(review.document, record,
                                                    keep_dismissal=keep_dismissal))
    if targets:
        # And the guess-era records this fact answers, which the store's own
        # supersession cannot see (the bridge is a lookup, not an alias).
        superseded += overrides_lib.retire_bridged(
            review.document, review.inventory, targets[0], record)
        if (record.get("kind") == overrides_lib.CONFIRM
                and targets[0].get("detection") == "inferred"):
            # The reviewer's yes covers the link the guess came with: the
            # holder whose configuration states the value. Recorded as
            # attachments now, while the guess stands, so they follow the
            # confirmation onto the fact a later scan states — the merge
            # drops the guess then, and with it the derived consumer.
            # One link per (workload, kind): two Deployments named `orders`
            # in two namespaces are one chain to the store, which would
            # otherwise retire the first attachment with the second and
            # report a decision the reviewer never made. A namespace is
            # recorded only when the holders agree on it.
            chains = {}
            for consumer in targets[0].get("consumers") or []:
                if consumer.get("workload"):
                    chains.setdefault((consumer["workload"], consumer.get("kind")),
                                      []).append(consumer)
            for (workload, consumer_kind), holders in chains.items():
                namespaces = {h.get("namespace") for h in holders}
                paths = {h.get("source_path") for h in holders}
                link = {"kind": overrides_lib.ATTACH, "address": address,
                        "directory": record.get("directory"),
                        "workload": workload,
                        "consumer_kind": consumer_kind,
                        "namespace": next(iter(namespaces)) if len(namespaces) == 1 else None,
                        "source_path": next(iter(paths)) if len(paths) == 1 else None,
                        "note": "confirmed with the guess: its configuration states the value",
                        "author": record.get("author"), "recorded_at": record.get("recorded_at")}
                if record.get("aliases"):
                    link["aliases"] = record["aliases"]
                superseded += overrides_lib.record_override(review.document, link)
    try:
        _save_overrides(review.bucket, review.document, review.doc_generation)
    except exceptions.PreconditionFailed:
        return ("ERROR: Concurrent update conflict writing the corrections "
                "object. Nothing was recorded; call the tool again.")
    except Exception as e:
        return f"ERROR: The correction could not be recorded: {e}"

    # Rebuilt from the scan's own output and the whole corrections document —
    # the same computation the next scan will run, rather than an incremental
    # edit of the corrected section that has to be argued into agreeing with
    # it. Four review rounds found four ways the two drifted apart; this makes
    # the question unaskable.
    written = overrides_lib.new_note_log()
    review.inventory["data_dependencies"], _ = overrides_lib.rebuild(
        review.baseline.get("data_dependencies") or [], review.document,
        review.baseline.get("truncated") or (),
        review.baseline.get("excluded") or (), written_out=written)
    # The scan's own notes count what the scan found. They do not rewrite
    # themselves when a human corrects the section under them, and they are the
    # copy extraction carries forward — so they are marked as superseded rather
    # than left to contradict the entry directly above them.
    overrides_lib.note_corrections_since_scan(
        review.inventory,
        overrides_lib.corrections_since_scan(
            review.document, review.baseline.get("corrections_replayed")))
    # Computed against the REBUILT entry, not the one `_target` resolved: that
    # one predates this correction and, on a reject-then-attach, predates the
    # rejection being retired too — so it showed no derived consumer and the
    # tool reported "attached by hand" while the section recorded a
    # confirmation. What the reviewer is told has to describe what was written.
    #
    # Matched the way `entries_at` matches, spellings included: a correction
    # placed through the ARN-spelling fallback carries an address no entry
    # carries, both equality tests fail, and `next` falls back to the stale
    # object — which is the very failure this lookup exists to avoid, arriving
    # by a different route.
    # Resolved with the SAME resolver the target was found with, against the
    # rebuilt section: `entries_at` knows every handle — the block address,
    # the ARN and its spellings (exact first, spellings only when nothing was
    # named exactly), the endpoint, and the `<service>:<identifier>` bridge —
    # and a private restatement here fell behind it twice: the ARN-spelling
    # fallback once, then the endpoint and the bridge, and the reply then
    # described the pre-rebuild object ("Still listed under that name") for a
    # consumer the rebuild had just removed.
    section = review.inventory["data_dependencies"]
    if targets:
        candidates = overrides_lib.entries_at(
            review.inventory, record["address"], None, record["directory"],
            exact=bool(record.get("ambiguous_spelling")))
        rebuilt = next((e for e in candidates
                        if e.get("identifier") == targets[0].get("identifier")),
                       candidates[0] if len(candidates) == 1 else targets[0])
    else:
        # An added entry: by its own handle, or the derived twin it landed on.
        rebuilt = (next((e for e in section if e.get("address") == record["address"]), None)
                   or overrides_lib.entry_twin(review.inventory, record.get("service"),
                                               record.get("identifier")))
    # The log too: what the correction DID is a fact the rebuild recorded,
    # not something to be read back out of the sentences it wrote.
    applied = applied(rebuilt, written) if callable(applied) else applied
    try:
        save_inventory(review.bucket, review.inventory, review.inv_generation)
    except exceptions.PreconditionFailed:
        # State-conditional, like the two refusals above it: at the deployment
        # step `list_data_dependencies` refuses, so naming it hands the
        # operator an instruction they cannot follow.
        return ("The correction is recorded and durable, but the inventory "
                "changed underneath this call and the section was not "
                "rewritten. Call "
                + ("list_data_migrations()"
                   if review.state_dict["current_state"] != REVIEW_STATE
                   else "list_data_dependencies()")
                + " to re-read it; the correction is replayed over every "
                "later scan regardless.")
    except Exception as e:
        return f"ERROR: The correction was recorded but the section write failed: {e}"

    # The deployment step's worklist is derived from this section, and
    # `keep-in-aws` recorded here is the only CORRECTION that changes what it
    # owes. Its artifact is the primary one at that state, and the step's
    # instructions tell the agent not to re-list, so without this a decision
    # the operator has just made would not appear there for weeks.
    runbook_warning, closed_under_us, exports_warning = "", False, ""
    if review.state_dict["current_state"] != REVIEW_STATE:
        from servers.phases.deployment.deployment_datamigration_2.tools import (
            CLOSED_UNDER_US, publish_data_gate_from_ledger,
            refresh_runbook_from_ledger)
        runbook_warning = refresh_runbook_from_ledger(review.bucket)
        closed_under_us = runbook_warning == CLOSED_UNDER_US
        # `keep-in-aws` taken HERE is the escape hatch that releases a
        # developer component held on an unmovable service, so the developer
        # side has to learn about it from the same call. Only on this branch:
        # a correction made at the review is followed by the extraction that
        # publishes the whole section anyway, and publishing mid-review would
        # cross a mapping the reviewer has not signed off yet into a
        # member-readable object.
        exports_warning = publish_data_gate_from_ledger(review.bucket)
    was_frozen = (_freeze_over_the_section(review.bucket, taken=True)
                  if closed_under_us else "")

    where = "; ".join(_describe(e) for e in (targets or ([rebuilt] if rebuilt else [])))
    # Named, not just counted: a reviewer who has just overwritten what they or
    # a colleague decided earlier should be able to see what it was without
    # reading the corrections object.
    replaced_lines = "".join(
        f"This replaced an earlier decision: {line}\n"
        for line in overrides_lib.summarize({"overrides": superseded}))
    return (
        f"{applied} — {where}.\n"
        + replaced_lines
        + (runbook_warning + "\n" if runbook_warning else "")
        + (exports_warning + "\n" if exports_warning else "")
        + f"Recorded in {OVERRIDES_BLOB}; it is replayed over every later "
        "scan, so a re-scan will not lose it.\n"
        # Where the correction was made decides which tail; what the
        # correction DID decides what the tail says. At the deployment step the
        # mapping is settled — reject/attach are sealed there and
        # `confirm_data_dependencies` refuses — so pointing at the sign-off
        # hands the operator a refusal. But keying the WORDING on the state
        # alone claimed every annotation excused the service, including a
        # note-only one that changed nothing and an upgrade to `migrate` that
        # made it owed for the first time. That sentence is the only thing the
        # agent sees, and it relays it.
        # Every forward-looking arm of the tail — "still owed", "will refuse
        # until", "no longer owed, so the close will proceed" — describes a
        # gate that the warning above has just said no longer exists.
        + ("The data migration step closed while this call was running, so "
           "nothing will ask about this service again from that step. The "
           "correction is recorded and replayed over every later scan "
           "regardless."
           # On the non-racing path the tail says both "that record is kept"
           # and what the close would freeze. Under the race both were
           # dropped — and `list_data_migrations()` refuses now, so the
           # operator cannot go and look for themselves.
           + (f" {was_frozen}" if was_frozen else "")
           if closed_under_us else
           _migration_step_tail(review.bucket, rebuilt or {}, was_owed,
                                (review.inventory or {}).get("data_dependencies"))
           if review.state_dict["current_state"] != REVIEW_STATE else
           "Continue correcting, or call confirm_data_dependencies() when the "
           "mapping is right — that raises the sign-off elicitation.")
    )


# --- tools ------------------------------------------------------------------


async def list_data_dependencies(unattributed_only: bool = False,
                                 ctx: Context = None) -> str:
    """Lists the recorded data services, who uses each, and what happens to it.

    Read-only; does not advance the DAG. Entries no reference chain reached
    carry a ranked list of candidate workloads from name similarity — always a
    guess, always with 'none of these' as an equal option. Entries with no
    settled plan carry the reminder that keeping the service in AWS, reached
    across the cloud boundary, is one of the answers available to the customer.

    Args:
        unattributed_only: show only the entries with no consumer, which are
            the ones the review has to act on.
    """
    logger.info(f"list_data_dependencies called (unattributed_only={unattributed_only}).")
    review, error = _open_review("list_data_dependencies")
    if error:
        return error

    entries = review.entries
    shown = [e for e in entries if not e.get("consumers")] if unattributed_only \
        else entries
    cut = sum(1 for e in entries if _why_empty(e) == "cut")
    unknown = sum(1 for e in entries if _why_empty(e) == "unknown")
    added = sum(1 for e in entries if _why_empty(e) == "added")
    empty = sum(1 for e in entries if not e.get("consumers"))
    # The same four-way split the scan notes make: an entry a reviewer emptied
    # is a decision, an entry the scan could not finish reading for is a
    # question it declined to answer, a hand-added entry is one the scan
    # never looked for a consumer of, and only the rest is a gap.
    out = [f"Data dependencies: {len(entries)} recorded, "
           f"{empty - cut - unknown - added} with no consumer the scan could find"
           + (f", {added} added by hand and awaiting a consumer" if added else "")
           + (f", {cut} whose link you rejected" if cut else "")
           + (f", {unknown} the scan could not answer for (part of the "
              "Terraform was unreadable)" if unknown else "")
           + (f" (showing {len(shown)})." if unattributed_only else ".")]
    guesses = [e for e in entries if e.get("detection") == "inferred"]
    if guesses:
        out.append(f"{len(guesses)} of them are GUESSES from a configuration "
                   "key's name and value (detection 'inferred'): each needs "
                   "confirm_data_dependency or dismiss_data_dependency before "
                   "the sign-off, and gates nothing until then.")
    hints = review.state_dict["variables"].get("data_dependency_hints") or []
    for entry in shown:
        out.append("")
        marker = ("GUESS — " if entry.get("detection") == "inferred"
                  # A confirmed guess is not an addition: a reviewer reading
                  # "ADDED BY HAND" over it would look for an `add` record
                  # that does not exist.
                  else ("ADDED BY HAND — " if overrides_lib.added_by_hand(entry)
                        else "CONFIRMED — ") if entry.get("detection")
                  == overrides_lib.HUMAN_DETECTION
                  else "REFERENCED, not declared here — "
                  if entry.get("detection") == "referenced" else "")
        out.append(f"- {marker}{_describe(entry)} — disposition "
                   f"{entry.get('disposition')}")
        for consumer in entry.get("consumers") or []:
            out.append(f"    used by {consumer.get('workload')} "
                       f"({consumer.get('kind')}, via "
                       f"{consumer.get('detection')}) — "
                       f"{consumer.get('evidence')}"
                       # A consumer carried from the replica, or attached by
                       # hand, says so here or the reviewer cannot tell it
                       # from a direct grant — nor where to withdraw it.
                       + (f" — {consumer['note']}" if consumer.get("note") else ""))
        for note in entry.get("notes") or []:
            out.append(f"    note: {note}")
        # Workloads whose own configuration states this entry's name exactly
        # (inferred.py). The strongest candidate there is, and still offered
        # rather than attached: it is a match on a name.
        for hint in hints:
            # Against every handle, not the address alone: the hint was made
            # before the merge, and the entry it matched may have folded onto
            # an ARN spelling or a declaration since.
            if hint.get("address") not in {h for h, _d in _handles(entry)}:
                continue
            listed = any(c.get("workload") == hint.get("workload")
                         and c.get("kind") == hint.get("kind")
                         for c in entry.get("consumers") or [])
            if listed:
                continue
            out.append(f"    likely consumer (not attached): {hint.get('workload')} "
                       f"({hint.get('kind')}"
                       + (f", {hint.get('source_path')}" if hint.get("source_path") else "")
                       + f") — {hint.get('reason')}. Attach it with "
                       "attach_data_consumer if the user confirms.")
        if not entry.get("consumers"):
            why_here = _why_empty(entry)
            # The pool minus anything this reviewer already rejected for this
            # entry. Re-proposing the exact link they decided against inverts
            # the argument the ranking rests on — name similarity is safe
            # BECAUSE a human decides — and the tool that consumes this is a
            # model working a candidate list across many rounds.
            standing = [o for o in review.document.get("overrides") or []
                        if o.get("kind") == overrides_lib.REJECT
                        and any(overrides_lib.overlaps(
                            o, {"address": handle, "directory": directory,
                                "identifier": entry.get("identifier"),
                                "workload": o.get("workload")})
                            for handle, directory in _handles(entry))]
            # Matched with `overlaps`, so a rejection of one CHAIN does not
            # hide the other: rejecting `orders (service_account)` says
            # nothing about `orders (helm_release)`, which is a different
            # Kubernetes object the reviewer may well want to attach.
            pool = [c for c in review.pool
                    if not any(overlaps_consumer(o, c) for o in standing)]
            dropped = len(review.pool) - len(pool)
            if pool:
                ranked, note = candidates_lib.rank(
                    entry, pool, chain_was_cut=why_here == "cut",
                    reading_incomplete=why_here == "unknown")
                out.append(f"    {note}")
                out.extend(candidates_lib.lines(ranked))
            elif review.pool:
                # `rank`'s empty-pool note says the scan saw no workload at
                # all, which would be false here and is contradicted by the
                # rejection note printed two lines above.
                out.append(f"    Candidates for {entry.get('identifier')}: "
                           f"every workload the scan saw ({dropped}) is one "
                           "you already rejected for this entry, so nothing "
                           "is offered. Attaching one would retire that "
                           "decision.")
            else:
                ranked, note = candidates_lib.rank(
                    entry, pool, chain_was_cut=why_here == "cut",
                    reading_incomplete=why_here == "unknown")
                out.append(f"    {note}")
            if dropped and pool:
                out.append(f"    ({dropped} candidate(s) left out of the "
                           "ranking: you rejected them for this entry. "
                           "Attaching one would retire that decision — say so "
                           "if you offer it.)")
        if entry.get("disposition") in UNSETTLED:
            out.append(f"    {_keep_in_aws_prompt(entry)}")

    standing = overrides_lib.summarize(review.document)
    if standing:
        out.append("")
        out.append(f"Corrections standing from this and earlier reviews "
                   f"({len(standing)}), replayed over every scan:")
        out.extend(f"- {line}" for line in standing)

    scan_notes = review.inventory.get("data_dependency_scan_notes") or []
    if scan_notes:
        out.append("")
        out.append("What the scan did not cover (relay verbatim):")
        out.extend(f"- {note}" for note in scan_notes)
    out.append("")
    # Stated for the whole section, not only the unsettled entries: the choice
    # is not limited to the ones the harvester could not place. "Migrate the
    # fleet but keep AWS RDS" is a valid engagement shape, and the assessment
    # knowledge doc already names it as one to surface rather than argue with.
    # Once, for the whole section — the entries above carry a one-line pointer
    # to it. Also states the part the per-entry line cannot: that the choice is
    # not confined to the entries the harvester could not place.
    out.append("Leaving a service in AWS: " + KEEP_IN_AWS_OPTION)
    out.append("That applies to any entry above, including one graded "
               "'migrate' — 'migrate the fleet but keep AWS RDS' is a valid "
               "shape. It is a decision, not a failure to migrate.")
    out.append("")
    out.append("Correct with reject_data_consumer / attach_data_consumer / "
               "annotate_data_dependency; answer each guess with "
               "confirm_data_dependency / dismiss_data_dependency; record a "
               "service nothing proposed with add_data_dependency; then call "
               "confirm_data_dependencies() to sign off.")
    return "\n".join(out)


async def reject_data_consumer(address: str, workload: str,
                               reason: Optional[str] = None,
                               kind: Optional[str] = None,
                               identifier: Optional[str] = None,
                               directory: Optional[str] = None,
                               ctx: Context = None) -> str:
    """Records that a derived consumer is wrong. Repeatable; no transition.

    The link came from a reference chain that resolved, so every later scan
    derives it again — this override is what keeps it out.

    Args:
        address: the entry's address as list_data_dependencies() prints it —
            a Terraform block address (module.orders_rds), or the ARN of an
            entry known only from a literal ARN.
        workload: the consumer to remove, by the name shown against the entry.
        reason: why it is wrong, in the user's words. Recorded on the entry —
            it is the only record of why the mapping disagrees with the code.
        kind: which consumer of that name, when the entry lists more than one
            (a helm_release and a service_account reaching the same database
            are two links, and rejecting one must not delete the other).
        identifier: only needed when one address names two entries.
        directory: the root module the entry was declared in (as
            list_data_dependencies() prints it). Needed when two root modules
            declare the same block and the identifier does not separate them
            either — the harvester falls back to the block address for a name
            built from variables, so both entries can read alike.
    """
    logger.info(f"reject_data_consumer called (address={address!r} workload={workload!r}).")

    def must_be_attached(review: _Review, record: dict) -> dict:
        """Refuses a workload the entry does not list and nobody has rejected.

        `_target` refuses an address nothing declares and prints the ones it
        does; the workload name is owed the same courtesy. Without it a typo —
        or the right workload named against the wrong entry — records a durable
        override that removes nothing, reports "Consumer 'x' rejected", and
        leaves the real consumer attached to gate that team.

        The standing-rejection exemption is not a special case, it is the
        common one. A reviewer sharpening their reason after talking to the
        team is rejecting a workload their own first rejection already took off
        the entry, and the replay takes it off again before every later review
        — so a bare "is it attached?" test refuses the reword in every state
        the tool can be called in, freezing the durable reason at its first
        wording. The guard is for typos; a workload the reviewer already ruled
        out is not one.
        """
        targets, error = _target(review, address, identifier, directory)
        if error:
            return record  # let _amend report the address problem itself
        listed = [c for c in targets[0].get("consumers") or []
                  if c.get("workload") == workload]
        if kind:
            listed = [c for c in listed if c.get("kind") == kind]
        # On the KINDS, not the count: `merge_datastores` can union two
        # structurally different consumers that share a (workload, kind), and
        # refusing on those printed the same kind twice as if it were a
        # choice — a refusal with no argument that resolves it.
        if len({c.get("kind") for c in listed}) > 1:
            # Two chains reached this entry under one workload name — a
            # helm_release and the service account of an IRSA role, say. They
            # are different Kubernetes objects, and a decision about the IAM
            # role is not a decision about the release, so the correction is
            # not applied to both. Same refusal `_target` makes one level up.
            # Distinct kinds, and the count of ANSWERABLE choices: two blocks
            # of the same kind and name are one choice, and offering
            # "helm_release, helm_release" is a refusal with no argument that
            # resolves it — the dead end removed from the condition
            # and left in the message.
            kinds = sorted({c.get("kind") for c in listed if c.get("kind")})
            raise _Refused(
                f"ERROR: '{workload}' names consumers of "
                f"{_describe(targets[0])} under {len(kinds)} different kinds, "
                "and those are different objects. Re-call with "
                "kind=<one of these>: " + ", ".join(kinds))
        if listed:
            return record
        # With the kind: this guard fires on a wrong KIND as readily as a
        # wrong name — checking the kind at all is what stops a rejection
        # naming a chain the entry never had from being accepted —
        # and a list of bare names then denies and asserts the same fact in
        # consecutive sentences. The reviewer's natural retry is to drop the
        # kind — which rejects every chain, including the one they did not
        # name. Every sibling refusal in this step names the axis.
        attached = [overrides_lib.consumer_label(c)
                    for c in targets[0].get("consumers") or []]
        # The exemption is for a REWORDING, so it has to match what a reword
        # is: the same decision, said again. Comparing the workload name alone
        # let a rejection naming a kind the entry never had ride in on an
        # unrelated one — reporting success, removing nothing, and writing a
        # durable, unwithdrawable sentence about a consumer the Terraform never
        # declared. That is what the typo guard exists to stop.
        mine = dict(record, address=address,
                    directory=_record_directory(targets[0]),
                    identifier=targets[0].get("identifier"),
                    consumer_kind=kind)
        mines = [dict(mine, address=handle, directory=directory)
                 for handle, directory in _handles(targets[0])]
        # An EXACT standing rejection — same entry, same stated kind. Matching
        # with the wildcard rule let a broad "reject orders" license a
        # narrower "reject orders (service_account)" on an entry that never
        # had one, which is the typo this guard exists to catch.
        if any(o.get("kind") == overrides_lib.REJECT
               and any(overrides_lib.overlaps(o, m) for m in mines)
               and (o.get("consumer_kind") or "") == (kind or "")
               for o in review.document.get("overrides") or []):
            return record
        # A narrowing of a rejection that already covers this link is refused,
        # not admitted. The guard would let it through as a reword, and then
        # `covers` — correctly — declines to supersede a broader decision with
        # a narrower one, so the call becomes a SECOND standing rejection and
        # the entry carries two reasons for one link, neither withdrawable
        # (issue 29). The reword they want is of the decision that is standing.
        if kind:
            broader = [o for o in review.document.get("overrides") or []
                       if o.get("kind") == overrides_lib.REJECT
                       and any(overrides_lib.covers(o, m)
                               and not overrides_lib.same_scope(o, m)
                               for m in mines)]
            if broader:
                raise _Refused(
                    f"ERROR: '{workload}' is already rejected on "
                    f"{_describe(targets[0])} by a correction that covers "
                    "every chain, so naming one would record a second reason "
                    "for the same link rather than sharpening the first. "
                    "Re-call without kind= to reword the standing decision"
                    + (f" ({broader[0].get('reason')})"
                       if broader[0].get("reason") else "") + ".")
        # ...or a consumer the scan itself derived before any correction ran.
        # That is the other legitimate reword: the standing rejection already
        # took it off the section, so "is it listed now" cannot see it.
        for scanned in (review.baseline or {}).get("data_dependencies") or []:
            if (scanned.get("identifier") != targets[0].get("identifier")
                    or not any(
                        (scanned.get("address"), _record_directory(scanned)) == h
                        or (scanned.get("arn") and scanned.get("arn") == h[0])
                        for h in _handles(targets[0]))):
                continue
            if any(c.get("workload") == workload
                   and (not kind or c.get("kind") == kind)
                   for c in scanned.get("consumers") or []):
                return record
        raise _Refused(
            f"ERROR: '{workload}' is not a consumer of "
            f"{_describe(targets[0])} and no correction has rejected it, so "
            "there is nothing to reject. "
            + (f"It lists: {', '.join(sorted(set(attached)))}."
               if attached else "It lists no consumers at all. If you meant to "
               "ADD one the user told you about, that is "
               "attach_data_consumer."))

    def what_happened(entry: dict, written) -> str:
        # The chain, and what is left of the name. "Consumer 'orders'
        # rejected" reads as "orders no longer uses this database", which is
        # the fact the workload data gate turns on — and it is false whenever
        # another chain of that name remains. The entry note, the store,
        # `summarize` and every refusal in this step already name the chain
        # everywhere else; only the sentence the agent relays did not.
        remaining = sorted(
            {overrides_lib.consumer_label(c)
             for c in entry.get("consumers") or []
             if c.get("workload") == workload})
        return (f"Consumer '{workload}'"
                + (f" ({kind})" if kind else "") + " rejected"
                + (". Still listed under that name: " + ", ".join(remaining)
                   if remaining else ""))

    return await _amend(
        "reject_data_consumer", address, identifier, directory,
        {"kind": overrides_lib.REJECT, "workload": workload, "reason": reason,
         "consumer_kind": kind},
        what_happened, enrich=must_be_attached)


async def attach_data_consumer(address: str, workload: str,
                               kind: Optional[str] = None,
                               namespace: Optional[str] = None,
                               source_path: Optional[str] = None,
                               note: Optional[str] = None,
                               identifier: Optional[str] = None,
                               directory: Optional[str] = None,
                               ctx: Context = None) -> str:
    """Attaches a consumer the scan could not reach. Repeatable; no transition.

    Only on a link the USER states. The ranking list_data_dependencies() shows
    is a guess to put to them, never grounds to attach on its own: this
    mapping decides which application team a later gate holds, and a wrong
    consumer stops the wrong team.

    Args:
        address: the entry's address as list_data_dependencies() prints
            it — a Terraform block address, or the ARN of an entry known only
            from a literal ARN.
        workload: the workload that uses it, as the user names it. When the
            scan saw a workload by that name, its kind, namespace and chart
            path are filled in from the scan.
        kind: what the consumer is (helm_release, service_account, ...) when
            the scan did not see it and the user knows.
        namespace: its Kubernetes namespace, when the user states one.
        source_path: repository-relative chart or source directory, when the
            user states one. This is the link to a developer's component scope.
        note: why they say it uses this — recorded with the consumer.
        identifier: only needed when one address names two entries.
        directory: the root module the entry was declared in (as
            list_data_dependencies() prints it). Needed when two root modules
            declare the same block and the identifier does not separate them
            either — the harvester falls back to the block address for a name
            built from variables, so both entries can read alike.
    """
    logger.info(f"attach_data_consumer called (address={address!r} workload={workload!r}).")

    workload = (workload or "").strip()
    if not workload:
        # The reject path guards its workload name; this one validates nothing,
        # and an agent relaying an empty or unparsed user answer — the
        # realistic failure, since the tool is driven by a model working
        # through a candidate list — would durably mark an unattributed service
        # as attributed. The entry then drops out of the unattributed count,
        # out of `unattributed_only`, and loses the note that is the only
        # signal anyone still has to place it, with no way to withdraw the
        # record (issue 29).
        return ("ERROR: A consumer needs a workload name. If the user did not "
                "name one, the entry stays unattributed — that is a legitimate "
                "outcome and the note on it is what keeps somebody looking.")

    def from_the_scan(review: _Review, record: dict) -> dict:
        """Fills in what the scan already knows about a workload of this name.

        The user should not have to retype a namespace the Terraform states,
        and a consumer whose kind disagrees with the scan's would read as a
        second, different workload downstream.

        The reviewer's own earlier correction wins over the scan, and that
        ordering is the whole subtlety. This runs before the record is stored,
        so filling a field from the scan here makes it look "supplied" to the
        field-merge in `record_override` — which then declines to carry the
        earlier value forward. A reviewer who set the namespace to `analytics`
        and later called again to add a chart path would silently have the
        namespace reverted to the scan's, durably and with the tool reporting
        success.
        """
        # Order matters: the effective kind comes from what the reviewer said
        # or what they said last time, and only then is the pool consulted —
        # with that kind. Consulting it first let a `helm_release`'s namespace
        # and chart path be copied onto an assertion about a service account.
        #
        # The pool never decides the record's IDENTITY, only fills in details.
        # `consumer_kind` is the scope the reviewer stated — unstated means
        # "that workload, whichever chain" — and letting the pool make it
        # specific turned a broad decision into a narrow one behind their
        # back: it stopped recognising the derived consumer of the other kind,
        # it made a later broad rejection fail to retire it, and it pulled a
        # namespace off an unrelated workload into a named human's assertion.
        # A hint is enough to LABEL the consumer that gets created.
        targets, error = _target(review, address, identifier, directory)
        standing = {}
        if not error:
            # `overlaps`, not a name comparison: two attaches of one workload
            # under different kinds are two assertions, and carrying the
            # first's namespace and chart path onto the second invents a
            # component scope the reviewer never stated — recorded as their
            # assertion, and read by a later workload gate.
            mine = dict(record, address=address,
                        directory=_record_directory(targets[0]),
                        identifier=targets[0].get("identifier"),
                        consumer_kind=kind)
            mines = [dict(mine, address=handle, directory=directory)
                     for handle, directory in _handles(targets[0])]
            # The same refusal on the DERIVED side. Two chains can reach one
            # entry under one workload name, and an unqualified confirmation
            # then lands on whichever comes first in the section — recording
            # the reviewer's reason against a Kubernetes object they did not
            # name, and naming the other one's chain back to them.
            # `reject_data_consumer` refuses this; the argument is the same.
            listed = {c.get("kind") for c in targets[0].get("consumers") or []
                      if c.get("workload") == workload}
            if not kind and len(listed) > 1:
                raise _Refused(
                    f"ERROR: '{workload}' names {len(listed)} consumers of "
                    f"{_describe(targets[0])}, and they are different objects. "
                    "Re-call with kind=<the one you mean>: "
                    + ", ".join(sorted(k for k in listed if k)))
            candidates = [o for o in review.document.get("overrides") or []
                          if o.get("kind") == overrides_lib.ATTACH
                          and any(overrides_lib.overlaps(o, m) for m in mines)]
            if len({o.get("consumer_kind") or "" for o in candidates}) > 1:
                # Two standing assertions about this workload under different
                # chains, and this call names neither. `next()` took the first
                # in document order, wrote the reviewer's namespace onto a
                # consumer they had not named, and then reported the other
                # one. `reject_data_consumer` refuses exactly this; so does
                # `_target` one level up.
                raise _Refused(
                    f"ERROR: you have already attached '{workload}' to "
                    f"{_describe(targets[0])} under "
                    + ", ".join(sorted(o.get("consumer_kind") or "(no kind)"
                                       for o in candidates))
                    + ". Re-call with kind=<the one you mean>; without it "
                    "this call would silently correct whichever was recorded "
                    "first.")
            standing = candidates[0] if candidates else {}

        # Now the pool, and only for the kind this record is actually about.
        effective = kind or standing.get("consumer_kind")
        matches = [c for c in review.pool
                   if c.get("workload") == workload
                   and (not effective or c.get("kind") == effective)]
        known = matches[0] if len(matches) == 1 else None

        def stated(field: str):
            """What the REVIEWER said — this call, else their standing record.

            Never the scan. `namespace` and `source_path` are read back by
            `_apply_one` as things a named human asserted: it fills them onto
            a derived consumer under a note saying the Terraform does not
            carry the value, and refuses them under a note quoting what the
            reviewer gave. Merging the scan's own output into those fields
            made both notes name a person for a value the scan produced —
            the same reason `consumer_kind` was split from
            `consumer_kind_hint`, applied to the two fields that never got
            the split.
            """
            return record.get(field) or standing.get(field)

        def hint(field: str, scanned_as: str):
            """The scan's LABEL for this workload: today's pool first.

            A hint describes what the walk just saw, and the pool is derived
            fresh every scan; the stored one exists only so a replay, which
            has no pool, can still label the consumer. Reading the stored one
            first made `consumer_kind_hint` sticky while the other two were
            re-derived, so a re-attach after the estate moved off Helm
            composed one consumer out of two different pool rows — the old
            row's `kind` beside the new row's `namespace`, naming a Kubernetes
            object that never existed. The rows are keyed together precisely
            because their fields belong together.
            """
            if known is not None:
                # Today's row is today's answer for every field, null
                # included. `or` could not tell "this scan says there is no
                # chart" from "this scan saw no row", so a registry chart
                # (`_chart_path` returns None) or an expression namespace
                # (`_namespace_of` returns None) fell through to an older
                # scan's row — one consumer composed of two of them.
                # Both fields are routinely null.
                return known.get(scanned_as)
            return record.get(field) or standing.get(field)

        resolved["consumer_kind"] = effective
        return dict(record,
                    # Identity: exactly what they said, wildcard when silent.
                    consumer_kind=effective,
                    # Label only, for the consumer this creates.
                    consumer_kind_hint=hint("consumer_kind_hint", "kind"),
                    # Assertions: only ever theirs.
                    namespace=stated("namespace"),
                    source_path=stated("source_path"),
                    # ...and the scan's guess at the same two, kept apart so
                    # `_consumer_from` can still label a hand-attached
                    # consumer without anyone being quoted. Stored, because a
                    # replay has no pool to re-derive them from.
                    # Stated wins over the label for these two: the
                    # reviewer's own value is not a guess to be refreshed.
                    namespace_hint=(stated("namespace")
                                    or hint("namespace_hint", "namespace")),
                    source_path_hint=(stated("source_path")
                                      or hint("source_path_hint",
                                              "source_path")))

    # What `from_the_scan` settled on, for the message to describe the record
    # that was actually written rather than the argument as passed.
    resolved = {}

    def what_happened(entry: dict, written) -> str:
        # The kind the RECORD ended up with, not the argument as passed: a
        # call that states none can still resolve to a specific chain through
        # a standing assertion, and describing the section by the argument
        # named whichever consumer happened to come first — reliably the one
        # this call did not touch.
        settled = resolved.get("consumer_kind") or kind

        def names_it(consumer: dict) -> bool:
            return (consumer.get("workload") == workload
                    and (not settled or consumer.get("kind") == settled))

        # Kind-scoped, the way `_apply_one` matches. Looking the derived
        # consumer up by name alone reported a confirmation while the section
        # recorded a second, hand-attached consumer of a different kind —
        # every clause of the message wrong, and a reviewer told "nothing else
        # changed" does not go back and check.
        derived = next((c for c in entry.get("consumers") or []
                        if names_it(c)
                        and c.get("detection") != overrides_lib.HUMAN_DETECTION),
                       None)
        mine = next((c for c in entry.get("consumers") or []
                     if names_it(c)
                     and c.get("detection") == overrides_lib.HUMAN_DETECTION),
                    None)
        if mine:
            # A consumer WAS written, whatever else the entry holds. Reporting
            # a confirmation here told the reviewer nothing else changed while
            # a second, hand-attached consumer went into the section.
            return (f"Consumer '{workload}' ({mine.get('kind')}) attached by "
                    "hand" + (", alongside the derived "
                              f"{derived.get('kind')} of the same name"
                              if derived else ""))
        if not derived:
            return f"Consumer '{workload}' attached by hand"
        # What the SECTION says was supplied, not what this call passed: a
        # value the Terraform already states is refused, and one supplied by
        # an earlier call is not news. An earlier round made the fill happen
        # and left
        # this sentence claiming nothing else changed.
        # From what the rebuild RECORDED doing, not from the notes it
        # wrote. `notes[]` also holds the reviewer's own prose — an annotation
        # opening by quoting the section's own sentence was read back as a
        # structured fact, and this message then told them they had supplied a
        # field they never passed and that a later scan would adopt it.
        # DESIGN.md issue 32: adding a note is fine, reading one back is not.
        # Only the fields THIS call passed. A rebuild re-derives `filled`
        # from the merged standing record every time, so the handle is present
        # on every later call about the link — and the sentence then told the
        # reviewer about something that did not happen in the call they just
        # made, which is the property this whole message is supposed to have.
        gave = {field for field, value in
                (("namespace", namespace), ("source_path", source_path))
                if value}
        did = written.handles(entry)
        supplied = [field for field in sorted(gave)
                    if ("supplied", field, workload,
                        derived.get("kind")) in did]
        refused = [field for field in sorted(gave)
                   if ("refused", field, workload,
                       derived.get("kind")) in did]
        also = []
        if note:
            also.append("your reason is now on the entry")
        if refused:
            # Not an aside: the value is kept in the store and a later scan
            # that finds none declared will adopt it, so a reviewer who is not
            # told here is not told at all.
            also.append(
                "the " + " and ".join(refused) + " you gave "
                + ("were" if len(refused) > 1 else "was")
                + " NOT applied — the Terraform declares "
                # Every block of the chain, not just the one resolved:
                # `merge_datastores` can union two consumers sharing a
                # (workload, kind), and quoting one of them back is how the
                # refusal came to depend on file order.
                + " and ".join(
                    f"{f}=" + " and ".join(sorted(
                        {c[f] for c in entry.get("consumers") or []
                         if c.get(f) and c.get("workload") == workload
                         and c.get("kind") == derived.get("kind")}))
                    for f in refused)
                + ", and a value with evidence behind it stands; yours is "
                "recorded on the entry and a later scan that finds none "
                "declared will use it")
        if supplied:
            also.append(
                "the " + " and ".join(supplied) + " you gave "
                + ("are" if len(supplied) > 1 else "is")
                + " now on the derived consumer, marked on the entry as your "
                "assertion rather than something "
                f"{derived.get('evidence')} states")
        return (f"Consumer '{workload}' was already derived from "
                f"{derived.get('evidence')} ({derived.get('detection')}), so it "
                "is recorded as confirmed rather than attached a second time"
                + (" and " + ", and ".join(also) if also
                   else " — nothing else changed"))

    return await _amend(
        "attach_data_consumer", address, identifier, directory,
        {"kind": overrides_lib.ATTACH, "workload": workload,
         "consumer_kind": kind, "namespace": namespace,
         "source_path": source_path, "note": note},
        what_happened, enrich=from_the_scan)


async def annotate_data_dependency(address: str, note: Optional[str] = None,
                                   disposition: Optional[str] = None,
                                   identifier: Optional[str] = None,
                                   directory: Optional[str] = None,
                                   ctx: Context = None) -> str:
    """Records a note, a disposition, or both. Repeatable; no transition.

    This is where a customer's decision to KEEP a data service in AWS is
    recorded (`disposition="keep-in-aws"`). The harvester never emits that
    value: it grades what the Terraform declares, and whether the customer
    wants a service moved at all is not in the files. Keeping a DynamoDB table
    or an S3 bucket where it is and reaching it from GKE across the cloud
    boundary is a legitimate outcome — for an AWS-only service, often the
    sensible one — and this tool is the only way the migration ever learns it.

    Args:
        address: the entry's address as list_data_dependencies() prints
            it — a Terraform block address, or the ARN of an entry known only
            from a literal ARN.
        note: what the user said about it. Team-facing: it stays on the entry
            and the assessment reads it. Record the reason with a keep-in-aws
            decision — later phases have to plan connectivity around it.
        disposition: migrate | rebuild | replatform | escalate | keep-in-aws |
            undecided. Use keep-in-aws when the customer says the service
            stays; it does not gate an application team the way migrate does.
        identifier: only needed when one address names two entries.
        directory: the root module the entry was declared in — as
            list_data_dependencies() prints it at the data review, or
            list_data_migrations() at the data migration step, this tool's
            other home. Needed when two root modules declare the same block
            and the identifier does not separate them either — the harvester
            falls back to the block address for a name built from variables,
            so both entries can read alike.
    """
    logger.info(f"annotate_data_dependency called (address={address!r}).")
    if not note and not disposition:
        return ("ERROR: Nothing to record — pass a note, a disposition, or "
                "both.")
    if disposition and disposition not in _dispositions():
        # Checked here rather than left to the inventory write: the override
        # is recorded first and is durable, so a rejected value would be
        # replayed into every later scan and fail its save instead of this
        # call.
        return (f"ERROR: '{disposition}' is not a disposition. One of: "
                + ", ".join(_dispositions()) + ".")

    def not_an_open_guess(review, record):
        # A guess is a question, and `undecided` is what keeps it out of the
        # gate; a disposition written onto it would gate (or excuse) a
        # resource nobody has said is real. The answer is confirm or dismiss.
        # A note alone is fine: it does not pretend to answer.
        if not disposition:
            return record
        targets, error = _target(review, address, identifier, directory)
        if error:
            raise _Refused(error)
        if targets[0].get("detection") == "inferred":
            raise _Refused(
                f"ERROR: {_describe(targets[0])} is a GUESS, not yet a data dependency, "
                "so it takes no disposition. Answer it first: "
                f"confirm_data_dependency(address='{address}', disposition='{disposition}') "
                "if it is real, or dismiss_data_dependency(address, reason) if it is not.")
        return record
    return await _amend(
        "annotate_data_dependency", address, identifier, directory,
        {"kind": overrides_lib.ANNOTATE, "note": note,
         "disposition": disposition},
        "Annotation recorded", states=CORRECTABLE_STATES, enrich=not_an_open_guess)


async def confirm_data_dependency(address: str, disposition: Optional[str] = None,
                                  note: Optional[str] = None,
                                  workload: Optional[str] = None,
                                  kind: Optional[str] = None,
                                  namespace: Optional[str] = None,
                                  source_path: Optional[str] = None,
                                  identifier: Optional[str] = None,
                                  directory: Optional[str] = None,
                                  ctx: Context = None) -> str:
    """Answers a guess with YES: the entry is a real data dependency.

    A guess (detection 'inferred') is a bare name a configuration key typed —
    INVOICE_BUCKET=acme-invoice-archive — recorded as a question. Confirming
    it makes it a 'human_review' entry with a plan: the disposition you give,
    or the table default for its service (a bucket migrates, a cache is
    rebuilt). The workload holding the key is already its consumer; name
    another with `workload` if the user says so. Repeatable; no transition.

    Args:
        address: the guess's address as list_data_dependencies() prints it
            (`s3:acme-invoice-archive`).
        disposition: migrate | rebuild | replatform | escalate | keep-in-aws |
            undecided. Omit to take the default for the service.
        note: what the user said — who owns it, why it is real.
        workload, kind, namespace, source_path: a further consumer to attach,
            recorded as a human assertion.
        identifier, directory: only when one address names two entries.
    """
    logger.info(f"confirm_data_dependency called (address={address!r}).")
    if disposition and disposition not in _dispositions():
        return (f"ERROR: '{disposition}' is not a disposition. One of: "
                + ", ".join(_dispositions()) + ".")
    if (kind or namespace or source_path) and not workload:
        return "ERROR: kind, namespace and source_path describe a workload — name it."
    out = await _amend(
        "confirm_data_dependency", address, identifier, directory,
        {"kind": overrides_lib.CONFIRM, "disposition": disposition, "note": note},
        "Confirmed as a real data dependency")
    if workload and not out.startswith("ERROR"):
        # The consumer is a LINK and goes through the attach tool proper — the
        # scan's knowledge of the workload (its kind, namespace, chart path),
        # the two-chains refusal, the link-level supersession — rather than
        # riding on the entry record unenriched.
        attached = await attach_data_consumer(
            address=address, workload=workload, kind=kind, namespace=namespace,
            source_path=source_path, identifier=identifier, directory=directory, ctx=ctx)
        out = _with_link_outcome(out, workload, attached, "confirmation")
    return out


_REPLY_TAIL_MARK = "Continue correcting"


def _with_link_outcome(out: str, workload: str, attached: str, what: str) -> str:
    """Two writes, one reply: say plainly whether the second one happened —
    the failure BEFORE the sign-off invitation that ends the first reply, and
    the second reply's own copy of that tail dropped."""
    body = attached.split(_REPLY_TAIL_MARK, 1)[0].rstrip()
    if attached.startswith("ERROR"):
        line = (f"The {what} stands; the consumer '{workload}' was NOT attached — fix the "
                f"call and attach it with attach_data_consumer:\n{body}\n")
    else:
        line = f"And its consumer:\n{body}\n"
    cut = out.rfind(_REPLY_TAIL_MARK)
    if cut < 0:
        return out + "\n" + line
    return out[:cut] + line + out[cut:]


async def dismiss_data_dependency(address: str, reason: str,
                                  identifier: Optional[str] = None,
                                  directory: Optional[str] = None,
                                  ctx: Context = None) -> str:
    """Answers a guess with NO: the value is not a data service this
    migration has to know about. The entry is removed and stays removed
    across re-scans. Repeatable; no transition.

    Only a guess ('inferred') or a hand-added entry ('human_review') can be
    dismissed. A declared or referenced entry is a fact the files state; to
    record that it is not this migration's concern, annotate it
    (annotate_data_dependency with a note, or disposition keep-in-aws).

    Args:
        address: the guess's address as list_data_dependencies() prints it.
        reason: why — required, it is the only record of the decision.
        identifier, directory: only when one address names two entries.
    """
    logger.info(f"dismiss_data_dependency called (address={address!r}).")
    if not reason or not reason.strip():
        return "ERROR: A reason is required; it is the only record of why."

    def only_guesses(review, record):
        targets, error = _target(review, address, identifier, directory)
        if error:
            raise _Refused(error)
        detection = targets[0].get("detection")
        if detection not in ("inferred", overrides_lib.HUMAN_DETECTION):
            raise _Refused(
                f"ERROR: {_describe(targets[0])} is {detection}: the scanned "
                "files state it, so it cannot be dismissed as a guess. If it is "
                "not this migration's concern, say so with "
                "annotate_data_dependency (a note, or disposition keep-in-aws); "
                "if a listed consumer is wrong, reject_data_consumer.")
        return record
    return await _amend(
        "dismiss_data_dependency", address, identifier, directory,
        {"kind": overrides_lib.DISMISS, "reason": reason},
        "Dismissed", enrich=only_guesses)


async def add_data_dependency(service: str, identifier: str,
                              disposition: Optional[str] = None,
                              note: Optional[str] = None,
                              arn: Optional[str] = None,
                              region: Optional[str] = None,
                              engine: Optional[str] = None,
                              workload: Optional[str] = None,
                              kind: Optional[str] = None,
                              namespace: Optional[str] = None,
                              source_path: Optional[str] = None,
                              ctx: Context = None) -> str:
    """Records a data service nothing in the scanned files proposed.

    For what the scan structurally cannot see: a database declared in
    CloudFormation, a bucket reached only through a library default, a
    service the user knows about and the files never name. Also the way to
    answer a scan note about a wildcard or variable-built ARN with the real
    name. The entry is 'human_review'; if a later scan derives the same
    resource, the note and disposition land on that entry instead.
    Not repeatable: the entry is recorded once, and annotate_data_dependency
    changes it. No transition.

    Args:
        service: rds | aurora | docdb | neptune | dynamodb | elasticache |
            memorydb | s3 | efs | fsx | kinesis | firehose | msk | mq | sqs |
            sns | eventbridge | opensearch | redshift | secretsmanager | ssm |
            other.
        identifier: the AWS name — bucket, table, queue, DB identifier.
        disposition: omit to take the default for the service.
        note: who owns it, how the user knows — the only evidence there is.
        arn, region, engine: when the user knows them.
        workload, kind, namespace, source_path: the workload that uses it.
    """
    logger.info(f"add_data_dependency called (service={service!r}, "
                f"identifier={identifier!r}).")
    services = _services()
    if service not in services:
        return f"ERROR: '{service}' is not a service. One of: {', '.join(services)}."
    identifier = (identifier or "").strip()
    if not identifier or any(ch.isspace() for ch in identifier):
        return "ERROR: identifier must be the resource's AWS name, without spaces."
    if disposition and disposition not in _dispositions():
        return (f"ERROR: '{disposition}' is not a disposition. One of: "
                + ", ".join(_dispositions()) + ".")
    if arn:
        parsed = [f for f in datastores_lib.find_arns(f'"{arn}"') if f.kind == "recorded"]
        if not parsed or parsed[0].token != arn:
            return (f"ERROR: '{arn}' is not a whole, literal ARN of a data store "
                    "this scan understands. Omit it, or give the exact ARN.")
        if parsed[0].identifier != identifier:
            return (f"ERROR: the ARN names '{parsed[0].identifier}', not "
                    f"'{identifier}'. Use the name the ARN states.")
        family = {"rds", "aurora", "docdb", "neptune"}
        if parsed[0].service != service and not (
                parsed[0].service in family and service in family):
            return (f"ERROR: the ARN names a {parsed[0].service} resource, not "
                    f"{service}. Use the service the ARN states.")
        region = region or parsed[0].region
    if (kind or namespace or source_path) and not workload:
        return "ERROR: kind, namespace and source_path describe a workload — name it."

    address = inferred_lib.inferred_address(service, identifier)

    def not_already_recorded(review, record):
        twins = overrides_lib.entry_twins(review.inventory, service, identifier)
        # As the replay lands an addition: not on an entry the verdicts place
        # in another account, region or partition — that is a same-named
        # resource elsewhere, and refusing the reviewer their own `orders`
        # because the finance account's is recorded sent them to annotate the
        # wrong queue. Set aside and said.
        aside = [t for t in twins if overrides_lib.definitely_elsewhere(t)]
        twins = [t for t in twins if t not in aside]
        if aside and not twins:
            record = dict(record, note=(record.get("note") or "") + (
                f" (recorded beside {', '.join(_describe(t) for t in aside)}, which the scan "
                "places in another account, region or partition)").strip())
        if len(twins) > 1:
            # Two same-named entries the scan keeps apart (two accounts it
            # could not reconcile): "nothing recorded" is the opposite of the
            # truth, and a third entry would gate beside them.
            raise _Refused(
                f"ERROR: the section already records this name {len(twins)} times "
                "and the scan keeps them apart — "
                + "; ".join(_describe(t) for t in twins)
                + ". Annotate the one you mean (annotate_data_dependency) rather "
                "than adding a third.")
        twin = twins[0] if twins else None
        if twin is not None and twin.get("detection") != "inferred":
            raise _Refused(
                f"ERROR: {_describe(twin)} already records this resource. "
                "Annotate it (annotate_data_dependency) or attach its consumer "
                "(attach_data_consumer) instead of adding a second entry.")
        if twin is not None:
            raise _Refused(
                f"ERROR: the scan already guessed this one — {_describe(twin)}. "
                f"Answer the guess: confirm_data_dependency(address="
                f"'{twin.get('address')}') or dismiss_data_dependency.")
        return record
    out = await _amend(
        "add_data_dependency", address, None, None,
        {"kind": overrides_lib.ADD, "service": service, "identifier": identifier,
         "disposition": disposition, "note": note, "arn": arn, "region": region,
         "engine": engine},
        "Added", enrich=not_already_recorded)
    if workload and not out.startswith("ERROR"):
        # As for a confirmation: the consumer goes through the attach tool,
        # against the entry the addition just created.
        attached = await attach_data_consumer(
            address=address, workload=workload, kind=kind, namespace=namespace,
            source_path=source_path, ctx=ctx)
        out = _with_link_outcome(out, workload, attached, "addition")
    return out


async def confirm_data_dependencies(ctx: Context = None) -> str:
    """Raises the mapping sign-off elicitation and acts on the user's answer.

    Call after putting the section to the user. The elicitation is the
    decision — do not ask in chat first and do not call again to act on the
    answer. Approve advances to extraction; decline leaves the review open and
    persists nothing.
    """
    logger.info("confirm_data_dependencies called.")
    review, error = _open_review("confirm_data_dependencies")
    if error:
        return error

    try:
        platform_dag = load_dag(review.bucket, "platform_dag.json")
    except Exception as e:
        return f"ERROR: {e}"
    state_def = platform_dag["states"][REVIEW_STATE]

    entries = review.entries
    unresolved = [e for e in entries if e.get("detection") == "inferred"]
    if unresolved:
        # A guess is a question, and a sign-off is an answer to a different
        # one. Approving over an open guess would carry `undecided` — the
        # mark of an unanswered question — into the section the assessment
        # grades and a later gate reads, with nothing saying anyone looked.
        return (
            f"ERROR: {len(unresolved)} guess(es) are still unanswered: "
            + ", ".join(f"{e.get('service')} {e.get('identifier')} "
                        f"(address '{e.get('address')}')" for e in unresolved[:5])
            + ("..." if len(unresolved) > 5 else "")
            + ". Each came from a configuration key's name and value and gates "
            "nothing until a human says whether it is real. Put them to the "
            "user and record the answers — confirm_data_dependency(address, "
            "disposition?, note?) or dismiss_data_dependency(address, reason) — "
            "then call confirm_data_dependencies() again. Nothing was recorded.")
    gating = [e for e in entries if e.get("disposition") == "migrate"]
    # Split, as the scan notes are. "It just cannot be placed with a team yet"
    # is exactly the sentence the scan notes no longer make for an entry
    # a reviewer emptied, and this is the one place a human actually answers a
    # question — and the stamp is the durable record of what they approved.
    withdrawn = [e for e in entries if _why_empty(e) == "cut"]
    # The third category. "It just cannot be placed with a team yet" is a
    # statement about the estate; for these the scan declined to make one,
    # and this is the sentence the human actually answers.
    unknown = [e for e in entries if _why_empty(e) == "unknown"]
    # And the fourth: a hand-added entry with no consumer named is not a
    # service "the scan could not attribute".
    added = [e for e in entries if _why_empty(e) == "added"]
    # By id, as the rest of this change does: `e not in withdrawn` is a
    # dict-equality scan, and two entries can compare equal.
    accounted = ({id(e) for e in withdrawn} | {id(e) for e in unknown}
                 | {id(e) for e in added})
    unattributed = [e for e in entries
                    if not e.get("consumers") and id(e) not in accounted]
    kept = [e for e in entries if e.get("disposition") == "keep-in-aws"]
    unsettled = [e for e in entries if e.get("disposition") in UNSETTLED]
    corrections = len((review.document or {}).get("overrides") or [])
    summary = (
        f"\n\nRecorded: {len(entries)} data service(s), {len(gating)} to "
        f"migrate, {len(unattributed)} still with no consumer attached"
        + (": " + ", ".join(e.get("identifier") for e in unattributed[:5])
           + ("..." if len(unattributed) > 5 else "") if unattributed else "")
        + f". {corrections} correction(s) recorded at review."
        # Not about the ones the customer chose to keep: they are not being
        # migrated, so "still needs migrating" contradicts a disposition this
        # very review recorded, and sending the human to find an owner for a
        # service that will not gate anybody wastes the ask. Same reason
        # this sentence is withheld from the other two no-consumer
        # categories.
        + (" An entry with no consumer stays in the inventory and still needs "
           "migrating — it just cannot be placed with a team yet."
           if [e for e in unattributed
               if e.get("disposition") != "keep-in-aws"] else "")
        # The fourth category, said in its own words: the scan never looked
        # for these consumers, so "no consumer attached" is the reviewer's
        # own gap, and a `migrate`-graded one holds nobody until filled.
        + (f" A further {len(added)} were added by hand with no consumer named yet: "
           + ", ".join(e.get("identifier") for e in added[:5])
           + ("..." if len(added) > 5 else "")
           + (" — of which " + ", ".join(e.get("identifier") for e in added
                                          if e.get("disposition") == "migrate")
              + " hold no component until one is attached"
              if [e for e in added if e.get("disposition") == "migrate"] else "")
           + "." if added else "")
        + (f" A further {len(unknown)} have no consumer that the scan could "
           "vouch for either way: part of the Terraform could not be read to "
           "the end, so whether a workload references them is unknown rather "
           "than answered — "
           + ", ".join(e.get("identifier") for e in unknown[:5])
           + ("..." if len(unknown) > 5 else "") + "." if unknown else "")
        + (f" A further {len(withdrawn)} have no consumer because you rejected "
           "the one the Terraform pointed at, which is a decision rather than "
           "a gap: " + ", ".join(e.get("identifier") for e in withdrawn[:5])
           + ("..." if len(withdrawn) > 5 else "") + "." if withdrawn else "")
        # Both halves of the plan are decision-driving at the gate, so both are
        # in the question rather than only in the agent's summary of it: what
        # the customer has chosen to keep, and what still has no plan at all.
        + (f" {len(kept)} service(s) stay in AWS by your decision ("
           + ", ".join(e.get("identifier") for e in kept[:5])
           + ("..." if len(kept) > 5 else "")
           + ") and need cross-cloud connectivity, credentials and egress "
             "budget rather than a migration." if kept else "")
        + (f" {len(unsettled)} have no settled plan (escalate/undecided): "
           + ", ".join(e.get("identifier") for e in unsettled[:5])
           + ("..." if len(unsettled) > 5 else "")
           + " — approving leaves them that way, which is a fair answer if the "
             "customer has not decided; leaving one in AWS is one of the "
             "decisions available." if unsettled else "")
    )
    # Escaped, like submit_assessment's: render_prompt runs the whole template
    # through str.format, so a brace in an identifier would either blank the
    # rendering (the workspace name shown raw) or, unbalanced, raise out of the
    # sign-off entirely.
    summary = summary.replace("{", "{{").replace("}", "}}")
    elicit_def = {**state_def,
                  "prompt_template": state_def.get("prompt_template", "") + summary}
    try:
        approved, _ = await run_elicitation(
            ctx, REVIEW_STATE, elicit_def, DataMappingApprovalSchema,
            review.state_dict, review.config)
    except Exception as e:
        return ("ERROR: Could not raise the approval elicitation (is this a "
                f"live MCP session?): {e}")

    state_dict = review.state_dict
    variables = state_dict["variables"]
    if not approved:
        dest = state_def["transitions"]["on_reject"]
        # Recorded even though the edge points back here and the state does not
        # move: "the mapping was put to them and they said no" is a different
        # history from silence, and it is the only trace a decline leaves —
        # nothing else about a declined review is persisted.
        state_dict["history"].append(
            "Data dependency mapping declined; the review stays open")
        state_dict["current_state"] = dest
        try:
            review.bucket.blob(STATE_BLOB).upload_from_string(
                json.dumps(state_dict, indent=2),
                content_type="application/json",
                if_generation_match=review.generation)
        except exceptions.PreconditionFailed:
            return ("Mapping not approved. The decline could not be written to "
                    "the history (concurrent update), but nothing else changed "
                    f"and the review is still open at {dest}.")
        return (
            f"Mapping not approved; nothing was persisted. Current State: {dest}.\n"
            "Correct it with reject_data_consumer / attach_data_consumer / "
            "annotate_data_dependency — the corrections already recorded are "
            "kept — and call confirm_data_dependencies again. Do not re-submit "
            "an unchanged mapping."
        )

    dest = state_def["transitions"]["on_approve"]
    # The approval asserts counts taken from the inventory, but it is written
    # to state.json — so state.json's generation, which is what guards the
    # write below, says nothing about whether the section still matches. The
    # elicitation is human-length, and a second platform session amending
    # during it would otherwise have this stamp commit against a section the
    # reviewer never saw, with no conflict raised. Re-read and refuse instead.
    try:
        _, current_generation = load_inventory(review.bucket)
    except Exception as e:
        return f"ERROR: Could not re-read the inventory to confirm: {e}"
    if current_generation != review.inv_generation:
        return (
            "ERROR: The data dependencies changed while the approval was being "
            "asked — another session corrected them, so this sign-off would "
            "cover a section you were not shown. Nothing was recorded. Call "
            "list_data_dependencies() to see the current one, then "
            "confirm_data_dependencies() again.")
    # Per-scan, not durable like the corrections: `data_dependencies` is
    # rebuilt from the checkout on every scan, so an approval carried across
    # one would be a sign-off on a section the reviewer never saw. The scan
    # clears this stamp for that reason.
    variables["data_dependency_review"] = {
        "approved_by": state_mgr.get_authenticated_user_email(),
        "approved_at": datetime.now(timezone.utc).isoformat(),
        "entries": len(entries),
        "unattributed": len(unattributed),
        # Hand-added entries with no consumer: the reviewer's own gap, kept
        # apart from what the scan could not find.
        "added_without_consumer": len(added),
        "rejected": len(withdrawn),
        # Recorded apart because it is not the same claim: these are entries
        # the scan declined to answer for, and a stamp saying "unattributed"
        # about them would be a sign-off on a finding nobody made.
        "unknown_unreadable_terraform": len(unknown),
        # What the customer chose not to move. Recorded on the stamp because it
        # is a commitment the landing zone has to plan around (connectivity and
        # credentials across the boundary), not just a row in the section.
        "kept_in_aws": len(kept),
        # Entries a human vouched for rather than the files: confirmed
        # guesses and hand-added services. On the stamp because a later gate
        # reading `human_review` should be able to see the reviewer knew.
        "vouched_by_hand": len([e for e in entries
                                if e.get("detection") == overrides_lib.HUMAN_DETECTION]),
        "corrections": corrections,
    }
    state_dict["history"].append(
        f"Data dependency mapping approved by "
        f"{variables['data_dependency_review']['approved_by']} "
        f"({len(entries)} entries, {len(unattributed)} unattributed, "
        + (f"{len(unknown)} unknown (unreadable Terraform), " if unknown else "")
        + f"{corrections} correction(s))")
    state_dict["history"].append(
        f"Transitioned {REVIEW_STATE} -> {dest} via confirm_data_dependencies")
    state_dict["current_state"] = dest
    try:
        review.bucket.blob(STATE_BLOB).upload_from_string(
            json.dumps(state_dict, indent=2), content_type="application/json",
            if_generation_match=review.generation)
    except exceptions.PreconditionFailed:
        return ("ERROR: Concurrent update conflict saving the migration state. "
                "The approval was not recorded; call confirm_data_dependencies "
                "again.")

    return (
        f"SUCCESS: Data dependency mapping approved — {len(entries)} data "
        f"service(s), {len(gating)} to migrate, "
        + (f"{len(kept)} staying in AWS, " if kept else "")
        + f"{len(unattributed)} with no consumer attached"
        # Not folded into the count above: the scan did not find these to have
        # no consumer, it could not finish reading and declined to say.
        + (f", {len(unknown)} the scan could not answer for (part of the "
           "Terraform was unreadable)" if unknown else "")
        + ".\n"
        + (f"Tell the user: the {len(kept)} service(s) kept in AWS need "
           "cross-cloud connectivity and credentials planned into the landing "
           "zone, plus an egress budget. The agent does not provision any of "
           "that today (DESIGN.md issue 30), so it is theirs to carry.\n"
           if kept else "")
        + f"Current State: {dest}. "
        # Named from the graph's own edge, not assumed.
        + ("Next: call run_discovery_extraction()."
           if dest == "STATE_DISCOVERY_RUNNING"
           else "Call get_next_stage for the current step's instructions.")
    )


def register(mcp):
    """Register this step's tools on the FastMCP server."""
    mcp.tool()(list_data_dependencies)
    mcp.tool()(reject_data_consumer)
    mcp.tool()(attach_data_consumer)
    mcp.tool()(annotate_data_dependency)
    mcp.tool()(confirm_data_dependency)
    mcp.tool()(dismiss_data_dependency)
    mcp.tool()(add_data_dependency)
    mcp.tool()(confirm_data_dependencies)
