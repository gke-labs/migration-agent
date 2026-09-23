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

"""What the migration still owes: data services that have to move and have not.

Pure — no I/O, no MCP, no ledger. The step package around it owns those.

WHERE THE RECORD LIVES, AND WHY NOT ON THE ENTRY. `images[].replication` puts
each image's outcome on its inventory entry, and the obvious move is to do the
same for `data_dependencies[]`. It does not work here. The data scan CLEARS
that section and rebuilds it from the checkout on every run, and
`amend_discovery_scope` makes a re-scan routine rather than exceptional — so a
completion fact written onto an entry would be destroyed by the next scan of a
checkout that had not even changed. Corrections survive that because the review
replays them; an operational outcome is not a correction to the mapping and
does not belong in the reviewer's store, whose count is what a human signs off.

So the outcomes live in their own ledger object, keyed by the identity axes the
corrections already use, and are joined onto the section by whatever needs
both. The section stays "what the Terraform declares plus what a human
corrected"; this file answers "and did it actually move".

ONLY `migrate` GATES. `keep-in-aws` is a decision the customer is entitled to
make, `rebuild` means an empty target is a working target, and `replatform`
means the shape changes rather than the data moving. `escalate` is the
harvester declining to have an opinion — it does not gate either, because
nobody has yet said the thing must move — and `undecided`, which the scan
assigns to a service it could not place at all, does not gate either, for the
same reason: nobody has said anything about it yet. `datastores.DISPOSITIONS` is
where those grades are assigned and argued.
"""

from servers.phases.discovery.discovery_init_1.overrides import entry_directory
from servers.phases.discovery.discovery_init_1.datastores import (
    AMBIGUOUS_ACCOUNT_NOTE_PREFIX, CROSS_ACCOUNT_NOTE_PREFIX,
    AMBIGUOUS_ENDPOINT_NOTE_PREFIX, CROSS_PARTITION_NOTE_PREFIX, CROSS_REGION_NOTE_PREFIX,
    REPLICA_NOTE_PREFIX, TWINS_NOTE_MARK, arn_spellings_agree, is_literal_handle,
    spelling_is_ambiguous)

# The ledger objects this phase owns.
MIGRATIONS_BLOB = "platform/deployment/data_migrations.json"
RUNBOOK_BLOB = "platform/deployment/data-migration-runbook.md"

SCHEMA_VERSION = 1

# Reported states. Absence of a record means "not started" — the honest
# default, and the one a fresh workspace is in.
IN_PROGRESS = "in_progress"
MIGRATED = "migrated"
STATUSES = (IN_PROGRESS, MIGRATED)

# The disposition that owes a move. See the module docstring.
GATING_DISPOSITION = "migrate"

# What the tool hands `runbook(rendered=…)` when the ledger's listing of
# adapted runbooks could not be read. Distinct from None ("no listing was
# taken", the callers' and tests' idiom) and from an empty set ("listed,
# none there"): the artifact must say "could not tell", not "not adapted".
# A frozenset, so `path in rendered` is simply False for every real path.
LISTING_UNREAD = frozenset({"<the ledger's runbook listing could not be read>"})


def empty_document() -> dict:
    return {"schema_version": SCHEMA_VERSION, "migrations": []}


def key_of(entry: dict) -> tuple:
    """The identity of a data service, as the corrections store spells it.

    `address` leads because it is the stable part; `directory` separates two
    root modules declaring one address (an `envs/dev` and an `envs/prod` that
    both say `aws_db_instance.orders`); `identifier` separates two entries a
    single directory declares at one address, which an `_override.tf` that
    renames produces. Fewer fields than this is the defect class that ran
    through the review step's whole history — a comparison keyed more coarsely
    than the thing it identifies.
    """
    return (entry.get("address"), directory_of(entry),
            entry.get("identifier"))


def directory_of(entry: dict) -> str:
    """The directory this store keys an entry on: its declaring root module,
    and none at all for an entry known only from its ARN.

    A referenced entry's `evidence[0]` is whichever file the walk met first,
    so a directory taken from it moves when the same ARN turns up in an
    earlier-sorting file — and then a recorded `migrated` stops being found,
    the gate counts the service as still owing a move, and every component
    consuming it is refused at its ship gate for data that has landed. The
    ARN is unique on its own and needs no directory. The corrections store
    settled this the same way (`_record_directory`); this store was written
    before referenced entries existed and did not know to.
    """
    # Any literal handle, not only an ARN: a guess (`s3:name`), a hand-added
    # entry and an endpoint-addressed one are unique on their own too, and a
    # directory read off a guess's `evidence[0]` moved when a later scan met
    # the value in an earlier-sorting file — the key moved with it.
    if entry.get("detection") == "referenced" or is_literal_handle(entry.get("address")):
        return ""
    return entry_directory(entry)


def key_of_record(record: dict) -> tuple:
    return (record.get("address"), record.get("directory"),
            record.get("identifier"))


def handles_of(entry: dict) -> list:
    """Every address this entry can have been recorded under: the one it
    carries now, and its ARN when the two differ.

    A referenced entry folds onto its declaration the moment a later scan
    finds one, and its `address` becomes the block address while the ARN
    moves to `arn`. Without the second handle the outcome recorded before the
    fold is unreachable, the service is owed again, and the ship gate holds
    every component consuming data that has already landed. The corrections
    store bridges the same gap with `_handles`.
    """
    handles = [entry.get("address")]
    # The endpoint too: a queue known from its URL in a ConfigMap takes the
    # ARN as its handle once a policy names it, and the URL survives only in
    # `endpoint` — an outcome reported under the URL is otherwise lost.
    for other in (entry.get("arn"), entry.get("endpoint")):
        if other and other not in handles:
            handles.append(other)
    return [h for h in handles if h]


def record_handles(record: dict) -> list:
    """Every address this record can be matched under: the one it was made
    with, and the entry's other handle at the time (`arn`).

    Stored because the bridge has to work in BOTH directions. `handles_of`
    reaches a folded entry from a record made under its ARN; nothing reached
    an ARN-addressed entry from a record made under a block address, which is
    the ordinary unfold — a scope amendment takes the declaring file out and
    the entry drops back to being known only from its ARN. The outcome then
    read as never reported, the gate held every consuming component, and the
    record showed up under "no entry in this scan". The corrections store
    solves it with `aliases` and says "and vice versa"; this is that field.
    """
    handles = [record.get("address")]
    handles += [a for a in record.get("aliases") or [] if isinstance(a, str)]
    return [h for h in handles if h]


def twinned(entry: dict) -> bool:
    """A referenced entry kept apart because two declarations share its name:
    the scan said so in a note, and it is the only place the fact lives."""
    return any(TWINS_NOTE_MARK in n for n in entry.get("notes") or [])


def placed_elsewhere(entry: dict) -> bool:
    """A referenced entry the scan's verdicts put in another account, region
    or partition than the estate's, among several it cannot choose from, or
    answered as the estate's own REPLICA — every entry `datastores._foreign`
    keeps apart from a declaration, read from the same notes. A replica is
    not "elsewhere", but it is not the declared resource a block record was
    about either, and a record on the primary must not settle it."""
    return entry.get("detection") == "referenced" and any(
        n.startswith((CROSS_ACCOUNT_NOTE_PREFIX, AMBIGUOUS_ACCOUNT_NOTE_PREFIX,
                      CROSS_REGION_NOTE_PREFIX, CROSS_PARTITION_NOTE_PREFIX,
                      REPLICA_NOTE_PREFIX, AMBIGUOUS_ENDPOINT_NOTE_PREFIX))
        for n in entry.get("notes") or [])


def record_matches(record: dict, entry: dict, entries=None) -> bool:
    """Does this record name this entry?

    Exact key first. Failing that, an ARN handle on either side is matched on
    the ARN alone, allowing two SPELLINGS of one ARN: the ARN is unique
    without a directory, and which spelling an entry carries changes when the
    set of sightings does. Every lookup in this module goes through here, so
    the runbook's counts, the gate's verdict and what `record_status`
    replaces cannot disagree — they disagreed in four different ways while
    three of them used exact tuples and one did not.
    """
    reach = _reaches(record, entry)
    if reach == "exact":
        return True
    if reach is None:
        return False
    if entries is not None:
        # The section-wide half of the same rule, which `overrides.apply`
        # has ("2 entries share it and the correction does not say which")
        # and this predicate could not, seeing one entry at a time: a
        # record made under a WILDCARD spelling while the entry stood alone
        # agrees with every literal spelling the next scan splits out —
        # `us-east-1` and `eu-west-1` disagree with each other, so neither is
        # flagged, and the record carries no stamp — and one `migrated` then
        # settled both, the cross-region one included, and opened the gate.
        # A spelling that reaches more than one entry in the section
        # reaches none; the record is listed as stale with the reason.
        # Counted with the SAME per-entry refusals as above: an entry the
        # record could never settle — a foreign spelling, for a block
        # record — does not make the estate's own spelling unreachable, or
        # the un-fold an earlier fix protected was lost the moment a cross-account
        # sighting of the name arrived.
        if sum(1 for e in entries if _reaches(record, e)) > 1:
            return False
    return True


def _reaches(record: dict, entry: dict):
    """How this record names this entry: "exact" (key or handle), "spelling"
    (an ARN spelling the tolerance accepts), or None. The per-entry rules
    live here so `record_matches` can count the section with them."""
    if key_of_record(record) == key_of(entry):
        return "exact"
    block_record = not is_literal_handle(record.get("address"))
    if twinned(entry) and not record.get("twinned") and block_record:
        # The corrections store's `twinned` rule, which this store lacked: an
        # ARN entry that stands alone BECAUSE two declarations share its name
        # is not the entry a record made under one of those declarations was
        # about. Without it, a `migrated` reported against `envs/dev`'s bucket
        # settled the twinned ARN entry the moment `envs/prod` declared the
        # same name — the gate published it migrated with its consumers, and
        # a later report on the ARN entry deleted dev's record through the
        # same predicate. ONE direction only: a record made on the twinned
        # entry itself still reaches the declaration that entry later folds
        # onto (prod's file leaves the scope, the ARN folds onto dev) — the
        # ordinary fold `record_handles` exists for, and the placement path
        # of the corrections store follows it too. Refusing both ways held the
        # gate for data reported landed and listed the record as nobody's.
        # And only for a BLOCK record: one made on the ARN-only entry before
        # any declaration existed is not twinned either, and refusing it once
        # two declarations arrived and the handle respelled lost a reported
        # outcome to "no entry in this scan".
        return None
    if (block_record and entry.get("detection") == "declared"
            and (entry.get("address"), directory_of(entry))
            != (record.get("address"), record.get("directory"))):
        # A block record reaching a DIFFERENT declaration through the ARN
        # alias: dev's file left the scope, prod declared the same name, the
        # ARN folded onto prod — and prod read `migrated` off dev's record
        # while the gate published it and the replay, for the same scenario,
        # refused ("recorded against 'envs/dev', which no longer declares
        # it"). The alias bridge is the un-fold onto a REFERENCED entry; a
        # declaration in another root module is the other-environment case,
        # whatever its address.
        return None
    arns = [a for a in record_handles(record)
            if is_literal_handle(a)]
    if not arns:
        return None
    if (placed_elsewhere(entry)
            and not is_literal_handle(record.get("address"))):
        # The alias bridge is for the UN-fold that takes the declaration
        # away — a scope amendment — and this store cannot see whether the
        # declaration is still there (the corrections store checks the
        # section; `status_of` has only the record and one entry). What it
        # can see is the verdict on the entry: an ARN the scan places in
        # another account or region, or among several, is by that verdict
        # not the declared resource a block-address record was made on,
        # however the alias reads. Without this the account verdict turning
        # complete left dev's queue AND the foreign spelling both `migrated`
        # on dev's one record, and a report on the foreign one then deleted
        # dev's through the same predicate.
        return None
    if any(handle == arn for arn in arns for handle in handles_of(entry)):
        # An exact handle is never a spelling match — the replay's rule too.
        # A record made on a flagged referenced entry, replayed against the
        # declaration that entry folded onto (its `arn` the very string the
        # record was keyed on), was refused as a spelling hop and the gate
        # held for data reported landed. After the twinned check, not before:
        # dev's alias equals the twinned entry's ARN exactly, and that is the
        # crossing the mark exists to stop.
        return "exact"
    if spelling_is_ambiguous(entry) or record.get("ambiguous_spelling"):
        # More than one spelling of this name stands in the section because
        # the scan could not reconcile their accounts or regions, so an
        # under-specified ARN agrees with several entries at once. Tolerating
        # it there let one reported `migrated` release the ship gate for two
        # queues nobody had moved, and let reporting on one spelling delete
        # the outcome recorded under another. Exact keys only: the entries
        # are distinct on purpose, and each is reported on its own. Asked of
        # the RECORD too, because the entry's flag goes away with the other
        # member of the pair while the record made against that member does
        # not — and then it read as the survivor's.
        return None
    if any(arn_spellings_agree(address, handle)
           for address in arns for handle in handles_of(entry)):
        return "spelling"
    return None


def _matching_records(document: dict, entry: dict, entries=None) -> list:
    """`entries` is the current section when the caller has it: the spelling
    tolerance is refused for a record that reaches more than one entry in
    it (`record_matches`). Every reader of the section passes it, and so
    does `record_status` through its `entries=` when the reporting tool
    hands it the section — a caller with one entry and no section still
    gets the one-entry answer."""
    return [r for r in (document or {}).get("migrations") or []
            if record_matches(r, entry, entries)]


def status_of(document: dict, entry: dict, entries=None):
    """The recorded status for this service, or None when nothing is recorded.

    Last write wins: `record_status` replaces rather than appends, so there is
    at most one record per key and no supersession rules are needed. This is
    the whole reason an operational outcome is kept apart from the corrections
    store, which needs four of them.
    """
    matches = _matching_records(document, entry, entries)
    if not matches:
        return None
    # Newest wins. `record_status` replaces what it matches, so there is
    # normally one — but a document written before this module matched
    # tolerantly can hold a superseded record under an older spelling, and
    # returning the first in list order handed back the stale one: a
    # completed status masking a live `in_progress`, and a component shipping
    # against a database still copying.
    return max(matches, key=lambda r: r.get("recorded_at") or "")


def clean_value(value: str) -> str:
    """A supplied `target` or `note` as it will be stored: stripped, and ""
    when it is only whitespace.

    `"   "` is not a destination and not a note, but `if value:` reads it as
    one. The target then defeats `record.get("target") or "target not
    recorded"` and reports a destination that was never given; the note
    defeats every `if record.get("note")` guard and leaves an empty "Note:"
    bullet in the artifact a platform engineer reads before shipping a
    component. Whitespace is also how a human spells "clear this" — the
    documented clear is `""`, which looks identical on screen.

    Stripping also makes what is stored match what was checked: the
    placeholder test strips before looking.

    Interior newlines collapse to spaces. The runbook renders a note as an
    indented continuation line, and `renderMarkdown` absorbs an indented line
    only while it does not start a list marker — so a second line leaves the
    item and becomes whatever it starts with. A note of
    `"...\n- Report it: ...\n## Everything is done"` produced a forged
    instruction bullet and a forged heading under a heading counting one item,
    in the state's PRIMARY artifact and the terminal's record. An earlier fix corrected
    the `"  - "` prefix; this is the same defect arriving in the value. Not a
    security hole — the renderer builds only text nodes — but it garbles the
    one document the step exists to produce.

    ALL interior whitespace collapses, not just `\n`: `str.split()` covers
    `\r`, the vertical tab and the Unicode line separators, any of which a
    renderer may treat as a break, and no legitimate target or note depends on
    a run of spaces surviving verbatim. Narrowing this to newlines would leave
    the same defect reachable by a different character.
    """
    return " ".join((value or "").split())


def looks_like_a_placeholder(target: str) -> bool:
    """True for a `<...>` stand-in rather than a destination.

    The printed call no longer carries one, but a user or an agent may still
    type `<the new instance>`, and this value is the durable record of where
    customer data went — the same reason `mark_replication_complete` refuses
    an unresolved `<AR_DESTINATION>`.

    Opening a stand-in is enough; it does not have to be closed. Requiring
    both ends let `<the new instance` — the shape a truncated paste actually
    produces — through, and no real destination starts with `<`. A refusal is
    recoverable, and this record is not.
    """
    return clean_value(target).startswith("<")


def record_status(document: dict, entry: dict, status: str, author: str,
                  recorded_at: str, target: str = None,
                  note: str = None, entries=None) -> tuple:
    """Records (or replaces) what has happened to one data service. In place.

    `target` is where it landed — a Cloud SQL instance, a bucket — recorded
    verbatim when the user supplies it and never invented, the same discipline
    `mark_replication_complete` keeps about digests. There is no way for the
    server to check it: the move happens with the user's own credentials,
    outside anything this server can see.

    A field the new call OMITS is carried forward from the record it replaces,
    rather than dropped. Re-reporting is ordinary — the call the listing prints
    carries no `target=`, so running it verbatim against an already-reported
    service was enough to erase one — and the note it erased is the one this
    module's own runbook comment calls "exactly what must not disappear before
    a component ships against that database". Passing a NEW value still
    replaces the old one; an explicit `""` clears the field, because a record
    that can only ever gain text is one a mistake cannot be taken back out of.

    `note` is carried only while the STATUS is unchanged. A note is written
    about the status it accompanies — "DMS job at 60%, cutover Tue" is a fact
    about a move in progress — and carrying it onto the completion says a
    finished migration is at 60%, in the runbook a platform engineer reads to
    decide whether a component may ship. `target` is not status-scoped: where
    the data landed is the same fact before and after the cutover.

    Returns (record, carried, dropped): what was kept, and what the status
    change discarded, so the caller can say both out loud.
    """
    if status not in STATUSES:
        raise ValueError(f"unknown status {status!r}")
    if target is not None:
        # Stored stripped, so "   " clears rather than recording a blank
        # destination — and `target=""`, the documented way to clear the
        # field, keeps working because it strips to itself. `note` gets the
        # same treatment in the loop below.
        target = clean_value(target)
    if target and looks_like_a_placeholder(target):
        raise ValueError(f"placeholder target {target!r}")
    # With the section when the caller has one: the record a report REPLACES,
    # and whose `target`/`note` it carries forward, must be one that names
    # this entry alone. A wildcard record reaching two spellings was replaced
    # by whichever was reported first, its note copied onto that one — a
    # foreign queue's durable record included — and the other left with
    # nothing, not even the stale line saying somebody had reported it.
    previous = status_of(document, entry, entries) or {}
    record = {
        "address": entry.get("address"),
        # The entry's other handle, so the record survives the entry folding
        # onto its declaration AND the declaration later leaving the scope.
        "aliases": [h for h in handles_of(entry) if h != entry.get("address")],
        "directory": directory_of(entry),
        "identifier": entry.get("identifier"),
        "service": entry.get("service"),
        "status": status,
        "author": author,
        "recorded_at": recorded_at,
    }
    if twinned(entry):
        # As the corrections store stamps it: the mark has to travel with the
        # record, because the record outlives the section that carried it.
        record["twinned"] = True
    if spelling_is_ambiguous(entry):
        # The corrections store's stamp, for the same reason: the flag lives
        # on the entry, and once the other member of the pair leaves the
        # section the survivor is unflagged — a `migrated` reported against a
        # dev-only secret then read as the declared secret's on the next scan
        # and dropped it from `outstanding`. `record_matches` refuses the
        # spelling tolerance on the record's word as well as the entry's.
        record["ambiguous_spelling"] = True
    same_status = previous.get("status") == status
    carried, dropped = [], []
    for field, value, survives in (
            ("target", target, True),
            # Normalised here rather than only at the tool, because this is
            # the function that decides what lands in the durable record.
            ("note", None if note is None else clean_value(note),
             same_status)):
        if value:
            record[field] = value
        elif value is not None:
            # An explicit "": the caller is clearing the field, not staying
            # silent about it. Without this there is no call that takes a
            # wrong note back out, and nothing at the terminal can either.
            pass
        elif previous.get(field):
            if survives:
                record[field] = previous[field]
                carried.append(field)
            else:
                dropped.append(field)
    records = document.setdefault("migrations", [])
    # Removed by the same test the read uses. An exact-key removal beside a
    # tolerant read left the superseded record in the document, where a later
    # respelling could surface it again.
    records[:] = [r for r in records if not record_matches(r, entry, entries)]
    records.append(record)
    return record, carried, dropped


def gating(entries: list) -> list:
    """The entries whose disposition owes a move, in section order."""
    return [e for e in entries or []
            if e.get("disposition") == GATING_DISPOSITION]


def outstanding(entries: list, document: dict) -> list:
    """Gating entries not yet reported migrated.

    `in_progress` is still outstanding — it is a status report, not a
    completion, and treating it as done would let a component ship against a
    database that is still copying.
    """
    return [e for e in gating(entries)
            if (status_of(document, e, entries) or {}).get("status") != MIGRATED]


def settled(entries: list, document: dict) -> list:
    """Every entry reported migrated, whatever it is graded now.

    NOT filtered through `gating`. What is OWED depends on the disposition;
    what was DONE does not. A service reported migrated and then recorded
    `keep-in-aws` — the ordinary sequence when a move turns out to be the wrong
    call after it has already run somewhere — stops being owed, and used to
    stop being reported at all: its entry was still in the section, so
    `stale_records` did not catch it either, and the only record that it moved
    lived on in the ledger where nothing would ever surface it again. The same
    hole swallowed a service recorded against a `rebuild` or `escalate` entry,
    which `mark_data_service_migrated` accepts on purpose.
    """
    return [e for e in entries or []
            if (status_of(document, e, entries) or {}).get("status") == MIGRATED]


def in_flight_not_owed(entries: list, document: dict) -> list:
    """Entries reported `in_progress` that no other view surfaces.

    The same hole `settled` was widened to close, for the other status.
    `outstanding` drops these on the disposition, `settled` drops them on the
    status, and `stale_records` drops them because the entry is still in the
    live section — three views, three different reasons, union empty. Both
    routes in are ordinary: report a move in flight and then record
    `keep-in-aws` (the step's own documented exit), or report one against a
    service that was never graded `migrate`, which the tool accepts on purpose
    and answers "Recorded anyway".

    This is the status where the AWS-side resource is still live and somebody
    is mid-cutover, so the note on it — "RDB export running, do not delete the
    AWS cluster" — is the one that must not disappear.
    """
    return [e for e in entries or []
            if (status_of(document, e, entries) or {}).get("status") == IN_PROGRESS
            and e.get("disposition") != GATING_DISPOSITION]


def closing_freezes_in_flight(entries: list, document: dict,
                              mono: str = "") -> str:
    """The cost of closing while a move is still running, or "".

    An in-flight move against a service the step does not gate satisfies
    "nothing owed, nothing unconfirmed" — so every site that offers the close
    on that test offers it over a live cutover. Rounds 17 and 18 established
    what the close then costs: `mark_data_service_migrated` refuses at the
    terminal and nothing transitions back, so the record can never be
    completed. The CLOSED runbook says exactly that.

    Saying it only afterwards says it too late. The same document tells the
    operator to "report it when it lands" — a promise about the future — and
    then offers the call that breaks it, eight lines apart. The close is right
    not to GATE on in-flight work, since the step does not wait on a service
    it never owed; what was missing is the sentence naming the trade.

    `mono` wraps the tool name for renderers that write markdown.
    """
    live = in_flight_not_owed(entries, document)
    if not live:
        return ""
    # "the data migration step", not "this step". Every one of the six callers
    # runs at that state today — `_migration_step_tail` is reached only on the
    # `!= REVIEW_STATE` arm of its one call site — so "this step" would also be
    # correct; naming it is for the reader, since three of the six sentences
    # arrive from a tool the operator thinks of as belonging to another phase.
    return (f"{len(live)} already reported in progress would be frozen by "
            f"that: {mono}mark_data_service_migrated{mono} is callable only "
            "from the data migration step, so a cutover still running when it "
            "closes can never be reported as landed — "
            + ", ".join(describe(e) for e in live) + ".")


def closed_over_in_flight(entries: list, document: dict) -> str:
    """The same cost, once it has been PAID, or "".

    The sibling above is stated wherever the close is offered. Nothing said it
    where the close is taken — and that is the channel the operator is in at
    the moment it happens: the close's own SUCCESS, and the two arms that
    report another session closed the step underneath this call. The closed
    runbook says it, but nothing in those responses sends anyone there, and
    the instructions have the agent hand back control rather than open
    artifacts.

    Past tense, because the choice is gone. An offer the operator can still
    decline and a fact they now have to work around are different sentences,
    and printing the conditional one after the event reads as a warning they
    still have time to act on.

    No `mono`, unlike the sibling: every caller here writes chat, because the
    runbook is not rewritten after the close. No claim about the runbook
    either — whether it carries these records depends on the caller. At the
    close's SUCCESS it does, and that site says so; in `_report`'s
    closed-under-us arm a record created after the close is in no section of
    it, and `CLOSED_UNDER_US` in the same response already says the document
    does not reflect the call.
    """
    live = in_flight_not_owed(entries, document)
    if not live:
        return ""
    return (f"{len(live)} move(s) still reported in progress can no longer be "
            "completed: mark_data_service_migrated does not run from the "
            "terminal, so these records stay 'in progress' for good — "
            + ", ".join(describe(e) for e in live) + ". They are kept.")


def consumers_of(entry: dict) -> list:
    """The workload names attached to an entry, deduplicated and sorted.

    Who is waiting for this move. An entry with none is not a mistake: the
    scan under-detects deliberately and the review may have left it
    unattributed, which the listing says out loud rather than reading as
    "nobody needs this".
    """
    return sorted({c.get("workload") for c in entry.get("consumers") or []
                   if c.get("workload")})


def describe(entry: dict) -> str:
    """How a data service is named back to the operator."""
    service = entry.get("service") or "data service"
    identifier = entry.get("identifier") or entry.get("address") or "(unnamed)"
    return f"{service} {identifier}"


# What each gating service moves to, and by what means. Short enough to put
# beside an entry in the runbook; the argument for each is in the phase
# knowledge document, which is where a reader goes next.
TARGETS = {
    "rds": ("Cloud SQL", "Database Migration Service, continuous"),
    "memorydb": ("Memorystore", "RDB snapshot export and import, point-in-time"),
    "s3": ("Cloud Storage", "Storage Transfer Service, repeatable"),
    "efs": ("Filestore", "Storage Transfer Service with a POSIX agent"),
    "fsx": ("Parallelstore, NetApp Volumes or Filestore",
            "depends on the flavour — Lustre copies to Parallelstore, ONTAP "
            "is a NetApp conversation, OpenZFS has no equivalent; often an "
            "escalation rather than a copy"),
    "secretsmanager": ("Secret Manager", "by hand; the values usually change"),
    "ssm": ("Secret Manager", "by hand; the values usually change"),
    # Only reachable when the harvester UPGRADED it: a Redis with
    # `snapshot_retention_limit > 0` persists, so it is graded `migrate`
    # rather than `rebuild`. Without an entry here the one case where an
    # ElastiCache really is owed printed "no default target".
    "elasticache": ("Memorystore",
                    "persistent Redis: RDB snapshot export and import"),
}


def ambiguity(entries: list) -> tuple:
    """(addresses declared more than once, (address, directory) pairs likewise).

    What a reporting call has to name to be accepted. Computed over the WHOLE
    section, because an address is ambiguous by virtue of another entry sharing
    it.
    """
    by_address, by_pair = {}, {}
    for entry in entries or []:
        address = entry.get("address")
        by_address[address] = by_address.get(address, 0) + 1
        pair = (address, directory_of(entry))
        by_pair[pair] = by_pair.get(pair, 0) + 1
    return ({a for a, n in by_address.items() if n > 1},
            {p for p, n in by_pair.items() if n > 1})


def reporting_call(entry: dict, repeated_addresses, repeated_pairs) -> str:
    """The `mark_data_service_migrated(...)` call that settles this entry.

    ONE builder for the chat listing and the ledger runbook. They were written
    separately and drifted immediately: the listing learned to add `identifier`
    when an `_override.tf` rename makes the directory insufficient, and the
    runbook did not — so the artifact the review UI shows as this step's
    subject printed calls the tool refuses. A reader following either verbatim
    has to get an accepted call.

    `directory=""` is emitted rather than skipped when the entry is declared at
    the repository root and another root module shares the address: an empty
    string is what `_target` matches on, and dropping it because it is falsy
    produces the ambiguous call again.

    No `target=`. It carried `"<where it landed>"`, which the tool refuses —
    correctly, since a stand-in in the durable record of where customer data
    went is worse than nothing — so the printed call was one that could not be
    run, while three places promised it could. The argument is optional and
    the instructions already say to omit it unless the user states a
    destination, so the honest call is the one without it.
    """
    return (f"mark_data_service_migrated("
            + targeting_args(entry, repeated_addresses, repeated_pairs) + ")")


def targeting_args(entry: dict, repeated_addresses, repeated_pairs) -> str:
    """The arguments that name exactly one entry, for any tool that takes them.

    Extracted from `reporting_call` when the runbook tools became the fourth
    and fifth callers of `_target`. Printing `address=` alone is correct until
    two root modules declare it, and then it is a call the tool refuses — the
    defect `reporting_call`'s docstring records being fixed once already, which
    a second builder promptly reintroduced for a second tool. There is one
    builder because there is one refusal.
    """
    address = entry.get("address")
    directory = directory_of(entry)
    args = f'address="{address}"'
    if address in repeated_addresses:
        args += f', directory="{directory}"'
        if (address, directory) in repeated_pairs:
            args += f', identifier="{entry.get("identifier")}"'
    return args


def runbook_call(entry: dict, repeated_addresses, repeated_pairs) -> str:
    """The `get_data_migration_runbook(...)` call for this entry."""
    return (f"get_data_migration_runbook("
            + targeting_args(entry, repeated_addresses, repeated_pairs) + ")")


def target_and_how(entry: dict) -> tuple:
    """What this entry moves to and by what means, refined by engine.

    `TARGETS` answers per service, which is right for nine of the eleven
    procedures and wrong for two of the four RDS ones: MariaDB has no continuous path and SQL
    Server seeds from backups, and both would otherwise be announced as
    "Database Migration Service, continuous" in the chat listing, the worklist
    and the first line of the hand-out.
    """
    from . import runbooks as rb
    return rb.target_for(entry, TARGETS.get(
        entry.get("service"),
        ("no default target for this service",
         "decide the target before starting")))


def _procedure_line(entry: dict, rendered=None, records=()) -> str:
    """Where the reader goes for the steps, for one owed entry.

    Imported here rather than at module import: `runbooks` reads this
    package's own directory and this module is the pure one, kept importable
    by anything. The import is cheap and cached, and keeping it local means a
    packaging problem surfaces at the one line that needs the file rather than
    at every import of the outcome store.
    """
    from . import runbooks as rb
    if rendered is LISTING_UNREAD:
        # "Could not tell" is not "not adapted": the listing failed, and the
        # artifact must not state as fact that the steps live only inside the
        # server when an adapted copy may be sitting in this ledger.
        return ("whether an adapted runbook exists in this ledger could not be "
                "read this time; the next refresh will say — until then ask at "
                "the step (`get_data_migration_runbook`), which looks again")
    # Under every handle the entry has had, not only the current one: the
    # name moves across a fold, the adapted file does not. The outcome
    # records matched to the entry carry the earlier spellings.
    path = next((p for p in rb.rendered_blobs(entry, records)
                 if rendered and p in rendered),
                rb.rendered_blob(entry))
    if rendered and path in rendered:
        # Checked FIRST, before the "is there a template" question. `save`
        # accepts an entry with no shipped procedure on purpose — `_no_runbook`
        # tells the agent the plan is the team's to write — and asking about
        # the template first made that written plan invisible in the one
        # artifact somebody opens weeks later.
        return f"`{path}` — adapted for this estate."
    name = rb.name_for(entry)
    if name is None:
        engine = rb.unsupported_engine(entry)
        if engine:
            return (f"none — Cloud SQL has no `{engine}` engine, so this is "
                    "an engine change rather than a move, and this repository "
                    "escalates those. The decision is the team's; "
                    "`keep-in-aws` records it if the answer is to leave it "
                    "where it is.")
        if rb.needs_engine(entry):
            # Four RDS procedures ship; which one this database needs depends
            # on a field the declaration built from a variable. Saying "no
            # procedure" here sends the reader away from four documents, one
            # of which is theirs.
            return ("the declaration does not state an engine, and the four "
                    "RDS procedures differ fundamentally. Name the engine ("
                    + ", ".join(rb.ENGINE_CHOICES) + ") and the agent at this "
                    "step can produce the runbook.")
        reason = rb.NO_RUNBOOK_REASON.get(entry.get("service"))
        return ("no generated procedure — "
                + (reason if reason else
                   "none has been written for this service.")
                + " See `knowledge/data-migration.md` for what the grade "
                  "means.")
    return (f"`{name}.md` ships with the server, not yet adapted for this "
            "estate — ask the agent at this step for it "
            "(`get_data_migration_runbook`), which is the only route to it: "
            "the file lives inside the server rather than in this ledger, and "
            "the phase document deliberately no longer carries the steps.")


# How the step left, for `runbook`. There is one way out — everything owed
# was settled — and at the terminal it reaches, every tool this document names
# refuses. So the closed document stops instructing and becomes the record of
# what the close left behind.
#
# There was a second value here for a close that left work owed on a damaged
# ledger. That close is gone: a damaged ledger does not finish the workflow.
CLOSED_COMPLETE = "complete"


def runbook(entries: list, document: dict, workspace: str,
            generated_at: str, copies: list = None, rendered=None,
            closed: str = None) -> str:
    """The per-estate worklist, as markdown for the ledger.

    Deliberately not a script. The image replication runbook can emit runnable
    `skopeo copy` lines because it knows both ends; here the target host, the
    credentials and the maintenance window are all things only the operator
    has, and a generated `gcloud database-migration` line with an invented
    hostname would be worse than no line at all. So this lists what is owed,
    who is waiting for each one, and what it moves to — and points each entry
    at its procedure: the copy an agent adapted for this estate where one
    exists, the shipped template where none does yet, and the reason where
    there is none to have (`_procedure_line`).

    `copies` is the unconfirmed image refs. They are the OTHER half of what
    holds the step open, and a document titled "what still has to move" that
    omits them told a platform engineer nothing was outstanding at a state
    that would not close.

    `rendered` is the set of ledger paths that already hold an adapted
    per-service runbook (`runbooks.rendered_blob`). An owed entry names its
    own, so a reader six weeks later finds the procedure somebody already
    tailored rather than starting from the phase document again — and an entry
    with a procedure available but none written yet says that too, since
    "nobody has adapted this" and "there is no procedure for this" are
    different states and only the second is a dead end.

    `closed` is set once the step has left, and then every tool this document
    names refuses from where its reader now stands: the artifact is registered
    at the terminal with the caveat "written as it stood when the step
    closed", so it is the durable record of what was left behind, not a set of
    instructions. Before that it offered `keep-in-aws` at a terminal where the
    call refuses, under a heading whose "these" referred to an empty list.
    """
    actionable = closed is None
    owed = outstanding(entries, document)
    done = settled(entries, document)
    repeated_addresses, repeated_pairs = ambiguity(entries)
    lines = [
        f"# Data migration runbook — {workspace}",
        "",
        f"Generated {generated_at} from the approved data dependency mapping.",
        "",
        "Every command in the procedures below runs with YOUR credentials, "
        "against your own accounts. The server never sees these moves happen, "
        "which is why completion is something you report rather than something "
        "it detects.",
        "",
        "**Before anything moves:** the landing zone Terraform has to be "
        "applied — the translation phase ends at a Pull Request and nothing "
        "here applies it, so the targets below do not exist until somebody "
        "merges it.",
        "",
    ]

    if closed:
        lines += [
            "> **This workspace has left the data migration step.** It closed "
            "with nothing outstanding."
            + " None of the tools named in this document can be called from "
            "the terminal state the workspace is now in — they are shown so "
            "the record is legible, not so anything can be reported. Nothing "
            "transitions back into the step; only an admin "
            "`join_ledger --reconfigure` reopens it, and that restarts the "
            "platform walk from the beginning.",
            "",
        ]

    if not owed:
        lines += ["No data service is waiting to move.", ""]
    else:
        lines += [f"## {len(owed)} service(s) still owe a move", ""]
        for entry in owed:
            target, how = target_and_how(entry)
            status = (status_of(document, entry, entries) or {}).get("status")
            consumers = consumers_of(entry)
            # `directory_of`, not `entry_directory`: an ARN-only entry is
            # declared nowhere, and `evidence[0]`'s dirname moves with the
            # walk. The artifact outlives the session, so the split the chat
            # listing already fixed matters more here, not less.
            directory = directory_of(entry)
            lines += [
                f"### {describe(entry)}",
                "",
                (f"- Declared: `{entry.get('address')}`"
                 + (f" in `{directory}`" if directory else " at the repository root"))
                if entry.get("detection") != "referenced" else
                f"- Known only from this ARN: `{entry.get('address')}`; nothing"
                " the scan read declares it",
                f"- Evidence: {', '.join(entry.get('evidence') or []) or 'none recorded'}",
                "- Used by: " + (", ".join(consumers) if consumers else
                                 "nobody the scan could attribute — see the "
                                 "entry's notes; unattributed is not unused"),
                f"- Target: {target} ({how})",
                f"- Status: {status or 'not started'}",
            ]
            # The status note, which is the whole value of an `in_progress`
            # report: "DMS job dms-orders-1 at 60%, cutover Tue 02:00 UTC" has
            # to survive the session, and this artifact is what survives it.
            # The done block carries its note; the owed block never did, and
            # that is the half that matters more.
            if (status_of(document, entry, entries) or {}).get("note"):
                lines.append(f"- Note: {status_of(document, entry, entries)['note']}")
            lines.append("- Procedure: " + _procedure_line(
                entry, rendered, _matching_records(document, entry, entries)))
            # No `actionable` arm here, unlike the settled and in-flight
            # sections below. A closed document cannot carry owed entries:
            # `closed` is set only on the success path, which the
            # `if owed or copies:` refusal returns before, and the refresh
            # re-renders from the same entries. The caveated close was the one
            # path that produced a closed runbook with work owed, and it is
            # gone — so a branch for it would be unreachable code that looks
            # reachable, which this package refuses elsewhere by name.
            lines += [
                "- Report it: `"
                + reporting_call(entry, repeated_addresses, repeated_pairs)
                + "`",
                "",
            ]

    if done:
        lines += [f"## {len(done)} already reported migrated", ""]
        for entry in done:
            record = status_of(document, entry, entries) or {}
            grade = entry.get("disposition")
            lines.append(
                f"- {describe(entry)} → {record.get('target') or 'target not recorded'}"
                f" ({record.get('author')} on {record.get('recorded_at')})"
                # The grade, for the same reason the chat listing carries it:
                # "this moved and was owed" and "this moved and then the
                # customer decided to keep it" are both true and only the
                # first is progress against the gate. The chat listing was
                # given this in an earlier revision and the artifact was not — the same
                # split a later review found for the reporting call, in the copy
                # that outlives the session.
                + (f" — graded '{grade}', not owed here"
                   if grade != GATING_DISPOSITION else ""))
            # The note is why the field exists: "only the orders schema moved,
            # the audit tables are still in RDS" is exactly what must not
            # disappear before a component ships against that database. It was
            # rendered only for entries still owed, so it vanished at the
            # moment it started to matter.
            if record.get("note"):
                # A continuation line, not a nested bullet. `renderMarkdown`
                # treats an indented line as part of the item above it only
                # when it does NOT start a list marker, so "  - " made the
                # note a sibling of the service and the list showed more
                # bullets than the heading counted. Reached the moment the
                # artifact was switched to the markdown renderer.
                lines.append(f"  Note: {record['note']}")
            # The address and the call, like every owed entry above. A record
            # here is amendable — a corrected target, a caveat the next reader
            # needs — and the instructions tell the agent to use the address
            # exactly as this document printed it and never to construct one.
            # `actionable`, for the same reason the owed section honours it:
            # from the terminal this call refuses, and a document whose own
            # banner says nothing transitions back must not hand one out.
            lines.append(
                ("  Amend it: `" if actionable else
                 "  Recorded with (not callable from here): `")
                + reporting_call(entry, repeated_addresses, repeated_pairs)
                + "`")
        lines.append("")

    in_flight = in_flight_not_owed(entries, document)
    if in_flight:
        lines += [
            f"## {len(in_flight)} move(s) in progress that nothing waits on",
            "",
            "Reported in flight against a service this step does not gate — "
            "either it was excused after the move started, or it was never "
            "graded `migrate`. It holds nothing up, but the AWS-side resource "
            "is still live and somebody is mid-cutover.",
            "",
        ]
        for entry in in_flight:
            record = status_of(document, entry, entries) or {}
            lines.append(
                f"- {describe(entry)} — graded '{entry.get('disposition')}'"
                f" ({record.get('author')} on {record.get('recorded_at')})")
            if record.get("note"):
                lines.append(f"  Note: {record['note']}")
            # Without this the record is unreportable: the session that made
            # it is gone — which is the whole premise of a state that survives
            # weeks — and the next one has no address to complete it with,
            # while `list_data_dependencies` refuses here. The document would
            # then say "the AWS-side resource is still live" permanently.
            #
            # Once the step has closed the promise inverts: "report it when it
            # lands" is a claim about the FUTURE, in the one document that also
            # says nothing transitions back into the step, for the one case
            # where the record genuinely can never be completed.
            lines.append(
                ("  Report it when it lands: `" if actionable else
                 "  Was to have been reported with (not callable from here, "
                 "and this record can no longer be completed): `")
                + reporting_call(entry, repeated_addresses, repeated_pairs)
                + "`")
        lines.append("")

    stale = stale_records(entries, document)
    if stale:
        # `stale_records`' own docstring says these are "reported rather than
        # kept quietly ... must not silently vanish from the count of what was
        # done" — and in the artifact a later reader actually opens, they did.
        lines += [
            f"## {len(stale)} recorded outcome(s) with no entry in this scan",
            "",
            "The scope may have narrowed, the resource may be gone, the "
            "service may now be kept in AWS — or the declaration the outcome "
            "was recorded under has left the scan while its ARN stands as a "
            "referenced entry the scan places in another account, region or "
            "partition, which is then owed above as a different resource. "
            "\"This moved\" stays true either way, so the record is kept rather "
            "than dropped — but it is not counted above, because there is no "
            "entry to count it against.",
            "",
        ]
        for record in stale:
            # The fourth cause, named on the record it applies to: the same
            # service name is owed above, and a reader must not take the two
            # lines for a contradiction.
            elsewhere = [e for e in entries or []
                         if placed_elsewhere(e)
                         and any(h == a for h in record_handles(record)
                                 for a in handles_of(e))]
            # And the fifth: a spelling that reaches more than one entry in
            # this scan reaches none (`record_matches`), so the record is
            # stale while every entry it could mean is owed above.
            arns = [a for a in record_handles(record)
                    if is_literal_handle(a)]
            shared = [e for e in entries or []
                      if any(arn_spellings_agree(a, h)
                             for a in arns for h in handles_of(e))]
            if len(shared) < 2:
                shared = []
            lines.append(
                f"- {record.get('service')} "
                f"{record.get('identifier') or record.get('address')} "
                f"({record.get('status')}"
                + (f" → {record['target']}" if record.get("target") else "")
                + ")"
                + ((" — recorded under a declaration this scan did not read; "
                    f"its ARN now stands as {', '.join(sorted(e.get('address') or '' for e in elsewhere))}, "
                    + ("which the scan cannot place in one account or region and "
                       "which may no longer be this resource"
                       if all(any(n.startswith(AMBIGUOUS_ACCOUNT_NOTE_PREFIX)
                                  for n in e.get("notes") or [])
                              and not any(n.startswith((CROSS_ACCOUNT_NOTE_PREFIX,
                                                        CROSS_REGION_NOTE_PREFIX,
                                                        CROSS_PARTITION_NOTE_PREFIX))
                                          for n in e.get("notes") or [])
                              for e in elsewhere)
                       else "which the scan places in another account, region or "
                            "partition")
                    + " — owed above as its own resource")
                   if elsewhere else "")
                + ((" — its spelling agrees with "
                    f"{len(shared)} entries in this scan ("
                    + ", ".join(sorted(e.get("address") or "" for e in shared))
                    + ") and is counted against none of them; "
                    + ("each is owed above on its own"
                       if any(e.get("disposition") == GATING_DISPOSITION for e in shared)
                       else "none of them is graded `migrate`, so nothing waits on "
                            "them, but each stands on its own")
                    + ", and a report against one must name the exact spelling "
                    "the listing prints")
                   if shared and not elsewhere else ""))
        lines.append("")

    lines += [
        "## Not owed here",
        "",
        "Only `migrate` gates a component. `rebuild`, `replatform`, "
        "`escalate`, `keep-in-aws` and `undecided` do not, and none of them "
        "appears in the owed list above. Any of them CAN appear in the two "
        "sections that report what happened anyway — \"already reported "
        "migrated\" and \"in progress that nothing waits on\", each labelled "
        "with its grade — because the tools accept a report against any "
        "service and a move that happened, or is happening, is worth "
        "recording whether or not anything waited on it. An empty "
        "cache is a working cache; a replatform changes the shape rather than "
        "moving bytes; an escalation is a decision nobody has made yet; "
        "`undecided` is what the scan grades a service it could not place.",
        "",
    ]

    copies = sorted(copies or [])
    if copies:
        # The other half of the gate. `list_data_migrations` has always
        # printed these; the artifact registered as the step's SUBJECT did
        # not, so it read "nothing outstanding" at a state that would not
        # close, and at the terminal it left an abandonment or a confirmed
        # copy no trace at all.
        lines += [
            f"## {len(copies)} container image cop(y/ies) also unconfirmed",
            "",
            "These hold the step open exactly the way an unmoved database "
            "does: the copy was planned or attempted and nobody has said it "
            "landed. Until each is settled a developer's image references "
            "stay un-rewritten, because the destination is a plan rather "
            "than an address.",
            "",
        ]
        lines += [f"- `{ref}`" for ref in copies]
        # No `closed` variant: the close requires nothing owed AND nothing
        # unconfirmed, so a closed document never carries a copy. A
        # non-actionable footer here would be unreachable code that looks
        # reachable, which this package refuses elsewhere by name.
        lines += [
            "",
            "Report each with `mark_replication_complete(refs=[...])`, or "
            "`abandon_image_replication(refs=[...], reason=...)` for one "
            "that will not be copied.",
            "",
        ]

    if actionable and not owed and not copies:
        # Nothing is holding the step open. The exit section's "one of these"
        # refers to the owed list, which is empty here — an earlier fix removed the
        # same dangling reference from the CLOSED document; the live all-
        # settled shape reaches it by another route.
        frozen = closing_freezes_in_flight(entries, document, mono="`")
        lines += [
            "## Nothing is outstanding",
            "",
            "`complete_data_migration()` closes the step and moves the "
            "platform walk to its terminal. A service that turns out to need "
            "moving after all can still be reported until then.",
            "",
        ] + ([frozen, ""] if frozen else [])
    elif actionable and owed:
        # `owed`, not just "whatever the first arm rejected". That arm also
        # rejects an empty owed list with a copy outstanding, and the heading's
        # "these" has no antecedent there either — the document says eight
        # lines above that no data service is waiting to move, and the only
        # outstanding item is a container image, whose two exits the section
        # directly above already names. Offering `annotate_data_dependency`,
        # which takes a data-service `address=`, as the answer to an image is
        # the same dangling reference, in the arm a later fix did not split.
        lines += [
            "## If one of these should not move after all",
            "",
            "`annotate_data_dependency(address=..., disposition=\"keep-in-aws\", "
            "note=...)` records that durably. The service stops being owed "
            "work and stops gating. It is not free — cross-cloud "
            "connectivity, a credential path for a pod that used to use "
            "IRSA, and egress on every call — so say why in the note.",
            "",
        ]
    return "\n".join(lines)


def unconfirmed_copies(inventory: dict) -> list:
    """Image refs whose replication is still waiting on a human assertion.

    `self_service` is a plan the server wrote and the operator accepted: the
    runbook exists, the copy has not been observed. `replication_failed` is a
    copy the server attempted and could not finish. Both are what
    `mark_replication_complete` exists to settle
    (`replication.USER_MARKABLE_STATUSES`), and that tool is callable only from
    the data migration step — so this step has to wait for them, or moving the
    tool here would simply have moved where they get stranded.

    An image with no `replication` record at all is not owed: replication was
    declined, the images stay in ECR by decision, and there is nothing to
    assert. Nor is one recorded `abandoned` — that IS the decision, made per
    image with a reason, and it is what keeps this step a decision point
    rather than a trap for a copy nobody is going to make.
    """
    from .replication import USER_MARKABLE_STATUSES
    return sorted(
        image["ref"] for image in (inventory or {}).get("images") or []
        if isinstance(image, dict) and image.get("ref")
        and ((image.get("replication") or {}).get("status")
             in USER_MARKABLE_STATUSES))


def stale_records(entries: list, document: dict) -> list:
    """Recorded outcomes whose entry the current section no longer carries.

    Kept rather than deleted, and reported rather than kept quietly. The scope
    may have narrowed, the resource may be gone, or the service may have been
    switched to `keep-in-aws` — and a record saying "this moved" is a fact
    about the estate that stays true whichever of those it was. What it must
    not do is silently vanish from the count of what was done.
    """
    live = list(entries or [])
    # Matched the way `settled` matches, through `record_matches`. While this
    # used exact keys and `settled` did not, one service was rendered under
    # both "already reported migrated" and "recorded outcome(s) with no entry
    # in this scan" — whose body says it is not counted above, in the same
    # artifact that counted it.
    return [r for r in (document or {}).get("migrations") or []
            if not any(record_matches(r, e, live) for e in live)]
