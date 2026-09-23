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

"""The human's corrections to the workload→data-service mapping.

Pure logic — no GCS, no LLM. `consumers.py` derives the mapping from the
Terraform; this module carries what a human said about the result, and replays
it over every later scan.

It exists because the section cannot hold the corrections itself.
`scan_data_dependencies` clears `data_dependencies` on every run — rediscovery
semantics, so an entry whose file the operator has since excluded cannot
survive — and `amend_discovery_scope` makes a re-scan a routine part of the
assessment review. A correction written into the section would be erased by
the next scan with nothing recording that it was ever made, and the reviewer
would be asked the same question again with no memory of their answer.

So the corrections live beside the section, in their own ledger object, and
are replayed after each harvest. Three kinds of decision about a LINK, and
three about an ENTRY. The link kinds:

  reject_consumer — a derived consumer is wrong. The link came from a
  reference chain that resolved, so the next scan will derive it again; the
  override is what keeps it out.

  attach_consumer — a consumer the chains could not reach. This is the other
  half of the trade `consumers.py` makes: it refuses name similarity and
  under-detects on purpose, which is only defensible because the entries it
  leaves unattributed are put in front of a human who can attach them.

  annotate — a note, a disposition, or both. `keep-in-aws` is reserved by the
  inventory schema for a human editing the section, and this is that human.

The entry kinds answer what `inferred.py` asks, and cover what nothing in the
files proposes:

  confirm_entry — a guess is real. The entry becomes `human_review`, takes a
  disposition (the reviewer's, or the service's default), loses its guess
  note, and may gain a consumer the reviewer names.

  dismiss_entry — a guess is wrong. The entry goes, and stays gone across
  re-scans; a dismissal also retires the other decisions on that entry, which
  could never be placed again. Only a guess or a hand-added entry can be
  dismissed — a declared or referenced entry is a fact the files state.

  add_entry — a data service nothing proposed. Created as `human_review`;
  when a later scan derives the same resource (a declaration, an ARN, an
  endpoint, or even its own guess), the record lands on that entry instead,
  because the file answers the question better than the assertion did.

Last-write-wins, and what makes two records the same decision is:

  The ENTRY. Not one address but a SET of `(address, directory)` handle
  pairs — the record's own, plus every handle the entry also had when the
  record was made (`aliases`) — intersected by `_same_subject`. An entry
  known from a literal ARN is addressed by that ARN and folds onto its
  declaration when a later scan finds one, so one entry has two spellings
  over its life and a decision recorded under either has to reach it. A
  block address is unique only within a root module, so its directory
  travels with it and `envs/dev` cannot retire `envs/prod`; an ARN is
  unique on its own, so its directory is normalised away (`_handle_pairs`).

  The IDENTIFIER, when both records state one. `_amend` records it only
  when the block address has siblings — an `_override.tf` that renames a
  resource leaves two entries at one address in one directory — and it
  cannot record one at all under an ARN handle. So an unstated identifier
  is a wildcard ONLY when the two met through an ARN handle
  (`_same_identifier`); at a block address it means the address had one
  entry when the record was made, and treating that as the same decision as
  a later sibling's would silently retire a customer's `keep-in-aws`.

  That narrow rule governs `same_scope` and the annotation branch, which
  are the ones that put a removed consumer back or overwrite a disposition.
  `overlaps` and `covers` still compare the field directly, so an unstated
  identifier is a plain wildcard for them and a rejection naming one
  sibling can retire an attachment about the other. That predates the ARN
  handles and closing it is a decision about the other three supersession
  directions, tracked on its own rather than folded in here.

  The WORKLOAD and the CONSUMER_KIND, for the three link-level kinds. Two
  chains reach one entry under one workload name — a `helm_release` and an
  IRSA service account are different Kubernetes objects — so a decision
  about the IAM role is not a decision about the release.

What replaces what is not one rule: a rejection is replaced only by a
rejection covering at least as much (`covers`), an attachment retires a
rejection only when it names exactly the same link (`same_scope`), the
remaining link-level cases retire whatever they overlap (`overlaps`), and an
entry-level decision replaces any other on the same entry — the matrix is in
`record_override`.

An override whose block is gone from a later scan is NOT dropped. The block
may be gone because the scope changed, because a file moved, or because the
resource was deleted, and this module cannot tell those apart — so it keeps
the record and says, in the scan notes, that it did not apply.
"""

import copy
import hashlib
import json
import os

from . import datastores

DOCUMENT_VERSION = 1

REJECT = "reject_consumer"
ATTACH = "attach_consumer"
ANNOTATE = "annotate"
# Decisions about an ENTRY rather than about one of its links: the answer to
# a guess (`inferred.py`), and a data service nothing in the files proposed.
CONFIRM = "confirm_entry"
DISMISS = "dismiss_entry"
ADD = "add_entry"
ENTRY_KINDS = (CONFIRM, DISMISS, ADD)
KINDS = (REJECT, ATTACH, ANNOTATE) + ENTRY_KINDS

# What an attached consumer records as its detection. Not one of the two
# derived chains: the gate that reads this section later has to be able to see
# that a human put it there, and a reader looking for the evidence file has to
# be told there is not one.
HUMAN_DETECTION = "human_review"


def empty_document() -> dict:
    """A fresh overrides document."""
    return {"schema_version": DOCUMENT_VERSION, "overrides": []}


# The two independent decisions an `annotate` record can carry. They are keyed
# apart because they are separately made: a reviewer who writes down the
# customer's reason and then, five minutes later, sets the disposition has made
# two decisions, not replaced one. Keying annotations on the entry alone would
# have the second call delete the first — and what it deletes is the reason a
# service is being kept, which is exactly the durable human decision this whole
# object exists to protect.
ANNOTATION_FIELDS = ("note", "disposition")


def _handle_pairs(record: dict) -> set:
    """Every (address, directory) a record may be keyed on: its own, plus
    the entry's other handles at the time it was recorded (`aliases` — the
    ARN of an entry that later folded onto its declaration, or that
    declaration's block address, with its directory, for a record made
    against the ARN). An ARN is unique on its own, so its directory is
    always ""; a block address is unique only within a root module, so its
    directory travels with it and `envs/dev` cannot retire `envs/prod`.
    Two records sharing a pair are about the same entry."""
    def pair(address, directory):
        if _is_arn(address):
            return (address, "")
        return (address, directory or "")
    pairs = {pair(record.get("address"), record.get("directory"))}
    for alias in record.get("aliases") or []:
        if isinstance(alias, dict):
            pairs.add(pair(alias.get("address"), alias.get("directory")))
        else:
            # An earlier shape recorded the alias as a bare address.
            pairs.add(pair(alias, None))
    return pairs


def _own_pair(record: dict) -> tuple:
    """The one handle a record was actually made under, normalised as
    `_handle_pairs` normalises: an ARN carries no directory."""
    address = record.get("address")
    if _is_arn(address):
        return (address, "")
    return (address, record.get("directory") or "")


def _is_arn(address) -> bool:
    """A literal handle — an ARN, an endpoint, a guess's or a hand-added
    entry's `<service>:<name>` — as opposed to a block address. The name
    predates the other three shapes; the rule is `datastores.is_literal_handle`."""
    return datastores.is_literal_handle(address)


def _addressed_by_arn(record: dict) -> bool:
    """Was this record made against an entry's ARN, rather than merely
    carrying one as an alias?

    The distinction is what keeps the alias channel to the one job it has.
    `_alias_records` attaches the folded entry's ARN to EVERY record made
    under the declaration's block address, so two records made under block
    addresses routinely share an ARN alias — in two different root modules,
    or at one address whose entry has since split. Letting that intersection
    decide identity bypassed both guards the module is built on: `envs/dev`
    retired `envs/prod`, and an unstated identifier at a block address
    became a wildcard that retired a customer's `keep-in-aws` in favour of a
    sibling's decision.
    """
    return _is_arn(record.get("address"))


def _spellings_may_meet(one: dict, other: dict) -> bool:
    """May the spelling tolerance run between these two records?

    Not when either was recorded against an entry the scan flagged as
    ambiguous. Several spellings of one name stand apart there because their
    accounts or regions could not be reconciled, and an under-specified ARN
    agrees with every one of them — so the tolerance that reunites one
    resource wearing two spellings instead conflates two resources wearing
    one name. It deleted a customer's `keep-in-aws` on a queue in one account
    for a decision about a queue in another, and reported success: the
    failure the deleted `_key` existed to prevent, arriving through the ARN
    channel. `_amend` stamps the record while the entry is in front of it,
    the same way it stamps `twinned`, because the entry may be gone by replay.
    """
    return not (one.get("ambiguous_spelling") or other.get("ambiguous_spelling"))


def _arn_pairs_meet(one: dict, other: dict) -> bool:
    """Do two records share a handle, allowing two SPELLINGS of one ARN?

    `entries_at` was taught that an entry's handle is one spelling and that
    the merge can settle on another; supersession has to know it too, or two
    corrections about one resource stand side by side and the section says
    both that a consumer was rejected and that it was attached. See
    `datastores.arn_spellings_agree`.
    """
    mine, theirs = _handle_pairs(one), _handle_pairs(other)
    if mine & theirs:
        return True
    if not _spellings_may_meet(one, other):
        return False
    return any(datastores.arn_spellings_agree(a, b)
               for a, _d in mine if _is_arn(a)
               for b, _e in theirs if _is_arn(b))


def _same_own_handle(one: dict, other: dict) -> bool:
    """The one handle each record was made under, comparing two ARN
    spellings as the same handle — but never across an ambiguous group."""
    a, b = _own_pair(one), _own_pair(other)
    if a == b:
        return True
    return (_addressed_by_arn(one) and _addressed_by_arn(other)
            and _spellings_may_meet(one, other)
            and datastores.arn_spellings_agree(a[0], b[0]))


def _same_subject(one: dict, other: dict) -> bool:
    if (one.get("workload") or "") != (other.get("workload") or ""):
        return False
    if _same_own_handle(one, other):
        return True
    # Otherwise they can only meet through an alias, and only in the
    # direction aliases exist for: a record made under an entry's ARN and one
    # made under the block address that ARN later folded onto. Two records
    # that were BOTH made under block addresses are compared on those
    # addresses and their directories, as they were before aliases existed.
    if not (_addressed_by_arn(one) or _addressed_by_arn(other)):
        return False
    if not _arn_pairs_meet(one, other):
        return False
    # The same entry under two spellings ONLY while the ARN belongs to one
    # entry. `twinned` marks a record made against an ARN that stands alone
    # precisely BECAUSE two declarations share its name: the ARN it names and
    # the block the other record names are then different entries, and
    # letting one retire the other loses a decision about a resource the
    # reviewer never revisited.
    return bool(one.get("twinned")) == bool(other.get("twinned"))


def covers(wider: dict, narrower: dict) -> bool:
    """Does `wider` speak about everything `narrower` speaks about?

    Rejection-over-rejection supersession asks this rather than "do these
    overlap" — the other three cases use `overlaps` or `same_scope`, see
    `record_override` — and the asymmetry is the point. An unstated `identifier` or `consumer_kind` is a wildcard —
    "reject orders" means every chain that reaches this entry — so a later
    "reject orders (service_account)" is NARROWER. Retiring the broad decision
    for the narrow one put the rejected consumer back on the entry, silently,
    and left a durable note about a consumer the Terraform never declared.
    A decision is only replaced by one that covers at least as much.
    """
    if not _same_subject(wider, narrower):
        return False
    for field in ("identifier", "consumer_kind"):
        value = wider.get(field) or ""
        if value and value != (narrower.get(field) or ""):
            return False
    return True


def overlaps(one: dict, other: dict) -> bool:
    """Do two consumer records speak about any of the same links?

    Symmetric, and used where the question is "is this decision about that
    consumer at all" — filtering candidates, finding a standing record to
    carry fields forward from, and retiring a decision that contradicts or
    corrects another WITHOUT restoring anything. The other two supersession
    cases are stricter: rejection-over-rejection uses `covers`, and
    attachment-over-rejection uses `same_scope`, because either would
    otherwise put back a consumer the reviewer removed.
    """
    if not _same_subject(one, other):
        return False
    for field in ("identifier", "consumer_kind"):
        values = {one.get(field) or "", other.get(field) or ""}
        if "" not in values and len(values) > 1:
            return False
    return True


def _matched_on_arn(one: dict, other: dict) -> bool:
    """Do these two records meet through an ARN handle?

    An ARN names one AWS resource, so two records sharing one are about one
    entry whatever else they say. A block address is not that: it names one
    block within a root module, and two entries can share it.

    "Meet through an ARN" means one of them was ADDRESSED by it, not merely
    carries it as an alias. Every record made under a folded entry's block
    address carries that entry's ARN, so a shared alias is the ordinary case
    between two block-addressed records and says nothing about identity —
    reading it as identity turned the narrow wildcard below into one that
    retired a sibling's decision.
    """
    if not (_addressed_by_arn(one) or _addressed_by_arn(other)):
        return False
    return _arn_pairs_meet(one, other)


def _same_identifier(one: dict, other: dict) -> bool:
    """Equal identifiers, or one side unstated AND the two met through an
    ARN handle.

    The wildcard is deliberately narrow. `_amend` records an identifier only
    when the entry's BLOCK address has siblings, and an entry no declaration
    has been found for is addressed by its ARN and has no block to have
    siblings — so no record made against it then can carry one, while one
    made after a later scan folds it onto a declaration with siblings does.
    The two legitimately disagree about it. Two records at one
    BLOCK address do not: an unstated identifier there means the address had
    one entry when the record was made, and an `_override.tf` has since
    given it a sibling. Treating those as one decision retires the earlier
    one — which is how a customer's `keep-in-aws` reverts to `migrate`, the
    failure the deleted `_key` was written to prevent. They stay two records;
    the replay reports the one it cannot place.
    """
    a, b = one.get("identifier") or "", other.get("identifier") or ""
    if a == b:
        return True
    return (not a or not b) and _matched_on_arn(one, other)


def same_scope(one: dict, other: dict) -> bool:
    """Do two records name exactly the same link, wildcards included?

    Stricter than `overlaps`: an unstated kind matches only another unstated
    kind. Used where retiring the other record would RESTORE something — an
    attachment retiring a rejection un-removes a consumer — and where doing
    that to a link the new decision did not specifically name is the silent
    failure this object exists to prevent. The identifier IS compared, but
    through `_same_identifier` rather than by equality: `_same_subject`
    cannot settle which entry on its own, since two siblings left by an
    `_override.tf` rename share a block address and a directory and so have
    identical handle pairs — the identifier is what tells them apart, and
    the wildcard is only for a record made under an ARN handle, which
    cannot carry one.
    """
    return (_same_subject(one, other)
            and _same_identifier(one, other)
            and (one.get("consumer_kind") or "")
            == (other.get("consumer_kind") or ""))


def _annotated(record: dict) -> set:
    return {field for field in ANNOTATION_FIELDS if record.get(field)}


def _without(record: dict, fields: set) -> dict:
    return {k: v for k, v in record.items() if k not in fields}


def record_override(document: dict, record: dict, keep_dismissal: bool = False) -> list:
    """Adds `record`, replacing the earlier decisions it supersedes. In place.

    Returns what it superseded — the reviewer is told what their decision
    replaced, because "you already said something about this" is the one thing
    a last-write-wins store can hide from them, and the caller needs it to take
    the superseded text back off the live section.

    Supersession is per DECISION, not per record. An annotation carries up to
    two of them, and the reviewer is told to record the customer's reason and
    the disposition in one call — so an older record is not dropped whole when
    the new one only speaks to one of its fields. It is split: the field the
    new record sets is superseded, and the field it says nothing about survives
    as a record of its own. Dropping it whole is how a disposition change made
    months later deletes the reason the service was kept, which is a lost human
    decision — the failure this object exists to prevent, arriving through the
    machinery built to prevent it.
    """
    overrides = document.setdefault("overrides", [])
    kind = record.get("kind")
    if kind in ENTRY_KINDS:
        # Entry-level decisions replace each other on one entry: a dismissal
        # after a confirmation is a change of mind, an addition after a
        # dismissal un-dismisses. A dismissal also retires the link-level and
        # annotation records on the entry — with the entry gone they could
        # never be placed again, and would be reported as unplaceable on
        # every later scan as if they were still decisions somebody meant.
        def same_entry(o: dict) -> bool:
            # The same rule the annotation branch and the link kinds apply
            # (`_same_subject`): own handles first, the alias channel only
            # when one side is addressed by a literal handle, never between
            # two block addresses — so a decision on prod's block cannot
            # retire one on dev's however their aliases overlap. The
            # workload is left out: an entry-level decision is about the
            # entry, and a dismissal retires the entry's link records too.
            return (_same_identifier(o, record)
                    and _same_subject(dict(o, workload=""), dict(record, workload="")))

        def retires(o: dict) -> bool:
            if not same_entry(o):
                return False
            if o.get("kind") == ADD and kind == CONFIRM:
                # A confirmation on a hand-added entry sets its disposition
                # or note; it does not replace the record that CREATES the
                # entry. Retiring the addition left a confirm with nothing to
                # land on — the entry vanished from the section under a
                # "Confirmed" message. Only a dismissal or a later addition
                # supersedes an addition.
                return False
            if o.get("kind") in ENTRY_KINDS:
                return True
            return kind == DISMISS
        superseded = [o for o in overrides if retires(o)]
        # A consumer named on a confirmation or an addition is a LINK, and
        # links have their own rules: it is stored as an ATTACH record of its
        # own, so a rejection of that workload retires it and it retires a
        # rejection, two confirmations naming two workloads keep both links,
        # and the entry record stays one decision about one entry. Carrying
        # the workload on the entry record instead let a confirm and a
        # reject of one link stand together, and a second confirm replace the
        # first's consumer.
        link = None
        if record.get("workload"):
            # A confirmation's `identifier` is the disambiguator `_amend`
            # recorded for a sibling-bearing block and travels with the
            # link; an addition's is the resource name, which the entry a
            # later scan derives may spell otherwise (a secret's console
            # form), so it does not.
            dropped = ("kind", "disposition", "note", "service", "arn", "region",
                       "engine", "reason") + (("identifier",) if kind != CONFIRM else ())
            link = {k: v for k, v in record.items() if k not in dropped}
            link["kind"] = ATTACH
            record = {k: v for k, v in record.items()
                      if k not in ("workload", "consumer_kind", "namespace", "source_path")}
        if kind == CONFIRM:
            # A repeated confirmation restates one thing — a note, a
            # disposition — not everything: what the earlier one carried
            # survives unless the new one speaks to it, as an ATTACH merges
            # rather than splits. Replacing whole flipped a `keep-in-aws`
            # back to the table default under a success message.
            for old in (o for o in superseded if o.get("kind") == CONFIRM):
                for field in ("disposition", "note"):
                    if record.get(field) is None and old.get(field) is not None:
                        record[field] = old[field]
            if record.get("disposition"):
                # And it takes over an annotation's DISPOSITION on the same
                # entry — one disposition stands per entry, whichever verb
                # set it — leaving the annotation's note as a remnant; and
                # an addition's, which TAKES the new value (the record is
                # the entry), so the store never lists two dispositions.
                remaining = []
                for old in overrides:
                    if (old.get("kind") == ANNOTATE and old.get("disposition")
                            and old not in superseded and same_entry(old)):
                        superseded.append(_without(old, {"note"}))
                        if old.get("note"):
                            remnant = _without(old, {"disposition"})
                            remnant.setdefault("replaced_from", fingerprint(old))
                            remaining.append(remnant)
                        continue
                    if (old.get("kind") == ADD
                            and old.get("disposition") != record["disposition"]
                            and same_entry(old)):
                        superseded.append(dict(_without(old, set(old) - {
                            "kind", "address", "disposition", "author", "recorded_at",
                            "service", "identifier"}), moved_disposition=True))
                        moved = dict(old, disposition=record["disposition"])
                        moved.setdefault("replaced_from", fingerprint(old))
                        remaining.append(moved)
                        continue
                    remaining.append(old)
                overrides[:] = remaining
        overrides[:] = [o for o in overrides if o not in superseded]
        if (kind == DISMISS and not keep_dismissal
                and any(o.get("kind") == ADD for o in superseded)):
            # Dismissing a hand-added entry withdraws the addition; there is
            # nothing for the dismissal itself to keep out — a later scan
            # guessing the same name is a new question — and a standing one
            # printed "0 correction(s) replayed … 1 dismissal(s) already
            # satisfied" on every scan forever. `keep_dismissal` is the one
            # exception: the addition had landed on the scan's own guess,
            # which the rebuild puts straight back as an open question.
            return superseded
        overrides.append(record)
        if link is not None:
            # After the entry record, so the replay creates or confirms the
            # entry before it attaches the link.
            superseded.extend(record_override(document, link))
        return superseded
    if kind != ANNOTATE:
        # A rejection and an attachment of the same workload on the same entry
        # are contradictory standing decisions, so recording one retires the
        # other. Keeping both left the section asserting that one human had
        # confirmed and rejected the same link, with nothing saying which was
        # current — and "I checked with the team and I was wrong" is the
        # ordinary way to arrive there.
        # `.get`, not indexing: a record of a kind this module does not
        # understand still has to be storable, so that `apply` can report it
        # rather than the store refusing it.
        opposite = {REJECT: ATTACH, ATTACH: REJECT}.get(kind)
        # Two different questions, and one predicate cannot answer both.
        #
        # A rejection retires a rejection only if it COVERS it. Its effect is
        # removal, so retiring a broad one for a narrow one un-removes links
        # the reviewer took out, silently.
        #
        # Everything else asks whether the two speak about the same link at
        # all, which is OVERLAPS. An attachment corrects an earlier attachment
        # of the same link (unstated kind then a specific one is a
        # narrowing — "actually it is the service account" — not a second
        # assertion), while two attachments naming DIFFERENT kinds do not
        # overlap and both stand. And a rejection retires the attachment it
        # contradicts, which is what keeps "was confirmed" and "was rejected"
        # from standing on one link at once.
        def retires(o: dict) -> bool:
            o_kind = o.get("kind")
            if o_kind == kind == REJECT:
                # A rejection retires a rejection only if it covers it: both
                # remove, and retiring the broader one un-removes links.
                return covers(record, o)
            if o_kind == REJECT:
                # An ATTACH retiring a REJECT restores a consumer, so it must
                # name exactly the link that was rejected. Under the looser
                # rules, asserting one chain silently un-rejected another —
                # "confirmed: the orders team owns this DB" bringing back an
                # IRSA grant the same reviewer had called over-broad.
                return same_scope(record, o)
            if o_kind == kind or o_kind == opposite:
                # An attachment corrects an attachment of the same link, and a
                # rejection retires the confirmation it contradicts. Neither
                # restores anything, so overlapping is enough.
                return overlaps(o, record)
            return False

        superseded = [o for o in overrides if retires(o)]
        if kind == ATTACH:
            # An attach is one assertion, not two decisions, so it merges
            # rather than splits: a reviewer correcting the namespace restates
            # the namespace, not the reason they gave for the link. Replacing
            # the record whole discarded that reason — from the entry AND from
            # the durable store, with nothing left to withdraw (issue 29).
            for old_record in (o for o in superseded
                               if o.get("kind") == ATTACH):
                for field in ("note", "consumer_kind", "namespace",
                              "source_path"):
                    if (record.get(field) is None
                            and old_record.get(field) is not None):
                        record[field] = old_record[field]
        if kind == REJECT and any(o.get("kind") == ATTACH
                                  or o.get("withdraws_attachment")
                                  for o in superseded):
            # Remembered on the record, because by replay time it is the only
            # place the fact survives: the attach it retired is gone from the
            # document, so the rebuilt entry never had the consumer and the
            # wording cannot be inferred from the section. Carried forward
            # through a REWORDED rejection too — sharpening the reason is the
            # common case, and by the second call there is no ATTACH left to
            # notice, so without this the section reverts to calling a
            # hand-attached consumer derived.
            record["withdraws_attachment"] = True
        overrides[:] = [o for o in overrides if o not in superseded]
        overrides.append(record)
        return superseded

    fields = _annotated(record)
    superseded, kept = [], []

    def same_entry(old: dict) -> bool:
        # The same decision about the same entry: the entry itself
        # (`_same_subject`, by handle pair, which also compares the workload)
        # and the identifier when both state one. This branch runs for
        # ANNOTATE only, whose records carry neither a workload nor a
        # consumer kind — the link-level kinds are handled above, where
        # `covers`/`overlaps`/`same_scope` do compare the consumer kind,
        # because two chains reaching one entry under one workload name are
        # two Kubernetes objects and a decision about the IAM role is not a
        # decision about the release.
        return (old.get("kind") == kind
                and _same_identifier(old, record)
                and _same_subject(old, record))
    for old in overrides:
        if (old.get("kind") in (CONFIRM, ADD)
                and (old.get("disposition") or old.get("kind") == ADD)
                and "disposition" in fields
                and _same_identifier(old, record)
                and _same_subject(old, record)):
            # The reverse of the confirm's take-over: one disposition per
            # entry. The confirmation stands for what else it says (the
            # answer, a note, a consumer); its disposition is retired.
            superseded.append(dict(_without(old, set(old) - {
                "kind", "address", "disposition", "author", "recorded_at", "service",
                "identifier"}), moved_disposition=old.get("kind") == ADD))
            # A confirmation loses the disposition; an addition TAKES the new
            # one, since that record is the entry it re-creates when the fact
            # it landed on leaves the scope. A remnant, not a new decision:
            # `corrections_since_scan` must count the annotation, not this.
            remnant = (dict(old, disposition=record["disposition"]) if old.get("kind") == ADD
                       else _without(old, {"disposition"}))
            remnant.setdefault("replaced_from", fingerprint(old))
            kept.append(remnant)
            continue
        taken = _annotated(old) & fields if same_entry(old) else set()
        if not taken:
            kept.append(old)
            continue
        # What the new record takes over, for the caller to un-apply...
        superseded.append(_without(old, _annotated(old) - taken))
        survives = _annotated(old) - taken
        if survives:
            # ...and what it says nothing about, still standing. It keeps the
            # identity of the record it is the remnant of: nobody decided it,
            # so a scan that replayed the whole record has replayed this half
            # too, and `corrections_since_scan` must not read the split as a
            # new correction.
            remnant = _without(old, taken)
            remnant.setdefault("replaced_from", fingerprint(old))
            kept.append(remnant)
    overrides[:] = kept + [record]
    return superseded


def entry_directory(entry: dict) -> str:
    """The root module an entry was declared in — the last thing that tells
    two same-named blocks apart when even the identifier does not."""
    evidence = (entry.get("evidence") or [""])[0]
    return os.path.dirname(evidence)


def _placed_elsewhere(entry: dict) -> bool:
    """A referenced entry the scan's verdicts put in another account, region
    or partition than the estate's, among several it cannot choose from, or
    answered as the estate's own replica — every entry `datastores._foreign`
    keeps apart from a declaration, read from the same notes."""
    return entry.get("detection") == "referenced" and any(
        n.startswith((datastores.CROSS_ACCOUNT_NOTE_PREFIX,
                      datastores.AMBIGUOUS_ACCOUNT_NOTE_PREFIX,
                      datastores.CROSS_REGION_NOTE_PREFIX,
                      datastores.CROSS_PARTITION_NOTE_PREFIX,
                      datastores.REPLICA_NOTE_PREFIX,
                      datastores.AMBIGUOUS_ENDPOINT_NOTE_PREFIX))
        for n in entry.get("notes") or [])


def definitely_elsewhere(entry: dict) -> bool:
    """A referenced entry the verdicts place with certainty in another
    account, region or partition, or answer as the estate's own replica —
    not one they merely cannot place (`AMBIGUOUS_*`). An addition is created
    beside the former; beside the latter it is refused, since which of the
    ambiguous spellings is the estate's own is exactly what nobody can say
    and a third entry would gate beside them."""
    return entry.get("detection") == "referenced" and any(
        n.startswith((datastores.CROSS_ACCOUNT_NOTE_PREFIX,
                      datastores.CROSS_REGION_NOTE_PREFIX,
                      datastores.CROSS_PARTITION_NOTE_PREFIX,
                      datastores.REPLICA_NOTE_PREFIX))
        for n in entry.get("notes") or [])


def _verdict_phrase(entries: list) -> str:
    """What the refusal may truthfully claim. The cross-account, cross-region
    and cross-partition verdicts say the spelling IS another resource; the
    ambiguous one says only that the files can no longer tell — the flagged
    spelling may well be the very ARN that folded onto the block when the
    decision was made, and "is not the resource" would overclaim there."""
    if all(any(n.startswith((datastores.AMBIGUOUS_ACCOUNT_NOTE_PREFIX,
                             datastores.AMBIGUOUS_ENDPOINT_NOTE_PREFIX))
               for n in e.get("notes") or [])
           and not any(n.startswith((datastores.CROSS_ACCOUNT_NOTE_PREFIX,
                                     datastores.CROSS_REGION_NOTE_PREFIX,
                                     datastores.CROSS_PARTITION_NOTE_PREFIX))
                       for n in e.get("notes") or [])
           for e in entries):
        return ("the scan cannot place in one account or region, which may no "
                "longer be the resource the decision was about")
    return ("the scan places in another account, region or partition, which "
            "is not the resource the decision was about")


def entries_at(inventory: dict, address: str, identifier: str = None,
               directory: str = None, exact: bool = False) -> list:
    """The data_dependencies entries an override's address names.

    `identifier` and `directory` narrow it when one address is declared in two
    root modules — which the merge key allows on purpose, since `envs/dev` and
    `envs/prod` declaring the same block are two resources in two accounts.

    Both are needed. `identifier` alone is not enough: `datastores._identifier`
    falls back to the block address whenever the declaration builds the name
    from variables — `name = "${var.env}-orders"` is the common shape, not the
    exception — so the two entries can carry the same identifier as well as the
    same address, and `directory` is then the only thing that tells them apart.
    """
    entries = inventory.get("data_dependencies") or []
    # By address, or by ARN: an entry known only from its ARN has the ARN as
    # its address, and when a later scan finds its declaration the two fold
    # into one declared entry whose `arn` is that ARN. A correction recorded
    # against the ARN must still find it.
    matched = [e for e in entries
               if e.get("address") == address
               or (address and address in (e.get("arn"), e.get("endpoint")))]
    if not matched and datastores.is_inferred_handle(address):
        # A guess's or a hand-added entry's handle, after a scan found the
        # fact and the merge dropped the guess for it: the record — a
        # confirmation, an annotation, an attachment — lands on the entry
        # that answered it, matched by name as the merge answers a guess
        # (`name_keys`: a secret's console form, an SSM slash). Several
        # entries of that name reach the caller's over-match refusal.
        # `name`, not `identifier`: that is the caller's filter, and rebinding
        # it here compared every candidate against the guess's bare name.
        service, _sep, name = address.partition(":")
        wanted = datastores.name_keys({"service": service, "identifier": name})
        matched = [e for e in entries
                   if not e.get("identifier_is_fallback")
                   and datastores.name_keys(e) & wanted]
    # `exact` is the replay of a record stamped `ambiguous_spelling`: it was
    # made while the entry stood beside another whose spelling agreed with
    # its own, and the note on both promised that a correction against one
    # is never taken for the other. The flag lives on the ENTRY, so the
    # moment the other member leaves the section — a scope amendment takes
    # its file away — the survivor is unflagged and the fallback below would
    # land the record on it: a `keep-in-aws` about a dev-only secret flipped
    # the declared secret's disposition on the next scan and reported it as
    # a success. The record remembers what the entry no longer can.
    if not matched and not exact and _is_arn(address):
        # The handle is one SPELLING of the ARN, and the merge can settle on
        # another when the set of sightings changes — a wildcard-region policy
        # joined by a fully-qualified one, a scope amendment taking the
        # specific file away, a secret's console form reduced to its bare
        # name. Equality alone loses the correction and reports it as a block
        # this scan did not find, about a resource that is in the section.
        matched = [e for e in entries
                   if datastores.arn_spellings_agree(address, e.get("arn"))
                   or datastores.arn_spellings_agree(address, e.get("address"))]
    if identifier:
        matched = [e for e in matched if e.get("identifier") == identifier]
    if directory is not None:
        matched = [e for e in matched if entry_directory(e) == directory]
    return matched


def _family(service: str) -> str:
    return "rds" if service in ("rds", "aurora", "docdb", "neptune") else service


def entry_twins(inventory: dict, service: str, identifier: str) -> list:
    """Every entry naming this resource by service family and name, whatever
    its detection — matched through `datastores.name_keys`, as the merge
    answers a guess (a secret under its console form, an SSM parameter with
    or without its slash), so an addition or a confirmation lands where the
    merge would have folded."""
    wanted = datastores.name_keys({"service": service, "identifier": identifier})
    return [e for e in inventory.get("data_dependencies") or []
            if not e.get("identifier_is_fallback")
            and datastores.name_keys(e) & wanted]


def retire_bridged(document: dict, inventory: dict, target: dict, record: dict) -> list:
    """Retires the standing records made under a guess's `<service>:<identifier>`
    handle that `entries_at` bridges onto exactly `target`, where `record` — a
    new decision made on the fact — decides the same thing. In place; returns
    what it retired, for the reply's "This replaced an earlier decision".

    The bridge is a lookup, not an alias (an alias let prod's decision retire
    dev's), so `record_override` cannot see that a guess-era record and a
    fact-side record are about one entry: both stood, the replay applied the
    later one while the fact was present, and the moment the declaration left
    the scope the fact-side records were unplaced and the guess-era one — a
    dismissal, a rejection — applied again, deleting the guess the reviewer had
    since confirmed. Resolved here, at recording time, where the inventory
    says which entry the guess handle reaches: the same handle that reaches
    two entries retires nothing, which is what keeps dev and prod apart.
    """
    retired, kept = [], []
    for old in document.get("overrides") or []:
        address = old.get("address")
        if (not datastores.is_inferred_handle(address) or address == record.get("address")
                or old is record):
            kept.append(old)
            continue
        # Without the record's own identifier: for a guess handle it is the
        # bare name, and the fact that answered it may spell its identifier
        # otherwise (a secret's console form) — the bridge's tolerance is
        # the point, and the identifier filter defeated it.
        reached = entries_at(inventory, address)
        if len(reached) != 1 or reached[0] is not target:
            kept.append(old)
            continue
        old_kind, new_kind = old.get("kind"), record.get("kind")
        same_workload = (old.get("workload") or "") == (record.get("workload") or "")
        linked = bool(record.get("workload"))
        old_ck, new_ck = old.get("consumer_kind") or "", record.get("consumer_kind") or ""
        # The store's own link rules, restated for a pair that never shares a
        # handle: an attachment retires a rejection only when it names
        # exactly the same chain (`same_scope` — an attach of the release
        # must not un-reject the service account); the other link cases
        # retire whatever they overlap (`overlaps` — an unstated kind is a
        # wildcard there).
        exact_chain = same_workload and old_ck == new_ck
        overlapping = same_workload and (not old_ck or not new_ck or old_ck == new_ck)
        if old_kind == DISMISS:
            conflicts = new_kind != REJECT
        elif new_kind == DISMISS:
            conflicts = True
        elif old_kind == REJECT:
            conflicts = linked and ((new_kind in (ATTACH, CONFIRM, ADD) and exact_chain)
                                    or (new_kind == REJECT and overlapping))
        elif old_kind == ATTACH:
            conflicts = linked and new_kind in (REJECT, ATTACH, CONFIRM, ADD) and overlapping
        else:
            # An entry-level decision: one disposition per entry, whichever
            # verb and whichever handle set it. The note survives as a
            # remnant, as the annotation take-over leaves one — and an
            # addition ALWAYS survives as a remnant, since the record is the
            # entry: dropping it made the hand-added service vanish the
            # moment the fact it had landed on left the scope.
            # An addition ALWAYS carries a disposition in effect — the table
            # default when the reviewer typed none — so the winner moves onto
            # it either way; keyed on the typed field, an addition made
            # without one reverted to the default when the fact left.
            conflicts = ((bool(old.get("disposition")) or old_kind == ADD)
                         and bool(record.get("disposition")))
            if conflicts:
                retired.append(dict(_without(old, {"note"}), moved_disposition=old_kind == ADD))
                if old_kind == ADD:
                    # The record IS the entry: the winning disposition MOVES
                    # onto it, so the entry it re-creates when the fact
                    # leaves the scope carries the reviewer's last word, not
                    # the table default.
                    remnant = dict(old, disposition=record["disposition"])
                    remnant.setdefault("replaced_from", fingerprint(old))
                    kept.append(remnant)
                elif old.get("note"):
                    remnant = _without(old, {"disposition"})
                    remnant.setdefault("replaced_from", fingerprint(old))
                    kept.append(remnant)
                continue
        if conflicts:
            retired.append(old)
        else:
            kept.append(old)
    document["overrides"] = kept
    return retired


def entry_twin(inventory: dict, service: str, identifier: str):
    """The ONE entry `entry_twins` finds, or None — none when there is none,
    and none when there are several: two same-named queues the scan keeps
    apart are not one resource to land a decision on, and the caller says so
    rather than creating a third."""
    matched = entry_twins(inventory, service, identifier)
    return matched[0] if len(matched) == 1 else None


def _attribution(record: dict) -> str:
    who = record.get("author") or "an unnamed reviewer"
    when = (record.get("recorded_at") or "")[:10]
    return f"{who}{f' on {when}' if when else ''}"


def _consumer_from(record: dict) -> dict:
    """The `consumers` item an attach override adds.

    `evidence` names the review rather than a file. The schema asks for a
    repository-relative path and there is none: the whole point of this record
    is that the repository does not state the link. Saying "the data review"
    is the honest answer, and it is the one a reader chasing the fact needs.
    """
    consumer = {
        "workload": record.get("workload"),
        # The stated scope, else what the scan called this workload, else a
        # plain label. The hint never reaches the record's identity — see
        # `from_the_scan`.
        "kind": (record.get("consumer_kind")
                 or record.get("consumer_kind_hint") or "workload"),
        # Stated, else the scan's label for this workload. This consumer is
        # `human_review` either way — the assertion is that the link exists,
        # and these two only describe it — so a hint is safe here in a way it
        # is not on a DERIVED consumer, where `_apply_one` reports the value
        # as something a named person testified to.
        "namespace": record.get("namespace") or record.get("namespace_hint"),
        "source_path": (record.get("source_path")
                        or record.get("source_path_hint")),
        "detection": HUMAN_DETECTION,
        "evidence": f"attached at the data review by {_attribution(record)}",
    }
    if record.get("note"):
        consumer["note"] = record["note"]
    return consumer


def _add_note(entry: dict, note: str) -> None:
    notes = entry.setdefault("notes", [])
    if note not in notes:
        notes.append(note)


def consumer_label(consumer: dict) -> str:
    """How a consumer is named back to the reviewer: '<workload> (<kind>)'.

    The kind because a workload name is not a consumer — the axis every
    refusal in the review step has to name, so that "it lists: orders" cannot
    read as a denial of the `orders` the caller just asked about.
    """
    return (f"{consumer.get('workload')}"
            + (f" ({consumer['kind']})" if consumer.get("kind") else ""))


def _named(workload: str, kind: str) -> str:
    """How the notes name one consumer. `kind` because a workload NAME is not
    a consumer: two chains reach one entry under one name, which is why
    `consumer_kind` is an identity axis here at all. The two
    per-field prefixes below are `_replace_note` handles, so a name that does
    not distinguish the chains makes the second note retire the first."""
    return f"'{workload}'" + (f" ({kind})" if kind else "")


def refused_prefix(field: str, workload: str, kind: str = None) -> str:
    """The note prefix for a detail the reviewer gave that the Terraform
    contradicts. A prefix so `what_happened` can report the refusal from the
    SECTION: an earlier round wrote this note and left the message saying
    nothing else
    changed, which is the half of the silent-overrule bug that survived
    the first attempt at it."""
    return f"the {field} given for {_named(workload, kind)} at the data review"


def supplied_prefix(field: str, workload: str, kind: str = None) -> str:
    """The note prefix for a detail the reviewer supplied, not the Terraform.

    A field filled onto a DERIVED consumer is the one place a human assertion
    ends up on a record that still reads `terraform_wiring`, with an evidence
    file that does not contain it — `HUMAN_DETECTION`'s rule ("a reader looking
    for the evidence file has to be told there is not one") applied to a single
    field rather than a whole consumer. Downgrading the whole consumer to
    `human_review` would be worse: the chain IS real and the evidence IS the
    file. So the entry says which part of it is not.

    A prefix, so re-supplying the field replaces the note instead of stacking a
    second one, and so `what_happened` can report what the section records
    rather than restating its own arguments.
    """
    return (f"the {field} for {_named(workload, kind)} was supplied at the "
            "data review")


def _annotation_note(record: dict) -> str:
    """The entry note an annotation's free text renders to.

    One function so every note is written and matched the same way wherever
    it is compared. Free prose has no prefix to supersede on, so the string
    itself is the
    handle — `_replace_note` cannot match it the way it matches the structured
    notes.
    """
    return f"{record.get('note')} — recorded at the data review by {_attribution(record)}"


def fingerprint(record: dict) -> str:
    """A stable digest of one correction's full content.

    `replaced_from` wins when present. The annotate branch SPLITS a record it
    supersedes, keeping the half the new one says nothing about as a record of
    its own — and that remnant is not a decision anybody made after the scan,
    it is the untouched part of one the scan already replayed. Digesting its
    content would give it a new identity and count it as recorded since.

    The scan keeps the fingerprints of the records it replayed so the review
    can say which corrections it has not seen. Content, not identity: a
    reviewer rewording a reason produces a record with the same key and a
    different meaning, and the scan notes it wrote are stale for exactly that
    reason.
    """
    inherited = record.get("replaced_from")
    if inherited:
        return inherited
    return hashlib.sha1(
        json.dumps(record, sort_keys=True, default=str).encode("utf-8")
    ).hexdigest()[:16]


def corrections_since_scan(document: dict, replayed) -> int:
    """How many standing corrections were recorded after the scan ran.

    Not `len(store) - len(replayed)`. `record_override` RETIRES the records a
    new one supersedes, so the store shrinks as often as it grows: two
    rejections consolidated into a wildcard leaves fewer records than the scan
    replayed, and the subtraction went negative. A reworded reason leaves the
    count identical and the notes just as stale.

    `replayed` that is not a list of fingerprints means the baseline predates
    this field — an older
    server wrote it — in which case nothing is known to have been replayed and
    every standing correction is reported, which is the pre-existing, cautious
    answer.
    """
    if not isinstance(replayed, (list, tuple, set)):
        # None, or a baseline written by the server version that recorded a
        # count here instead of fingerprints.
        return len(document.get("overrides") or [])
    seen = set(replayed)
    return sum(1 for record in (document.get("overrides") or [])
               if fingerprint(record) not in seen)


CORRECTED_SINCE_SCAN = (
    "the counts in the notes above describe the scan as it ran; "
    "correction(s) have been recorded at the review since")


def note_corrections_since_scan(inventory: dict, count: int) -> None:
    """Marks the scan-level notes as describing a superseded section.

    `data_dependency_scan_notes` carries statements like "1 of 2 could not be
    attributed to a workload: orders-exports". A human attaching that consumer
    live does not make the sentence rewrite itself, and the note is
    scan-owned — `run_discovery_extraction` carries it to the assessment as the
    durable record. So the reviewer would read "orders-exports is used by
    orders", "0 still with no consumer" and "1 could not be attributed" in one
    sitting, and the last one would outlive the other two.

    Recomputing the sentences is not possible here — they are free text from
    three different passes over a checkout this step does not read. Saying that
    they are stale is, and it is the honest half.
    """
    notes = inventory.setdefault("data_dependency_scan_notes", [])
    notes[:] = [n for n in notes if not n.startswith(CORRECTED_SINCE_SCAN)]
    notes.append(f"{CORRECTED_SINCE_SCAN}: {count}, listed with the section. "
                 "The next scan replays them and recounts.")


def new_note_log() -> "_Written":
    """A log a caller can hand to `rebuild` to learn what it wrote.

    The class is private because nothing outside this module should construct
    notes; the log is public because the tool layer has a legitimate question
    ("what did the correction I just recorded actually do") that it must not
    answer by reading the notes back.
    """
    return _Written()


def apply(inventory: dict, document: dict, emptied_by_rejection=None,
          derived=None, written_out=None) -> list:
    """Replays the recorded corrections over a freshly harvested section.

    In place, and AFTER the merge, because the addresses a reviewer acted on
    are the ones the merged section showed them. Before `note_unattributed`,
    so that an attached consumer clears the "nothing references this" note and
    a rejection that empties an entry earns it — otherwise the section would
    contradict itself in the same breath.

    Returns scan-level notes, in the same discipline as the rest of the
    harvest: what was applied, and what could not be.
    """
    records = (document or {}).get("overrides") or []
    if not records:
        return []
    applied = {kind: 0 for kind in KINDS}
    # Dismissals whose guess is gone, or that reached a fact: honoured in
    # spirit, nothing removed. Counted apart from `applied[DISMISS]`.
    satisfied = 0
    unplaced = []
    # Which of the reasons below actually occurred. The rollup used to state
    # one cause for all four branches, and half of them had found the block.
    unplaced_absent = False
    # (entry, rejection, links it removed) — checked once every record has
    # been applied, because a later attachment can put one of them back.
    undone = []
    written = written_out if written_out is not None else _Written()
    for record in records:
        kind = record.get("kind")
        address = record.get("address")
        if kind not in KINDS or not address:
            # A record this module does not understand. Reported, never
            # silently skipped: it was a human decision, and one this scan is
            # not honouring.
            unplaced.append(
                f"{address or '(no address)'} (not a correction this server "
                f"understands: kind={kind!r} — the record is malformed)")
            continue
        # A literal handle carries no directory, whatever an older record
        # wrote beside it: a guess's directory was read off `evidence[0]`
        # before the review learned better, and honouring it left the
        # dismissal "satisfied" while the guess stood in the section again.
        directory = None if _is_arn(address) else record.get("directory")
        # Exact handles only for a record made on an entry whose spelling was
        # ambiguous at the time (`_amend` stamps it): see `entries_at`.
        exact = bool(record.get("ambiguous_spelling"))
        targets = entries_at(inventory, address, record.get("identifier"),
                             directory, exact=exact)
        # Under the entry's other handle at the time of recording: a record
        # made on the block address after a fold still applies when the
        # declaration leaves again and the entry is addressed by its ARN, and
        # vice versa. Only when the record's own address names NOTHING in the
        # section, though: an address that still names an entry in another
        # root module is the "what is left is the OTHER environment" case the
        # refusal below exists for, and an alias hop would apply a decision
        # about dev to a resource in prod that nobody reviewed.
        if not targets and not entries_at(inventory, address, record.get("identifier"),
                                          exact=exact):
            for alias_address, alias_directory in sorted(
                    _handle_pairs(record) - {(address, directory or "")},
                    key=lambda pair: (pair[0] or "", pair[1] or "")):
                if not alias_address:
                    continue
                if datastores.is_inferred_handle(alias_address):
                    # The typed guess handle is an alias for SUPERSESSION
                    # only. Followed here it is the name bridge, and a
                    # record made on prod's block landed on any referenced
                    # `orders` — the twin the scan keeps apart included —
                    # once prod's file left the scope. The ARN alias is
                    # different: it exists only because the merge folded
                    # that ARN onto that block.
                    continue
                targets = entries_at(
                    inventory, alias_address, record.get("identifier"),
                    None if _is_arn(alias_address) else alias_directory,
                    exact=exact)
                if targets:
                    break
        if (targets and not _is_arn(address)
                and all(t.get("address") != address for t in targets)):
            # A block record's alias hop never lands on ANOTHER declaration:
            # the ARN that folded onto dev's block now folds onto prod's
            # (`module.orders_queue` in `envs/prod`, or the same address in
            # another root module), and applying dev's decision there is the
            # other-environment failure the directory refusal below exists
            # for. Dropped from the targets, so that refusal speaks.
            targets = [t for t in targets
                       if t.get("detection") not in ("declared", "inferred")
                       or (t.get("address"), entry_directory(t)) == (address, directory or "")]
            # Nor onto a GUESS: a `keep-in-aws` recorded on dev's declared
            # queue must not become the disposition of a bare name a chart
            # happens to state once dev's file leaves — the live tool refuses
            # a disposition on a guess, and the replay may not grant one.
        if (targets and not _is_arn(address)
                and all(t.get("address") != address for t in targets)):
            # Reached through the ARN alias from a BLOCK-address record —
            # the un-fold bridge, for the declaration leaving the scope. Not
            # onto an entry the scan's verdicts place in another account,
            # region or partition, or among several: that entry is by the
            # same verdict not the declared resource the decision was about,
            # however the alias reads. Without this a `keep-in-aws` recorded
            # on dev's secret landed on the finance account's spelling once
            # the provider stated the estate's account and dev's file left,
            # and the data gate published zero gating services. The outcome
            # store refuses the same hop (`datamigration.placed_elsewhere`).
            elsewhere = [t for t in targets if _placed_elsewhere(t)]
            if elsewhere:
                unplaced.append(
                    f"{address} ({kind}, recorded under a declaration this scan "
                    "did not read; its ARN now stands as a referenced entry "
                    + _verdict_phrase(elsewhere) + " — "
                    + ", ".join(sorted(t.get("address") or "" for t in elsewhere))
                    + ")")
                continue
        if (targets and kind != ADD and datastores.is_inferred_handle(address)
                and all(t.get("address") != address for t in targets)):
            # Reached through the NAME bridge from a guess-era record (an
            # addition filters its twins itself and is created beside). Not
            # onto an entry the scan's verdicts place in another account,
            # region or partition, or among several — the finance account's
            # queue is not the resource the reviewer confirmed under the
            # guess, however the name reads — as the block alias hop and the
            # ADD landing refuse it. A dismissal there is satisfied (the file
            # answered the question); anything else is said, not applied.
            elsewhere = [t for t in targets if _placed_elsewhere(t)]
            if elsewhere:
                if kind == DISMISS:
                    satisfied += 1
                    continue
                unplaced.append(
                    f"{address} ({kind}, recorded against a guess; the name now stands as a "
                    "referenced entry " + _verdict_phrase(elsewhere) + " — "
                    + ", ".join(sorted(t.get("address") or "" for t in elsewhere))
                    + ")")
                continue
        if kind == ADD:
            # An addition names no block. It lands on the entry a later scan
            # derived for the same resource, if there is exactly one — the
            # declaration or the literal answers the question better than
            # the assertion did — and otherwise creates the entry.
            twins = (targets if len(targets) == 1
                     else entry_twins(inventory, record.get("service"),
                                      record.get("identifier")))
            # Not onto an entry the scan's verdicts place in another account,
            # region or partition (or among several), and not onto one whose
            # ARN contradicts the ARN the reviewer gave: that is a same-named
            # resource elsewhere, and landing there handed the foreign queue
            # the reviewer's disposition and lost their own. Those twins are
            # not twins; with none left the entry is created as recorded.
            twins = [t for t in twins
                     if not definitely_elsewhere(t)
                     and not (record.get("arn") and t.get("arn")
                              and not datastores.arn_spellings_agree(record["arn"], t["arn"]))]
            if any(_placed_elsewhere(t) for t in twins):
                # What is left is AMBIGUOUS — a spelling the scan could not
                # place in one account or region, alone or among several. The
                # live guard refuses to add beside it; so does the replay,
                # rather than landing the reviewer's entry on a spelling that
                # may not be theirs.
                unplaced.append(
                    f"{address} ({kind}: the name is recorded as a spelling the scan keeps "
                    "apart as ambiguous — "
                    + ", ".join(sorted(t.get("address") or "" for t in twins if _placed_elsewhere(t)))
                    + "; annotate the one you mean instead)")
                continue
            if len(twins) > 1:
                # Several entries of this name stand apart on purpose (two
                # accounts the scan could not reconcile). Creating a third
                # is not the answer and neither is choosing one: said.
                unplaced.append(
                    f"{address} ({kind}: {len(twins)} entries already record this "
                    "name and the scan keeps them apart — "
                    + ", ".join(sorted(t.get("address") or "" for t in twins))
                    + "; annotate the one you mean instead)")
                continue
            if not twins:
                inventory.setdefault("data_dependencies", []).append(
                    _entry_from(record))
            else:
                _confirm(twins[0], record, written)
            applied[kind] += 1
            continue
        if kind == DISMISS and not targets:
            # What it wanted gone is gone — the scan no longer guesses it, or
            # a later scan derived the resource for real and the guess was
            # dropped for it. Satisfied, not unplaced — and counted as
            # satisfied, not as a dismissal the rollup claims happened.
            satisfied += 1
            continue
        if not targets and directory is not None:
            # The root module this decision was recorded against no longer
            # declares the block. Falling back to the address alone cannot
            # tell a rename from "the entry I meant was excluded from the
            # scope, and what is left is the OTHER environment" — both arrive
            # by the same routine route, `amend_discovery_scope`, and guessing
            # wrong applies a decision about dev to a resource in the prod
            # account that nobody reviewed.
            unplaced.append(
                f"{address} ({kind}, recorded against '{directory}', which no "
                "longer declares it)")
            unplaced_absent = True
            continue
        if targets:
            # Reached by SPELLING onto an entry the scan keeps apart as
            # ambiguous: refused, as `datamigration.record_matches` refuses it
            # on the candidate's flag. The record's own stamp covers "flagged
            # when recorded"; this covers "flagged now" — a record made on
            # A(111) while it stood alone, replayed after A left, landed on
            # the still-flagged any-account B beside C and D and reported
            # success. An exact handle is never a spelling match.
            handles = {address} | {a for a, _ in _handle_pairs(record)}
            by_spelling = [t for t in targets
                           if datastores.spelling_is_ambiguous(t)
                           and t.get("address") not in handles
                           and t.get("arn") not in handles]
            if by_spelling:
                unplaced.append(
                    f"{address} ({kind}, matches by spelling only "
                    f"{len(by_spelling)} entr{'y' if len(by_spelling) == 1 else 'ies'} "
                    "the scan keeps apart as ambiguous: "
                    + ", ".join(sorted(t.get("address") or "" for t in by_spelling))
                    + "; a correction against one of them must name its exact "
                    "spelling)")
                continue
        if len(targets) > 1:
            # No directory on the record and the address is declared more than
            # once. Same refusal, same reason.
            unplaced.append(
                f"{address} ({kind}, {len(targets)} entries share it and "
                "the correction does not say which)")
            continue
        if not targets:
            # A stamped record whose exact spelling has gone while the
            # resource is one row away under another: said as what it is,
            # with the spelling to re-record against. The generic wording
            # below blames a deleted or excluded block, which is false here.
            respelled = (entries_at(inventory, address, record.get("identifier"))
                         if exact else [])
            if respelled:
                unplaced.append(
                    f"{address} ({kind}, recorded while this spelling stood beside "
                    "another that agreed with it, so it applies to that exact "
                    "spelling only — which is no longer in the section; the "
                    "resource may now be recorded as "
                    + ", ".join(sorted(e.get("address") or "" for e in respelled))
                    + " — re-record against the spelling the listing prints)")
                continue
            unplaced.append(f"{address} ({kind}, not declared by anything this "
                            "scan read)")
            unplaced_absent = True
            continue
        if kind == DISMISS:
            if targets[0].get("detection") not in ("inferred", HUMAN_DETECTION):
                # Reached a FACT — a later scan declared the name or found
                # its ARN, and the guess handle resolved onto it by name.
                # The dismissal said "this value is not a data service";
                # the file says it is, and the file wins: satisfied, as a
                # dismissal whose guess is gone is, never a deletion of the
                # declaration the ship gate reads. The live tool refuses the
                # same thing.
                satisfied += 1
                continue
            inventory["data_dependencies"] = [
                e for e in inventory.get("data_dependencies") or []
                if e is not targets[0]]
            applied[kind] += 1
            continue
        if kind == CONFIRM:
            _confirm(targets[0], record, written)
            applied[kind] += 1
            continue
        before = _links(targets[0])
        removed_derived = _apply_one(
            targets[0], record, kind,
            (derived or {}).get(id(targets[0])) or [], written)
        if kind == REJECT:
            # Every rejection, not only the ones that removed something. A
            # rejection whose link is created LATER in document order — by an
            # attachment it does not retire, which `same_scope` makes ordinary
            # — removed nothing on its own pass, so the reconciliation below
            # never ran and its withdrawal note stood over a listed consumer.
            undone.append((targets[0], record, before - _links(targets[0])))
        if (emptied_by_rejection is not None and kind == REJECT
                and removed_derived and not targets[0].get("consumers")):
            # Only when a DERIVED link went away. Withdrawing a consumer the
            # reviewer attached themselves empties the entry too, and crediting
            # that to "a chain reached this and a human cut it" states
            # something false about the Terraform AND drops the entry out of
            # every unattributed count — the section, the sign-off, the stamp,
            # and the scan notes the assessment reads — so the one service
            # still needing an owner stops being counted as one.
            #
            # By id: within one rebuild the entry objects are the ones
            # note_unattributed is about to walk, and nothing else identifies
            # an entry uniquely (two root modules share an address AND an
            # identifier when the name is an expression).
            emptied_by_rejection.add(id(targets[0]))
        applied[kind] += 1

    for entry, record, gone in undone:
        # What the section says NOW, in the scope this rejection speaks about
        # — not what this rejection removed. The two differ whenever a later
        # record puts the link back, and the note has to agree with the
        # section rather than with the pass that wrote it.
        workload = record.get("workload")
        scope = record.get("consumer_kind")
        back = {(w, k) for w, k in _links(entry)
                if w == workload and (not scope or k == scope)}
        if not back:
            continue
        # All-or-nothing was wrong in the other direction too: a
        # WILDCARD rejection removes every chain of that workload,
        # and restoring one of them dropped the note explaining why the others
        # are missing. The section then omitted a link the Terraform declares
        # with nothing at all saying why — the corrections object is not
        # carried to the assessment, so that record vanished for every later
        # reader.
        written.drop_removals(entry, workload, scope)
        for gone_workload, gone_kind in sorted(
                gone - back, key=lambda p: (p[0] or "", p[1] or "")):
            prefix = _removal_prefix(gone_workload, gone_kind, False)
            _replace_note(
                written, entry, ("removal", gone_workload, gone_kind),
                f"{prefix} by {_attribution(record)}: "
                f"{record.get('reason') or 'no reason recorded'}")

    notes = []
    total = sum(applied.values())
    if total or satisfied:
        notes.append(
            f"{total} correction(s) recorded at the data review were replayed "
            f"over this scan: {applied[REJECT]} consumer(s) rejected, "
            f"{applied[ATTACH]} attached by hand, {applied[ANNOTATE]} entry "
            "annotation(s)"
            + (f", {applied[CONFIRM]} confirmation(s)" if applied[CONFIRM] else "")
            + (f", {applied[DISMISS]} guess(es) dismissed" if applied[DISMISS] else "")
            + (f", {satisfied} dismissal(s) already satisfied (the guess is gone, or a "
               "later scan found the resource for real)" if satisfied else "")
            + (f", {applied[ADD]} data service(s) added by hand" if applied[ADD] else "")
            + ". They are durable across re-scans; the approval of "
            "the mapping is not, and is asked for again."
        )
    if unplaced:
        notes.append(
            f"{len(unplaced)} correction(s) recorded at an earlier data review "
            "could not be placed on this scan's section, so they were not "
            "applied and are kept as a record only. Each says why below"
            + (" — a block this scan did not find may have been deleted, "
               "moved, or excluded by the confirmed scope, and this scan "
               "cannot tell those apart" if unplaced_absent else "")
            + ": " + ", ".join(sorted(unplaced))
        )
    return notes


def rebuild(scanned: list, document: dict, truncated=(), excluded=(),
            written_out=None) -> tuple:
    """The corrected section, from the scan's own output and the corrections.

    (section, scan-level notes). This is the ONE way the section is produced.
    The review used to edit the live section in place, correction by
    correction, while a re-scan replayed the surviving records over a freshly
    derived one — two different computations that had to be argued into
    agreeing, and four review rounds found four places where they did not. The
    arguments were getting subtler each time (a superseded record moves to the
    end of the document, so the replay takes a branch the live path never
    took), which is the signal that case analysis was the wrong tool.

    Now both paths call this. Equality is by construction rather than by
    inspection, and `scanned` — the section as the Terraform alone describes
    it, kept by the scan — is what makes that possible.
    """
    from .consumers import note_unattributed

    inventory = {"data_dependencies": copy.deepcopy(scanned)}
    # What the Terraform declared, per entry, before any correction ran. An
    # attachment that re-asserts one of these must restore the derived record
    # — its chain and its evidence file — rather than mint a human one.
    derived = {id(entry): original.get("consumers") or []
               for entry, original in zip(inventory["data_dependencies"],
                                          scanned)}
    # A replica entry's consumers are the primary's — copied BEFORE the
    # replay and registered as derived, so a correction about one of them is
    # governed by every placement rule below (directory, alias, kind, the
    # stamps) and an attach restores the derived record rather than minting a
    # human one. Keying a post-replay carry on the records' raw addresses
    # re-implemented placement with a weaker rule and honoured a dev-only
    # rejection on prod. `reconcile_carried` after the replay withdraws a
    # copy whose source the reviewer rejected on the replica.
    for primary_id, copies in datastores.carry_replica_consumers(
            inventory["data_dependencies"]).items():
        derived[primary_id] = list(derived.get(primary_id) or []) + copies
    emptied = set()
    # `written_out`, when a caller passes a `_Written`, comes back holding
    # what this rebuild wrote on each entry — the honest answer to "what did
    # my correction just do", which the tool layer previously reconstructed by
    # reading the notes back.
    notes = apply(inventory, document, emptied, derived, written_out)
    datastores.reconcile_carried(inventory["data_dependencies"])
    notes.extend(note_unattributed(inventory, truncated, excluded, emptied))
    return inventory["data_dependencies"], notes


def _links(entry: dict) -> set:
    return {(c.get("workload"), c.get("kind"))
            for c in entry.get("consumers") or []}


# (opening, closing) of the two sentences that say a link is gone by
# decision, with the consumer's name between them.
_REMOVAL_SHAPES = (
    ("the consumer ", "attached at the data review was withdrawn"),
    ("the derived consumer ", "was rejected at the data review"),
)


class _Written:
    """What structured notes this rebuild has put on which entries.

    Notes are free-text sentences in a list the schema fixes, and a structured
    fact about a link had been recoverable from one only by reading it back:
    first `"phrase" in note`, then `note.startswith(prefix)`, then a parser.
    A reviewer's reason is part of the sentence, so each of those was in turn
    defeated by prose someone plausibly writes — a reason naming another chain
    in parentheses, a reason quoting the section's own sentence, a workload
    with an apostrophe in it.

    There is nothing to recover. `rebuild` copies `scanned`, which has no
    correction notes on it, so every structured note in the entry was written
    by this pass. The handle is a tuple, the note is remembered under it, and
    replacing or dropping one is a dict lookup that cannot be spelled by
    anything a reviewer types.
    """

    def __init__(self):
        self._by_handle = {}

    def handles(self, entry: dict) -> set:
        """The handles this rebuild wrote on `entry`.

        For callers that need to know what a correction DID — the tool
        message, which used to ask by reading the notes back and could be told
        anything a reviewer typed (DESIGN.md issue 32).
        """
        return {handle for (entry_id, handle) in self._by_handle
                if entry_id == id(entry)}

    def replace(self, entry: dict, handle: tuple, note: str) -> None:
        """Adds `note`, removing whatever this handle wrote before."""
        notes = entry.setdefault("notes", [])
        previous = self._by_handle.get((id(entry), handle))
        if previous is not None:
            notes[:] = [n for n in notes if n != previous]
        notes.append(note)
        self._by_handle[(id(entry), handle)] = note

    def drop_removals(self, entry: dict, workload: str, kind) -> None:
        """Takes off the removal notes of a link that is listed again.

        A rejection and an attachment of one link can both stand in the store
        — deliberately, since an attachment may only retire a rejection naming
        exactly the same link, and retiring a broader one would restore chains
        the reviewer never spoke about. But when the attachment re-creates the
        very link the rejection removed, the entry was left listing that
        consumer AND carrying a note saying a named human rejected it: two
        contradictory statements about one link, in the section the assessment
        reads. The section is the fact and the note has to agree with it; both
        decisions stay in the corrections listing, where seeing both is the
        point.

        `kind` of None is a wildcard: the record removed every chain of the
        workload, so it owns every chain's note.
        """
        stale = {}
        for key, note in self._by_handle.items():
            entry_id, handle = key
            if entry_id != id(entry) or handle[0] != "removal":
                continue
            if handle[1] != workload:
                continue
            if kind and handle[2] != kind:
                continue
            stale[key] = note
        if not stale:
            return
        notes = entry.setdefault("notes", [])
        notes[:] = [n for n in notes if n not in set(stale.values())]
        for key in stale:
            del self._by_handle[key]


def _removal_prefix(workload: str, kind: str, asserted: bool) -> str:
    """The sentence saying a link is gone by decision.

    One function so the two shapes are spelled once. What replaces or drops
    one is `_Written`'s handle, never this string.
    """
    opening, closing = _REMOVAL_SHAPES[0 if asserted else 1]
    return f"{opening}{_named(workload, kind)} {closing}"


def _replace_note(written, entry: dict, handle: tuple, note: str) -> None:
    """Adds `note`, dropping any earlier note that opened the same way.

    A rebuild applies every surviving record to a freshly copied section, and
    two records that both speak about one link — a rejection re-issued in
    different words, an annotation replacing an earlier one — would otherwise
    each leave their own note, so the entry would carry a reason its author has
    already withdrawn.
    """
    written.replace(entry, handle, note)


def _apply_one(entry: dict, record: dict, kind: str,
               derived: list = (), written=None) -> bool:
    """Applies one record. Returns whether it removed a DERIVED consumer —
    the distinction `note_unattributed` needs and cannot recover later."""
    if kind == REJECT:
        workload = record.get("workload")
        consumers = entry.get("consumers") or []
        # Scoped to the kind when the record names one. Without that, a
        # rejection aimed at an over-broad IRSA grant also deleted the
        # helm_release attribution that tells the workload data gate which team owns
        # the database — the entry-level machinery refuses that kind of spread
        # and the consumer-level machinery did not.
        wanted_kind = record.get("consumer_kind")

        def names_it(consumer: dict) -> bool:
            return (consumer.get("workload") == workload
                    and (not wanted_kind
                         or consumer.get("kind") == wanted_kind))

        removed = [c for c in consumers if names_it(c)]
        entry["consumers"] = [c for c in consumers if not names_it(c)]
        reason = record.get("reason") or "no reason recorded"
        # "derived" only when something derived it. Reject is the documented
        # undo for an attach, so this fires on a consumer the same reviewer
        # asserted minutes earlier — and calling that one derived puts a false
        # statement about the Terraform into the section the workload data gate
        # reads.
        # "withdrawn" only when nothing derived the link: a reviewer who
        # CONFIRMED a derived consumer and then rejected it has rejected the
        # derivation, and saying they withdrew an attachment would be as wrong
        # in the other direction.
        derived_removed = any(c.get("detection") != HUMAN_DETECTION
                              for c in removed)
        asserted = not derived_removed and (
            record.get("withdraws_attachment")
            or any(c.get("detection") == HUMAN_DETECTION for c in removed))
        prefix = _removal_prefix(workload, wanted_kind, asserted)
        handle = ("removal", workload, wanted_kind)
        # The note is written whether or not this call removed anything, and
        # that is not incidental. A reviewer rewording their reason rejects a
        # consumer the FIRST rejection already took off the entry, so gating
        # the note on removal made the second call a silent no-op: the store
        # held the new reason, the section a reviewer approved still showed the
        # old one, and the tool cheerfully reported the replacement it had not
        # made. On a replay the same rule says something true and useful — the
        # chain no longer derives this consumer, and a human ruled it out.
        _replace_note(written, entry, handle,
                      f"{prefix} by {_attribution(record)}: {reason}")
        return derived_removed
    if kind == ATTACH:
        consumers = entry.setdefault("consumers", [])
        workload = record.get("workload")
        # Replaced, not skipped: a reviewer who re-attaches the same workload
        # is correcting the namespace or the kind, and skipping would show them
        # the old one while the store holds the new.
        asserted_kind = record.get("consumer_kind")

        def names_it(consumer: dict) -> bool:
            # A stated kind narrows; an unstated one means "that workload",
            # which is what a reviewer who did not distinguish two chains
            # meant. The tool refuses the ambiguous case before it gets here.
            return (consumer.get("workload") == workload
                    and (not asserted_kind
                         or consumer.get("kind") == asserted_kind))

        consumers[:] = [c for c in consumers
                        if not (names_it(c)
                                and c.get("detection") == HUMAN_DETECTION)]
        listed = next((c for c in consumers if names_it(c)), None)
        if listed is None:
            # Not listed — but the scan may have derived it and a standing
            # rejection stripped it before this record ran. Re-asserting a link
            # the Terraform declares must restore the DERIVED record, with its
            # chain and its evidence file, not mint a `human_review` one that
            # tells a later gate no evidence exists. `_consumer_from`'s own
            # docstring says the review names itself as evidence precisely
            # because "the repository does not state the link" — here it does.
            # ALL of them, not the first. `merge_datastores` unions two
            # structurally different consumers that share a (workload, kind)
            # — a blue/green pair of releases under one name, deploying
            # different charts from different files — which is why the scan
            # pool keys on `source_path` too. A wildcard
            # rejection removes both and restoring one deleted the other
            # permanently: `_links` is a set of (workload, kind), so `gone`
            # and `back` collapsed the pair to one key and the reconciliation
            # could not see that anything was still missing. The section then
            # lost a link the Terraform declares, with its chart path and its
            # evidence file, and said nothing. A link the section drops
            # without explaining is the failure this whole module exists to
            # prevent, and it has had more disguises than any other.
            #
            # Scoped to the kind of the first match rather than to everything
            # `names_it` accepts: an unqualified attachment still lands on one
            # chain, which is DESIGN.md issue 35 and deliberately unchanged
            # here. What moves together is one chain's consumers.
            from_scan = [c for c in derived or () if names_it(c)]
            if from_scan:
                chain = from_scan[0].get("kind")
                for scanned_consumer in from_scan:
                    if scanned_consumer.get("kind") != chain:
                        continue
                    restored = dict(scanned_consumer)
                    consumers.append(restored)
                    # The one the rest of this branch treats as "the" derived
                    # record: the first, as before.
                    listed = listed or restored
        if listed:
            derived = listed
            # A derived chain reaches what the human asserted — either it
            # always did (they are confirming a link they were shown) or the
            # scope was widened and the wiring came into view. Either way the
            # assertion is confirmation, not an addition: a second entry would
            # list one workload twice in the section the workload data gate reads,
            # and the derived record is the better of the two because it names
            # a file.
            #
            # Their reason still has to land somewhere. It is the whole content
            # of "yes, orders does use that, and here is why", and dropping it
            # on the floor because the consumer happened to be derivable is a
            # lost human decision of exactly the kind this module exists to
            # prevent — quieter than most, because the tool reported success.
            # Fields the derived record leaves empty are filled from what the
            # reviewer stated — `_chart_path` returns None for any registry
            # chart, so `source_path` is routinely absent and routinely the
            # thing they are adding, and the schema calls it the link to a
            # developer's component scope. A value the Terraform DOES state is
            # left alone: that is a fact with evidence behind it. Dropping both
            # silently, while reporting "nothing else changed", was literally
            # true and maximally misleading.
            # Across the WHOLE chain, not just the one `next(...)` picked.
            # `merge_datastores` unions two structurally different consumers
            # sharing a (workload, kind) — a blue/green pair of releases — and
            # deciding from one of them made the same reviewer statement land
            # or be refused according to the order of two `resource` blocks,
            # and wrote a note swearing the Terraform states no namespace for
            # a link declared one line further down. The tool exposes the link
            # at (workload, kind), so that is the granularity the answer has
            # to be given at: if any block of this chain declares the field,
            # evidence stands and the reviewer is told what it says.
            chain = [c for c in consumers
                     if c.get("workload") == workload
                     and c.get("kind") == derived.get("kind")]
            stated_by_terraform = {
                field: sorted({c[field] for c in chain if c.get(field)})
                for field in ("namespace", "source_path")}
            filled = {field: record[field]
                      for field in ("namespace", "source_path")
                      if record.get(field)
                      and not stated_by_terraform[field]}
            # A field the Terraform DOES state is left alone — it is a fact
            # with evidence — but the reviewer said something different, and
            # reporting success while overruling them is the silent
            # overrule this note exists to stop.
            # Their value stays in the store, so if the declaration later stops
            # stating one it fills in; this note is what keeps that from
            # arriving unannounced.
            for field in sorted(
                    f for f in ("namespace", "source_path")
                    if record.get(f) and stated_by_terraform[f]
                    and record[f] not in stated_by_terraform[f]):
                prefix = refused_prefix(field, workload,
                                        derived.get("kind"))
                _replace_note(
                    written, entry,
                    ("refused", field, workload, derived.get("kind")),
                    f"{prefix} "
                    f"({record[field]}) was not applied: the Terraform "
                    f"declares {' and '.join(stated_by_terraform[field])}"
                    ", and a value with evidence "
                    "behind it stands. It remains recorded, and a later scan "
                    f"that finds no declared {field} will use it. Recorded by "
                    f"{_attribution(record)}")
            for field in sorted(filled):
                prefix = supplied_prefix(field, workload,
                                         derived.get("kind"))
                _replace_note(
                    written, entry,
                    ("supplied", field, workload, derived.get("kind")),
                    f"{prefix} by {_attribution(record)}: {filled[field]}. "
                    f"The Terraform does not state a {field} for it, so this "
                    "part of the consumer is a human assertion and "
                    f"{derived.get('evidence') or 'the evidence file'} does "
                    "not carry it.")
            if filled:
                # Onto every consumer of the chain, for the same reason the
                # decision was taken across it: they are one link, and leaving
                # half of them without the value the reviewer supplied is the
                # asymmetry that made the outcome depend on file order.
                #
                # Replaced, not mutated in place. Even with `record()` copying
                # per entry, editing a dict this function does not own is a
                # standing invitation for the same aliasing bug, and the
                # replacement is free.
                for index, existing in enumerate(consumers):
                    if existing.get("workload") != workload or (
                            existing.get("kind") != derived.get("kind")):
                        continue
                    replacement = dict(existing, **filled)
                    consumers[index] = replacement
                    if existing is derived:
                        derived = replacement
            if record.get("note"):
                prefix = (f"the derived consumer '{workload}' "
                          f"({derived.get('kind')}) was confirmed at the data "
                          "review")
                _replace_note(written, entry,
                              ("confirmed", workload, derived.get("kind")),
                              f"{prefix} by {_attribution(record)}: "
                              f"{record['note']}")
            return
        consumers.append(_consumer_from(record))
        return
    if record.get("note"):
        _add_note(entry, _annotation_note(record))
    disposition = record.get("disposition")
    if disposition:
        # No "changed from X": the value it changed from is the scan's grade on
        # a replay and the previous human value on a live second call, so
        # naming it makes the two paths disagree over a detail that records
        # nothing the entry does not already show. What the human decided is
        # the value they chose.
        # A handle, not the sentence: `_add_note` writes the reviewer's own
        # note verbatim three lines above, and one opening "disposition set to
        # keep-in-aws at the customer's request..." was deleted by the
        # disposition it accompanied. Issue 30 makes that sentence the durable
        # record of a commitment nothing else derives.
        _replace_note(
            written, entry, ("disposition",),
            f"disposition set to '{disposition}' at the data review by "
            f"{_attribution(record)}")
        entry["disposition"] = disposition


def _default_disposition(service: str) -> str:
    from .datastores import DISPOSITIONS
    return DISPOSITIONS.get(service, "undecided")


def _confirm(entry: dict, record: dict, written) -> None:
    """A guess (or an added entry's later-derived twin) confirmed by a human.

    The entry stops being a question: its detection becomes `human_review`
    when it was a guess (a declared or referenced entry keeps its own — the
    files vouch for it better than a person does), the guess note goes, and
    it takes the disposition the reviewer gave or, failing that, the table
    default for its service — `undecided` was the mark of an unanswered
    question, and the question has been answered. A consumer named on the
    record is attached as a human_review one.
    """
    from .inferred import GUESS_NOTE_PREFIX
    was_a_guess = entry.get("detection") == "inferred"
    if was_a_guess:
        entry["detection"] = HUMAN_DETECTION
    entry["notes"] = [n for n in entry.get("notes") or []
                      if not n.startswith(GUESS_NOTE_PREFIX)]
    disposition = record.get("disposition")
    # The table default replaces `undecided` only on a GUESS, where it was the
    # mark of the unanswered question. On a declared or referenced entry the
    # scan graded `undecided` on purpose (platform machinery, an unknown
    # engine), and a confirmation that names no disposition is an annotation
    # — it must not turn that grade into `migrate` and gate a component.
    defaulted = False
    if not disposition and was_a_guess and entry.get("disposition") in (None, "undecided"):
        disposition = _default_disposition(entry.get("service"))
        defaulted = True
    if disposition:
        entry["disposition"] = disposition
        # The note says who chose: a default is the table's, not the
        # reviewer's, and the persisted note must not claim a choice they
        # did not make.
        _replace_note(written, entry, ("disposition",),
                      (f"disposition defaulted to '{disposition}' for {entry.get('service')} "
                       f"when {_attribution(record)} confirmed it at the data review")
                      if defaulted else
                      f"disposition set to '{disposition}' at the data review by "
                      f"{_attribution(record)}")
    if record.get("kind") == ADD and was_a_guess:
        # An addition that landed on the scan's own guess of the name: the
        # entry is the reviewer's, as an addition nothing proposed would be,
        # and the listing and the counts should say so (`added_by_hand`).
        entry.setdefault("evidence", []).insert(
            0, f"{ADDED_EVIDENCE_PREFIX} by {_attribution(record)}")
    verb = "added" if record.get("kind") == ADD else "confirmed as a real dependency"
    _replace_note(written, entry, ("confirmed-entry",),
                  f"{verb} at the data review by {_attribution(record)}"
                  + (f": {record['note']}" if record.get("note") else ""))
    # A consumer the reviewer named travels as its own ATTACH record
    # (`record_override` splits it off), so the link-level rules apply to it.


ADDED_EVIDENCE_PREFIX = datastores.ADDED_EVIDENCE_PREFIX


def added_by_hand(entry: dict) -> bool:
    """An entry an `add_entry` record created (`_entry_from`) rather than a
    scan: its evidence names the review, not a file. The replay re-creates
    it from the corrections document on every rebuild, so it is the one kind
    of live entry a scan baseline is entitled not to hold — the review's
    baseline check (`entries_a_rebuild_would_drop`) has to know that, or the
    first correction after an addition refuses over the addition itself."""
    return (entry.get("detection") == HUMAN_DETECTION
            and str((entry.get("evidence") or [""])[0]).startswith(ADDED_EVIDENCE_PREFIX))


def _entry_from(record: dict) -> dict:
    """The entry an `add_entry` record creates when no scan derives it."""
    note_log = _Written()
    entry = {
        "service": record.get("service"),
        "identifier": record.get("identifier"),
        "address": record.get("address"),
        "engine": record.get("engine"),
        "engine_version": None,
        "multi_az": None,
        "allocated_storage": None,
        "storage_type": None,
        "region": record.get("region"),
        # The account the given ARN states, when it states one.
        "account": record.get("account") or (
            (record.get("arn") or "").split(":")[4] if (record.get("arn") or "").count(":") >= 5
            else None) or None,
        "arn": record.get("arn"),
        "detection": HUMAN_DETECTION,
        "declared_in_repo": False,
        "module_source": None,
        "disposition": record.get("disposition")
        or _default_disposition(record.get("service")),
        "identifier_is_fallback": False,
        "evidence": [f"{ADDED_EVIDENCE_PREFIX} by {_attribution(record)}"],
        # The consumer named on the addition arrives as an ATTACH record.
        "consumers": [],
        "notes": [],
    }
    _replace_note(note_log, entry, ("confirmed-entry",),
                  f"added at the data review by {_attribution(record)}"
                  + (f": {record['note']}" if record.get("note") else ""))
    return entry


def summarize(document: dict) -> list:
    """One line per standing correction, for the review listing."""
    lines = []
    for record in (document or {}).get("overrides") or []:
        kind = record.get("kind")
        where = record.get("address")
        # The consumer kind belongs in the line for the reason the identifier
        # does: two standing decisions differing only by which chain they are
        # about otherwise render identically, and this listing is the
        # reviewer's only view of the durable store.
        named = f"'{record.get('workload')}'" + (
            f" ({record['consumer_kind']})" if record.get("consumer_kind") else "")
        if kind == REJECT:
            what = (f"reject consumer {named} "
                    f"({record.get('reason') or 'no reason recorded'})")
        elif kind == ATTACH:
            # With the note: an attach's note is the reason the link is
            # asserted, and this line is what tells a reviewer what their new
            # decision just replaced.
            what = (f"attach consumer {named}"
                    + (f" ({record['note']})" if record.get("note") else ""))
        elif kind == CONFIRM:
            what = ("confirm this is a real data dependency"
                    + (f", disposition {record['disposition']}"
                       if record.get("disposition") else "")
                    + (f", used by {named}" if record.get("workload") else "")
                    + (f" ({record['note']})" if record.get("note") else ""))
        elif kind == DISMISS:
            what = (f"dismiss — not a data dependency "
                    f"({record.get('reason') or 'no reason recorded'})")
        elif kind == ADD and record.get("moved_disposition"):
            # A stub the take-over reports: the addition still stands; only
            # its disposition gave way to the newer decision.
            what = (f"add {record.get('service')} {record.get('identifier')} — its "
                    f"disposition{' ' + repr(record['disposition']) if record.get('disposition') else ' (the default)'} "
                    "gave way to the newer decision; the addition itself stands")
        elif kind == ADD:
            what = (f"add {record.get('service')} {record.get('identifier')}"
                    + (f", disposition {record['disposition']}"
                       if record.get("disposition") else "")
                    + (f", used by {named}" if record.get("workload") else "")
                    + (f" ({record['note']})" if record.get("note") else ""))
        else:
            parts = []
            if record.get("note"):
                parts.append(f"note: {record['note']}")
            if record.get("disposition"):
                parts.append(f"disposition: {record['disposition']}")
            what = ("annotate — " + "; ".join(parts)) if parts else "annotate"
        # With the identifier and the directory when there are any: two
        # entries can share an address, and two ROOT MODULES can share both
        # the address and the identifier (the harvester falls back to the
        # block address for a variable-built name). "This replaced an earlier
        # decision" about a DIFFERENT resource has to read as that rather than
        # as a duplicate of the decision just made — and two opposite
        # dispositions, one per environment, otherwise render as one resource
        # contradicting itself. The identifier and the consumer kind were
        # each this same bug on their own axis; `directory` was the one left.
        if record.get("identifier"):
            where += f" ({record['identifier']})"
        if record.get("directory"):
            where += f" in {record['directory']}"
        lines.append(f"{where}: {what} [{_attribution(record)}]")
    return lines
