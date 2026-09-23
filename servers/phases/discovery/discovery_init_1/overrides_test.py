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

"""The human corrections to the data mapping, and their replay over a re-scan."""

import os
import tempfile
import unittest

from servers.phases.discovery.discovery_init_1 import (
    consumers, datastores, overrides)


def _record(kind, address, **fields):
    return dict({"kind": kind, "address": address,
                 "author": "platform-user@google.com",
                 "recorded_at": "2026-08-27T10:00:00+00:00"}, **fields)


def _document(*records):
    document = overrides.empty_document()
    for record in records:
        overrides.record_override(document, record)
    return document


class RecordingTest(unittest.TestCase):
    """Last-write-wins on
    (address, directory, identifier, workload, consumer_kind, record kind),
    with the two narrowing fields acting as wildcards when unstated."""

    def test_a_second_decision_on_the_same_thing_replaces_the_first(self):
        document = overrides.empty_document()
        first = _record(overrides.REJECT, "module.db", workload="orders",
                        reason="wrong team")
        overrides.record_override(document, first)
        replaced = overrides.record_override(
            document, _record(overrides.REJECT, "module.db", workload="orders",
                              reason="actually a shared job"))

        self.assertTrue(replaced)
        self.assertEqual(len(document["overrides"]), 1)
        self.assertEqual(document["overrides"][0]["reason"],
                         "actually a shared job")

    def test_different_workloads_on_one_entry_are_different_decisions(self):
        document = _document(
            _record(overrides.REJECT, "module.db", workload="orders"),
            _record(overrides.REJECT, "module.db", workload="carts"))
        self.assertEqual(len(document["overrides"]), 2)

    def test_a_rejection_and_an_annotation_do_not_replace_each_other(self):
        # Same address, no workload on the annotation: the kind has to be part
        # of the key or one decision would silently eat the other.
        document = _document(
            _record(overrides.REJECT, "module.db", workload="orders"),
            _record(overrides.ANNOTATE, "module.db", note="ask the DBA"))
        self.assertEqual(len(document["overrides"]), 2)

    def test_a_note_and_a_disposition_on_one_entry_are_two_decisions(self):
        """The reason a service is being kept must not be deleted by the call
        that records the keeping. Both are annotations on the same entry with
        no workload, so a key that stopped at the entry made the second call
        erase the first — losing a human decision, which is the failure this
        object exists to prevent."""
        document = _document(
            _record(overrides.ANNOTATE, "module.ddb",
                    note="customer keeps it: legacy reporting reads it"),
            _record(overrides.ANNOTATE, "module.ddb", disposition="keep-in-aws"))

        self.assertEqual(len(document["overrides"]), 2)
        self.assertTrue(any(o.get("note") for o in document["overrides"]))
        self.assertTrue(any(o.get("disposition") for o in document["overrides"]))

    def test_a_reworded_note_still_replaces_the_one_before_it(self):
        document = _document(
            _record(overrides.ANNOTATE, "module.ddb", note="first thought"),
            _record(overrides.ANNOTATE, "module.ddb", note="second thought"))
        self.assertEqual([o["note"] for o in document["overrides"]],
                         ["second thought"])

    def test_a_later_disposition_does_not_take_the_reason_with_it(self):
        """The instructions tell the reviewer to record the reason and the
        decision in ONE call, so supersession has to work per decision rather
        than per record: months later, a plan change that sets only the
        disposition must not delete why the service was kept. Whether the
        reason survives cannot depend on how the agent batched the arguments."""
        document = _document(
            _record(overrides.ANNOTATE, "module.ddb",
                    note="customer keeps it: analytics reads it from Athena",
                    disposition="keep-in-aws"),
            _record(overrides.ANNOTATE, "module.ddb", disposition="migrate"))

        self.assertEqual(len(document["overrides"]), 2)
        surviving = [o for o in document["overrides"] if o.get("note")]
        self.assertEqual(len(surviving), 1)
        self.assertIn("Athena", surviving[0]["note"])
        # ...and it no longer carries the disposition it was recorded with,
        # which is now somebody else's decision.
        self.assertIsNone(surviving[0].get("disposition"))
        self.assertEqual([o["disposition"] for o in document["overrides"]
                          if o.get("disposition")], ["migrate"])

    def test_the_split_record_still_replays_both_decisions(self):
        document = _document(
            _record(overrides.ANNOTATE, "module.orders_rds",
                    note="keep it", disposition="keep-in-aws"),
            _record(overrides.ANNOTATE, "module.orders_rds",
                    disposition="migrate"))
        inventory = {"data_dependencies": [
            {"service": "rds", "identifier": "orders-db",
             "address": "module.orders_rds", "disposition": "migrate",
             "evidence": ["infra/rds.tf"], "consumers": [], "notes": []}]}
        overrides.apply(inventory, document)

        entry = inventory["data_dependencies"][0]
        self.assertEqual(entry["disposition"], "migrate")
        self.assertTrue(any("keep it" in n for n in entry["notes"]))

    def test_a_call_setting_both_supersedes_both_earlier_decisions(self):
        document = _document(
            _record(overrides.ANNOTATE, "module.ddb", note="old reason"),
            _record(overrides.ANNOTATE, "module.ddb", disposition="migrate"),
            _record(overrides.ANNOTATE, "module.ddb", note="new reason",
                    disposition="keep-in-aws"))
        self.assertEqual(len(document["overrides"]), 1)
        self.assertEqual(document["overrides"][0]["note"], "new reason")

    def test_the_same_address_in_two_directories_stays_two_decisions(self):
        document = _document(
            _record(overrides.REJECT, "module.this", directory="envs/dev",
                    workload="orders"),
            _record(overrides.REJECT, "module.this", directory="envs/prod",
                    workload="orders"))
        self.assertEqual(len(document["overrides"]), 2)


class ApplyTest(unittest.TestCase):
    """Replay over a freshly harvested section."""

    def _inventory(self, **fields):
        entry = dict({
            "service": "rds", "identifier": "orders-db",
            "address": "module.orders_rds", "disposition": "migrate",
            "evidence": ["infra/rds.tf"],
            "consumers": [{"workload": "carts", "kind": "helm_release",
                           "namespace": "carts", "source_path": None,
                           "detection": "terraform_wiring",
                           "evidence": "infra/carts.tf"}],
        }, **fields)
        return {"data_dependencies": [entry]}

    def test_a_rejected_consumer_is_removed_and_the_reason_kept(self):
        inventory = self._inventory()
        notes = overrides.apply(inventory, _document(
            _record(overrides.REJECT, "module.orders_rds", workload="carts",
                    reason="that is the read replica's client")))

        entry = inventory["data_dependencies"][0]
        self.assertEqual(entry["consumers"], [])
        self.assertTrue(any("rejected at the data review" in n
                            and "read replica's client" in n
                            for n in entry["notes"]))
        self.assertTrue(any("1 consumer(s) rejected" in n for n in notes))

    def test_a_rejection_is_recorded_even_when_it_removes_nothing(self):
        """The chain stopped reaching the consumer on its own, so there is
        nothing to take off — but the note is still written. Gating it on the
        removal is what made a reworded rejection a silent no-op in the live
        path, where the first rejection had already emptied the entry; and on a
        replay the note says something true, that a human ruled this consumer
        out."""
        inventory = self._inventory(consumers=[])
        overrides.apply(inventory, _document(
            _record(overrides.REJECT, "module.orders_rds", workload="carts",
                    reason="never touched it")))
        notes = inventory["data_dependencies"][0]["notes"]
        self.assertTrue(any("'carts' was rejected" in n and "never touched it" in n
                            for n in notes))

    def test_an_attached_consumer_says_a_human_attached_it(self):
        inventory = self._inventory(consumers=[])
        overrides.apply(inventory, _document(
            _record(overrides.ATTACH, "module.orders_rds", workload="orders",
                    consumer_kind="helm_release", namespace="orders",
                    source_path="src/orders/chart", note="team confirmed")))

        attached = inventory["data_dependencies"][0]["consumers"][0]
        self.assertEqual(attached["workload"], "orders")
        self.assertEqual(attached["kind"], "helm_release")
        self.assertEqual(attached["source_path"], "src/orders/chart")
        # The detection is the whole point: a gate reading this section must be
        # able to tell an asserted link from a derived one.
        self.assertEqual(attached["detection"], "human_review")
        self.assertIn("platform-user@google.com", attached["evidence"])
        self.assertEqual(attached["note"], "team confirmed")

    def test_attaching_twice_does_not_duplicate_the_consumer(self):
        inventory = self._inventory(consumers=[])
        document = _document(
            _record(overrides.ATTACH, "module.orders_rds", workload="orders"))
        overrides.apply(inventory, document)
        overrides.apply(inventory, document)
        self.assertEqual(len(inventory["data_dependencies"][0]["consumers"]), 1)

    def test_an_annotation_records_the_note_and_the_disposition_change(self):
        inventory = self._inventory()
        overrides.apply(inventory, _document(
            _record(overrides.ANNOTATE, "module.orders_rds",
                    note="stays until the reporting rewrite ships",
                    disposition="keep-in-aws")))

        entry = inventory["data_dependencies"][0]
        self.assertEqual(entry["disposition"], "keep-in-aws")
        self.assertTrue(any("reporting rewrite" in n for n in entry["notes"]))
        # That a human chose it is recorded, not just the value. Deliberately
        # WITHOUT the value it changed from: on a replay that is the scan's
        # grade and on a live second call it is the previous human value, so
        # naming it made the two paths disagree over a detail the entry
        # already shows.
        self.assertTrue(any("disposition set to 'keep-in-aws' at the data "
                            "review" in n for n in entry["notes"]))

    def test_an_override_for_a_block_that_is_gone_is_reported_not_dropped(self):
        inventory = self._inventory()
        document = _document(
            _record(overrides.REJECT, "module.deleted", workload="orders"))
        notes = overrides.apply(inventory, document)

        self.assertTrue(any("module.deleted" in n and "not applied" in n
                            for n in notes))
        # Kept: the block may be gone because the scope changed rather than
        # because the resource did, and this cannot tell those apart.
        self.assertEqual(len(document["overrides"]), 1)

    def test_a_directory_narrows_an_address_two_root_modules_share(self):
        inventory = {"data_dependencies": [
            {"service": "sqs", "identifier": "orders", "address": "module.q",
             "evidence": ["envs/dev/main.tf"], "consumers": []},
            {"service": "sqs", "identifier": "orders", "address": "module.q",
             "evidence": ["envs/prod/main.tf"], "consumers": []},
        ]}
        overrides.apply(inventory, _document(
            _record(overrides.ATTACH, "module.q", directory="envs/prod",
                    workload="orders")))

        dev, prod = inventory["data_dependencies"]
        self.assertEqual(dev["consumers"], [])
        self.assertEqual(prod["consumers"][0]["workload"], "orders")

    def test_a_correction_whose_root_module_is_gone_is_reported_not_re_aimed(self):
        """The directory gates, and it has to. Falling back to the address
        alone kept a moved file's correction alive — but it could not tell a
        rename from "the environment I corrected was excluded from the scope
        and what is left is the other one", and both arrive by the same routine
        route. Guessing wrong strips a consumer from a resource in another
        account that nobody reviewed."""
        inventory = self._inventory(evidence=["platform/rds.tf"])
        notes = overrides.apply(inventory, _document(
            _record(overrides.REJECT, "module.orders_rds", directory="infra",
                    workload="carts")))

        # Untouched, and the reviewer is told why rather than left to notice.
        self.assertEqual(
            [c["workload"] for c in inventory["data_dependencies"][0]["consumers"]],
            ["carts"])
        self.assertTrue(any("no longer declares it" in n for n in notes))

    def test_an_address_declared_twice_is_not_corrected_at_all(self):
        """A record with no directory (nothing narrowed it) against an address
        two root modules declare. Applying to both is the wrong-team failure;
        this refuses and reports."""
        inventory = {"data_dependencies": [
            {"service": "sqs", "identifier": "orders", "address": "module.q",
             "evidence": ["envs/dev/main.tf"],
             "consumers": [{"workload": "orders"}], "notes": []},
            {"service": "sqs", "identifier": "orders", "address": "module.q",
             "evidence": ["envs/prod/main.tf"],
             "consumers": [{"workload": "orders"}], "notes": []},
        ]}
        notes = overrides.apply(inventory, _document(
            _record(overrides.REJECT, "module.q", workload="orders")))

        for entry in inventory["data_dependencies"]:
            self.assertEqual([c["workload"] for c in entry["consumers"]], ["orders"])
        self.assertTrue(any("2 entries share it" in n for n in notes))

    def test_a_record_of_an_unknown_kind_is_reported_not_ignored(self):
        inventory = self._inventory()
        notes = overrides.apply(inventory, _document(
            _record("delete_everything", "module.orders_rds")))
        self.assertTrue(any("not applied" in n for n in notes))


class RebuildTest(unittest.TestCase):
    """`rebuild` is the only way the section is produced.

    The review used to edit the corrected section in place while a re-scan
    replayed the surviving records over a freshly derived one. Four review
    rounds found four ways those two computations disagreed — a reworded
    annotation, a reworded rejection, an attach the chain later derived, and a
    superseded record moving to the end of the document and changing which
    branch a later one took. There is now one computation, so these are tests
    of what it produces rather than of whether two things match.
    """

    def _scanned(self, **fields):
        return [dict({"service": "rds", "identifier": "orders-db",
                      "address": "m.x", "disposition": "migrate",
                      "evidence": ["infra/rds.tf"], "notes": [],
                      "consumers": [{"workload": "orders",
                                     "kind": "helm_release", "namespace": None,
                                     "source_path": None,
                                     "detection": "terraform_wiring",
                                     "evidence": "infra/app.tf"}]}, **fields)]

    def _rebuilt(self, scanned, *records, **kwargs):
        section, _ = overrides.rebuild(scanned, _document(*records), **kwargs)
        return section[0]

    def test_a_reworded_annotation_leaves_one_note(self):
        entry = self._rebuilt(
            self._scanned(),
            _record(overrides.ANNOTATE, "m.x", note="first thought"),
            _record(overrides.ANNOTATE, "m.x", note="second thought"))
        annotations = [n for n in entry["notes"] if "thought" in n]
        self.assertEqual(len(annotations), 1)
        self.assertIn("second thought", annotations[0])

    def test_a_second_disposition_leaves_one_statement_of_it(self):
        entry = self._rebuilt(
            self._scanned(),
            _record(overrides.ANNOTATE, "m.x", disposition="keep-in-aws"),
            _record(overrides.ANNOTATE, "m.x", disposition="rebuild"))
        self.assertEqual(entry["disposition"], "rebuild")
        self.assertEqual(len([n for n in entry["notes"]
                              if "disposition set to" in n]), 1)

    def test_a_reworded_rejection_carries_only_the_current_reason(self):
        entry = self._rebuilt(
            self._scanned(),
            _record(overrides.REJECT, "m.x", workload="orders",
                    reason="it reads the replica"),
            _record(overrides.REJECT, "m.x", workload="orders",
                    reason="it never touches this at all"))
        self.assertEqual(entry["consumers"], [])
        self.assertTrue(any("never touches this" in n for n in entry["notes"]))
        self.assertFalse(any("reads the replica" in n for n in entry["notes"]))

    def test_a_rejection_after_an_attach_does_not_leave_both_standing(self):
        """The ordinary "I checked with the team and I was wrong" sequence. It
        used to leave the entry asserting that one human both confirmed and
        rejected the same link, with nothing saying which was current."""
        entry = self._rebuilt(
            self._scanned(),
            _record(overrides.ATTACH, "m.x", workload="orders",
                    note="the team says yes"),
            _record(overrides.REJECT, "m.x", workload="orders",
                    reason="wrong: that was a different release"))

        self.assertEqual(entry["consumers"], [])
        self.assertFalse(any("was confirmed at the data review" in n
                             for n in entry["notes"]))
        # The link WAS derived, and confirming it did not change that — so the
        # rejection is of the derivation, not the withdrawal of an assertion.
        self.assertTrue(any("the derived consumer 'orders' was rejected" in n
                            for n in entry["notes"]))

    def test_a_withdrawal_note_does_not_outlive_the_link_coming_back(self):
        """The reconciliation used to run only for rejections that removed
        something on their own pass. A rejection whose link is created LATER
        in document order removed nothing, so its withdrawal note stood over a
        consumer the section lists — two contradictory statements about one
        link, durable across a re-scan. attach-over-reject is `same_scope`, so
        a wildcard rejection surviving a narrow re-attachment is by design;
        what has to give is the note."""
        scanned = self._scanned(consumers=[])
        entry = self._rebuilt(
            scanned,
            _record(overrides.ATTACH, "m.x", workload="orders",
                    consumer_kind="service_account",
                    note="the orders IRSA role writes the exports"),
            _record(overrides.REJECT, "m.x", workload="orders",
                    reason="wrong bucket, that is the catalog team"),
            _record(overrides.ATTACH, "m.x", workload="orders",
                    consumer_kind="service_account",
                    note="checked with the team: it really is orders"))

        self.assertEqual([c["workload"] for c in entry["consumers"]], ["orders"])
        self.assertFalse(
            any("withdrawn" in n or "was rejected at the data review" in n
                for n in entry["notes"]),
            entry["notes"])

    def test_a_wildcard_rejection_still_explains_the_chain_that_stays_cut(self):
        """The other half of the same rule, now that the pass runs for
        every rejection: restoring one chain must not erase the record of why
        the other is missing."""
        scanned = self._scanned(consumers=[
            {"workload": "orders", "kind": "helm_release", "namespace": None,
             "source_path": None, "detection": "terraform_wiring",
             "evidence": "data.tf"},
            {"workload": "orders", "kind": "service_account",
             "namespace": "orders", "source_path": None, "detection": "irsa",
             "evidence": "data.tf"}])
        entry = self._rebuilt(
            scanned,
            _record(overrides.REJECT, "m.x", workload="orders",
                    reason="nothing in orders touches this bucket"),
            _record(overrides.ATTACH, "m.x", workload="orders",
                    consumer_kind="helm_release",
                    note="I was wrong about the release"))

        self.assertEqual([c["kind"] for c in entry["consumers"]],
                         ["helm_release"])
        self.assertTrue(any(
            "the derived consumer 'orders' (service_account) was rejected"
            in n for n in entry["notes"]), entry["notes"])
        self.assertFalse(any(
            "the derived consumer 'orders' (helm_release) was rejected" in n
            for n in entry["notes"]), entry["notes"])

    def test_the_untouched_half_of_a_split_record_is_not_a_new_decision(self):
        """`record_override` splits a record it partly supersedes, keeping the
        half the new one says nothing about. That remnant is not something
        anybody decided after the scan, so it must keep the identity of the
        record the scan replayed — otherwise `corrections_since_scan` counts
        one call as two."""
        document = overrides.empty_document()
        overrides.record_override(document, _record(
            overrides.ANNOTATE, "m.x", note="customer keeps it",
            disposition="keep-in-aws"))
        replayed = [overrides.fingerprint(r) for r in document["overrides"]]

        overrides.record_override(document, _record(
            overrides.ANNOTATE, "m.x", disposition="migrate"))

        self.assertEqual(len(document["overrides"]), 2)
        self.assertEqual(
            1, overrides.corrections_since_scan(document, replayed))

    def _two_chains(self, ns_a=None, ns_b=None):
        """One entry two chains reach under one workload name — the shape
        `consumer_kind` exists for."""
        return self._scanned(consumers=[
            {"workload": "carts", "kind": "helm_release", "namespace": ns_a,
             "source_path": None, "detection": "terraform_wiring",
             "evidence": "data.tf"},
            {"workload": "carts", "kind": "kubernetes_secret", "namespace": ns_b,
             "source_path": None, "detection": "terraform_wiring",
             "evidence": "data.tf"}])

    def test_a_refusal_on_one_chain_does_not_erase_the_other_chains(self):
        """The prefixes are `_replace_note` handles. Keyed on the workload
        NAME, the second chain's refusal retired the first one's — and that
        note is the section's only record of it, since the corrections object
        is not carried to the assessment. Its own text promises a later scan
        will adopt the value, so erasing it is erasing the warning."""
        entry = self._rebuilt(
            self._two_chains(ns_a="carts-a", ns_b="carts-b"),
            _record(overrides.ATTACH, "m.x", workload="carts",
                    consumer_kind="helm_release", namespace="analytics"),
            _record(overrides.ATTACH, "m.x", workload="carts",
                    consumer_kind="kubernetes_secret", namespace="vault"))

        refusals = [n for n in entry["notes"] if "was not applied" in n]
        self.assertEqual(len(refusals), 2, refusals)
        self.assertTrue(any("analytics" in n and "carts-a" in n
                            for n in refusals), refusals)
        self.assertTrue(any("vault" in n and "carts-b" in n
                            for n in refusals), refusals)

    def test_a_supplied_detail_on_one_chain_does_not_erase_the_other_chains(self):
        """Two `terraform_wiring` consumers whose evidence file carries
        neither value; both have to say so."""
        entry = self._rebuilt(
            self._two_chains(),
            _record(overrides.ATTACH, "m.x", workload="carts",
                    consumer_kind="helm_release", namespace="from-the-release"),
            _record(overrides.ATTACH, "m.x", workload="carts",
                    consumer_kind="kubernetes_secret",
                    namespace="from-the-secret"))

        self.assertEqual(
            {(c["kind"], c["namespace"]) for c in entry["consumers"]},
            {("helm_release", "from-the-release"),
             ("kubernetes_secret", "from-the-secret")})
        for kind, value in (("helm_release", "from-the-release"),
                            ("kubernetes_secret", "from-the-secret")):
            note = next(
                (n for n in entry["notes"]
                 if n.startswith(overrides.supplied_prefix(
                     "namespace", "carts", kind))), None)
            self.assertIsNotNone(note, entry["notes"])
            self.assertIn(value, note)

    def _blue_green(self):
        """Two releases of one name deploying different charts from different
        files — `merge_datastores` unions them onto one entry, so the section
        carries two consumers sharing a (workload, kind)."""
        return self._scanned(consumers=[
            {"workload": "orders", "kind": "helm_release",
             "namespace": "orders-blue", "source_path": "charts/orders",
             "detection": "terraform_wiring", "evidence": "main.tf"},
            {"workload": "orders", "kind": "helm_release",
             "namespace": "orders-green", "source_path": "charts/orders-next",
             "detection": "terraform_wiring", "evidence": "green.tf"}])

    def _mixed_blue_green(self, blue_first=True):
        """A blue/green pair where only ONE member declares a namespace —
        the shape that made the outcome depend on file order."""
        blue = {"workload": "orders", "kind": "helm_release",
                "namespace": None, "source_path": "charts/orders",
                "detection": "terraform_wiring", "evidence": "data.tf"}
        green = {"workload": "orders", "kind": "helm_release",
                 "namespace": "orders-green",
                 "source_path": "charts/orders-next",
                 "detection": "terraform_wiring", "evidence": "data.tf"}
        return self._scanned(
            consumers=[blue, green] if blue_first else [green, blue])

    def test_a_supplied_field_is_decided_by_the_chain_not_by_file_order(self):
        """The Terraform declares a namespace for this (workload, kind) — in
        one of the two blocks that share it. Deciding from whichever the walk
        read first meant the same reviewer statement was applied or refused
        according to the order of two `resource` blocks, and the supplied note
        swore the Terraform states no namespace for a link declared one line
        further down."""
        for blue_first in (True, False):
            entry = self._rebuilt(
                self._mixed_blue_green(blue_first),
                _record(overrides.ATTACH, "m.x", workload="orders",
                        namespace="orders-staging",
                        note="the platform team confirms orders owns it"))

            with self.subTest(blue_first=blue_first):
                # Evidence stands, both ways round.
                self.assertEqual(
                    {c["namespace"] for c in entry["consumers"]},
                    {None, "orders-green"})
                refusal = next(
                    (n for n in entry["notes"] if "was not applied" in n), None)
                self.assertIsNotNone(refusal, entry["notes"])
                self.assertIn("orders-staging", refusal)
                self.assertIn("declares orders-green", refusal)
                # And nothing claims the Terraform is silent about it.
                self.assertFalse(
                    any(n.startswith(overrides.supplied_prefix(
                        "namespace", "orders", "helm_release"))
                        for n in entry["notes"]), entry["notes"])

    def test_a_supplied_field_lands_on_every_consumer_of_the_chain(self):
        """When no block of the chain declares it, the reviewer's value is
        theirs to give — to the whole link, not half of it."""
        neither = self._scanned(consumers=[
            dict(c, namespace=None) for c in self._blue_green()[0]["consumers"]])
        entry = self._rebuilt(
            neither,
            _record(overrides.ATTACH, "m.x", workload="orders",
                    namespace="orders-staging"))

        self.assertEqual({c["namespace"] for c in entry["consumers"]},
                         {"orders-staging"})
        # The chart paths, which differ per block, are untouched.
        self.assertEqual({c["source_path"] for c in entry["consumers"]},
                         {"charts/orders", "charts/orders-next"})

    def test_a_reason_quoting_the_removal_sentence_does_not_survive_it(self):
        """The parser was greedy on the right so a kind containing ')' would
        round-trip, which let a reason containing ") was rejected at the data
        review" backtrack into the kind slot. Nothing parses a note now: the
        pass remembers what it wrote."""
        scanned = self._scanned(consumers=[
            {"workload": "orders", "kind": "helm_release", "namespace": None,
             "source_path": None, "detection": "terraform_wiring",
             "evidence": "data.tf"}])
        entry = self._rebuilt(
            scanned,
            _record(overrides.REJECT, "m.x", workload="orders",
                    consumer_kind="helm_release",
                    reason="as bob noted, 'orders' (helm_release) was "
                           "rejected at the data review in July; I agree"),
            _record(overrides.ATTACH, "m.x", workload="orders",
                    note="the team says it does use it after all"))

        self.assertEqual([c["workload"] for c in entry["consumers"]],
                         ["orders"])
        self.assertFalse(
            any("was rejected at the data review by" in n
                for n in entry["notes"]), entry["notes"])
        self.assertTrue(
            any("does use it after all" in n for n in entry["notes"]),
            entry["notes"])

    def test_re_attaching_restores_every_consumer_of_the_chain(self):
        """The restore path took `next(...)` — one consumer — while a wildcard
        rejection had removed both. `_links` is a set of (workload, kind), so
        the reconciliation collapsed the pair to one key, saw the link back
        and re-issued nothing: the second consumer's namespace, chart path and
        evidence file were gone from the section permanently, with nothing
        saying why, and every later scan reproduced the loss."""
        entry = self._rebuilt(
            self._blue_green(),
            _record(overrides.REJECT, "m.x", workload="orders",
                    reason="the platform team said this DB is not theirs"),
            _record(overrides.ATTACH, "m.x", workload="orders",
                    consumer_kind="helm_release",
                    note="I was wrong, the orders release does use it"))

        self.assertEqual(
            {(c["namespace"], c["source_path"], c["evidence"])
             for c in entry["consumers"]},
            {("orders-blue", "charts/orders", "main.tf"),
             ("orders-green", "charts/orders-next", "green.tf")})
        self.assertTrue(all(c["detection"] == "terraform_wiring"
                            for c in entry["consumers"]), entry["consumers"])

    def test_a_workload_named_with_an_apostrophe_still_reconciles(self):
        """`workload` is free text from the tool — "as the user names it" —
        and `_named` does not escape it, so a name containing an apostrophe
        closed the quoted slot early and the note became unparseable. The
        entry then listed the consumer and said a named human had withdrawn
        it, in the same breath."""
        entry = self._rebuilt(
            self._scanned(consumers=[]),
            _record(overrides.ATTACH, "m.x", workload="o'brien-api",
                    consumer_kind="helm_release",
                    note="the team confirmed it"),
            _record(overrides.REJECT, "m.x", workload="o'brien-api",
                    reason="wrong service, my mistake"),
            _record(overrides.ATTACH, "m.x", workload="o'brien-api",
                    consumer_kind="helm_release",
                    note="no, it really is this one"))

        self.assertEqual([c["workload"] for c in entry["consumers"]],
                         ["o'brien-api"])
        self.assertFalse(any("was withdrawn" in n for n in entry["notes"]),
                         entry["notes"])

    def test_a_wildcard_rejection_does_not_erase_a_confirmation(self):
        """The wildcard twin of the kind-scoped case above. The wildcard
        branch matched two
        openings and then searched the WHOLE note for one of two phrases —
        and every note this module writes about a consumer of that workload
        opens with one of those openings, including the confirmation. An
        attach whose reason quotes the phrase lost the reason it was written
        for, while the tool reported "your reason is now on the entry"."""
        scanned = self._scanned(consumers=[
            {"workload": "orders", "kind": "helm_release", "namespace": None,
             "source_path": None, "detection": "terraform_wiring",
             "evidence": "data.tf"}])
        entry = self._rebuilt(
            scanned,
            _record(overrides.REJECT, "m.x", workload="orders",
                    reason="wrong team"),
            _record(overrides.ATTACH, "m.x", workload="orders",
                    consumer_kind="helm_release",
                    note="the service_account grant was rejected at the data "
                         "review in error; the release really does use this"))

        self.assertEqual([c["kind"] for c in entry["consumers"]],
                         ["helm_release"])
        self.assertTrue(any("really does use this" in n
                            for n in entry["notes"]), entry["notes"])
        # ...and the rejection note it contradicts is still gone.
        self.assertFalse(any("was rejected at the data review by" in n
                             for n in entry["notes"]), entry["notes"])

    def test_a_wildcard_rejection_does_not_erase_an_annotation(self):
        """Same branch, reached through `_annotation_note`, which renders the
        reviewer's words verbatim — so an annotation quoting the section's own
        sentence opens with the handle too."""
        scanned = self._scanned(consumers=[
            {"workload": "orders", "kind": "helm_release", "namespace": None,
             "source_path": None, "detection": "terraform_wiring",
             "evidence": "data.tf"}])
        entry = self._rebuilt(
            scanned,
            _record(overrides.ANNOTATE, "m.x",
                    note="the consumer 'orders' was rejected at the data "
                         "review, but finance still queries it nightly"),
            _record(overrides.REJECT, "m.x", workload="orders",
                    reason="wrong team"),
            _record(overrides.ATTACH, "m.x", workload="orders",
                    consumer_kind="helm_release", note="it does use it"))

        self.assertTrue(any("finance still queries it nightly" in n
                            for n in entry["notes"]), entry["notes"])

    def test_an_annotation_is_not_eaten_by_the_disposition_beside_it(self):
        """`"disposition set to "` was a `_replace_note` handle — free-standing
        prose with no name slot — so a reason opening with those words was
        deleted by the disposition it accompanied, inside one call, with
        nothing reporting a replacement. Issue 30 makes that reason the
        durable record of a commitment nothing else derives."""
        entry = self._rebuilt(
            self._scanned(consumers=[]),
            _record(overrides.ANNOTATE, "m.x",
                    note="disposition set to keep-in-aws at the customer's "
                         "request: the analytics pipeline reads it directly",
                    disposition="keep-in-aws"))

        self.assertEqual(entry["disposition"], "keep-in-aws")
        self.assertTrue(any("analytics pipeline reads it directly" in n
                            for n in entry["notes"]), entry["notes"])
        self.assertTrue(any(n.startswith("disposition set to 'keep-in-aws'")
                            for n in entry["notes"]), entry["notes"])

    def test_a_re_set_disposition_still_replaces_its_own_note(self):
        """The handle had to get narrower without getting useless."""
        entry = self._rebuilt(
            self._scanned(consumers=[]),
            _record(overrides.ANNOTATE, "m.x", disposition="keep-in-aws"),
            _record(overrides.ANNOTATE, "m.x", disposition="migrate"))

        marks = [n for n in entry["notes"] if n.startswith("disposition set")]
        self.assertEqual(len(marks), 1, marks)
        self.assertIn("'migrate'", marks[0])

    def test_a_reason_naming_the_other_chain_does_not_erase_its_note(self):
        """The kind test used to be a substring search over the whole note,
        which ends in the reviewer's free-text reason. "(helm_release)" in
        that reason is the ordinary way to say which chain you mean when two
        reach one entry — and it made one reconciliation delete the other
        chain's note, leaving the section without a link the Terraform
        declares and nothing saying why — the same loss the per-link
        re-issue exists to prevent, reached another way."""
        scanned = self._scanned(consumers=[
            {"workload": "carts", "kind": "helm_release", "namespace": None,
             "source_path": None, "detection": "terraform_wiring",
             "evidence": "data.tf"},
            {"workload": "carts", "kind": "service_account",
             "namespace": "carts-ns", "source_path": None,
             "detection": "irsa", "evidence": "data.tf"}])
        entry = self._rebuilt(
            scanned,
            _record(overrides.REJECT, "m.x", workload="carts",
                    consumer_kind="helm_release",
                    reason="the release only reads a replica"),
            _record(overrides.REJECT, "m.x", workload="carts",
                    consumer_kind="service_account",
                    reason="the (helm_release) rejection already covers this"),
            # Unqualified, and legal: after the two rejections the entry
            # lists no `carts` consumer, so the ambiguity refusal cannot fire.
            # It restores one link without retiring either rejection —
            # `same_scope` — so the helm_release record's reconciliation runs
            # with the other chain's note sitting on the entry.
            _record(overrides.ATTACH, "m.x", workload="carts",
                    note="the platform team confirms carts uses it"))

        self.assertEqual([c["kind"] for c in entry["consumers"]],
                         ["helm_release"])
        # The chain that is still cut is still explained.
        self.assertTrue(any(
            "the derived consumer 'carts' (service_account) was rejected" in n
            for n in entry["notes"]), entry["notes"])
        # ...and the one that came back is not.
        self.assertFalse(any(
            "the derived consumer 'carts' (helm_release) was rejected" in n
            for n in entry["notes"]), entry["notes"])

    def test_the_standing_listing_separates_two_root_modules(self):
        """`envs/dev` and `envs/prod` both declaring one address, with the
        identifier falling back to the block address so it separates nothing
        either. Without the directory the two lines render as one resource
        contradicting itself, and this listing is the reviewer's only view of
        the durable store. The identifier and the consumer kind were each
        this same bug on their own axis."""
        lines = overrides.summarize(_document(
            _record(overrides.ANNOTATE, "aws_sqs_queue.orders",
                    directory="envs/dev", note="dev stays put",
                    disposition="keep-in-aws"),
            _record(overrides.ANNOTATE, "aws_sqs_queue.orders",
                    directory="envs/prod", note="prod moves",
                    disposition="migrate")))

        self.assertEqual(len(lines), 2)
        self.assertEqual(len(set(lines)), 2, lines)
        self.assertTrue(any("in envs/dev" in n and "keep-in-aws" in n
                            for n in lines), lines)
        self.assertTrue(any("in envs/prod" in n and "migrate" in n
                            for n in lines), lines)

    def test_the_standing_listing_does_not_invent_a_root_module(self):
        """A correction recorded against the repository root carries '' — the
        tools match on it, and printing "in " would read as a missing name."""
        lines = overrides.summarize(_document(
            _record(overrides.REJECT, "aws_sqs_queue.orders", directory="",
                    workload="orders", reason="the dev copy")))

        self.assertEqual(len(lines), 1)
        # The address segment only — a reason is free text and may say "in".
        self.assertNotIn(" in ", lines[0].split(":")[0])

    def test_a_detail_the_reviewer_supplies_is_marked_as_theirs(self):
        """The value lands on a consumer that still reads `terraform_wiring`
        with an evidence file that does not contain it. Downgrading the whole
        consumer would be worse — the chain is real — so the entry says which
        part of it is not."""
        scanned = self._scanned(consumers=[
            {"workload": "orders", "kind": "helm_release", "namespace": "orders",
             "source_path": None, "detection": "terraform_wiring",
             "evidence": "data.tf"}])
        entry = self._rebuilt(
            scanned,
            _record(overrides.ATTACH, "m.x", workload="orders",
                    source_path="src/orders/chart"))

        consumer = entry["consumers"][0]
        self.assertEqual(consumer["source_path"], "src/orders/chart")
        self.assertEqual(consumer["detection"], "terraform_wiring")
        note = next(n for n in entry["notes"]
                    if n.startswith(overrides.supplied_prefix(
                        "source_path", "orders", "helm_release")))
        self.assertIn("src/orders/chart", note)
        self.assertIn("data.tf", note)
        self.assertIn("human assertion", note)
        # The namespace came from the Terraform, so it is not claimed.
        self.assertFalse(any(n.startswith(overrides.supplied_prefix(
            "namespace", "orders", "helm_release")) for n in entry["notes"]))

    def test_re_supplying_a_detail_replaces_the_marker_rather_than_stacking(self):
        scanned = self._scanned(consumers=[
            {"workload": "orders", "kind": "helm_release", "namespace": None,
             "source_path": None, "detection": "terraform_wiring",
             "evidence": "data.tf"}])
        entry = self._rebuilt(
            scanned,
            _record(overrides.ATTACH, "m.x", workload="orders",
                    namespace="staging"),
            _record(overrides.ATTACH, "m.x", workload="orders",
                    namespace="analytics"))

        marks = [n for n in entry["notes"]
                 if n.startswith(overrides.supplied_prefix(
                     "namespace", "orders", "helm_release"))]
        self.assertEqual(len(marks), 1)
        self.assertIn("analytics", marks[0])

    def test_a_namespace_the_terraform_contradicts_is_refused_out_loud(self):
        """A field with evidence behind it wins, but the reviewer who stated a
        different one is told — reporting success while overruling them is how
        the silent overrule read. And because the value stays recorded, the
        note also
        says what a later scan will do with it."""
        scanned = self._scanned(consumers=[
            {"workload": "orders", "kind": "helm_release", "namespace": "orders",
             "source_path": None, "detection": "terraform_wiring",
             "evidence": "main.tf"}])
        entry = self._rebuilt(
            scanned,
            _record(overrides.ATTACH, "m.x", workload="orders",
                    namespace="analytics", source_path="charts/orders",
                    note="the analytics team runs it"))

        self.assertEqual(entry["consumers"][0]["namespace"], "orders")
        # The one the Terraform did not state still lands, silently.
        self.assertEqual(entry["consumers"][0]["source_path"], "charts/orders")
        note = next(n for n in entry["notes"] if "was not applied" in n)
        self.assertIn("namespace given for 'orders'", note)
        self.assertIn("analytics", note)
        self.assertIn("the Terraform declares orders", note)
        self.assertIn("a later scan that finds no declared namespace", note)
        self.assertNotIn("source_path", note)

    def test_the_unplaced_rollup_does_not_blame_terraform_for_a_bad_record(self):
        """Two of the four branches that feed the rollup found the block
        perfectly well. Sending the operator to their Terraform and their scope
        for a record this server cannot parse costs them the search."""
        scanned = self._scanned(consumers=[])
        _, notes = overrides.rebuild(scanned, {"overrides": [
            _record("reject", "m.x", workload="orders")]})

        rollup = next(n for n in notes if "could not be placed" in n)
        self.assertIn("the record is malformed", rollup)
        self.assertIn("not a correction this server understands", rollup)
        self.assertNotIn("did not find", rollup)

    def test_re_attaching_keeps_the_reason_and_takes_the_new_namespace(self):
        """A reviewer correcting the namespace restates the namespace, not the
        reason — and the reason is the durable record of why the link is
        asserted at all."""
        scanned = self._scanned(consumers=[])
        entry = self._rebuilt(
            scanned,
            _record(overrides.ATTACH, "m.x", workload="billing",
                    note="the nightly export writes it", namespace="default"),
            _record(overrides.ATTACH, "m.x", workload="billing",
                    namespace="billing"))

        self.assertEqual(len(entry["consumers"]), 1)
        self.assertEqual(entry["consumers"][0]["namespace"], "billing")
        self.assertEqual(entry["consumers"][0]["note"],
                         "the nightly export writes it")

    def test_re_asserting_a_rejected_link_restores_the_derived_record(self):
        """A standing rejection strips the derived consumer before the
        attachment runs, so the attachment found nothing to confirm and minted
        a `human_review` record for a link the Terraform declares — discarding
        its chain and its evidence file, and telling a later gate that no
        evidence exists. This CL is what added `human_review` so a gate could
        tell the three apart."""
        entry = self._rebuilt(
            self._scanned(),
            # Kind-scoped, so it is not retired by the unqualified attach.
            _record(overrides.REJECT, "m.x", workload="orders",
                    consumer_kind="helm_release", reason="reads the replica"),
            _record(overrides.ATTACH, "m.x", workload="orders",
                    note="the team confirmed they own it"))

        self.assertEqual(
            [(c["kind"], c["detection"], c["evidence"])
             for c in entry["consumers"]],
            [("helm_release", "terraform_wiring", "infra/app.tf")])

    def test_restoring_one_link_keeps_the_reason_the_others_are_gone(self):
        """A wildcard rejection removes every chain of that workload. Dropping
        its note the moment ONE came back left the section omitting a link the
        Terraform declares with nothing saying why — and the corrections
        object is not what reaches the assessment."""
        scanned = [dict(self._scanned()[0], consumers=[
            {"workload": "orders", "kind": "helm_release", "namespace": None,
             "source_path": None, "detection": "terraform_wiring",
             "evidence": "infra/app.tf"},
            {"workload": "orders", "kind": "service_account",
             "namespace": "orders", "source_path": None, "detection": "irsa",
             "evidence": "infra/iam.tf"}])]
        entry = self._rebuilt(
            scanned,
            _record(overrides.REJECT, "m.x", workload="orders",
                    reason="nothing in orders touches this"),
            _record(overrides.ATTACH, "m.x", workload="orders",
                    consumer_kind="helm_release",
                    note="I was wrong about the release"))

        self.assertEqual([c["kind"] for c in entry["consumers"]],
                         ["helm_release"])
        # The one still removed is named, with the reason it was removed for.
        self.assertTrue(any("(service_account) was rejected" in n
                            and "nothing in orders touches this" in n
                            for n in entry["notes"]))
        # ...and the restored one is not described as rejected.
        self.assertFalse(any("(helm_release) was rejected" in n
                             for n in entry["notes"]))

    def test_an_attachment_the_chain_already_finds_is_a_confirmation(self):
        entry = self._rebuilt(
            self._scanned(),
            _record(overrides.ATTACH, "m.x", workload="orders",
                    note="confirmed with the team"))

        self.assertEqual([(c["workload"], c["detection"])
                          for c in entry["consumers"]],
                         [("orders", "terraform_wiring")])
        self.assertTrue(any("was confirmed at the data review" in n
                            and "confirmed with the team" in n
                            for n in entry["notes"]))

    def test_an_entry_the_scan_left_empty_says_unknown_when_it_truncated(self):
        # No consumer in the scan's own output, and a file it could not finish
        # reading: the honest answer is "unknown", not "nothing".
        entry = self._rebuilt(self._scanned(consumers=[]),
                              truncated={"infra/broken.tf"})
        self.assertIn(consumers.TRUNCATED_NOTE, entry["notes"])
        self.assertNotIn(consumers.UNATTRIBUTED_NOTE, entry["notes"])

    def test_an_entry_the_scan_left_empty_says_no_chain_reached_it(self):
        entry = self._rebuilt(self._scanned(consumers=[]))
        self.assertIn(consumers.UNATTRIBUTED_NOTE, entry["notes"])

    def test_an_entry_a_reviewer_emptied_is_not_called_unattributed(self):
        """UNATTRIBUTED_NOTE is a statement about the SCAN — no chain reached
        it, the link may be outside the repository, a human must attach it.
        Every clause is false once a reviewer has cut a link the scan found,
        and it would sit directly under their own note saying they cut it."""
        entry = self._rebuilt(
            self._scanned(),
            _record(overrides.REJECT, "m.x", workload="orders",
                    reason="the IRSA role is over-broad"))

        self.assertEqual(entry["consumers"], [])
        self.assertIn(consumers.REJECTED_NOTE, entry["notes"])
        self.assertNotIn(consumers.UNATTRIBUTED_NOTE, entry["notes"])
        self.assertNotIn(consumers.TRUNCATED_NOTE, entry["notes"])

    def test_the_rollup_counts_a_rejected_link_apart_from_an_absent_one(self):
        """The rollup is what escapes: it is persisted to
        data_dependency_scan_notes and carried to the assessment verbatim, so
        "could not be attributed from the Terraform alone" about a database the
        Terraform wires up sends the next reader hunting for a link that was
        deliberately removed."""
        scanned = self._scanned() + [
            {"service": "s3", "identifier": "exports", "address": "m.y",
             "disposition": "migrate", "evidence": ["infra/s3.tf"],
             "notes": [], "consumers": []}]
        _, notes = overrides.rebuild(scanned, _document(
            _record(overrides.REJECT, "m.x", workload="orders")))

        self.assertTrue(any("a reviewer rejected the one the Terraform "
                            "pointed at" in n and "orders-db" in n
                            for n in notes))
        attribution = [n for n in notes if "could not be attributed" in n]
        self.assertEqual(len(attribution), 1)
        self.assertIn("exports", attribution[0])
        self.assertNotIn("orders-db", attribution[0])

    def test_an_attached_consumer_clears_the_unattributed_note(self):
        entry = self._rebuilt(
            self._scanned(consumers=[]),
            _record(overrides.ATTACH, "m.x", workload="billing"))
        self.assertNotIn(consumers.UNATTRIBUTED_NOTE, entry.get("notes") or [])



class ReplayThroughTheHarvestTest(unittest.TestCase):
    """The end the whole object exists for: surviving a re-scan."""

    TF = (
        'resource "aws_db_instance" "orders" {\n'
        '  identifier = "orders-db"\n}\n'
        'resource "helm_release" "carts" {\n'
        '  name = "carts"\n'
        '  set { value = aws_db_instance.orders.endpoint }\n}\n'
    )

    def _harvest(self, document=None):
        with tempfile.TemporaryDirectory() as root:
            with open(os.path.join(root, "main.tf"), "w") as f:
                f.write(self.TF)
            inventory = {"data_dependencies": []}
            harvest = datastores.harvest_datastores(
                inventory, root, None, document)
        return (inventory["data_dependencies"][0], harvest.notes,
                harvest.workloads)

    def test_a_rejected_consumer_does_not_come_back_on_the_next_scan(self):
        entry, _, _ = self._harvest()
        self.assertEqual([c["workload"] for c in entry["consumers"]], ["carts"])

        entry, notes, _ = self._harvest(_document(
            _record(overrides.REJECT, "aws_db_instance.orders",
                    workload="carts", reason="wrong team")))

        self.assertEqual(entry["consumers"], [])
        # Counted as rejected rather than unattributed: a chain did reach this
        # one, and saying otherwise in the durable scan notes would send the
        # next reader looking for a link that was deliberately cut.
        self.assertIn(consumers.REJECTED_NOTE, entry["notes"])
        self.assertNotIn(consumers.UNATTRIBUTED_NOTE, entry["notes"])
        self.assertTrue(any("a reviewer rejected the one the Terraform "
                            "pointed at" in n for n in notes))

    def test_an_attached_consumer_survives_and_clears_the_unattributed_note(self):
        entry, _, _ = self._harvest(_document(
            _record(overrides.REJECT, "aws_db_instance.orders",
                    workload="carts"),
            _record(overrides.ATTACH, "aws_db_instance.orders",
                    workload="orders", consumer_kind="helm_release")))

        self.assertEqual([c["workload"] for c in entry["consumers"]], ["orders"])
        self.assertNotIn(consumers.UNATTRIBUTED_NOTE, entry.get("notes") or [])

    def test_the_scan_reports_what_it_replayed(self):
        _, notes, _ = self._harvest(_document(
            _record(overrides.ANNOTATE, "aws_db_instance.orders",
                    disposition="keep-in-aws")))
        self.assertTrue(any("replayed over this scan" in n for n in notes))

    def test_the_walk_hands_back_the_workloads_it_saw(self):
        # The review's candidate pool. Collected during the same walk, so it
        # describes the same commit as the entries.
        _, _, workloads = self._harvest()
        self.assertEqual([(w["workload"], w["kind"]) for w in workloads],
                         [("carts", "helm_release")])


class ArnKeyedCorrectionTest(unittest.TestCase):
    """A correction recorded against an entry's ARN survives the entry
    becoming declared: the fold keeps the ARN in `arn`, and `entries_at`
    matches on it."""

    def test_an_annotation_keyed_on_the_arn_lands_on_the_absorbing_declaration(self):
        scanned = [datastores._entry(
            "s3", "acme-invoice-archive", "s3.tf", {"bucket": "acme-invoice-archive"},
            None, [], address="aws_s3_bucket.archive")]
        scanned[0]["arn"] = "arn:aws:s3:::acme-invoice-archive"
        document = _document(_record(overrides.ANNOTATE, "arn:aws:s3:::acme-invoice-archive",
                                     disposition="keep-in-aws", note="finance owns it"))
        section, notes = overrides.rebuild(scanned, document)
        self.assertEqual(section[0]["disposition"], "keep-in-aws")
        self.assertFalse(any("could not be placed" in n for n in notes), notes)


class AliasSupersessionTest(unittest.TestCase):
    """A record made under one handle of an entry supersedes one made under
    the other — the ARN before a fold, the block address after it."""

    def test_a_reword_under_the_block_address_retires_the_arn_keyed_rejection(self):
        document = _document(_record(overrides.REJECT, "arn:aws:s3:::acme-invoice-archive",
                                     workload="orders", reason="wrong team"))
        replaced = overrides.record_override(document, _record(
            overrides.REJECT, "aws_s3_bucket.archive", directory="",
            aliases=[{"address": "arn:aws:s3:::acme-invoice-archive", "directory": None}],
            workload="orders", reason="finance owns it"))
        self.assertEqual(len(replaced), 1)
        self.assertEqual([r["reason"] for r in document["overrides"]], ["finance owns it"])

    def test_an_attach_under_the_block_address_retires_the_arn_keyed_rejection(self):
        document = _document(_record(overrides.REJECT, "arn:aws:s3:::acme-invoice-archive",
                                     workload="orders", reason="wrong team"))
        overrides.record_override(document, _record(
            overrides.ATTACH, "aws_s3_bucket.archive", directory="",
            aliases=[{"address": "arn:aws:s3:::acme-invoice-archive", "directory": None}],
            workload="orders"))
        self.assertEqual([r["kind"] for r in document["overrides"]], [overrides.ATTACH])

    def test_an_annotation_under_the_other_handle_replaces_the_disposition(self):
        document = _document(_record(overrides.ANNOTATE, "arn:aws:s3:::acme-invoice-archive",
                                     disposition="keep-in-aws"))
        overrides.record_override(document, _record(
            overrides.ANNOTATE, "aws_s3_bucket.archive", directory="",
            aliases=[{"address": "arn:aws:s3:::acme-invoice-archive", "directory": None}],
            disposition="migrate"))
        self.assertEqual([r["disposition"] for r in document["overrides"]], ["migrate"])

    def test_different_entries_do_not_alias_each_other(self):
        document = _document(_record(overrides.REJECT, "arn:aws:s3:::other",
                                     workload="orders", reason="x"))
        overrides.record_override(document, _record(
            overrides.REJECT, "aws_s3_bucket.archive", directory="",
            aliases=[{"address": "arn:aws:s3:::acme-invoice-archive", "directory": None}],
            workload="orders", reason="y"))
        self.assertEqual(len(document["overrides"]), 2)

    def test_a_block_alias_carries_its_directory_so_prod_cannot_retire_dev(self):
        """The ARN folded onto the dev declaration; a rejection made by ARN
        carries the dev block as its alias. A later rejection on the prod
        block of the same name must not retire it — and one on the dev block
        must."""
        arn_keyed = _record(overrides.REJECT, "arn:aws:s3:::acme-invoice-archive",
                            aliases=[{"address": "aws_s3_bucket.archive",
                                      "directory": "envs/dev"}],
                            workload="orders", reason="dev: wrong team")
        document = _document(arn_keyed)
        replaced = overrides.record_override(document, _record(
            overrides.REJECT, "aws_s3_bucket.archive", directory="envs/prod",
            workload="orders", reason="prod: wrong team"))
        self.assertEqual(replaced, [])
        self.assertEqual(len(document["overrides"]), 2)
        replaced = overrides.record_override(document, _record(
            overrides.REJECT, "aws_s3_bucket.archive", directory="envs/dev",
            workload="orders", reason="dev: finance owns it"))
        self.assertEqual([r["reason"] for r in replaced], ["dev: wrong team"])
        self.assertEqual(sorted(r["reason"] for r in document["overrides"]),
                         ["dev: finance owns it", "prod: wrong team"])

    def test_a_bare_string_alias_from_an_earlier_record_still_matches(self):
        document = _document(_record(overrides.REJECT, "arn:aws:s3:::acme-invoice-archive",
                                     workload="orders", reason="x"))
        replaced = overrides.record_override(document, _record(
            overrides.REJECT, "aws_s3_bucket.archive", directory="",
            aliases=["arn:aws:s3:::acme-invoice-archive"], workload="orders", reason="y"))
        self.assertEqual(len(replaced), 1)

    def test_a_block_keyed_record_still_applies_when_the_entry_is_arn_addressed_again(self):
        """Recorded under the block address while the ARN was folded onto
        it; the declaration then leaves the repo. The record's alias — the
        ARN — is what finds the entry now."""
        with tempfile.TemporaryDirectory() as root:
            with open(os.path.join(root, "iam.tf"), "w") as f:
                f.write('resource "aws_iam_policy" "p" {\n  policy = jsonencode({ Resource = "arn:aws:s3:::acme-invoice-archive" })\n}\n')
            inventory = {"data_dependencies": []}
            scanned = datastores.harvest_datastores(inventory, root).scanned
        document = _document(_record(
            overrides.ANNOTATE, "aws_s3_bucket.archive", directory="",
            aliases=[{"address": "arn:aws:s3:::acme-invoice-archive", "directory": None}],
            disposition="keep-in-aws"))
        section, notes = overrides.rebuild(scanned, document)
        self.assertEqual(section[0]["disposition"], "keep-in-aws")
        self.assertFalse(any("could not be placed" in n for n in notes), notes)

    def test_an_identifier_on_one_side_only_does_not_split_one_decision(self):
        """The review records an identifier only when the block address has
        siblings; a record made under the ARN before that, or by an older
        server, lacks it. The two are still one decision about one entry."""
        arn = "arn:aws:s3:::acme-invoice-archive"
        document = _document(_record(overrides.ANNOTATE, arn, disposition="keep-in-aws"))
        overrides.record_override(document, _record(
            overrides.ANNOTATE, "aws_s3_bucket.archive", directory="envs/dev",
            identifier="acme-invoice-archive",
            aliases=[{"address": arn, "directory": None}], disposition="migrate"))
        self.assertEqual([r["disposition"] for r in document["overrides"]], ["migrate"])

        document = _document(_record(overrides.REJECT, arn, workload="orders", reason="x"))
        overrides.record_override(document, _record(
            overrides.ATTACH, "aws_s3_bucket.archive", directory="envs/dev",
            identifier="acme-invoice-archive",
            aliases=[{"address": arn, "directory": None}], workload="orders"))
        self.assertEqual([r["kind"] for r in document["overrides"]], [overrides.ATTACH])

    def test_an_alias_hop_does_not_reach_the_other_environment(self):
        """The record names `module.orders` in envs/dev, which the scope
        now excludes, and carries the ARN as an alias. envs/prod declares
        the same block and has folded the same ARN — the alias must not
        apply a dev decision to prod; the refusal has to fire instead.
        """
        prod = datastores._entry("sqs", "orders", "envs/prod/main.tf",
                                 {"name": "orders"}, None, [],
                                 address="module.orders")
        prod["arn"] = "arn:aws:sqs:us-east-1:111111111111:orders"
        document = _document(_record(
            overrides.ANNOTATE, "module.orders", directory="envs/dev",
            aliases=[{"address": "arn:aws:sqs:us-east-1:111111111111:orders",
                      "directory": None}],
            disposition="keep-in-aws"))
        section, notes = overrides.rebuild([prod], document)
        # Unchanged: the scan's own grade for a queue, not the dev decision.
        self.assertEqual(section[0]["disposition"], "replatform")
        self.assertTrue(any("no longer declares it" in n for n in notes), notes)

    def test_an_alias_hop_still_applies_when_the_block_is_gone_everywhere(self):
        referenced = datastores._referenced_entry(
            datastores.find_arns('"arn:aws:sqs:us-east-1:111111111111:orders"')[0],
            "iam.tf")
        document = _document(_record(
            overrides.ANNOTATE, "module.orders", directory="envs/dev",
            aliases=[{"address": "arn:aws:sqs:us-east-1:111111111111:orders",
                      "directory": None}],
            disposition="keep-in-aws"))
        section, notes = overrides.rebuild([referenced], document)
        self.assertEqual(section[0]["disposition"], "keep-in-aws")
        self.assertFalse(any("could not be placed" in n for n in notes), notes)

    def test_a_sibling_appearing_later_does_not_retire_the_earlier_decision(self):
        """The identifier wildcard is for the ARN handle only. Two
        records at one BLOCK address, one made before an `_override.tf`
        gave it a sibling, are two decisions about two entries — the
        keep-in-aws must not be retired by the annotation on the other.
        """
        document = _document(_record(
            overrides.ANNOTATE, "aws_db_instance.orders", directory="envs/prod",
            disposition="keep-in-aws", note="customer keeps the live DB in AWS"))
        replaced = overrides.record_override(document, _record(
            overrides.ANNOTATE, "aws_db_instance.orders", directory="envs/prod",
            identifier="orders-db-legacy", disposition="rebuild"))
        self.assertEqual(replaced, [])
        self.assertEqual(sorted(r["disposition"] for r in document["overrides"]),
                         ["keep-in-aws", "rebuild"])

    def test_an_attach_at_a_block_address_does_not_un_reject_a_sibling(self):
        document = _document(_record(
            overrides.REJECT, "aws_db_instance.orders", directory="envs/prod",
            workload="orders", reason="wrong team"))
        overrides.record_override(document, _record(
            overrides.ATTACH, "aws_db_instance.orders", directory="envs/prod",
            identifier="orders-db-legacy", workload="orders"))
        self.assertEqual(sorted(r["kind"] for r in document["overrides"]),
                         [overrides.ATTACH, overrides.REJECT])

    def test_a_twinned_arn_record_is_not_retired_by_a_block_record(self):
        """The ARN folded onto envs/dev in scan 1 and a correction was
        made against the block. A second declaration then appears, the
        ARN no longer folds, and a correction is made against it. Those
        are two entries now, and neither may retire the other.
        """
        arn = "arn:aws:s3:::acme-invoice-archive"
        document = _document(_record(
            overrides.ANNOTATE, "module.orders", directory="envs/dev",
            aliases=[{"address": arn, "directory": None}],
            disposition="keep-in-aws", note="finance owns it"))
        replaced = overrides.record_override(document, _record(
            overrides.ANNOTATE, arn, twinned=True, note="two declarations"))
        self.assertEqual(replaced, [])
        self.assertEqual(len(document["overrides"]), 2)
        self.assertIn("keep-in-aws",
                      [r.get("disposition") for r in document["overrides"]])

    def test_an_untwinned_arn_record_is_still_retired_after_a_fold(self):
        arn = "arn:aws:s3:::acme-invoice-archive"
        document = _document(_record(overrides.ANNOTATE, arn,
                                     disposition="keep-in-aws"))
        replaced = overrides.record_override(document, _record(
            overrides.ANNOTATE, "aws_s3_bucket.archive", directory="",
            aliases=[{"address": arn, "directory": None}],
            disposition="migrate"))
        self.assertEqual(len(replaced), 1)
        self.assertEqual([r["disposition"] for r in document["overrides"]],
                         ["migrate"])


class AliasChannelIsOneDirectionalTest(unittest.TestCase):
    """`_alias_records` attaches a folded entry's ARN to every record made
    under the declaration's block address, so two block-addressed records
    routinely share an ARN alias. That intersection must not decide identity:
    the alias exists to bridge a record made under an ARN and one made under
    the block address the ARN folded onto, and nothing else."""

    def test_prod_cannot_retire_dev_when_both_carry_the_same_arn_alias(self):
        # Scan 1 scoped to envs/dev, scan 2 to envs/prod; the same ARN folds
        # onto whichever declaration is in scope, so both records carry it.
        arn = "arn:aws:s3:::acme-invoice-archive"
        document = _document(_record(
            overrides.REJECT, "aws_s3_bucket.archive", directory="envs/dev",
            aliases=[{"address": arn}], workload="orders",
            reason="dev: wrong team"))
        replaced = overrides.record_override(document, _record(
            overrides.REJECT, "aws_s3_bucket.archive", directory="envs/prod",
            aliases=[{"address": arn}], workload="orders",
            reason="prod: wrong team"))
        self.assertEqual(replaced, [])
        self.assertEqual(sorted(r["reason"] for r in document["overrides"]),
                         ["dev: wrong team", "prod: wrong team"])

    def test_a_shared_arn_alias_is_not_an_identifier_wildcard(self):
        # Recorded when the address had one entry, so the record carries no
        # identifier. An `_override.tf` then leaves a sibling and the ARN
        # folds onto that one instead — a decision about the sibling must not
        # retire the customer's keep-in-aws.
        arn = "arn:aws:sqs:us-east-1:111111111111:orders"
        document = _document(_record(
            overrides.ANNOTATE, "aws_sqs_queue.q", directory="",
            aliases=[{"address": arn}], disposition="keep-in-aws",
            reason="finance owns it"))
        replaced = overrides.record_override(document, _record(
            overrides.ANNOTATE, "aws_sqs_queue.q", directory="",
            identifier="orders-v2", aliases=[{"address": arn}],
            disposition="migrate", reason="the sibling"))
        self.assertEqual(replaced, [])
        self.assertEqual(sorted(r["disposition"] for r in document["overrides"]),
                         ["keep-in-aws", "migrate"])

    def test_the_bridge_the_alias_exists_for_still_works(self):
        # The one intended direction: recorded under the ARN before the fold,
        # then under the block address after it.
        arn = "arn:aws:s3:::acme-invoice-archive"
        document = _document(_record(
            overrides.REJECT, arn, workload="orders", reason="by arn"))
        replaced = overrides.record_override(document, _record(
            overrides.REJECT, "aws_s3_bucket.archive", directory="",
            aliases=[{"address": arn}], workload="orders",
            reason="by block address"))
        self.assertEqual([r["reason"] for r in replaced], ["by arn"])

class ACorrectionSurvivesANewSpellingTest(unittest.TestCase):
    """An entry's handle is one spelling of its ARN, and the merge can settle
    on another when the set of sightings changes. A correction keyed on the
    spelling the reviewer was shown has to still find the entry."""

    def _inventory(self, arn):
        return {"data_dependencies": [
            {"service": "sqs", "identifier": "orders", "detection": "referenced",
             "address": arn, "arn": arn, "disposition": "migrate",
             "evidence": ["a.tf"], "consumers": []}]}

    def test_a_wildcard_handle_finds_the_qualified_entry(self):
        inventory = self._inventory("arn:aws:sqs:us-east-1:111111111111:orders")
        found = overrides.entries_at(inventory, "arn:aws:sqs:*:*:orders")
        self.assertEqual([e["identifier"] for e in found], ["orders"])

    def test_a_qualified_handle_finds_the_wildcard_entry(self):
        # The reverse: the file holding the specific spelling left the
        # confirmed scope, so the entry dropped back to the looser handle.
        inventory = self._inventory("arn:aws:sqs:::orders")
        found = overrides.entries_at(
            inventory, "arn:aws:sqs:us-east-1:111111111111:orders")
        self.assertEqual([e["identifier"] for e in found], ["orders"])

    def test_another_region_is_still_another_queue(self):
        inventory = self._inventory("arn:aws:sqs:us-east-1:111111111111:orders")
        self.assertEqual(overrides.entries_at(
            inventory, "arn:aws:sqs:eu-west-1:111111111111:orders"), [])

    def test_an_exact_match_still_wins_over_the_fallback(self):
        exact = "arn:aws:sqs:us-east-1:111111111111:orders"
        inventory = self._inventory(exact)
        inventory["data_dependencies"].append(
            {"service": "sqs", "identifier": "orders", "detection": "referenced",
             "address": "arn:aws:sqs:*:*:orders", "arn": "arn:aws:sqs:*:*:orders",
             "disposition": "migrate", "evidence": ["b.tf"], "consumers": []})
        found = overrides.entries_at(inventory, exact)
        self.assertEqual([e["arn"] for e in found], [exact])

class SupersessionFollowsASpellingTest(unittest.TestCase):
    """`entries_at` tolerates a handle that drifted to another spelling of
    one ARN; supersession has to as well, or two corrections about one
    resource stand side by side and the section says both that a consumer
    was rejected and that it was attached."""

    LOOSE = "arn:aws:sqs:::orders"
    EXACT = "arn:aws:sqs:us-east-1:111111111111:orders"
    OTHER = "arn:aws:sqs:eu-west-1:111111111111:orders"

    def test_an_attach_retires_a_rejection_recorded_under_another_spelling(self):
        document = _document(_record(
            overrides.REJECT, self.LOOSE, workload="orders", reason="wrong team"))
        replaced = overrides.record_override(document, _record(
            overrides.ATTACH, self.EXACT, workload="orders",
            reason="billing drains it"))
        self.assertEqual([r["reason"] for r in replaced], ["wrong team"])
        self.assertEqual([r["kind"] for r in document["overrides"]],
                         [overrides.ATTACH])

    def test_another_region_still_supersedes_nothing(self):
        document = _document(_record(
            overrides.REJECT, self.EXACT, workload="orders", reason="wrong team"))
        replaced = overrides.record_override(document, _record(
            overrides.ATTACH, self.OTHER, workload="orders", reason="a queue in Ireland"))
        self.assertEqual(replaced, [])
        self.assertEqual(len(document["overrides"]), 2)

    def test_two_rejections_under_two_spellings_do_not_both_stand(self):
        document = _document(_record(
            overrides.REJECT, self.LOOSE, workload="orders", reason="first"))
        replaced = overrides.record_override(document, _record(
            overrides.REJECT, self.EXACT, workload="orders", reason="second"))
        self.assertEqual([r["reason"] for r in replaced], ["first"])
        self.assertEqual([r["reason"] for r in document["overrides"]], ["second"])

class AmbiguousSpellingsDoNotSupersedeTest(unittest.TestCase):
    """Where the scan kept several spellings of one name apart because their
    accounts could not be reconciled, the spelling tolerance must not run
    between them: an under-specified ARN agrees with every one."""

    LOOSE = "arn:aws:sqs:::orders"
    ONE = "arn:aws:sqs:us-east-1:111111111111:orders"

    def test_a_decision_on_one_spelling_does_not_retire_another(self):
        document = _document(_record(
            overrides.ANNOTATE, self.ONE, disposition="keep-in-aws",
            reason="finance owns the us-east-1 queue", ambiguous_spelling=True))
        replaced = overrides.record_override(document, _record(
            overrides.ANNOTATE, self.LOOSE, disposition="migrate",
            reason="the other one", ambiguous_spelling=True))
        self.assertEqual(replaced, [])
        self.assertEqual(sorted(r["disposition"] for r in document["overrides"]),
                         ["keep-in-aws", "migrate"])

    def test_an_attach_does_not_un_reject_across_an_ambiguous_group(self):
        document = _document(_record(
            overrides.REJECT, self.ONE, workload="orders", reason="wrong team",
            ambiguous_spelling=True))
        replaced = overrides.record_override(document, _record(
            overrides.ATTACH, self.LOOSE, workload="orders",
            reason="billing drains it", ambiguous_spelling=True))
        self.assertEqual(replaced, [])
        self.assertEqual(len(document["overrides"]), 2)

    def test_an_unflagged_pair_still_supersedes(self):
        # The counter-direction: without the stamp the tolerance is what
        # keeps one resource's two spellings from standing side by side.
        document = _document(_record(
            overrides.REJECT, self.LOOSE, workload="orders", reason="first"))
        replaced = overrides.record_override(document, _record(
            overrides.REJECT, self.ONE, workload="orders", reason="second"))
        self.assertEqual([r["reason"] for r in replaced], ["first"])


_SECRET = "arn:aws:secretsmanager:us-east-1:111111111111:secret:"


def _secret_pair_files() -> dict:
    """A declared secret that absorbs one console form, beside a second console
    form in another root module that the merge refuses: both flagged."""
    return {
        "sm.tf": 'resource "aws_secretsmanager_secret" "db" {\n  name = "acme/db"\n}\n',
        "envs/prod/iam.tf": (
            'resource "aws_iam_policy" "p" {\n  policy = jsonencode({ Resource = ['
            f'"{_SECRET}acme/db", "{_SECRET}acme/db-Prod01"] }})\n}}\n'),
        "envs/dev/iam.tf": (
            'resource "aws_iam_policy" "p" {\n  policy = jsonencode({ Resource = '
            f'"{_SECRET}acme/db-Dev001" }})\n}}\n'),
    }


class ReviewRoundFortyFiveTest(unittest.TestCase):
    """Regressions from the forty-fifth adversarial review round."""

    def _harvest(self, scope=None, document=None):
        inventory = {"data_dependencies": []}
        with tempfile.TemporaryDirectory() as root:
            for rel_path, content in _secret_pair_files().items():
                full = os.path.join(root, rel_path)
                os.makedirs(os.path.dirname(full), exist_ok=True)
                with open(full, "w") as handle:
                    handle.write(content)
            harvest = datastores.harvest_datastores(inventory, root, scope, document)
        return inventory["data_dependencies"], harvest.notes

    def _stamped_annotation(self, address):
        # What `_amend` writes for annotate_data_dependency on a flagged entry.
        return {"schema_version": 1, "overrides": [_record(
            overrides.ANNOTATE, address, disposition="keep-in-aws",
            note="dev-only secret, stays", directory=None, ambiguous_spelling=True)]}

    def test_a_stamped_correction_is_not_replayed_onto_the_surviving_spelling(self):
        # The flag lives on the entry; the record carries its own copy. Once
        # envs/dev leaves the scope the Dev001 entry is gone, the declared
        # secret is unflagged, and the spelling fallback landed a dev-only
        # `keep-in-aws` on it — flipping a `migrate` and reporting success.
        entries, _ = self._harvest()
        dev = next(e for e in entries if e["identifier"] == "acme/db-Dev001")
        self.assertTrue(datastores.spelling_is_ambiguous(dev))
        document = self._stamped_annotation(dev["address"])
        entries, notes = self._harvest(
            {"included": [], "excluded": ["envs/dev/**"]}, document)
        self.assertEqual([e["identifier"] for e in entries], ["acme/db"])
        self.assertEqual(entries[0]["disposition"], "migrate")
        self.assertTrue(any("could not be placed" in n and "acme/db-Dev001" in n
                            for n in notes), notes)

    def test_a_stamped_correction_still_lands_on_its_own_entry(self):
        # Exact handles are not a weaker match: the record applies wherever
        # the entry it was made on still stands.
        entries, _ = self._harvest()
        dev = next(e for e in entries if e["identifier"] == "acme/db-Dev001")
        entries, _ = self._harvest(None, self._stamped_annotation(dev["address"]))
        by_id = {e["identifier"]: e for e in entries}
        self.assertEqual(by_id["acme/db-Dev001"]["disposition"], "keep-in-aws")
        self.assertEqual(by_id["acme/db"]["disposition"], "migrate")


class ReviewRoundFortySixTest(unittest.TestCase):
    """Regressions from the forty-sixth adversarial review round."""

    def _harvest(self, files, scope=None, document=None):
        inventory = {"data_dependencies": []}
        with tempfile.TemporaryDirectory() as root:
            for rel_path, content in files.items():
                full = os.path.join(root, rel_path)
                os.makedirs(os.path.dirname(full), exist_ok=True)
                with open(full, "w") as handle:
                    handle.write(content)
            harvest = datastores.harvest_datastores(inventory, root, scope, document)
        return inventory["data_dependencies"], harvest.notes

    def test_a_stamped_record_whose_spelling_recomposed_says_so_and_names_the_new_one(self):
        # A stamped record applies to its exact spelling only, and a new
        # sighting can recompose the entry's handle while the entry is still
        # there and still flagged. Losing the correction is the sound trade
        # — landing it on a sibling is worse — but the note blamed a deleted
        # block and said nothing about the spelling one row away.
        files = dict(_secret_pair_files())
        files["envs/dev/iam.tf"] = (
            'resource "aws_iam_policy" "p" {\n  policy = jsonencode({ Resource = '
            '"arn:aws:secretsmanager:::secret:acme/db-Dev001" })\n}\n')
        entries, _ = self._harvest(files)
        dev = next(e for e in entries if e["identifier"] == "acme/db-Dev001")
        self.assertEqual(dev["address"], "arn:aws:secretsmanager:::secret:acme/db-Dev001")
        document = {"schema_version": 1, "overrides": [_record(
            overrides.ANNOTATE, dev["address"], disposition="keep-in-aws",
            directory=None, ambiguous_spelling=True)]}
        files["envs/dev/qualified.tf"] = (
            'resource "aws_iam_policy" "q" {\n  policy = jsonencode({ Resource = '
            f'"{_SECRET}acme/db-Dev001" }})\n}}\n')
        entries, notes = self._harvest(files, None, document)
        dev = next(e for e in entries if e["identifier"] == "acme/db-Dev001")
        self.assertEqual(dev["address"], f"{_SECRET}acme/db-Dev001")
        self.assertEqual(dev["disposition"], "migrate")
        note = next(n for n in notes if "could not be placed" in n)
        self.assertIn("re-record against the spelling the listing prints", note)
        self.assertIn(f"{_SECRET}acme/db-Dev001", note)
        self.assertNotIn("not declared by anything this scan read", note)

    def test_an_unstamped_record_is_not_replayed_by_spelling_onto_a_flagged_entry(self):
        # The outcome store refuses the tolerance whenever the CANDIDATE is
        # flagged; the replay asked only after the record's stamp. A record
        # made on A(111) while it stood alone, replayed after A left, landed
        # on the still-flagged any-account B beside C(222) and D(333).
        queue = "arn:aws:sqs:{}:orders"
        entries, _ = self._harvest({"iam.tf": (
            'resource "aws_iam_policy" "p" {\n  policy = jsonencode({ Resource = ['
            f'"{queue.format("*:*")}", "{queue.format("us-east-1:222222222222")}", '
            f'"{queue.format("us-east-1:333333333333")}"] }})\n}}\n')})
        self.assertEqual(len(entries), 3)
        self.assertTrue(all(datastores.spelling_is_ambiguous(e) for e in entries))
        document = {"schema_version": 1, "overrides": [_record(
            overrides.ANNOTATE, queue.format("us-east-1:111111111111"),
            disposition="keep-in-aws", note="finance's queue", directory=None)]}
        entries, notes = self._harvest({"iam.tf": (
            'resource "aws_iam_policy" "p" {\n  policy = jsonencode({ Resource = ['
            f'"{queue.format("*:*")}", "{queue.format("us-east-1:222222222222")}", '
            f'"{queue.format("us-east-1:333333333333")}"] }})\n}}\n')}, None, document)
        self.assertNotIn("keep-in-aws", {e["disposition"] for e in entries})
        note = next(n for n in notes if "could not be placed" in n)
        self.assertIn("keeps apart as ambiguous", note)


class ReviewRoundFiftyTest(unittest.TestCase):
    """Regressions from the fiftieth adversarial review round."""

    def test_a_block_record_does_not_hop_onto_a_spelling_the_verdicts_place_elsewhere(self):
        # The outcome store refuses this since round 49; the replay did not.
        # Provider silent: the 222 spelling folds onto dev's secret, and the
        # reviewer records `keep-in-aws` on dev's block (alias: the ARN).
        # Then the provider states the estate's account and dev's file leaves
        # the scope: the ARN stands cross-account, and the alias hop landed
        # dev's decision on the finance account's secret — zero gating.
        arn = "arn:aws:secretsmanager:us-east-1:222222222222:secret:acme/db"
        files = {
            "envs/dev/sm.tf": 'resource "aws_secretsmanager_secret" "db" {\n  name = "acme/db"\n}\n',
            "iam/policy.tf": ('resource "aws_iam_policy" "p" {\n  policy = jsonencode({ Resource = '
                              f'"{arn}" }})\n}}\n'),
        }

        def harvest(extra=None, without=(), document=None):
            # Dev's file is REMOVED, not excluded by scope: an excluded file
            # may hold a provider block, so exclusion leaves the account set
            # incomplete and the verdict "unknown" — which the bridge rightly
            # honours. The verdict that refuses needs the set complete.
            inventory = {"data_dependencies": []}
            with tempfile.TemporaryDirectory() as root:
                for rel_path, content in dict(files, **(extra or {})).items():
                    if rel_path in without:
                        continue
                    full = os.path.join(root, rel_path)
                    os.makedirs(os.path.dirname(full), exist_ok=True)
                    with open(full, "w") as handle:
                        handle.write(content)
                harvest = datastores.harvest_datastores(inventory, root, None, document)
            return inventory["data_dependencies"], harvest.notes

        (dev,), _ = harvest()
        self.assertEqual((dev["detection"], dev["arn"]), ("declared", arn))
        document = {"schema_version": 1, "overrides": [_record(
            overrides.ANNOTATE, dev["address"], disposition="keep-in-aws",
            directory="envs/dev", aliases=[arn])]}
        provider = {"providers.tf": ('provider "aws" {\n  region = "us-east-1"\n'
                                     '  allowed_account_ids = ["111111111111"]\n}\n')}
        entries, notes = harvest(provider, ("envs/dev/sm.tf",), document)
        (foreign,) = entries
        self.assertEqual(foreign["detection"], "referenced")
        self.assertTrue(overrides._placed_elsewhere(foreign), foreign["notes"])
        self.assertEqual(foreign["disposition"], "migrate")
        note = next(n for n in notes if "could not be placed" in n)
        self.assertIn("not the resource the decision was about", note)
        # The designed bridge is intact: provider silent, dev's file gone.
        entries, notes = harvest(None, ("envs/dev/sm.tf",), document)
        (bridged,) = entries
        self.assertEqual(bridged["disposition"], "keep-in-aws")


class ReviewRoundFiftyOneTest(unittest.TestCase):
    """Regressions from the fifty-first adversarial review round."""

    _S = "arn:aws:secretsmanager:us-east-1:111111111111:secret:"

    def _harvest(self, files, document=None):
        inventory = {"data_dependencies": []}
        with tempfile.TemporaryDirectory() as root:
            for rel_path, content in files.items():
                full = os.path.join(root, rel_path)
                os.makedirs(os.path.dirname(full), exist_ok=True)
                with open(full, "w") as handle:
                    handle.write(content)
            harvest = datastores.harvest_datastores(inventory, root, None, document)
        return inventory["data_dependencies"], harvest.notes

    def test_a_block_records_alias_never_lands_on_another_root_modules_declaration(self):
        # Dev's `keep-in-aws`, dev's file gone, prod declares the same name
        # under a DIFFERENT block address: the alias hop applied dev's
        # decision to prod's secret. The other-environment refusal speaks.
        policy = ('resource "aws_iam_policy" "p" {\n  policy = jsonencode({ Resource = '
                  f'"{self._S}acme/db" }})\n}}\n')
        (dev,), _ = self._harvest({
            "envs/dev/sm.tf": 'resource "aws_secretsmanager_secret" "db" {\n  name = "acme/db"\n}\n',
            "iam/p.tf": policy})
        document = {"schema_version": 1, "overrides": [_record(
            overrides.ANNOTATE, dev["address"], disposition="keep-in-aws",
            directory="envs/dev", aliases=[dev["arn"]])]}
        entries, notes = self._harvest({
            "envs/prod/sm.tf": ('module "db_secret" {\n  source = "terraform-aws-modules/'
                                'secrets-manager/aws"\n  name = "acme/db"\n}\n'),
            "iam/p.tf": policy}, document)
        (prod,) = entries
        self.assertEqual(prod["detection"], "declared")
        self.assertEqual(prod["disposition"], "migrate")
        note = next(n for n in notes if "could not be placed" in n)
        self.assertIn("no longer declares it", note)

    def test_the_refusal_does_not_overclaim_for_an_ambiguous_spelling(self):
        # Provider silent, dev gone, a second spelling in another account:
        # both flagged, refused — but the flagged spelling IS the alias that
        # folded onto dev, and "is not the resource" overclaimed.
        (dev,), _ = self._harvest({
            "envs/dev/sm.tf": 'resource "aws_secretsmanager_secret" "db" {\n  name = "acme/db"\n}\n',
            "iam/p.tf": ('resource "aws_iam_policy" "p" {\n  policy = jsonencode({ Resource = '
                         f'"{self._S}acme/db" }})\n}}\n')})
        document = {"schema_version": 1, "overrides": [_record(
            overrides.ANNOTATE, dev["address"], disposition="keep-in-aws",
            directory="envs/dev", aliases=[dev["arn"]])]}
        entries, notes = self._harvest({"iam/p.tf": (
            'resource "aws_iam_policy" "p" {\n  policy = jsonencode({ Resource = ['
            f'"{self._S}acme/db", '
            '"arn:aws:secretsmanager:us-east-1:222222222222:secret:acme/db"] })\n}\n')},
            document)
        self.assertEqual({e["disposition"] for e in entries}, {"migrate"})
        note = next(n for n in notes if "could not be placed" in n)
        self.assertIn("may no longer be the resource", note)
        self.assertNotIn("is not the resource", note)


class ReviewRoundFiftyEightTest(unittest.TestCase):
    """Regressions from the fifty-eighth adversarial review round."""

    _PROVIDER = ('provider "aws" {\n  region = "us-east-1"\n'
                 '  allowed_account_ids = ["111111111111"]\n}\n')
    _SECRET = ('resource "aws_secretsmanager_secret" "db" {\n  name = "prod/db"\n'
               '  replica {\n    region = "eu-west-1"\n  }\n}\n')
    _REPLICA_ARN = "arn:aws:secretsmanager:eu-west-1:111111111111:secret:prod/db"
    _IRSA = ('module "orders_irsa" {\n'
             '  source = "terraform-aws-modules/iam/aws//modules/iam-role-for-service-accounts-eks"\n'
             '  role_policy_arns = {}\n'
             '  oidc_providers = { main = { provider_arn = "x", '
             'namespace_service_accounts = ["default:orders-sa"] } }\n'
             '  policy_statements = [{ actions = ["secretsmanager:GetSecretValue"], '
             'resources = ["' + _REPLICA_ARN + '-??????"] }]\n}\n')

    def _harvest(self, document=None):
        inventory = {"data_dependencies": []}
        with tempfile.TemporaryDirectory() as root:
            os.makedirs(os.path.join(root, "envs/dev"))
            with open(os.path.join(root, "envs/dev/main.tf"), "w") as handle:
                handle.write(self._PROVIDER + self._SECRET + self._IRSA)
            datastores.harvest_datastores(inventory, root, None, document)
        return {e["detection"]: e for e in inventory["data_dependencies"]}

    def test_a_rejection_on_the_replica_withdraws_the_carried_copy_too(self):
        # The carry ran before the replay, so rejecting the workload on the
        # replica — the entry the chain derived it from — left the carried
        # copy on the `migrate` primary, and the gate kept holding it.
        entries = self._harvest()
        self.assertEqual([c["workload"] for c in entries["declared"]["consumers"]], ["orders-sa"])
        self.assertEqual(entries["declared"]["consumers"][0]["via_replica"], self._REPLICA_ARN)
        entries = self._harvest(_document(_record(
            overrides.REJECT, self._REPLICA_ARN, workload="orders-sa", reason="not this app")))
        self.assertEqual(entries["referenced"]["consumers"], [])
        self.assertEqual(entries["declared"]["consumers"], [])

    def test_a_rejection_on_the_primary_withdraws_the_carried_copy(self):
        entries = self._harvest(_document(_record(
            overrides.REJECT, "aws_secretsmanager_secret.db", workload="orders-sa",
            reason="not this app", directory="envs/dev")))
        self.assertEqual(entries["declared"]["consumers"], [])
        # The replica still lists it: that is where it was derived.
        self.assertEqual([c["workload"] for c in entries["referenced"]["consumers"]], ["orders-sa"])
        # Round 59: rejected, not "unattributed" — a carried consumer is a
        # derived consumer, and removing the only one counts as emptied by
        # rejection, as it does for a direct grant.
        notes = " ".join(entries["declared"]["notes"])
        self.assertIn("no consumer by decision", notes)
        self.assertNotIn("neither of the two reference chains", notes)

    def _harvest_files(self, files, scope=None, document=None):
        inventory = {"data_dependencies": []}
        with tempfile.TemporaryDirectory() as root:
            for rel_path, content in files.items():
                full = os.path.join(root, rel_path)
                os.makedirs(os.path.dirname(full), exist_ok=True)
                with open(full, "w") as handle:
                    handle.write(content)
            harvest = datastores.harvest_datastores(inventory, root, scope, document)
        return inventory["data_dependencies"], harvest.notes

    def test_a_rejection_about_dev_is_not_honoured_on_prod_by_the_carry(self):
        # Round 59: the carry keyed on raw record addresses and ignored the
        # directory; a rejection the replay refused ("recorded against
        # 'envs/dev', which no longer declares it") still emptied prod.
        files = {"envs/dev/main.tf": self._PROVIDER + self._SECRET,
                 "envs/prod/main.tf": self._PROVIDER + self._SECRET + self._IRSA}
        document = _document(_record(
            overrides.REJECT, "aws_secretsmanager_secret.db", workload="orders-sa",
            reason="dev only", directory="envs/dev"))
        entries, notes = self._harvest_files(
            {"envs/prod/main.tf": files["envs/prod/main.tf"]}, None, document)
        prod = next(e for e in entries if e["detection"] == "declared")
        self.assertEqual([c["workload"] for c in prod["consumers"]], ["orders-sa"])
        self.assertTrue(any("could not be placed" in n for n in notes), notes)

    def test_confirming_a_carried_consumer_restores_the_derived_record(self):
        # Round 59: the attach ran against a copy with no carried consumer,
        # minted a `human_review` one, and the carry then deduped itself away
        # — the IRSA evidence gone and the tool saying "attached by hand".
        entries = self._harvest(_document(_record(
            overrides.ATTACH, "aws_secretsmanager_secret.db", workload="orders-sa",
            directory="envs/dev", consumer_kind="service_account",
            consumer_kind_hint="service_account", namespace_hint="default")))
        (consumer,) = entries["declared"]["consumers"]
        self.assertEqual(consumer["detection"], "irsa")
        self.assertEqual(consumer["via_replica"], self._REPLICA_ARN)

    def test_a_rejection_under_the_primarys_arn_survives_a_respelling(self):
        # Round 59: the reviewer rejected under the primary's ARN; when that
        # sighting's file left and `arn` went None, the carry re-added the
        # consumer beside its own withdrawal note.
        primary_arn = "arn:aws:secretsmanager:us-east-1:111111111111:secret:prod/db"
        second = self._IRSA.replace('module "orders_irsa"', 'module "orders_us_irsa"').replace(
            self._REPLICA_ARN + "-??????", primary_arn + "-??????")
        files = {"envs/dev/main.tf": self._PROVIDER + self._SECRET + self._IRSA,
                 "envs/dev/us.tf": second}
        entries, _ = self._harvest_files(files)
        declared = next(e for e in entries if e["detection"] == "declared")
        self.assertEqual(declared["arn"], primary_arn)
        document = _document(_record(
            overrides.REJECT, primary_arn, workload="orders-sa", reason="withdrawn",
            aliases=[{"address": "aws_secretsmanager_secret.db", "directory": "envs/dev"}]))
        entries, _ = self._harvest_files({"envs/dev/main.tf": files["envs/dev/main.tf"]},
                                         None, document)
        declared = next(e for e in entries if e["detection"] == "declared")
        self.assertIsNone(declared["arn"])
        self.assertEqual(declared["consumers"], [])
        self.assertTrue(any("was rejected" in n for n in declared["notes"]), declared["notes"])

    def test_a_kind_scoped_rejection_leaves_a_carried_consumer_of_another_kind(self):
        entries = self._harvest(_document(_record(
            overrides.REJECT, "aws_secretsmanager_secret.db", workload="orders-sa",
            reason="the helm link only", directory="envs/dev", consumer_kind="helm_release")))
        self.assertEqual([(c["workload"], c["kind"]) for c in entries["declared"]["consumers"]],
                         [("orders-sa", "service_account")])

class EntryDecisionTest(unittest.TestCase):
    """Confirm, dismiss and add: decisions about an entry rather than a link."""

    GUESS = {"charts/orders/Chart.yaml": "apiVersion: v2\nname: orders\n",
             "charts/orders/values.yaml": "invoiceBucket: acme-invoice-archive\n"}

    def _scan(self, files):
        with tempfile.TemporaryDirectory() as root:
            for rel_path, content in files.items():
                full = os.path.join(root, rel_path)
                os.makedirs(os.path.dirname(full), exist_ok=True)
                with open(full, "w") as f:
                    f.write(content)
            inventory = {"data_dependencies": []}
            harvest = datastores.harvest_datastores(inventory, root)
            return harvest.scanned

    def test_a_confirmed_guess_becomes_a_human_review_entry_with_a_plan(self):
        scanned = self._scan(self.GUESS)
        self.assertEqual(scanned[0]["detection"], "inferred")
        document = _document(_record(overrides.CONFIRM, "s3:acme-invoice-archive",
                                     note="finance confirms the archive"))
        section, notes = overrides.rebuild(scanned, document)
        entry = section[0]
        self.assertEqual(entry["detection"], overrides.HUMAN_DETECTION)
        # The table default for a bucket, since the reviewer gave none.
        self.assertEqual(entry["disposition"], "migrate")
        self.assertFalse(any(n.startswith("a guess: ") for n in entry["notes"]))
        self.assertTrue(any("confirmed as a real dependency" in n
                            and "finance confirms" in n for n in entry["notes"]))
        # The holder found by the scan is still its consumer.
        self.assertEqual([c["workload"] for c in entry["consumers"]], ["orders"])
        self.assertTrue(any("1 confirmation(s)" in n for n in notes))

    def test_a_confirmation_can_carry_the_disposition_and_a_consumer(self):
        scanned = self._scan(self.GUESS)
        document = _document(_record(
            overrides.CONFIRM, "s3:acme-invoice-archive", disposition="keep-in-aws",
            workload="billing", consumer_kind="Deployment", namespace="finance"))
        section, _ = overrides.rebuild(scanned, document)
        entry = section[0]
        self.assertEqual(entry["disposition"], "keep-in-aws")
        self.assertEqual(sorted((c["workload"], c["detection"]) for c in entry["consumers"]),
                         [("billing", overrides.HUMAN_DETECTION), ("orders", "config_value")])

    def test_a_dismissed_guess_is_gone_and_stays_gone(self):
        scanned = self._scan(self.GUESS)
        document = _document(_record(overrides.DISMISS, "s3:acme-invoice-archive",
                                     reason="a legacy value; the bucket was deleted"))
        section, notes = overrides.rebuild(scanned, document)
        self.assertEqual(section, [])
        self.assertTrue(any("1 guess(es) dismissed" in n for n in notes))
        # Replayed over a scan that no longer guesses it: satisfied, not
        # reported as a correction that could not be placed.
        _, notes = overrides.rebuild([], document)
        self.assertFalse(any("could not be placed" in n for n in notes))

    def test_a_dismissal_retires_the_other_decisions_on_the_entry(self):
        document = _document(
            _record(overrides.ANNOTATE, "s3:acme-invoice-archive", note="maybe finance"),
            _record(overrides.CONFIRM, "s3:acme-invoice-archive"))
        replaced = overrides.record_override(
            document, _record(overrides.DISMISS, "s3:acme-invoice-archive",
                              reason="no such bucket"))
        self.assertEqual(sorted(r["kind"] for r in replaced),
                         [overrides.ANNOTATE, overrides.CONFIRM])
        self.assertEqual([r["kind"] for r in document["overrides"]], [overrides.DISMISS])

    def test_confirming_after_a_dismissal_un_dismisses(self):
        document = _document(_record(overrides.DISMISS, "s3:acme-invoice-archive",
                                     reason="thought it was gone"))
        overrides.record_override(document, _record(overrides.CONFIRM,
                                                    "s3:acme-invoice-archive"))
        self.assertEqual([r["kind"] for r in document["overrides"]], [overrides.CONFIRM])

    def test_a_dismissal_does_not_touch_a_different_entry(self):
        document = _document(
            _record(overrides.ANNOTATE, "sqs:orders", note="keep"),
            _record(overrides.DISMISS, "s3:acme-invoice-archive", reason="gone"))
        self.assertEqual(sorted(r["kind"] for r in document["overrides"]),
                         [overrides.ANNOTATE, overrides.DISMISS])

    def test_an_added_entry_is_created_when_nothing_derives_it(self):
        document = _document(_record(
            overrides.ADD, "rds:legacy-orders", service="rds", identifier="legacy-orders",
            note="the on-prem replica everyone forgets", workload="orders",
            consumer_kind="Deployment"))
        section, notes = overrides.rebuild([], document)
        self.assertEqual(len(section), 1)
        entry = section[0]
        self.assertEqual((entry["service"], entry["identifier"], entry["address"],
                          entry["detection"], entry["disposition"]),
                         ("rds", "legacy-orders", "rds:legacy-orders",
                          overrides.HUMAN_DETECTION, "migrate"))
        self.assertEqual([(c["workload"], c["detection"]) for c in entry["consumers"]],
                         [("orders", overrides.HUMAN_DETECTION)])
        self.assertTrue(entry["evidence"][0].startswith("added at the data review"))
        self.assertTrue(any("1 data service(s) added by hand" in n for n in notes))

    def test_an_added_entry_lands_on_the_entry_a_later_scan_derives(self):
        """The reviewer added the bucket by hand; a later scan finds its ARN
        in a policy. One entry, referenced, carrying the reviewer's note —
        the literal answers the question better than the assertion did."""
        document = _document(_record(
            overrides.ADD, "s3:acme-invoice-archive", service="s3",
            identifier="acme-invoice-archive", disposition="keep-in-aws",
            note="finance owns it"))
        scanned = self._scan({"iam.tf": '''
resource "aws_iam_policy" "p" {
  policy = jsonencode({ Resource = "arn:aws:s3:::acme-invoice-archive" })
}
'''})
        section, _ = overrides.rebuild(scanned, document)
        self.assertEqual(len(section), 1)
        entry = section[0]
        self.assertEqual(entry["detection"], "referenced")
        self.assertEqual(entry["address"], "arn:aws:s3:::acme-invoice-archive")
        self.assertEqual(entry["disposition"], "keep-in-aws")
        self.assertTrue(any("added at the data review" in n and "finance owns it" in n
                            for n in entry["notes"]))

    def test_an_added_entry_lands_on_its_own_later_guess(self):
        document = _document(_record(
            overrides.ADD, "s3:acme-invoice-archive", service="s3",
            identifier="acme-invoice-archive"))
        section, _ = overrides.rebuild(self._scan(self.GUESS), document)
        self.assertEqual(len(section), 1)
        self.assertEqual(section[0]["detection"], overrides.HUMAN_DETECTION)
        self.assertEqual([c["workload"] for c in section[0]["consumers"]], ["orders"])

    def test_two_confirmations_naming_two_workloads_are_one_answer_and_two_links(self):
        # Round 4 of the review: "name another with `workload`" means add,
        # so a second confirmation naming a different workload stands beside
        # the first, as two attachments naming different chains do — while a
        # second confirmation naming the SAME workload restates the first.
        document = _document(
            _record(overrides.CONFIRM, "s3:acme-invoice-archive", workload="orders"),
            _record(overrides.CONFIRM, "s3:acme-invoice-archive", workload="billing"))
        # One confirmation, two attachments: the consumer is a link record.
        self.assertEqual(sorted(r["kind"] for r in document["overrides"]),
                         [overrides.ATTACH, overrides.ATTACH, overrides.CONFIRM])
        self.assertEqual(sorted(r["workload"] for r in document["overrides"]
                                if r["kind"] == overrides.ATTACH), ["billing", "orders"])
        overrides.record_override(document, _record(
            overrides.CONFIRM, "s3:acme-invoice-archive", workload="billing", note="restated"))
        self.assertEqual(sorted(r["kind"] for r in document["overrides"]),
                         [overrides.ATTACH, overrides.ATTACH, overrides.CONFIRM])
        section, _ = overrides.rebuild(self._scan(self.GUESS), document)
        self.assertEqual(sorted(c["workload"] for c in section[0]["consumers"]),
                         ["billing", "orders"])

    def test_the_listing_names_the_new_decisions(self):
        lines = overrides.summarize(_document(
            _record(overrides.CONFIRM, "s3:a", disposition="migrate"),
            _record(overrides.DISMISS, "s3:b", reason="gone"),
            _record(overrides.ADD, "rds:c", service="rds", identifier="c",
                    workload="orders")))
        self.assertTrue(any(l.startswith("s3:a: confirm this is a real data dependency, "
                                         "disposition migrate") for l in lines), lines)
        self.assertTrue(any(l.startswith("s3:b: dismiss — not a data dependency (gone)")
                            for l in lines), lines)
        self.assertTrue(any(l.startswith("rds:c (c): add rds c") for l in lines), lines)
        self.assertTrue(any("attach consumer" in l and "orders" in l for l in lines), lines)



class Cl6RoundOneTest(unittest.TestCase):
    """Regressions from the first adversarial review round of the guess
    change."""

    GUESS = {"charts/orders/Chart.yaml": "apiVersion: v2\nname: orders\n",
             "charts/orders/values.yaml": "invoiceBucket: acme-invoice-archive\n"}
    POLICY = {"iam.tf": ('resource "aws_iam_policy" "p" {\n  policy = jsonencode({ Resource = '
                         '"arn:aws:s3:::acme-invoice-archive" })\n}\n')}

    def _scan(self, files):
        with tempfile.TemporaryDirectory() as root:
            for rel_path, content in files.items():
                full = os.path.join(root, rel_path)
                os.makedirs(os.path.dirname(full), exist_ok=True)
                with open(full, "w") as f:
                    f.write(content)
            inventory = {"data_dependencies": []}
            return datastores.harvest_datastores(inventory, root).scanned

    def test_a_confirmation_follows_the_guess_onto_the_fact_that_answers_it(self):
        document = _document(_record(
            overrides.CONFIRM, "s3:acme-invoice-archive", disposition="keep-in-aws",
            note="finance owns it", directory=None))
        section, notes = overrides.rebuild(self._scan(self.POLICY), document)
        (entry,) = section
        self.assertEqual((entry["detection"], entry["disposition"]), ("referenced", "keep-in-aws"))
        self.assertTrue(any("finance owns it" in n for n in entry["notes"]), entry["notes"])
        self.assertFalse(any("could not be placed" in n for n in notes), notes)

    def test_a_confirmation_without_a_disposition_keeps_a_declared_grade(self):
        declared = {"service": "s3", "identifier": "tf-state", "address": "aws_s3_bucket.state",
                    "detection": "declared", "disposition": "undecided",
                    "evidence": ["s3.tf"], "consumers": [], "notes": []}
        section, _ = overrides.rebuild([declared], _document(_record(
            overrides.CONFIRM, "aws_s3_bucket.state", directory="", note="ours")))
        self.assertEqual((section[0]["detection"], section[0]["disposition"]),
                         ("declared", "undecided"))
        # A guess still takes the table default: the question was answered.
        section, _ = overrides.rebuild(self._scan(self.GUESS), _document(_record(
            overrides.CONFIRM, "s3:acme-invoice-archive", directory=None)))
        self.assertEqual((section[0]["detection"], section[0]["disposition"]),
                         (overrides.HUMAN_DETECTION, "migrate"))

    def test_a_dismissal_applies_whatever_directory_an_older_record_carries(self):
        # The review used to record the guess's `evidence[0]` directory; a
        # later scan meeting the value in an earlier-sorting file moved it,
        # the dismissal was "satisfied", and the guess stood again.
        moved = dict(self.GUESS, **{"apps/orders/deploy.yaml": '''
apiVersion: apps/v1
kind: Deployment
metadata:
  name: orders
spec:
  template:
    spec:
      containers:
        - name: orders
          env:
            - name: INVOICE_BUCKET
              value: acme-invoice-archive
'''})
        scanned = self._scan(moved)
        self.assertEqual(scanned[0]["evidence"][0], "apps/orders/deploy.yaml")
        section, notes = overrides.rebuild(scanned, _document(_record(
            overrides.DISMISS, "s3:acme-invoice-archive", reason="gone",
            directory="charts/orders")))
        self.assertEqual(section, [])
        self.assertTrue(any("1 guess(es) dismissed" in n for n in notes), notes)

    def test_two_confirmations_meet_through_the_handle_pairs(self):
        document = _document(_record(overrides.CONFIRM, "s3:acme-invoice-archive",
                                     directory="charts/orders", note="first"))
        replaced = overrides.record_override(document, _record(
            overrides.CONFIRM, "s3:acme-invoice-archive", directory=None, note="second"))
        self.assertEqual([r["note"] for r in replaced], ["first"])
        self.assertEqual([r["note"] for r in document["overrides"]], ["second"])



class Cl6RoundTwoTest(unittest.TestCase):
    """Regressions from the second adversarial review round."""

    def test_a_confirmation_does_not_retire_the_addition_that_creates_the_entry(self):
        document = _document(_record(
            overrides.ADD, "s3:acme-archive", service="s3", identifier="acme-archive",
            note="declared in CloudFormation", directory=""))
        replaced = overrides.record_override(document, _record(
            overrides.CONFIRM, "s3:acme-archive", disposition="keep-in-aws", directory=None))
        # Round 11: the addition is not retired, but its (default) disposition
        # is taken over by the confirmation's and reported as replaced.
        self.assertEqual([r["kind"] for r in replaced], [overrides.ADD])
        self.assertEqual([r["kind"] for r in document["overrides"]],
                         [overrides.ADD, overrides.CONFIRM])
        self.assertEqual(document["overrides"][0]["disposition"], "keep-in-aws")
        section, notes = overrides.rebuild([], document)
        (entry,) = section
        self.assertEqual((entry["detection"], entry["disposition"]),
                         (overrides.HUMAN_DETECTION, "keep-in-aws"))
        self.assertFalse(any("could not be placed" in n for n in notes), notes)
        # A dismissal still retires both — and, having withdrawn the addition,
        # is not stored itself (round 8): nothing is left for it to keep out.
        replaced = overrides.record_override(document, _record(overrides.DISMISS, "s3:acme-archive",
                                                               reason="gone"))
        self.assertEqual(sorted(r["kind"] for r in replaced), [overrides.ADD, overrides.CONFIRM])
        self.assertEqual(document["overrides"], [])

    def test_a_confirmation_lands_on_the_console_form_that_answered_the_guess(self):
        arn = "arn:aws:secretsmanager:us-east-1:111111111111:secret:acme/db-AbC1dE"
        referenced = datastores._referenced_entry(datastores.find_arns(f'"{arn}"')[0], "iam.tf")
        section, notes = overrides.rebuild([referenced], _document(_record(
            overrides.CONFIRM, "secretsmanager:acme/db", disposition="keep-in-aws",
            directory=None)))
        self.assertEqual(section[0]["disposition"], "keep-in-aws")
        self.assertFalse(any("could not be placed" in n for n in notes), notes)
        self.assertIs(overrides.entry_twin({"data_dependencies": [referenced]},
                                           "ssm", "x"), None)

    def test_an_addition_beside_two_ambiguous_entries_creates_no_third(self):
        queue = "arn:aws:sqs:us-east-1:{}:orders"
        entries = [datastores._referenced_entry(datastores.find_arns(f'"{queue.format(a)}"')[0],
                                                "iam.tf") for a in ("222222222222", "333333333333")]
        document = _document(_record(
            overrides.ADD, "sqs:orders", service="sqs", identifier="orders", directory=""))
        section, notes = overrides.rebuild(entries, document)
        self.assertEqual(len(section), 2)
        note = next(n for n in notes if "could not be placed" in n)
        self.assertIn("keeps them apart", note)
        self.assertEqual(len(overrides.entry_twins({"data_dependencies": entries}, "sqs", "orders")), 2)



class Cl6RoundThreeTest(unittest.TestCase):
    """Regressions from the third adversarial review round."""

    def _added(self):
        return _document(_record(overrides.ADD, "s3:x", service="s3", identifier="x", directory=""))

    def test_a_repeated_confirmation_carries_the_earlier_fields_forward(self):
        document = self._added()
        overrides.record_override(document, _record(
            overrides.CONFIRM, "s3:x", disposition="keep-in-aws", note="finance owns it",
            directory=None))
        overrides.record_override(document, _record(
            overrides.CONFIRM, "s3:x", workload="billing", consumer_kind="Deployment",
            directory=None))
        confirms = [r for r in document["overrides"] if r["kind"] == overrides.CONFIRM]
        self.assertEqual(len(confirms), 1)
        self.assertEqual((confirms[0]["disposition"], confirms[0]["note"]),
                         ("keep-in-aws", "finance owns it"))
        self.assertEqual([r["workload"] for r in document["overrides"]
                          if r["kind"] == overrides.ATTACH], ["billing"])
        section, _ = overrides.rebuild([], document)
        self.assertEqual(section[0]["disposition"], "keep-in-aws")
        self.assertEqual([c["workload"] for c in section[0]["consumers"]], ["billing"])

    def test_one_disposition_stands_per_entry_whichever_verb_set_it(self):
        document = self._added()
        overrides.record_override(document, _record(
            overrides.ANNOTATE, "s3:x", disposition="keep-in-aws", note="finance owns it",
            directory=None))
        replaced = overrides.record_override(document, _record(
            overrides.CONFIRM, "s3:x", disposition="migrate", directory=None))
        self.assertEqual([r["disposition"] for r in replaced if r["kind"] == overrides.ANNOTATE],
                         ["keep-in-aws"])
        annotations = [r for r in document["overrides"] if r["kind"] == overrides.ANNOTATE]
        self.assertEqual([(r.get("disposition"), r.get("note")) for r in annotations],
                         [(None, "finance owns it")])
        section, _ = overrides.rebuild([], document)
        self.assertEqual(section[0]["disposition"], "migrate")
        # And back: an annotation retires the confirmation's disposition.
        replaced = overrides.record_override(document, _record(
            overrides.ANNOTATE, "s3:x", disposition="keep-in-aws", directory=None))
        self.assertIn(overrides.CONFIRM, [r["kind"] for r in replaced])
        confirms = [r for r in document["overrides"] if r["kind"] == overrides.CONFIRM]
        self.assertEqual(len(confirms), 1)
        self.assertNotIn("disposition", confirms[0])
        section, _ = overrides.rebuild([], document)
        self.assertEqual(section[0]["disposition"], "keep-in-aws")
        lines = overrides.summarize(document)
        # The entry carries one disposition; the listing may show the
        # addition's moved one beside the annotation that moved it — agreeing.
        stated = [l for l in lines if "disposition" in l]
        self.assertTrue(all("keep-in-aws" in l for l in stated), lines)
        self.assertFalse(any("migrate" in l for l in stated), lines)



class Cl6RoundFourTest(unittest.TestCase):
    """Regressions from the fourth adversarial review round."""

    def test_an_annotation_that_trims_a_confirmation_leaves_a_remnant_not_a_new_decision(self):
        document = _document(_record(
            overrides.ADD, "s3:x", service="s3", identifier="x", directory=""))
        overrides.record_override(document, _record(
            overrides.CONFIRM, "s3:x", disposition="keep-in-aws", directory=None))
        overrides.record_override(document, _record(
            overrides.ANNOTATE, "s3:x", disposition="migrate", directory=None))
        (confirm,) = [r for r in document["overrides"] if r["kind"] == overrides.CONFIRM]
        self.assertNotIn("disposition", confirm)
        self.assertIn("replaced_from", confirm)



class Cl6RoundFiveTest(unittest.TestCase):
    """Regressions from the fifth adversarial review round: a consumer named
    on a confirmation is a link record, and a guess's handle reaches the fact
    that answered it."""

    GUESS = {"charts/orders/Chart.yaml": "apiVersion: v2\nname: orders\n",
             "charts/orders/values.yaml": "invoiceBucket: acme-invoice-archive\n"}

    def _scan(self, files):
        with tempfile.TemporaryDirectory() as root:
            for rel_path, content in files.items():
                full = os.path.join(root, rel_path)
                os.makedirs(os.path.dirname(full), exist_ok=True)
                with open(full, "w") as f:
                    f.write(content)
            inventory = {"data_dependencies": []}
            return datastores.harvest_datastores(inventory, root).scanned

    def test_a_reject_and_a_confirm_of_one_link_never_stand_together(self):
        document = _document(_record(overrides.REJECT, "s3:acme-invoice-archive",
                                     workload="orders", reason="wrong team"))
        replaced = overrides.record_override(document, _record(
            overrides.CONFIRM, "s3:acme-invoice-archive", workload="orders"))
        self.assertEqual([r["kind"] for r in replaced], [overrides.REJECT])
        self.assertEqual(sorted(r["kind"] for r in document["overrides"]),
                         [overrides.ATTACH, overrides.CONFIRM])
        replaced = overrides.record_override(document, _record(
            overrides.REJECT, "s3:acme-invoice-archive", workload="orders", reason="no"))
        self.assertEqual([r["kind"] for r in replaced], [overrides.ATTACH])
        section, _ = overrides.rebuild(self._scan(self.GUESS), document)
        self.assertEqual(section[0]["consumers"], [])

    def test_a_note_only_confirm_never_flips_the_disposition(self):
        document = _document(_record(overrides.CONFIRM, "s3:x", workload="a",
                                     disposition="keep-in-aws", directory=None))
        overrides.record_override(document, _record(
            overrides.CONFIRM, "s3:x", workload="b", disposition="migrate", directory=None))
        overrides.record_override(document, _record(
            overrides.CONFIRM, "s3:x", workload="a", note="owned by team a", directory=None))
        confirms = [r for r in document["overrides"] if r["kind"] == overrides.CONFIRM]
        self.assertEqual([(r["disposition"], r["note"]) for r in confirms],
                         [("migrate", "owned by team a")])
        self.assertEqual(sum("disposition" in l for l in overrides.summarize(document)), 1)

    def test_a_record_made_under_the_guess_handle_reaches_the_fact(self):
        arn = "arn:aws:s3:::acme-invoice-archive"
        fact = datastores._referenced_entry(datastores.find_arns(f'"{arn}"')[0], "iam.tf")
        inventory = {"data_dependencies": [fact]}
        self.assertEqual(overrides.entries_at(inventory, "s3:acme-invoice-archive"), [fact])
        document = _document(
            _record(overrides.ANNOTATE, "s3:acme-invoice-archive", note="asked finance",
                    directory=None),
            _record(overrides.CONFIRM, "s3:acme-invoice-archive", disposition="keep-in-aws",
                    workload="billing", directory=None))
        section, notes = overrides.rebuild([fact], document)
        (entry,) = section
        self.assertEqual(entry["disposition"], "keep-in-aws")
        self.assertTrue(any("asked finance" in n for n in entry["notes"]))
        self.assertEqual([c["workload"] for c in entry["consumers"]], ["billing"])
        self.assertFalse(any("could not be placed" in n for n in notes), notes)

    def test_a_hand_added_entry_without_a_consumer_is_not_unattributed(self):
        from . import consumers
        section, notes = overrides.rebuild([], _document(_record(
            overrides.ADD, "dynamodb:sessions", service="dynamodb", identifier="sessions",
            directory="")))
        self.assertIn(consumers.ADDED_NOTE, section[0]["notes"])
        self.assertNotIn(consumers.UNATTRIBUTED_NOTE, section[0]["notes"])
        self.assertFalse(any("could not be attributed" in n for n in notes), notes)



class Cl6RoundSixTest(unittest.TestCase):
    """Regressions from the sixth adversarial review round: the guess-handle
    bridge is a lookup, not an alias, and a replayed dismissal never deletes
    a fact."""

    def _scan(self, files):
        with tempfile.TemporaryDirectory() as root:
            for rel_path, content in files.items():
                full = os.path.join(root, rel_path)
                os.makedirs(os.path.dirname(full), exist_ok=True)
                with open(full, "w") as f:
                    f.write(content)
            inventory = {"data_dependencies": []}
            return datastores.harvest_datastores(inventory, root).scanned

    def test_a_replayed_dismissal_never_deletes_the_fact_that_answered_the_guess(self):
        document = _document(_record(overrides.DISMISS, "s3:acme-invoice-archive",
                                     reason="stale value", directory=None))
        for files in ({"s3.tf": 'resource "aws_s3_bucket" "archive" {\n  bucket = "acme-invoice-archive"\n}\n'},
                      {"iam.tf": ('resource "aws_iam_policy" "p" {\n  policy = jsonencode({ Resource = '
                                  '"arn:aws:s3:::acme-invoice-archive" })\n}\n')}):
            with self.subTest(files=list(files)):
                section, notes = overrides.rebuild(self._scan(files), document)
                self.assertEqual(len(section), 1)
                self.assertIn(section[0]["detection"], ("declared", "referenced"))
                self.assertFalse(any("could not be placed" in n for n in notes), notes)

    def test_a_decision_on_prods_block_cannot_retire_one_on_devs(self):
        # Records as `_amend` builds them: block address, directory, no
        # `<service>:<identifier>` alias — that handle is a lookup, not an
        # alias, or both queues would share it.
        document = _document(_record(
            overrides.ANNOTATE, "aws_sqs_queue.orders", directory="envs/dev",
            disposition="keep-in-aws", note="dev stays"))
        replaced = overrides.record_override(document, _record(
            overrides.CONFIRM, "aws_sqs_queue.orders", directory="envs/prod",
            disposition="migrate"))
        self.assertEqual(replaced, [])
        self.assertEqual(sorted(r["disposition"] for r in document["overrides"]),
                         ["keep-in-aws", "migrate"])
        replaced = overrides.record_override(document, _record(
            overrides.CONFIRM, "aws_sqs_queue.orders", directory="envs/dev", note="x"))
        self.assertEqual([r["kind"] for r in replaced], [])
        # A CONFIRM against a foreign spelling of the name does not retire the
        # declared queue's either.
        replaced = overrides.record_override(document, _record(
            overrides.CONFIRM, "arn:aws:sqs:us-east-1:999999999999:orders",
            disposition="keep-in-aws", directory=None))
        self.assertEqual(replaced, [])

    def test_a_dismissal_still_retires_the_link_records_on_its_guess(self):
        document = _document(
            _record(overrides.REJECT, "s3:acme-invoice-archive", workload="orders", reason="no"),
            _record(overrides.ANNOTATE, "s3:acme-invoice-archive", note="maybe"))
        replaced = overrides.record_override(document, _record(
            overrides.DISMISS, "s3:acme-invoice-archive", reason="gone"))
        self.assertEqual(sorted(r["kind"] for r in replaced), [overrides.ANNOTATE, overrides.REJECT])
        # But not a REJECT recorded on a same-named DECLARED queue elsewhere.
        document = _document(_record(overrides.REJECT, "aws_sqs_queue.orders",
                                     directory="envs/dev", workload="orders", reason="no"))
        replaced = overrides.record_override(document, _record(
            overrides.DISMISS, "sqs:orders", reason="gone", directory=None))
        self.assertEqual(replaced, [])

    def test_a_confirmations_link_keeps_the_sibling_disambiguator(self):
        scanned = self._scan({
            "s3.tf": 'resource "aws_s3_bucket" "b" {\n  bucket = "one"\n}\n',
            "s3_override.tf": 'resource "aws_s3_bucket" "b" {\n  bucket = "two"\n}\n'})
        self.assertEqual(len(scanned), 2)
        document = _document(_record(
            overrides.CONFIRM, "aws_s3_bucket.b", identifier="one", directory="",
            workload="billing", consumer_kind="Deployment"))
        (link,) = [r for r in document["overrides"] if r["kind"] == overrides.ATTACH]
        self.assertEqual(link.get("identifier"), "one")
        section, notes = overrides.rebuild(scanned, document)
        by_id = {e["identifier"]: e for e in section}
        self.assertEqual([c["workload"] for c in by_id["one"]["consumers"]], ["billing"])
        self.assertFalse(any("could not be placed" in n for n in notes), notes)

    def test_a_block_records_alias_hop_never_lands_on_a_guess(self):
        scanned = self._scan({"charts/web/Chart.yaml": "apiVersion: v2\nname: web\n",
                              "charts/web/values.yaml": "sqsQueue: orders\n"})
        self.assertEqual(scanned[0]["detection"], "inferred")
        document = _document(_record(
            overrides.ANNOTATE, "aws_sqs_queue.orders", directory="envs/dev",
            aliases=[{"address": "sqs:orders", "directory": None}], disposition="keep-in-aws"))
        section, notes = overrides.rebuild(scanned, document)
        self.assertEqual((section[0]["detection"], section[0]["disposition"]),
                         ("inferred", "undecided"))

    def test_a_hand_added_entry_whose_consumer_was_rejected_is_the_rejected_case(self):
        from . import consumers
        document = _document(_record(
            overrides.ADD, "dynamodb:sessions", service="dynamodb", identifier="sessions",
            workload="web", consumer_kind="Deployment", directory=""))
        overrides.record_override(document, _record(
            overrides.REJECT, "dynamodb:sessions", workload="web", reason="not web",
            directory=None))
        section, _ = overrides.rebuild([], document)
        self.assertEqual(section[0]["consumers"], [])
        # The withdrawal is recorded, and the added-entry note does not claim
        # that no consumer was ever named; the scan's "no chain reached it"
        # note is not written for an entry the scan never produced.
        self.assertTrue(any("was withdrawn" in n for n in section[0]["notes"]), section[0]["notes"])
        self.assertIn(consumers.ADDED_NOTE, section[0]["notes"])
        self.assertNotIn(consumers.UNATTRIBUTED_NOTE, section[0]["notes"])



class Cl6RoundSevenTest(unittest.TestCase):
    """Regressions from the seventh adversarial review round."""

    GUESS = {"charts/orders/Chart.yaml": "apiVersion: v2\nname: orders\n",
             "charts/orders/values.yaml": "invoiceBucket: acme-invoice-archive\n"}
    DECLARED = {"envs/prod/s3.tf": 'resource "aws_s3_bucket" "archive" {\n  bucket = "acme-invoice-archive"\n}\n'}

    def _scan(self, files):
        with tempfile.TemporaryDirectory() as root:
            for rel_path, content in files.items():
                full = os.path.join(root, rel_path)
                os.makedirs(os.path.dirname(full), exist_ok=True)
                with open(full, "w") as f:
                    f.write(content)
            inventory = {"data_dependencies": []}
            return datastores.harvest_datastores(inventory, root).scanned

    def test_a_guess_era_dismissal_is_retired_by_a_decision_on_the_fact(self):
        # Dismissed as a guess; a later scan declares it; the reviewer keeps
        # it in AWS; the declaration leaves the scope again: the guess stands
        # with the reviewer's LAST decision, not deleted by the first.
        document = _document(_record(overrides.DISMISS, "s3:acme-invoice-archive",
                                     reason="stale", directory=None))
        fact_scan = self._scan(dict(self.GUESS, **self.DECLARED))
        (fact,) = fact_scan
        self.assertEqual(fact["detection"], "declared")
        annotate = _record(overrides.ANNOTATE, "aws_s3_bucket.archive", directory="envs/prod",
                           disposition="keep-in-aws", note="finance keeps it")
        overrides.record_override(document, annotate)
        retired = overrides.retire_bridged(document, {"data_dependencies": fact_scan},
                                           fact, annotate)
        self.assertEqual([r["kind"] for r in retired], [overrides.DISMISS])
        section, notes = overrides.rebuild(self._scan(self.GUESS), document)
        self.assertEqual([e["detection"] for e in section], ["inferred"])
        self.assertFalse(any("guess(es) dismissed" in n for n in notes), notes)

    def test_a_handle_reaching_two_entries_retires_nothing(self):
        dev = {"service": "sqs", "identifier": "orders", "address": "aws_sqs_queue.orders",
               "detection": "declared", "evidence": ["envs/dev/main.tf"], "consumers": [], "notes": []}
        prod = dict(dev, evidence=["envs/prod/main.tf"])
        document = _document(_record(overrides.DISMISS, "sqs:orders", reason="x", directory=None))
        annotate = _record(overrides.ANNOTATE, "aws_sqs_queue.orders", directory="envs/prod",
                           disposition="keep-in-aws")
        retired = overrides.retire_bridged(document, {"data_dependencies": [dev, prod]},
                                           prod, annotate)
        self.assertEqual(retired, [])
        self.assertEqual(len(document["overrides"]), 1)

    def test_a_guess_era_rejection_is_retired_by_an_attachment_of_the_link(self):
        document = _document(_record(overrides.REJECT, "s3:acme-invoice-archive",
                                     workload="orders", reason="no", directory=None))
        (fact,) = self._scan(self.DECLARED)
        attach = _record(overrides.ATTACH, "aws_s3_bucket.archive", directory="envs/prod",
                         workload="orders")
        retired = overrides.retire_bridged(document, {"data_dependencies": [fact]}, fact, attach)
        self.assertEqual([r["kind"] for r in retired], [overrides.REJECT])
        # A different workload leaves it standing.
        document = _document(_record(overrides.REJECT, "s3:acme-invoice-archive",
                                     workload="billing", reason="no", directory=None))
        self.assertEqual(overrides.retire_bridged(document, {"data_dependencies": [fact]},
                                                  fact, attach), [])

    def test_the_rollup_counts_a_satisfied_dismissal_apart(self):
        document = _document(_record(overrides.DISMISS, "s3:acme-invoice-archive",
                                     reason="stale", directory=None))
        _, notes = overrides.rebuild(self._scan(self.DECLARED), document)
        rollup = next(n for n in notes if "were replayed" in n)
        self.assertIn("1 dismissal(s) already satisfied", rollup)
        self.assertNotIn("guess(es) dismissed", rollup)
        section, notes = overrides.rebuild(self._scan(self.GUESS), document)
        self.assertEqual(section, [])
        self.assertIn("1 guess(es) dismissed", next(n for n in notes if "were replayed" in n))



class Cl6RoundEightTest(unittest.TestCase):
    """Regressions from the eighth adversarial review round."""

    def _fact(self):
        return {"service": "sqs", "identifier": "orders", "address": "aws_sqs_queue.orders",
                "detection": "declared", "disposition": "replatform",
                "evidence": ["envs/prod/sqs.tf"], "consumers": [], "notes": []}

    def test_an_attachment_of_another_chain_does_not_un_reject_through_the_bridge(self):
        fact = self._fact()
        document = _document(_record(overrides.REJECT, "sqs:orders", workload="orders",
                                     consumer_kind="service_account", reason="over-broad",
                                     directory=None))
        attach = _record(overrides.ATTACH, "aws_sqs_queue.orders", directory="envs/prod",
                         workload="orders", consumer_kind="helm_release")
        self.assertEqual(overrides.retire_bridged(document, {"data_dependencies": [fact]},
                                                  fact, attach), [])
        same_chain = dict(attach, consumer_kind="service_account")
        self.assertEqual([r["kind"] for r in overrides.retire_bridged(
            document, {"data_dependencies": [fact]}, fact, same_chain)], [overrides.REJECT])

    def test_an_addition_always_survives_a_disposition_take_over(self):
        arn = "arn:aws:s3:::acme-archive"
        fact = datastores._referenced_entry(datastores.find_arns(f'"{arn}"')[0], "iam.tf")
        document = _document(_record(overrides.ADD, "s3:acme-archive", service="s3",
                                     identifier="acme-archive", disposition="keep-in-aws",
                                     directory=""))
        annotate = _record(overrides.ANNOTATE, arn, disposition="migrate", directory=None)
        overrides.record_override(document, annotate)
        retired = overrides.retire_bridged(document, {"data_dependencies": [fact]}, fact, annotate)
        self.assertEqual([r["kind"] for r in retired], [overrides.ADD])
        adds = [r for r in document["overrides"] if r["kind"] == overrides.ADD]
        self.assertEqual(len(adds), 1)
        # Round 9: the addition TAKES the winning disposition, since it is the
        # record that re-creates the entry.
        self.assertEqual(adds[0]["disposition"], "migrate")
        # The fact leaves: the hand-added entry is still there, so graded.
        section, _ = overrides.rebuild([], document)
        self.assertEqual([(e["address"], e["disposition"]) for e in section],
                         [("s3:acme-archive", "migrate")])

    def test_dismissing_a_hand_added_entry_withdraws_the_addition_and_stores_nothing(self):
        document = _document(_record(overrides.ADD, "s3:reports-archive", service="s3",
                                     identifier="reports-archive", directory=""))
        replaced = overrides.record_override(document, _record(
            overrides.DISMISS, "s3:reports-archive", reason="added by mistake", directory=None))
        self.assertEqual([r["kind"] for r in replaced], [overrides.ADD])
        self.assertEqual(document["overrides"], [])
        section, notes = overrides.rebuild([], document)
        self.assertEqual(section, [])
        self.assertFalse(any("were replayed" in n for n in notes), notes)



class Cl6RoundNineTest(unittest.TestCase):
    """Regressions from the ninth adversarial review round."""

    def test_an_addition_never_lands_on_a_foreign_or_contradicting_twin(self):
        foreign = datastores._referenced_entry(datastores.find_arns(
            '"arn:aws:sqs:us-east-1:999999999999:orders"')[0], "iam.tf")
        foreign["notes"].append(datastores.CROSS_ACCOUNT_NOTE_PREFIX + "999999999999 …")
        document = _document(_record(
            overrides.ADD, "sqs:orders", service="sqs", identifier="orders",
            arn="arn:aws:sqs:us-east-1:111111111111:orders", disposition="migrate", directory=""))
        section, notes = overrides.rebuild([foreign], document)
        self.assertEqual(sorted(e["address"] for e in section),
                         ["arn:aws:sqs:us-east-1:999999999999:orders", "sqs:orders"])
        mine = next(e for e in section if e["address"] == "sqs:orders")
        self.assertEqual((mine["disposition"], mine["account"]), ("migrate", "111111111111"))
        self.assertNotIn("added at the data review", " ".join(foreign_note for foreign_note in
                         next(e for e in section if e is not mine)["notes"]))
        # A same-named entry with no verdict and no contradicting ARN is still
        # the twin the addition lands on.
        plain = datastores._referenced_entry(datastores.find_arns(
            '"arn:aws:sqs:us-east-1:111111111111:orders"')[0], "iam.tf")
        section, _ = overrides.rebuild([plain], document)
        self.assertEqual([e["address"] for e in section], ["arn:aws:sqs:us-east-1:111111111111:orders"])

    def test_an_additions_remnant_carries_the_reviewers_last_disposition(self):
        arn = "arn:aws:s3:::acme-archive"
        fact = datastores._referenced_entry(datastores.find_arns(f'"{arn}"')[0], "iam.tf")
        document = _document(_record(overrides.ADD, "s3:acme-archive", service="s3",
                                     identifier="acme-archive", disposition="migrate",
                                     directory=""))
        annotate = _record(overrides.ANNOTATE, arn, disposition="keep-in-aws", directory=None)
        overrides.record_override(document, annotate)
        overrides.retire_bridged(document, {"data_dependencies": [fact]}, fact, annotate)
        (add,) = [r for r in document["overrides"] if r["kind"] == overrides.ADD]
        self.assertEqual(add["disposition"], "keep-in-aws")
        section, _ = overrides.rebuild([], document)
        self.assertEqual(section[0]["disposition"], "keep-in-aws")
        # The annotate branch's own trim moves the disposition the same way.
        document = _document(_record(overrides.ADD, "s3:x", service="s3", identifier="x",
                                     disposition="migrate", directory=""))
        overrides.record_override(document, _record(overrides.ANNOTATE, "s3:x",
                                                    disposition="keep-in-aws", directory=None))
        (add,) = [r for r in document["overrides"] if r["kind"] == overrides.ADD]
        self.assertEqual(add["disposition"], "keep-in-aws")



class Cl6RoundTenTest(unittest.TestCase):
    """Regressions from the tenth adversarial review round."""

    def test_the_typed_handle_alias_is_never_followed_at_replay(self):
        arn = "arn:aws:sqs:us-east-1:111111111111:orders"
        referenced = datastores._referenced_entry(datastores.find_arns(f'"{arn}"')[0], "iam.tf")
        document = _document(_record(
            overrides.ANNOTATE, "aws_sqs_queue.orders", directory="envs/prod",
            aliases=[{"address": "sqs:orders", "directory": None}], disposition="keep-in-aws"))
        section, notes = overrides.rebuild([referenced], document)
        self.assertEqual(section[0]["disposition"], "replatform")
        self.assertTrue(any("could not be placed" in n for n in notes), notes)
        # The ARN alias — the un-fold bridge — still works.
        document = _document(_record(
            overrides.ANNOTATE, "aws_sqs_queue.orders", directory="envs/prod",
            aliases=[{"address": arn, "directory": None}], disposition="keep-in-aws"))
        section, _ = overrides.rebuild([referenced], document)
        self.assertEqual(section[0]["disposition"], "keep-in-aws")

    def test_a_confirmations_disposition_moves_onto_the_addition(self):
        document = _document(_record(overrides.ADD, "s3:x", service="s3", identifier="x",
                                     disposition="migrate", directory=""))
        replaced = overrides.record_override(document, _record(
            overrides.CONFIRM, "s3:x", disposition="keep-in-aws", directory=None))
        self.assertEqual([(r["kind"], r["disposition"]) for r in replaced],
                         [(overrides.ADD, "migrate")])
        self.assertEqual(sorted((r["kind"], r["disposition"]) for r in document["overrides"]),
                         [(overrides.ADD, "keep-in-aws"), (overrides.CONFIRM, "keep-in-aws")])
        self.assertEqual(sum("disposition" in l for l in overrides.summarize(document)), 2)
        section, _ = overrides.rebuild([], document)
        self.assertEqual(section[0]["disposition"], "keep-in-aws")



class Cl6RoundElevenTest(unittest.TestCase):
    """Regressions from the eleventh adversarial review round."""

    def test_an_addition_without_a_typed_disposition_still_takes_the_winner(self):
        arn = "arn:aws:s3:::acme-invoice-archive"
        fact = datastores._referenced_entry(datastores.find_arns(f'"{arn}"')[0], "iam.tf")
        document = _document(_record(overrides.ADD, "s3:acme-invoice-archive", service="s3",
                                     identifier="acme-invoice-archive", directory=""))
        annotate = _record(overrides.ANNOTATE, arn, disposition="keep-in-aws", directory=None)
        overrides.record_override(document, annotate)
        retired = overrides.retire_bridged(document, {"data_dependencies": [fact]}, fact, annotate)
        self.assertEqual([r["kind"] for r in retired], [overrides.ADD])
        section, _ = overrides.rebuild([], document)
        self.assertEqual(section[0]["disposition"], "keep-in-aws")
        # The annotate branch and the confirm branch move it the same way.
        for kind in (overrides.ANNOTATE, overrides.CONFIRM):
            with self.subTest(kind=kind):
                document = _document(_record(overrides.ADD, "s3:x", service="s3",
                                             identifier="x", directory=""))
                overrides.record_override(document, _record(kind, "s3:x",
                                                            disposition="keep-in-aws", directory=None))
                (add,) = [r for r in document["overrides"] if r["kind"] == overrides.ADD]
                self.assertEqual(add["disposition"], "keep-in-aws")

    def test_the_bridge_refuses_an_entry_the_verdicts_place_elsewhere(self):
        foreign = datastores._referenced_entry(datastores.find_arns(
            '"arn:aws:sqs:us-east-1:999999999999:orders"')[0], "iam.tf")
        foreign["notes"].append(datastores.CROSS_ACCOUNT_NOTE_PREFIX + "999999999999 …")
        document = _document(
            _record(overrides.CONFIRM, "sqs:orders", disposition="keep-in-aws", directory=None),
            _record(overrides.ATTACH, "sqs:orders", workload="orders-web", directory=None),
            _record(overrides.DISMISS, "sqs:x", reason="gone", directory=None))
        section, notes = overrides.rebuild([foreign], document)
        self.assertEqual((section[0]["disposition"], section[0]["consumers"]),
                         ("replatform", []))
        note = next(n for n in notes if "could not be placed" in n)
        self.assertIn("recorded against a guess", note)
        self.assertIn("is not the resource the decision was about", note)

    def test_a_defaulted_disposition_is_noted_as_defaulted(self):
        with tempfile.TemporaryDirectory() as root:
            os.makedirs(os.path.join(root, "charts/orders"))
            with open(os.path.join(root, "charts/orders/Chart.yaml"), "w") as f:
                f.write("apiVersion: v2\nname: orders\n")
            with open(os.path.join(root, "charts/orders/values.yaml"), "w") as f:
                f.write("invoiceBucket: acme-invoice-archive\n")
            inventory = {"data_dependencies": []}
            scanned = datastores.harvest_datastores(inventory, root).scanned
        section, _ = overrides.rebuild(scanned, _document(_record(
            overrides.CONFIRM, "s3:acme-invoice-archive", directory=None)))
        self.assertTrue(any(n.startswith("disposition defaulted to 'migrate' for s3")
                            for n in section[0]["notes"]), section[0]["notes"])
        # An addition that lands on the guess makes the entry the reviewer's.
        section, _ = overrides.rebuild(scanned, _document(_record(
            overrides.ADD, "s3:acme-invoice-archive", service="s3",
            identifier="acme-invoice-archive", directory="")))
        self.assertTrue(overrides.added_by_hand(section[0]))



class Cl6RoundTwelveTest(unittest.TestCase):
    """Regressions from the twelfth adversarial review round."""

    def test_a_superseded_addition_stub_names_the_entry_and_says_the_disposition_moved(self):
        document = _document(_record(overrides.ADD, "s3:acme-exports", service="s3",
                                     identifier="acme-exports", directory=""))
        replaced = overrides.record_override(document, _record(
            overrides.CONFIRM, "s3:acme-exports", disposition="keep-in-aws", directory=None))
        (line,) = overrides.summarize({"overrides": replaced})
        self.assertIn("add s3 acme-exports", line)
        self.assertIn("gave way to the newer decision", line)
        self.assertNotIn("None", line)
        replaced = overrides.record_override(document, _record(
            overrides.ANNOTATE, "s3:acme-exports", disposition="migrate", directory=None))
        lines = overrides.summarize({"overrides": replaced})
        self.assertTrue(any("add s3 acme-exports" in l and "gave way" in l for l in lines), lines)



class Cl6RoundThirteenTest(unittest.TestCase):
    """Regressions from the thirteenth adversarial review round."""

    def test_the_bridge_in_retire_bridged_tolerates_a_console_form(self):
        arn = "arn:aws:secretsmanager:us-east-1:111111111111:secret:acme/db-AbC123"
        fact = datastores._referenced_entry(datastores.find_arns(f'"{arn}"')[0], "iam.tf")
        document = _document(_record(overrides.ADD, "secretsmanager:acme/db", service="secretsmanager",
                                     identifier="acme/db", directory=""))
        annotate = _record(overrides.ANNOTATE, arn, disposition="keep-in-aws", directory=None)
        overrides.record_override(document, annotate)
        retired = overrides.retire_bridged(document, {"data_dependencies": [fact]}, fact, annotate)
        self.assertEqual([r["kind"] for r in retired], [overrides.ADD])
        section, _ = overrides.rebuild([], document)
        self.assertEqual(section[0]["disposition"], "keep-in-aws")

    def test_a_replayed_addition_beside_a_lone_ambiguous_spelling_is_refused(self):
        spelling = datastores._referenced_entry(datastores.find_arns(
            '"arn:aws:sqs:*:*:orders"')[0], "iam.tf")
        spelling["notes"].append(datastores.AMBIGUOUS_ACCOUNT_NOTE_PREFIX + "(…)")
        document = _document(_record(overrides.ADD, "sqs:orders", service="sqs",
                                     identifier="orders", directory=""))
        section, notes = overrides.rebuild([spelling], document)
        self.assertEqual(len(section), 1)
        self.assertNotIn("added at the data review", " ".join(section[0]["notes"]))
        self.assertTrue(any("keeps apart as ambiguous" in n for n in notes), notes)

    def test_a_dismissal_of_an_addition_that_landed_on_a_guess_is_kept_when_asked(self):
        document = _document(_record(overrides.ADD, "s3:x", service="s3", identifier="x",
                                     directory=""))
        replaced = overrides.record_override(document, _record(
            overrides.DISMISS, "s3:x", reason="mistake", directory=None), keep_dismissal=True)
        self.assertEqual([r["kind"] for r in replaced], [overrides.ADD])
        self.assertEqual([r["kind"] for r in document["overrides"]], [overrides.DISMISS])


if __name__ == "__main__":
    unittest.main()
