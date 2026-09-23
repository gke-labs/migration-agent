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

"""Unit tests for the data-migration outcome store (pure)."""

import unittest

from servers.phases.deployment import datamigration as dm
from servers.phases.discovery.discovery_init_1 import datastores


def _entry(service="rds", identifier="orders-db",
           address="aws_db_instance.orders", disposition="migrate",
           evidence=("envs/prod/data.tf",), consumers=()):
    return {
        "service": service,
        "identifier": identifier,
        "address": address,
        "disposition": disposition,
        "evidence": list(evidence),
        "consumers": [dict(c) for c in consumers],
        "notes": [],
    }


def _consumer(workload="orders", kind="helm_release"):
    return {"workload": workload, "kind": kind, "namespace": None,
            "source_path": None, "detection": "terraform_wiring",
            "evidence": "envs/prod/data.tf"}


NOW = "2026-09-01T10:00:00+00:00"
WHO = "platform-user@google.com"


class KeyTest(unittest.TestCase):

    def test_the_directory_separates_two_root_modules_at_one_address(self):
        """`envs/dev` and `envs/prod` both declaring `aws_db_instance.orders`
        are two databases. Keying on the address alone would report one moved
        when the other did."""
        dev = _entry(evidence=("envs/dev/data.tf",))
        prod = _entry(evidence=("envs/prod/data.tf",))

        self.assertNotEqual(dm.key_of(dev), dm.key_of(prod))

    def test_the_identifier_separates_two_entries_in_one_directory(self):
        """An `_override.tf` that renames leaves two entries sharing an address
        AND a directory; the identifier is what is left."""
        one = _entry(identifier="orders-db")
        two = _entry(identifier="orders-db-replica")

        self.assertNotEqual(dm.key_of(one), dm.key_of(two))

    def test_an_entry_declared_at_the_repository_root_keys_consistently(self):
        entry = _entry(evidence=("data.tf",))

        self.assertEqual(dm.key_of(entry),
                         ("aws_db_instance.orders", "", "orders-db"))


class RecordTest(unittest.TestCase):

    def test_recording_a_status_replaces_rather_than_appends(self):
        """Last write wins, with no supersession rules — the whole reason an
        operational outcome is kept out of the corrections store."""
        document = dm.empty_document()
        entry = _entry()

        dm.record_status(document, entry, dm.IN_PROGRESS, WHO, NOW)
        dm.record_status(document, entry, dm.MIGRATED, WHO, NOW,
                         target="orders-db-gcp")

        self.assertEqual(len(document["migrations"]), 1)
        self.assertEqual(document["migrations"][0]["status"], dm.MIGRATED)
        self.assertEqual(document["migrations"][0]["target"], "orders-db-gcp")

    def test_a_record_for_one_root_module_does_not_settle_the_other(self):
        document = dm.empty_document()
        dev = _entry(evidence=("envs/dev/data.tf",))
        prod = _entry(evidence=("envs/prod/data.tf",))

        dm.record_status(document, dev, dm.MIGRATED, WHO, NOW)

        self.assertEqual(dm.outstanding([dev, prod], document), [prod])

    def test_an_unknown_status_is_refused(self):
        with self.assertRaises(ValueError):
            dm.record_status(dm.empty_document(), _entry(), "done", WHO, NOW)

    def test_a_note_is_scoped_to_its_status_and_a_target_is_not(self):
        """A note describes the status it accompanies — "at 60%, cutover Tue"
        is a fact about a move in progress, and carrying it onto the
        completion says a finished migration is at 60%. Where the data landed
        is the same fact on both sides of the cutover."""
        document = dm.empty_document()
        entry = _entry()
        dm.record_status(document, entry, dm.IN_PROGRESS, WHO, NOW,
                         target="orders-db-gcp", note="DMS at 60%")

        _, carried, dropped = dm.record_status(
            document, entry, dm.MIGRATED, WHO, NOW)

        record = document["migrations"][0]
        self.assertNotIn("note", record)
        self.assertEqual(dropped, ["note"])
        self.assertEqual(record["target"], "orders-db-gcp")
        self.assertEqual(carried, ["target"])

        # Same status: the note is the current one and survives.
        _, carried, dropped = dm.record_status(
            document, entry, dm.MIGRATED, WHO, NOW, note="verified by hand")
        _, carried, dropped = dm.record_status(
            document, entry, dm.MIGRATED, WHO, NOW)
        self.assertEqual(document["migrations"][0]["note"],
                         "verified by hand")
        self.assertEqual(dropped, [])

    def test_an_empty_string_clears_a_field_and_silence_does_not(self):
        """Without this there is no call that takes a wrong note back out."""
        document = dm.empty_document()
        entry = _entry()
        dm.record_status(document, entry, dm.MIGRATED, WHO, NOW,
                         target="orders-db-gcp", note="wrong service")

        dm.record_status(document, entry, dm.MIGRATED, WHO, NOW, note="")

        self.assertNotIn("note", document["migrations"][0])
        self.assertEqual(document["migrations"][0]["target"], "orders-db-gcp")

    def test_a_whitespace_only_note_is_not_a_note(self):
        """Truthy, so it was stored, survived the carry-forward and defeated
        every `if record.get("note")` guard — leaving an empty "Note:" bullet
        in the artifact read before a component ships."""
        document = dm.empty_document()
        entry = _entry()
        dm.record_status(document, entry, dm.MIGRATED, WHO, NOW,
                         note="only the orders schema moved")

        dm.record_status(document, entry, dm.MIGRATED, WHO, NOW, note="   ")

        self.assertNotIn("note", document["migrations"][0])

    def test_an_unclosed_placeholder_is_not_a_destination(self):
        """Only the opening half was covered, so a target that opens a
        stand-in and never closes it — the shape a truncated paste actually
        produces — was written into the durable record of where customer data
        went."""
        self.assertTrue(dm.looks_like_a_placeholder("<the new instance>"))
        self.assertTrue(dm.looks_like_a_placeholder("<the new instance"))
        self.assertFalse(dm.looks_like_a_placeholder("cloudsql-orders"))
        self.assertFalse(dm.looks_like_a_placeholder("gs://exports-gcp"))

    def test_a_placeholder_target_is_refused_by_the_recorder_itself(self):
        """The refusal is in two places on purpose — the tool, so the message
        is good, and here, so no other caller can write one. Only the tool's
        copy was tested, which is half a defence."""
        with self.assertRaises(ValueError):
            dm.record_status(dm.empty_document(), _entry(), dm.MIGRATED, WHO,
                             NOW, target="<the new instance>")

    def test_a_whitespace_only_target_is_not_a_destination(self):
        """`if target:` read "   " as one, and
        `record.get("target") or "target not recorded"` then reported a
        destination that was never given — the placeholder refusal's own
        failure, reached from the other side."""
        document = dm.empty_document()
        dm.record_status(document, _entry(), dm.MIGRATED, WHO, NOW,
                         target="   ")

        self.assertNotIn("target", document["migrations"][0])

    def test_a_target_is_stored_as_it_was_checked(self):
        """The placeholder test strips before looking; storing the unstripped
        value makes what was checked and what was written two things."""
        document = dm.empty_document()
        dm.record_status(document, _entry(), dm.MIGRATED, WHO, NOW,
                         target="  orders-db-gcp  ")

        self.assertEqual(document["migrations"][0]["target"], "orders-db-gcp")

    def test_a_target_is_recorded_only_when_supplied(self):
        """Never invented: the server cannot see where it landed."""
        document = dm.empty_document()
        dm.record_status(document, _entry(), dm.MIGRATED, WHO, NOW)

        self.assertNotIn("target", document["migrations"][0])


class InFlightTest(unittest.TestCase):
    """The fourth view, and the partition the four of them form."""

    def test_only_a_non_gating_entry_lands_here(self):
        document = dm.empty_document()
        owed = _entry(disposition="migrate")
        cache = _entry(service="elasticache", identifier="sessions",
                       address="aws_elasticache_cluster.sessions",
                       disposition="rebuild")
        dm.record_status(document, owed, dm.IN_PROGRESS, WHO, NOW)
        dm.record_status(document, cache, dm.IN_PROGRESS, WHO, NOW)

        self.assertEqual(dm.in_flight_not_owed([owed, cache], document),
                         [cache])
        # ...and the owed one is where it belongs, not in both or neither.
        self.assertEqual(dm.outstanding([owed, cache], document), [owed])

    def test_the_four_views_partition_every_entry(self):
        """No entry in two of them, none in none of them, over every
        combination of disposition and recorded status."""
        document = dm.empty_document()
        entries = []
        for i, disposition in enumerate(
                ("migrate", "rebuild", "keep-in-aws", "undecided")):
            for j, status in enumerate((None, dm.IN_PROGRESS, dm.MIGRATED)):
                entry = _entry(identifier=f"svc-{i}-{j}",
                               address=f"aws_db_instance.svc_{i}_{j}",
                               disposition=disposition)
                entries.append(entry)
                if status:
                    dm.record_status(document, entry, status, WHO, NOW)

        views = (dm.outstanding(entries, document),
                 dm.settled(entries, document),
                 dm.in_flight_not_owed(entries, document))
        seen = [dm.key_of(e) for view in views for e in view]
        self.assertEqual(len(seen), len(set(seen)), "an entry is in two views")

        # Everything with a RECORD is surfaced by one of them; an entry with
        # no record and no gating grade is simply not work, and is described
        # by the runbook's "Not owed here" prose instead.
        for entry in entries:
            recorded = dm.status_of(document, entry) is not None
            gating = entry.get("disposition") == "migrate"
            if recorded or gating:
                self.assertIn(dm.key_of(entry), set(seen),
                              f"{dm.key_of(entry)} is in no view")

    def test_a_record_whose_entry_was_rescanned_away_is_stale_not_in_flight(self):
        document = dm.empty_document()
        gone = _entry(identifier="deleted-db",
                      address="aws_db_instance.deleted", disposition="rebuild")
        dm.record_status(document, gone, dm.IN_PROGRESS, WHO, NOW)

        self.assertEqual(dm.in_flight_not_owed([], document), [])
        self.assertEqual(len(dm.stale_records([], document)), 1)


class RunbookModeTest(unittest.TestCase):
    """The four `copies` x `closed` shapes the artifact can take."""

    def _owed(self):
        return [_entry()]

    def test_a_live_runbook_instructs(self):
        text = dm.runbook(self._owed(), dm.empty_document(), "ws", NOW)

        self.assertIn("- Report it: `", text)
        self.assertIn("If one of these should not move after all", text)
        self.assertNotIn("left the data migration step", text)

    def test_a_clean_close_records_rather_than_instructs(self):
        document = dm.empty_document()
        dm.record_status(document, _entry(), dm.MIGRATED, WHO, NOW)

        text = dm.runbook([_entry()], document, "ws", NOW,
                          closed=dm.CLOSED_COMPLETE)

        self.assertIn("closed with nothing outstanding", text)
        self.assertNotIn("If one of these should not move after all", text)
        self.assertNotIn("scan baseline was absent", text)
        # The name of this test is the property, and it was asserted only of
        # the exit section: the settled entry still carried "Amend it: `...`",
        # a call that refuses from the terminal this document is registered
        # at, under a banner saying nothing transitions back.
        self.assertNotIn("Amend it:", text)
        self.assertIn("not callable from here", text)

    def test_a_closed_runbook_promises_nothing_about_a_move_in_flight(self):
        """"Report it when it lands" is a claim about the FUTURE, and the
        in-flight section is exactly the case where the record can never be
        completed: the AWS resource is live, the step is gone, and the same
        document says nothing transitions back into it."""
        document = dm.empty_document()
        cache = _entry(service="elasticache", identifier="sessions",
                       address="aws_elasticache_cluster.sessions",
                       disposition="rebuild")
        dm.record_status(document, cache, dm.IN_PROGRESS, WHO, NOW,
                         note="RDB export running")

        live = dm.runbook([cache], document, "ws", NOW)
        closed = dm.runbook([cache], document, "ws", NOW,
                            closed=dm.CLOSED_COMPLETE)

        # Live: the call is an instruction, which is what round 17 added.
        self.assertIn("Report it when it lands:", live)
        # Closed: the same address, as a record, and no promise.
        self.assertNotIn("Report it when it lands:", closed)
        self.assertIn("can no longer be completed", closed)
        self.assertIn("aws_elasticache_cluster.sessions", closed)

    def test_a_closed_runbook_never_instructs_in_any_section(self):
        """One assertion over the whole document, because the sections were
        fixed one at a time across rounds 12, 13 and 17 and each round found
        the one before it had missed a renderer.

        The two sections a closed document can carry, and no owed one:
        `closed` is set only on the success path, which the "anything owed"
        refusal returns before. An earlier version passed an owed entry as
        well, and that kept a dead branch in `runbook` alive while reporting
        full coverage of it — the combination it exercised became unreachable
        when the caveated close was deleted."""
        document = dm.empty_document()
        done = _entry(identifier="done-db", address="aws_db_instance.done")
        flight = _entry(service="elasticache", identifier="sessions",
                        address="aws_elasticache_cluster.sessions",
                        disposition="rebuild")
        dm.record_status(document, done, dm.MIGRATED, WHO, NOW)
        dm.record_status(document, flight, dm.IN_PROGRESS, WHO, NOW)

        text = dm.runbook([done, flight], document, "ws", NOW,
                          closed=dm.CLOSED_COMPLETE)

        # STRUCTURAL, not a list of literals. The three-string version was
        # blind to any section added after it was written — which is how the
        # "Nothing is outstanding" section reached the terminal document
        # unnoticed. Every backtick-quoted call in a closed document must sit
        # on a line that says it cannot be called.
        undisclaimed = [line for line in text.split("\n")
                        if "`" in line and "(" in line
                        and "not callable from here" not in line]
        self.assertEqual(undisclaimed, [],
                         "a closed document must not print a callable-looking "
                         "instruction without saying it cannot be called")
        # Both are still NAMED, with their address: the document is the record
        # of what the close left behind, not a blank.
        self.assertEqual(text.count("not callable from here"), 2)
        for entry in (done, flight):
            self.assertIn(entry["address"], text)

    def test_copies_are_listed_with_both_of_their_exits(self):
        text = dm.runbook([], dm.empty_document(), "ws", NOW,
                          copies=["1234.dkr.ecr.us-east-1.amazonaws.com/a:1"])

        self.assertIn("also unconfirmed", text)
        self.assertIn("/a:1", text)
        self.assertIn("mark_replication_complete", text)
        self.assertIn("abandon_image_replication", text)


class OutstandingTest(unittest.TestCase):

    def test_only_migrate_is_owed(self):
        """An empty cache is a working cache, a replatform changes the shape
        rather than moving bytes, an escalation is a decision nobody has made,
        and a kept service is the customer's choice."""
        entries = [
            _entry(service="rds", disposition="migrate"),
            _entry(service="elasticache", identifier="sessions",
                   address="aws_elasticache_cluster.sessions",
                   disposition="rebuild"),
            _entry(service="sqs", identifier="events",
                   address="aws_sqs_queue.events", disposition="replatform"),
            _entry(service="dynamodb", identifier="carts",
                   address="aws_dynamodb_table.carts", disposition="escalate"),
            _entry(service="s3", identifier="exports",
                   address="aws_s3_bucket.exports", disposition="keep-in-aws"),
        ]

        self.assertEqual([e["identifier"] for e in dm.outstanding(entries, {})],
                         ["orders-db"])

    def test_in_progress_is_still_outstanding(self):
        """A status report is not a completion. Treating it as one would let a
        component ship against a database that is still copying."""
        document = dm.empty_document()
        entry = _entry()
        dm.record_status(document, entry, dm.IN_PROGRESS, WHO, NOW)

        self.assertEqual(dm.outstanding([entry], document), [entry])
        self.assertEqual(dm.settled([entry], document), [])

    def test_a_migrated_service_leaves_the_owed_list(self):
        document = dm.empty_document()
        entry = _entry()
        dm.record_status(document, entry, dm.MIGRATED, WHO, NOW)

        self.assertEqual(dm.outstanding([entry], document), [])
        self.assertEqual(dm.settled([entry], document), [entry])

    def test_keeping_a_service_in_aws_removes_it_from_the_owed_list(self):
        """The escape hatch, seen from here: the disposition changes in the
        section and this list simply stops naming it."""
        entry = _entry()
        self.assertEqual(dm.outstanding([entry], {}), [entry])

        entry["disposition"] = "keep-in-aws"
        self.assertEqual(dm.outstanding([entry], {}), [])


class StaleTest(unittest.TestCase):

    def test_a_record_whose_entry_is_gone_is_reported_not_dropped(self):
        """The scope may have narrowed or the resource may be gone. "This
        moved" stays true either way, and must not vanish from the count."""
        document = dm.empty_document()
        gone = _entry(identifier="retired-db",
                      address="aws_db_instance.retired")
        dm.record_status(document, gone, dm.MIGRATED, WHO, NOW)

        stale = dm.stale_records([_entry()], document)

        self.assertEqual(len(stale), 1)
        self.assertEqual(stale[0]["identifier"], "retired-db")

    def test_a_live_entry_is_not_stale(self):
        document = dm.empty_document()
        entry = _entry()
        dm.record_status(document, entry, dm.MIGRATED, WHO, NOW)

        self.assertEqual(dm.stale_records([entry], document), [])

    def test_staleness_is_keyed_on_all_three_axes(self):
        """A dev/prod pair declares one address in two directories. The dev
        database is reported, the scope then narrows and only prod survives.
        Keyed on address — or on address and identifier — the dev record
        matches the prod entry and is not stale; it is not `outstanding`,
        `settled` or `in_flight_not_owed` either, because it has no entry of
        its own. The record that a customer database moved would then appear
        in no view of either renderer. The other three-axis keys are pinned
        (round 19's `entries_a_rebuild_would_drop`); this one was not."""
        dev = _entry(evidence=("envs/dev/data.tf",))
        prod = _entry(evidence=("envs/prod/data.tf",))
        document = dm.empty_document()
        dm.record_status(document, dev, dm.MIGRATED, WHO, NOW)

        stale = dm.stale_records([prod], document)

        self.assertEqual(len(stale), 1)
        self.assertEqual(stale[0]["directory"], "envs/dev")
        # ...and it really is invisible to every other view, which is why the
        # key is the only thing standing between the record and silence.
        self.assertEqual(dm.outstanding([prod], document), [prod])
        self.assertEqual(dm.settled([prod], document), [])
        self.assertEqual(dm.in_flight_not_owed([prod], document), [])


class RunbookTest(unittest.TestCase):

    def test_the_runbook_names_who_is_waiting_for_each_move(self):
        entry = _entry(consumers=[_consumer("orders"),
                                  _consumer("checkout", "service_account")])

        text = dm.runbook([entry], dm.empty_document(), "acme", NOW)

        self.assertIn("rds orders-db", text)
        self.assertIn("checkout, orders", text)
        self.assertIn("Cloud SQL", text)
        self.assertIn("Database Migration Service", text)

    def test_an_unattributed_entry_does_not_read_as_unused(self):
        """The scan under-detects on purpose and the review may have left it
        unattributed. "Nobody uses this" is a claim nobody made."""
        text = dm.runbook([_entry(consumers=[])], dm.empty_document(),
                          "acme", NOW)

        self.assertIn("unattributed is not unused", text)

    def test_the_runbook_states_the_landing_zone_precondition(self):
        """The translation phase ends at a Pull Request and nothing here
        applies it, so the targets do not exist until somebody merges it."""
        text = dm.runbook([_entry()], dm.empty_document(), "acme", NOW)

        self.assertIn("landing zone Terraform has to be applied", text)

    def test_a_service_with_no_default_target_says_so_rather_than_guessing(self):
        entry = _entry(service="neptune", identifier="graph",
                       address="aws_neptune_cluster.graph")

        text = dm.runbook([entry], dm.empty_document(), "acme", NOW)

        self.assertIn("no default target for this service", text)

    def test_a_settled_service_is_reported_with_where_it_landed(self):
        document = dm.empty_document()
        entry = _entry()
        dm.record_status(document, entry, dm.MIGRATED, WHO, NOW,
                         target="orders-db-gcp")

        text = dm.runbook([entry], document, "acme", NOW)

        # The RENDERED heading, with its count. The bare phrase also
        # appears in the "Not owed here" paragraph below, which names both
        # sections verbatim — so asserting it alone passed with the heading
        # deleted entirely.
        self.assertIn("## 1 already reported migrated", text)
        self.assertIn("orders-db-gcp", text)
        self.assertNotIn("still owe a move", text)

    def test_a_closed_runbook_does_not_offer_the_close(self):
        """The section round 37 added is `actionable`-gated for the reason
        rounds 12, 13 and 17 each established: at the terminal every tool this
        document names refuses, and its own banner says so. Both sentences of
        that section are false there."""
        document = dm.empty_document()
        entry = _entry()
        dm.record_status(document, entry, dm.MIGRATED, WHO, NOW)

        text = dm.runbook([entry], document, "acme", NOW,
                          closed=dm.CLOSED_COMPLETE)

        self.assertIn("left the data migration step", text)
        self.assertNotIn("## Nothing is outstanding", text)
        self.assertNotIn("complete_data_migration()", text)

    def test_an_unconfirmed_copy_suppresses_the_close_in_the_runbook(self):
        """Round 37 pinned this in the chat listing's footer, both
        directions; the runbook footer added in the same work got neither.
        This is the PRIMARY review-UI artifact — the one a platform engineer
        opens rather than the chat text."""
        text = dm.runbook([], dm.empty_document(), "acme", NOW,
                          copies=["1234.dkr.ecr.us-east-1.amazonaws.com/a:1"])

        self.assertIn("also unconfirmed", text)
        self.assertIn("/a:1", text)
        self.assertNotIn("## Nothing is outstanding", text)
        # ...and this asked only what is absent, never what stands in its
        # place. What stood there was the `keep-in-aws` heading, inherited by
        # the else-arm: "one of these" with nothing owed to refer to, offering
        # a tool that takes a data-service address as the answer to a
        # container image whose own two exits are named just above.
        self.assertNotIn("If one of these should not move after all", text)
        self.assertNotIn("annotate_data_dependency", text)

    def test_an_all_settled_runbook_names_the_close_not_the_exit(self):
        """The state's PRIMARY artifact, at the moment everything is settled,
        offered only `keep-in-aws` under a heading whose "one of these" refers
        to an empty owed list. Round 13 removed that dangling reference from
        the CLOSED document; the live all-settled shape reached it by another
        route."""
        document = dm.empty_document()
        entry = _entry()
        dm.record_status(document, entry, dm.MIGRATED, WHO, NOW,
                         target="cloudsql-orders")

        text = dm.runbook([entry], document, "acme", NOW)

        self.assertIn("## Nothing is outstanding", text)
        self.assertIn("complete_data_migration()", text)
        self.assertNotIn("If one of these should not move after all", text)

    def test_a_runbook_with_work_left_keeps_the_exit(self):
        """The control: while anything is owed, the exit is the right thing
        to offer and the close is not."""
        text = dm.runbook([_entry()], dm.empty_document(), "acme", NOW)

        self.assertIn("If one of these should not move after all", text)
        self.assertNotIn("## Nothing is outstanding", text)

    def test_one_workload_reached_by_two_chains_is_named_once(self):
        """`consumer_kind` separates a helm_release from an IRSA service
        account, so one workload legitimately appears twice on an entry —
        the state `attach_data_consumer` refuses an unqualified call over and
        DESIGN issue 35 is about. Without the dedupe the artifact a platform
        engineer reads to decide who is waiting says "used by: orders,
        orders"."""
        entry = _entry(consumers=(_consumer(kind="helm_release"),
                                  _consumer(kind="service_account")))

        self.assertEqual(dm.consumers_of(entry), ["orders"])
        self.assertIn("- Used by: orders\n",
                      dm.runbook([entry], dm.empty_document(), "acme", NOW))

    def test_an_in_flight_entry_is_named_not_just_called(self):
        """The fourth view's whole argument is that the AWS-side resource is
        live and somebody is mid-cutover — so who and what has to be on the
        line. Deleting the bullet left the section as a heading, a paragraph
        and a reporting call, with no service name, grade, author or date."""
        document = dm.empty_document()
        cache = _entry(service="elasticache", identifier="sessions",
                       address="aws_elasticache_cluster.sessions",
                       disposition="rebuild")
        dm.record_status(document, cache, dm.IN_PROGRESS, WHO, NOW,
                         note="RDB export running")

        text = dm.runbook([cache], document, "acme", NOW)

        self.assertIn("## 1 move(s) in progress that nothing waits on", text)
        self.assertIn("- elasticache sessions — graded 'rebuild'", text)
        self.assertIn(WHO, text)
        self.assertIn(NOW, text)
        # This document builds the exact shape and asserted only the bullet.
        # Eight lines below it, the footer offered the close — the call that
        # makes this record permanently uncompletable — under a section that
        # had just said "report it when it lands".
        self.assertIn("## Nothing is outstanding", text)
        self.assertIn("1 already reported in progress would be frozen", text)
        self.assertIn("elasticache sessions", text.split("frozen by that")[1])
        # The markdown renderer's `mono` — the one parameter differentiating
        # the two renderers, and the tool name would otherwise be the only
        # bare one in the document.
        self.assertIn("`mark_data_service_migrated`", text)

    def test_the_past_tense_freeze_counts_what_it_enumerates(self):
        """The sibling's count was pinned by round 45 finding 3 for exactly
        this reason — the enumeration was asserted at every site and the count
        at none, so the message could contradict its own list. The past-tense
        form shipped without that treatment, and without any pure unit test at
        all."""
        document = dm.empty_document()
        one = _entry(service="elasticache", identifier="sessions",
                     address="aws_elasticache_cluster.sessions",
                     disposition="rebuild")
        two = _entry(service="sqs", identifier="jobs",
                     address="aws_sqs_queue.jobs", disposition="rebuild")
        for entry in (one, two):
            dm.record_status(document, entry, dm.IN_PROGRESS, WHO, NOW)

        text = dm.closed_over_in_flight([one, two], document)

        self.assertTrue(text.startswith("2 move(s) still reported in progress"))
        self.assertIn("elasticache sessions", text)
        self.assertIn("sqs jobs", text)
        # No runbook claim in the shared sentence: whether that document
        # carries these records depends on the caller, and in `_report`'s
        # closed-under-us arm — where the record is created AFTER the close
        # wrote it — it does not.
        self.assertNotIn("runbook", text)
        # Chat only: nothing rewrites the runbook after the close, so there is
        # no markdown renderer for this one.
        self.assertNotIn("`", text)

    def test_nothing_in_flight_means_no_past_tense_sentence(self):
        document = dm.empty_document()
        entry = _entry()
        dm.record_status(document, entry, dm.MIGRATED, WHO, NOW)

        self.assertEqual(dm.closed_over_in_flight([entry], document), "")

    def test_the_freeze_warning_counts_what_it_enumerates(self):
        """The enumeration was pinned at every site and the count at none, so
        it could contradict the list on its own line. Round 42 finding 2's
        class."""
        document = dm.empty_document()
        one = _entry(service="elasticache", identifier="sessions",
                     address="aws_elasticache_cluster.sessions",
                     disposition="rebuild")
        two = _entry(service="sqs", identifier="jobs",
                     address="aws_sqs_queue.jobs", disposition="rebuild")
        for entry in (one, two):
            dm.record_status(document, entry, dm.IN_PROGRESS, WHO, NOW)

        text = dm.closing_freezes_in_flight([one, two], document)

        self.assertTrue(text.startswith("2 already reported in progress"))
        self.assertIn("elasticache sessions", text)
        self.assertIn("sqs jobs", text)
        # And the chat renderer leaves the tool name bare.
        self.assertNotIn("`", text)

    def test_no_in_flight_move_means_no_freeze_warning(self):
        """The other direction: the warning is keyed on the fourth view, not
        printed beside every offer of the close."""
        document = dm.empty_document()
        entry = _entry()
        dm.record_status(document, entry, dm.MIGRATED, WHO, NOW)

        text = dm.runbook([entry], document, "acme", NOW)

        self.assertIn("## Nothing is outstanding", text)
        self.assertNotIn("frozen by that", text)

    def test_an_empty_estate_says_nothing_is_waiting(self):
        text = dm.runbook([], dm.empty_document(), "acme", NOW)

        self.assertIn("No data service is waiting to move", text)


class ReferencedEntryKeyTest(unittest.TestCase):
    """A referenced entry's `evidence[0]` is whichever file the walk met
    first, and its address is one spelling of its ARN. Neither may decide
    whether a recorded `migrated` is found again — a lost outcome makes the
    gate hold every consuming component for data that has landed."""

    ARN = "arn:aws:s3:::acme-invoice-archive"

    def _referenced(self, evidence, address=None):
        entry = _entry(service="s3", identifier="acme-invoice-archive",
                       address=address or self.ARN, evidence=evidence)
        entry["detection"] = "referenced"
        entry["arn"] = address or self.ARN
        return entry

    def test_the_outcome_survives_an_earlier_sorting_sighting(self):
        document = dm.empty_document()
        dm.record_status(
            document, self._referenced(("iam/policies.tf",)),
            dm.MIGRATED, WHO, NOW)
        moved = self._referenced(("aaa-extra.tf", "iam/policies.tf"))
        self.assertEqual(
            (dm.status_of(document, moved) or {}).get("status"),
            dm.MIGRATED)

    def test_the_outcome_survives_the_handle_moving_to_another_spelling(self):
        loose = "arn:aws:sqs:::orders"
        exact = "arn:aws:sqs:us-east-1:111111111111:orders"
        document = dm.empty_document()
        first = _entry(service="sqs", identifier="orders", address=loose,
                       evidence=("iam/policies.tf",))
        first.update({"detection": "referenced", "arn": loose})
        dm.record_status(
            document, first, dm.MIGRATED, WHO, NOW)
        second = _entry(service="sqs", identifier="orders", address=exact,
                        evidence=("iam/policies.tf",))
        second.update({"detection": "referenced", "arn": exact})
        self.assertEqual(
            (dm.status_of(document, second) or {}).get("status"),
            dm.MIGRATED)

    def test_a_queue_in_another_region_is_not_the_same_service(self):
        exact = "arn:aws:sqs:us-east-1:111111111111:orders"
        other = "arn:aws:sqs:eu-west-1:111111111111:orders"
        document = dm.empty_document()
        first = _entry(service="sqs", identifier="orders", address=exact,
                       evidence=("iam/policies.tf",))
        first.update({"detection": "referenced", "arn": exact})
        dm.record_status(
            document, first, dm.MIGRATED, WHO, NOW)
        second = _entry(service="sqs", identifier="orders", address=other,
                        evidence=("iam/policies.tf",))
        second.update({"detection": "referenced", "arn": other})
        self.assertIsNone(dm.status_of(document, second))

    def test_a_declared_entry_still_keys_on_its_directory(self):
        document = dm.empty_document()
        dm.record_status(
            document, _entry(evidence=("envs/prod/data.tf",)),
            dm.MIGRATED, WHO, NOW)
        self.assertIsNone(dm.status_of(
            document, _entry(evidence=("envs/dev/data.tf",))))

class OneMatcherEverywhereTest(unittest.TestCase):
    """Every lookup in this store goes through `record_matches`. While three
    used exact tuples and one did not, the runbook counted a service twice,
    a superseded record survived a replacement, and a completed status masked
    a live one."""

    LOOSE = "arn:aws:sqs:::orders"
    EXACT = "arn:aws:sqs:us-east-1:111111111111:orders"

    def _ref(self, address, identifier="orders", evidence=("iam/p.tf",)):
        entry = _entry(service="sqs", identifier=identifier, address=address,
                       evidence=evidence)
        entry.update({"detection": "referenced", "arn": address})
        return entry

    def test_the_outcome_survives_the_entry_folding_onto_its_declaration(self):
        # Scope widens, the declaration is found, the entries fold: the
        # address becomes the block and the ARN moves to `arn`.
        document = dm.empty_document()
        dm.record_status(document, self._ref("arn:aws:s3:::acme-invoice-archive",
                                             identifier="acme-invoice-archive"),
                         dm.MIGRATED, WHO, NOW)
        folded = _entry(service="s3", identifier="acme-invoice-archive",
                        address="aws_s3_bucket.archive",
                        evidence=("envs/prod/data.tf",))
        folded["arn"] = "arn:aws:s3:::acme-invoice-archive"
        self.assertEqual((dm.status_of(document, folded) or {}).get("status"),
                         dm.MIGRATED)
        self.assertEqual(dm.outstanding([folded], document), [])
        self.assertEqual(dm.stale_records([folded], document), [])

    def test_a_respelling_replaces_rather_than_duplicates(self):
        document = dm.empty_document()
        dm.record_status(document, self._ref(self.LOOSE), dm.MIGRATED, WHO,
                         NOW, target="gs://done")
        dm.record_status(document, self._ref(self.EXACT), dm.IN_PROGRESS, WHO,
                         "2026-09-02T10:00:00+00:00", note="still copying")
        self.assertEqual(len(document["migrations"]), 1)
        # And the live status is what the loose spelling reads back, not the
        # completion it superseded.
        self.assertEqual(
            (dm.status_of(document, self._ref(self.LOOSE)) or {}).get("status"),
            dm.IN_PROGRESS)

    def test_the_newest_record_wins_when_a_document_holds_two(self):
        # A document written before this module matched tolerantly.
        document = {"schema_version": dm.SCHEMA_VERSION, "migrations": [
            {"address": self.LOOSE, "directory": "", "identifier": "orders",
             "service": "sqs", "status": dm.MIGRATED, "author": WHO,
             "recorded_at": "2026-01-01T00:00:00+00:00"},
            {"address": self.EXACT, "directory": "", "identifier": "orders",
             "service": "sqs", "status": dm.IN_PROGRESS, "author": WHO,
             "recorded_at": "2026-09-02T00:00:00+00:00"}]}
        self.assertEqual(
            (dm.status_of(document, self._ref(self.LOOSE)) or {}).get("status"),
            dm.IN_PROGRESS)

    def test_settled_and_stale_records_agree(self):
        document = dm.empty_document()
        dm.record_status(document, self._ref(self.LOOSE), dm.MIGRATED, WHO, NOW)
        moved = self._ref(self.EXACT)
        self.assertEqual([e["address"] for e in dm.settled([moved], document)],
                         [self.EXACT])
        self.assertEqual(dm.stale_records([moved], document), [])

class AmbiguousSpellingsStayApartTest(unittest.TestCase):
    """When the scan could not reconcile two spellings of one name it keeps
    them as separate entries on purpose. The outcome store must not undo that
    with its spelling tolerance: an under-specified ARN agrees with all of
    them at once."""

    LOOSE = "arn:aws:sqs:::orders"
    ONE = "arn:aws:sqs:us-east-1:111111111111:orders"
    TWO = "arn:aws:sqs:us-east-1:222222222222:orders"
    # Built from the real constant: the coupling between this store and the
    # note the extraction writes is the thing under test.
    AMBIGUOUS = datastores.AMBIGUOUS_ACCOUNT_NOTE_PREFIX + "(two accounts)"

    def _ambiguous(self, address):
        entry = _entry(service="sqs", identifier="orders", address=address,
                       evidence=("iam/p.tf",))
        entry.update({"detection": "referenced", "arn": address,
                      "notes": [self.AMBIGUOUS]})
        return entry

    def test_a_move_on_one_spelling_does_not_release_the_others(self):
        document = dm.empty_document()
        dm.record_status(document, self._ambiguous(self.LOOSE),
                         dm.MIGRATED, WHO, NOW)
        self.assertIsNone(dm.status_of(document, self._ambiguous(self.ONE)))
        self.assertIsNone(dm.status_of(document, self._ambiguous(self.TWO)))
        self.assertEqual(
            (dm.status_of(document, self._ambiguous(self.LOOSE)) or {}).get("status"),
            dm.MIGRATED)

    def test_reporting_on_one_spelling_does_not_delete_another_record(self):
        document = dm.empty_document()
        dm.record_status(document, self._ambiguous(self.ONE),
                         dm.MIGRATED, WHO, NOW)
        dm.record_status(document, self._ambiguous(self.LOOSE),
                         dm.IN_PROGRESS, WHO, NOW)
        self.assertEqual(len(document["migrations"]), 2)
        self.assertEqual(
            (dm.status_of(document, self._ambiguous(self.ONE)) or {}).get("status"),
            dm.MIGRATED)

class TheFoldBridgesBothWaysTest(unittest.TestCase):
    """A record has to survive the entry folding onto its declaration AND the
    declaration later leaving the confirmed scope. The second direction needs
    the handle stored on the record, because the entry no longer carries the
    block address the record was made under."""

    ARN = "arn:aws:s3:::acme-invoice-archive"

    def _referenced(self):
        entry = _entry(service="s3", identifier="acme-invoice-archive",
                       address=self.ARN, evidence=("iam/p.tf",))
        entry.update({"detection": "referenced", "arn": self.ARN})
        return entry

    def _declared(self):
        entry = _entry(service="s3", identifier="acme-invoice-archive",
                       address="aws_s3_bucket.archive",
                       evidence=("envs/prod/data.tf",))
        entry["arn"] = self.ARN
        return entry

    def test_reported_on_the_declaration_then_the_scope_narrows(self):
        document = dm.empty_document()
        dm.record_status(document, self._declared(), dm.MIGRATED, WHO, NOW)
        unfolded = self._referenced()
        self.assertEqual((dm.status_of(document, unfolded) or {}).get("status"),
                         dm.MIGRATED)
        self.assertEqual(dm.outstanding([unfolded], document), [])
        self.assertEqual(dm.stale_records([unfolded], document), [])

    def test_reported_on_the_arn_then_the_scope_widens(self):
        document = dm.empty_document()
        dm.record_status(document, self._referenced(), dm.MIGRATED, WHO, NOW)
        self.assertEqual(
            (dm.status_of(document, self._declared()) or {}).get("status"),
            dm.MIGRATED)

    def test_the_runbook_does_not_invent_a_directory_for_an_arn_entry(self):
        entry = self._referenced()
        entry["evidence"] = ["platform/iam/policies.tf"]
        text = dm.runbook([entry], dm.empty_document(), "ws", NOW)
        # Evidence still names the file that states the ARN, which is a fact.
        # What must not appear is a DECLARING directory derived from it.
        self.assertNotIn("in `platform/iam`", text)
        self.assertNotIn("at the repository root", text)
        self.assertIn("Known only from this ARN", text)
        self.assertIn("Evidence: platform/iam/policies.tf", text)


class ReviewRoundFortyFiveTest(unittest.TestCase):
    """Regressions from the forty-fifth adversarial review round."""

    _S = "arn:aws:secretsmanager:us-east-1:111111111111:secret:"

    def test_an_outcome_on_a_flagged_entry_does_not_reach_its_sibling_once_the_flag_is_gone(self):
        # The flag lives on the entry and leaves with the other member of the
        # pair; the record made against that member does not. A `migrated`
        # reported against a dev-only secret then read as the declared
        # secret's on the next scan and dropped it from `outstanding`.
        sibling = _entry(service="secretsmanager", identifier="acme/db-Dev001",
                         address=f"{self._S}acme/db-Dev001",
                         evidence=("envs/dev/iam.tf",))
        sibling.update({"arn": f"{self._S}acme/db-Dev001", "detection": "referenced",
                        "notes": [datastores.SECRET_OTHER_FORM_NOTE_PREFIX
                                  + "'acme/db', which already folded in 'acme/db-Prod01'"]})
        # The declared secret as the NEXT scan records it: sibling gone, unflagged.
        survivor = _entry(service="secretsmanager", identifier="acme/db",
                          address="aws_secretsmanager_secret.db", evidence=("sm.tf",))
        survivor.update({"arn": f"{self._S}acme/db", "detection": "declared"})
        self.assertFalse(datastores.spelling_is_ambiguous(survivor))
        # The tolerance alone WOULD cross them: that is the hole.
        self.assertTrue(datastores.arn_spellings_agree(sibling["arn"], survivor["arn"]))
        document = dm.empty_document()
        record, _, _ = dm.record_status(document, sibling, dm.MIGRATED, WHO, NOW)
        self.assertTrue(record["ambiguous_spelling"])
        self.assertTrue(dm.record_matches(record, sibling))
        self.assertFalse(dm.record_matches(record, survivor))
        self.assertIsNone(dm.status_of(document, survivor))
        self.assertIn(survivor, dm.outstanding([survivor], document))

    def test_record_status_leaves_an_unflagged_entry_unstamped(self):
        record, _, _ = dm.record_status(dm.empty_document(), _entry(), dm.MIGRATED, WHO, NOW)
        self.assertNotIn("ambiguous_spelling", record)


class ReviewRoundFortySixTest(unittest.TestCase):
    """Regressions from the forty-sixth adversarial review round."""

    def test_a_record_under_one_declaration_never_settles_the_twinned_arn_entry(self):
        # The corrections store guards this with `twinned`; this store had no
        # equivalent. Dev declares the bucket and an IRSA grant names its ARN,
        # which folds onto dev; dev is reported migrated. Prod then declares
        # the same name, the ARN stands alone as a twinned referenced entry
        # carrying the consumer — and read dev's record as its own, so the
        # gate published it migrated. Reporting on the ARN entry afterwards
        # deleted dev's record through the same predicate.
        import os
        import tempfile
        declared = 'resource "aws_s3_bucket" "archive" {\n  bucket = "invoice-archive"\n}\n'
        irsa = ('module "orders_irsa" {\n'
                '  source = "terraform-aws-modules/iam/aws//modules/'
                'iam-role-for-service-accounts-eks"\n'
                '  role_policy_arns = {}\n'
                '  oidc_providers = { main = { provider_arn = "x", '
                'namespace_service_accounts = ["default:orders"] } }\n'
                '  policy_statements = [{ actions = ["s3:GetObject"], '
                'resources = ["arn:aws:s3:::invoice-archive"] }]\n}\n')

        def harvest(files):
            inventory = {"data_dependencies": []}
            with tempfile.TemporaryDirectory() as root:
                for rel_path, content in files.items():
                    full = os.path.join(root, rel_path)
                    os.makedirs(os.path.dirname(full), exist_ok=True)
                    with open(full, "w") as handle:
                        handle.write(content)
                datastores.harvest_datastores(inventory, root)
            return inventory["data_dependencies"]

        dev_only = {"envs/dev/s3.tf": declared, "iam/irsa.tf": irsa}
        (dev,) = harvest(dev_only)
        document = dm.empty_document()
        dm.record_status(document, dev, dm.MIGRATED, WHO, NOW, target="gs://dev-archive")
        entries = harvest(dict(dev_only, **{"envs/prod/s3.tf": declared}))
        twinned = [e for e in entries if dm.twinned(e)]
        self.assertEqual(len(twinned), 1, [e["address"] for e in entries])
        self.assertIsNone(dm.status_of(document, twinned[0]))
        self.assertIn(twinned[0], dm.outstanding(entries, document))
        # And the other direction: a report on the ARN entry leaves dev's alone.
        dm.record_status(document, twinned[0], dm.IN_PROGRESS, WHO, NOW)
        dev_again = next(e for e in entries if "envs/dev" in e["evidence"][0])
        self.assertEqual((dm.status_of(document, dev_again) or {}).get("status"), dm.MIGRATED)
        self.assertEqual(len(document["migrations"]), 2)


class ReviewRoundFortySevenTest(unittest.TestCase):
    """Regressions from the forty-seventh adversarial review round."""

    def test_a_record_on_the_twinned_entry_follows_it_onto_the_fold(self):
        # The twinned rule is one-directional. A `migrated` reported on the
        # twinned ARN entry must still reach the declaration it folds onto
        # once the other twin leaves the scope — the ordinary fold the
        # handles exist for. Refusing both ways held the gate for data
        # reported landed and listed the record as nobody's.
        import os
        import tempfile
        declared = 'resource "aws_s3_bucket" "archive" {\n  bucket = "acme-archive"\n}\n'
        policy = ('resource "aws_iam_policy" "p" {\n  policy = jsonencode({ Resource = '
                  '"arn:aws:s3:::acme-archive" })\n}\n')

        def harvest(files):
            inventory = {"data_dependencies": []}
            with tempfile.TemporaryDirectory() as root:
                for rel_path, content in files.items():
                    full = os.path.join(root, rel_path)
                    os.makedirs(os.path.dirname(full), exist_ok=True)
                    with open(full, "w") as handle:
                        handle.write(content)
                datastores.harvest_datastores(inventory, root)
            return inventory["data_dependencies"]

        both = {"envs/dev/s3.tf": declared, "envs/prod/s3.tf": declared, "iam/p.tf": policy}
        twinned = next(e for e in harvest(both) if dm.twinned(e))
        document = dm.empty_document()
        record, _, _ = dm.record_status(document, twinned, dm.MIGRATED, WHO, NOW,
                                        target="gs://landed")
        self.assertTrue(record["twinned"])
        (folded,) = harvest({"envs/dev/s3.tf": declared, "iam/p.tf": policy})
        self.assertEqual((folded["detection"], folded["arn"]),
                         ("declared", "arn:aws:s3:::acme-archive"))
        self.assertFalse(dm.twinned(folded))
        self.assertEqual((dm.status_of(document, folded) or {}).get("target"), "gs://landed")
        self.assertNotIn(folded, dm.outstanding([folded], document))


class ReviewRoundFortyEightTest(unittest.TestCase):
    """Regressions from the forty-eighth adversarial review round."""

    _ARN = "arn:aws:secretsmanager:us-east-1:111111111111:secret:acme/db"

    def test_an_exact_alias_handle_is_never_a_spelling_match(self):
        # The ambiguity refusal fired before any handle comparison, so a
        # record keyed on the very string a folded declaration carries as its
        # `arn` was treated as a spelling hop and the fold lost its outcome.
        flagged = _entry(service="secretsmanager", identifier="acme/db", address=self._ARN,
                         evidence=("iam/policy.tf",))
        flagged.update({"arn": self._ARN, "detection": "referenced", "notes": [
            datastores.REFERENCED_NOTE, datastores.SHARED_SPELLING_NOTE_PREFIX + "'x'"]})
        document = dm.empty_document()
        record, _, _ = dm.record_status(document, flagged, dm.MIGRATED, WHO, NOW)
        self.assertTrue(record["ambiguous_spelling"])
        folded = _entry(service="secretsmanager", identifier="acme/db",
                        address="aws_secretsmanager_secret.db", evidence=("envs/prod/main.tf",))
        folded.update({"arn": self._ARN, "detection": "declared"})
        self.assertEqual((dm.status_of(document, folded) or {}).get("status"), dm.MIGRATED)
        self.assertEqual(dm.stale_records([folded], document), [])
        # Still a spelling hop, still refused: a different spelling of the name.
        respelled = dict(folded, arn="arn:aws:secretsmanager:::secret:acme/db")
        self.assertIsNone(dm.status_of(document, respelled))


class ReviewRoundFortyNineTest(unittest.TestCase):
    """Regressions from the forty-ninth adversarial review round."""

    def test_a_block_record_never_reaches_a_spelling_the_verdicts_place_elsewhere(self):
        # The alias bridge is for the declaration LEAVING. Here it stays: the
        # provider was silent, so the 222 ARN folded onto dev's queue and dev
        # was reported migrated; the provider then states the estate's
        # account, the ARN is cross-account and stands alone — and read dev's
        # record through the alias, so both were `migrated` on one report and
        # a report on the foreign one deleted dev's.
        import os
        import tempfile
        # A `migrate`-graded, account-bearing service, so the entry gates.
        arn = "arn:aws:secretsmanager:us-east-1:222222222222:secret:orders"

        def harvest(provider_extra):
            inventory = {"data_dependencies": []}
            with tempfile.TemporaryDirectory() as root:
                os.makedirs(os.path.join(root, "envs/dev"))
                with open(os.path.join(root, "envs/dev/main.tf"), "w") as handle:
                    handle.write(
                        f'provider "aws" {{\n  region = "us-east-1"\n{provider_extra}}}\n'
                        'resource "aws_secretsmanager_secret" "orders" {\n  name = "orders"\n}\n'
                        'resource "aws_iam_policy" "p" {\n  policy = jsonencode({ Statement = '
                        f'[{{ Resource = ["{arn}"] }}] }})\n}}\n')
                datastores.harvest_datastores(inventory, root)
            return inventory["data_dependencies"]

        (dev,) = harvest("")
        self.assertEqual(dev["disposition"], "migrate")
        document = dm.empty_document()
        dm.record_status(document, dev, dm.MIGRATED, WHO, NOW, target="gsm://orders")
        entries = harvest('  allowed_account_ids = ["111111111111"]\n')
        by_detection = {e["detection"]: e for e in entries}
        self.assertEqual(sorted(by_detection), ["declared", "referenced"])
        self.assertTrue(dm.placed_elsewhere(by_detection["referenced"]))
        self.assertEqual((dm.status_of(document, by_detection["declared"]) or {}).get("status"),
                         dm.MIGRATED)
        self.assertIsNone(dm.status_of(document, by_detection["referenced"]))
        self.assertEqual(dm.outstanding(entries, document), [by_detection["referenced"]])
        dm.record_status(document, by_detection["referenced"], dm.IN_PROGRESS, WHO, NOW)
        self.assertEqual((dm.status_of(document, by_detection["declared"]) or {}).get("target"),
                         "gsm://orders")
        self.assertEqual(len(document["migrations"]), 2)

    def test_the_runbook_names_the_verdict_when_a_stale_record_and_an_owed_entry_share_a_name(self):
        # Round 50: after the refusal above, dev's file gone and the foreign
        # spelling owed, the runbook listed `secretsmanager orders` once as
        # owed and once as a recorded outcome "with no entry to count it
        # against" — none of whose three causes was the real one.
        import os
        import tempfile
        arn = "arn:aws:secretsmanager:us-east-1:222222222222:secret:orders"

        def harvest(files):
            inventory = {"data_dependencies": []}
            with tempfile.TemporaryDirectory() as root:
                for rel_path, content in files.items():
                    full = os.path.join(root, rel_path)
                    os.makedirs(os.path.dirname(full), exist_ok=True)
                    with open(full, "w") as handle:
                        handle.write(content)
                datastores.harvest_datastores(inventory, root)
            return inventory["data_dependencies"]

        policy = ('resource "aws_iam_policy" "p" {\n  policy = jsonencode({ Statement = '
                  f'[{{ Resource = ["{arn}"] }}] }})\n}}\n')
        (dev,) = harvest({
            "envs/dev/sm.tf": 'resource "aws_secretsmanager_secret" "orders" {\n  name = "orders"\n}\n',
            "iam/p.tf": policy})
        document = dm.empty_document()
        dm.record_status(document, dev, dm.MIGRATED, WHO, NOW, target="gsm://orders")
        entries = harvest({
            "providers.tf": ('provider "aws" {\n  region = "us-east-1"\n'
                             '  allowed_account_ids = ["111111111111"]\n}\n'),
            "iam/p.tf": policy})
        (foreign,) = entries
        self.assertTrue(dm.placed_elsewhere(foreign))
        text = dm.runbook(entries, document, "ws", "2026-09-09")
        self.assertIn("still owe a move", text)
        self.assertIn("owed above as its own resource", text)
        self.assertIn(arn, text)


class ReviewRoundFiftyOneTest(unittest.TestCase):
    """Regressions from the fifty-first adversarial review round."""

    _S = "arn:aws:secretsmanager:us-east-1:111111111111:secret:"

    def _harvest(self, files):
        import os
        import tempfile
        inventory = {"data_dependencies": []}
        with tempfile.TemporaryDirectory() as root:
            for rel_path, content in files.items():
                full = os.path.join(root, rel_path)
                os.makedirs(os.path.dirname(full), exist_ok=True)
                with open(full, "w") as handle:
                    handle.write(content)
            datastores.harvest_datastores(inventory, root)
        return inventory["data_dependencies"]

    def test_a_block_record_never_settles_another_root_modules_declaration(self):
        # Dev's file leaves the scope and prod declares the same name: the
        # ARN that folded onto dev now folds onto prod, and prod read
        # `migrated` off dev's record through the alias — the gate published
        # it — while the replay refused the same scenario.
        declared = 'resource "aws_secretsmanager_secret" "db" {\n  name = "acme/db"\n}\n'
        policy = ('resource "aws_iam_policy" "p" {\n  policy = jsonencode({ Resource = '
                  f'"{self._S}acme/db" }})\n}}\n')
        (dev,) = self._harvest({"envs/dev/sm.tf": declared, "iam/p.tf": policy})
        document = dm.empty_document()
        dm.record_status(document, dev, dm.MIGRATED, WHO, NOW, target="gsm://dev")
        (prod,) = self._harvest({"envs/prod/sm.tf": declared, "iam/p.tf": policy})
        self.assertEqual((prod["detection"], prod["arn"]), ("declared", f"{self._S}acme/db"))
        self.assertIsNone(dm.status_of(document, prod))
        self.assertEqual(dm.outstanding([prod], document), [prod])
        # A different block address in the other root module: the same.
        (prod_module,) = self._harvest({
            "envs/prod/sm.tf": ('module "db_secret" {\n  source = "terraform-aws-modules/'
                                'secrets-manager/aws"\n  name = "acme/db"\n}\n'),
            "iam/p.tf": policy})
        self.assertIsNone(dm.status_of(document, prod_module))
        # The designed bridge still holds: dev's file gone, nothing declares
        # the name, the ARN stands alone as a referenced entry.
        (alone,) = self._harvest({"iam/p.tf": policy})
        self.assertEqual(alone["detection"], "referenced")
        self.assertEqual((dm.status_of(document, alone) or {}).get("target"), "gsm://dev")

    def test_an_arn_only_record_survives_the_name_becoming_twinned_and_respelled(self):
        # A record made while the entry was known from its ARN alone is not
        # twinned either; refusing it once two declarations arrived (entry
        # twinned) and the handle respelled lost the outcome entirely.
        bare = "arn:aws:sqs:::orders"
        (alone,) = self._harvest({"iam/p.tf": (
            'resource "aws_iam_policy" "p" {\n  policy = jsonencode({ Resource = '
            f'"{bare}" }})\n}}\n')})
        document = dm.empty_document()
        dm.record_status(document, alone, dm.MIGRATED, WHO, NOW, target="pubsub://orders")
        declared = 'resource "aws_sqs_queue" "orders" {\n  name = "orders"\n}\n'
        entries = self._harvest({
            "envs/dev/q.tf": declared, "envs/prod/q.tf": declared,
            "iam/p.tf": ('resource "aws_iam_policy" "p" {\n  policy = jsonencode({ Resource = ['
                         f'"{bare}", "arn:aws:sqs:us-east-1:111111111111:orders"] }})\n}}\n')})
        twinned = [e for e in entries if dm.twinned(e)]
        self.assertEqual(len(twinned), 1)
        self.assertEqual(twinned[0]["arn"], "arn:aws:sqs:us-east-1:111111111111:orders")
        self.assertEqual((dm.status_of(document, twinned[0]) or {}).get("target"), "pubsub://orders")
        self.assertEqual(dm.stale_records(entries, document), [])


class ReviewRoundFiftyTwoTest(unittest.TestCase):
    """Regressions from the fifty-second adversarial review round."""

    _PROVIDER = ('provider "aws" {\n  region = "us-east-1"\n'
                 '  allowed_account_ids = ["111111111111"]\n}\n')

    def _harvest(self, arns):
        import os
        import tempfile
        inventory = {"data_dependencies": []}
        with tempfile.TemporaryDirectory() as root:
            os.makedirs(os.path.join(root, "envs/dev"))
            with open(os.path.join(root, "envs/dev/main.tf"), "w") as handle:
                handle.write(self._PROVIDER + (
                    'resource "aws_iam_role_policy" "p" {\n  role = "x"\n  policy = jsonencode({ '
                    'Statement = [{ Effect = "Allow", Action = ["ssm:GetParameter"], Resource = ['
                    + ", ".join(f'"{a}"' for a in arns) + '] }] })\n}\n'))
            datastores.harvest_datastores(inventory, root)
        return inventory["data_dependencies"]

    def test_a_wildcard_record_reaching_two_spellings_settles_neither(self):
        # Recorded while the parameter stood alone under `*:*`; the next scan
        # splits the name into us-east-1 and eu-west-1 — which disagree with
        # each other, so neither is flagged — and the record settled BOTH,
        # the cross-region one included. The replay and `_target` refuse
        # this handle; the outcome store was the one reader that did not.
        wild = "arn:aws:ssm:*:*:parameter/orders/db-url"
        (alone,) = self._harvest([wild])
        document = dm.empty_document()
        dm.record_status(document, alone, dm.MIGRATED, WHO, NOW, target="gsm://db-url")
        entries = self._harvest([wild,
                                 "arn:aws:ssm:us-east-1:111111111111:parameter/orders/db-url",
                                 "arn:aws:ssm:eu-west-1:111111111111:parameter/orders/db-url"])
        self.assertEqual(len(entries), 2)
        for entry in entries:
            self.assertFalse(datastores.spelling_is_ambiguous(entry), entry["notes"])
            self.assertIsNone(dm.status_of(document, entry, entries), entry["address"])
        self.assertEqual(dm.outstanding(entries, document), entries)
        self.assertEqual(dm.settled(entries, document), [])
        (stale,) = dm.stale_records(entries, document)
        self.assertEqual(stale["target"], "gsm://db-url")
        text = dm.runbook(entries, document, "ws", "2026-09-09")
        self.assertIn("counted against none of them", text)
        # The exports slice, through the injected predicate: both gating.
        from servers.dag.server import exports as exports_lib
        fields, _ = exports_lib.derive_data_gate(
            {"data_dependencies": entries}, document, dm.key_of, dm.status_of)
        self.assertEqual([s["status"] for s in fields["data_gate"]["services"]], [None, None])
        # One entry at a time — no section — the tolerance still applies.
        self.assertIsNotNone(dm.status_of(document, entries[0]))


class ReviewRoundFiftyThreeTest(unittest.TestCase):
    """Regressions from the fifty-third adversarial review round."""

    _PROVIDER = ('provider "aws" {\n  region = "us-east-1"\n'
                 '  allowed_account_ids = ["111111111111"]\n}\n')

    def _harvest(self, body):
        import os
        import tempfile
        inventory = {"data_dependencies": []}
        with tempfile.TemporaryDirectory() as root:
            os.makedirs(os.path.join(root, "envs/dev"))
            with open(os.path.join(root, "envs/dev/main.tf"), "w") as handle:
                handle.write(self._PROVIDER + body)
            datastores.harvest_datastores(inventory, root)
        return inventory["data_dependencies"]

    @staticmethod
    def _policy(arns):
        return ('resource "aws_iam_role_policy" "p" {\n  role = "x"\n  policy = jsonencode({ '
                'Statement = [{ Effect = "Allow", Action = ["rds:*"], Resource = ['
                + ", ".join(f'"{a}"' for a in arns) + '] }] })\n}\n')

    def test_the_section_count_applies_the_same_refusals_as_the_match(self):
        # A `migrated` on the declaration follows the un-fold onto the
        # estate's own spelling (round 49); a cross-account sighting of the
        # name then counted as "reached" though a block record could never
        # settle it, and the estate's own outcome flipped to owed and stale.
        declared = ('resource "aws_db_instance" "orders" {\n  identifier = "orders"\n'
                    '  engine = "postgres"\n  allocated_storage = 20\n}\n')
        before = self._harvest(declared + self._policy(["arn:aws:rds:us-east-1:*:db:orders"]))
        dev = next(e for e in before if e["detection"] == "declared")
        document = dm.empty_document()
        dm.record_status(document, dev, dm.MIGRATED, WHO, NOW, target="csql://orders")
        own = self._harvest(self._policy(["arn:aws:rds:us-east-1:111111111111:db:orders"]))
        self.assertEqual((dm.status_of(document, own[0], own) or {}).get("status"), dm.MIGRATED)
        both = self._harvest(self._policy(["arn:aws:rds:us-east-1:111111111111:db:orders",
                                           "arn:aws:rds:us-east-1:999999999999:db:orders"]))
        by_account = {e["account"]: e for e in both}
        self.assertTrue(dm.placed_elsewhere(by_account["999999999999"]))
        self.assertEqual((dm.status_of(document, by_account["111111111111"], both) or {}).get("status"),
                         dm.MIGRATED)
        self.assertIsNone(dm.status_of(document, by_account["999999999999"], both))
        self.assertEqual(dm.stale_records(both, document), [])
        self.assertEqual(dm.outstanding(both, document), [by_account["999999999999"]])

    def test_record_status_with_the_section_neither_replaces_nor_carries_a_wildcard_record(self):
        # Reporting on one of two spellings a wildcard record reaches must
        # not replace that record nor copy its note onto this one.
        wild = "arn:aws:ssm:*:*:parameter/orders/db-url"
        policy = ('resource "aws_iam_role_policy" "p" {\n  role = "x"\n  policy = jsonencode({ '
                  'Statement = [{ Effect = "Allow", Action = ["ssm:GetParameter"], Resource = [%s] }] })\n}\n')
        (alone,) = self._harvest(policy % f'"{wild}"')
        document = dm.empty_document()
        dm.record_status(document, alone, dm.MIGRATED, WHO, NOW, note="only dev values copied")
        entries = self._harvest(policy % ", ".join(f'"{a}"' for a in (
            wild, "arn:aws:ssm:us-east-1:111111111111:parameter/orders/db-url",
            "arn:aws:ssm:eu-west-1:111111111111:parameter/orders/db-url")))
        east = next(e for e in entries if e["region"] == "us-east-1")
        record, carried, _ = dm.record_status(document, east, dm.MIGRATED, WHO, NOW,
                                              entries=entries)
        self.assertEqual(carried, [])
        self.assertNotIn("note", record)
        self.assertEqual(len(document["migrations"]), 2)
        (stale,) = dm.stale_records(entries, document)
        self.assertEqual(stale["address"], "arn:aws:ssm:::parameter/orders/db-url")

    def test_a_procedure_line_says_when_the_listing_could_not_be_read(self):
        entry = {"service": "s3", "identifier": "acme-logs", "address": "aws_s3_bucket.logs",
                 "detection": "declared", "disposition": "migrate",
                 "evidence": ["envs/prod/main.tf"], "consumers": [], "notes": []}
        self.assertIn("could not be read", dm._procedure_line(entry, rendered=dm.LISTING_UNREAD))
        self.assertIn("not yet adapted", dm._procedure_line(entry, rendered=set()))
        self.assertIn("not yet adapted", dm._procedure_line(entry, rendered=None))

    def test_a_record_on_the_primary_never_settles_its_replica_even_one_entry_at_a_time(self):
        # Round 57: `placed_elsewhere` lagged `_foreign` on the replica
        # prefix, so a block record with a region-unstated alias reached an
        # unflagged replica whenever a caller asked without the section.
        arn = "arn:aws:secretsmanager::111111111111:secret:prod/db"
        primary = _entry(service="secretsmanager", identifier="prod/db",
                         address="aws_secretsmanager_secret.db", evidence=("envs/dev/main.tf",))
        primary.update({"arn": arn, "detection": "declared"})
        replica = _entry(service="secretsmanager", identifier="prod/db",
                         address=arn.replace("::", ":eu-west-1:"), evidence=("envs/dev/iam.tf",),
                         disposition="rebuild")
        replica.update({"arn": replica["address"], "detection": "referenced",
                        "notes": [datastores.REPLICA_NOTE_PREFIX + "eu-west-1: …"]})
        document = dm.empty_document()
        dm.record_status(document, primary, dm.MIGRATED, WHO, NOW)
        self.assertTrue(dm.placed_elsewhere(replica))
        self.assertIsNone(dm.status_of(document, replica))
        self.assertIsNone(dm.status_of(document, replica, [primary, replica]))



class Cl6RoundOneTest(unittest.TestCase):
    """The endpoint is a handle: an outcome recorded under a queue's URL
    survives the ARN becoming the handle, and the reverse."""

    URL = "https://sqs.us-east-1.amazonaws.com/111111111111/orders"
    ARN = "arn:aws:sqs:us-east-1:111111111111:orders"

    def _by_url(self):
        entry = _entry(service="sqs", identifier="orders", address=self.URL,
                       evidence=("k8s/config.yaml",))
        entry.update({"detection": "referenced", "arn": None, "endpoint": self.URL})
        return entry

    def _by_arn(self):
        entry = _entry(service="sqs", identifier="orders", address=self.ARN,
                       evidence=("iam/p.tf",))
        entry.update({"detection": "referenced", "arn": self.ARN, "endpoint": self.URL})
        return entry

    def test_reported_under_the_url_then_the_arn_arrives(self):
        document = dm.empty_document()
        dm.record_status(document, self._by_url(), dm.MIGRATED, WHO, NOW)
        self.assertEqual((dm.status_of(document, self._by_arn()) or {}).get("status"),
                         dm.MIGRATED)
        self.assertEqual(dm.outstanding([self._by_arn()], document), [])

    def test_reported_under_the_arn_then_only_the_url_is_left(self):
        document = dm.empty_document()
        dm.record_status(document, self._by_arn(), dm.MIGRATED, WHO, NOW)
        self.assertEqual((dm.status_of(document, self._by_url()) or {}).get("status"),
                         dm.MIGRATED)

    def test_a_guess_and_an_endpoint_entry_are_keyed_with_no_directory(self):
        guess = {"service": "s3", "identifier": "x", "address": "s3:x",
                 "detection": "inferred", "evidence": ["charts/orders/values.yaml"]}
        self.assertEqual(dm.directory_of(guess), "")
        self.assertEqual(dm.directory_of(self._by_url()), "")
        self.assertEqual(dm.directory_of(_entry()), "envs/prod")

    def test_the_runbook_lists_the_url_era_name(self):
        from servers.phases.deployment import runbooks as rb
        entry = self._by_arn()
        expected = rb.rendered_blob(dict(entry, address=self.URL))
        self.assertIn(expected, rb.rendered_blobs(entry))


if __name__ == "__main__":
    unittest.main()
