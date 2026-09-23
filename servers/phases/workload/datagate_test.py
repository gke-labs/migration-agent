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

"""Tests for the developer-side data gate (pure; no ledger, no MCP).

The cases that matter are the ones where the gate has to choose between
holding a component and letting it ship: which dispositions gate, which
attribution signals count, and what an absent or unattributable answer does.
"""

import unittest

from servers.phases.workload import datagate


def service(identifier, disposition="migrate", status=None, consumers=(),
            name="rds", address=None, directory=""):
    return {"service": name, "identifier": identifier,
            "address": address or f"aws_db_instance.{identifier}",
            "directory": directory, "disposition": disposition,
            "status": status, "consumers": list(consumers)}


def consumer(workload, kind="service_account", namespace=None,
             source_path=None, detection="irsa"):
    return {"workload": workload, "kind": kind, "namespace": namespace,
            "source_path": source_path, "detection": detection}


def exports(services, scanned=True):
    return {"data_gate": {"schema_version": 1, "scanned": scanned,
                          "services": list(services)}}


# A component owning two files: a plain manifest and a chart directory.
SEED = {
    "k8s/orders.yaml": {"kinds": ["Deployment", "ServiceAccount"],
                        "namespaces": ["orders"], "team_labels": [],
                        "names": ["Deployment/orders", "ServiceAccount/orders"]},
    "charts/web/values.yaml": {"kinds": ["helm-chart"], "namespaces": [],
                               "team_labels": [], "names": []},
    "k8s/other-team.yaml": {"kinds": ["Deployment"], "namespaces": ["billing"],
                            "team_labels": [], "names": ["Deployment/billing"]},
}
SCOPE = ["k8s/orders.yaml", "charts/web/values.yaml"]


class OutstandingTest(unittest.TestCase):

    def test_only_migrate_gates(self):
        doc = exports([service("a", disposition="migrate"),
                       service("b", disposition="keep-in-aws"),
                       service("c", disposition="rebuild"),
                       service("d", disposition="replatform"),
                       service("e", disposition="escalate"),
                       service("f", disposition="undecided")])
        self.assertEqual([s["identifier"] for s in
                          datagate.outstanding(datagate.slice_of(doc))], ["a"])

    def test_in_progress_is_still_outstanding(self):
        doc = exports([service("a", status="in_progress"),
                       service("b", status="migrated")])
        self.assertEqual(
            [s["identifier"] for s in
             datagate.outstanding(datagate.slice_of(doc))], ["a"],
            "a status report is not a completion: a database that is still "
            "copying must not let a component ship against it")

    def test_a_migrated_service_stops_gating(self):
        doc = exports([service("a", status="migrated")])
        self.assertEqual(datagate.outstanding(datagate.slice_of(doc)), [])

    def test_a_missing_or_malformed_slice_is_not_a_document(self):
        for value in (None, {}, {"data_gate": None}, {"data_gate": []}):
            self.assertIsNone(datagate.slice_of(value), value)


class AttributionTest(unittest.TestCase):

    def verdict(self, services, scope=SCOPE, seed=None, scanned=True):
        return datagate.verdict(exports(services, scanned), scope,
                                SEED if seed is None else seed)

    def test_an_irsa_service_account_in_scope_holds_the_component(self):
        # The acme shape: the consumer is a KSA name reached through an IAM
        # policy ARN, with no chart path anywhere.
        result = self.verdict([service("orders-db", consumers=[
            consumer("orders", namespace="orders")])])
        self.assertEqual(len(result["blocking"]), 1)
        self.assertIn("ServiceAccount 'orders'",
                      result["blocking"][0]["matched"][0]["how"])
        self.assertEqual(result["mismatches"], [])

    def test_a_chart_path_inside_the_scope_holds_the_component(self):
        result = self.verdict([service("cache", consumers=[
            consumer("web", kind="helm_release", source_path="charts/web",
                     detection="terraform_wiring")])])
        self.assertEqual(len(result["blocking"]), 1)
        self.assertIn("charts/web", result["blocking"][0]["matched"][0]["how"])

    def test_a_consumer_in_another_components_files_holds_nothing_here(self):
        # SCOPE carries a chart file, so this component is partly blind and
        # the honest answer is "could not test", not "billing's". The
        # fully-indexed positive control for `elsewhere` is in BlindSpotTest.
        result = self.verdict([service("billing-db", consumers=[
            consumer("billing", namespace="billing")])])
        self.assertEqual(result["blocking"], [])
        self.assertEqual(len(result["untestable"]), 1)

    def test_a_service_with_no_consumer_holds_nobody(self):
        result = self.verdict([service("orphan-db")])
        self.assertEqual(result["blocking"], [], (
            "an unattributed service is real work that is really owed, but "
            "the gate cannot say whose — blocking here blocks the estate"))
        self.assertEqual(len(result["unattributed"]), 1)

    def test_a_degraded_scope_is_untestable_and_ships(self):
        # The name says what happens, not what one might hope: with no file
        # list nothing can be attributed, so the component is reported and
        # ships. "Rather than clearing" would be a promise the gate does not
        # keep — `untestable` holds nobody, by the same rule as everything
        # else it cannot place.
        result = self.verdict([service("orders-db", consumers=[
            consumer("orders", namespace="orders")])], scope=None)
        self.assertFalse(result["attributable"])
        self.assertEqual(result["blocking"], [])
        self.assertEqual(len(result["untestable"]), 1,
                         "no resolved file list is 'cannot tell', not 'clear'")
        self.assertEqual(result["unattributed"], [],
                         "the mapping named a consumer; the gap is on our side")

    def test_the_bare_name_fallback_over_matches_a_different_kind(self):
        """The kind-qualified lookup does NOT narrow whether a consumer
        matches — `_seed_names` registers the bare name too, so the fallback
        is always a superset. All the kind buys is which paths land in
        `where`, and therefore which namespaces the mismatch check sees."""
        seed = {"k8s/orders.yaml": {"kinds": ["Deployment"], "namespaces": [],
                                    "team_labels": [],
                                    "names": ["Deployment/orders"]}}
        # The consumer is a ServiceAccount named 'orders'; the only 'orders'
        # in scope is a Deployment. It matches anyway, and that is the
        # deliberate over-match: a held component is recoverable.
        result = self.verdict([service("db", consumers=[
            consumer("orders")])], scope=["k8s/orders.yaml"], seed=seed)
        self.assertEqual(len(result["blocking"]), 1)

    def test_an_unpublished_slice_is_not_an_empty_one(self):
        result = datagate.verdict({}, SCOPE, SEED)
        self.assertFalse(result["published"])
        self.assertIn("no data_gate slice", datagate.refusal(result, "orders"))

    def test_an_unscanned_estate_says_so_rather_than_clearing(self):
        result = self.verdict([], scanned=False)
        self.assertFalse(result["scanned"])
        self.assertIn("data scan has not run", datagate.advisory(result))

    def test_an_unreadable_scope_does_not_hold_when_nothing_is_owed(self):
        """The scope only ever decides WHOSE an outstanding service is. With
        none outstanding, the file list cannot change the answer, and holding
        would claim the dependencies are "unknown" when they are known and
        empty — one flaky read parking every component in a data-free
        estate."""
        clear = datagate.verdict(exports([]), None, SEED,
                                 scope_unreadable=True)
        self.assertEqual(datagate.refusal(clear, "orders-component"), "")

        owed = datagate.verdict(
            exports([service("orders-db", consumers=[consumer("orders")])]),
            None, SEED, scope_unreadable=True)
        self.assertIn("could not be read on this run",
                      datagate.refusal(owed, "orders-component"))

    def test_an_unscanned_estate_also_refuses_at_the_ship_gate(self):
        """`scanned` exists to tell 'no dependencies' from 'nobody looked'.
        Drawing the distinction and then shipping over it anyway would make
        the distinction pointless."""
        result = self.verdict([], scanned=False)
        text = datagate.refusal(result, "orders-component")
        self.assertIn("data scan has not run", text)
        self.assertIn("Owner: Platform Engineer", text)

    def test_a_scanned_estate_with_no_dependencies_ships(self):
        # The positive control for the case above: an empty `services` is a
        # PASS when somebody actually looked.
        result = self.verdict([], scanned=True)
        self.assertEqual(datagate.refusal(result, "orders-component"), "")


class BlindSpotTest(unittest.TestCase):
    """A component the gate cannot TEST is not a component the service
    belongs away from. Saying "somebody else's" about a database this
    component may well use is the one sentence that stops a developer
    looking further."""

    # A Helm-deployed component: chart content is never parsed, so every
    # scoped file carries names: []. This is the acme shape.
    CHART_SEED = {
        "charts/orders/Chart.yaml": {"kinds": ["helm-chart"], "namespaces": [],
                                     "team_labels": [], "names": []},
        "charts/orders/values.yaml": {"kinds": ["helm-chart"], "namespaces": [],
                                      "team_labels": [], "names": []},
    }
    CHART_SCOPE = ["charts/orders/Chart.yaml", "charts/orders/values.yaml"]

    def test_chart_content_is_blind(self):
        self.assertEqual(
            datagate.blind_spots(self.CHART_SCOPE, self.CHART_SEED),
            self.CHART_SCOPE)

    def test_terraform_files_are_blind(self):
        """An earlier draft exempted them on the grounds that no workload
        could be declared in Terraform. A `helm_release` on a registry chart
        is exactly that: `consumers.py` gives it no `source_path`, so it is a
        bare NAME, declared in a file the index parses no names from."""
        seed = {"tf/main.tf": {"kinds": ["terraform"], "namespaces": [],
                               "team_labels": [], "names": []}}
        self.assertEqual(datagate.blind_spots(["tf/main.tf"], seed),
                         ["tf/main.tf"])

    def test_a_registry_chart_consumer_in_terraform_is_not_called_elsewhere(self):
        """The end-to-end shape of the case above: nothing places the
        consumer, and the gate must not claim it is another team's."""
        result = datagate.verdict(
            exports([service("cache", consumers=[
                consumer("orders", kind="helm_release", namespace=None,
                         source_path=None, detection="terraform_wiring")])]),
            ["tf/main.tf"],
            {"tf/main.tf": {"kinds": ["terraform"], "namespaces": [],
                            "team_labels": [], "names": []}})
        self.assertEqual(result["elsewhere"], [])
        self.assertEqual(len(result["untestable"]), 1)

    def test_a_path_missing_from_the_index_is_blind(self):
        # The drift case: the scope was confirmed against one published index
        # and a later extraction republished a narrower one.
        self.assertEqual(
            datagate.blind_spots(["k8s/orders.yaml"], {}), ["k8s/orders.yaml"])

    def test_a_parsed_manifest_with_names_is_not_blind(self):
        self.assertEqual(datagate.blind_spots(SCOPE, SEED),
                         ["charts/web/values.yaml"],
                         "only the chart file; the parsed manifest carries names")

    def test_an_irsa_service_on_a_chart_component_is_not_called_elsewhere(self):
        """The regression this class exists for. Before the fix the service
        landed in `elsewhere` and residue() printed that it held someone
        else's component — a positive false statement about the exact
        dependency shape the join was built for."""
        result = datagate.verdict(
            exports([service("orders-db", consumers=[
                consumer("orders", namespace="orders")])]),
            self.CHART_SCOPE, self.CHART_SEED)
        self.assertEqual(result["elsewhere"], [])
        self.assertEqual(len(result["untestable"]), 1)
        self.assertEqual(result["unattributed"], [],
                         "the mapping DID name a consumer — the gap is ours")
        residue = datagate.residue(result)
        self.assertNotIn("records in no file this component ships", residue)
        self.assertIn("records a name only where a document declares",
                      residue,
                      "state the RULE: two enumerations of causes were both "
                      "wrong for a kustomization.yaml, which parses fine and "
                      "records no name")
        self.assertIn("Helm chart content", residue)
        self.assertIn("kustomization", residue)
        self.assertIn("carry no indexed workload name", residue)

    def test_a_re_resolved_scope_never_claims_elsewhere(self):
        """Re-resolved paths come from the index BY CONSTRUCTION, so
        `blind_spots` can never flag one that is missing — and nobody checked
        the globs enumerate the component. A developer who wrote `k8s/**`
        while the component also ships `charts/orders/` gets a
        fully-indexed-LOOKING scope that is simply incomplete."""
        seed = {"k8s/app.yaml": {"kinds": ["Deployment"], "namespaces": [],
                                 "team_labels": [],
                                 "names": ["Deployment/app"]}}
        args = (exports([service("orders-db", consumers=[consumer("orders")])]),
                ["k8s/app.yaml"], seed)

        verified = datagate.verdict(*args)
        self.assertEqual(len(verified["elsewhere"]), 1,
                         "a scope the developer signed off against the index "
                         "may make the claim")

        guessed = datagate.verdict(*args, scope_reresolved=True)
        self.assertEqual(guessed["elsewhere"], [])
        self.assertEqual(len(guessed["untestable"]), 1)
        residue = datagate.residue(guessed)
        self.assertNotIn("records in no file this component ships", residue)
        # The reason must be THERE. `_cannot_tell` keyed on `blind` alone
        # returned "" for this route — a re-resolved scope has nothing blind
        # — and the residue rendered "hold nothing as a result — .", blanking
        # the explanation out of the one paragraph read before approving.
        self.assertIn("re-resolved", residue)
        self.assertIn("nobody has ever checked", residue)
        self.assertNotIn("— .", residue)
        self.assertIn("re-resolved", datagate.advisory(guessed))

    def test_the_advisory_names_the_blind_case_too(self):
        """Both sites word it, and they drifted the moment they were written
        separately — hence the single `_cannot_tell` builder."""
        result = datagate.verdict(
            exports([service("orders-db", consumers=[consumer("orders")])]),
            self.CHART_SCOPE, self.CHART_SEED)
        advisory = datagate.advisory(result)
        self.assertIn("could not be matched to this component", advisory)
        self.assertIn("carry no indexed workload name", advisory)
        self.assertIn("rds orders-db", advisory)
        self.assertNotIn("no attributed consumer", advisory,
                         "the mapping named one; the gap is on our side")

    def test_a_fully_indexed_component_still_says_elsewhere(self):
        """The positive control: `elsewhere` must remain reachable, or the
        fix above would be indistinguishable from deleting the bucket."""
        result = datagate.verdict(
            exports([service("billing-db", consumers=[consumer("billing")])]),
            ["k8s/orders.yaml"],
            {"k8s/orders.yaml": SEED["k8s/orders.yaml"]})
        self.assertEqual(len(result["elsewhere"]), 1)
        self.assertEqual(result["untestable"], [])
        self.assertEqual(result["unattributed"], [])
        residue = datagate.residue(result)
        self.assertIn("records in no file this component ships", residue)
        self.assertIn("what the check establishes and no more", residue,
                      "the index recording no name is weaker than the "
                      "component not declaring one — a kind: List bundle "
                      "contributes none")
        self.assertNotIn("hold their own components", residue.lower(),
                         "being fully indexed proves the name is not HERE; it "
                         "proves nothing about where it is")

    def test_a_blind_scope_does_not_suppress_a_real_match(self):
        # One chart file (blind) plus one indexed manifest that DOES name the
        # workload: the match wins, and the component is held.
        seed = dict(self.CHART_SEED)
        seed["k8s/orders.yaml"] = SEED["k8s/orders.yaml"]
        result = datagate.verdict(
            exports([service("orders-db", consumers=[
                consumer("orders", namespace="orders")])]),
            self.CHART_SCOPE + ["k8s/orders.yaml"], seed)
        self.assertEqual(len(result["blocking"]), 1)
        self.assertEqual(result["untestable"], [])


class RepoWidePathTest(unittest.TestCase):
    """`attach_data_consumer(source_path=...)` is free text and the record is
    durable, replayed over every later scan."""

    def test_a_repo_wide_chart_path_is_not_evidence(self):
        for wide in (".", "/", "", "  ", "./"):
            result = datagate.verdict(
                exports([service("db", consumers=[
                    consumer("nothing-here", kind="helm_release",
                             source_path=wide, detection="human_review")])]),
                ["k8s/orders.yaml"],
                {"k8s/orders.yaml": SEED["k8s/orders.yaml"]})
            self.assertEqual(result["blocking"], [], f"source_path={wide!r}")

    def test_a_hand_typed_dot_slash_prefix_still_matches(self):
        """`consumers.py` normpaths what it produces, but
        `attach_data_consumer(source_path=...)` is free text. A reviewer
        typing the path the way it appears in Terraform must not silently
        fail to hold the component."""
        self.assertTrue(
            datagate._under("charts/web/values.yaml", "./charts/web"))
        self.assertTrue(
            datagate._under("charts/web/values.yaml", "charts/web/"))
        self.assertFalse(
            datagate._under("charts/web/values.yaml", "../charts/web"),
            "a path climbing out of the checkout is under nothing in scope")

    def test_a_real_chart_path_still_matches(self):
        result = datagate.verdict(
            exports([service("db", consumers=[
                consumer("web", kind="helm_release", source_path="charts/web",
                         detection="terraform_wiring")])]), SCOPE, SEED)
        self.assertEqual(len(result["blocking"]), 1)

    def test_a_sibling_directory_is_not_a_prefix_match(self):
        """The scope is the SIBLING and the consumer names the shorter path,
        which is the direction a bare `startswith` gets wrong. Written the
        other way round the assertion cannot fail."""
        self.assertTrue(datagate._under("charts/web/values.yaml", "charts/web"))
        self.assertFalse(
            datagate._under("charts/web-legacy/values.yaml", "charts/web"),
            "charts/web-legacy is a different chart")
        result = datagate.verdict(
            exports([service("db", consumers=[
                consumer("nothing-here", kind="helm_release",
                         source_path="charts/web",
                         detection="terraform_wiring")])]),
            ["charts/web-legacy/values.yaml"],
            {"charts/web-legacy/values.yaml": {
                "kinds": ["helm-chart"], "namespaces": [], "team_labels": [],
                "names": []}})
        self.assertEqual(result["blocking"], [])


class MismatchTest(unittest.TestCase):
    """Discovery proposes, the developer binds — and when the two disagree
    the gate reports it instead of picking a side. It still holds: the
    disagreement is about a detail, not about whether the workload is here."""

    def test_a_namespace_contradiction_is_reported_and_still_holds(self):
        result = datagate.verdict(
            exports([service("orders-db", consumers=[
                consumer("orders", namespace="payments")])]), SCOPE, SEED)
        self.assertEqual(len(result["blocking"]), 1)
        self.assertEqual(len(result["mismatches"]), 1)
        detail = result["mismatches"][0]["detail"]
        self.assertIn("namespace 'payments'", detail)
        self.assertIn("orders", detail)

    def test_a_chart_path_outside_the_scope_with_the_name_inside_is_reported(self):
        result = datagate.verdict(
            exports([service("orders-db", consumers=[
                consumer("orders", kind="helm_release",
                         source_path="charts/legacy-orders",
                         detection="terraform_wiring")])]), SCOPE, SEED)
        self.assertEqual(len(result["blocking"]), 1)
        self.assertEqual(len(result["mismatches"]), 1)
        self.assertIn("charts/legacy-orders", result["mismatches"][0]["detail"])

    def test_an_agreeing_namespace_produces_no_mismatch(self):
        result = datagate.verdict(
            exports([service("orders-db", consumers=[
                consumer("orders", namespace="orders")])]), SCOPE, SEED)
        self.assertEqual(result["mismatches"], [])


class CopyTest(unittest.TestCase):

    def result(self, services, scope=SCOPE):
        return datagate.verdict(exports(services), scope, SEED)

    def test_a_clear_component_gets_no_advisory_and_no_refusal(self):
        result = self.result([service("a", status="migrated"),
                              service("b", disposition="keep-in-aws")])
        self.assertEqual(datagate.advisory(result), "")
        self.assertEqual(datagate.refusal(result, "orders"), "")
        self.assertEqual(datagate.residue(result), "")

    def test_the_advisory_says_it_does_not_block(self):
        result = self.result([service("orders-db", consumers=[
            consumer("orders", namespace="orders")])])
        text = datagate.advisory(result)
        self.assertIn("rds orders-db", text)
        self.assertIn("does not block scoping, planning, translation or "
                      "review", text)

    def test_the_refusal_names_the_owner_and_both_exits(self):
        result = self.result([service("orders-db", consumers=[
            consumer("orders", namespace="orders")])])
        text = datagate.refusal(result, "orders-component")
        self.assertIn("orders-component", text)
        self.assertIn("Owner: Platform Engineer", text)
        self.assertIn("mark_data_service_migrated", text)
        self.assertIn("keep-in-aws", text)
        self.assertIn("nothing has to be re-planned or re-translated",
                      text.lower())

    def test_the_refusal_reports_the_status_it_read(self):
        result = self.result([service("orders-db", status="in_progress",
                                      consumers=[consumer("orders")])])
        self.assertIn("reported in_progress", datagate.refusal(result, "c"))
        result = self.result([service("orders-db",
                                      consumers=[consumer("orders")])])
        self.assertIn("reported not started", datagate.refusal(result, "c"))

    def test_a_degraded_scope_is_reported_as_itself_not_as_a_bad_mapping(self):
        """Two different situations, and one message for both sent the
        reader to the data review to fix a mapping that was fine."""
        result = datagate.verdict(
            exports([service("orders-db", consumers=[consumer("orders")])]),
            None, SEED)
        residue = datagate.residue(result)
        self.assertIn("never resolved to a file list", residue)
        self.assertNotIn("no attributed consumer", residue)
        self.assertIn("never resolved to a file list",
                      datagate.advisory(result))

    def test_the_residue_reports_both_kinds_of_nobody_separately(self):
        """Two facts with two remedies. SCOPE carries a chart file, so the
        billing consumer cannot be tested here rather than being placed."""
        result = self.result([service("orphan-db"),
                              service("billing-db",
                                      consumers=[consumer("billing")])])
        text = datagate.residue(result)
        self.assertIn("no attributed consumer", text)
        self.assertIn("rds orphan-db", text)
        self.assertIn("could not be tested against this component", text)
        self.assertIn("rds billing-db", text)
        self.assertNotIn("outside this component's scope", text,
                         "a blind scope must not be reported as placement")

    def test_the_two_sides_name_a_service_the_same_way(self):
        # The handoff is one person telling another that a service landed.
        from servers.phases.deployment import datamigration as dm
        entry = {"service": "s3", "identifier": "acme-invoice-archive"}
        self.assertEqual(datagate.describe(entry), dm.describe(entry))


class DriftTest(unittest.TestCase):
    """One rule, stated in three modules that may not import each other.

    The deployment phase owns it, the workload phase restates it (a developer
    module must not import a platform one — different sessions, different
    credentials), and servers/dag/server restates it again because it imports
    no phase code at all. Nothing but this test keeps them equal, and a drift
    would mean the platform step and the ship gate disagree about what is
    owed while both report success.
    """

    def test_the_gating_disposition_is_the_same_in_all_three(self):
        from servers.dag.server import exports
        from servers.phases.deployment import datamigration as dm
        self.assertEqual(datagate.GATING_DISPOSITION, dm.GATING_DISPOSITION)
        self.assertEqual(datagate.GATING_DISPOSITION, exports.GATING_DISPOSITION)

    def test_the_migrated_status_is_the_same_in_all_three(self):
        from servers.dag.server import exports
        from servers.phases.deployment import datamigration as dm
        self.assertEqual(datagate.MIGRATED, dm.MIGRATED)
        self.assertEqual(datagate.MIGRATED, exports.MIGRATED_STATUS)

    def test_every_disposition_the_schema_allows_is_decided(self):
        """A disposition the schema gains and this gate has never seen would
        default to not-gating, silently. The enumeration is the decision."""
        import json
        import os
        schema_path = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            "..", "..", "dag", "server", "schema", "inventory.json")
        with open(os.path.normpath(schema_path)) as f:
            schema = json.load(f)
        allowed = set(schema["properties"]["data_dependencies"]["items"]
                      ["properties"]["disposition"]["enum"])
        self.assertEqual(
            allowed,
            {"migrate", "rebuild", "replatform", "escalate", "keep-in-aws",
             "undecided"},
            "a new disposition must be argued about in workload/datagate's "
            "docstring before this list is widened — the default is 'does "
            "not gate', which is the wrong default to reach by accident")


if __name__ == "__main__":
    unittest.main()
