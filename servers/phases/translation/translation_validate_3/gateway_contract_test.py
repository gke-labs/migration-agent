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

"""Unit tests for the shared-Gateway output contract. Pure, no GCS."""

import copy
import unittest

from servers.phases.translation.translation_validate_3 import gateway_contract

MARKED = """\
apiVersion: gateway.networking.k8s.io/v1
kind: Gateway
metadata:
  name: shared-entry
  namespace: platform-gateway
  annotations:
    gkma.dev/shared-gateway: "true"
spec:
  gatewayClassName: gke-l7-global-external-managed
  listeners:
  - name: http
    protocol: HTTP
    port: 80
    allowedRoutes:
      namespaces:
        from: Selector
        selector:
          matchLabels:
            gkma.dev/gateway-access: "shared"
"""

# The brief-conforming platform Namespace the gateway unit ships beside its
# Gateway: byte-identical name, carrying the product attach label.
PLATFORM_NS = """\
apiVersion: v1
kind: Namespace
metadata:
  name: platform-gateway
  labels:
    gkma.dev/gateway-access: "shared"
"""


def unit(content=MARKED, kind="gateway", path="gateway.yaml",
         ns=PLATFORM_NS):
    files = [{"path": path, "content": content}]
    if ns is not None:
        files.append({"path": "namespace.yaml", "content": ns})
    return {"unit": {"unit_id": "gateway", "kind": kind},
            "result": {"files": files}}


def tenancy(*namespaces):
    """A tenancy unit shipping one Namespace document per (name, labeled)."""
    docs = []
    for name, labeled in namespaces:
        doc = f"apiVersion: v1\nkind: Namespace\nmetadata:\n  name: {name}\n"
        if labeled:
            doc += '  labels:\n    gkma.dev/gateway-access: "shared"\n'
        docs.append(doc)
    return {"unit": {"unit_id": "tenancy", "kind": "tenancy"},
            "result": {"files": [{"path": "namespaces.yaml",
                                  "content": "---\n".join(docs)}]}}


class GatewayContractTest(unittest.TestCase):

    def test_the_brief_conforming_unit_passes(self):
        report = gateway_contract.check_units([unit()])
        self.assertEqual(report["checked"], ["gateway"])
        self.assertEqual(report["findings"], [])

    def test_an_unmarked_gateway_is_a_finding_not_a_silent_null(self):
        # The failure this gate exists for: every other instruction followed,
        # the annotation dropped — exports.gateway would stay null forever.
        content = MARKED.replace(
            '  annotations:\n    gkma.dev/shared-gateway: "true"\n', "")
        report = gateway_contract.check_units([unit(content)])
        self.assertEqual(len(report["findings"]), 1)
        error = report["findings"][0]["error"]
        self.assertIn("platform-gateway/shared-entry", error)
        self.assertIn("gkma.dev/shared-gateway", error)

    def test_a_marker_set_to_false_is_not_a_marker(self):
        report = gateway_contract.check_units(
            [unit(MARKED.replace('"true"', '"false"'))])
        self.assertEqual(len(report["findings"]), 1)

    def test_a_label_marker_counts(self):
        report = gateway_contract.check_units(
            [unit(MARKED.replace("annotations:", "labels:"))])
        self.assertEqual(report["findings"], [])

    def test_a_gateway_unit_shipping_no_gateway_manifest_is_a_finding(self):
        report = gateway_contract.check_units(
            [unit("apiVersion: v1\nkind: Namespace\nmetadata:\n  name: gw\n")])
        self.assertEqual(len(report["findings"]), 1)
        self.assertIn("no Gateway API Gateway manifest",
                      report["findings"][0]["error"])

    def test_a_missing_namespace_is_a_finding(self):
        # exports.gateway.namespace is read off metadata.namespace alone; a
        # namespace applied out of band publishes null and parks every route.
        report = gateway_contract.check_units(
            [unit(MARKED.replace("  namespace: platform-gateway\n", ""))])
        self.assertEqual(len(report["findings"]), 1)
        self.assertIn("metadata.namespace", report["findings"][0]["error"])

    def test_two_marked_gateways_are_a_finding(self):
        report = gateway_contract.check_units(
            [unit(MARKED + "---\n" + MARKED.replace("shared-entry", "other"))])
        self.assertEqual(len(report["findings"]), 1)
        self.assertIn("2 Gateway manifests", report["findings"][0]["error"])

    def test_an_extra_unmarked_gateway_beside_the_marked_one_is_a_finding(self):
        extra = MARKED.replace("shared-entry", "app-gw").replace(
            '  annotations:\n    gkma.dev/shared-gateway: "true"\n', "")
        report = gateway_contract.check_units([unit(MARKED + "---\n" + extra)])
        self.assertEqual(len(report["findings"]), 1)
        self.assertIn("extra Gateway manifests", report["findings"][0]["error"])

    def test_an_unparseable_manifest_is_named_not_read_as_absent(self):
        report = gateway_contract.check_units([unit("a: [oops")])
        self.assertTrue(any("gateway.yaml" in f["error"]
                            for f in report["findings"]))

    def test_only_gateway_kind_units_are_checked(self):
        # A network unit legitimately emitting an app-scoped Gateway (the
        # 2026-08-14 e2e case) is not this contract's business.
        report = gateway_contract.check_units(
            [unit(MARKED.replace('    gkma.dev/shared-gateway: "true"\n', ""),
                  kind="network")])
        self.assertEqual(report["checked"], [])
        self.assertEqual(report["findings"], [])

    def test_no_units_at_all_is_clean(self):
        self.assertEqual(gateway_contract.check_units([]),
                         {"checked": [], "findings": []})

    def test_a_satisfied_selector_over_shipped_namespaces_passes(self):
        # Both sides of the attach contract shipped: the tenancy unit's
        # Namespaces carry the product label the Selector reads.
        report = gateway_contract.check_units(
            [unit(), tenancy(("acme-shop", True), ("acme-batch", True))])
        self.assertEqual(report["findings"], [])

    def test_an_unlabeled_shipped_namespace_is_named(self):
        # The acme e2e failure: a Selector no shipped Namespace satisfied —
        # attachedRoutes 0 behind a structurally-valid report (KB1).
        report = gateway_contract.check_units(
            [unit(), tenancy(("acme-shop", False))])
        self.assertEqual(len(report["findings"]), 1)
        error = report["findings"][0]["error"]
        self.assertIn("acme-shop", error)
        self.assertIn("accepted and never attach", error)
        self.assertIn("gkma.dev/gateway-access", error)

    def test_from_all_passes_without_any_labels(self):
        content = MARKED.replace(
            "        from: Selector\n        selector:\n"
            "          matchLabels:\n"
            '            gkma.dev/gateway-access: "shared"\n',
            "        from: All\n")
        report = gateway_contract.check_units(
            [unit(content), tenancy(("acme-shop", False))])
        self.assertEqual(report["findings"], [])

    def test_from_same_is_a_finding(self):
        content = MARKED.replace(
            "        from: Selector\n        selector:\n"
            "          matchLabels:\n"
            '            gkma.dev/gateway-access: "shared"\n',
            "        from: Same\n")
        report = gateway_contract.check_units([unit(content)])
        self.assertEqual(len(report["findings"]), 1)
        self.assertIn("`from: Same`", report["findings"][0]["error"])
        self.assertIn("attaches none", report["findings"][0]["error"])

    def test_an_absent_allowedroutes_policy_is_a_finding(self):
        # Absence IS `Same`: the Gateway API default accepts a route and
        # never attaches it — the silent arm the 2026-08-14 run dodged by
        # luck. The pre-audit fixture (no allowedRoutes) must now fail.
        content = MARKED[:MARKED.index("    allowedRoutes:")]
        report = gateway_contract.check_units([unit(content)])
        self.assertEqual(len(report["findings"]), 1)
        error = report["findings"][0]["error"]
        self.assertIn("no explicit allowedRoutes.namespaces", error)
        self.assertIn("Gateway API default", error)

    def test_a_gateway_with_no_listeners_is_a_finding(self):
        content = MARKED[:MARKED.index("  listeners:")]
        report = gateway_contract.check_units([unit(content)])
        self.assertEqual(len(report["findings"]), 1)
        self.assertIn("declares no listeners", report["findings"][0]["error"])

    def test_an_unverifiable_selector_is_a_finding(self):
        content = MARKED.replace(
            "        selector:\n          matchLabels:\n"
            '            gkma.dev/gateway-access: "shared"\n',
            "        selector:\n          matchExpressions:\n"
            "          - {key: team, operator: Exists}\n")
        report = gateway_contract.check_units([unit(content)])
        self.assertEqual(len(report["findings"]), 1)
        self.assertIn("cannot verify", report["findings"][0]["error"])

    def test_an_unknown_from_value_is_a_finding(self):
        report = gateway_contract.check_units(
            [unit(MARKED.replace("from: Selector", "from: Cluster"))])
        self.assertEqual(len(report["findings"]), 1)
        self.assertIn("'Cluster'", report["findings"][0]["error"])

    def test_an_unshipped_platform_namespace_is_a_finding(self):
        # exports publishes the attach point from FILES: without this arm a
        # Gateway whose namespace nothing creates publishes cleanly while
        # `kubectl apply` fails — and apply -f order is alphabetical, so
        # gateway.yaml lands before any namespace file anyway (F1-EXT-2).
        report = gateway_contract.check_units([unit(ns=None)])
        self.assertEqual(len(report["findings"]), 1)
        error = report["findings"][0]["error"]
        self.assertIn('namespaces "platform-gateway" not found', error)

    def test_a_namespace_name_mismatch_is_a_finding(self):
        report = gateway_contract.check_units(
            [unit(ns=PLATFORM_NS.replace("name: platform-gateway",
                                         "name: platform-gw"))])
        findings = [f["error"] for f in report["findings"]]
        self.assertTrue(any('namespaces "platform-gateway" not found' in e
                            for e in findings))

    def test_a_cross_unit_namespace_collision_is_a_finding(self):
        # The X9 reverse arm: the gateway unit picks a name the tenancy
        # unit also issues — two Namespace manifests for one object.
        report = gateway_contract.check_units(
            [unit(), tenancy(("platform-gateway", True))])
        self.assertEqual(len(report["findings"]), 1)
        error = report["findings"][0]["error"]
        self.assertIn("more than one unit", error)
        self.assertIn("gateway, tenancy", error)

    def test_shape_findings_come_first_and_alone(self):
        # A malformed Gateway returns to review on its shape finding; the
        # attach half is judged only over a well-formed marked Gateway.
        content = MARKED.replace(
            '  annotations:\n    gkma.dev/shared-gateway: "true"\n', ""
        ).replace(
            "        from: Selector\n        selector:\n"
            "          matchLabels:\n"
            '            gkma.dev/gateway-access: "shared"\n',
            "        from: Same\n")
        report = gateway_contract.check_units([unit(content)])
        self.assertEqual(len(report["findings"]), 1)
        self.assertIn("gkma.dev/shared-gateway",
                      report["findings"][0]["error"])

    def test_namespaces_the_run_does_not_ship_are_not_judged(self):
        # The estate has namespaces this run never issues; the gate stays
        # silent about them — only shipped documents are evidence.
        report = gateway_contract.check_units([unit()])
        self.assertEqual(report["findings"], [])

    def test_findings_carry_the_unit_id(self):
        entry = copy.deepcopy(unit("apiVersion: v1\nkind: Namespace\n"
                                   "metadata:\n  name: gw\n"))
        entry["unit"]["unit_id"] = "gateway-2"
        report = gateway_contract.check_units([entry])
        self.assertEqual(report["findings"][0]["unit_id"], "gateway-2")


if __name__ == "__main__":
    unittest.main()
