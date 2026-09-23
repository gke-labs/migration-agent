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

"""T4: the facts-present set of the live coverage map over FROZEN fixtures.

A coverage CL adds a map row sourced only from the typed field it adds
(the new-row rule, coverage-guards G1): on every inventory extracted before
that CL the row is `no-facts`, so no in-flight workspace can be wedged at
the validate gate. This test pins that, mechanically. The fixtures under
fixtures/ are frozen: a coverage CL ADDS a fixture, never edits one, and
the expected set per fixture is a literal here. Three kinds of edit may
move a literal — an edited `Discovered from` cell, a schema edit that
removes or renames a declared path (the row drops to unknown-section), an
artifact-kind rename or row deletion — and the CL that moves it says why.
"""

import json
import os
import unittest

from servers.dag.server import coverage_map
from servers.phases.landingzone.landingzone_translationplan_3 import coverage

_FIXTURES = os.path.join(os.path.dirname(os.path.realpath(__file__)), "fixtures")

# The facts-present rows every frozen fixture must yield, by normalized kind.
_REAL_MAP_ROWS = {
    "gke cluster (control plane, target shape)",
    "base vpc, subnets, secondary ranges",
    "gke node pools",
    "node auto-provisioning (karpenter replacement)",
    "cluster addon disposition (drop / built-in / reconfigure)",
    "workload identity: gsa and iam bindings",
    "cross-vpc connectivity and load balancers",
    "filestore instances",
    "privileged workload policy",
    "hostnetwork workload compatibility",
    "storageclass tier menu",
    "gateway (shared entry point)",
    "cluster dns resolver configuration (stub domains, upstream nameservers)",
    "cluster dns zones and records (hosts entries, forwarding zones)",
    "namespaces",
    "resourcequota and limitrange",
    "httproute (per-workload routing)",
    "ksa and workload identity annotation",
    "pvc storage class usage",
    "networkpolicy",
}

EXPECTED = {
    # A dated copy of coverage_gate_test.RealMapEndToEndTest.INVENTORY: no
    # typed NodePool, so the ComputeClass row is no-facts here.
    "real-map-e2e-2026-09-16.json": _REAL_MAP_ROWS,
    "empty-2026-09-16.json": set(),
    # The same estate with one typed NodePool (acme shop-burst): the
    # ComputeClass row joins, and only it.
    "acme-karpenter-2026-09-16.json": _REAL_MAP_ROWS | {
        "computeclass (karpenter nodepool replacement)"},
}


def _facts_present(inventory):
    rows = coverage_map.load_coverage_map(refresh=True)
    schema = coverage.load_inventory_schema()
    verdicts = coverage.instantiate_coverage_map(rows, inventory, schema)
    return {v["row_key"] for v in verdicts if v["status"] == coverage.STATUS_FACTS_PRESENT}


class FrozenFixtureTest(unittest.TestCase):

    def test_every_fixture_has_an_expected_set(self):
        on_disk = sorted(f for f in os.listdir(_FIXTURES) if f.endswith(".json"))
        self.assertEqual(on_disk, sorted(EXPECTED))

    def test_facts_present_sets_are_pinned(self):
        for name, expected in EXPECTED.items():
            with self.subTest(fixture=name):
                with open(os.path.join(_FIXTURES, name), encoding="utf-8") as f:
                    inventory = json.load(f)
                self.assertEqual(_facts_present(inventory), expected)

    def test_no_row_is_unknown_section_on_any_fixture(self):
        rows = coverage_map.load_coverage_map(refresh=True)
        schema = coverage.load_inventory_schema()
        for name in EXPECTED:
            with open(os.path.join(_FIXTURES, name), encoding="utf-8") as f:
                inventory = json.load(f)
            verdicts = coverage.instantiate_coverage_map(rows, inventory, schema)
            self.assertEqual(
                [v["row_key"] for v in verdicts if v["status"] == coverage.STATUS_UNKNOWN_SECTION],
                [], name)


if __name__ == "__main__":
    unittest.main()
