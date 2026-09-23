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

"""Tests for the Live IR contract (server/schema/live_discovery.json).

The walk's own suites hold the IRs they produce to the schema (the seam test
in live_discovery_test, the happy-path walk in k8s_live_test); these tests
cover the schema itself — that it is a well-formed draft 2020-12 document,
that the smallest honest IRs pass, and that the two invariants it states
outright (a finding carries no value, a Secret record carries key names)
actually bite.
"""

import copy
import unittest

import jsonschema

from servers.phases.discovery.discovery_livescan_0 import (k8s_live, live_schema,
                                                            projection)

_SUMMARY = {
    "regions_scanned": 1, "regions_unreachable": [],
    "clusters_found": 0, "clusters_walked": 0,
    "clusters_unreachable": [], "clusters_using_karpenter": 0,
    "workload_totals": {}, "note_count": 0,
}


def _minimal_ir(**overrides):
    ir = {"live_ir_version": "2.0", "regions": ["us-east-1"], "clusters": [],
          "notes": [], "summary": dict(_SUMMARY)}
    ir.update(overrides)
    return ir


def _empty_walk():
    """The in-cluster walk over a control plane that serves /version and
    404s everything else: every section present and empty."""
    return k8s_live.discover_cluster(
        lambda path: {"gitVersion": "v1.29.0"} if path == "/version" else None)


class SchemaDocumentTest(unittest.TestCase):

    def test_schema_is_a_valid_draft_2020_12_document(self):
        schema = live_schema.load_schema()
        self.assertEqual(schema["$schema"],
                         "https://json-schema.org/draft/2020-12/schema")
        jsonschema.Draft202012Validator.check_schema(schema)

    def test_schema_version_matches_the_engine(self):
        from servers.phases.discovery.discovery_livescan_0 import live_discovery
        self.assertEqual(
            live_schema.load_schema()["properties"]["live_ir_version"]["const"],
            live_discovery.LIVE_IR_VERSION)


class MinimalDocumentsTest(unittest.TestCase):

    def test_empty_estate_validates(self):
        live_schema.validate_live_ir(_minimal_ir())

    def test_tool_provenance_stamp_is_allowed(self):
        live_schema.validate_live_ir(
            _minimal_ir(scanned_by="arn:aws:iam::1:user/eng"))

    def test_unreachable_cluster_stands_on_its_aws_record(self):
        live_schema.validate_live_ir(_minimal_ir(clusters=[{
            "name": "prod", "region": "us-east-1",
            "kubernetes_error": "timed out", "tags": {"team": "<omitted>"}}]))

    def test_empty_in_cluster_walk_validates(self):
        live_schema.validate_live_ir(_minimal_ir(clusters=[{
            "name": "prod", "region": "us-east-1",
            "kubernetes": _empty_walk()}]))

    def test_violation_names_the_path_and_not_the_value(self):
        # The message is persisted as a coverage note; a value in the wrong
        # field must not travel with it.
        with self.assertRaises(ValueError) as caught:
            live_schema.validate_live_ir(_minimal_ir(regions="us-east-1"))
        message = str(caught.exception)
        self.assertIn("regions", message)
        self.assertIn("type", message)
        self.assertNotIn("us-east-1", message)

    def test_unknown_top_level_key_is_a_violation(self):
        # The structure the walk owns is closed: a new key is a contract
        # change, and the schema is where it must land first.
        with self.assertRaises(ValueError):
            live_schema.validate_live_ir(_minimal_ir(extra=1))


class StatedInvariantsTest(unittest.TestCase):

    def test_a_secret_record_admits_the_marker_and_nothing_else(self):
        walk = _empty_walk()
        walk["config"]["secrets"].append({
            "apiVersion": "v1", "kind": "Secret", "type": "Opaque",
            "metadata": {"name": "db", "namespace": "shop"}})
        live_schema.validate_live_ir(_minimal_ir(clusters=[{
            "name": "prod", "region": "us-east-1", "kubernetes": walk}]))
        # A data value it carries is the marker and nothing else — the
        # schema names the constant, so a walk that persisted a Secret
        # without projecting it fails here rather than reaching the bucket.
        for field in ("data", "stringData"):
            projected = copy.deepcopy(walk)
            projected["config"]["secrets"][0][field] = {
                "password": projection.OMITTED}
            live_schema.validate_live_ir(_minimal_ir(clusters=[{
                "name": "prod", "region": "us-east-1", "kubernetes": projected}]))
            leaked = copy.deepcopy(walk)
            leaked["config"]["secrets"][0][field] = {"password": "aHVudGVyMg=="}
            with self.assertRaises(ValueError) as caught:
                live_schema.validate_live_ir(_minimal_ir(clusters=[{
                    "name": "prod", "region": "us-east-1", "kubernetes": leaked}]))
            self.assertIn(field, str(caught.exception))

    def test_manifest_bodies_are_open(self):
        # A Deployment's spec is Kubernetes' contract, not this one's.
        walk = _empty_walk()
        walk["workloads"]["Deployment"].append({
            "apiVersion": "apps/v1", "kind": "Deployment",
            "metadata": {"name": "web", "namespace": "shop",
                         "labels": {"app": "web"}},
            "spec": {"replicas": 3, "template": {"spec": {"containers": []}}}})
        live_schema.validate_live_ir(_minimal_ir(clusters=[{
            "name": "prod", "region": "us-east-1", "kubernetes": walk}]))


class DescribeTest(unittest.TestCase):
    """A persisted note quotes the schema's side of a violation, never the
    instance's — and the count keywords repeat the instance they measured,
    so they are not on the safe list."""

    def test_a_count_violation_does_not_quote_the_instance(self):
        for schema, instance in (
                ({"type": "array", "minItems": 2}, ["SECRETVAL"]),
                ({"type": "array", "maxItems": 1}, ["SECRETVAL", "x"]),
                ({"type": "array", "uniqueItems": True}, ["SECRETVAL", "SECRETVAL"]),
                ({"type": "object", "minProperties": 2}, {"k": "SECRETVAL"}),
                ({"type": "string", "pattern": "^a$"}, "SECRETVAL")):
            with self.subTest(schema=schema):
                err = next(jsonschema.Draft202012Validator(schema)
                           .iter_errors(instance))
                text = live_schema._describe(err)
                self.assertNotIn("SECRETVAL", text)
                self.assertIn(err.validator, text)

    def test_a_missing_property_is_named(self):
        err = next(jsonschema.Draft202012Validator(
            {"type": "object", "required": ["name"]})
            .iter_errors({"other": "SECRETVAL"}))
        text = live_schema._describe(err)
        self.assertIn("'name' is a required property", text)
        self.assertNotIn("SECRETVAL", text)


if __name__ == "__main__":
    unittest.main()
