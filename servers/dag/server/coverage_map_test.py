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

"""Unit tests for the coverage map parser. Pure — reads the real document."""

import unittest

from server import coverage_map


def _table(rows):
    """A minimal document holding the given table rows."""
    body = "\n".join(rows)
    return (
        "# Title\n\nprose\n\n## Coverage map\n\n"
        "| Artifact kind | Column | Owner | Discovered from | Notes |\n"
        "|---|---|---|---|---|\n"
        f"{body}\n\n## Next section\n"
    )


class RealDocumentTest(unittest.TestCase):
    """The bundled coverage-map.md is the authority; pin its load-bearing shape."""

    @classmethod
    def setUpClass(cls):
        cls.rows = coverage_map.load_coverage_map(refresh=True)

    def test_every_row_uses_the_fixed_vocabulary(self):
        for key, row in self.rows.items():
            self.assertIn(row["owner"], coverage_map.OWNERS, key)
            self.assertIn(row["column"], coverage_map.COLUMNS, key)

    def test_all_three_owners_are_represented(self):
        owners = {row["owner"] for row in self.rows.values()}
        self.assertEqual(owners, set(coverage_map.OWNERS))

    def test_known_boundary_assignments(self):
        by_kind = {key: row for key, row in self.rows.items()}
        sc = by_kind[coverage_map.normalize_kind("StorageClass tier menu")]
        self.assertEqual((sc["column"], sc["owner"]), ("k8s", "platform-translation"))
        cluster = by_kind[coverage_map.normalize_kind("GKE cluster (control plane, target shape)")]
        self.assertEqual((cluster["column"], cluster["owner"]), ("terraform", "landing-zone"))
        route = by_kind[coverage_map.normalize_kind("HTTPRoute (per-workload routing)")]
        self.assertEqual((route["column"], route["owner"]), ("k8s", "workload"))

    def test_workload_terraform_cell_stays_empty(self):
        # A structural property of the map (see the document): the developer
        # phase writes Kubernetes objects, never infrastructure. A row landing
        # in workload x terraform is a misclassification until argued otherwise.
        offenders = [row["kind"] for row in self.rows.values()
                     if row["owner"] == "workload" and row["column"] == "terraform"]
        self.assertEqual(offenders, [])


class ParserTest(unittest.TestCase):

    def test_missing_section_is_fatal(self):
        with self.assertRaises(coverage_map.CoverageMapError):
            coverage_map.parse_coverage_map("# Title\n\nno table here\n", "doc")

    def test_empty_table_is_fatal(self):
        with self.assertRaises(coverage_map.CoverageMapError):
            coverage_map.parse_coverage_map(_table([]), "doc")

    def test_unknown_owner_is_fatal_not_skipped(self):
        rows = ["| Thing | terraform | platform | — |  |"]
        with self.assertRaisesRegex(coverage_map.CoverageMapError, "unknown owner"):
            coverage_map.parse_coverage_map(_table(rows), "doc")

    def test_unknown_column_is_fatal(self):
        rows = ["| Thing | hcl | landing-zone | — |  |"]
        with self.assertRaisesRegex(coverage_map.CoverageMapError, "unknown column"):
            coverage_map.parse_coverage_map(_table(rows), "doc")

    def test_duplicate_kind_is_fatal_across_formatting(self):
        rows = [
            "| `GKE  cluster` | terraform | landing-zone | — |  |",
            "| gke cluster | terraform | landing-zone | — |  |",
        ]
        with self.assertRaisesRegex(coverage_map.CoverageMapError, "listed twice"):
            coverage_map.parse_coverage_map(_table(rows), "doc")

    def test_empty_kind_is_fatal_not_skipped(self):
        # An empty kind must not slip past vocabulary validation: a blanked
        # cell would otherwise vanish the row (bogus owner and all) from the
        # map the future checks read.
        rows = ["| | terraform | bogus-owner | x | note |"]
        with self.assertRaisesRegex(coverage_map.CoverageMapError, "empty artifact kind"):
            coverage_map.parse_coverage_map(_table(rows), "doc")

    def test_short_row_is_fatal(self):
        rows = ["| Thing | terraform |"]
        with self.assertRaises(coverage_map.CoverageMapError):
            coverage_map.parse_coverage_map(_table(rows), "doc")

    def test_owner_and_column_are_case_insensitive(self):
        rows = ["| Thing | Terraform | Landing-Zone | `network` | note |"]
        parsed = coverage_map.parse_coverage_map(_table(rows), "doc")
        row = parsed[coverage_map.normalize_kind("Thing")]
        self.assertEqual((row["column"], row["owner"]), ("terraform", "landing-zone"))
        self.assertEqual(row["source"], "`network`")
        self.assertEqual(row["notes"], "note")

    def test_heading_prefix_match_survives_retitle(self):
        text = _table(["| Thing | k8s | workload | — |  |"]).replace(
            "## Coverage map", "### Coverage map — ownership table, v2")
        parsed = coverage_map.parse_coverage_map(text, "doc")
        self.assertEqual(len(parsed), 1)


if __name__ == "__main__":
    unittest.main()
