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

"""Unit tests for the family table parser. Pure — reads the real document."""

import unittest

from server import computeclass_families as families

HEADER = ("| AWS family | GCP `machineFamily` candidates | arch |\n"
          "|---|---|---|\n")


def _table(rows, heading="## 3. Family table (machine-read)"):
    """A minimal document holding the given table rows."""
    body = "\n".join(rows)
    return f"# Title\n\nprose\n\n{heading}\n\nintro line\n\n{HEADER}{body}\n\n## 4. Rules\n"


SYNTHETIC = [
    "| m5, m6i, m7i | n4, n2 | amd64 |",
    "| c5, c6i | c4, c2 | amd64 |",
    "| m6g, m7g | n4a, c4a | arm64 |",
    "| (no constraint) | n4, n2, e2 | amd64 |",
    "| (no constraint) | n4a, c4a | arm64 |",
]


class RealDocumentTest(unittest.TestCase):
    """The bundled gke-compute-classes.md is the authority; pin its shape."""

    @classmethod
    def setUpClass(cls):
        cls.rows = families.load_family_table(refresh=True)

    def test_every_row_uses_the_fixed_arch_vocabulary(self):
        for row in self.rows:
            self.assertIn(row["arch"], families.ARCHITECTURES, row)
            self.assertTrue(row["gcp"], row)

    def test_load_bearing_families_are_present(self):
        for family in ("m6i", "c6i", "r6i", "m7g", "t3"):
            self.assertTrue(families.candidates_for(family, table=self.rows), family)

    def test_both_no_constraint_rows_are_present(self):
        archs = sorted(row["arch"] for row in self.rows if not row["aws"])
        self.assertEqual(archs, ["amd64", "arm64"])

    def test_known_mappings(self):
        self.assertEqual(families.candidates_for("m6i", table=self.rows), ["n4", "n2"])
        self.assertEqual(families.candidates_for("M7G", table=self.rows), ["n4a", "c4a"])
        self.assertEqual(families.candidates_for("t3", table=self.rows), ["e2"])

    def test_validate_passes_on_the_real_document(self):
        families.validate_family_table()

    def test_cache_is_reused_until_refreshed(self):
        self.assertIs(families.load_family_table(), self.rows)


class ParseTest(unittest.TestCase):

    def test_parses_rows_lowercased_and_in_order(self):
        rows = families.parse_family_table(_table(SYNTHETIC), "t")
        self.assertEqual(len(rows), 5)
        self.assertEqual(rows[0], {"aws": ["m5", "m6i", "m7i"], "gcp": ["n4", "n2"],
                                   "arch": "amd64"})
        self.assertEqual(rows[3]["aws"], [])
        self.assertEqual(rows[4], {"aws": [], "gcp": ["n4a", "c4a"], "arch": "arm64"})

    def test_backticks_and_case_in_cells_are_tolerated(self):
        rows = families.parse_family_table(
            _table(["| `M5` | `n4`, N2 | AMD64 |"]), "t")
        self.assertEqual(rows, [{"aws": ["m5"], "gcp": ["n4", "n2"], "arch": "amd64"}])

    def test_section_heading_survives_a_renumber(self):
        rows = families.parse_family_table(
            _table(SYNTHETIC, heading="### Family table"), "t")
        self.assertEqual(len(rows), 5)

    def test_missing_section_is_fatal(self):
        with self.assertRaises(families.ComputeClassFamiliesError) as cm:
            families.parse_family_table("# Title\n\n## Other\n\n" + HEADER, "t")
        self.assertIn("Family table", str(cm.exception))

    def test_empty_table_is_fatal(self):
        with self.assertRaises(families.ComputeClassFamiliesError) as cm:
            families.parse_family_table(_table([]), "t")
        self.assertIn("empty", str(cm.exception))

    def test_unknown_arch_is_fatal(self):
        with self.assertRaises(families.ComputeClassFamiliesError) as cm:
            families.parse_family_table(_table(["| m5 | n4 | x86 |"]), "t")
        self.assertIn("x86", str(cm.exception))

    def test_empty_candidates_cell_is_fatal(self):
        with self.assertRaises(families.ComputeClassFamiliesError) as cm:
            families.parse_family_table(_table(["| m5 |  | amd64 |"]), "t")
        self.assertIn("no GCP machineFamily candidate", str(cm.exception))

    def test_duplicate_aws_family_across_rows_is_fatal(self):
        with self.assertRaises(families.ComputeClassFamiliesError) as cm:
            families.parse_family_table(
                _table(["| m5, m6i | n4 | amd64 |", "| m6i | n2 | amd64 |"]), "t")
        self.assertIn("m6i", str(cm.exception))
        self.assertIn("twice", str(cm.exception))

    def test_duplicate_no_constraint_row_for_one_arch_is_fatal(self):
        with self.assertRaises(families.ComputeClassFamiliesError):
            families.parse_family_table(
                _table(["| (no constraint) | n4 | amd64 |",
                        "| (no constraint) | n2 | amd64 |"]), "t")

    def test_pipe_count_mismatch_is_fatal(self):
        with self.assertRaises(families.ComputeClassFamiliesError) as cm:
            families.parse_family_table(_table(["| m5 | n4 |"]), "t")
        self.assertIn("cells", str(cm.exception))
        with self.assertRaises(families.ComputeClassFamiliesError):
            families.parse_family_table(_table(["| m5 | n4 | amd64 | extra |"]), "t")

    def test_missing_document_is_fatal(self):
        with self.assertRaises(families.ComputeClassFamiliesError):
            families.load_family_table(repo_root="/nonexistent/anchor", refresh=True)
        families.load_family_table(refresh=True)  # restore the cache


class LookupTest(unittest.TestCase):

    def setUp(self):
        self.table = families.parse_family_table(_table(SYNTHETIC), "t")

    def test_candidates_for_matches_lowercase(self):
        self.assertEqual(families.candidates_for("M6I", table=self.table), ["n4", "n2"])
        self.assertEqual(families.candidates_for("m7g", table=self.table), ["n4a", "c4a"])

    def test_candidates_for_unknown_family_is_empty(self):
        self.assertEqual(families.candidates_for("z9", table=self.table), [])

    def test_candidates_for_respects_arch(self):
        self.assertEqual(families.candidates_for("m6i", arch="arm64", table=self.table), [])
        self.assertEqual(families.candidates_for("m6i", arch="amd64", table=self.table),
                         ["n4", "n2"])

    def test_candidates_for_none_is_the_no_constraint_union(self):
        self.assertEqual(families.candidates_for(None, table=self.table),
                         ["n4", "n2", "e2", "n4a", "c4a"])
        self.assertEqual(families.candidates_for(None, arch="arm64", table=self.table),
                         ["n4a", "c4a"])

    def test_allowed_families_unions_the_source_families(self):
        self.assertEqual(families.allowed_families(["m6i", "c6i"], [], table=self.table),
                         {"n4", "n2", "c4", "c2"})

    def test_allowed_families_filters_by_architecture(self):
        self.assertEqual(families.allowed_families(["m6i", "m7g"], ["arm64"], table=self.table),
                         {"n4a", "c4a"})
        self.assertEqual(families.allowed_families(["m6i", "m7g"], [], table=self.table),
                         {"n4", "n2", "n4a", "c4a"})

    def test_allowed_families_with_no_source_family_uses_no_constraint_rows(self):
        self.assertEqual(families.allowed_families([], ["amd64"], table=self.table),
                         {"n4", "n2", "e2"})
        self.assertEqual(families.allowed_families([], [], table=self.table),
                         {"n4", "n2", "e2", "n4a", "c4a"})

    def test_allowed_families_unknown_family_contributes_nothing(self):
        self.assertEqual(families.allowed_families(["z9"], ["amd64"], table=self.table), set())


if __name__ == "__main__":
    unittest.main()
