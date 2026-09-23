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

"""Unit tests for the shared Kubernetes manifest structure check. Pure."""

import unittest

from servers.phases import k8s_manifests

VALID_MANIFEST = """apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: fast-ssd
provisioner: pd.csi.storage.gke.io
"""


class ManifestStructureErrorTest(unittest.TestCase):

    def test_valid_single_document(self):
        self.assertEqual(k8s_manifests.manifest_structure_error(VALID_MANIFEST), "")

    def test_valid_multi_document(self):
        content = VALID_MANIFEST + "---\n" + VALID_MANIFEST.replace("fast-ssd", "slow-hdd")
        self.assertEqual(k8s_manifests.manifest_structure_error(content), "")

    def test_trailing_separator_is_not_a_document(self):
        self.assertEqual(k8s_manifests.manifest_structure_error(VALID_MANIFEST + "---\n"), "")

    def test_unparseable_yaml(self):
        error = k8s_manifests.manifest_structure_error("key: [unclosed")
        self.assertIn("not parseable as YAML", error)

    def test_empty_file(self):
        for content in ("", "---\n", "# comment only\n"):
            self.assertIn("contains no YAML documents",
                          k8s_manifests.manifest_structure_error(content))

    def test_non_mapping_document(self):
        error = k8s_manifests.manifest_structure_error("- a\n- b\n")
        self.assertEqual(error, "document 1 is not a mapping")

    def test_missing_api_version(self):
        error = k8s_manifests.manifest_structure_error("kind: Namespace\nmetadata:\n  name: x\n")
        self.assertEqual(error, "document 1 is missing 'apiVersion'")

    def test_missing_kind(self):
        error = k8s_manifests.manifest_structure_error("apiVersion: v1\nmetadata:\n  name: x\n")
        self.assertEqual(error, "document 1 is missing 'kind'")

    def test_missing_metadata_name(self):
        content = "apiVersion: v1\nkind: Namespace\nmetadata:\n  generateName: x-\n"
        error = k8s_manifests.manifest_structure_error(content)
        self.assertEqual(error, "document 1 (kind Namespace) is missing 'metadata.name'")

    def test_missing_metadata_entirely(self):
        error = k8s_manifests.manifest_structure_error("apiVersion: v1\nkind: Namespace\n")
        self.assertIn("missing 'metadata.name'", error)

    def test_error_names_the_failing_document(self):
        content = VALID_MANIFEST + "---\napiVersion: v1\nmetadata:\n  name: x\n"
        error = k8s_manifests.manifest_structure_error(content)
        self.assertEqual(error, "document 2 is missing 'kind'")

    def test_aliases_are_rejected(self):
        # Anchor/alias expansion is the billion-laughs vector; the loader
        # refuses the alias outright rather than expanding it.
        error = k8s_manifests.manifest_structure_error("a: &x [1, 2]\nb: *x\n")
        self.assertIn("not parseable as YAML", error)
        self.assertIn("aliases are not supported", error)

    def test_anchor_without_alias_is_allowed(self):
        content = "apiVersion: v1\nkind: Namespace\nmetadata:\n  name: &n payments\n"
        self.assertEqual(k8s_manifests.manifest_structure_error(content), "")

    def test_oversized_content_is_rejected(self):
        error = k8s_manifests.manifest_structure_error(
            "#" + "x" * k8s_manifests.MAX_MANIFEST_BYTES)
        self.assertIn("too large", error)


class LoadManifestDocumentsTest(unittest.TestCase):
    """The shared hardened loader the workload planner parses with."""

    def test_returns_non_null_documents(self):
        docs = k8s_manifests.load_manifest_documents(
            "apiVersion: v1\nkind: Namespace\nmetadata:\n  name: a\n---\n")
        self.assertEqual(len(docs), 1)
        self.assertEqual(docs[0]["kind"], "Namespace")

    def test_alias_raises_value_error(self):
        with self.assertRaisesRegex(ValueError, "aliases"):
            k8s_manifests.load_manifest_documents(
                "a: &x [1, 2]\nb: *x\n")

    def test_oversize_raises_value_error(self):
        with self.assertRaisesRegex(ValueError, "too large"):
            k8s_manifests.load_manifest_documents(
                "#" + "x" * k8s_manifests.MAX_MANIFEST_BYTES)

    def test_unparseable_raises_value_error(self):
        with self.assertRaisesRegex(ValueError, "not parseable"):
            k8s_manifests.load_manifest_documents("a: [unclosed")


if __name__ == "__main__":
    unittest.main()
