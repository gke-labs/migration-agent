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

"""Unit tests for the pure worker-result validation. No GCS, no LLM."""

import unittest

from servers.phases.translation.translation_translate_1 import translator

VALID_TF = 'resource "google_storage_bucket" "b" {\n  name = var.name\n}\n'
VALID_MANIFEST = """apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: fast-ssd
provisioner: pd.csi.storage.gke.io
"""


def _result(files):
    return {"files": files, "tradeoffs": "t", "assumptions": [], "open_questions": []}


class ValidateTranslationTest(unittest.TestCase):

    def test_accepts_terraform_only(self):
        result = _result([{"path": "main.tf", "content": VALID_TF}])
        self.assertEqual(translator.validate_translation(result), "")

    def test_accepts_manifest_only(self):
        result = _result([{"path": "storageclass.yaml", "content": VALID_MANIFEST}])
        self.assertEqual(translator.validate_translation(result), "")

    def test_accepts_mixed_terraform_and_manifests(self):
        result = _result([
            {"path": "gsa.tf", "content": VALID_TF},
            {"path": "manifests/ksa.yml", "content": VALID_MANIFEST},
        ])
        self.assertEqual(translator.validate_translation(result), "")

    def test_rejects_other_extensions(self):
        result = _result([{"path": "notes.txt", "content": "x"}])
        error = translator.validate_translation(result)
        self.assertIn("must end in .tf, .yaml, or .yml", error)

    def test_rejects_escaping_paths_regardless_of_type(self):
        for path in ("../evil.yaml", "/abs/evil.tf"):
            result = _result([{"path": path, "content": VALID_MANIFEST}])
            self.assertIn("must be unit-relative", translator.validate_translation(result))

    def test_terraform_brace_check_still_applies(self):
        result = _result([{"path": "main.tf", "content": "resource {\n"}])
        self.assertIn("unbalanced braces", translator.validate_translation(result))

    def test_manifest_errors_name_the_file(self):
        result = _result([{"path": "bad.yaml", "content": "just a plain string"}])
        error = translator.validate_translation(result)
        self.assertIn("bad.yaml", error)
        self.assertIn("not a mapping", error)




class FamilyKnowledgeTest(unittest.TestCase):
    """The knowledge document for a unit kind rides with the prompt, not the brief."""

    def test_cluster_dns_prompt_carries_the_mapping_document(self):
        unit = {"unit_id": "cluster-dns", "kind": "cluster-dns", "title": "t",
                "inputs": {"cluster_dns": {"sources": []}}, "notes": ["n"]}
        prompt = translator.build_translation_prompt(unit, {})
        self.assertIn("--- Unit knowledge", prompt)
        self.assertIn("Cloud DNS for GKE", prompt)
        self.assertIn("## Output contract", prompt)
        # Attached after the unit payload, so the JSON stays parseable on its own.
        self.assertLess(prompt.index("Translation unit:"), prompt.index("--- Unit knowledge"))

    def test_other_kinds_get_no_document(self):
        unit = {"unit_id": "storage", "kind": "storage", "title": "t", "inputs": {}, "notes": []}
        self.assertNotIn("--- Unit knowledge", translator.build_translation_prompt(unit, {}))

    def test_every_registered_document_exists_and_reads(self):
        translator.validate_family_knowledge()
        for kind in translator.FAMILY_KNOWLEDGE:
            self.assertTrue(translator.load_family_knowledge(kind, refresh=True).strip())

    def test_a_missing_document_is_fatal(self):
        from unittest import mock
        with mock.patch.dict(translator.FAMILY_KNOWLEDGE, {"ghost": "landingzone/knowledge/ghost.md"}):
            with self.assertRaises(translator.FamilyKnowledgeError):
                translator.validate_family_knowledge()


if __name__ == "__main__":
    unittest.main()
