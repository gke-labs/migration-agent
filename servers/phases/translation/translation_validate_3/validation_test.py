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

"""Unit tests for the validation module over materialized units.

check_unit_manifests and the fix-worker path guards run with no terraform
and no LLM; the run_validation ordering proof needs a real terraform on
PATH (provider-free fixtures — no network, no credentials) and stubs the
fix worker.
"""

import asyncio
import os
import shutil
import tempfile
import unittest
from unittest.mock import AsyncMock, patch

from servers.phases.translation.translation_validate_3 import root_wiring, validation

VALID_MANIFEST = """apiVersion: v1
kind: Namespace
metadata:
  name: payments
"""


def _write(root, rel, content):
    full = os.path.join(root, rel)
    os.makedirs(os.path.dirname(full), exist_ok=True)
    with open(full, "w", encoding="utf-8") as f:
        f.write(content)


class CheckUnitManifestsTest(unittest.TestCase):

    def test_clean_and_invalid_are_reported_per_file(self):
        with tempfile.TemporaryDirectory() as clone:
            unit = "translation-units/tenancy"
            _write(clone, f"{unit}/namespace.yaml", VALID_MANIFEST)
            _write(clone, f"{unit}/nested/quota.yml", "kind: ResourceQuota\n")
            report = validation.check_unit_manifests(clone, [unit])
        self.assertEqual(report["checked"], 2)
        self.assertEqual(report["clean"], [f"{unit}/namespace.yaml"])
        self.assertEqual([e["file"] for e in report["invalid"]], [f"{unit}/nested/quota.yml"])
        self.assertIn("missing 'apiVersion'", report["invalid"][0]["error"])

    def test_terraform_files_are_ignored(self):
        with tempfile.TemporaryDirectory() as clone:
            unit = "translation-units/storage"
            _write(clone, f"{unit}/main.tf", "resource {}\n")
            report = validation.check_unit_manifests(clone, [unit])
        self.assertEqual(report, {"checked": 0, "clean": [], "invalid": []})

    def test_only_the_unit_dirs_are_scanned(self):
        with tempfile.TemporaryDirectory() as clone:
            _write(clone, "translation-units/u1/ok.yaml", VALID_MANIFEST)
            # Clone-root YAML (the target repo's own files) is not this gate's
            # to judge and must stay out of the report.
            _write(clone, "ci-config.yaml", "not: [valid k8s")
            report = validation.check_unit_manifests(clone, ["translation-units/u1"])
        self.assertEqual(report["checked"], 1)
        self.assertEqual(report["invalid"], [])

    def test_missing_unit_dir_contributes_nothing(self):
        with tempfile.TemporaryDirectory() as clone:
            report = validation.check_unit_manifests(clone, ["translation-units/absent"])
        self.assertEqual(report, {"checked": 0, "clean": [], "invalid": []})

    def test_ordering_is_deterministic_across_dirs_and_files(self):
        with tempfile.TemporaryDirectory() as clone:
            _write(clone, "u/b/z.yaml", VALID_MANIFEST)
            _write(clone, "u/b/a.yaml", VALID_MANIFEST)
            _write(clone, "u/a/m.yml", VALID_MANIFEST)
            report = validation.check_unit_manifests(clone, ["u"])
        self.assertEqual(report["clean"], ["u/a/m.yml", "u/b/a.yaml", "u/b/z.yaml"])

    def test_terraform_init_droppings_are_not_judged(self):
        # The gate runs after `terraform init`, which materializes remote
        # module sources — CI workflows and all — under .terraform/. Those
        # files are gitignored and never ship; judging them would dead-end
        # validation on files the reviewer cannot fix.
        with tempfile.TemporaryDirectory() as clone:
            unit = "translation-units/storage"
            _write(clone, f"{unit}/storageclass.yaml", VALID_MANIFEST)
            _write(clone, f"{unit}/.terraform/modules/gcs/.github/workflows/ci.yml",
                   "name: ci\non: push\n")
            report = validation.check_unit_manifests(clone, [unit])
        self.assertEqual(report["checked"], 1)
        self.assertEqual(report["clean"], [f"{unit}/storageclass.yaml"])
        self.assertEqual(report["invalid"], [])


class WorkerFixScopeTest(unittest.TestCase):
    """The clone root's repair worker may not write into unit directories:
    a nested path returned there is invented unit code that would bypass the
    unit-blob fold-back and ship while the Review UI shows the original."""

    ROOT_FILES = {"main.tf": "# root\n"}

    def _fix(self, worker_files, allow_subdirs):
        async def run():
            with patch.object(validation.agent_workers, "call_worker",
                              new=AsyncMock(return_value="raw")), \
                 patch.object(validation.agent_workers, "parse_worker_json",
                              return_value={"files": worker_files}):
                return await validation._worker_fix(
                    dict(self.ROOT_FILES), "boom", "model",
                    allow_subdirs=allow_subdirs)
        return asyncio.run(run())

    def test_the_root_pass_rejects_nested_paths(self):
        merged = self._fix(
            [{"path": "translation-units/u-1/main.tf", "content": "# invented\n"},
             {"path": "main.tf", "content": "# repaired\n"}],
            allow_subdirs=False)
        self.assertEqual(merged, {"main.tf": "# repaired\n"})

    def test_a_unit_pass_keeps_its_own_subdirectories(self):
        merged = self._fix(
            [{"path": "modules/local/extra.tf", "content": "# repaired\n"}],
            allow_subdirs=True)
        self.assertEqual(merged["modules/local/extra.tf"], "# repaired\n")
        self.assertEqual(merged["main.tf"], "# root\n")


class ReadTfFilesTest(unittest.TestCase):

    def test_terraform_module_cache_is_not_swept_in(self):
        # The fix worker and the fold-back must see the unit's own code, not
        # the module sources `terraform init` downloaded under .terraform/.
        with tempfile.TemporaryDirectory() as unit_dir:
            _write(unit_dir, "main.tf", "resource {}\n")
            _write(unit_dir, "modules/local/extra.tf", "# unit-authored\n")
            _write(unit_dir, ".terraform/modules/gcs/main.tf", "# downloaded\n")
            files = validation._read_tf_files(unit_dir)
        self.assertEqual(sorted(files),
                         ["main.tf", os.path.join("modules", "local", "extra.tf")])


@unittest.skipUnless(shutil.which("terraform"), "terraform is not on PATH")
class RunValidationOrderingTest(unittest.TestCase):
    """Units before the root: a fixable unit-level error must end all_valid
    True. The unit's own pass (the only one whose worker sees the unit's
    files) repairs it first; the root pass then compiles the repaired code.
    Root-first burned the root's fix attempts on an error its worker could
    not reach and left `.` failing after the unit was fixed."""

    def test_a_fixable_unit_error_ends_all_valid_true(self):
        terraform = shutil.which("terraform")
        broken = 'output "b" {\n  value = local.never_declared\n}\n'
        repaired = 'output "b" {\n  value = "ok"\n}\n'

        async def stub_fix(files, error, model, allow_subdirs=True):
            self.assertIn("never_declared", error)
            return {**files, "main.tf": repaired}

        with tempfile.TemporaryDirectory() as clone:
            _write(clone, "main.tf", 'locals {\n  lz = "draft"\n}\n')
            _write(clone, "translation-units/u-1/main.tf", broken)
            placed = {"u-1": "translation-units/u-1"}
            root_wiring.write(clone, placed)
            tf_dirs = sorted(placed.values()) + ["."]
            with patch.object(validation, "_worker_fix", new=stub_fix):
                report = asyncio.run(
                    validation.run_validation(clone, tf_dirs, terraform))

        self.assertTrue(report["all_valid"], report)
        self.assertEqual([f["dir"] for f in report["fixed"]],
                         ["translation-units/u-1"])
        self.assertIn(".", report["clean"])
        self.assertEqual(report["remaining"], [])


if __name__ == "__main__":
    unittest.main()
