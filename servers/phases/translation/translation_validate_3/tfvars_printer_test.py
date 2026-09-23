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

"""Unit tests for the tfvars printer.

The printer is pure text plus a directory listing, like the root wiring.
The terraform-backed class at the end is the Pass-3 proof: with the
printed file a provider-free clone answers `terraform plan` with zero
value prompts, and without it the same clone halts on
`No value for required variable`.
"""

import inspect
import os
import shutil
import tempfile
import unittest

from servers.phases.translation.translation_validate_3 import (
    root_wiring, tfvars_printer, tools, validation)


def _write(root, rel, content):
    full = os.path.join(root, rel)
    os.makedirs(os.path.dirname(full), exist_ok=True)
    with open(full, "w", encoding="utf-8") as f:
        f.write(content)


def _read(root, rel):
    with open(os.path.join(root, rel), "r", encoding="utf-8") as f:
        return f.read()


CLUSTER_TF = ('resource "google_container_cluster" "this" {\n'
              '  name     = "acme-gke"\n'
              '  location = "us-central1"\n'
              '}\n')


class CollectValuesTest(unittest.TestCase):
    """Which facts are recorded, and which absences stay absences."""

    def collect(self, tree: dict, config: dict) -> dict:
        with tempfile.TemporaryDirectory() as clone:
            for rel, content in tree.items():
                _write(clone, rel, content)
            return tfvars_printer.collect_values(config, clone)

    def test_the_workspace_project_is_a_recorded_fact(self):
        values = self.collect({}, {"gcp_project": "real-project"})
        self.assertEqual(values["project_id"]["value"], "real-project")
        self.assertEqual(values["project"]["value"], "real-project")
        self.assertIn("workspace settings", values["project_id"]["source"])

    def test_no_workspace_project_prints_nothing_for_it(self):
        self.assertNotIn("project_id", self.collect({}, {}))
        self.assertNotIn("project_id", self.collect({}, {"gcp_project": ""}))

    def test_a_single_literal_cluster_records_name_and_region(self):
        values = self.collect(
            {"terraform/modules/gke-cluster/main.tf": CLUSTER_TF}, {})
        self.assertEqual(values["cluster_name"]["value"], "acme-gke")
        self.assertEqual(values["region"]["value"], "us-central1")
        self.assertIn("design literal", values["cluster_name"]["source"])

    def test_a_zonal_location_is_not_printed_as_a_region(self):
        values = self.collect(
            {"main.tf": CLUSTER_TF.replace("us-central1", "us-central1-a")}, {})
        self.assertEqual(values["cluster_name"]["value"], "acme-gke")
        self.assertNotIn("region", values)

    def test_two_clusters_record_no_cluster_facts(self):
        # With two on disk "the cluster's name" is not a fact; the coverage
        # gate's singleton check is what rejects that clone.
        values = self.collect(
            {"a.tf": CLUSTER_TF, "b.tf": CLUSTER_TF.replace("acme", "beta")}, {})
        self.assertNotIn("cluster_name", values)
        self.assertNotIn("region", values)

    def test_a_commented_out_cluster_is_not_a_recorded_fact(self):
        commented = "".join(f"# {line}\n" for line in CLUSTER_TF.splitlines())
        self.assertNotIn("cluster_name", self.collect({"main.tf": commented}, {}))

    def test_a_nested_name_is_never_the_clusters_own(self):
        # The 2026-08-16 review's blocker: without nested-brace folding,
        # a name inside node_pool{} or resource_labels={} would print as
        # the cluster's — a wrong value wearing a recorded-fact comment.
        tf = ('resource "google_container_cluster" "this" {\n'
              '  resource_labels = {\n    name = "team-alpha"\n  }\n'
              '  node_pool {\n    name = "default-pool"\n  }\n'
              '  name     = "acme-gke"\n'
              '  location = "us-central1"\n'
              '}\n')
        values = self.collect({"main.tf": tf}, {})
        self.assertEqual(values["cluster_name"]["value"], "acme-gke")

    def test_a_nested_literal_never_stands_in_for_a_computed_name(self):
        tf = ('resource "google_container_cluster" "this" {\n'
              '  name = var.cluster_name\n'
              '  node_pool {\n    name = "default-pool"\n  }\n'
              '}\n')
        self.assertNotIn("cluster_name", self.collect({"main.tf": tf}, {}))

    def test_a_unit_declared_cluster_does_not_void_the_landing_zones_facts(self):
        # Ownership matches the coverage gate's count: the unit-declared
        # cluster is that gate's owner-boundary finding, and must not
        # silently drop cluster_name/region here (F-C would reopen).
        values = self.collect(
            {"main.tf": CLUSTER_TF,
             "translation-units/np/main.tf": CLUSTER_TF.replace("acme", "np")},
            {})
        self.assertEqual(values["cluster_name"]["value"], "acme-gke")

    def test_the_designs_literal_project_outranks_the_workspace_project(self):
        # The exports derivation's precedence: a single literal cluster
        # project wins; gcp_project is the fallback.
        tf = CLUSTER_TF.replace('  name     = "acme-gke"\n',
                                '  name     = "acme-gke"\n'
                                '  project  = "design-project"\n')
        values = self.collect({"main.tf": tf}, {"gcp_project": "ledger-project"})
        self.assertEqual(values["project_id"]["value"], "design-project")
        self.assertIn("design literal", values["project_id"]["source"])
        fallback = self.collect({"main.tf": CLUSTER_TF},
                                {"gcp_project": "ledger-project"})
        self.assertEqual(fallback["project_id"]["value"], "ledger-project")

    def test_a_computed_name_is_not_a_literal(self):
        # The exports scan discipline: computed values are read as None,
        # never guessed at.
        tf = ('resource "google_container_cluster" "this" {\n'
              '  name     = var.cluster_name\n'
              '  location = "us-central1"\n'
              '}\n')
        values = self.collect({"main.tf": tf}, {})
        self.assertNotIn("cluster_name", values)
        self.assertEqual(values["region"]["value"], "us-central1")

    def test_an_interpolated_name_is_not_a_literal(self):
        tf = CLUSTER_TF.replace('"acme-gke"', '"${var.prefix}-gke"')
        self.assertNotIn("cluster_name", self.collect({"main.tf": tf}, {}))

    def test_a_template_directive_is_not_a_literal_either(self):
        # %{...} directives are computed values too; _strip_hcl blanks the
        # braces, so without the % exclusion this would print a mangled
        # string as a design literal (2026-08-16 gate-2 review).
        tf = CLUSTER_TF.replace('"acme-gke"', '"acme-%{if true}gke%{endif}"')
        self.assertNotIn("cluster_name", self.collect({"main.tf": tf}, {}))

    def test_units_subdir_matches_the_materializers(self):
        # Duplicated constant, pinned — the coverage gate's pattern.
        self.assertEqual(tfvars_printer.UNITS_SUBDIR, tools.UNITS_SUBDIR)


class RenderTest(unittest.TestCase):

    def test_values_are_printed_under_their_sources_sorted_and_stable(self):
        printed = {
            "region": {"value": "us-central1", "source": "design literal"},
            "project_id": {"value": "p-1", "source": "workspace settings"},
        }
        text = tfvars_printer.render(printed)
        self.assertTrue(text.startswith(tfvars_printer.HEADER_LINES[0]))
        self.assertIn('# source: workspace settings\nproject_id = "p-1"', text)
        self.assertIn('# source: design literal\nregion = "us-central1"', text)
        self.assertLess(text.index("project_id ="), text.index("region ="))
        self.assertEqual(text, tfvars_printer.render(dict(reversed(printed.items()))))

    def test_nothing_printed_renders_nothing(self):
        self.assertEqual(tfvars_printer.render({}), "")

    def test_values_are_escaped_never_interpolated(self):
        printed = {"odd": {"value": 'a"b\\c${d}%{e}', "source": "s"}}
        text = tfvars_printer.render(printed)
        self.assertIn(r'odd = "a\"b\\c$${d}%%{e}"', text)


class EmitTest(unittest.TestCase):
    """What lands on disk, what is reported missing, and whose file wins."""

    VALUES = {"project_id": {"value": "p-1", "source": "workspace settings"}}

    def test_prints_covered_root_variables_and_reports_uncovered_required(self):
        with tempfile.TemporaryDirectory() as clone:
            _write(clone, "variables.tf",
                   'variable "project_id" {}\n'
                   'variable "region" {\n  default = "us-central1"\n}\n')
            result = tfvars_printer.emit(clone, self.VALUES)
            text = _read(clone, tfvars_printer.TFVARS_FILENAME)
        self.assertEqual(result["file"], tfvars_printer.TFVARS_FILENAME)
        self.assertEqual(result["printed"],
                         {"project_id": "workspace settings"})
        self.assertEqual(result["missing"], [])
        self.assertIn('project_id = "p-1"', text)
        # A defaulted root variable with NO recorded fact keeps its own
        # default (the design's interior choice) and is not missing.
        self.assertNotIn("region", text)

    def test_a_recorded_fact_overrides_a_defaulted_root_declaration(self):
        # The F-C shape one level up (2026-08-16 review): an LLM-authored
        # placeholder default at the root must not outvote the ledger — a
        # tfvars assignment legally overrides a declaration default.
        with tempfile.TemporaryDirectory() as clone:
            _write(clone, "variables.tf",
                   'variable "project_id" {\n  default = "invented-proj"\n}\n')
            result = tfvars_printer.emit(clone, self.VALUES)
            text = _read(clone, tfvars_printer.TFVARS_FILENAME)
        self.assertEqual(list(result["printed"]), ["project_id"])
        self.assertEqual(result["missing"], [])
        self.assertIn('project_id = "p-1"', text)

    def test_an_uncovered_no_default_variable_is_missing_with_its_line(self):
        with tempfile.TemporaryDirectory() as clone:
            _write(clone, "variables.tf",
                   '# a comment line\nvariable "gsa_email" {}\n')
            result = tfvars_printer.emit(clone, self.VALUES)
        self.assertIsNone(result["file"])
        self.assertEqual(result["missing"],
                         [{"name": "gsa_email", "file": "variables.tf",
                           "line": 2}])

    def test_generated_declarations_in_the_wiring_file_count(self):
        # emit runs after root_wiring.write: the pass-through declarations
        # the wiring generates are root declarations like any other.
        with tempfile.TemporaryDirectory() as clone:
            _write(clone, "translation-units/wi/main.tf",
                   'variable "project_id" {}\nvariable "gsa_email" {}\n')
            root_wiring.write(clone, {"wi": "translation-units/wi"})
            result = tfvars_printer.emit(clone, self.VALUES)
        self.assertEqual(list(result["printed"]), ["project_id"])
        self.assertEqual([m["name"] for m in result["missing"]], ["gsa_email"])
        self.assertEqual(result["missing"][0]["file"],
                         root_wiring.WIRING_FILENAME)

    def test_the_file_is_rewritten_whole_and_stable(self):
        with tempfile.TemporaryDirectory() as clone:
            _write(clone, "variables.tf", 'variable "project_id" {}\n')
            tfvars_printer.emit(clone, self.VALUES)
            first = _read(clone, tfvars_printer.TFVARS_FILENAME)
            _write(clone, tfvars_printer.TFVARS_FILENAME,
                   first + '\nstale = "left behind"\n')
            tfvars_printer.emit(clone, self.VALUES)
            second = _read(clone, tfvars_printer.TFVARS_FILENAME)
        self.assertEqual(first, second)

    def test_a_stale_generated_file_is_removed_when_nothing_prints(self):
        with tempfile.TemporaryDirectory() as clone:
            _write(clone, "variables.tf", 'variable "project_id" {}\n')
            tfvars_printer.emit(clone, self.VALUES)
            # The next run's root declares nothing the facts cover.
            os.remove(os.path.join(clone, "variables.tf"))
            result = tfvars_printer.emit(clone, self.VALUES)
            exists = os.path.exists(
                os.path.join(clone, tfvars_printer.TFVARS_FILENAME))
        self.assertIsNone(result["file"])
        self.assertFalse(exists)

    def test_a_customer_file_at_the_tfvars_name_is_never_touched(self):
        foreign = 'their_var = "their value"\n'
        with tempfile.TemporaryDirectory() as clone:
            _write(clone, "variables.tf",
                   'variable "project_id" {}\nvariable "gsa_email" {}\n')
            _write(clone, tfvars_printer.TFVARS_FILENAME, foreign)
            result = tfvars_printer.emit(clone, self.VALUES)
            on_disk = _read(clone, tfvars_printer.TFVARS_FILENAME)
        self.assertEqual(on_disk, foreign)
        self.assertIsNone(result["file"])
        self.assertEqual(result["foreign_file"], tfvars_printer.TFVARS_FILENAME)
        # An unread customer file proves nothing about the values plan will
        # prompt for: missing still reports.
        self.assertEqual([m["name"] for m in result["missing"]], ["gsa_email"])


class GitignoreTest(unittest.TestCase):
    """The clone is the customer's repo: their canonical Terraform
    .gitignore excludes *.tfvars, which would silently drop the printed
    file from the PR. The validation .gitignore must force-include it."""

    def test_a_customer_tfvars_ignore_is_negated(self):
        with tempfile.TemporaryDirectory() as clone:
            _write(clone, ".gitignore", "*.tfvars\n.terraform/\n")
            tools._write_gitignore(clone)
            text = _read(clone, ".gitignore")
        negation = f"!{tfvars_printer.TFVARS_FILENAME}"
        self.assertIn(negation, text)
        # Last matching rule wins in git: the negation must come after the
        # customer's exclusion.
        self.assertLess(text.index("*.tfvars"), text.index(negation))

    def test_the_generated_gitignore_carries_the_negation_too(self):
        with tempfile.TemporaryDirectory() as clone:
            tools._write_gitignore(clone)
            first = _read(clone, ".gitignore")
            tools._write_gitignore(clone)
            second = _read(clone, ".gitignore")
        self.assertIn(f"!{tfvars_printer.TFVARS_FILENAME}", first)
        self.assertIn(".terraform/", first)
        # Idempotent: a re-run appends nothing.
        self.assertEqual(first, second)

    def test_a_complete_gitignore_is_left_untouched(self):
        content = ".terraform/\n!" + tfvars_printer.TFVARS_FILENAME + "\n"
        with tempfile.TemporaryDirectory() as clone:
            _write(clone, ".gitignore", content)
            tools._write_gitignore(clone)
            self.assertEqual(_read(clone, ".gitignore"), content)


class ToolsConnectionTest(unittest.TestCase):
    """Source pins: the validate tool actually consults the printer.

    The full tool needs GCS; these pins assert the connection the way the
    coverage gate's harness arm does — a report key nobody reads and a
    finding that does not flip all_valid are both silent regressions.
    """

    SOURCE = inspect.getsource(tools.run_generated_validation)

    def test_the_tool_emits_and_gates_on_the_printer(self):
        self.assertIn("tfvars_printer.collect_values(config, clone_dir)",
                      self.SOURCE)
        self.assertIn("tfvars_printer.emit(clone_dir, facts)", self.SOURCE)
        self.assertIn('known_values=frozenset(facts)', self.SOURCE)
        self.assertIn('report["tfvars"] = tfvars', self.SOURCE)

    def test_emit_runs_after_the_wiring_and_after_the_fix_loop(self):
        # The printer's whole contract: the wiring's generated declarations
        # must count as root declarations, and the root fix worker can add
        # declarations during the run — printing earlier would miss both.
        self.assertLess(self.SOURCE.index("root_wiring.write"),
                        self.SOURCE.index("validation.run_validation"))
        self.assertLess(self.SOURCE.index("validation.run_validation"),
                        self.SOURCE.index("tfvars_printer.emit"))

    def test_every_printer_finding_class_fails_validation(self):
        gate = 'if tfvars["missing"] or tfvars["foreign_file"]:'
        self.assertIn(gate, self.SOURCE)
        after = self.SOURCE.split(gate, 1)[1]
        self.assertIn('report["all_valid"] = False',
                      after.splitlines()[1])


@unittest.skipUnless(shutil.which("terraform"), "terraform is not on PATH")
class PlanPromptsForNothingTest(unittest.TestCase):
    """The Pass-3 proof, against a real terraform.

    No provider is required by any file here, so init needs no network and
    plan needs no credentials — the offline-validation principle holds.
    The negative control is the 2026-08-15 halt reproduced: the same clone
    without the printed file refuses to plan.
    """

    UNIT_TF = ('variable "project_id" {\n  type = string\n}\n'
               'output "id" {\n  value = var.project_id\n}\n')

    def _plan(self, clone: str) -> tuple:
        import subprocess
        terraform = shutil.which("terraform")
        env = dict(os.environ, TF_IN_AUTOMATION="1")
        subprocess.run([terraform, "init", "-backend=false", "-input=false"],
                       cwd=clone, capture_output=True, text=True, env=env)
        done = subprocess.run(
            [terraform, "plan", "-input=false", "-refresh=false", "-no-color"],
            cwd=clone, capture_output=True, text=True, env=env)
        return done.returncode, (done.stderr or "") + (done.stdout or "")

    def _clone(self, tmp: str) -> str:
        _write(tmp, "main.tf", 'locals {\n  landing_zone = "draft"\n}\n')
        _write(tmp, "translation-units/wi/main.tf", self.UNIT_TF)
        return tmp

    def test_without_the_printer_plan_halts_on_a_value_prompt(self):
        with tempfile.TemporaryDirectory() as tmp:
            clone = self._clone(tmp)
            root_wiring.write(clone, {"wi": "translation-units/wi"})
            code, output = self._plan(clone)
        self.assertNotEqual(code, 0)
        self.assertIn("No value for required variable", output)

    def test_with_the_printer_plan_prompts_for_nothing(self):
        values = {"project_id": {"value": "p-1", "source": "workspace"}}
        with tempfile.TemporaryDirectory() as tmp:
            clone = self._clone(tmp)
            root_wiring.write(clone, {"wi": "translation-units/wi"},
                              known_values=frozenset(values))
            result = tfvars_printer.emit(clone, values)
            code, output = self._plan(clone)
        self.assertEqual(result["missing"], [])
        self.assertEqual(code, 0, output)
        self.assertNotIn("No value for required variable", output)


if __name__ == "__main__":
    unittest.main()
