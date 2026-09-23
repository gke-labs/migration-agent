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

"""Unit tests for the generated root module that wires the units in.

The generator is pure text plus a directory listing. The last class is the
end-to-end proof and needs a real terraform on PATH: without the wiring the
root pass is vacuous (DESIGN §14 issue 14), with it a planted unit-level
error is caught.
"""

import os
import shutil
import tempfile
import unittest

from servers.phases.translation.translation_validate_3 import root_wiring, validation


def _write(root, rel, content):
    full = os.path.join(root, rel)
    os.makedirs(os.path.dirname(full), exist_ok=True)
    with open(full, "w", encoding="utf-8") as f:
        f.write(content)


def _read(root, rel):
    with open(os.path.join(root, rel), "r", encoding="utf-8") as f:
        return f.read()


class ModuleLabelTest(unittest.TestCase):

    def test_unit_ids_become_valid_hcl_identifiers(self):
        cases = {
            "storage": "unit-storage",
            "workload-identity": "unit-workload-identity",
            # Dots are legal in the materializer's directory slug and illegal
            # in an HCL identifier.
            "net.core": "unit-net-core",
            "2-networking": "unit-2-networking",
            "a b/c": "unit-a-b-c",
            "--": "unit",
            "": "unit",
        }
        for unit_id, expected in cases.items():
            with self.subTest(unit_id=unit_id):
                self.assertEqual(root_wiring.module_label(unit_id), expected)


class PlanWiringTest(unittest.TestCase):
    """Which materialized units get a module block, and which do not."""

    def plan(self, tree: dict, placed: dict,
             known_values: frozenset = frozenset()) -> dict:
        with tempfile.TemporaryDirectory() as clone:
            for rel, content in tree.items():
                _write(clone, rel, content)
            return root_wiring.plan_wiring(clone, placed, known_values)

    def test_terraform_units_are_wired_in_unit_id_order(self):
        plan = self.plan(
            {"translation-units/networking/main.tf": "# net\n",
             "translation-units/artifacts/main.tf": "# ar\n"},
            {"networking": "translation-units/networking",
             "artifacts": "translation-units/artifacts"})
        self.assertEqual(
            plan["modules"],
            [{"label": "unit-artifacts", "unit_id": "artifacts",
              "source": "./translation-units/artifacts", "args": []},
             {"label": "unit-networking", "unit_id": "networking",
              "source": "./translation-units/networking", "args": []}])
        self.assertEqual(plan["unwired"], [])

    def test_required_unit_variables_are_wired_as_pass_through_args(self):
        # TRANSLATE_RULES orders every unit to declare var.project_id-style
        # variables instead of inventing values, so a variable-free call
        # would fail the root pass on Missing required argument for every
        # contract-compliant unit. Required (default-less) variables become
        # `<name> = var.<name>` args; defaulted ones are the unit's own.
        plan = self.plan(
            {"translation-units/wi/variables.tf":
                 'variable "project_id" {\n  type = string\n}\n'
                 'variable "region" {\n  type    = string\n'
                 '  default = "us-central1"\n}\n',
             "translation-units/wi/main.tf": "# wi\n"},
            {"wi": "translation-units/wi"})
        self.assertEqual(plan["modules"][0]["args"], ["project_id"])
        # The clone root declares nothing, so the wiring must.
        self.assertEqual(plan["declared_vars"], ["project_id"])

    def test_root_declared_variables_are_not_redeclared(self):
        # A duplicate root declaration would fail the root pass outright.
        plan = self.plan(
            {"variables.tf": 'variable "project_id" {\n  type = string\n}\n',
             "translation-units/wi/main.tf":
                 'variable "project_id" {}\nvariable "network" {}\n'},
            {"wi": "translation-units/wi"})
        self.assertEqual(plan["modules"][0]["args"], ["network", "project_id"])
        self.assertEqual(plan["declared_vars"], ["network"])

    def test_a_defaulted_variable_with_a_recorded_value_is_wired(self):
        # The 2026-08-15 e2e's cluster_name case: the unit defaulted the
        # name to the SOURCE cluster's ("acme-prod"), the root never passed
        # the real one, and apply died on a cluster that does not exist.
        # A defaulted name the tfvars printer holds a recorded value for is
        # wired (and root-declared), so the printed value outvotes the
        # baked-in default; unknown defaulted names stay the unit's own.
        plan = self.plan(
            {"translation-units/np/variables.tf":
                 'variable "cluster_name" {\n  default = "acme-prod"\n}\n'
                 'variable "machine_type" {\n  default = "e2-standard-2"\n}\n'
                 'variable "project_id" {}\n',
             "translation-units/np/main.tf": "# np\n"},
            {"np": "translation-units/np"},
            known_values=frozenset({"cluster_name", "region"}))
        self.assertEqual(plan["modules"][0]["args"],
                         ["cluster_name", "project_id"])
        self.assertEqual(plan["declared_vars"], ["cluster_name", "project_id"])

    def test_a_defaulted_variable_matching_a_root_declaration_is_wired(self):
        plan = self.plan(
            {"variables.tf": 'variable "project_id" {\n  type = string\n}\n',
             "translation-units/wi/main.tf":
                 'variable "project_id" {\n  default = "wrong-project"\n}\n'},
            {"wi": "translation-units/wi"})
        self.assertEqual(plan["modules"][0]["args"], ["project_id"])
        self.assertEqual(plan["declared_vars"], [])

    def test_a_defaulted_meta_argument_name_is_left_alone(self):
        # It cannot be passed on a call; with a default nothing breaks when
        # it is not — unlike the required case, which is unsatisfiable.
        plan = self.plan(
            {"variables.tf": 'variable "count" {}\n',
             "translation-units/u/main.tf":
                 'variable "count" {\n  default = 1\n}\nvariable "ok" {}\n'},
            {"u": "translation-units/u"})
        self.assertEqual(plan["modules"][0]["args"], ["ok"])
        self.assertEqual(plan["unsatisfied"], [])

    def test_a_default_inside_a_nested_block_is_not_the_variables(self):
        # `default` can appear nested (an object type, a validation block);
        # only a top-level default makes the variable optional.
        plan = self.plan(
            {"translation-units/u/main.tf":
                 'variable "tricky" {\n'
                 '  validation {\n'
                 '    condition     = var.tricky != "default = no"\n'
                 '    error_message = "See default = docs."\n'
                 '  }\n'
                 '}\n'},
            {"u": "translation-units/u"})
        self.assertEqual(plan["modules"][0]["args"], ["tricky"])

    def test_a_meta_argument_named_variable_is_unsatisfied_not_wired(self):
        # `source = var.source` is not a thing a module call can say: the
        # name is a meta-argument. Wiring the call anyway would guarantee a
        # red root no worker can fix; the unit is a finding instead.
        plan = self.plan(
            {"translation-units/odd/main.tf":
                 'variable "count" {}\nvariable "ok" {}\n'},
            {"odd": "translation-units/odd"})
        self.assertEqual(plan["modules"], [])
        self.assertEqual(plan["unsatisfied"],
                         [{"unit_id": "odd", "variables": ["count"]}])
        self.assertEqual(plan["declared_vars"], [])

    def test_yaml_only_units_are_not_wired(self):
        # storage/gateway/tenancy ship manifests only: terraform refuses a
        # module directory with no configuration files, and the manifest gate
        # is what covers them.
        plan = self.plan(
            {"translation-units/gateway/gateway.yaml": "kind: Gateway\n",
             "translation-units/tenancy/ns.yml": "kind: Namespace\n"},
            {"gateway": "translation-units/gateway",
             "tenancy": "translation-units/tenancy"})
        self.assertEqual(plan["modules"], [])
        self.assertEqual(plan["unwired"], ["gateway", "tenancy"])

    def test_a_unit_with_terraform_only_in_a_subdirectory_is_a_finding(self):
        # `module { source = ... }` reads the named directory's own .tf files
        # only, and terraform validate in the unit's own directory succeeds
        # vacuously with nothing at its top level — so this unit's Terraform
        # would be compiled by NO pass. It is surfaced as nested_tf (the
        # validate tool fails the run on it), never as a quiet unwired entry.
        plan = self.plan(
            {"translation-units/mixed/modules/vpc/main.tf": "# nested\n",
             "translation-units/mixed/notes.yaml": "kind: ConfigMap\n"},
            {"mixed": "translation-units/mixed"})
        self.assertEqual(plan["modules"], [])
        self.assertEqual(plan["unwired"], [])
        self.assertEqual(plan["nested_tf"], ["mixed"])

    def test_init_droppings_do_not_make_a_yaml_unit_a_nested_finding(self):
        # .terraform/ holds downloaded module sources after `terraform init`,
        # not the unit's own code.
        plan = self.plan(
            {"translation-units/gw/gw.yaml": "kind: Gateway\n",
             "translation-units/gw/.terraform/modules/m/main.tf": "# cached\n"},
            {"gw": "translation-units/gw"})
        self.assertEqual(plan["nested_tf"], [])
        self.assertEqual(plan["unwired"], ["gw"])

    def test_a_missing_unit_directory_is_unwired_not_a_crash(self):
        plan = self.plan({}, {"absent": "translation-units/absent"})
        self.assertEqual(plan["modules"], [])
        self.assertEqual(plan["unwired"], ["absent"])

    def test_one_directory_shared_by_two_unit_ids_is_wired_once(self):
        # The materializer's slug is not injective ("a/b" and "a-b" both land
        # in translation-units/a-b); two module blocks on one source would be
        # a duplicate module call terraform refuses outright.
        plan = self.plan({"translation-units/a-b/main.tf": "# x\n"},
                         {"a/b": "translation-units/a-b",
                          "a-b": "translation-units/a-b"})
        self.assertEqual([m["unit_id"] for m in plan["modules"]], ["a-b"])
        self.assertEqual(plan["unwired"], ["a/b"])

    def test_colliding_labels_from_distinct_directories_are_numbered(self):
        plan = self.plan({"translation-units/a.b/main.tf": "# 1\n",
                          "translation-units/a-b/main.tf": "# 2\n"},
                         {"a.b": "translation-units/a.b",
                          "a-b": "translation-units/a-b"})
        self.assertEqual([m["label"] for m in plan["modules"]],
                         ["unit-a-b", "unit-a-b-2"])


class RenderTest(unittest.TestCase):

    def test_module_blocks_are_emitted_under_the_generated_header(self):
        text = root_wiring.render([
            {"label": "unit-networking", "unit_id": "networking",
             "source": "./translation-units/networking"}])
        self.assertIn("# Generated by GKE Agentic Migration", text)
        self.assertIn('module "unit-networking" {\n'
                      '  source = "./translation-units/networking"\n}\n', text)

    def test_nothing_to_wire_renders_nothing(self):
        self.assertEqual(root_wiring.render([]), "")

    def test_args_and_pass_through_declarations_are_rendered(self):
        text = root_wiring.render(
            [{"label": "unit-wi", "unit_id": "wi",
              "source": "./translation-units/wi",
              "args": ["project_id", "region"]}],
            ["project_id"])
        self.assertIn('variable "project_id" {', text)
        # Valueless on purpose: a default would be an invented value, and
        # `terraform validate` needs none.
        self.assertNotIn("default", text)
        self.assertIn('module "unit-wi" {\n'
                      '  source = "./translation-units/wi"\n'
                      '  project_id = var.project_id\n'
                      '  region = var.region\n}\n', text)


class WriteTest(unittest.TestCase):

    def test_the_file_is_written_and_rewritten_whole_and_stable(self):
        with tempfile.TemporaryDirectory() as clone:
            _write(clone, "translation-units/networking/main.tf", "# net\n")
            placed = {"networking": "translation-units/networking"}
            plan = root_wiring.write(clone, placed)
            first = _read(clone, root_wiring.WIRING_FILENAME)
            # A second materialization of the same units is byte-identical,
            # and a block a previous run left behind does not accumulate.
            _write(clone, root_wiring.WIRING_FILENAME,
                   first + '\nmodule "unit-stale" {\n  source = "./gone"\n}\n')
            root_wiring.write(clone, placed)
            second = _read(clone, root_wiring.WIRING_FILENAME)
        self.assertEqual(plan["file"], root_wiring.WIRING_FILENAME)
        self.assertEqual(first, second)
        self.assertNotIn("unit-stale", second)

    def test_no_terraform_bearing_unit_leaves_no_generated_file_behind(self):
        with tempfile.TemporaryDirectory() as clone:
            _write(clone, "translation-units/gateway/gw.yaml", "kind: Gateway\n")
            # A previous run wired a unit whose revision dropped its Terraform:
            # left in place the file would fail the root on a missing module.
            # The stale file carries the banner — it is the generator's own.
            _write(clone, root_wiring.WIRING_FILENAME, root_wiring.render(
                [{"label": "unit-old", "unit_id": "old",
                  "source": "./translation-units/old"}]))
            plan = root_wiring.write(clone, {"gateway": "translation-units/gateway"})
            exists = os.path.exists(os.path.join(clone, root_wiring.WIRING_FILENAME))
        self.assertIsNone(plan["file"])
        self.assertIsNone(plan["foreign_file"])
        self.assertFalse(exists)

    def test_a_foreign_file_at_the_wiring_name_is_never_overwritten(self):
        # The clone is the customer's repository: a translation-units.tf THEY
        # own (no generator banner) is a reviewer question, not a casualty.
        foreign = 'module "theirs" {\n  source = "./their-module"\n}\n'
        with tempfile.TemporaryDirectory() as clone:
            _write(clone, "translation-units/net/main.tf", "# net\n")
            _write(clone, root_wiring.WIRING_FILENAME, foreign)
            plan = root_wiring.write(clone, {"net": "translation-units/net"})
            on_disk = _read(clone, root_wiring.WIRING_FILENAME)
        self.assertEqual(on_disk, foreign)
        self.assertIsNone(plan["file"])
        self.assertEqual(plan["foreign_file"], root_wiring.WIRING_FILENAME)

    def test_a_foreign_file_is_not_deleted_in_the_zero_tf_case_either(self):
        foreign = "# customer-owned placeholder\n"
        with tempfile.TemporaryDirectory() as clone:
            _write(clone, "translation-units/gw/gw.yaml", "kind: Gateway\n")
            _write(clone, root_wiring.WIRING_FILENAME, foreign)
            plan = root_wiring.write(clone, {"gw": "translation-units/gw"})
            on_disk = _read(clone, root_wiring.WIRING_FILENAME)
        self.assertEqual(on_disk, foreign)
        self.assertIsNone(plan["file"])
        self.assertEqual(plan["foreign_file"], root_wiring.WIRING_FILENAME)


class DroppedModulesTest(unittest.TestCase):
    """The wiring file is one of the root's .tf files, so the auto-fix loop
    can rewrite it; a module block it deletes must not pass as clean."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.clone = self.tmp.name
        _write(self.clone, "translation-units/a/main.tf", "# a\n")
        _write(self.clone, "translation-units/b/main.tf", "# b\n")
        self.plan = root_wiring.write(
            self.clone, {"a": "translation-units/a", "b": "translation-units/b"})

    def tearDown(self):
        self.tmp.cleanup()

    def test_an_untouched_file_drops_nothing(self):
        self.assertEqual(root_wiring.dropped_modules(self.clone, self.plan), [])

    def test_a_deleted_module_block_is_named(self):
        text = _read(self.clone, root_wiring.WIRING_FILENAME)
        _write(self.clone, root_wiring.WIRING_FILENAME,
               text.replace('module "unit-b" {\n  source = "./translation-units/b"\n}',
                            ""))
        self.assertEqual(root_wiring.dropped_modules(self.clone, self.plan),
                         ["unit-b"])

    def test_a_deleted_file_drops_every_module(self):
        os.remove(os.path.join(self.clone, root_wiring.WIRING_FILENAME))
        self.assertEqual(root_wiring.dropped_modules(self.clone, self.plan),
                         ["unit-a", "unit-b"])

    def test_a_repointed_source_is_a_dropped_module(self):
        # Deleting the block is not the only escape: repointing its source at
        # a sibling unit keeps `module "unit-b"` in the file while the unit's
        # own code is again compiled by nothing.
        text = _read(self.clone, root_wiring.WIRING_FILENAME)
        _write(self.clone, root_wiring.WIRING_FILENAME,
               text.replace('source = "./translation-units/b"',
                            'source = "./translation-units/a"'))
        self.assertEqual(root_wiring.dropped_modules(self.clone, self.plan),
                         ["unit-b"])

    def test_an_extra_invented_argument_is_a_dropped_module(self):
        # The wired args are derived from the unit's own declared variables:
        # a worker that edits them from the root is answering an error whose
        # code it cannot see, so any alteration of the block is a finding.
        text = _read(self.clone, root_wiring.WIRING_FILENAME)
        _write(self.clone, root_wiring.WIRING_FILENAME,
               text.replace('module "unit-a" {\n',
                            'module "unit-a" {\n  gsa_email = "invented@x"\n'))
        self.assertEqual(root_wiring.dropped_modules(self.clone, self.plan),
                         ["unit-a"])

    def test_nothing_wired_means_nothing_to_drop(self):
        self.assertEqual(
            root_wiring.dropped_modules(self.clone, {"modules": [], "file": None}), [])


@unittest.skipUnless(shutil.which("terraform"), "terraform is not on PATH")
class RootValidateReachesWiredUnitsTest(unittest.TestCase):
    """The issue-14 proof, against a real terraform.

    No provider is required by any file here, so `terraform init
    -backend=false` needs no network and no credentials.
    """

    LZ_ROOT = 'locals {\n  landing_zone = "draft"\n}\n'
    GOOD_UNIT = 'output "ok" {\n  value = "ok"\n}\n'
    BROKEN_UNIT = 'output "broken" {\n  value = var.never_declared\n}\n'

    def test_wiring_is_what_makes_the_root_pass_see_unit_code(self):
        terraform = shutil.which("terraform")
        with tempfile.TemporaryDirectory() as clone:
            _write(clone, "main.tf", self.LZ_ROOT)
            _write(clone, "translation-units/good/main.tf", self.GOOD_UNIT)
            _write(clone, "translation-units/broken/main.tf", self.BROKEN_UNIT)
            placed = {"good": "translation-units/good",
                      "broken": "translation-units/broken"}
            # Issue 14 reproduced: unreferenced modules are never compiled, so
            # the root pass reports success over code it never read.
            self.assertEqual(validation.terraform_validate(terraform, clone), "")
            root_wiring.write(clone, placed)
            error = validation.terraform_validate(terraform, clone)
        self.assertIn("never_declared", error)

    def test_a_wired_tree_that_is_sound_passes(self):
        terraform = shutil.which("terraform")
        with tempfile.TemporaryDirectory() as clone:
            _write(clone, "main.tf", self.LZ_ROOT)
            _write(clone, "translation-units/good/main.tf", self.GOOD_UNIT)
            _write(clone, "translation-units/gateway/gw.yaml", "kind: Gateway\n")
            root_wiring.write(clone, {"good": "translation-units/good",
                                      "gateway": "translation-units/gateway"})
            error = validation.terraform_validate(terraform, clone)
        self.assertEqual(error, "")

    def test_a_contract_compliant_unit_with_required_variables_wires_green(self):
        # The worker contract orders units to declare variables rather than
        # invent values, so this shape is the NORMAL case: the wired call
        # passes the variable through a generated valueless root declaration
        # and the root pass validates without any value being supplied.
        unit_tf = ('variable "project_id" {\n  type = string\n}\n'
                   'output "id" {\n  value = var.project_id\n}\n')
        terraform = shutil.which("terraform")
        with tempfile.TemporaryDirectory() as clone:
            _write(clone, "main.tf", self.LZ_ROOT)
            _write(clone, "translation-units/wi/main.tf", unit_tf)
            plan = root_wiring.write(clone, {"wi": "translation-units/wi"})
            error = validation.terraform_validate(terraform, clone)
            wiring_text = _read(clone, root_wiring.WIRING_FILENAME)
        self.assertEqual(error, "")
        self.assertEqual(plan["declared_vars"], ["project_id"])
        self.assertIn("project_id = var.project_id", wiring_text)

    def test_a_root_declared_variable_is_passed_without_redeclaration(self):
        # When the landing-zone draft already declares the name, the wiring
        # must reference it, not redeclare it — a duplicate declaration
        # would fail the root pass on its own.
        root_tf = self.LZ_ROOT + '\nvariable "project_id" {\n  type = string\n}\n'
        unit_tf = 'variable "project_id" {}\noutput "p" {\n  value = var.project_id\n}\n'
        terraform = shutil.which("terraform")
        with tempfile.TemporaryDirectory() as clone:
            _write(clone, "main.tf", root_tf)
            _write(clone, "translation-units/wi/main.tf", unit_tf)
            plan = root_wiring.write(clone, {"wi": "translation-units/wi"})
            error = validation.terraform_validate(terraform, clone)
        self.assertEqual(error, "")
        self.assertEqual(plan["declared_vars"], [])


if __name__ == "__main__":
    unittest.main()
