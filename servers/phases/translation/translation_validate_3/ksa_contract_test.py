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

"""Unit tests for the ksa_annotations output contract parser. Pure, no GCS."""

import unittest

from servers.phases.translation.translation_validate_3 import ksa_contract

VALID_TF = """\
resource "google_service_account" "orders" {
  account_id = "orders"
}

output "ksa_annotations" {
  description = "namespace/ksa -> GSA email"
  value = {
    "acme-shop/orders" = "orders@my-project.iam.gserviceaccount.com",
    "acme-shop/frontend" = "frontend@my-project.iam.gserviceaccount.com"
  }
}
"""


def parse(content, path="main.tf"):
    return ksa_contract.parse_ksa_annotations({path: content})


class ParseTest(unittest.TestCase):

    def test_a_valid_literal_map_parses_to_bindings(self):
        result = parse(VALID_TF)
        self.assertEqual(result["status"], "ok")
        self.assertEqual(result["bindings"], {
            "acme-shop/orders": "orders@my-project.iam.gserviceaccount.com",
            "acme-shop/frontend": "frontend@my-project.iam.gserviceaccount.com",
        })
        self.assertEqual(result["file"], "main.tf")

    EMAIL = "orders-sa@acme-prod.iam.gserviceaccount.com"

    def test_comments_and_blank_lines_inside_the_map_are_tolerated(self):
        result = parse(
            'output "ksa_annotations" {\n  value = {\n'
            '    # the orders service\n\n'
            f'    "ns/sa" = "{self.EMAIL}"  # trailing comment\n  }}\n}}\n')
        self.assertEqual(result["status"], "ok")
        self.assertEqual(result["bindings"], {"ns/sa": self.EMAIL})

    def test_an_entry_wrapped_across_lines_is_still_literal(self):
        result = parse(
            'output "ksa_annotations" {\n  value = {\n'
            f'    "ns/sa" =\n      "{self.EMAIL}"\n  }}\n}}\n')
        self.assertEqual(result["status"], "ok")
        self.assertEqual(result["bindings"], {"ns/sa": self.EMAIL})

    def test_a_commented_out_draft_block_is_not_a_second_declaration(self):
        content = ('# output "ksa_annotations" { value = { "old/sa" = "x" } }\n'
                   + VALID_TF)
        result = ksa_contract.parse_ksa_annotations({"main.tf": content})
        self.assertEqual(result["status"], "ok", result["error"])

    def test_a_heredoc_quoting_the_block_is_not_a_declaration(self):
        content = ('resource "local_file" "doc" {\n  content = <<-EOT\n'
                   '    output "ksa_annotations" { value = { } }\n'
                   '  EOT\n}\n' + VALID_TF)
        result = ksa_contract.parse_ksa_annotations({"main.tf": content})
        self.assertEqual(result["status"], "ok", result["error"])

    def test_a_heredoc_marker_inside_a_string_opens_no_phantom_heredoc(self):
        # "<<manifest" inside a quoted string is prose, not a heredoc; the
        # round-2 review showed it blanking the rest of the file and
        # reporting the real output as absent.
        content = ('locals {\n  banner = "run: kubectl apply <<manifest"\n}\n'
                   + VALID_TF)
        result = ksa_contract.parse_ksa_annotations({"main.tf": content})
        self.assertEqual(result["status"], "ok", result["error"])

    def test_a_string_heredoc_marker_after_the_block_hides_nothing(self):
        # Mirror case: phantom heredoc mode after the block would blank a
        # genuine duplicate declaration below it.
        content = (VALID_TF
                   + '\nlocals {\n  note = "pipe via <<eof"\n}\n'
                   + VALID_TF)
        result = ksa_contract.parse_ksa_annotations({"main.tf": content})
        self.assertEqual(result["status"], "malformed")
        self.assertIn("declared 2 times", result["error"])

    def test_a_lone_brace_inside_a_map_comment_does_not_derail_the_scan(self):
        result = parse(
            'output "ksa_annotations" {\n  value = {\n'
            '    # entries look like { this\n'
            f'    "ns/sa" = "{self.EMAIL}"\n  }}\n}}\n')
        self.assertEqual(result["status"], "ok", result["error"])

    def test_a_description_mentioning_value_is_not_the_value_attribute(self):
        result = parse(
            'output "ksa_annotations" {\n'
            '  description = "value = map of ns/sa to email"\n'
            f'  value = {{\n    "ns/sa" = "{self.EMAIL}"\n  }}\n}}\n')
        self.assertEqual(result["status"], "ok", result["error"])

    def test_a_placeholder_project_is_rejected_as_not_a_real_email(self):
        # Shipped live in the 2026-08-14 e2e run: literal, email-shaped, and
        # useless — a consumer would inject PROJECT_ID into manifests.
        result = parse(
            'output "ksa_annotations" {\n  value = {\n'
            '    "ns/sa" = "orders-sa@PROJECT_ID.iam.gserviceaccount.com"\n  }\n}\n')
        self.assertEqual(result["status"], "malformed")
        self.assertIn("placeholder", result["error"])

    def test_a_duplicate_map_key_is_malformed(self):
        result = parse(
            'output "ksa_annotations" {\n  value = {\n'
            f'    "ns/sa" = "{self.EMAIL}"\n'
            f'    "ns/sa" = "{self.EMAIL}"\n  }}\n}}\n')
        self.assertEqual(result["status"], "malformed")
        self.assertIn("twice", result["error"])

    def test_an_empty_map_is_shape_valid_with_no_bindings(self):
        # Shape-level truth only: the PARSER accepts the empty map (the
        # exports derivation needs the ok/{} distinction). Whether it may
        # SHIP is check_units' call — over recorded IRSA bindings it is a
        # finding, pinned in CheckUnitsTest below.
        result = parse('output "ksa_annotations" {\n  value = {}\n}\n')
        self.assertEqual(result["status"], "ok")
        self.assertEqual(result["bindings"], {})

    def test_no_output_block_is_absent_not_an_error(self):
        result = parse('output "something_else" {\n  value = {}\n}\n')
        self.assertEqual(result["status"], "absent")
        self.assertIsNone(result["bindings"])

    def test_no_tf_files_at_all_is_absent(self):
        result = ksa_contract.parse_ksa_annotations({})
        self.assertEqual(result["status"], "absent")

    def test_a_resource_reference_value_is_malformed(self):
        result = parse(
            'output "ksa_annotations" {\n  value = {\n'
            '    "ns/sa" = google_service_account.orders.email\n  }\n}\n')
        self.assertEqual(result["status"], "malformed")
        self.assertIn("not a literal", result["error"])

    def test_interpolation_is_malformed(self):
        result = parse(
            'output "ksa_annotations" {\n  value = {\n'
            '    "ns/sa" = "${google_service_account.orders.email}"\n  }\n}\n')
        self.assertEqual(result["status"], "malformed")
        self.assertIn("interpolation", result["error"])

    def test_a_for_expression_is_malformed(self):
        result = parse(
            'output "ksa_annotations" {\n  value = '
            '{ for b in var.bindings : b.key => b.email }\n}\n')
        self.assertEqual(result["status"], "malformed")

    def test_a_non_map_value_is_malformed(self):
        result = parse('output "ksa_annotations" {\n  value = jsonencode({})\n}\n')
        self.assertEqual(result["status"], "malformed")
        self.assertIn("not a literal map", result["error"])

    def test_a_missing_value_attribute_is_malformed(self):
        result = parse('output "ksa_annotations" {\n  description = "d"\n}\n')
        self.assertEqual(result["status"], "malformed")
        self.assertIn("no value attribute", result["error"])

    def test_a_key_without_namespace_and_name_is_malformed(self):
        result = parse(
            'output "ksa_annotations" {\n  value = {\n'
            '    "orders" = "orders-sa@acme-prod.iam.gserviceaccount.com"\n  }\n}\n')
        self.assertEqual(result["status"], "malformed")
        self.assertIn("namespace/ksa-name", result["error"])

    def test_a_non_gsa_email_value_is_malformed(self):
        result = parse(
            'output "ksa_annotations" {\n  value = {\n'
            '    "ns/sa" = "arn:aws:iam::123:role/orders"\n  }\n}\n')
        self.assertEqual(result["status"], "malformed")
        self.assertIn("gserviceaccount.com", result["error"])

    def test_a_duplicate_declaration_is_malformed(self):
        files = {"a.tf": VALID_TF, "b.tf": VALID_TF}
        result = ksa_contract.parse_ksa_annotations(files)
        self.assertEqual(result["status"], "malformed")
        self.assertIn("declared 2 times", result["error"])

    def test_a_brace_inside_a_string_does_not_end_the_block(self):
        result = parse(
            'output "ksa_annotations" {\n  description = "maps {ns/sa}"\n'
            '  value = {\n    "ns/sa" = "orders-sa@acme-prod.iam.gserviceaccount.com"\n  }\n}\n')
        self.assertEqual(result["status"], "ok")


def wi_unit(files, unit_id="workload-identity"):
    return {"unit": {"unit_id": unit_id, "kind": "workload-identity"},
            "result": {"files": files}}


class CheckUnitsTest(unittest.TestCase):

    def test_only_workload_identity_units_are_checked(self):
        report = ksa_contract.check_units([
            {"unit": {"unit_id": "storage", "kind": "storage"},
             "result": {"files": [{"path": "main.tf", "content": ""}]}},
        ])
        self.assertEqual(report["checked"], [])
        self.assertEqual(report["findings"], [])

    def test_a_valid_unit_produces_no_findings(self):
        report = ksa_contract.check_units(
            [wi_unit([{"path": "main.tf", "content": VALID_TF}])])
        self.assertEqual(report["checked"], ["workload-identity"])
        self.assertEqual(report["findings"], [])

    def test_a_missing_output_is_a_finding_naming_the_legacy_case(self):
        report = ksa_contract.check_units(
            [wi_unit([{"path": "main.tf", "content": "# no outputs\n"}])])
        self.assertEqual(len(report["findings"]), 1)
        self.assertIn("legacy unit", report["findings"][0]["error"])

    def test_a_legacy_blob_without_result_is_reported_not_crashed_on(self):
        report = ksa_contract.check_units(
            [{"unit": {"unit_id": "wi-1", "kind": "workload-identity"}}])
        self.assertEqual(report["checked"], ["wi-1"])
        self.assertEqual(len(report["findings"]), 1)
        self.assertIn('no output "ksa_annotations"', report["findings"][0]["error"])

    def test_a_malformed_output_carries_the_parser_error(self):
        content = 'output "ksa_annotations" {\n  value = var.bindings\n}\n'
        report = ksa_contract.check_units(
            [wi_unit([{"path": "main.tf", "content": content}])])
        self.assertEqual(len(report["findings"]), 1)
        self.assertIn("not a literal map", report["findings"][0]["error"])

    EMPTY_MAP_TF = 'output "ksa_annotations" {\n  value = {}\n}\n'

    def test_an_empty_map_over_recorded_irsa_bindings_is_a_finding(self):
        # The undecided-project escape must reach review loudly: shipped
        # silently, every recorded ServiceAccount lands with no Google
        # identity and exports gsa_bindings publishes null.
        unit = wi_unit([{"path": "main.tf", "content": self.EMPTY_MAP_TF}])
        unit["unit"]["inputs"] = {"irsa_bindings": ["acme-shop/orders"]}
        report = ksa_contract.check_units([unit])
        self.assertEqual(len(report["findings"]), 1)
        self.assertIn("empty map", report["findings"][0]["error"])
        self.assertIn("no target project was supplied",
                      report["findings"][0]["error"])

    def test_an_empty_map_despite_a_recorded_target_project_names_it(self):
        # The other half: the project WAS supplied and the worker still
        # took the escape — review fixes the unit, not the pipeline.
        unit = wi_unit([{"path": "main.tf", "content": self.EMPTY_MAP_TF}])
        unit["unit"]["inputs"] = {"irsa_bindings": ["acme-shop/orders"],
                                  "target_project": "acme-prod"}
        report = ksa_contract.check_units([unit])
        self.assertEqual(len(report["findings"]), 1)
        self.assertIn("'acme-prod'", report["findings"][0]["error"])
        self.assertIn("genuinely undecidable", report["findings"][0]["error"])

    def test_an_empty_map_with_no_recorded_bindings_is_not_a_finding(self):
        # No bindings recorded means there is nothing to bind: the empty
        # map is the honest complete answer, not an escape.
        unit = wi_unit([{"path": "main.tf", "content": self.EMPTY_MAP_TF}])
        unit["unit"]["inputs"] = {"irsa_bindings": []}
        report = ksa_contract.check_units([unit])
        self.assertEqual(report["findings"], [])


class KsaResourceBanTest(unittest.TestCase):
    """The shed of DESIGN §14 issue 18 is machine-checked, not brief-only."""

    def test_a_serviceaccount_yaml_document_is_a_finding(self):
        doc = ("apiVersion: v1\nkind: ServiceAccount\nmetadata:\n"
               "  name: orders\n")
        report = ksa_contract.check_units([wi_unit(
            [{"path": "main.tf", "content": VALID_TF},
             {"path": "ksa.yaml", "content": doc}])])
        self.assertEqual(len(report["findings"]), 1)
        self.assertIn("ksa.yaml", report["findings"][0]["error"])
        self.assertIn("wkld-identity", report["findings"][0]["error"])

    def test_a_kubernetes_service_account_resource_is_a_finding(self):
        tf = VALID_TF + ('\nresource "kubernetes_service_account_v1" "ksa" {\n'
                         '  metadata { name = "orders" }\n}\n')
        report = ksa_contract.check_units(
            [wi_unit([{"path": "main.tf", "content": tf}])])
        self.assertEqual(len(report["findings"]), 1)
        self.assertIn("kubernetes_service_account_v1",
                      report["findings"][0]["error"])

    def test_a_kubernetes_manifest_serviceaccount_is_a_finding(self):
        tf = VALID_TF + ('\nresource "kubernetes_manifest" "ksa" {\n'
                         '  manifest = {\n    apiVersion = "v1"\n'
                         '    kind = "ServiceAccount"\n  }\n}\n')
        report = ksa_contract.check_units(
            [wi_unit([{"path": "main.tf", "content": tf}])])
        self.assertEqual(len(report["findings"]), 1)
        self.assertIn("kubernetes_manifest", report["findings"][0]["error"])

    def test_a_commented_out_draft_is_not_a_finding(self):
        tf = VALID_TF + ('\n# resource "kubernetes_service_account" "ksa" {\n'
                         '#   metadata { name = "orders" }\n# }\n')
        report = ksa_contract.check_units(
            [wi_unit([{"path": "main.tf", "content": tf}])])
        self.assertEqual(report["findings"], [])

    def test_another_kind_of_yaml_document_is_not_a_finding(self):
        doc = "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: c\n"
        report = ksa_contract.check_units([wi_unit(
            [{"path": "main.tf", "content": VALID_TF},
             {"path": "cm.yaml", "content": doc}])])
        self.assertEqual(report["findings"], [])


if __name__ == "__main__":
    unittest.main()
