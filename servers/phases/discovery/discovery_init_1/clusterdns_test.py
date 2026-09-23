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

"""Unit tests for the cluster DNS harvest. No GCS, no LLM.

The harvest copies; these tests hold it to that. Every assertion on `text`
is byte-for-byte, and the one thing the section must never carry is an
interpreted field.
"""

import os
import tempfile
import unittest

from servers.phases.discovery.discovery_init_1 import clusterdns

COREFILE = (
    ".:53 {\n"
    "    errors\n"
    "    health\n"
    "    kubernetes cluster.local in-addr.arpa ip6.arpa {\n"
    "      pods insecure\n"
    "      fallthrough in-addr.arpa ip6.arpa\n"
    "    }\n"
    "    forward . 10.0.0.2 10.0.0.3\n"
    "    cache 30\n"
    "}\n"
    "corp.example.com:53 {\n"
    "    forward . 10.1.2.3 10.1.2.4\n"
    "}\n")


def _write(root, rel_path, content):
    full = os.path.join(root, rel_path)
    os.makedirs(os.path.dirname(full), exist_ok=True)
    with open(full, "w") as f:
        f.write(content)


def _harvest(files: dict, scope: dict = None, chart_roots=()) -> tuple:
    with tempfile.TemporaryDirectory() as root:
        for rel_path, content in files.items():
            _write(root, rel_path, content)
        result = clusterdns.harvest_cluster_dns(root, scope, chart_roots)
        return result.section, result.notes


def _sources(files: dict, **kw) -> list:
    section, _ = _harvest(files, **kw)
    return section.get("sources", [])


class AddonTest(unittest.TestCase):
    """The managed add-on's configuration_values, in each form it is written."""

    def test_heredoc_configuration_is_copied_byte_for_byte(self):
        body = '{\n  "corefile": ' + repr(COREFILE) + ',\n  "replicaCount": 3\n}\n'
        tf = ('resource "aws_eks_addon" "coredns" {\n'
              '  cluster_name  = aws_eks_cluster.main.name\n'
              '  addon_name    = "coredns"\n'
              '  addon_version = "v1.11.1-eksbuild.4"\n'
              '  resolve_conflicts_on_update = "PRESERVE"\n'
              '  configuration_values = <<EOT\n' + body + 'EOT\n'
              '}\n')
        [source] = _sources({"envs/prod/eks.tf": tf})
        self.assertEqual(source["kind"], "eks_addon")
        self.assertEqual(source["name"], "coredns")
        self.assertEqual(source["address"], "aws_eks_addon.coredns")
        self.assertEqual(source["directory"], "envs/prod")
        self.assertEqual(source["form"], "heredoc")
        self.assertEqual(source["addon_version"], "v1.11.1-eksbuild.4")
        self.assertEqual(source["resolve_conflicts_on_update"], "PRESERVE")
        self.assertEqual(source["text"], body)
        self.assertEqual(source["evidence"], ["envs/prod/eks.tf:1"])

    def test_indented_heredoc_is_dedented_as_terraform_does(self):
        tf = ('resource "aws_eks_addon" "coredns" {\n'
              '  addon_name = "coredns"\n'
              '  configuration_values = <<-EOT\n'
              '    {\n'
              '      "corefile": ".:53 { forward . 10.0.0.2 }"\n'
              '    }\n'
              '  EOT\n'
              '}\n')
        [source] = _sources({"eks.tf": tf})
        self.assertEqual(source["text"],
                         '{\n  "corefile": ".:53 { forward . 10.0.0.2 }"\n}\n')

    def test_quoted_string_is_unescaped(self):
        tf = ('resource "aws_eks_addon" "coredns" {\n'
              '  addon_name = "coredns"\n'
              '  configuration_values = "{\\"corefile\\": \\".:53 {\\\\n forward . 10.0.0.2 }\\"}"\n'
              '}\n')
        [source] = _sources({"eks.tf": tf})
        self.assertEqual(source["form"], "string")
        self.assertEqual(source["text"],
                         '{"corefile": ".:53 {\\n forward . 10.0.0.2 }"}')

    def test_jsonencode_object_is_recorded_as_raw_text(self):
        tf = ('resource "aws_eks_addon" "coredns" {\n'
              '  addon_name = "coredns"\n'
              '  configuration_values = jsonencode({\n'
              '    corefile = <<-EOF\n'
              '      .:53 {\n'
              '        forward . 10.0.0.2\n'
              '      }\n'
              '    EOF\n'
              '    replicaCount = 2\n'
              '  })\n'
              '}\n')
        [source] = _sources({"eks.tf": tf})
        self.assertEqual(source["form"], "expression")
        self.assertTrue(source["text"].startswith("jsonencode({"))
        self.assertTrue(source["text"].endswith("})"))
        self.assertIn("forward . 10.0.0.2", source["text"])
        self.assertIn("replicaCount = 2", source["text"])

    def test_variable_reference_is_unread_and_noted(self):
        tf = ('resource "aws_eks_addon" "coredns" {\n'
              '  addon_name = "coredns"\n'
              '  configuration_values = var.coredns_config\n'
              '}\n')
        section, notes = _harvest({"eks.tf": tf})
        self.assertEqual(section, {})
        self.assertTrue(any("var.coredns_config" in n and "not recorded" in n
                            for n in notes), notes)

    def test_file_call_is_unread_and_names_the_file(self):
        tf = ('resource "aws_eks_addon" "coredns" {\n'
              '  addon_name = "coredns"\n'
              '  configuration_values = file("${path.module}/coredns.json")\n'
              '}\n')
        section, notes = _harvest({"eks.tf": tf})
        self.assertEqual(section, {})
        self.assertTrue(any("coredns.json" in n for n in notes), notes)

    def test_default_addon_is_a_note_not_a_source(self):
        tf = ('resource "aws_eks_addon" "coredns" {\n'
              '  cluster_name = aws_eks_cluster.main.name\n'
              '  addon_name   = "coredns"\n'
              '}\n')
        section, notes = _harvest({"eks.tf": tf})
        self.assertEqual(section, {})
        self.assertTrue(any("default configuration" in n for n in notes), notes)

    def test_empty_configuration_is_a_default(self):
        for empty in ('"{}"', '""', 'jsonencode({})'):
            with self.subTest(empty=empty):
                tf = ('resource "aws_eks_addon" "coredns" {\n  addon_name = "coredns"\n'
                      f'  configuration_values = {empty}\n}}\n')
                section, notes = _harvest({"eks.tf": tf})
                self.assertEqual(section, {})
                self.assertTrue(any("configuration_values is empty" in n for n in notes), notes)

    def test_unicode_escape_and_dollar_escape_decode(self):
        tf = ('resource "aws_eks_addon" "coredns" {\n  addon_name = "coredns"\n'
              '  configuration_values = "{\\"corefile\\": \\"\\u002e:53 { log $${x} }\\"}"\n}\n')
        [source] = _sources({"eks.tf": tf})
        self.assertEqual(source["text"], '{"corefile": ".:53 { log ${x} }"}')

    def test_single_line_map_with_coredns_second(self):
        tf = ('module "eks" {\n  source = "terraform-aws-modules/eks/aws"\n'
              '  cluster_addons = { kube-proxy = {}, coredns = { configuration_values = "{\\"replicaCount\\": 3}" } }\n}\n')
        [source] = _sources({"eks.tf": tf})
        self.assertEqual(source["kind"], "eks_module_addon")
        self.assertEqual(source["text"], '{"replicaCount": 3}')

    def test_kubernetes_manifest_from_a_file_is_noted(self):
        tf = ('resource "kubernetes_manifest" "coredns" {\n'
              '  manifest = yamldecode(templatefile("${path.module}/coredns.yaml.tftpl", {}))\n}\n')
        section, notes = _harvest({"k8s.tf": tf})
        self.assertEqual(section, {})
        self.assertTrue(any("coredns.yaml.tftpl" in n and "not recorded" in n for n in notes), notes)

    def test_kubectl_manifest_interpolation_is_noted(self):
        tf = ('resource "kubectl_manifest" "coredns" {\n'
              '  yaml_body = <<YAML\n'
              'apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: coredns\n'
              'data:\n  Corefile: ".:53 { forward . ${var.upstream} }"\n'
              'YAML\n}\n')
        section, notes = _harvest({"k8s.tf": tf})
        self.assertEqual(len(section["sources"]), 1)
        self.assertTrue(any("also references ${var.upstream}" in n for n in notes), notes)

    def test_wrapper_keeps_nested_escapes_as_written(self):
        tf = ('resource "aws_eks_addon" "coredns" {\n  addon_name = "coredns"\n'
              '  configuration_values = "${jsonencode({ corefile = "a\\nb" })}"\n}\n')
        [source] = _sources({"eks.tf": tf})
        self.assertEqual(source["text"], 'jsonencode({ corefile = "a\\nb" })')

    def test_non_string_addon_fields_become_null(self):
        tf = ('resource "aws_eks_addon" "coredns" {\n  addon_name = "coredns"\n'
              '  addon_version = 1\n'
              '  configuration_values = "{\\"replicaCount\\": 2}"\n}\n')
        [source] = _sources({"eks.tf": tf})
        self.assertIsNone(source["addon_version"])

    def test_other_addons_are_ignored(self):
        tf = ('resource "aws_eks_addon" "proxy" {\n'
              '  addon_name = "kube-proxy"\n'
              '  configuration_values = "{}"\n'
              '}\n')
        section, notes = _harvest({"eks.tf": tf})
        self.assertEqual(section, {})
        self.assertFalse(any("kube-proxy" in n for n in notes), notes)

    def test_for_each_addon_with_configuration_is_noted(self):
        tf = ('resource "aws_eks_addon" "all" {\n'
              '  for_each = var.addons\n'
              '  addon_name = each.key\n'
              '  configuration_values = each.value\n'
              '}\n')
        section, notes = _harvest({"eks.tf": tf})
        self.assertEqual(section, {})
        self.assertTrue(any("addon_name is not a literal" in n for n in notes), notes)


class TerraformConfigMapTest(unittest.TestCase):

    def test_corefile_in_a_nested_data_map(self):
        tf = ('resource "kubernetes_config_map" "coredns" {\n'
              '  metadata {\n'
              '    name      = "coredns"\n'
              '    namespace = "kube-system"\n'
              '    labels = { name = "not-the-name" }\n'
              '  }\n'
              '  data = {\n'
              '    Corefile = <<EOF\n' + COREFILE + 'EOF\n'
              '  }\n'
              '}\n')
        [source] = _sources({"k8s/coredns.tf": tf})
        self.assertEqual(source["kind"], "kubernetes_config_map")
        self.assertEqual(source["name"], "coredns")
        self.assertEqual(source["address"], "kubernetes_config_map.coredns")
        self.assertEqual(source["text"], COREFILE)

    def test_quoted_corefile_key_and_v1_resource(self):
        tf = ('resource "kubernetes_config_map_v1" "dns" {\n'
              '  metadata {\n'
              '    name = "node-local-dns"\n'
              '  }\n'
              '  data = {\n'
              '    "Corefile" = ".:53 { forward . 10.0.0.2 }"\n'
              '  }\n'
              '}\n')
        [source] = _sources({"dns.tf": tf})
        self.assertEqual(source["name"], "node-local-dns")
        self.assertEqual(source["text"], ".:53 { forward . 10.0.0.2 }")

    def test_kubectl_manifest_yaml_body_is_a_source(self):
        tf = ('resource "kubectl_manifest" "coredns" {\n'
              '  yaml_body = <<YAML\n'
              'apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: coredns\n'
              '  namespace: kube-system\ndata:\n  Corefile: |\n'
              + "".join("    " + line + "\n" for line in COREFILE.splitlines())
              + 'YAML\n}\n')
        [source] = _sources({"k8s.tf": tf})
        self.assertEqual(source["kind"], "configmap")
        self.assertEqual(source["address"], "kubectl_manifest.coredns")
        self.assertIsNone(source["path"])
        self.assertEqual(source["text"], COREFILE)
        self.assertEqual(source["evidence"], ["k8s.tf:1"])

    def test_kubectl_manifest_from_a_file_is_noted_when_it_mentions_coredns(self):
        tf = ('resource "kubectl_manifest" "coredns" {\n'
              '  yaml_body = file("${path.module}/coredns.yaml")\n}\n')
        section, notes = _harvest({"k8s.tf": tf})
        self.assertEqual(section, {})
        self.assertTrue(any("coredns.yaml" in n and "not recorded" in n for n in notes), notes)

    def test_kubernetes_manifest_inline_configmap_is_a_source(self):
        tf = ('resource "kubernetes_manifest" "coredns" {\n'
              '  manifest = {\n'
              '    apiVersion = "v1"\n'
              '    kind       = "ConfigMap"\n'
              '    metadata = {\n      name      = "coredns"\n      namespace = "kube-system"\n    }\n'
              '    data = {\n      Corefile = <<EOF\n' + COREFILE + 'EOF\n    }\n'
              '  }\n}\n')
        [source] = _sources({"k8s.tf": tf})
        self.assertEqual(source["kind"], "kubernetes_config_map")
        self.assertEqual(source["address"], "kubernetes_manifest.coredns")
        self.assertEqual(source["text"], COREFILE)

    def test_kubernetes_manifest_of_another_kind_is_ignored(self):
        tf = ('resource "kubernetes_manifest" "svc" {\n'
              '  manifest = {\n    kind = "Service"\n'
              '    metadata = { name = "coredns", namespace = "kube-system" }\n  }\n}\n')
        section, notes = _harvest({"k8s.tf": tf})
        self.assertEqual(section, {})

    def test_non_literal_namespace_is_recorded_with_a_note(self):
        tf = ('resource "kubernetes_config_map" "coredns" {\n'
              '  metadata {\n    name      = "coredns"\n'
              '    namespace = kubernetes_namespace.lab.metadata[0].name\n  }\n'
              '  data = { Corefile = "x" }\n}\n')
        section, notes = _harvest({"dns.tf": tf})
        self.assertEqual(len(section["sources"]), 1)
        self.assertTrue(any("namespace is not a literal" in n for n in notes), notes)

    def test_single_line_data_map_with_corefile_second(self):
        tf = ('resource "kubernetes_config_map" "coredns" {\n'
              '  metadata {\n    name = "coredns"\n  }\n'
              '  data = { other = "y", Corefile = "x.:53 { }" }\n}\n')
        [source] = _sources({"dns.tf": tf})
        self.assertEqual(source["text"], "x.:53 { }")

    def test_kubernetes_manifest_non_literal_namespace_is_noted(self):
        tf = ('resource "kubernetes_manifest" "coredns" {\n'
              '  manifest = {\n    kind = "ConfigMap"\n'
              '    metadata = { name = "coredns", namespace = var.ns }\n'
              '    data = { Corefile = "x" }\n  }\n}\n')
        section, notes = _harvest({"k8s.tf": tf})
        self.assertEqual(len(section["sources"]), 1)
        self.assertTrue(any("namespace is not a literal" in n for n in notes), notes)

    def test_empty_corefile_is_a_note_in_both_forms(self):
        tf = ('resource "kubernetes_config_map" "coredns" {\n'
              '  metadata {\n    name = "coredns"\n  }\n  data = { Corefile = "" }\n}\n')
        yaml = ("apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: coredns\n"
                "data:\n  Corefile: ''\n")
        section, notes = _harvest({"dns.tf": tf, "cm.yaml": yaml})
        self.assertEqual(section, {})
        self.assertEqual(sum("Corefile is empty" in n for n in notes), 2)

    def test_percent_directive_is_an_unread_template(self):
        tf = ('resource "kubernetes_config_map" "coredns" {\n'
              '  metadata {\n    name = "coredns"\n  }\n'
              '  data = { Corefile = "%{ if var.x }a%{ endif } %%{ literal }" }\n}\n')
        section, notes = _harvest({"dns.tf": tf})
        self.assertEqual(section, {})
        self.assertTrue(any("interpolated string" in n and "var.x" in n for n in notes), notes)

    def test_other_namespace_and_other_name_are_ignored(self):
        tf = ('resource "kubernetes_config_map" "a" {\n'
              '  metadata {\n    name = "coredns"\n    namespace = "dns-lab"\n  }\n'
              '  data = { Corefile = "x" }\n}\n'
              'resource "kubernetes_config_map" "b" {\n'
              '  metadata {\n    name = "app-config"\n  }\n'
              '  data = { Corefile = "x" }\n}\n')
        self.assertEqual(_sources({"dns.tf": tf}), [])

    def test_configmap_without_corefile_is_noted(self):
        tf = ('resource "kubernetes_config_map" "coredns" {\n'
              '  metadata {\n    name = "coredns"\n  }\n'
              '  data = { other = "x" }\n}\n')
        section, notes = _harvest({"dns.tf": tf})
        self.assertEqual(section, {})
        self.assertTrue(any("no Corefile key" in n for n in notes), notes)


class ManifestTest(unittest.TestCase):

    def test_configmap_in_kube_system(self):
        yaml = ("apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: coredns\n"
                "  namespace: kube-system\ndata:\n  Corefile: |\n"
                + "".join("    " + line + "\n" for line in COREFILE.splitlines()))
        [source] = _sources({"k8s/kube-system/coredns.yaml": yaml})
        self.assertEqual(source["kind"], "configmap")
        self.assertEqual(source["form"], "manifest")
        self.assertEqual(source["path"], "k8s/kube-system/coredns.yaml")
        self.assertEqual(source["directory"], "k8s/kube-system")
        self.assertEqual(source["text"], COREFILE)
        self.assertEqual(source["evidence"], ["k8s/kube-system/coredns.yaml (document 1)"])

    def test_multi_document_file_records_the_document_index(self):
        yaml = ("apiVersion: v1\nkind: Namespace\nmetadata:\n  name: kube-system\n---\n"
                "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: node-local-dns\n"
                "  namespace: kube-system\ndata:\n  Corefile: 'cluster.local:53 { cache 30 }'\n")
        [source] = _sources({"dns.yaml": yaml})
        self.assertEqual(source["name"], "node-local-dns")
        self.assertEqual(source["evidence"], ["dns.yaml (document 2)"])

    def test_other_namespace_is_ignored(self):
        yaml = ("apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: coredns\n"
                "  namespace: dns-lab\ndata:\n  Corefile: x\n")
        self.assertEqual(_sources({"c.yaml": yaml}), [])

    def test_deployment_is_a_note(self):
        yaml = ("apiVersion: apps/v1\nkind: Deployment\nmetadata:\n  name: coredns\n"
                "  namespace: kube-system\nspec:\n  replicas: 3\n")
        section, notes = _harvest({"d.yaml": yaml})
        self.assertEqual(section, {})
        self.assertTrue(any("Deployment" in n and "replicas: 3" in n for n in notes), notes)

    def test_unparseable_yaml_is_noted_not_raised(self):
        yaml = "kind: ConfigMap\nmetadata:\n  name: coredns\n  - broken\n"
        section, notes = _harvest({"c.yaml": yaml})
        self.assertEqual(section, {})
        self.assertTrue(any("c.yaml: mentions coredns but was not read" in n for n in notes), notes)

    def test_two_configmaps_in_one_file_are_two_sources(self):
        yaml = ("apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: coredns\n"
                "  namespace: kube-system\ndata:\n  Corefile: 'a'\n---\n"
                "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: node-local-dns\n"
                "  namespace: kube-system\ndata:\n  Corefile: 'b'\n")
        sources = _sources({"kube-system.yaml": yaml})
        self.assertEqual([(s["name"], s["text"]) for s in sources],
                         [("coredns", "a"), ("node-local-dns", "b")])

    def test_a_chart_at_the_repository_root_skips_every_yaml(self):
        yaml = ("apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: coredns\n"
                "data:\n  Corefile: '{{ .Values.corefile }}'\n")
        section, notes = _harvest(
            {"templates/cm.yaml": yaml, "Chart.yaml": "name: platform\n"},
            chart_roots=["."])
        self.assertEqual(section, {})
        self.assertTrue(any("under Helm chart roots (.)" in n for n in notes), notes)
        self.assertFalse(any("was not read (" in n for n in notes), notes)

    def test_a_chart_at_the_root_does_not_hide_terraform(self):
        tf = ('resource "aws_eks_addon" "coredns" {\n  addon_name = "coredns"\n'
              '  configuration_values = "{\\"replicaCount\\": 2}"\n}\n')
        section, notes = _harvest({"eks.tf": tf, "Chart.yaml": "name: platform\n"},
                                  chart_roots=["."])
        self.assertEqual(len(section["sources"]), 1)

    def test_chart_roots_are_skipped_and_named(self):
        yaml = ("apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: coredns\n"
                "data:\n  Corefile: x\n")
        section, notes = _harvest(
            {"charts/dns/templates/cm.yaml": yaml, "charts/dns/Chart.yaml": "name: dns\n"},
            chart_roots=["charts/dns"])
        self.assertEqual(section, {})
        self.assertTrue(any("charts/dns" in n and "not read" in n for n in notes), notes)
        self.assertFalse(any("rendered, not parsed" in n for n in notes), notes)


class ScopeAndShapeTest(unittest.TestCase):

    def test_excluded_files_are_not_read_and_counted(self):
        tf = ('resource "aws_eks_addon" "coredns" {\n  addon_name = "coredns"\n'
              '  configuration_values = "{\\"replicaCount\\": 2}"\n}\n')
        section, notes = _harvest(
            {"old/eks.tf": tf},
            scope={"included": [], "excluded": ["old/**"]})
        self.assertEqual(section, {})
        self.assertTrue(any("scope excludes" in n for n in notes), notes)

    def test_helm_release_is_a_note(self):
        tf = ('resource "helm_release" "dns" {\n  name  = "coredns"\n'
              '  chart = "coredns"\n  values = [file("values.yaml")]\n}\n')
        section, notes = _harvest({"helm.tf": tf})
        self.assertEqual(section, {})
        self.assertTrue(any("Helm release" in n and "values.yaml" in n for n in notes), notes)

    def test_empty_estate_is_an_empty_section_with_the_coverage_note(self):
        section, notes = _harvest({"main.tf": 'resource "aws_vpc" "v" {}\n'})
        self.assertEqual(section, {})
        self.assertIn(clusterdns.COVERAGE_NOTE, notes)

    def test_section_carries_no_interpreted_field(self):
        tf = ('resource "aws_eks_addon" "coredns" {\n  addon_name = "coredns"\n'
              '  configuration_values = "{\\"replicaCount\\": 2}"\n}\n')
        [source] = _sources({"eks.tf": tf})
        self.assertEqual(
            set(source), {"kind", "name", "address", "path", "directory", "form",
                          "addon_version", "resolve_conflicts_on_update", "text",
                          "evidence"})
        self.assertEqual(source["form"], "string")

    def test_long_text_is_truncated_with_a_note(self):
        big = "x" * (clusterdns.MAX_TEXT_BYTES + 10)
        tf = ('resource "aws_eks_addon" "coredns" {\n  addon_name = "coredns"\n'
              '  configuration_values = <<EOT\n' + big + '\nEOT\n}\n')
        section, notes = _harvest({"eks.tf": tf})
        self.assertEqual(len(section["sources"][0]["text"]), clusterdns.MAX_TEXT_BYTES)
        self.assertTrue(any("truncated" in n for n in notes), notes)

    def test_section_cap_demotes_the_overflow_to_a_note(self):
        big = "y" * (clusterdns.MAX_TEXT_BYTES - 10)
        files = {}
        for i in range(6):
            files[f"e{i}/eks.tf"] = (
                f'resource "aws_eks_addon" "c{i}" {{\n  addon_name = "coredns"\n'
                '  configuration_values = <<EOT\n' + big + '\nEOT\n}\n')
        section, notes = _harvest(files)
        self.assertEqual(len(section["sources"]), 4)
        self.assertEqual(sum("section is capped" in n for n in notes), 2)

    def test_same_source_twice_unions_evidence(self):
        yaml = ("apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: coredns\n"
                "data:\n  Corefile: x\n---\n"
                "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: coredns\n"
                "data:\n  Corefile: x\n")
        [source] = _sources({"c.yaml": yaml})
        self.assertEqual(source["evidence"],
                         ["c.yaml (document 1)", "c.yaml (document 2)"])


class ModuleFormTest(unittest.TestCase):
    """The add-on declared through an EKS module's cluster_addons map — the
    dominant real-world shape, and the one a resource-only walk misses."""

    MODULE = ('module "eks" {\n'
              '  source  = "terraform-aws-modules/eks/aws"\n'
              '  version = "~> 20.0"\n'
              '  cluster_addons = {\n'
              '    kube-proxy = {}\n'
              '    coredns = {\n'
              '      addon_version = "v1.11.1-eksbuild.4"\n'
              '      resolve_conflicts_on_update = "PRESERVE"\n'
              '      configuration_values = jsonencode({\n'
              '        corefile = <<-EOF\n'
              '          .:53 {\n'
              '            forward . 10.0.0.2\n'
              '          }\n'
              '          corp.example.com:53 {\n'
              '            forward . 10.1.2.3 10.1.2.4\n'
              '          }\n'
              '        EOF\n'
              '        replicaCount = 2\n'
              '      })\n'
              '    }\n'
              '  }\n'
              '}\n')

    def test_cluster_addons_map_is_a_source(self):
        [source] = _sources({"envs/prod/eks.tf": self.MODULE})
        self.assertEqual(source["kind"], "eks_module_addon")
        self.assertEqual(source["address"], "module.eks")
        self.assertEqual(source["addon_version"], "v1.11.1-eksbuild.4")
        self.assertEqual(source["resolve_conflicts_on_update"], "PRESERVE")
        self.assertEqual(source["form"], "expression")
        self.assertTrue(source["text"].startswith("jsonencode({"))
        self.assertIn("forward . 10.1.2.3 10.1.2.4", source["text"])
        self.assertEqual(source["evidence"], ["envs/prod/eks.tf:1"])

    def test_v21_addons_key_is_read_too(self):
        [source] = _sources({"eks.tf": self.MODULE.replace("cluster_addons", "addons")})
        self.assertEqual(source["kind"], "eks_module_addon")

    def test_module_coredns_without_configuration_is_a_note(self):
        tf = ('module "eks" {\n  source = "terraform-aws-modules/eks/aws"\n'
              '  cluster_addons = {\n    coredns = { most_recent = true }\n  }\n}\n')
        section, notes = _harvest({"eks.tf": tf})
        self.assertEqual(section, {})
        self.assertTrue(any("module.eks" in n and "default configuration" in n
                            for n in notes), notes)

    def test_module_addons_expression_is_noted(self):
        tf = ('module "eks" {\n  source = "terraform-aws-modules/eks/aws"\n'
              '  cluster_addons = var.addons\n}\n')
        section, notes = _harvest({"eks.tf": tf})
        self.assertEqual(section, {})
        self.assertTrue(any("cluster_addons is an expression" in n for n in notes), notes)

    def test_blueprints_eks_addons_key_is_read(self):
        tf = ('module "addons" {\n  source = "aws-ia/eks-blueprints-addons/aws"\n'
              '  eks_addons = {\n    coredns = {\n'
              '      configuration_values = jsonencode({ replicaCount = 3 })\n'
              '    }\n  }\n}\n')
        [source] = _sources({"addons.tf": tf})
        self.assertEqual(source["address"], "module.addons")
        # The value ends at the map entry, not at the end of the line.
        self.assertEqual(source["text"], "jsonencode({ replicaCount = 3 })")

    def test_trailing_comma_is_not_recorded(self):
        tf = ('module "eks" {\n  source = "terraform-aws-modules/eks/aws"\n'
              '  cluster_addons = {\n    coredns = { configuration_values = jsonencode({ replicaCount = 3 }), most_recent = true }\n'
              '  }\n}\n')
        [source] = _sources({"eks.tf": tf})
        self.assertEqual(source["text"], "jsonencode({ replicaCount = 3 })")

    def test_module_without_coredns_says_nothing(self):
        tf = ('module "eks" {\n  source = "terraform-aws-modules/eks/aws"\n'
              '  cluster_addons = {\n    vpc-cni = {}\n  }\n}\n'
              'module "vpc" {\n  source = "terraform-aws-modules/vpc/aws"\n}\n')
        section, notes = _harvest({"eks.tf": tf})
        self.assertEqual(section, {})
        self.assertFalse(any("module." in n for n in notes), notes)


class ReferenceTest(unittest.TestCase):
    """A recorded text must carry the configuration; a reference to a value
    outside the file is reported, never recorded as if it were the value."""

    def test_nested_file_call_is_unread_and_named(self):
        tf = ('resource "aws_eks_addon" "coredns" {\n  addon_name = "coredns"\n'
              '  configuration_values = jsonencode({\n'
              '    corefile = file("${path.module}/Corefile")\n'
              '  })\n}\n')
        section, notes = _harvest({"eks.tf": tf})
        self.assertEqual(section, {})
        self.assertTrue(any('file("${path.module}/Corefile")' in n and "not recorded" in n
                            for n in notes), notes)

    def test_merge_of_a_local_is_unread(self):
        tf = ('resource "aws_eks_addon" "coredns" {\n  addon_name = "coredns"\n'
              '  configuration_values = jsonencode(merge(local.dns, { replicaCount = 2 }))\n}\n')
        section, notes = _harvest({"eks.tf": tf})
        self.assertEqual(section, {})
        self.assertTrue(any("local.dns" in n for n in notes), notes)

    def test_readable_corefile_beside_a_reference_is_kept_and_noted(self):
        tf = ('resource "aws_eks_addon" "coredns" {\n  addon_name = "coredns"\n'
              '  configuration_values = jsonencode({\n'
              '    corefile = <<-EOF\n      .:53 {\n        forward . 10.0.0.2\n      }\n    EOF\n'
              '    replicaCount = var.coredns_replicas\n'
              '  })\n}\n')
        section, notes = _harvest({"eks.tf": tf})
        [source] = section["sources"]
        self.assertIn("forward . 10.0.0.2", source["text"])
        self.assertTrue(any("also references var.coredns_replicas" in n for n in notes), notes)

    def test_hostname_in_a_corefile_is_not_a_reference(self):
        tf = ('resource "aws_eks_addon" "coredns" {\n  addon_name = "coredns"\n'
              '  configuration_values = jsonencode({\n'
              '    corefile = <<-EOF\n      data.corp.example.com:53 {\n'
              '        forward . 10.1.2.3\n      }\n    EOF\n'
              '  })\n}\n')
        section, notes = _harvest({"eks.tf": tf})
        self.assertEqual(len(section["sources"]), 1)
        self.assertFalse(any("references" in n for n in notes), notes)

    def test_interpolated_configmap_string_is_unread(self):
        tf = ('resource "kubernetes_config_map" "coredns" {\n'
              '  metadata {\n    name = "coredns"\n  }\n'
              '  data = {\n    Corefile = "${file("${path.module}/Corefile")}"\n  }\n}\n')
        section, notes = _harvest({"dns.tf": tf})
        self.assertEqual(section, {})
        self.assertTrue(any("file(" in n and "not recorded" in n for n in notes), notes)

    def test_heredoc_interpolation_is_recorded_with_a_note(self):
        tf = ('resource "kubernetes_config_map" "coredns" {\n'
              '  metadata {\n    name = "coredns"\n  }\n'
              '  data = {\n    Corefile = <<EOF\n.:53 {\n    forward . ${var.upstream}\n}\nEOF\n'
              '  }\n}\n')
        section, notes = _harvest({"dns.tf": tf})
        [source] = section["sources"]
        self.assertIn("forward . ${var.upstream}", source["text"])
        self.assertTrue(any("also references ${var.upstream}" in n for n in notes), notes)

    def test_configmap_v1_data_patch_is_a_source(self):
        # The add-on owns the coredns ConfigMap on EKS, so the Terraform way
        # to customize it is a data patch over the existing object.
        tf = ('resource "kubernetes_config_map_v1_data" "coredns" {\n'
              '  metadata {\n    name      = "coredns"\n    namespace = "kube-system"\n  }\n'
              '  force = true\n'
              '  data = {\n    Corefile = <<EOF\n' + COREFILE + 'EOF\n  }\n}\n')
        [source] = _sources({"dns.tf": tf})
        self.assertEqual(source["kind"], "kubernetes_config_map")
        self.assertEqual(source["address"], "kubernetes_config_map_v1_data.coredns")
        self.assertEqual(source["text"], COREFILE)

    def test_resource_attribute_reference_is_unread(self):
        tf = ('resource "aws_eks_addon" "coredns" {\n  addon_name = "coredns"\n'
              '  configuration_values = aws_ssm_parameter.coredns.value\n}\n')
        section, notes = _harvest({"eks.tf": tf})
        self.assertEqual(section, {})
        self.assertTrue(any("aws_ssm_parameter.coredns.value" in n and "not recorded" in n
                            for n in notes), notes)

    def test_corefile_from_a_resource_attribute_is_unread(self):
        tf = ('resource "aws_eks_addon" "coredns" {\n  addon_name = "coredns"\n'
              '  configuration_values = jsonencode({\n'
              '    corefile = kubernetes_config_map.base.data["Corefile"]\n'
              '  })\n}\n')
        section, notes = _harvest({"eks.tf": tf})
        self.assertEqual(section, {})
        self.assertTrue(any("kubernetes_config_map.base" in n for n in notes), notes)

    def test_heredoc_interpolation_inside_jsonencode_is_noted(self):
        tf = ('resource "aws_eks_addon" "coredns" {\n  addon_name = "coredns"\n'
              '  configuration_values = jsonencode({\n'
              '    corefile = <<-EOF\n      .:53 {\n        forward . ${var.upstream}\n      }\n    EOF\n'
              '  })\n}\n')
        section, notes = _harvest({"eks.tf": tf})
        [source] = section["sources"]
        self.assertIn("forward . ${var.upstream}", source["text"])
        self.assertTrue(any("also references ${var.upstream}" in n for n in notes), notes)

    def test_wrapped_resource_attribute_is_unread(self):
        # The TF 0.11 wrapper goes through the same rules as a bare expression.
        tf = ('resource "aws_eks_addon" "coredns" {\n  addon_name = "coredns"\n'
              '  configuration_values = "${aws_ssm_parameter.coredns.value}"\n}\n')
        section, notes = _harvest({"eks.tf": tf})
        self.assertEqual(section, {})
        self.assertTrue(any("aws_ssm_parameter.coredns.value" in n for n in notes), notes)

    def test_wrapped_object_with_an_attribute_corefile_is_unread(self):
        tf = ('resource "kubernetes_config_map" "coredns" {\n'
              '  metadata {\n    name = "coredns"\n  }\n'
              '  data = {\n'
              '    Corefile = "${kubernetes_config_map.base.data["Corefile"]}"\n'
              '  }\n}\n')
        section, notes = _harvest({"dns.tf": tf})
        self.assertEqual(section, {})
        self.assertTrue(any("kubernetes_config_map.base" in n for n in notes), notes)

    def test_one_line_object_note_names_only_the_reference(self):
        tf = ('resource "aws_eks_addon" "coredns" {\n  addon_name = "coredns"\n'
              '  configuration_values = jsonencode({ corefile = local.corefile, replicaCount = 2 })\n}\n')
        section, notes = _harvest({"eks.tf": tf})
        self.assertEqual(section, {})
        self.assertTrue(any("read from local.corefile," in n for n in notes), notes)
        self.assertFalse(any("replicaCount" in n for n in notes), notes)

    def test_multi_line_template_is_read_whole(self):
        tf = ('resource "aws_eks_addon" "coredns" {\n  addon_name = "coredns"\n'
              '  configuration_values = "${jsonencode({\n'
              '    corefile = "x.:53 { forward . 10.0.0.2 }"\n'
              '    replicaCount = 3\n'
              '  })}"\n'
              '}\n')
        [source] = _sources({"eks.tf": tf})
        self.assertEqual(source["form"], "expression")
        self.assertTrue(source["text"].startswith("jsonencode({"))
        self.assertIn("replicaCount = 3", source["text"])
        self.assertIn("forward . 10.0.0.2", source["text"])


if __name__ == "__main__":
    unittest.main()
