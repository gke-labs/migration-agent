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

"""Unit tests for the cluster-dns output contract. Pure, no GCS, no LLM.

The gate must judge a translation without knowing CoreDNS: every case
below is phrased as "these files, this source text, this verdict".
"""

import json
import unittest

from servers.phases.translation.translation_validate_3 import clusterdns_contract as gate

COREFILE = (
    ".:53 {\n    errors\n    forward . 10.0.0.2 10.0.0.3\n    cache 30\n}\n"
    "corp.example.com:53 {\n    forward . 10.1.2.3 10.1.2.4\n}\n")

CONFIGMAP = (
    "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: kube-dns\n  namespace: kube-system\n"
    "data:\n"
    "  stubDomains: |\n    " + json.dumps({"corp.example.com": ["10.1.2.3", "10.1.2.4"]}) + "\n"
    "  upstreamNameservers: |\n    " + json.dumps(["10.0.0.2", "10.0.0.3"]) + "\n")

FORWARDING_TF = '''variable "network" {
  type = string
}

variable "network_project" {
  type = string
}

resource "google_dns_managed_zone" "corp" {
  name       = "corp-example-com"
  dns_name   = "corp.example.com."
  visibility = "private"
  private_visibility_config {
    networks {
      network_url = "projects/${var.network_project}/global/networks/${var.network}"
    }
  }
  forwarding_config {
    target_name_servers {
      ipv4_address = "10.1.2.3"
    }
    target_name_servers {
      ipv4_address = "10.1.2.4"
    }
  }
}

'''


AUTOPILOT_QUESTIONS = ["pinned upstreams 10.0.0.2 and 10.0.0.3: what did they resolve that public DNS will not?"]


def unit(files, source=COREFILE, decision="GKE_STANDARD_NAP", tradeoffs="t",
         open_questions=(), kind="cluster-dns"):
    inputs = {"decision": decision,
              "cluster_dns": {"sources": [{"kind": "configmap", "name": "coredns",
                                           "text": source}]} if source is not None else {}}
    return {"unit": {"unit_id": "cluster-dns", "kind": kind, "inputs": inputs},
            "result": {"files": [{"path": p, "content": c} for p, c in files],
                       "tradeoffs": tradeoffs, "assumptions": [],
                       "open_questions": list(open_questions)}}


def errors(entry):
    return [f["error"] for f in gate.check_units([entry])["findings"]]


class HappyPathTest(unittest.TestCase):

    def test_faithful_standard_unit_passes(self):
        self.assertEqual(errors(unit([("kube-dns.yaml", CONFIGMAP)])), [])

    def test_faithful_autopilot_unit_passes(self):
        self.assertEqual(errors(unit([("dns.tf", FORWARDING_TF)], decision="GKE_AUTOPILOT",
                                     open_questions=AUTOPILOT_QUESTIONS)), [])

    def test_an_unrecorded_mode_is_treated_like_autopilot(self):
        self.assertEqual(errors(unit([("dns.tf", FORWARDING_TF)], decision=None,
                                     open_questions=AUTOPILOT_QUESTIONS)), [])
        e = errors(unit([("kube-dns.yaml", CONFIGMAP)], decision=None))
        self.assertTrue(any("mode is not recorded" in x for x in e), e)
        entry = unit([("kube-dns.yaml", CONFIGMAP)], decision=None)
        entry["unit"]["inputs"]["decision_reason"] = "disagree: karpenter=GKE_AUTOPILOT, privileged_daemonsets=GKE_STANDARD"
        e = errors(entry)
        self.assertTrue(any("decisions disagree on the mode (karpenter=GKE_AUTOPILOT" in x for x in e), e)
        self.assertFalse(any("mode is not recorded" in x for x in e), e)

    def test_other_kinds_are_not_contract_checked(self):
        # A storage unit is only swept for foreign DNS objects; its own
        # StorageClass is none of this gate's business.
        sc = "apiVersion: storage.k8s.io/v1\nkind: StorageClass\nmetadata:\n  name: fast\nprovisioner: pd.csi.storage.gke.io\n"
        report = gate.check_units([unit([("sc.yaml", sc)], kind="storage")])
        self.assertEqual(report, {"checked": [], "findings": []})

    def test_a_dropped_value_accounted_for_in_the_tradeoffs_passes(self):
        # Only the stub domain shipped; the upstreams are explained away.
        cm = CONFIGMAP.replace("  upstreamNameservers: |\n    " + json.dumps(["10.0.0.2", "10.0.0.3"]) + "\n", "")
        self.assertEqual(errors(unit([("kube-dns.yaml", cm)],
                                     tradeoffs="10.0.0.2 and 10.0.0.3 are the VPC resolver; dropped")),
                         [])


class SchemaTest(unittest.TestCase):

    def test_wrong_namespace(self):
        cm = CONFIGMAP.replace("namespace: kube-system", "namespace: default")
        self.assertTrue(any("kube-system only" in e for e in errors(unit([("c.yaml", cm)]))))

    def test_corefile_key_is_refused(self):
        cm = CONFIGMAP + "  Corefile: |\n    .:53 { forward . 10.0.0.2 }\n"
        self.assertTrue(any("Corefile key" in e for e in errors(unit([("c.yaml", cm)]))))

    def test_unknown_key_is_refused(self):
        cm = CONFIGMAP + "  extra: 'x'\n"
        self.assertTrue(any("data.extra" in e for e in errors(unit([("c.yaml", cm)]))))

    def test_stub_domains_must_be_a_json_object_of_ips(self):
        cm = CONFIGMAP.replace(json.dumps({"corp.example.com": ["10.1.2.3", "10.1.2.4"]}),
                               json.dumps({"corp.example.com": ["resolver.corp.example.com"]}))
        self.assertTrue(any("not an IP address" in e for e in errors(unit([("c.yaml", cm)]))))
        cm = CONFIGMAP.replace(json.dumps({"corp.example.com": ["10.1.2.3", "10.1.2.4"]}), "[1]")
        self.assertTrue(any("JSON object" in e for e in errors(unit([("c.yaml", cm)]))))
        cm = CONFIGMAP.replace(json.dumps({"corp.example.com": ["10.1.2.3", "10.1.2.4"]}), "{not json")
        self.assertTrue(any("not valid JSON" in e for e in errors(unit([("c.yaml", cm)]))))

    def test_too_many_upstreams(self):
        src = COREFILE.replace("10.0.0.2 10.0.0.3", "10.0.0.2 10.0.0.3 10.0.0.4 10.0.0.5")
        cm = CONFIGMAP.replace(json.dumps(["10.0.0.2", "10.0.0.3"]),
                               json.dumps(["10.0.0.2", "10.0.0.3", "10.0.0.4", "10.0.0.5"]))
        self.assertTrue(any("at most 3" in e for e in errors(unit([("c.yaml", cm)], source=src))))

    def test_two_configmaps(self):
        e = errors(unit([("a.yaml", CONFIGMAP), ("b.yaml", CONFIGMAP)]))
        self.assertTrue(any("more than one kube-dns ConfigMap" in x for x in e), e)

    def test_public_zone_and_undotted_name(self):
        tf = ('resource "google_dns_managed_zone" "z" {\n  name = "z"\n'
              '  dns_name = "corp.example.com"\n  visibility = "public"\n}\n')
        e = errors(unit([("dns.tf", tf)], decision="GKE_AUTOPILOT",
                        tradeoffs="10.0.0.2 10.0.0.3 10.1.2.3 10.1.2.4 dropped"))
        self.assertTrue(any('visibility = "private"' in x for x in e), e)
        self.assertTrue(any("trailing dot" in x for x in e), e)

    def test_record_set_must_name_a_declared_zone(self):
        tf = FORWARDING_TF + ('resource "google_dns_record_set" "r" {\n  name = "db.corp.example.com."\n'
                              '  managed_zone = google_dns_managed_zone.other.name\n  type = "A"\n'
                              '  rrdatas = ["10.1.2.3"]\n}\n')
        e = errors(unit([("dns.tf", tf)], decision="GKE_AUTOPILOT",
                        open_questions=AUTOPILOT_QUESTIONS))
        self.assertTrue(any("must name a zone this unit declares" in x for x in e), e)


class BoundaryTest(unittest.TestCase):

    def test_gke_owned_objects_are_refused(self):
        for kind, name in (("Deployment", "coredns"), ("DaemonSet", "node-local-dns"),
                           ("ConfigMap", "coredns"), ("Namespace", "kube-system")):
            with self.subTest(kind=kind):
                doc = f"apiVersion: v1\nkind: {kind}\nmetadata:\n  name: {name}\n  namespace: kube-system\n"
                e = errors(unit([("kube-dns.yaml", CONFIGMAP), ("x.yaml", doc)]))
                self.assertTrue(any(f"{kind} named {name}" in x for x in e), e)

    def test_cluster_and_dns_config_are_refused(self):
        tf = FORWARDING_TF + ('resource "google_container_cluster" "c" {\n  name = "c"\n'
                              '  dns_config {\n    cluster_dns = "CLOUD_DNS"\n  }\n}\n')
        e = errors(unit([("dns.tf", tf)], decision="GKE_AUTOPILOT",
                        open_questions=AUTOPILOT_QUESTIONS))
        self.assertTrue(any("google_container_cluster" in x for x in e), e)
        self.assertTrue(any("dns_config block" in x for x in e), e)

    def test_a_commented_out_cluster_is_not_a_finding(self):
        tf = FORWARDING_TF + '# resource "google_container_cluster" "c" {}\n'
        self.assertEqual(errors(unit([("dns.tf", tf)], decision="GKE_AUTOPILOT",
                                     open_questions=AUTOPILOT_QUESTIONS)), [])

    def test_a_dns_policy_is_refused(self):
        tf = FORWARDING_TF + ('resource "google_dns_policy" "p" {\n  name = "p"\n'
                              '  alternative_name_server_config {\n    target_name_servers {\n'
                              '      ipv4_address = "10.0.0.2"\n    }\n  }\n}\n')
        e = errors(unit([("dns.tf", tf)], decision="GKE_AUTOPILOT",
                        open_questions=["10.0.0.3"]))
        self.assertTrue(any("google_dns_policy" in x for x in e), e)

    def test_a_forwarding_zone_must_also_be_private(self):
        tf = FORWARDING_TF.replace('  visibility = "private"\n', "")
        e = errors(unit([("dns.tf", tf)], decision="GKE_AUTOPILOT",
                        open_questions=AUTOPILOT_QUESTIONS))
        self.assertTrue(any('visibility = "private"' in x for x in e), e)

    def test_configmap_under_autopilot_is_refused(self):
        e = errors(unit([("kube-dns.yaml", CONFIGMAP)], decision="GKE_AUTOPILOT"))
        self.assertTrue(any("Autopilot cluster" in x for x in e), e)

    def test_configmap_over_empty_inputs_is_refused(self):
        e = errors(unit([("kube-dns.yaml", CONFIGMAP)], source=None,
                        tradeoffs="10.0.0.2 10.0.0.3 10.1.2.3 10.1.2.4"))
        self.assertTrue(any("carry no Corefile text" in x for x in e), e)


class ConservationTest(unittest.TestCase):

    def test_invented_ip_is_a_finding(self):
        cm = CONFIGMAP.replace("10.0.0.3", "8.8.8.8")
        e = errors(unit([("kube-dns.yaml", cm)], tradeoffs="10.0.0.3 replaced"))
        self.assertTrue(any("address 8.8.8.8" in x and "not come from" in x for x in e), e)

    def test_invented_name_is_a_finding(self):
        cm = CONFIGMAP.replace("corp.example.com", "corp.example.net")
        e = errors(unit([("kube-dns.yaml", cm)]))
        self.assertTrue(any("name corp.example.net" in x for x in e), e)

    def test_dropped_source_ip_is_a_finding(self):
        cm = CONFIGMAP.replace(json.dumps(["10.0.0.2", "10.0.0.3"]), json.dumps(["10.0.0.2"]))
        e = errors(unit([("kube-dns.yaml", cm)]))
        self.assertTrue(any("address 10.0.0.3" in x and "dropped silently" in x for x in e), e)

    def test_a_source_ip_named_in_an_open_question_is_accounted_for(self):
        cm = CONFIGMAP.replace(json.dumps(["10.0.0.2", "10.0.0.3"]), json.dumps(["10.0.0.2"]))
        e = errors(unit([("kube-dns.yaml", cm)],
                        open_questions=["is 10.0.0.3 still a resolver?"]))
        self.assertEqual(e, [])

    def test_names_from_the_inputs_and_allowed_suffixes_pass(self):
        tf = FORWARDING_TF.replace('type = string\n}', 'type = string\n  default = "projects/p/global/networks/shared-vpc"\n}')
        tf = tf.replace(
            'network_url = "projects/${var.network_project}/global/networks/${var.network}"',
            'network_url = "https://www.googleapis.com/compute/v1/projects/${var.network_project}/global/networks/${var.network}"')
        self.assertIn("www.googleapis.com", tf)  # the replace took: an allowed suffix inside a URL
        self.assertEqual(errors(unit([("dns.tf", tf)], decision="GKE_AUTOPILOT",
                                     open_questions=AUTOPILOT_QUESTIONS)), [])

    def test_interpolations_and_prose_are_not_names(self):
        tf = FORWARDING_TF.replace('type = string\n}', 'type = string\n  description = "the VPC, e.g. shared-vpc (see local.notes, i.e. the design)"\n}', 1)
        yaml = ("apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: kube-dns\n  namespace: kube-system\n"
                "  annotations:\n    note: \"resolvers per corp.example.com, e.g. ${var.x} and each.value\"\n"
                "data:\n  stubDomains: '{\"corp.example.com\": [\"10.1.2.3\", \"10.1.2.4\"]}'\n"
                "  upstreamNameservers: '[\"10.0.0.2\", \"10.0.0.3\"]'\n")
        self.assertEqual(errors(unit([("dns.tf", tf)], decision="GKE_AUTOPILOT",
                                     open_questions=AUTOPILOT_QUESTIONS)), [])
        self.assertEqual(errors(unit([("kube-dns.yaml", yaml)])), [])

    def test_identifiers_are_not_names(self):
        # google_dns_managed_zone.corp.name and var.network are identifiers,
        # not hostnames; only string scalars are scanned.
        tf = FORWARDING_TF + ('resource "google_dns_record_set" "r" {\n  name = "db.corp.example.com."\n'
                              '  managed_zone = google_dns_managed_zone.corp.name\n  type = "A"\n'
                              '  rrdatas = ["10.1.2.3"]\n}\n')
        src = COREFILE + "hosts {\n    10.1.2.3 db.corp.example.com\n    fallthrough\n}\n"
        self.assertEqual(errors(unit([("dns.tf", tf)], source=src, decision="GKE_AUTOPILOT",
                                     open_questions=AUTOPILOT_QUESTIONS)), [])

    def test_a_sentence_final_address_in_the_prose_is_accounted_for(self):
        cm = CONFIGMAP.replace(json.dumps(["10.0.0.2", "10.0.0.3"]), json.dumps(["10.0.0.2"]))
        e = errors(unit([("kube-dns.yaml", cm)],
                        tradeoffs="The second upstream was the VPC resolver at 10.0.0.3."))
        self.assertEqual(e, [])

    def test_a_single_label_stub_domain_is_a_domain(self):
        src = "consul:53 {\n    forward . 10.1.2.3\n}\n"
        cm = ("apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: kube-dns\n  namespace: kube-system\n"
              "data:\n  stubDomains: '{\"consul\": [\"10.1.2.3\"]}'\n")
        self.assertEqual(errors(unit([("kube-dns.yaml", cm)], source=src)), [])

    def test_file_names_in_descriptions_are_not_hosts(self):
        tf = FORWARDING_TF.replace('type = string\n}',
                                   'type = string\n  description = "declared in variables.tf, see design-decisions.md and kube-dns.yaml"\n}')
        self.assertEqual(errors(unit([("dns.tf", tf)], decision="GKE_AUTOPILOT",
                                     open_questions=AUTOPILOT_QUESTIONS)), [])

    def test_an_address_inside_a_heredoc_is_judged_too(self):
        tf = FORWARDING_TF + 'locals {\n  note = <<EOT\nresolver 8.8.4.4\nEOT\n}\n'
        e = errors(unit([("dns.tf", tf)], decision="GKE_AUTOPILOT",
                        open_questions=AUTOPILOT_QUESTIONS))
        self.assertTrue(any("address 8.8.4.4" in x for x in e), e)

    def test_a_record_set_in_a_variable_zone_is_a_finding(self):
        tf = FORWARDING_TF + ('resource "google_dns_record_set" "r" {\n  name = "db.corp.example.com."\n'
                              '  managed_zone = var.existing_zone\n  type = "A"\n'
                              '  rrdatas = ["10.1.2.3"]\n}\n')
        e = errors(unit([("dns.tf", tf)], decision="GKE_AUTOPILOT",
                        open_questions=AUTOPILOT_QUESTIONS))
        self.assertTrue(any("var.existing_zone" in x and "must name a zone this unit declares" in x
                            for x in e), e)

    def test_an_input_address_must_match_whole_not_as_a_substring(self):
        src = COREFILE.replace("10.0.0.2 10.0.0.3", "10.0.0.25 10.0.0.3")
        cm = CONFIGMAP  # ships 10.0.0.2, a prefix of the source's 10.0.0.25
        e = errors(unit([("kube-dns.yaml", cm)], source=src, tradeoffs="10.0.0.25 dropped"))
        self.assertTrue(any("address 10.0.0.2" in x and "not come from" in x for x in e), e)

    def test_a_templated_domain_is_an_invented_name(self):
        tf = FORWARDING_TF.replace('dns_name   = "corp.example.com."', 'dns_name   = "${var.env}.corp.example.net."')
        e = errors(unit([("dns.tf", tf)], decision="GKE_AUTOPILOT",
                        open_questions=AUTOPILOT_QUESTIONS))
        self.assertTrue(any("x.corp.example.net" in x for x in e), e)

    def test_a_two_label_file_name_is_skipped_but_a_cctld_name_is_not(self):
        tf = FORWARDING_TF.replace('type = string\n}', 'type = string\n  description = "see variables.tf; mirrors corp.example.md"\n}')
        e = errors(unit([("dns.tf", tf)], decision="GKE_AUTOPILOT",
                        open_questions=AUTOPILOT_QUESTIONS))
        self.assertFalse(any("variables.tf" in x for x in e), e)
        self.assertTrue(any("corp.example.md" in x for x in e), e)

    def test_a_non_literal_dns_name_is_a_finding(self):
        tf = FORWARDING_TF.replace('dns_name   = "corp.example.com."', 'dns_name   = var.stub_domain')
        e = errors(unit([("dns.tf", tf)], decision="GKE_AUTOPILOT",
                        open_questions=AUTOPILOT_QUESTIONS))
        self.assertTrue(any("not a literal" in x for x in e), e)

    def test_the_aws_vpc_resolver_link_local_is_nobodys_data(self):
        src = COREFILE.replace("forward . 10.0.0.2 10.0.0.3", "forward . 169.254.169.253")
        cm = CONFIGMAP.replace("  upstreamNameservers: |\n    " + json.dumps(["10.0.0.2", "10.0.0.3"]) + "\n", "")
        self.assertEqual(errors(unit([("kube-dns.yaml", cm)], source=src)), [])

    def test_a_leading_comment_does_not_disable_the_scan(self):
        tf = "# forwarding zone for corp\n" + FORWARDING_TF.replace('ipv4_address = "10.1.2.4"', 'ipv4_address = "8.8.4.4"')
        e = errors(unit([("dns.tf", tf)], decision="GKE_AUTOPILOT",
                        open_questions=AUTOPILOT_QUESTIONS + ["10.1.2.4 dropped"]))
        self.assertTrue(any("address 8.8.4.4" in x for x in e), e)

    def test_a_url_string_does_not_disable_the_scan(self):
        tf = FORWARDING_TF.replace('type = string\n}', 'type = string\n  description = "see https://cloud.google.com/dns"\n}', 1)
        tf = tf.replace('ipv4_address = "10.1.2.4"', 'ipv4_address = "8.8.4.4"')
        e = errors(unit([("dns.tf", tf)], decision="GKE_AUTOPILOT",
                        open_questions=AUTOPILOT_QUESTIONS + ["10.1.2.4 dropped"]))
        self.assertTrue(any("address 8.8.4.4" in x for x in e), e)

    def test_hcl_string_lexer_handles_nested_quotes_and_comments(self):
        content = ('# "not.a.host.example" in a comment\n'
                   'a = "${lookup(var.m, "key.example.org")}.corp.example.net."  // "trailing.example.io"\n'
                   'b = <<EOT\n"inside.heredoc.example"\nEOT\n'
                   'c = "plain.example.com"\n')
        self.assertEqual(gate._hcl_strings(content),
                         ['${lookup(var.m, "key.example.org")}.corp.example.net.', "plain.example.com"])

    def test_every_legacy_token_is_read_as_its_base_mode(self):
        # A stored plan from before the projection carries a token in
        # inputs.decision and no cluster_mode key: the registry's mode_of
        # decides, advisory (gpu_tpu) tokens included.
        from servers.dag.server import decisions as decisions_lib
        for token in decisions_lib.all_tokens():
            mode, _advisory = decisions_lib.mode_of(token)
            with self.subTest(token=token, mode=mode):
                e = errors(unit([("kube-dns.yaml", CONFIGMAP)], decision=token))
                if mode == "standard":
                    self.assertEqual(e, [])
                elif mode == "autopilot":
                    self.assertTrue(any("Autopilot cluster" in x for x in e), e)
                else:
                    self.assertTrue(any("mode is not recorded" in x for x in e), e)

    def test_a_present_cluster_mode_wins_over_the_legacy_token(self):
        entry = unit([("kube-dns.yaml", CONFIGMAP)], decision="GKE_STANDARD_NAP")
        entry["unit"]["inputs"]["cluster_mode"] = "autopilot"
        self.assertTrue(any("Autopilot cluster" in x for x in errors(entry)))
        entry["unit"]["inputs"]["cluster_mode"] = "standard"
        entry["unit"]["inputs"]["decision"] = "GKE_AUTOPILOT"
        self.assertEqual(errors(entry), [])
        # Present-but-null is judged on the null (NAP[T]+BYPASS[T] plans),
        # never by falling through to the token.
        entry["unit"]["inputs"]["cluster_mode"] = None
        entry["unit"]["inputs"]["decision"] = "GKE_STANDARD_NAP"
        entry["unit"]["inputs"]["decision_reason"] = "disagree: karpenter=GKE_STANDARD_NAP, privileged_daemonsets=GKE_AUTOPILOT_BYPASS"
        e = errors(entry)
        self.assertTrue(any("decisions disagree on the mode" in x for x in e), e)

    def test_a_widened_forwarding_zone_is_a_finding(self):
        # The source forwards internal.corp.example.com; a zone for its
        # parent would send the whole corp domain to the stub resolvers.
        src = "internal.corp.example.com:53 {\n    forward . 10.1.2.3 10.1.2.4\n}\n"
        e = errors(unit([("dns.tf", FORWARDING_TF)], source=src, decision="GKE_AUTOPILOT"))
        self.assertTrue(any("corp.example.com" in x and "never a parent" in x for x in e), e)
        cm = ("apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: kube-dns\n  namespace: kube-system\n"
              "data:\n  stubDomains: '{\"corp.example.com\": [\"10.1.2.3\", \"10.1.2.4\"]}'\n")
        e = errors(unit([("kube-dns.yaml", cm)], source=src))
        self.assertTrue(any("never a parent" in x for x in e), e)

    def test_a_record_zone_may_be_the_parent_of_its_records(self):
        src = "hosts {\n    10.1.2.3 db.corp.example.com\n    10.1.2.4 cache.corp.example.com\n    fallthrough\n}\n"
        tf = ('variable "network" {\n  type = string\n}\nvariable "network_project" {\n  type = string\n}\n'
              'resource "google_dns_managed_zone" "corp" {\n  name = "corp"\n  dns_name = "corp.example.com."\n'
              '  visibility = "private"\n  private_visibility_config {\n    networks {\n'
              '      network_url = "projects/${var.network_project}/global/networks/${var.network}"\n    }\n  }\n}\n'
              'resource "google_dns_record_set" "db" {\n  name = "db.corp.example.com."\n'
              '  managed_zone = google_dns_managed_zone.corp.name\n  type = "A"\n  rrdatas = ["10.1.2.3"]\n}\n'
              'resource "google_dns_record_set" "cache" {\n  name = "cache.corp.example.com."\n'
              '  managed_zone = google_dns_managed_zone.corp.name\n  type = "A"\n  rrdatas = ["10.1.2.4"]\n}\n')
        self.assertEqual(errors(unit([("dns.tf", tf)], source=src, decision="GKE_AUTOPILOT")), [])

    def test_a_substring_of_a_source_name_is_not_a_name(self):
        cm = CONFIGMAP.replace("corp.example.com", "xample.com")
        e = errors(unit([("kube-dns.yaml", cm)]))
        self.assertTrue(any("name xample.com" in x for x in e), e)

    def test_any_other_configmap_is_a_finding(self):
        other = ("apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: coredns-custom\n"
                 "  namespace: kube-system\ndata:\n  Corefile: 'x'\n")
        e = errors(unit([("kube-dns.yaml", CONFIGMAP), ("extra.yaml", other)]))
        self.assertTrue(any("ConfigMap named coredns-custom" in x for x in e), e)

    def test_any_other_kube_system_object_is_a_finding(self):
        svc = ("apiVersion: v1\nkind: Service\nmetadata:\n  name: kube-dns\n  namespace: kube-system\n"
               "spec:\n  ports:\n  - port: 53\n")
        e = errors(unit([("kube-dns.yaml", CONFIGMAP), ("svc.yaml", svc)]))
        self.assertTrue(any("Service named kube-dns in kube-system" in x for x in e), e)

    def test_an_apex_record_over_a_child_source_name_is_a_finding(self):
        src = "hosts {\n    10.1.2.3 db.corp.example.com\n    fallthrough\n}\n"
        tf = ('variable "network" {\n  type = string\n}\nvariable "network_project" {\n  type = string\n}\n'
              'resource "google_dns_managed_zone" "corp" {\n  name = "corp"\n  dns_name = "corp.example.com."\n'
              '  visibility = "private"\n  private_visibility_config {\n    networks {\n'
              '      network_url = "projects/${var.network_project}/global/networks/${var.network}"\n    }\n  }\n}\n'
              'resource "google_dns_record_set" "apex" {\n  name = "corp.example.com."\n'
              '  managed_zone = google_dns_managed_zone.corp.name\n  type = "A"\n  rrdatas = ["10.1.2.3"]\n}\n')
        e = errors(unit([("dns.tf", tf)], source=src, decision="GKE_AUTOPILOT"))
        self.assertTrue(any("name corp.example.com" in x and "never a parent" in x for x in e), e)

    def test_a_json_escaped_corefile_is_read_through_its_escapes(self):
        # The managed add-on's configuration_values: the Corefile is one JSON
        # string, so a stub domain sits right after a literal backslash-n.
        source = ('{\n  "replicaCount": 2,\n  "corefile": ".:53 {\\n    hosts {\\n10.7.7.7 db.corp.acme.internal'
                  '\\n        fallthrough\\n    }\\n    forward . 10.42.0.2\\n}\\n'
                  'corp.acme.internal:53 {\\n    forward . 10.20.0.53 10.20.1.53\\n}\\n"\n}\n')
        cm = ("apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: kube-dns\n  namespace: kube-system\n"
              "data:\n  stubDomains: '{\"corp.acme.internal\": [\"10.20.0.53\", \"10.20.1.53\"]}'\n")
        # The hosts address sits right after an escaped newline too: it must be
        # seen as dropped when nothing carries it, and as known when a zone does.
        e = errors(unit([("kube-dns.yaml", cm)], source=source,
                        tradeoffs="10.42.0.2 is the VPC resolver; dropped"))
        self.assertTrue(any("address 10.7.7.7" in x and "dropped silently" in x for x in e), e)
        self.assertEqual(errors(unit([("kube-dns.yaml", cm)], source=source,
                                     tradeoffs="10.42.0.2 is the VPC resolver; dropped; "
                                               "10.7.7.7 db host left as an open question")), [])
        typo = cm.replace("10.20.1.53", "10.20.1.54")
        e = errors(unit([("kube-dns.yaml", typo)], source=source,
                        tradeoffs="10.42.0.2 is the VPC resolver; dropped"))
        self.assertTrue(any("address 10.20.1.54" in x for x in e), e)
        self.assertTrue(any("address 10.20.1.53" in x and "dropped silently" in x for x in e), e)

    def test_ignored_addresses_do_not_count(self):
        cm = CONFIGMAP
        src = COREFILE + "# node-local 169.254.20.10 and loopback 127.0.0.1\n"
        self.assertEqual(errors(unit([("kube-dns.yaml", cm)], source=src)), [])

    def test_unparseable_yaml_is_a_finding(self):
        e = errors(unit([("kube-dns.yaml", "kind: ConfigMap\nmetadata: [unclosed\n")],
                        tradeoffs="10.0.0.2 10.0.0.3 10.1.2.3 10.1.2.4"))
        self.assertTrue(any("cannot read it" in x for x in e), e)


class ForeignObjectTest(unittest.TestCase):
    """One family answers for CoreDNS: other units may not ship its objects."""

    def test_other_kinds_are_swept_not_contract_checked(self):
        addons = {"unit": {"unit_id": "cluster-addons", "kind": "cluster-addons"},
                  "result": {"files": [{"path": "kube-dns.yaml", "content": CONFIGMAP}]}}
        report = gate.check_units([addons])
        self.assertEqual(report["checked"], [])
        self.assertEqual([f["unit_id"] for f in report["findings"]], ["cluster-addons"])
        self.assertIn("one family answers for CoreDNS", report["findings"][0]["error"])

    def test_a_forwarding_zone_from_the_network_unit_is_a_finding(self):
        network = {"unit": {"unit_id": "network", "kind": "network"},
                   "result": {"files": [{"path": "dns.tf", "content": FORWARDING_TF}]}}
        report = gate.check_units([network])
        self.assertTrue(any("google_dns_managed_zone.corp with a forwarding_config" in f["error"]
                            for f in report["findings"]), report)

    def test_a_peering_zone_from_the_network_unit_is_not(self):
        peering = ('resource "google_dns_managed_zone" "peer" {\n  name = "peer"\n'
                   '  dns_name = "shared.internal."\n  visibility = "private"\n'
                   '  peering_config {\n    target_network {\n      network_url = var.peer\n    }\n  }\n}\n'
                   'resource "google_dns_record_set" "r" {\n  name = "a.shared.internal."\n'
                   '  managed_zone = google_dns_managed_zone.peer.name\n  type = "A"\n  rrdatas = ["10.9.9.9"]\n}\n'
                   'resource "google_dns_policy" "p" {\n  name = "p"\n  enable_logging = true\n}\n')
        network = {"unit": {"unit_id": "network", "kind": "network"},
                   "result": {"files": [{"path": "dns.tf", "content": peering}]}}
        e = [f["error"] for f in gate.check_units([network])["findings"]]
        self.assertFalse(any("google_dns_managed_zone.peer" in x for x in e), e)
        self.assertTrue(any("google_dns_policy.p" in x for x in e), e)

    def test_a_recorded_kube_system_namespace_from_tenancy_is_not_swept(self):
        tenancy = {"unit": {"unit_id": "tenancy", "kind": "tenancy"},
                   "result": {"files": [{"path": "ns.yaml",
                                         "content": "apiVersion: v1\nkind: Namespace\nmetadata:\n  name: kube-system\n"}]}}
        self.assertEqual(gate.check_units([tenancy])["findings"], [])
        coredns = {"unit": {"unit_id": "cluster-addons", "kind": "cluster-addons"},
                   "result": {"files": [{"path": "d.yaml",
                                         "content": "apiVersion: apps/v1\nkind: Deployment\nmetadata:\n  name: coredns\n  namespace: kube-system\n"}]}}
        e = [f["error"] for f in gate.check_units([coredns])["findings"]]
        self.assertTrue(any("no unit ships them" in x for x in e), e)

    def test_a_clean_other_unit_has_no_findings(self):
        tenancy = {"unit": {"unit_id": "tenancy", "kind": "tenancy"},
                   "result": {"files": [{"path": "ns.yaml",
                                         "content": "apiVersion: v1\nkind: Namespace\nmetadata:\n  name: acme-shop\n"}]}}
        self.assertEqual(gate.check_units([tenancy]), {"checked": [], "findings": []})


if __name__ == "__main__":
    unittest.main()
