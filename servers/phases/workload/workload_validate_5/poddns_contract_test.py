"""Unit tests for the pod DNS contract (poddns_contract.py).

Shape and closed-list checks over any unit's output; conservation against
the persisted pod_dns_facts for wkld-manifests: the policy rule (unchanged,
None -> ClusterFirst permitted, REQUIRED over a cluster DNS address), the
unit-wide literal sets in both directions (dropped-and-unnamed, invented),
rendered documents read as the manifests unit's output, the skip for a
blob without facts, and the hostNetwork advisory.
"""

import unittest

import yaml

from servers.phases.workload.workload_validate_5 import poddns_contract as contract

EKS_DNS = "172.20.0.10"
CORP = "10.20.0.53"


PATHS = {"Pod": "spec", "CronJob": "spec.jobTemplate.spec.template.spec"}


def pod_doc(kind="Deployment", name="orders", namespace="acme-shop", spec=None):
    pod = {"containers": [{"name": "app", "image": "x"}]}
    pod.update(spec or {})
    if kind == "Pod":
        return {"apiVersion": "v1", "kind": "Pod",
                "metadata": {"name": name, "namespace": namespace}, "spec": pod}
    if kind == "CronJob":
        return {"apiVersion": "batch/v1", "kind": "CronJob",
                "metadata": {"name": name, "namespace": namespace},
                "spec": {"jobTemplate": {"spec": {"template": {"spec": pod}}}}}
    api = "batch/v1" if kind == "Job" else "apps/v1"
    return {"apiVersion": api, "kind": kind,
            "metadata": {"name": name, "namespace": namespace},
            "spec": {"template": {"spec": pod}}}


def fact(kind="Deployment", name="orders", namespace="acme-shop", **fields):
    base = {"label": name, "kind": kind, "namespace": namespace, "name": name,
            "node_path": "spec.template.spec", "dns_policy": None, "host_network": False,
            "nameservers": [], "searches": [], "options": [], "host_aliases": []}
    base.update(fields)
    return base


def entry(docs, facts=None, family="wkld-manifests", tradeoffs="", assumptions=(),
          open_questions=(), with_facts_key=True):
    inputs = {"documents": []}
    if with_facts_key:
        inputs["pod_dns_facts"] = list(facts or [])
    return {"unit": {"unit_id": family, "family": family, "inputs": inputs},
            "result": {"files": [{"path": "app.yaml",
                                  "content": yaml.safe_dump_all(docs, sort_keys=False)}],
                       "tradeoffs": tradeoffs, "assumptions": list(assumptions),
                       "open_questions": list(open_questions)}}


def errors(report):
    return [f["error"] for f in report["findings"]]


class ShapeAndClosedListTest(unittest.TestCase):

    def test_a_plain_deployment_is_clean(self):
        report = contract.check_component([entry([pod_doc()], facts=[])])
        self.assertEqual(report["findings"], [])
        self.assertEqual(report["checked"], 0)  # no DNS key at all: no node

    def test_forbidden_nameservers_and_ec2_suffixes(self):
        doc = pod_doc(spec={"dnsConfig": {"nameservers": [EKS_DNS, "169.254.169.253"],
                                           "searches": ["ec2.internal", "us-east-1.compute.internal", "svc.cluster.local"]}})
        report = contract.check_component([entry([doc], with_facts_key=False)])
        text = "\n".join(errors(report))
        self.assertIn("172.20.0.10 is the EKS default cluster DNS address", text)
        self.assertIn("169.254.169.253 is the AWS VPC resolver", text)
        self.assertIn("ec2.internal is an EC2 suffix", text)
        self.assertIn("us-east-1.compute.internal is an EC2 suffix", text)
        self.assertNotIn("svc.cluster.local", text)

    def test_gke_node_addresses_are_never_pinned(self):
        for address in ("169.254.169.254", "169.254.20.10"):
            doc = pod_doc(spec={"dnsConfig": {"nameservers": [address]}})
            report = contract.check_component([entry([doc], with_facts_key=False)])
            self.assertTrue(any("never pinned" in e for e in errors(report)), address)

    def test_schema_limits(self):
        fields = {"dns_policy": "Sometimes", "nameservers": ["1.1.1.1", "2.2.2.2", "3.3.3.3", "4.4.4.4", "nope"],
                  "searches": ["a"] * 33, "options": [{"name": "", "value": None}], "host_network": False,
                  "host_aliases": []}
        text = "\n".join(contract.schema_errors(fields))
        self.assertIn("not one of", text)
        self.assertIn("5 nameservers", text)
        self.assertIn("'nope' is not an IP address", text)
        self.assertIn("33 search domains", text)
        self.assertIn("option has no name", text)
        self.assertIn("no resolver at all",
                      "\n".join(contract.schema_errors({"dns_policy": "None", "nameservers": []})))

    def test_shape_checks_run_over_other_families_too(self):
        doc = pod_doc(spec={"dnsConfig": {"nameservers": [EKS_DNS]}})
        report = contract.check_component([entry([doc], family="wkld-storage", with_facts_key=False)])
        self.assertEqual(len(report["findings"]), 1)
        self.assertEqual(report["skipped"], [])  # conservation is not that family's

    def test_a_pod_template_in_an_unknown_kind_is_read(self):
        rollout = {"apiVersion": "argoproj.io/v1alpha1", "kind": "Rollout",
                   "metadata": {"name": "web"},
                   "spec": {"template": {"spec": {"containers": [{"name": "w"}],
                                                  "dnsConfig": {"nameservers": [EKS_DNS]}}}}}
        report = contract.check_component([entry([rollout], with_facts_key=False)])
        self.assertEqual(report["checked"], 1)
        self.assertIn("Rollout/web", report["findings"][0]["subject"])

    def test_configmap_data_and_policy_objects_are_not_pod_specs(self):
        cm = {"apiVersion": "v1", "kind": "ConfigMap", "metadata": {"name": "c"},
              "data": {"dnsPolicy": "whatever", "dnsConfig": "x"}}
        psp = {"apiVersion": "policy/v1beta1", "kind": "PodSecurityPolicy",
               "metadata": {"name": "privileged"}, "spec": {"hostNetwork": True, "privileged": True}}
        kyverno = {"apiVersion": "kyverno.io/v1", "kind": "ClusterPolicy", "metadata": {"name": "dns"},
                   "spec": {"rules": [{"validate": {"pattern": {"spec": {"dnsPolicy": "!None"}}}}]}}
        report = contract.check_component([entry([cm, psp, kyverno], facts=[])])
        self.assertEqual((report["checked"], report["findings"], report["advisory"]), (0, [], []))


class PolicyRuleTest(unittest.TestCase):

    def test_none_kept_over_a_cluster_dns_address_is_a_finding(self):
        facts = [fact(kind="Job", name="mailer", dns_policy="None", nameservers=[EKS_DNS, CORP])]
        shipped = pod_doc("Job", "mailer", spec={"dnsPolicy": "None", "dnsConfig": {"nameservers": [CORP]}})
        report = contract.check_component([entry([shipped], facts, tradeoffs=f"dropped {EKS_DNS}")])
        self.assertTrue(any("resolves no cluster name" in e for e in errors(report)), errors(report))

    def test_none_to_clusterfirst_with_the_rest_named_is_clean(self):
        facts = [fact(kind="Job", name="mailer", dns_policy="None", nameservers=[EKS_DNS, CORP])]
        shipped = pod_doc("Job", "mailer", spec={"dnsPolicy": "ClusterFirst"})
        report = contract.check_component([entry(
            [shipped], facts, tradeoffs=f"{EKS_DNS} dropped: cluster DNS is the node's on GKE",
            open_questions=[f"resolver {CORP} belongs in the cluster-dns unit"])])
        self.assertEqual(report["findings"], [])

    def test_any_other_policy_change_is_a_finding(self):
        facts = [fact(dns_policy="ClusterFirstWithHostNet", host_network=True)]
        shipped = pod_doc(spec={"hostNetwork": True, "dnsPolicy": "ClusterFirst"})
        report = contract.check_component([entry([shipped], facts)])
        self.assertTrue(any("changed from ClusterFirstWithHostNet to ClusterFirst" in e
                            for e in errors(report)), errors(report))
        # absent and ClusterFirst are the same policy
        facts = [fact(dns_policy="ClusterFirst")]
        report = contract.check_component([entry([pod_doc(spec={"dnsConfig": {}})], facts)])
        self.assertEqual(report["findings"], [])

    def test_a_dropped_pod_document_is_a_finding(self):
        facts = [fact(name="orders", dns_policy="Default")]
        report = contract.check_component([entry([pod_doc(name="other", spec={"dnsPolicy": "Default"})], facts)])
        self.assertTrue(any("not in the shipped output" in e for e in errors(report)))

    def test_namespace_lost_in_a_render_still_matches(self):
        facts = [fact(dns_policy="Default", namespace="acme-shop")]
        shipped = pod_doc(namespace=None, spec={"dnsPolicy": "Default"})
        del shipped["metadata"]["namespace"]
        self.assertEqual(contract.check_component([entry([shipped], facts)])["findings"], [])

    def test_a_namespace_less_twin_does_not_hide_the_exact_match(self):
        facts = [fact(dns_policy="Default", namespace="b")]
        twin = pod_doc(spec={"dnsPolicy": "ClusterFirst"})
        del twin["metadata"]["namespace"]
        right = pod_doc(namespace="b", spec={"dnsPolicy": "Default"})
        self.assertEqual(contract.check_component([entry([twin, right], facts)])["findings"], [])

    def test_a_namespace_move_is_named_as_such(self):
        facts = [fact(dns_policy="Default", namespace="acme-shop")]
        shipped = pod_doc(namespace="elsewhere", spec={"dnsPolicy": "Default"})
        [finding] = contract.check_component([entry([shipped], facts)])["findings"]
        self.assertIn("shipped in namespace elsewhere", finding["error"])


class ConservationTest(unittest.TestCase):

    def test_dropped_literals_must_be_named(self):
        facts = [fact(nameservers=[CORP], searches=["corp.acme.internal", "ec2.internal"],
                      options=[{"name": "ndots", "value": "2"}],
                      host_aliases=[{"ip": "10.0.5.5", "hostnames": ["db.internal"]}])]
        shipped = pod_doc(spec={"dnsConfig": {"searches": ["corp.acme.internal"]}})
        report = contract.check_component([entry([shipped], facts, tradeoffs="ec2.internal dropped")])
        subjects = sorted(f["subject"] for f in report["findings"])
        self.assertEqual(subjects, ["host alias address 10.0.5.5", "host alias hostname db.internal",
                                    "nameserver 10.20.0.53", "option ndots=2"])
        named = contract.check_component([entry(
            [shipped], facts,
            tradeoffs="ec2.internal dropped; 10.20.0.53 dropped; ndots dropped",
            open_questions=["hostAliases 10.0.5.5 db.internal: does it exist on GCP?"])])
        self.assertEqual(named["findings"], [])

    def test_naming_is_label_aligned(self):
        facts = [fact(nameservers=["10.20.0.5"])]
        shipped = pod_doc(spec={"dnsPolicy": "ClusterFirst"})
        self.assertEqual(len(contract.check_component(
            [entry([shipped], facts, tradeoffs="dropped 10.20.0.53")])["findings"]), 1)
        self.assertEqual(contract.check_component(
            [entry([shipped], facts, tradeoffs="dropped 10.20.0.5.")])["findings"], [])
        self.assertEqual(contract.check_component(
            [entry([shipped], facts, tradeoffs="nameserver:10.20.0.5 dropped")])["findings"], [])
        facts = [fact(host_aliases=[{"ip": "10.0.5.5", "hostnames": ["db"]}])]
        report = contract.check_component([entry([shipped], facts, open_questions=["10.0.5.5 db.internal?"])])
        self.assertEqual([f["subject"] for f in report["findings"]], ["host alias hostname db"])

    def test_invented_literals_are_findings(self):
        facts = [fact(searches=["corp.acme.internal"])]
        shipped = pod_doc(spec={"dnsConfig": {"nameservers": ["8.8.8.8"],
                                               "searches": ["corp.acme.internal", "gcp.internal"],
                                               "options": [{"name": "ndots", "value": "1"}]},
                                 "hostAliases": [{"ip": "10.9.9.9", "hostnames": ["h"]}]})
        report = contract.check_component([entry([shipped], facts)])
        subjects = sorted(f["subject"] for f in report["findings"])
        self.assertEqual(subjects, ["host alias address 10.9.9.9", "host alias hostname h",
                                    "nameserver 8.8.8.8", "option ndots=1", "search domain gcp.internal"])

    def test_an_option_value_change_is_not_kept_but_not_invented(self):
        facts = [fact(options=[{"name": "ndots", "value": "2"}])]
        shipped = pod_doc(spec={"dnsConfig": {"options": [{"name": "ndots", "value": "5"}]}})
        report = contract.check_component([entry([shipped], facts)])
        self.assertEqual([f["subject"] for f in report["findings"]], ["option ndots=2"])
        self.assertEqual(contract.check_component(
            [entry([shipped], facts, assumptions=["ndots raised to 5"])])["findings"], [])

    def test_rendered_documents_are_the_manifests_units_output(self):
        facts = [fact(name="web", nameservers=[CORP])]
        rendered = [("workloads/c/charts/web", pod_doc(name="web", spec={"dnsConfig": {"nameservers": [CORP]}}))]
        clean = contract.check_component([entry([], facts)], rendered_docs=rendered)
        self.assertEqual(clean["findings"], [])
        self.assertEqual(clean["checked"], 1)
        missing = contract.check_component([entry([], facts)])
        self.assertEqual(len(missing["findings"]), 2)  # document gone + resolver unnamed

    def test_a_blob_without_facts_is_skipped_visibly(self):
        shipped = pod_doc(spec={"dnsConfig": {"nameservers": [CORP]}})
        report = contract.check_component([entry([shipped], with_facts_key=False)])
        self.assertEqual(report["findings"], [])
        self.assertEqual(len(report["skipped"]), 1)
        self.assertIn("before the field", report["skipped"][0]["note"])

    def test_host_network_under_clusterfirst_is_advisory_only(self):
        facts = [fact(host_network=True)]
        report = contract.check_component([entry([pod_doc(spec={"hostNetwork": True})], facts)])
        self.assertEqual(report["findings"], [])
        self.assertEqual(len(report["advisory"]), 1)
        self.assertIn("ClusterFirstWithHostNet", report["advisory"][0]["note"])

    def test_two_pod_templates_in_one_document_match_by_path(self):
        cr = {"apiVersion": "x/v1", "kind": "Pair", "metadata": {"name": "p"},
              "spec": {"a": {"template": {"spec": {"dnsPolicy": "ClusterFirst", "containers": []}}},
                       "b": {"template": {"spec": {"dnsPolicy": "None", "containers": [],
                                                   "dnsConfig": {"nameservers": [CORP]}}}}}}
        facts = [fact(kind="Pair", name="p", node_path="spec.a.template.spec", dns_policy="ClusterFirst"),
                 fact(kind="Pair", name="p", node_path="spec.b.template.spec", dns_policy="None", nameservers=[CORP])]
        self.assertEqual(contract.check_component([entry([cr], facts)])["findings"], [])
        # The first template loses its only DNS field: the second is still found at its path.
        del cr["spec"]["a"]["template"]["spec"]["dnsPolicy"]
        report = contract.check_component([entry([cr], facts)])
        self.assertEqual(report["findings"], [])

    def test_removing_the_last_dns_field_is_the_ordinary_clean_translation(self):
        facts = [fact(nameservers=["10.100.0.10"])]
        shipped = pod_doc()  # dnsConfig gone entirely
        report = contract.check_component([entry([shipped], facts, tradeoffs="10.100.0.10 dropped")])
        self.assertEqual(report["findings"], [])
        facts = [fact(dns_policy="ClusterFirst")]  # the written default, removed
        self.assertEqual(contract.check_component([entry([pod_doc()], facts)])["findings"], [])

    def test_a_cronjob_matches_at_its_deeper_path(self):
        facts = [fact(kind="CronJob", name="nightly", node_path=PATHS["CronJob"], dns_policy="Default")]
        shipped = pod_doc("CronJob", "nightly", spec={"dnsPolicy": "Default"})
        self.assertEqual(contract.check_component([entry([shipped], facts)])["findings"], [])
        shipped = pod_doc("CronJob", "nightly", spec={"dnsPolicy": "ClusterFirst"})
        self.assertEqual(len(contract.check_component([entry([shipped], facts)])["findings"]), 1)

    def test_host_network_none_must_become_clusterfirstwithhostnet(self):
        facts = [fact(kind="DaemonSet", name="agent", host_network=True, dns_policy="None",
                      nameservers=[EKS_DNS])]
        good = pod_doc("DaemonSet", "agent", spec={"hostNetwork": True, "dnsPolicy": "ClusterFirstWithHostNet"})
        self.assertEqual(contract.check_component(
            [entry([good], facts, tradeoffs=f"{EKS_DNS} dropped")])["findings"], [])
        for policy, extra in (("None", {"dnsConfig": {"nameservers": [CORP]}}), ("ClusterFirst", {})):
            kept = pod_doc("DaemonSet", "agent", spec={"hostNetwork": True, "dnsPolicy": policy, **extra})
            report = contract.check_component([entry([kept], facts, tradeoffs=f"{EKS_DNS} dropped", open_questions=[CORP])])
            self.assertTrue(any("make it dnsPolicy ClusterFirstWithHostNet" in e for e in errors(report)), (policy, errors(report)))
        # Without host networking the required target is ClusterFirst.
        facts = [fact(dns_policy="None", nameservers=[EKS_DNS])]
        shipped = pod_doc(spec={"dnsPolicy": "ClusterFirstWithHostNet"})
        self.assertTrue(any("make it dnsPolicy ClusterFirst (" in e for e in errors(
            contract.check_component([entry([shipped], facts, tradeoffs=f"{EKS_DNS} dropped")]))))
        # And a None pod with no cluster DNS address may only flip to ClusterFirst off host networking...
        facts = [fact(dns_policy="None", nameservers=[CORP])]
        shipped = pod_doc(spec={"dnsPolicy": "ClusterFirstWithHostNet"})
        self.assertTrue(any("changed from None to ClusterFirstWithHostNet" in e for e in errors(
            contract.check_component([entry([shipped], facts, open_questions=[CORP])]))))
        # ...while on host networking with only the VPC resolver either policy is accepted.
        facts = [fact(kind="DaemonSet", name="agent", host_network=True, dns_policy="None",
                      nameservers=["169.254.169.253"])]
        for policy in ("ClusterFirst", "ClusterFirstWithHostNet"):
            shipped = pod_doc("DaemonSet", "agent", spec={"hostNetwork": True, "dnsPolicy": policy})
            self.assertEqual(contract.check_component(
                [entry([shipped], facts, tradeoffs="169.254.169.253 dropped")])["findings"], [], policy)

    def test_a_nameless_bundle_is_held_by_its_literals_only(self):
        facts = [fact(kind="List", name=None, label="k8s/bundle.yaml#0", node_path="items.0.spec",
                      searches=["corp.acme.internal"])]
        split = pod_doc("Pod", "p", spec={"dnsConfig": {"searches": ["corp.acme.internal"]}})
        self.assertEqual(contract.check_component([entry([split], facts)])["findings"], [])
        dropped = pod_doc("Pod", "p")
        report = contract.check_component([entry([dropped], facts)])
        self.assertEqual([f["subject"] for f in report["findings"]], ["search domain corp.acme.internal"])

    def test_an_unread_render_source_suspends_the_invention_check(self):
        e = entry([pod_doc(spec={"dnsConfig": {"searches": ["corp.acme.internal"]}})], facts=[])
        e["unit"]["inputs"]["pod_dns_unread"] = ["chart charts/web could not be rendered (x)"]
        report = contract.check_component([e])
        self.assertEqual(report["findings"], [])
        self.assertEqual(report["skipped"], [])
        self.assertTrue(any("facts are incomplete" in s["note"] for s in report["incomplete"]))
        # The closed list still applies.
        e = entry([pod_doc(spec={"dnsConfig": {"nameservers": [EKS_DNS]}})], facts=[])
        e["unit"]["inputs"]["pod_dns_unread"] = ["chart charts/web could not be rendered (x)"]
        self.assertEqual(len(contract.check_component([e])["findings"]), 1)

    def test_two_facts_with_one_identity_each_accept_a_matching_twin(self):
        facts = [fact(dns_policy="Default"), fact(dns_policy="ClusterFirst")]
        shipped = [pod_doc(spec={"dnsPolicy": "ClusterFirst"}), pod_doc(spec={"dnsPolicy": "Default"})]
        self.assertEqual(contract.check_component([entry(shipped, facts)])["findings"], [])
        only_one = [pod_doc(spec={"dnsPolicy": "ClusterFirst"})]
        self.assertEqual(len(contract.check_component([entry(only_one, facts)])["findings"]), 1)

    def test_finding_texts_keep_their_remedy_within_the_reply_cut(self):
        cases = []
        facts = [fact(dns_policy="Default", namespace="acme-shop")]
        cases.append(contract.check_component(
            [entry([pod_doc(namespace="elsewhere", spec={"dnsPolicy": "Default"})], facts)]))
        facts = [fact(kind="DaemonSet", name="agent", host_network=True, dns_policy="None", nameservers=[EKS_DNS])]
        cases.append(contract.check_component([entry(
            [pod_doc("DaemonSet", "agent", spec={"hostNetwork": True, "dnsPolicy": "ClusterFirst"})],
            facts, tradeoffs=f"{EKS_DNS} dropped")]))
        plan_unit = {"unit_id": "wkld-manifests", "inputs": {"pod_dns_facts": [fact(nameservers=[CORP, "10.9.9.9"])]}}
        cases.append(contract.check_component(
            [entry([pod_doc(spec={"dnsConfig": {"nameservers": [CORP]}})], [fact(nameservers=[CORP])])],
            plan_units=[plan_unit]))
        texts = [f["error"] for report in cases for f in report["findings"]]
        self.assertEqual(len(texts), 3)
        for text in texts:
            self.assertLess(len(text), 240, text)
        self.assertIn("retranslate", texts[2][:60])
        self.assertIn("make it dnsPolicy ClusterFirstWithHostNet", texts[1][:60])

    def test_facts_drift_between_plan_and_blob_is_a_finding(self):
        blob_facts = [fact(nameservers=[CORP])]
        plan_unit = {"unit_id": "wkld-manifests", "inputs": {"pod_dns_facts": [fact(nameservers=[CORP, "10.9.9.9"])]}}
        shipped = pod_doc(spec={"dnsConfig": {"nameservers": [CORP]}})
        report = contract.check_component([entry([shipped], blob_facts)], plan_units=[plan_unit])
        self.assertEqual([f["subject"] for f in report["findings"]], ["pod_dns_facts"])
        self.assertIn("retranslate", report["findings"][0]["error"])
        same = {"unit_id": "wkld-manifests", "inputs": {"pod_dns_facts": list(blob_facts)}}
        self.assertEqual(contract.check_component([entry([shipped], blob_facts)], plan_units=[same])["findings"], [])

    def test_carrier_routed_files_are_read_once_through_the_render(self):
        e = entry([], facts=[fact(name="web", dns_policy="Default")])
        e["unit"]["inputs"]["documents"] = [{"path": "chart", "doc_index": 0,
                                             "rendered_from": {"type": "helm", "chart_path": "chart"}}]
        e["result"]["files"] = [{"path": "chart/templates/deploy.yaml",
                                 "content": yaml.safe_dump(pod_doc(name="web", spec={"dnsPolicy": "Default"}))}]
        rendered = [("workloads/c/chart", pod_doc(name="web", spec={"dnsPolicy": "Default"}))]
        report = contract.check_component([e], rendered_docs=rendered)
        self.assertEqual((report["checked"], report["findings"]), (1, []))


if __name__ == "__main__":
    unittest.main()
