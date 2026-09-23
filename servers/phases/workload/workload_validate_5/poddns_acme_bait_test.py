"""The acme-cafe estate's pod DNS bait, pinned end to end at unit level.

The e2e-qa harness drives the platform persona only, so the developer
journey that would exercise this bait does not run there yet. This test is
the contract for it meanwhile: the three manifests below are copies of the
estate's (acme-eks-estate, "Add pod-level DNS settings as workload bait")
trimmed to what the planner's facts reader and the contract read — the pod
DNS fields, the kind, name and namespace, the container list; comments, the
frontend's env and resources, its Service and the agent's volumes are left
out. If the estate's DNS fields change, this file must change with them
(the pinned values are also in tests/e2e-qa/fixtures/acme-cafe/expected.yaml).
The planner must record the facts the estate README
promises, the translation the README describes must pass the contract,
and the two tempting wrong answers — copy as written, or drop the dead
address and keep dnsPolicy None — must fail it with the finding that names
the remedy.
"""

import os
import shutil
import tempfile
import unittest

import yaml

from servers.phases.workload.workload_plan_2 import planner
from servers.phases.workload.workload_validate_5 import poddns_contract as contract

EXPORTS = {"gateway": None,
           "cluster": {"name": "gke-1", "type": "standard", "location": "us-central1"},
           "node_shapes": None, "storage_class_menu": None, "gsa_bindings": None,
           "artifact_registry": {"destinations": [], "image_map": {}},
           "staging_bucket": None, "generated_at": "T0",
           "generations": {"discovery": 1, "translation": 1, "deployment": 1}}

MENU_SYNC = """\
apiVersion: batch/v1
kind: CronJob
metadata:
  name: menu-sync
  namespace: acme-shop
spec:
  schedule: "15 4 * * *"
  concurrencyPolicy: Forbid
  jobTemplate:
    spec:
      template:
        metadata:
          labels:
            app: menu-sync
        spec:
          restartPolicy: OnFailure
          dnsPolicy: None
          dnsConfig:
            nameservers:
              - 172.20.0.10
              - 10.20.0.53
            searches:
              - acme-shop.svc.cluster.local
              - corp.acme.internal
          containers:
            - name: menu-sync
              image: 869935070097.dkr.ecr.us-east-1.amazonaws.com/acme/orders:2.2.0
              command: ["/orders", "sync-menu"]
              env:
                - name: MENU_CATALOG_URL
                  value: https://menu-catalog.corp.acme.internal/v1/menu
                - name: ORDERS_URL
                  value: http://orders:9000
"""

FRONTEND = """\
apiVersion: apps/v1
kind: Deployment
metadata:
  name: frontend
  namespace: acme-shop
spec:
  replicas: 3
  selector:
    matchLabels:
      app: frontend
  template:
    metadata:
      labels:
        app: frontend
    spec:
      dnsConfig:
        searches:
          - corp.acme.internal
        options:
          - name: ndots
            value: "2"
      containers:
        - name: frontend
          image: 869935070097.dkr.ecr.us-east-1.amazonaws.com/acme/frontend:1.5.0
          ports:
            - containerPort: 8080
"""

NODE_AGENT = """\
apiVersion: apps/v1
kind: DaemonSet
metadata:
  name: node-agent
  namespace: acme-shop
spec:
  selector:
    matchLabels:
      app: node-agent
  template:
    metadata:
      labels:
        app: node-agent
    spec:
      hostNetwork: true
      hostPID: true
      dnsPolicy: ClusterFirstWithHostNet
      containers:
        - name: node-agent
          image: 869935070097.dkr.ecr.us-east-1.amazonaws.com/acme/node-agent:0.9.1
          securityContext:
            privileged: true
"""

# The translation the estate README describes, as a worker would ship it.
MENU_SYNC_TRANSLATED = MENU_SYNC.replace(
    "          dnsPolicy: None\n"
    "          dnsConfig:\n"
    "            nameservers:\n"
    "              - 172.20.0.10\n"
    "              - 10.20.0.53\n"
    "            searches:\n",
    "          dnsPolicy: ClusterFirst\n"
    "          dnsConfig:\n"
    "            searches:\n")
GOOD_PROSE = {
    "tradeoffs": ("menu-sync: dnsPolicy None -> ClusterFirst; 172.20.0.10 is the EKS cluster "
                  "DNS Service IP and does not exist on GKE, where the node resolves cluster "
                  "names. node-agent: on Dataplane V2 with NodeLocal DNSCache a hostNetwork "
                  "pod under ClusterFirstWithHostNet may not reach the cluster DNS backends."),
    "assumptions": ["The cluster domain stays cluster.local (landing-zone default)."],
    "open_questions": [
        "menu-sync reached the on-prem resolver 10.20.0.53 directly; on GKE that belongs "
        "in the cluster-dns unit (upstreamNameservers or the corp.acme.internal stub domain).",
        "Confirm the cluster-dns unit carries corp.acme.internal (frontend and menu-sync "
        "search it)."],
}


def _facts_of(plan):
    unit = next(u for u in plan["units"] if u["unit_id"] == "wkld-manifests")
    return unit, unit["inputs"]["pod_dns_facts"]


class AcmePodDnsBaitTest(unittest.TestCase):

    def setUp(self):
        self.root = tempfile.mkdtemp(prefix="acme_poddns_")
        self.addCleanup(shutil.rmtree, self.root, True)
        os.makedirs(os.path.join(self.root, "k8s"))
        for name, text in (("menu-sync-cronjob.yaml", MENU_SYNC),
                           ("frontend.yaml", FRONTEND),
                           ("node-agent-daemonset.yaml", NODE_AGENT)):
            with open(os.path.join(self.root, "k8s", name), "w") as f:
                f.write(text)

    def plan(self):
        def no_render(source_root, rel_dir):
            raise AssertionError(f"unexpected render of {rel_dir}")
        return planner.build_workload_plan(
            "acme-shop", {"included": ["k8s/**"], "excluded": []}, self.root, EXPORTS,
            renderers={"helm": no_render, "kustomize": no_render})

    def shipped(self, menu_sync, prose):
        unit, _ = _facts_of(self.plan())
        files = [{"path": "k8s/menu-sync-cronjob.yaml", "content": menu_sync},
                 {"path": "k8s/frontend.yaml", "content": FRONTEND},
                 {"path": "k8s/node-agent-daemonset.yaml", "content": NODE_AGENT}]
        entry = {"unit": {**unit, "status": "done"}, "result": {"files": files, **prose}}
        return contract.check_component([entry], plan_units=[unit])

    def test_the_planner_records_the_three_baits(self):
        unit, facts = _facts_of(self.plan())
        by_name = {f["name"]: f for f in facts}
        self.assertEqual(sorted(by_name), ["frontend", "menu-sync", "node-agent"])
        sync = by_name["menu-sync"]
        self.assertEqual((sync["kind"], sync["node_path"], sync["dns_policy"]),
                         ("CronJob", "spec.jobTemplate.spec.template.spec", "None"))
        self.assertEqual(sync["nameservers"], ["172.20.0.10", "10.20.0.53"])
        self.assertEqual(sync["searches"], ["acme-shop.svc.cluster.local", "corp.acme.internal"])
        front = by_name["frontend"]
        self.assertEqual(front["options"], [{"name": "ndots", "value": "2"}])
        self.assertEqual(front["searches"], ["corp.acme.internal"])
        agent = by_name["node-agent"]
        self.assertTrue(agent["host_network"])
        self.assertEqual(agent["dns_policy"], "ClusterFirstWithHostNet")
        self.assertEqual(unit["inputs"]["pod_dns_unread"], [])
        self.assertEqual(sum(1 for n in unit["notes"] if n.startswith("Pod DNS facts of")), 3)

    def test_the_readme_translation_passes(self):
        report = self.shipped(MENU_SYNC_TRANSLATED, GOOD_PROSE)
        self.assertEqual(report["findings"], [], report)
        self.assertEqual(report["checked"], 3)
        self.assertEqual(report["advisory"], [])

    def test_copying_the_estate_as_written_fails_on_the_dead_address(self):
        report = self.shipped(MENU_SYNC, GOOD_PROSE)
        texts = [f["error"] for f in report["findings"]]
        self.assertTrue(any("172.20.0.10 is the EKS default cluster DNS address" in t for t in texts), texts)
        self.assertTrue(any("make it dnsPolicy ClusterFirst" in t for t in texts), texts)

    def test_dropping_the_address_but_keeping_none_fails_on_the_policy(self):
        kept_none = MENU_SYNC.replace("              - 172.20.0.10\n", "")
        report = self.shipped(kept_none, GOOD_PROSE)
        texts = [f["error"] for f in report["findings"]]
        self.assertEqual(len(texts), 1, texts)
        self.assertIn("make it dnsPolicy ClusterFirst (it ships None)", texts[0])

    def test_silence_about_the_on_prem_resolver_fails(self):
        prose = {**GOOD_PROSE, "open_questions": [GOOD_PROSE["open_questions"][1]]}
        report = self.shipped(MENU_SYNC_TRANSLATED, prose)
        self.assertEqual([f["subject"] for f in report["findings"]], ["nameserver 10.20.0.53"])

    def test_the_embedded_manifests_parse(self):
        # Guard against a trimmed copy that drifted into invalid YAML.
        for text in (MENU_SYNC, FRONTEND, NODE_AGENT, MENU_SYNC_TRANSLATED):
            doc = yaml.safe_load(text)
            self.assertIn(doc["kind"], ("CronJob", "Deployment", "DaemonSet"))


if __name__ == "__main__":
    unittest.main()
