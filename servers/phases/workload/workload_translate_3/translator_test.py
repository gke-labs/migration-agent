"""Unit tests for the workload worker contract (translator.py).

Pure where possible: the re-render gate runs with injected renderers (no
helm/kubectl binaries), and the missing-binary arm patches subprocess.run.
Covers each envelope rejection singly, the one-retry loop, the chart
re-render pass/fail arms (a raw Go template must NEVER reach the YAML
loader), the kustomize arm, and RenderToolMissing.
"""

import asyncio
import json
import os
import shutil
import subprocess
import tempfile
import unittest
from unittest.mock import patch

from servers.phases.workload.workload_translate_3 import translator

GOOD_DOC = "apiVersion: v1\nkind: Service\nmetadata:\n  name: web\n"
GO_TEMPLATE = "apiVersion: apps/v1\nkind: Deployment\nmetadata:\n  name: {{ .Values.name }}\n"

PLAIN_UNIT = {"unit_id": "wkld-manifests", "family": "wkld-manifests",
              "inputs": {"documents": [
                  {"path": "k8s/app.yaml", "doc_index": 0,
                   "rendered_from": None}]}}

CHART_UNIT = {"unit_id": "wkld-manifests", "family": "wkld-manifests",
              "inputs": {"documents": [
                  {"path": "chart", "doc_index": 0,
                   "rendered_from": {"type": "helm", "chart_path": "chart"}}]}}

KUSTOMIZE_UNIT = {"unit_id": "wkld-manifests", "family": "wkld-manifests",
                  "inputs": {"documents": [
                      {"path": "overlays/prod", "doc_index": 0,
                       "rendered_from": {"type": "kustomize",
                                         "kustomize_dir": "overlays/prod"}}]}}


def result_with(files, tradeoffs="honest tradeoffs", **extra):
    return {"files": files, "tradeoffs": tradeoffs,
            "assumptions": [], "open_questions": [], **extra}


class EnvelopeValidationTest(unittest.TestCase):
    """Each rejection singly, no render context needed."""

    def test_valid_plain_result_passes(self):
        result = result_with([{"path": "svc.yaml", "content": GOOD_DOC}])
        self.assertEqual(
            translator.validate_workload_translation(result, PLAIN_UNIT), "")

    def test_tf_path_is_a_named_rejection(self):
        result = result_with([{"path": "main.tf", "content": "resource {}\n"}])
        error = translator.validate_workload_translation(result, PLAIN_UNIT)
        self.assertIn("Terraform", error)
        self.assertIn("never .tf", error)

    def test_path_escape_is_rejected(self):
        for path in ("../evil.yaml", "/abs.yaml", "a/../../b.yaml"):
            with self.subTest(path=path):
                result = result_with([{"path": path, "content": GOOD_DOC}])
                error = translator.validate_workload_translation(
                    result, PLAIN_UNIT)
                self.assertIn("unit-relative", error)

    def test_empty_tradeoffs_is_rejected(self):
        result = result_with([{"path": "svc.yaml", "content": GOOD_DOC}],
                             tradeoffs="  ")
        error = translator.validate_workload_translation(result, PLAIN_UNIT)
        self.assertIn("tradeoffs", error)

    def test_bad_yaml_is_rejected(self):
        result = result_with([{"path": "svc.yaml", "content": "a: [b\n"}])
        error = translator.validate_workload_translation(result, PLAIN_UNIT)
        self.assertIn("svc.yaml", error)

    def test_non_yaml_extension_is_rejected(self):
        result = result_with([{"path": "notes.md", "content": "# hi\n"}])
        error = translator.validate_workload_translation(result, PLAIN_UNIT)
        self.assertIn("must end in .yaml or .yml", error)

    def test_empty_files_list_is_rejected(self):
        error = translator.validate_workload_translation(
            result_with([]), PLAIN_UNIT)
        self.assertIn("non-empty", error)

    def test_missing_metadata_name_is_rejected(self):
        result = result_with([{
            "path": "svc.yaml",
            "content": "apiVersion: v1\nkind: Service\nmetadata: {}\n"}])
        error = translator.validate_workload_translation(result, PLAIN_UNIT)
        self.assertIn("metadata.name", error)

    def test_non_list_assumptions_is_rejected(self):
        result = result_with([{"path": "svc.yaml", "content": GOOD_DOC}])
        result["assumptions"] = "not a list"
        error = translator.validate_workload_translation(result, PLAIN_UNIT)
        self.assertIn("assumptions", error)


class ChartGateTest(unittest.TestCase):
    """The re-render gate with injected renderers — no binaries."""

    def setUp(self):
        self.root = tempfile.mkdtemp(prefix="wkld_tr_test_")
        self.addCleanup(shutil.rmtree, self.root, True)
        chart = os.path.join(self.root, "chart")
        os.makedirs(os.path.join(chart, "templates"))
        for rel, content in (
                ("Chart.yaml", "name: web\nversion: 1.0.0\n"),
                ("values.yaml", "name: web\n"),
                ("templates/deployment.yaml", GO_TEMPLATE)):
            with open(os.path.join(chart, rel), "w") as f:
                f.write(content)

    def ctx(self, helm=None, kustomize=None):
        renderers = {}
        if helm:
            renderers["helm"] = helm
        if kustomize:
            renderers["kustomize"] = kustomize
        return {"source_root": self.root, "renderers": renderers}

    def test_chart_strategy_passes_via_render_never_via_loader(self):
        """The edited chart carries a raw Go template. Only the injected
        renderer's output is parsed: a pass here proves the template never
        reached the YAML loader (it is not valid YAML)."""
        seen = {}

        def fake_helm(root, rel_dir):
            with open(os.path.join(root, rel_dir, "values.yaml")) as f:
                seen["overlaid"] = f.read()
            return GOOD_DOC, None

        result = result_with([
            {"path": "chart/values.yaml", "content": "name: edited\n"},
            {"path": "chart/templates/deployment.yaml", "content": GO_TEMPLATE},
        ])
        error = translator.validate_workload_translation(
            result, CHART_UNIT, self.ctx(helm=fake_helm))
        self.assertEqual(error, "")
        self.assertEqual(seen["overlaid"], "name: edited\n")

    def test_broken_edited_template_fails_the_render_gate(self):
        def failing_helm(root, rel_dir):
            return None, "template: parse error at deployment.yaml:4"

        result = result_with(
            [{"path": "chart/templates/deployment.yaml",
              "content": "{{ broken"}])
        error = translator.validate_workload_translation(
            result, CHART_UNIT, self.ctx(helm=failing_helm))
        self.assertIn("does not render deterministically", error)
        self.assertIn("parse error", error)

    def test_render_output_failing_structure_is_a_rejection(self):
        def helm_bad_output(root, rel_dir):
            return "kind: Service\nmetadata:\n  name: x\n", None  # no apiVersion

        result = result_with(
            [{"path": "chart/values.yaml", "content": "name: y\n"}])
        error = translator.validate_workload_translation(
            result, CHART_UNIT, self.ctx(helm=helm_bad_output))
        self.assertIn("structural gate", error)

    def test_bare_carrier_path_with_single_cite_routes_to_the_chart(self):
        groups, error = translator.split_output_files(
            CHART_UNIT, [{"path": "values.yaml", "content": "a: 1\n"}])
        self.assertEqual(error, "")
        self.assertIn("chart", groups["charts"])
        self.assertIn("values.yaml", groups["charts"]["chart"])

    def test_flattened_output_gets_the_plain_gate_not_the_render(self):
        result = result_with(
            [{"path": "deployment.yaml", "content": "a: [broken\n"}])
        error = translator.validate_workload_translation(
            result, CHART_UNIT,
            self.ctx(helm=lambda r, d: (GOOD_DOC, None)))
        self.assertIn("deployment.yaml", error)  # plain gate caught it

    def test_kustomize_arm(self):
        os.makedirs(os.path.join(self.root, "overlays", "prod"))
        with open(os.path.join(self.root, "overlays", "prod",
                               "kustomization.yaml"), "w") as f:
            f.write("resources: []\n")
        calls = []

        def fake_kustomize(root, rel_dir):
            calls.append(rel_dir)
            return GOOD_DOC, None

        result = result_with([{
            "path": "overlays/prod/kustomization.yaml",
            "content": "resources:\n- svc.yaml\n"}])
        error = translator.validate_workload_translation(
            result, KUSTOMIZE_UNIT, self.ctx(kustomize=fake_kustomize))
        self.assertEqual(error, "")
        self.assertEqual(calls, ["overlays/prod"])

    def test_an_emitted_helm_partial_is_refused(self):
        """helm never renders templates/_*.tpl, so an edit there would ship
        with nothing having gated it — refuse rather than fail open."""
        rendered = []

        def fake_helm(root, rel_dir):
            rendered.append(rel_dir)
            return GOOD_DOC, None

        # .tpl is already refused by the extension rule; a partial named
        # with a YAML extension is the one that reached the render gate.
        result = result_with([
            {"path": "chart/templates/_helpers.yaml",
             "content": '{{- define "x" -}}edited{{- end }}\n'}])
        error = translator.validate_workload_translation(
            result, CHART_UNIT, self.ctx(helm=fake_helm))
        self.assertIn("Helm partials", error)
        self.assertIn("chart/templates/_helpers.yaml", error)
        # Refused before the render, so no render was even attempted.
        self.assertEqual(rendered, [])

    def test_an_ordinary_template_edit_is_not_treated_as_a_partial(self):
        result = result_with([
            {"path": "chart/templates/deployment.yaml",
             "content": GO_TEMPLATE}])
        error = translator.validate_workload_translation(
            result, CHART_UNIT, self.ctx(helm=lambda r, d: (GOOD_DOC, None)))
        self.assertEqual(error, "")

    def test_missing_binary_is_a_hard_tool_error_naming_it(self):
        result = result_with(
            [{"path": "chart/values.yaml", "content": "a: 1\n"}])
        with patch.object(translator.subprocess, "run",
                          side_effect=FileNotFoundError()):
            with self.assertRaises(translator.RenderToolMissing) as raised:
                translator.validate_workload_translation(
                    result, CHART_UNIT, {"source_root": self.root})
        self.assertIn("helm", str(raised.exception))

    def test_missing_render_ctx_for_a_chart_unit_is_rejected(self):
        result = result_with(
            [{"path": "chart/values.yaml", "content": "a: 1\n"}])
        error = translator.validate_workload_translation(result, CHART_UNIT)
        self.assertIn("re-render gate", error)


class RetryTest(unittest.IsolatedAsyncioTestCase):

    async def test_one_retry_feeds_the_rejection_back(self):
        good = json.dumps(result_with(
            [{"path": "svc.yaml", "content": GOOD_DOC}]))
        outputs = ["not json at all", good]
        prompts = []

        async def fake_worker(prompt, model):
            prompts.append(prompt)
            return outputs.pop(0)

        with patch.object(translator, "_run_worker", fake_worker):
            result = await translator.translate_unit(
                PLAIN_UNIT, {"unit": {}, "documents": []})
        self.assertEqual(result["files"][0]["path"], "svc.yaml")
        self.assertEqual(len(prompts), 2)
        self.assertIn("previous attempt was rejected", prompts[1])

    async def test_two_failures_raise_after_the_single_retry(self):
        async def bad_worker(prompt, model):
            return "still not json"

        with patch.object(translator, "_run_worker", bad_worker):
            with self.assertRaises(ValueError) as raised:
                await translator.translate_unit(
                    PLAIN_UNIT, {"unit": {}, "documents": []})
        self.assertIn("failed after retry", str(raised.exception))


HTTPROUTE_DOC = (
    "apiVersion: gateway.networking.k8s.io/v1\nkind: HTTPRoute\nmetadata:\n"
    "  name: web\n  namespace: shop\nspec:\n  parentRefs:\n"
    "  - name: shared-gateway\n    namespace: gateway-infra\n"
    "  hostnames: [shop.acme.example]\n  rules:\n"
    "  - matches: [{path: {type: PathPrefix, value: /}}]\n"
    "    backendRefs: [{name: frontend, port: 80}]\n")

ROUTING_UNIT = {"unit_id": "wkld-routing", "family": "wkld-routing",
                "inputs": {"documents": [
                    {"path": "k8s/ing.yaml", "doc_index": 0,
                     "rendered_from": None}]}}


class HttpRouteGateTest(unittest.TestCase):
    """M4 §2.8: an HTTPRoute document is an ordinary manifest to the
    structural gate — proving the worker contract needed NO change."""

    def test_wellformed_httproute_passes_the_gate(self):
        result = result_with([{"path": "httproute.yaml",
                               "content": HTTPROUTE_DOC}])
        self.assertEqual(
            translator.validate_workload_translation(result, ROUTING_UNIT),
            "")

    def test_httproute_missing_metadata_name_is_rejected(self):
        broken = HTTPROUTE_DOC.replace("  name: web\n", "")
        result = result_with([{"path": "httproute.yaml", "content": broken}])
        error = translator.validate_workload_translation(result, ROUTING_UNIT)
        self.assertIn("metadata.name", error)



class FactKnowledgeTest(unittest.TestCase):
    """The per-fact knowledge delivery: a unit with pod_dns_facts gets the
    mapping document appended to its prompt and the facts in its payload;
    a unit without does not; the document exists (start-up check)."""

    FACTS = [{"label": "mailer", "kind": "Job", "namespace": "acme-shop", "name": "mailer",
              "node_path": "spec.template.spec", "dns_policy": "None", "host_network": False,
              "nameservers": ["172.20.0.10"], "searches": [], "options": [], "host_aliases": []}]

    def test_the_document_is_present_and_validates(self):
        translator.validate_fact_knowledge()
        text = translator.load_fact_knowledge("pod_dns_facts")
        self.assertIn("## Output contract", text)
        self.assertIn("169.254.169.253", text)
        self.assertIsNone(translator.load_fact_knowledge("no_such_key"))

    def test_prompt_carries_the_knowledge_only_with_facts_or_an_unread_source(self):
        unit = {**PLAIN_UNIT, "inputs": {**PLAIN_UNIT["inputs"], "pod_dns_facts": self.FACTS}}
        prompt = translator.build_translation_prompt(unit, {"unit": {}, "documents": []})
        self.assertIn("--- Unit knowledge for inputs.pod_dns_facts", prompt)
        self.assertIn("# Pod DNS translation", prompt)
        bare = {**PLAIN_UNIT, "inputs": {**PLAIN_UNIT["inputs"], "pod_dns_facts": []}}
        self.assertNotIn("Unit knowledge", translator.build_translation_prompt(bare, {"unit": {}, "documents": []}))
        unread = {**PLAIN_UNIT, "inputs": {**PLAIN_UNIT["inputs"], "pod_dns_facts": [],
                                            "pod_dns_unread": ["chart x could not be rendered"]}}
        self.assertIn("inputs.pod_dns_unread", translator.build_translation_prompt(unread, {"unit": {}, "documents": []}))
        both = {**PLAIN_UNIT, "inputs": {**PLAIN_UNIT["inputs"], "pod_dns_facts": self.FACTS,
                                          "pod_dns_unread": ["chart x could not be rendered"]}}
        self.assertEqual(translator.build_translation_prompt(both, {"unit": {}, "documents": []}).count("# Pod DNS translation"), 1)
        self.assertNotIn("Unit knowledge", translator.build_translation_prompt(PLAIN_UNIT, {"unit": {}, "documents": []}))

    def test_worker_input_carries_the_facts(self):
        import tempfile, os
        with tempfile.TemporaryDirectory() as root:
            os.makedirs(os.path.join(root, "k8s"))
            with open(os.path.join(root, "k8s", "app.yaml"), "w") as f:
                f.write(GOOD_DOC)
            unit = {**PLAIN_UNIT, "inputs": {**PLAIN_UNIT["inputs"], "pod_dns_facts": self.FACTS}}
            payload = translator.build_worker_input(unit, {"carriers": []}, root)
            self.assertEqual(payload["unit"]["pod_dns_facts"], self.FACTS)
            plain = translator.build_worker_input(PLAIN_UNIT, {"carriers": []}, root)
            self.assertNotIn("pod_dns_facts", plain["unit"])


if __name__ == "__main__":
    unittest.main()
