"""Unit tests for carrier-first materialization (materialize.py).

Covers: carrier-then-edits order (the source chart tree ships once, edits
overlay it), the two-unit-edit finding, carriage never reverting an edit,
the mixed-carrier-strategy finding, the kustomize graph's non-directory
files, and the path-escape guard.
"""

import os
import shutil
import tempfile
import unittest

from servers.phases.workload.workload_validate_5 import materialize

COMPONENT = "orders-component"
GOOD_DOC = "apiVersion: v1\nkind: Service\nmetadata:\n  name: web\n"


def chart_unit(unit_id):
    return {"unit_id": unit_id, "family": unit_id,
            "inputs": {"documents": [
                {"path": "chart", "doc_index": 0,
                 "rendered_from": {"type": "helm", "chart_path": "chart"}}]}}


def plain_unit(unit_id):
    return {"unit_id": unit_id, "family": unit_id,
            "inputs": {"documents": [
                {"path": "k8s/app.yaml", "doc_index": 0,
                 "rendered_from": None}]}}


def entry(unit, files):
    return {"unit": unit,
            "result": {"files": [{"path": p, "content": c}
                                 for p, c in files],
                       "tradeoffs": "t"}}


class MaterializeTest(unittest.TestCase):

    def setUp(self):
        self.source = tempfile.mkdtemp(prefix="wkld_mat_src_")
        self.clone = tempfile.mkdtemp(prefix="wkld_mat_clone_")
        self.addCleanup(shutil.rmtree, self.source, True)
        self.addCleanup(shutil.rmtree, self.clone, True)
        chart = os.path.join(self.source, "chart")
        os.makedirs(os.path.join(chart, "templates"))
        self.files = {
            "Chart.yaml": "name: web\nversion: 1.0.0\n",
            "values.yaml": "name: source\n",
            "templates/dep.yaml": "kind: {{ .Values.k }}\n",
            "templates/_helpers.tpl": "{{- define \"x\" -}}web{{- end }}\n",
        }
        for rel, content in self.files.items():
            with open(os.path.join(chart, rel), "w") as f:
                f.write(content)

    def plan(self, units):
        return {"component": COMPONENT, "units": units,
                "carriers": [{"chart_path": "chart", "values_digest": "d",
                              "owner": "wkld-manifests"}]}

    def read(self, rel):
        with open(os.path.join(self.clone, rel)) as f:
            return f.read()

    def test_carrier_first_then_owned_file_edits(self):
        manifests = chart_unit("wkld-manifests")
        identity = chart_unit("wkld-identity")
        storage = plain_unit("wkld-storage")
        done = [
            entry(manifests, [("chart/values.yaml", "name: edited\n"),
                              ("chart/Chart.yaml", self.files["Chart.yaml"])]),
            entry(identity, [("chart/templates/sa.yaml", GOOD_DOC)]),
            entry(storage, [("pvc.yaml", GOOD_DOC)]),
        ]
        result = materialize.materialize_component(
            self.clone, COMPONENT,
            self.plan([manifests, identity, storage]), done, self.source)
        base = f"workloads/{COMPONENT}"
        # The whole source tree shipped (helpers .tpl included), edits on top.
        self.assertEqual(self.read(f"{base}/chart/values.yaml"),
                         "name: edited\n")
        self.assertEqual(self.read(f"{base}/chart/templates/dep.yaml"),
                         self.files["templates/dep.yaml"])
        self.assertEqual(self.read(f"{base}/chart/templates/_helpers.tpl"),
                         self.files["templates/_helpers.tpl"])
        self.assertEqual(self.read(f"{base}/chart/templates/sa.yaml"),
                         GOOD_DOC)
        self.assertEqual(self.read(f"{base}/wkld-storage/pvc.yaml"), GOOD_DOC)
        self.assertEqual(result["conflicts"], [])
        self.assertEqual(result["unit_dirs"], [f"{base}/wkld-storage"])
        self.assertEqual(result["render_dirs"],
                         [{"kind": "helm", "dir": f"{base}/chart",
                           "source": "chart"}])

    def test_a_file_edited_by_two_units_is_a_finding_never_a_merge(self):
        identity = chart_unit("wkld-identity")
        storage = chart_unit("wkld-storage")
        done = [entry(identity, [("chart/templates/sa.yaml", "a: 1\n")]),
                entry(storage, [("chart/templates/sa.yaml", "b: 2\n")])]
        result = materialize.materialize_component(
            self.clone, COMPONENT, self.plan([identity, storage]), done,
            self.source)
        self.assertEqual(len(result["conflicts"]), 1)
        finding = result["conflicts"][0]
        self.assertEqual(finding["units"], ["wkld-identity", "wkld-storage"])
        self.assertIn("chart/templates/sa.yaml", finding["path"])

    def test_re_emitting_an_untouched_carrier_file_is_not_an_edit(self):
        manifests = chart_unit("wkld-manifests")
        identity = chart_unit("wkld-identity")
        done = [entry(manifests, [("chart/values.yaml", "name: source\n")]),
                entry(identity, [("chart/values.yaml", "name: identity\n")])]
        result = materialize.materialize_component(
            self.clone, COMPONENT, self.plan([manifests, identity]), done,
            self.source)
        # The owner's byte-identical re-emission does not count as an edit,
        # so exactly one unit EDITED the file: no conflict.
        self.assertEqual(result["conflicts"], [])
        # And the edit is what ships. Carriage used to be WRITTEN anyway, so
        # a later byte-identical re-emission silently reverted the earlier
        # unit's edit while staying out of the conflict list entirely.
        self.assertEqual(
            self.read(f"workloads/{COMPONENT}/chart/values.yaml"),
            "name: identity\n")

    def test_carriage_after_an_edit_does_not_revert_it(self):
        """Plan order decides who writes; carriage never un-writes."""
        identity = chart_unit("wkld-identity")
        manifests = chart_unit("wkld-manifests")
        # wkld-identity edits FIRST in plan order, the owner re-emits the
        # untouched source file after it.
        done = [entry(identity, [("chart/values.yaml", "name: identity\n")]),
                entry(manifests, [("chart/values.yaml", "name: source\n")])]
        result = materialize.materialize_component(
            self.clone, COMPONENT, self.plan([identity, manifests]), done,
            self.source)
        self.assertEqual(result["conflicts"], [])
        self.assertEqual(
            self.read(f"workloads/{COMPONENT}/chart/values.yaml"),
            "name: identity\n")

    def test_a_satellite_editing_a_chart_its_owner_flattened_is_a_finding(self):
        """Decision 6's other half: one strategy per carrier, owner decides.

        The owner emits plain manifests (it flattened the chart) while a
        satellite edits the chart. Copying the source tree for the satellite
        would ship the un-migrated chart beside the flattened translation —
        two Deployments, one still pointing at the source registry.
        """
        manifests = plain_unit("wkld-manifests")
        identity = chart_unit("wkld-identity")
        done = [entry(manifests, [("dep.yaml", GOOD_DOC)]),
                entry(identity, [("chart/templates/sa.yaml", GOOD_DOC)])]
        result = materialize.materialize_component(
            self.clone, COMPONENT, self.plan([manifests, identity]), done,
            self.source)
        self.assertEqual(len(result["strategy"]), 1)
        finding = result["strategy"][0]
        self.assertEqual(finding["chart_path"], "chart")
        self.assertEqual(finding["owner"], "wkld-manifests")
        self.assertEqual(finding["units"], ["wkld-identity"])
        self.assertIn("un-migrated source chart", finding["error"])

    def test_the_owner_keeping_the_chart_is_not_a_strategy_finding(self):
        manifests = chart_unit("wkld-manifests")
        identity = chart_unit("wkld-identity")
        done = [entry(manifests, [("chart/values.yaml", "name: edited\n")]),
                entry(identity, [("chart/templates/sa.yaml", GOOD_DOC)])]
        result = materialize.materialize_component(
            self.clone, COMPONENT, self.plan([manifests, identity]), done,
            self.source)
        self.assertEqual(result["strategy"], [])

    def test_path_escape_raises(self):
        storage = plain_unit("wkld-storage")
        done = [entry(storage, [("../../evil.yaml", GOOD_DOC)])]
        with self.assertRaises(ValueError):
            materialize.materialize_component(
                self.clone, COMPONENT, self.plan([storage]), done,
                self.source)
        self.assertFalse(os.path.exists(
            os.path.join(self.clone, "..", "evil.yaml")))

    def test_kustomize_graph_files_outside_a_kustomization_dir_are_copied(self):
        """`resources: - ../shared/ns.yaml` resolves a plain FILE in a
        directory with no kustomization.yaml of its own. Copying only the
        graph's dirs produced a tree kubectl could not render."""
        os.makedirs(os.path.join(self.source, "overlays", "prod"))
        os.makedirs(os.path.join(self.source, "shared"))
        with open(os.path.join(self.source, "shared", "ns.yaml"), "w") as f:
            f.write("apiVersion: v1\nkind: Namespace\nmetadata:\n"
                    "  name: acme\n")
        with open(os.path.join(self.source, "overlays", "prod",
                               "kustomization.yaml"), "w") as f:
            f.write("resources:\n- ../../shared/ns.yaml\n")
        unit = {"unit_id": "wkld-manifests", "family": "wkld-manifests",
                "inputs": {"documents": [
                    {"path": "overlays/prod", "doc_index": 0,
                     "rendered_from": {"type": "kustomize",
                                       "kustomize_dir": "overlays/prod"}}]}}
        done = [entry(unit, [("overlays/prod/kustomization.yaml",
                              "resources:\n- ../../shared/ns.yaml\n"
                              "namespace: acme\n")])]
        plan = {"component": COMPONENT, "units": [unit], "carriers": []}
        result = materialize.materialize_component(
            self.clone, COMPONENT, plan, done, self.source)
        base = f"workloads/{COMPONENT}"
        self.assertIn("kind: Namespace", self.read(f"{base}/shared/ns.yaml"))
        self.assertEqual(result["render_dirs"],
                         [{"kind": "kustomize",
                           "dir": f"{base}/overlays/prod",
                           "source": "overlays/prod"}])

    def test_stale_files_from_a_previous_run_are_cleared(self):
        stale = os.path.join(self.clone, "workloads", COMPONENT, "old")
        os.makedirs(stale)
        with open(os.path.join(stale, "stale.yaml"), "w") as f:
            f.write("old: true\n")
        storage = plain_unit("wkld-storage")
        materialize.materialize_component(
            self.clone, COMPONENT, self.plan([storage]),
            [entry(storage, [("pvc.yaml", GOOD_DOC)])], self.source)
        self.assertFalse(os.path.exists(stale))


if __name__ == "__main__":
    unittest.main()
