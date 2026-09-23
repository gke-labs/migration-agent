import json
import os
import tempfile
import unittest

from google.api_core import exceptions

from server import exports
from servers.phases.discovery.discovery_init_1.datastores import is_literal_handle
from server import ledger_iam


class FakeBlob:
    def __init__(self, store, name):
        self.store = store
        self.name = name
        self.generation = None

    def reload(self):
        if self.name not in self.store.objects:
            raise exceptions.NotFound(self.name)
        self.generation = self.store.objects[self.name]["generation"]

    def download_as_text(self):
        if self.name not in self.store.objects:
            raise exceptions.NotFound(self.name)
        return self.store.objects[self.name]["text"]

    def upload_from_string(self, data, content_type=None, if_generation_match=None):
        error = self.store.upload_errors.get(self.name)
        if error:
            raise error
        if self.store.conflicts.get(self.name):
            # A concurrent writer lands between the read and this write: the
            # stored generation moves on and the precondition write is refused.
            self.store.conflicts[self.name] -= 1
            self.store.put(self.name, self.store.interloper_text)
            raise exceptions.PreconditionFailed("generation mismatch")
        existing = self.store.objects.get(self.name)
        current = existing["generation"] if existing else 0
        if if_generation_match is not None and if_generation_match != current:
            raise exceptions.PreconditionFailed("generation mismatch")
        self.store.put(self.name, data)


class FakeBucket:
    def __init__(self):
        self.objects = {}
        self.conflicts = {}       # blob name -> number of writes to interrupt
        self.upload_errors = {}   # blob name -> exception raised on upload
        self.interloper_text = "{}"

    def blob(self, name):
        return FakeBlob(self, name)

    def put(self, name, text):
        generation = self.objects.get(name, {}).get("generation", 0) + 1
        self.objects[name] = {"text": text, "generation": generation}

    def exports_doc(self):
        return json.loads(self.objects[exports.EXPORTS_BLOB]["text"])


DEPLOYMENT_YAML = """\
apiVersion: apps/v1
kind: Deployment
metadata:
  name: orders
  namespace: payments
  labels:
    app: orders
    team: payments-team-a
    example.com/owning-team: checkout
"""

MULTI_DOC_YAML = """\
apiVersion: v1
kind: Service
metadata:
  name: orders
  namespace: payments
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: orders-config
  namespace: shared
"""

FIXTURE_FILES = {
    "app/deploy.yaml": DEPLOYMENT_YAML,
    "app/multi.yaml": MULTI_DOC_YAML,
    "bad/broken.yaml": "a: [unclosed",
    "bad/alias.yaml": "a: &x [1]\nb: *x\n",
    "charts/mychart/templates/deploy.yaml": "{{- if .Values.enabled }}\nbad\n",
    "infra/main.tf": 'resource "aws_eks_cluster" "x" {}',
    "config/settings.json": '{"kind": "AppConfig"}',
}


def fixture_reader(path):
    return FIXTURE_FILES[path]


def entries(*paths):
    return [{"path": p, "size": 1, "kind": "any"} for p in paths]


class SourceRepoTest(unittest.TestCase):

    def test_onboarding_variables_map_straight_through(self):
        variables = {"source_repo_url": "sso://estate", "source_branch": "main",
                     "source_path": "clusters/prod"}
        self.assertEqual(exports.derive_source_repo(variables),
                         {"url": "sso://estate", "branch": "main", "path": "clusters/prod"})

    def test_missing_or_empty_values_become_null_not_guesses(self):
        self.assertEqual(exports.derive_source_repo({"source_path": ""}),
                         {"url": None, "branch": None, "path": None})


class TargetRepoTest(unittest.TestCase):
    """target_repo is the developer-side ship channel: developers cannot read
    platform/onboarding/state.json, so exports.json is the only place these
    coordinates can legally reach a developer session."""

    def test_onboarding_variables_map_straight_through(self):
        variables = {"target_repo_url": "https://ssm/target.git",
                     "target_branch": "main", "target_path": "/"}
        value, notes = exports.derive_target_repo(variables)
        self.assertEqual(value, {"url": "https://ssm/target.git",
                                 "branch": "main", "path": "/"})
        self.assertEqual(notes, [])

    def test_missing_or_empty_values_become_null_not_guesses(self):
        value, notes = exports.derive_target_repo({"target_path": ""})
        self.assertEqual(value, {"url": None, "branch": None, "path": None})
        self.assertEqual(notes, [])

    def test_an_unresolved_ssm_triple_is_a_note_never_a_guessed_url(self):
        value, notes = exports.derive_target_repo({
            "target_branch": "main", "ssm_instance": "inst",
            "ssm_location": "us-central1", "ssm_repository": "repo"})
        self.assertIsNone(value["url"])
        self.assertEqual(len(notes), 1)
        self.assertIn("SSM repository triple", notes[0])
        self.assertIn("never guessed", notes[0])

    def test_a_resolved_ssm_repo_carries_its_url_without_a_note(self):
        value, notes = exports.derive_target_repo({
            "target_repo_url": "https://ssm/target.git", "target_branch": "main",
            "ssm_instance": "inst", "ssm_location": "us-central1",
            "ssm_repository": "repo"})
        self.assertEqual(value["url"], "https://ssm/target.git")
        self.assertEqual(notes, [])


class SeedIndexTest(unittest.TestCase):

    def index_for(self, paths, read_text=fixture_reader, chart_roots=()):
        return exports.derive_seed_index(entries(*paths), read_text, list(chart_roots))

    def test_terraform_files_are_kinded_terraform_without_parsing(self):
        index, notes = self.index_for(["infra/main.tf"], read_text=None)
        self.assertEqual(index["infra/main.tf"],
                         {"kinds": ["terraform"], "namespaces": [],
                          "team_labels": [], "names": []})
        self.assertEqual(notes, [])

    def test_a_parseable_manifest_yields_kinds_namespace_and_team_labels(self):
        index, notes = self.index_for(["app/deploy.yaml"])
        self.assertEqual(index["app/deploy.yaml"], {
            "kinds": ["Deployment"],
            "namespaces": ["payments"],
            "team_labels": ["checkout", "payments-team-a"],
            "names": ["Deployment/orders"],
        })
        self.assertEqual(notes, [])

    def test_multi_document_files_union_their_metadata(self):
        index, _ = self.index_for(["app/multi.yaml"])
        self.assertEqual(index["app/multi.yaml"]["kinds"], ["ConfigMap", "Service"])
        self.assertEqual(index["app/multi.yaml"]["namespaces"], ["payments", "shared"])

    def test_an_unparseable_file_degrades_to_empty_metadata_with_a_note(self):
        index, notes = self.index_for(["bad/broken.yaml"])
        self.assertEqual(index["bad/broken.yaml"],
                         {"kinds": [], "namespaces": [], "team_labels": [],
                          "names": []})
        self.assertEqual(len(notes), 1)
        self.assertIn("bad/broken.yaml", notes[0])
        self.assertIn("failed to parse", notes[0])

    def test_yaml_aliases_are_refused_like_the_manifest_gate(self):
        index, notes = self.index_for(["bad/alias.yaml"])
        self.assertEqual(index["bad/alias.yaml"]["kinds"], [])
        self.assertEqual(len(notes), 1, "the hardened loader must reject aliases")

    def test_chart_root_files_are_marked_helm_chart_and_never_parsed(self):
        def exploding_reader(path):
            raise AssertionError(f"chart-root file must not be read: {path}")
        index, notes = exports.derive_seed_index(
            entries("charts/mychart/templates/deploy.yaml"),
            exploding_reader, ["charts/mychart"])
        self.assertEqual(index["charts/mychart/templates/deploy.yaml"],
                         {"kinds": ["helm-chart"], "namespaces": [],
                          "team_labels": [], "names": []})
        self.assertEqual(notes, [])

    def test_json_files_go_through_the_same_parser(self):
        index, notes = self.index_for(["config/settings.json"])
        self.assertEqual(index["config/settings.json"]["kinds"], ["AppConfig"])
        self.assertEqual(notes, [])

    def test_without_a_checkout_entries_carry_index_facts_only(self):
        index, notes = self.index_for(["app/deploy.yaml", "infra/main.tf"], read_text=None)
        self.assertEqual(index["app/deploy.yaml"],
                         {"kinds": [], "namespaces": [], "team_labels": [],
                          "names": []})
        self.assertEqual(index["infra/main.tf"]["kinds"], ["terraform"])
        self.assertEqual(notes, [], "the caller records one checkout-level note instead")

    def test_an_oversized_file_is_not_parsed(self):
        index, notes = self.index_for(
            ["big.yaml"], read_text=lambda path: "x" * (exports.MAX_SEED_FILE_BYTES + 1))
        self.assertEqual(index["big.yaml"]["kinds"], [])
        self.assertEqual(len(notes), 1)
        self.assertIn("exceeds", notes[0])

    def test_an_unreadable_file_gets_a_note_naming_it(self):
        index, notes = self.index_for(["gone.yaml"])
        self.assertEqual(index["gone.yaml"]["kinds"], [])
        self.assertIn("gone.yaml: unreadable", notes[0])


class PublishTest(unittest.TestCase):

    def setUp(self):
        self.bucket = FakeBucket()

    def test_blob_name_matches_the_bootstrap_grant_object(self):
        self.assertEqual(exports.EXPORTS_BLOB, ledger_iam.EXPORTS_OBJECT,
                         "the conditional grants name exactly this object")

    def test_first_publish_creates_the_document_with_null_discipline(self):
        doc = exports.publish(self.bucket, "discovery",
                              {"source_repo": {"url": "u", "branch": "b", "path": "p"},
                               "component_seed_index": {}},
                              ["discovery: note"], now_iso="2026-08-14T00:00:00+00:00")
        self.assertEqual(self.bucket.objects[exports.EXPORTS_BLOB]["generation"], 1)
        for field in ("storage_class_menu", "gateway", "gsa_bindings", "compute_classes", "artifact_registry",
                      "node_shapes", "project", "cluster", "workload_pool",
                      "staging_bucket", "data_gate"):
            self.assertIsNone(doc[field], f"{field} must stay an explicit null")
        self.assertEqual(doc["generations"],
                         {"discovery": 1, "translation": 0, "deployment": 0,
                          "data": 0})
        self.assertEqual(doc["generated_at"], "2026-08-14T00:00:00+00:00")
        self.assertEqual(doc["derivation_notes"], ["discovery: note"])

    def test_republish_bumps_only_its_source_and_replaces_its_notes(self):
        exports.publish(self.bucket, "discovery", {"component_seed_index": {}},
                        ["discovery: old note"])
        doc = exports.publish(self.bucket, "discovery", {"component_seed_index": {"a": {}}},
                              ["discovery: new note"])
        self.assertEqual(doc["generations"]["discovery"], 2)
        self.assertEqual(doc["derivation_notes"], ["discovery: new note"],
                         "stale notes from the same source must not accumulate")

    def test_publish_preserves_the_other_sources_fields_and_notes(self):
        exports.publish(self.bucket, "translation",
                        {"storage_class_menu": ["gp3-encrypted"]}, ["translation: note"])
        doc = exports.publish(self.bucket, "discovery", {"component_seed_index": {}}, [])
        self.assertEqual(doc["storage_class_menu"], ["gp3-encrypted"])
        self.assertEqual(doc["derivation_notes"], ["translation: note"])
        self.assertEqual(doc["generations"], {"discovery": 1, "translation": 1,
                                              "deployment": 0, "data": 0})

    def test_a_field_outside_the_sources_row_is_refused(self):
        with self.assertRaises(ValueError):
            exports.publish(self.bucket, "discovery", {"storage_class_menu": []}, [])

    def test_an_unknown_source_is_refused(self):
        with self.assertRaises(ValueError):
            exports.publish(self.bucket, "cutover", {}, [])

    def test_a_precondition_conflict_is_retried_against_the_new_document(self):
        interloper = exports.empty_exports()
        interloper["storage_class_menu"] = ["standard-rwo"]
        interloper["generations"]["translation"] = 3
        self.bucket.interloper_text = json.dumps(interloper)
        self.bucket.conflicts[exports.EXPORTS_BLOB] = 1

        doc = exports.publish(self.bucket, "discovery", {"component_seed_index": {}}, [])

        self.assertEqual(doc["storage_class_menu"], ["standard-rwo"],
                         "the retry must re-read and merge over the interloper's write")
        self.assertEqual(doc["generations"], {"discovery": 1, "translation": 3,
                                              "deployment": 0, "data": 0})

    def test_a_second_conflict_propagates(self):
        self.bucket.conflicts[exports.EXPORTS_BLOB] = 2
        with self.assertRaises(exceptions.PreconditionFailed):
            exports.publish(self.bucket, "discovery", {"component_seed_index": {}}, [])


class DiscoveryHookTest(unittest.TestCase):

    def setUp(self):
        self.bucket = FakeBucket()
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.variables = {
            "source_repo_url": "sso://estate", "source_branch": "main",
            "source_path": "clusters/prod",
            "target_repo_url": "https://ssm/target.git", "target_branch": "main",
            "target_path": "/",
            "discovery_scope": {"root_dir": self.tmp.name, "excluded": [], "included": []},
        }

    def write_file(self, rel_path, content):
        full = os.path.join(self.tmp.name, rel_path)
        os.makedirs(os.path.dirname(full), exist_ok=True)
        with open(full, "w", encoding="utf-8") as f:
            f.write(content)

    def put_manifest(self, *paths):
        self.bucket.put(exports.MANIFEST_BLOB, json.dumps({"files": entries(*paths)}))

    def test_happy_path_publishes_repo_and_seed_index(self):
        self.write_file("app/deploy.yaml", DEPLOYMENT_YAML)
        self.write_file("charts/mychart/templates/deploy.yaml", "{{ bad }}")
        self.put_manifest("app/deploy.yaml", "charts/mychart/templates/deploy.yaml")
        inventory = {"render_targets": [
            {"id": "charts/mychart", "type": "helm", "root": "charts/mychart",
             "status": "rendered"}]}

        warning = exports.publish_discovery_exports(self.bucket, self.variables, inventory)

        self.assertEqual(warning, "")
        doc = self.bucket.exports_doc()
        self.assertEqual(doc["source_repo"]["url"], "sso://estate")
        self.assertEqual(doc["target_repo"],
                         {"url": "https://ssm/target.git", "branch": "main", "path": "/"},
                         "the ship coordinates must ride the developer-readable object")
        self.assertEqual(doc["component_seed_index"]["app/deploy.yaml"]["kinds"],
                         ["Deployment"])
        self.assertEqual(
            doc["component_seed_index"]["charts/mychart/templates/deploy.yaml"]["kinds"],
            ["helm-chart"])
        self.assertEqual(doc["generations"]["discovery"], 1)

    def test_missing_manifest_is_an_explicit_null_with_a_note(self):
        warning = exports.publish_discovery_exports(self.bucket, self.variables, {})
        self.assertEqual(warning, "")
        doc = self.bucket.exports_doc()
        self.assertIsNone(doc["component_seed_index"])
        self.assertTrue(any("manifest.json is absent" in n for n in doc["derivation_notes"]))

    def test_missing_checkout_degrades_to_index_facts_with_one_note(self):
        self.put_manifest("app/deploy.yaml")
        variables = dict(self.variables, discovery_scope={"root_dir": "/nonexistent-xyz"})
        warning = exports.publish_discovery_exports(self.bucket, variables, {})
        self.assertEqual(warning, "")
        doc = self.bucket.exports_doc()
        self.assertEqual(doc["component_seed_index"]["app/deploy.yaml"]["kinds"], [])
        self.assertTrue(any("source checkout unavailable" in n
                            for n in doc["derivation_notes"]))

    def test_a_manifest_path_escaping_the_checkout_is_refused_per_entry(self):
        self.put_manifest("../evil.yaml")
        warning = exports.publish_discovery_exports(self.bucket, self.variables, {})
        self.assertEqual(warning, "")
        doc = self.bucket.exports_doc()
        self.assertEqual(doc["component_seed_index"]["../evil.yaml"]["kinds"], [])
        self.assertTrue(any("escapes the source checkout" in n
                            for n in doc["derivation_notes"]))

    @staticmethod
    def prefix_filter(files, scope):
        kept, removed = [], []
        for f in files:
            excluded = any(f["path"] == p or f["path"].startswith(p.rstrip("/") + "/")
                           for p in scope.get("excluded", []))
            (removed if excluded else kept).append(f)
        return kept, removed

    def test_scope_excluded_files_are_not_indexed(self):
        self.write_file("app/deploy.yaml", DEPLOYMENT_YAML)
        self.write_file("secret/internal.yaml", DEPLOYMENT_YAML)
        self.put_manifest("app/deploy.yaml", "secret/internal.yaml")
        self.variables["discovery_scope"]["excluded"] = ["secret/"]

        warning = exports.publish_discovery_exports(
            self.bucket, self.variables, {}, self.prefix_filter)

        self.assertEqual(warning, "")
        doc = self.bucket.exports_doc()
        self.assertIn("app/deploy.yaml", doc["component_seed_index"])
        self.assertNotIn("secret/internal.yaml", doc["component_seed_index"],
                         "excluded files' metadata must not cross into the "
                         "member-readable object")
        self.assertTrue(any("excluded by the confirmed discovery scope" in n
                            for n in doc["derivation_notes"]))

    def test_included_extras_are_honestly_reported_as_unindexed(self):
        self.write_file("app/deploy.yaml", DEPLOYMENT_YAML)
        self.put_manifest("app/deploy.yaml")
        self.variables["discovery_scope"]["included"] = ["/somewhere/outside.yaml"]

        warning = exports.publish_discovery_exports(
            self.bucket, self.variables, {}, self.prefix_filter)

        self.assertEqual(warning, "")
        doc = self.bucket.exports_doc()
        self.assertTrue(any("fall outside the persisted manifest" in n
                            for n in doc["derivation_notes"]))

    def test_an_include_that_unexcludes_an_indexed_path_is_not_noted(self):
        # The common include: un-excluding an in-manifest path. It is fully
        # indexed, so a "not indexed" note would be a false degradation record.
        self.write_file("app/deploy.yaml", DEPLOYMENT_YAML)
        self.put_manifest("app/deploy.yaml")
        self.variables["discovery_scope"]["included"] = ["app/deploy.yaml"]

        warning = exports.publish_discovery_exports(
            self.bucket, self.variables, {}, self.prefix_filter)

        self.assertEqual(warning, "")
        doc = self.bucket.exports_doc()
        self.assertFalse(any("fall outside the persisted manifest" in n
                             for n in doc["derivation_notes"]))
        self.assertIn("app/deploy.yaml", doc["component_seed_index"])

    def test_an_oversized_checkout_file_is_rejected_before_reading(self):
        self.write_file("big.yaml", "x" * (exports.MAX_SEED_FILE_BYTES + 10))
        self.put_manifest("big.yaml")
        warning = exports.publish_discovery_exports(self.bucket, self.variables, {})
        self.assertEqual(warning, "")
        doc = self.bucket.exports_doc()
        self.assertEqual(doc["component_seed_index"]["big.yaml"]["kinds"], [])
        self.assertTrue(any("exceeds" in n for n in doc["derivation_notes"]))

    def test_a_publish_failure_warns_instead_of_raising(self):
        self.put_manifest("app/deploy.yaml")
        self.bucket.upload_errors[exports.EXPORTS_BLOB] = RuntimeError("bucket on fire")
        warning = exports.publish_discovery_exports(self.bucket, self.variables, {})
        self.assertIn("WARNING: exports.json publication failed", warning)
        self.assertIn("bucket on fire", warning)
        self.assertNotIn(exports.EXPORTS_BLOB, self.bucket.objects)


STORAGE_UNIT = {
    "unit": {"unit_id": "storage", "kind": "storage"},
    "result": {"files": [
        {"path": "main.tf", "content": "# tf\n"},
        {"path": "classes.yaml", "content":
            "apiVersion: storage.k8s.io/v1\nkind: StorageClass\nmetadata:\n"
            "  name: gp3-encrypted\n---\napiVersion: storage.k8s.io/v1\n"
            "kind: StorageClass\nmetadata:\n  name: premium-rwo\n"},
    ]},
}

GATEWAY_UNIT = {
    "unit": {"unit_id": "network", "kind": "network"},
    "result": {"files": [{"path": "gw.yaml", "content":
        "apiVersion: gateway.networking.k8s.io/v1\nkind: Gateway\nmetadata:\n"
        "  name: shared-gw\n  namespace: infra\n  annotations:\n"
        "    gkma.dev/shared-gateway: \"true\"\n"}]},
}

# The planner's 2d `gateway` unit family output shape: unit id/kind
# "gateway", ONE marked Gateway API manifest with the HTTP-on-80 listener
# default the brief orders (no TLS — an open question by design). The
# derivation must find exactly this shape.
PLANNED_GATEWAY_UNIT = {
    "unit": {"unit_id": "gateway", "kind": "gateway",
             "inputs": {"network": {"load_balancers": ["frontend-alb"]}}},
    "result": {"files": [{"path": "gateway.yaml", "content":
        "apiVersion: gateway.networking.k8s.io/v1\nkind: Gateway\nmetadata:\n"
        "  name: shared-entry\n  namespace: platform-ingress\n  annotations:\n"
        "    gkma.dev/shared-gateway: \"true\"\nspec:\n"
        "  gatewayClassName: gke-l7-global-external-managed\n  listeners:\n"
        "  - name: http\n    protocol: HTTP\n    port: 80\n"
        "    allowedRoutes:\n      namespaces:\n        from: All\n"}]},
}

# The 2026-08-14 e2e case: a unit legitimately emits a real Gateway API
# Gateway for one app — it must never be published as the shared entry point.
APP_GATEWAY_UNIT = {
    "unit": {"unit_id": "network", "kind": "network"},
    "result": {"files": [{"path": "gw.yaml", "content":
        "apiVersion: gateway.networking.k8s.io/v1\nkind: Gateway\nmetadata:\n"
        "  name: frontend\n  namespace: frontend\n"}]},
}

ISTIO_GATEWAY_UNIT = {
    "unit": {"unit_id": "network", "kind": "network"},
    "result": {"files": [{"path": "gw.yaml", "content":
        "apiVersion: networking.istio.io/v1beta1\nkind: Gateway\nmetadata:\n"
        "  name: mesh-gw\n  namespace: istio-system\n"}]},
}


def fake_parse_ksa(status="ok", bindings=None, error=None):
    def parse(tf_files):
        return {"status": status, "bindings": bindings, "error": error,
                "file": sorted(tf_files) and sorted(tf_files)[0]}
    return parse


class TranslationDerivationTest(unittest.TestCase):

    def test_storage_class_menu_from_the_storage_units_yaml(self):
        fields, notes = exports.derive_translation_fields(
            [STORAGE_UNIT], fake_parse_ksa("absent"))
        self.assertEqual(fields["storage_class_menu"], ["gp3-encrypted", "premium-rwo"])

    def test_no_done_storage_unit_is_an_explicit_null_with_a_note(self):
        fields, notes = exports.derive_translation_fields([], fake_parse_ksa("absent"))
        self.assertIsNone(fields["storage_class_menu"])
        self.assertTrue(any("no done storage unit" in n for n in notes))

    def test_an_unparseable_storage_yaml_is_skipped_with_a_note(self):
        unit = {"unit": {"unit_id": "storage", "kind": "storage"},
                "result": {"files": [{"path": "bad.yaml", "content": "a: [oops"}]}}
        fields, notes = exports.derive_translation_fields([unit], fake_parse_ksa("absent"))
        self.assertEqual(fields["storage_class_menu"], [])
        self.assertTrue(any("bad.yaml" in n for n in notes))

    COMPUTE_CLASS_UNIT = {
        "unit": {"unit_id": "compute-class-shop-burst", "kind": "compute-class"},
        "result": {"files": [{"path": "shop-burst.yaml", "content":
            "apiVersion: cloud.google.com/v1\nkind: ComputeClass\nmetadata:\n  name: shop-burst\n"}]},
    }

    def _plan(self, *statuses):
        return {"units": [{"unit_id": f"compute-class-{i}", "kind": "compute-class", "status": st}
                          for i, st in enumerate(statuses)]
                + [{"unit_id": "storage", "kind": "storage", "status": "done"}]}

    def test_compute_classes_from_the_done_compute_class_units(self):
        fields, _ = exports.derive_translation_fields(
            [STORAGE_UNIT, self.COMPUTE_CLASS_UNIT], fake_parse_ksa("absent"), self._plan("done"))
        self.assertEqual(fields["compute_classes"], ["shop-burst"])

    def test_compute_classes_is_empty_only_when_the_plan_proves_no_class(self):
        fields, notes = exports.derive_translation_fields(
            [STORAGE_UNIT], fake_parse_ksa("absent"), self._plan("skipped"))
        self.assertEqual(fields["compute_classes"], [])
        self.assertTrue(any("no ComputeClass in the validated translation" in n for n in notes))
        # A plan with no compute-class unit at all proves it vacuously.
        fields, _ = exports.derive_translation_fields(
            [STORAGE_UNIT], fake_parse_ksa("absent"), {"units": []})
        self.assertEqual(fields["compute_classes"], [])

    def test_compute_classes_is_null_while_a_unit_is_pending_or_the_plan_is_unreadable(self):
        for statuses in (("planned",), ("done", "revise"), ("error",)):
            fields, notes = exports.derive_translation_fields(
                [STORAGE_UNIT], fake_parse_ksa("absent"), self._plan(*statuses))
            self.assertIsNone(fields["compute_classes"], statuses)
            self.assertTrue(any("pending" in n for n in notes), statuses)
        fields, notes = exports.derive_translation_fields([STORAGE_UNIT], fake_parse_ksa("absent"))
        self.assertIsNone(fields["compute_classes"])
        self.assertTrue(any("plan is unavailable" in n for n in notes))

    def test_a_done_class_beside_a_pending_unit_is_still_null(self):
        fields, notes = exports.derive_translation_fields(
            [STORAGE_UNIT, self.COMPUTE_CLASS_UNIT], fake_parse_ksa("absent"),
            self._plan("done", "planned"))
        self.assertIsNone(fields["compute_classes"])
        self.assertTrue(any("pending" in n for n in notes))

    def test_an_unreadable_file_in_a_done_unit_is_null_not_empty(self):
        broken = {"unit": {"unit_id": "compute-class-0", "kind": "compute-class"},
                  "result": {"files": [{"path": "cc.yaml", "content": "a: [oops"}]}}
        fields, notes = exports.derive_translation_fields(
            [broken], fake_parse_ksa("absent"), self._plan("done"))
        self.assertIsNone(fields["compute_classes"])
        self.assertTrue(any("unreadable" in n for n in notes))

    def test_a_foreign_compute_class_is_never_counted(self):
        foreign = {"unit": {"unit_id": "storage", "kind": "storage"},
                   "result": {"files": [{"path": "cc.yaml", "content":
                       "apiVersion: cloud.google.com/v1\nkind: ComputeClass\nmetadata:\n  name: rogue\n"}]}}
        fields, _ = exports.derive_translation_fields(
            [foreign], fake_parse_ksa("absent"), self._plan("skipped"))
        self.assertEqual(fields["compute_classes"], [])

    def test_a_marked_shared_gateway_populates_the_field(self):
        fields, _ = exports.derive_translation_fields(
            [STORAGE_UNIT, GATEWAY_UNIT], fake_parse_ksa("absent"))
        self.assertEqual(fields["gateway"], {"name": "shared-gw", "namespace": "infra"})

    def test_the_planned_gateway_units_manifest_shape_is_derived(self):
        # The 2d arm: the landingzone planner's gateway family output — a
        # marked Gateway under unit id "gateway" — must be what the existing
        # derivation publishes, with no gateway-side degradation note.
        fields, notes = exports.derive_translation_fields(
            [STORAGE_UNIT, PLANNED_GATEWAY_UNIT], fake_parse_ksa("absent"))
        self.assertEqual(fields["gateway"],
                         {"name": "shared-entry", "namespace": "platform-ingress"})
        self.assertFalse([n for n in notes if n.startswith("translation: gateway:")])

    def test_the_planned_gateway_unit_without_the_marker_derives_null(self):
        # The interlock the brief exists to enforce, and the discriminating
        # half of the arm above: the SAME planner-shaped unit with the
        # annotation dropped publishes nothing, and the note names the unit
        # to revise rather than a family that has not shipped.
        unmarked = json.loads(json.dumps(PLANNED_GATEWAY_UNIT))
        unmarked["result"]["files"][0]["content"] = (
            unmarked["result"]["files"][0]["content"]
            .replace('  annotations:\n    gkma.dev/shared-gateway: "true"\n', ""))
        fields, notes = exports.derive_translation_fields(
            [STORAGE_UNIT, unmarked], fake_parse_ksa("absent"))
        self.assertIsNone(fields["gateway"])
        gateway_notes = [n for n in notes if n.startswith("translation: gateway:")]
        self.assertTrue(any("platform-ingress/shared-entry" in n
                            for n in gateway_notes), gateway_notes)
        self.assertTrue(any("revise that unit" in n for n in gateway_notes),
                        gateway_notes)

    def test_an_unmarked_app_gateway_is_a_candidate_not_the_shared_entry_point(self):
        fields, notes = exports.derive_translation_fields(
            [APP_GATEWAY_UNIT], fake_parse_ksa("absent"))
        self.assertIsNone(fields["gateway"],
                          "an app-scoped Gateway must never become THE attach point")
        self.assertTrue(any("frontend/frontend" in n and "marker" in n for n in notes))

    def test_a_non_gateway_api_gateway_kind_is_ignored(self):
        fields, notes = exports.derive_translation_fields(
            [ISTIO_GATEWAY_UNIT], fake_parse_ksa("absent"))
        self.assertIsNone(fields["gateway"])
        self.assertTrue(any("no Gateway manifest" in n for n in notes),
                        "an Istio Gateway is a different API, not a candidate")

    def test_two_distinct_marked_gateways_are_never_guessed_between(self):
        second = json.loads(json.dumps(GATEWAY_UNIT))
        second["unit"]["unit_id"] = "network-2"
        second["result"]["files"][0]["content"] = (
            second["result"]["files"][0]["content"].replace("shared-gw", "other-gw"))
        fields, notes = exports.derive_translation_fields(
            [GATEWAY_UNIT, second], fake_parse_ksa("absent"))
        self.assertIsNone(fields["gateway"])
        self.assertTrue(any("not guessing" in n for n in notes))

    def test_the_same_marked_gateway_in_two_units_is_agreement(self):
        second = json.loads(json.dumps(GATEWAY_UNIT))
        second["unit"]["unit_id"] = "network-2"
        fields, _ = exports.derive_translation_fields(
            [GATEWAY_UNIT, second], fake_parse_ksa("absent"))
        self.assertEqual(fields["gateway"], {"name": "shared-gw", "namespace": "infra"},
                         "identical manifests across units are not ambiguity")

    def test_a_parse_broken_gateway_file_is_named_not_silently_skipped(self):
        broken = {"unit": {"unit_id": "network", "kind": "network"},
                  "result": {"files": [{"path": "gw.yaml", "content": "a: [oops"}]}}
        fields, notes = exports.derive_translation_fields(
            [broken], fake_parse_ksa("absent"))
        self.assertIsNone(fields["gateway"])
        self.assertTrue(any("gw.yaml" in n and "file skipped" in n for n in notes),
                        "a broken platform Gateway must not read as not-yet-shipped")

    def test_the_attach_permission_label_pair_is_the_product_owned_literal(self):
        # One definition for the tenancy/gateway briefs, the attach gate and
        # the workload routing brief (they import it from here — 2026-08-15
        # audit, family 1). Changing these values is a product decision that
        # must fail a test, not a silent cross-chain drift.
        self.assertEqual(exports.GATEWAY_ACCESS_LABEL, "gkma.dev/gateway-access")
        self.assertEqual(exports.GATEWAY_ACCESS_VALUE, "shared")

    def test_no_gateway_is_an_explicit_null_naming_the_refresh_obligation(self):
        # No gateway unit ran here, so the remedy is a re-plan / refresh —
        # never "revise the unit", which is the OTHER null's remedy.
        fields, notes = exports.derive_translation_fields(
            [STORAGE_UNIT], fake_parse_ksa("absent"))
        self.assertIsNone(fields["gateway"])
        self.assertTrue(any("run refresh_exports once a unit ships one" in n
                            for n in notes))
        self.assertFalse(any("revise that unit" in n for n in notes))

    def test_an_empty_contract_map_over_real_bindings_is_null_not_empty(self):
        # The undecided-project escape: the unit's inputs record bindings but
        # the worker could not truthfully write any email — and no target
        # project was supplied, which the note must say (the pipeline half).
        wi = {"unit": {"unit_id": "wi", "kind": "workload-identity",
                       "inputs": {"irsa_bindings": ["acme-shop/orders"]}},
              "result": {"files": [{"path": "main.tf", "content": "..."}]}}
        fields, notes = exports.derive_translation_fields(
            [wi], fake_parse_ksa("ok", bindings={}))
        self.assertIsNone(fields["gsa_bindings"])
        self.assertTrue(any("no target project was supplied" in n for n in notes))

    def test_an_empty_map_despite_a_supplied_project_names_the_worker_half(self):
        # The other half: the project WAS in the unit's inputs and the worker
        # still shipped an empty map — a different defect, named as such.
        wi = {"unit": {"unit_id": "wi", "kind": "workload-identity",
                       "inputs": {"irsa_bindings": ["acme-shop/orders"],
                                  "target_project": "acme-prod"}},
              "result": {"files": [{"path": "main.tf", "content": "..."}]}}
        fields, notes = exports.derive_translation_fields(
            [wi], fake_parse_ksa("ok", bindings={}))
        self.assertIsNone(fields["gsa_bindings"])
        self.assertTrue(any("worker declined to bind" in n
                            and "'acme-prod'" in n for n in notes))

    def test_an_empty_contract_map_with_no_bindings_recorded_stays_empty(self):
        wi = {"unit": {"unit_id": "wi", "kind": "workload-identity",
                       "inputs": {"irsa_bindings": []}},
              "result": {"files": [{"path": "main.tf", "content": "..."}]}}
        fields, _ = exports.derive_translation_fields(
            [wi], fake_parse_ksa("ok", bindings={}))
        self.assertEqual(fields["gsa_bindings"], {})

    def test_gsa_bindings_come_from_the_contract_parser(self):
        wi = {"unit": {"unit_id": "wi", "kind": "workload-identity"},
              "result": {"files": [{"path": "main.tf", "content": "..."}]}}
        bindings = {"ns/sa": "sa@p.iam.gserviceaccount.com"}
        fields, _ = exports.derive_translation_fields(
            [wi], fake_parse_ksa("ok", bindings=bindings))
        self.assertEqual(fields["gsa_bindings"], bindings)

    def test_an_absent_contract_is_null_with_a_legacy_note(self):
        wi = {"unit": {"unit_id": "wi", "kind": "workload-identity"},
              "result": {"files": [{"path": "main.tf", "content": "..."}]}}
        fields, notes = exports.derive_translation_fields([wi], fake_parse_ksa("absent"))
        self.assertIsNone(fields["gsa_bindings"])
        self.assertTrue(any("contract absent" in n for n in notes))

    def test_a_malformed_contract_is_null_carrying_the_parser_error(self):
        wi = {"unit": {"unit_id": "wi", "kind": "workload-identity"},
              "result": {"files": [{"path": "main.tf", "content": "..."}]}}
        fields, notes = exports.derive_translation_fields(
            [wi], fake_parse_ksa("malformed", error="value is not a literal map"))
        self.assertIsNone(fields["gsa_bindings"])
        self.assertTrue(any("value is not a literal map" in n for n in notes))

    def test_no_wi_unit_is_null_with_a_note(self):
        fields, notes = exports.derive_translation_fields(
            [STORAGE_UNIT], fake_parse_ksa("ok", bindings={}))
        self.assertIsNone(fields["gsa_bindings"])
        self.assertTrue(any("no done workload-identity unit" in n for n in notes))


ECR = "123456789012.dkr.ecr.us-east-1.amazonaws.com"

DEPLOY_VARIABLES = {
    "artifact_registry_destinations": [
        {"project": "acme-prod", "location": "us-central1", "repository": "images",
         "url": "us-central1-docker.pkg.dev/acme-prod/images"}],
    "lz_decisions": {"karpenter": "GKE_STANDARD_NAP",
                     "privileged_daemonsets": "GKE_STANDARD"},
}

DEPLOY_INVENTORY = {
    "nodegroups": [{"name": "system", "instance_types": ["m5.large", "m5.xlarge"]},
                   {"name": "gpu-pool", "instance_types": ["g5.2xlarge"], "gpu": True}],
    "images": [
        {"ref": f"{ECR}/api:1.0", "registry": "ecr",
         "replication": {"status": "replicated",
                         "destination": "us-central1-docker.pkg.dev/acme-prod/images/api:1.0"}},
        {"ref": f"{ECR}/web:2.0", "registry": "ecr",
         "replication": {"status": "self_service"}},
        {"ref": f"{ECR}/old:0.1", "registry": "ecr",
         "replication": {"status": "replication_failed", "destination": "x", "error": "e"}},
        {"ref": "busybox:stable", "registry": "dockerhub"},
    ],
}

PLANNED = {f"{ECR}/web:2.0": "us-central1-docker.pkg.dev/acme-prod/images/web:2.0"}


class DeploymentDerivationTest(unittest.TestCase):

    def derive(self, variables=None, inventory=None, ledger_project="meta-project",
               planned=None, cluster_scan=None):
        return exports.derive_deployment_fields(
            variables if variables is not None else dict(DEPLOY_VARIABLES),
            inventory if inventory is not None else dict(DEPLOY_INVENTORY),
            ledger_project, PLANNED if planned is None else planned, cluster_scan)

    def test_image_map_keeps_only_replicated_and_self_service(self):
        fields, _ = self.derive()
        image_map = fields["artifact_registry"]["image_map"]
        self.assertEqual(set(image_map), {f"{ECR}/api:1.0", f"{ECR}/web:2.0"},
                         "failed and never-planned images stay out")
        self.assertEqual(image_map[f"{ECR}/api:1.0"]["status"], "replicated")

    def test_self_service_entries_carry_the_planned_dest_ref(self):
        fields, _ = self.derive()
        entry = fields["artifact_registry"]["image_map"][f"{ECR}/web:2.0"]
        self.assertEqual(entry, {
            "dest_ref": "us-central1-docker.pkg.dev/acme-prod/images/web:2.0",
            "status": "self_service"},
            "no content_digest key without a recorded digest — never derived")

    def test_a_recorded_content_digest_passes_through_to_the_map(self):
        digest = "sha256:" + "c" * 64
        inventory = json.loads(json.dumps(DEPLOY_INVENTORY))
        inventory["images"][0]["replication"]["content_digest"] = digest
        inventory["images"][0]["replication"]["verified_by"] = "user_asserted"
        fields, _ = self.derive(inventory=inventory)
        entry = fields["artifact_registry"]["image_map"][f"{ECR}/api:1.0"]
        self.assertEqual(entry["content_digest"], digest)
        self.assertEqual(entry["status"], "replicated")
        self.assertEqual(entry["verified_by"], "user_asserted",
                         "a consumer pinning by this digest must be able to "
                         "tell an observed copy from an asserted one")

    def test_provenance_absent_from_the_record_stays_absent_from_the_map(self):
        entry = self.derive()[0]["artifact_registry"]["image_map"]
        self.assertNotIn("verified_by", entry[f"{ECR}/api:1.0"],
                         "never invented for a record that does not carry it")

    def test_an_unplannable_self_service_entry_gets_a_null_dest_and_a_note(self):
        fields, notes = self.derive(planned={})
        entry = fields["artifact_registry"]["image_map"][f"{ECR}/web:2.0"]
        self.assertIsNone(entry["dest_ref"])
        self.assertTrue(any("planned destination unresolved" in n for n in notes))

    def test_no_destinations_recorded_means_deployment_never_ran(self):
        fields, notes = self.derive(variables={})
        self.assertIsNone(fields["artifact_registry"])
        self.assertTrue(any("no destinations recorded" in n for n in notes))

    def test_node_shapes_summarize_the_nodegroups(self):
        fields, _ = self.derive()
        self.assertEqual(fields["node_shapes"],
                         ["system: m5.large, m5.xlarge", "gpu-pool: g5.2xlarge (gpu)"])

    def test_no_inventory_at_all_leaves_node_shapes_null(self):
        fields, notes = self.derive(inventory={})
        self.assertIsNone(fields["node_shapes"])
        self.assertTrue(any("no discovery inventory" in n for n in notes))

    def test_project_comes_from_a_literal_cluster_scan_first(self):
        fields, _ = self.derive(cluster_scan=[
            {"name": "prod", "location": "us-central1", "project": "acme-cluster-proj"}])
        self.assertEqual(fields["project"], "acme-cluster-proj")
        self.assertEqual(fields["workload_pool"], "acme-cluster-proj.svc.id.goog")

    def test_project_falls_back_to_a_destination_project_that_is_not_the_ledgers(self):
        fields, _ = self.derive()
        self.assertEqual(fields["project"], "acme-prod")
        self.assertEqual(fields["workload_pool"], "acme-prod.svc.id.goog")

    def test_a_project_equal_to_the_ledger_project_is_never_trusted(self):
        fields, notes = self.derive(ledger_project="acme-prod")
        self.assertIsNone(fields["project"], "the provisioning default reuses the "
                          "ledger project, so equality proves nothing")
        self.assertIsNone(fields["workload_pool"])
        self.assertTrue(any("ledger metadata project" in n for n in notes))

    def test_cluster_type_and_literals_from_decisions_and_scan(self):
        fields, _ = self.derive(cluster_scan=[
            {"name": "prod", "location": "us-central1", "project": None}])
        self.assertEqual(fields["cluster"],
                         {"name": "prod", "type": "standard", "location": "us-central1"})

    def test_autopilot_decision_implies_autopilot_type(self):
        variables = dict(DEPLOY_VARIABLES, lz_decisions={"karpenter": "GKE_AUTOPILOT"})
        fields, _ = self.derive(variables=variables, cluster_scan=None)
        self.assertEqual(fields["cluster"]["type"], "autopilot")

    def test_cluster_type_follows_the_registry_projection_with_triggers(self):
        # Compatibility rows 4 and 5 (coverage-guards G0): a default choice on
        # an unfired trigger is mute, so the pair no longer reads as a
        # conflict; rows 3 and 12 keep their values.
        cases = [
            ({"karpenter": "GKE_AUTOPILOT", "privileged_daemonsets": "GKE_STANDARD"},
             {"karpenter": True, "privileged_daemonsets": False}, "autopilot"),
            ({"karpenter": "GKE_STANDARD_NAP", "privileged_daemonsets": "GKE_AUTOPILOT_BYPASS"},
             {"karpenter": False, "privileged_daemonsets": True}, "autopilot"),
            ({"karpenter": "GKE_AUTOPILOT", "privileged_daemonsets": "GKE_STANDARD"},
             {"karpenter": True, "privileged_daemonsets": True}, None),
            ({"karpenter": "GKE_STANDARD_NAP", "privileged_daemonsets": "GKE_STANDARD"},
             {"karpenter": False, "privileged_daemonsets": False}, "standard"),
        ]
        for choices, triggers, expected in cases:
            with self.subTest(choices=choices, triggers=triggers):
                inventory = dict(DEPLOY_INVENTORY, triggers=dict(
                    {"gpu_tpu": False, "vpc_peering": False}, **triggers))
                fields, _ = self.derive(variables=dict(DEPLOY_VARIABLES, lz_decisions=choices),
                                        inventory=inventory, cluster_scan=None)
                self.assertEqual((fields["cluster"] or {}).get("type"), expected)

    def test_conflicting_decisions_leave_type_null_with_a_note(self):
        variables = dict(DEPLOY_VARIABLES, lz_decisions={
            "karpenter": "GKE_AUTOPILOT", "privileged_daemonsets": "GKE_STANDARD"})
        fields, notes = self.derive(
            variables=variables,
            cluster_scan=[{"name": "prod", "location": "us-central1", "project": None}])
        self.assertIsNone(fields["cluster"]["type"])
        self.assertEqual(fields["cluster"]["name"], "prod")
        self.assertTrue(any("conflicting cluster modes" in n for n in notes))

    def test_computed_cluster_attributes_stay_null_with_a_note(self):
        fields, notes = self.derive(cluster_scan=[
            {"name": None, "location": None, "project": None}])
        self.assertIsNone(fields["cluster"]["name"])
        self.assertIsNone(fields["cluster"]["location"])
        self.assertTrue(any("computed name/location" in n for n in notes))

    def test_multiple_cluster_resources_are_never_guessed_between(self):
        scan = [{"name": "a", "location": "l", "project": None},
                {"name": "b", "location": "l", "project": None}]
        fields, notes = self.derive(cluster_scan=scan)
        self.assertIsNone(fields["cluster"]["name"])
        self.assertTrue(any("not guessing which one" in n for n in notes))

    def test_conflicting_literal_cluster_projects_are_never_guessed_between(self):
        scan = [{"name": "a", "location": "l", "project": "proj-a"},
                {"name": "b", "location": "l", "project": "proj-b"}]
        fields, notes = self.derive(variables={}, cluster_scan=scan)
        self.assertIsNone(fields["project"],
                          "two literal projects is ambiguity, not evidence")
        self.assertIsNone(fields["workload_pool"])
        self.assertTrue(any("different literal projects" in n for n in notes))

    def test_agreeing_literal_cluster_projects_are_evidence(self):
        scan = [{"name": "a", "location": "l", "project": "proj-a"},
                {"name": "b", "location": "l", "project": "proj-a"}]
        fields, _ = self.derive(variables={}, cluster_scan=scan)
        self.assertEqual(fields["project"], "proj-a")

    def test_nothing_derivable_collapses_cluster_to_null(self):
        fields, notes = self.derive(variables={}, inventory={}, cluster_scan=None)
        self.assertIsNone(fields["cluster"])

    def test_staging_bucket_is_always_an_explicit_null_today(self):
        fields, notes = self.derive()
        self.assertIsNone(fields["staging_bucket"])
        self.assertTrue(any("staging bucket" in n for n in notes))


class DegradedDeploymentPublishTest(unittest.TestCase):
    """mark_replication_complete can run weeks later on a machine that never
    ran provisioning. Publishing the whole slice from there would overwrite
    the healthy machine's literals with nulls."""

    def setUp(self):
        self.bucket = FakeBucket()
        # What a healthy provisioning run left behind.
        exports.publish(self.bucket, "deployment", {
            "artifact_registry": {"image_map": {}}, "project": "acme-prod",
            "cluster": {"name": "prod", "type": "standard", "location": "us-c1"},
            "node_shapes": ["system: m5.large"],
            "workload_pool": "acme-prod.svc.id.goog", "staging_bucket": None},
            ["deployment: artifact_registry: 0 images",
             "deployment: cluster: scanned from the clone"])

    def publish(self, degraded=None):
        return exports.publish_deployment_exports(
            self.bucket, dict(DEPLOY_VARIABLES), dict(DEPLOY_INVENTORY),
            "meta-project", PLANNED, None, degraded=degraded)

    def test_a_healthy_publish_recomputes_the_whole_slice(self):
        self.assertEqual(self.publish(), "")
        doc, _ = exports.load_exports(self.bucket)
        self.assertEqual(doc["cluster"]["name"], None,
                         "with no clone to scan, a full republish is exactly "
                         "the regression the degraded leg exists to avoid")

    def test_a_degraded_publish_republishes_the_image_map_alone(self):
        note = self.publish(degraded="the target clone is not on this machine")
        self.assertIn("only the exports image_map was republished", note)
        doc, _ = exports.load_exports(self.bucket)
        self.assertEqual(doc["cluster"],
                         {"name": "prod", "type": "standard", "location": "us-c1"},
                         "the healthy machine's literals survive")
        self.assertEqual(doc["node_shapes"], ["system: m5.large"])
        self.assertEqual(set(doc["artifact_registry"]["image_map"]),
                         {f"{ECR}/api:1.0", f"{ECR}/web:2.0"})
        self.assertEqual(doc["generations"]["deployment"], 2)

    def test_a_degraded_publish_keeps_the_notes_it_did_not_recompute(self):
        self.publish(degraded="the target clone is not on this machine")
        doc, _ = exports.load_exports(self.bucket)
        notes = doc["derivation_notes"]
        self.assertIn("deployment: cluster: scanned from the clone", notes,
                      "publish replaces a source's notes wholesale, so the "
                      "notes for the kept fields must be carried forward")
        self.assertNotIn("deployment: artifact_registry: 0 images", notes,
                         "the recomputed field's stale note must not survive")
        self.assertTrue(any("keep their last published values" in n
                            for n in notes))


def key_of(entry):
    """servers/phases/deployment/datamigration.key_of, restated.

    Injected there and here for the same reason: this module must not import
    phase code. Restated rather than imported so the test exercises the
    CONTRACT — three axes, directory from the first evidence path — which is
    what a drift between the two definitions would break.
    """
    evidence = (entry.get("evidence") or [""])[0]
    # No directory for an entry known only from its ARN: its `evidence[0]`
    # is whichever file the walk met first (`datamigration.directory_of`).
    directory = "" if entry.get("detection") == "referenced" else os.path.dirname(evidence)
    return (entry.get("address"), directory, entry.get("identifier"))


def key_of_record(record):
    return (record.get("address"), record.get("directory"),
            record.get("identifier"))


def status_of(document, entry, entries=None):
    """The local restatement of the deployment phase's lookup CONTRACT.

    Exact key, then an ARN-addressed record matched on the ARN alone against
    either of the entry's handles, allowing a `*` or unstated region and
    account. Written out here rather than imported for the same reason
    `key_of` is: a drift between this and `datamigration.record_matches` is
    exactly what these tests exist to catch.
    """
    def handles(e):
        found = [e.get("address")]
        if e.get("arn") and e.get("arn") != e.get("address"):
            found.append(e.get("arn"))
        return [h for h in found if h]

    def secrets_agree(namespace, one, other):
        # One side stripped, never both, as `_secret_resources_agree` does.
        if namespace != "secretsmanager":
            return False

        def name(resource):
            head, sep, rest = (resource or "").partition(":")
            return rest if head == "secret" and sep else None

        def bare(value):
            import re
            return re.sub(
                r"-(?=[A-Za-z0-9]{6}$)(?=[A-Za-z0-9]*[a-z])(?=[A-Za-z0-9]*[A-Z])"
                r"(?=[A-Za-z0-9]*\d)[A-Za-z0-9]{6}$", "", value or "")

        a, b = name(one), name(other)
        if a is None or b is None:
            return False
        return (bare(a) != a and bare(a) == b) or (bare(b) != b and bare(b) == a)

    def agree(one, other):
        if not one or not other:
            return False
        if one == other:
            return True
        a, b = one.split(":", 5), other.split(":", 5)
        if len(a) != 6 or len(b) != 6 or a[1:3] != b[1:3]:
            return False
        if not all(not (x and y) or x == y
                   for x, y in (("" if p == "*" else p, "" if q == "*" else q)
                                for p, q in zip(a[3:5], b[3:5]))):
            return False
        return a[5] == b[5] or secrets_agree(a[2], a[5], b[5])

    # An entry the scan flagged as ambiguous matches on exact keys only:
    # several spellings of one name stand apart there on purpose, and an
    # under-specified ARN agrees with every one of them.
    ambiguous = any(
        n.startswith("named by ARNs in more than one AWS account")
        or n.startswith("its ARN spelling also matches another entry")
        or n.startswith("a console spelling of the secret ")
        for n in entry.get("notes") or [])
    twinned = any(" declared entries share this name (" in n
                  for n in entry.get("notes") or [])
    matches = []
    for record in (document or {}).get("migrations") or []:
        if not isinstance(record, dict):
            continue
        arns = [a for a in [record.get("address")] + list(record.get("aliases") or [])
                if is_literal_handle(a)]
        # Exact key first; a tolerant match only when neither the entry nor
        # the RECORD is flagged ambiguous (the record keeps its own copy of
        # the fact, since the entry's flag leaves with its sibling), and never
        # across the twinned mark.
        crosses_twinned = twinned and not record.get("twinned")
        # A block-address record never reaches a referenced entry the
        # verdicts place in another account/region/partition or among several.
        placed_elsewhere = entry.get("detection") == "referenced" and any(
            n.startswith(("in AWS account ", "named by ARNs in more than one AWS account",
                          "in AWS region ", "in AWS partition ", "the replica in AWS region ",
                          "named by more than one endpoint "))
            for n in entry.get("notes") or [])
        block_record = not is_literal_handle(record.get("address"))
        exact_handle = any(a == h for a in arns for h in handles(entry))
        # A spelling that reaches more than one entry in the section reaches
        # none — the section-wide half of the rule, asked only when the
        # caller passed the section.
        reaches = [e for e in (entries or [])
                   if any(agree(a, h) for a in arns for h in handles(e))]
        tolerant = (not ambiguous and not record.get("ambiguous_spelling")
                    and any(agree(a, h) for a in arns for h in handles(entry))
                    and (entries is None or len(reaches) < 2))
        if key_of_record(record) == key_of(entry) or (
                not crosses_twinned and not (placed_elsewhere and block_record)
                and (exact_handle or tolerant)):
            matches.append(record)
    return max(matches, key=lambda r: r.get("recorded_at") or "", default=None)


def dep(identifier, disposition="migrate", address=None, evidence="tf/main.tf",
        consumers=()):
    return {"service": "rds", "identifier": identifier,
            "address": address or f"aws_db_instance.{identifier}",
            "disposition": disposition, "evidence": [evidence],
            "consumers": list(consumers)}


class DataGateTest(unittest.TestCase):
    """The one slice that joins two objects a developer may not read."""

    def gate(self, inventory, migrations):
        fields, notes = exports.derive_data_gate(
            inventory, migrations, key_of, status_of)
        return fields["data_gate"], notes

    def test_a_move_lands_after_the_handle_changes_spelling(self):
        # The gate has to use the deployment phase's own lookup, not an index
        # of its own: while it built one, a service whose ARN handle moved
        # between scans read as never migrated here while the runbook printed
        # it as done, and every consuming component was held.
        entry = dep("orders", address="arn:aws:sqs:us-east-1:111111111111:orders")
        entry.update({"detection": "referenced",
                      "arn": "arn:aws:sqs:us-east-1:111111111111:orders"})
        migrations = {"migrations": [
            {"address": "arn:aws:sqs:::orders", "directory": "",
             "identifier": "orders", "status": "migrated",
             "recorded_at": "2026-09-01T00:00:00+00:00"}]}
        gate, notes = self.gate({"data_dependencies": [entry]}, migrations)
        self.assertEqual(gate["services"][0]["status"], "migrated")
        self.assertTrue(any("1 data service(s), 0 graded" in n for n in notes),
                        notes)

    def test_a_queue_in_another_region_is_still_gating(self):
        entry = dep("orders", address="arn:aws:sqs:eu-west-1:111111111111:orders")
        entry.update({"detection": "referenced",
                      "arn": "arn:aws:sqs:eu-west-1:111111111111:orders"})
        migrations = {"migrations": [
            {"address": "arn:aws:sqs:us-east-1:111111111111:orders",
             "directory": "", "identifier": "orders", "status": "migrated",
             "recorded_at": "2026-09-01T00:00:00+00:00"}]}
        gate, notes = self.gate({"data_dependencies": [entry]}, migrations)
        self.assertIsNone(gate["services"][0]["status"])
        self.assertTrue(any("1 data service(s), 1 graded" in n for n in notes),
                        notes)

    def test_a_reported_move_lands_on_its_entry(self):
        inventory = {"data_dependencies": [dep("orders")]}
        migrations = {"migrations": [
            {"address": "aws_db_instance.orders", "directory": "tf",
             "identifier": "orders", "status": "migrated"}]}
        gate, notes = self.gate(inventory, migrations)
        self.assertEqual(gate["services"][0]["status"], "migrated")
        self.assertIn("1 data service(s), 0 graded 'migrate'", notes[0])

    def test_the_join_is_keyed_on_all_three_axes(self):
        # Two root modules declaring the same block is two resources; a
        # completion recorded against one must not settle the other.
        inventory = {"data_dependencies": [
            dep("orders", evidence="envs/dev/main.tf"),
            dep("orders", evidence="envs/prod/main.tf")]}
        migrations = {"migrations": [
            {"address": "aws_db_instance.orders", "directory": "envs/dev",
             "identifier": "orders", "status": "migrated"}]}
        gate, _ = self.gate(inventory, migrations)
        self.assertEqual([s["status"] for s in gate["services"]],
                         ["migrated", None])
        self.assertEqual([s["directory"] for s in gate["services"]],
                         ["envs/dev", "envs/prod"])

    def test_the_directory_is_spelled_out_for_a_reader_without_evidence(self):
        gate, _ = self.gate({"data_dependencies": [dep("orders")]}, {})
        self.assertEqual(gate["services"][0]["directory"], "tf")
        self.assertNotIn("evidence", gate["services"][0],
                         "the projection is narrow on purpose")

    def test_an_unscanned_inventory_is_not_an_empty_one(self):
        gate, notes = self.gate({}, {})
        self.assertFalse(gate["scanned"])
        self.assertEqual(gate["services"], [])
        self.assertIn("scanned=false", notes[0])

    def test_a_scanned_estate_with_no_dependencies_says_scanned(self):
        gate, _ = self.gate({"data_dependencies": []}, {})
        self.assertTrue(gate["scanned"])

    def test_consumers_are_projected_without_evidence_or_notes(self):
        inventory = {"data_dependencies": [dep("orders", consumers=[
            {"workload": "orders", "kind": "service_account",
             "namespace": "orders", "source_path": None, "detection": "irsa",
             "evidence": "tf/iam.tf", "note": "reviewer said so"}])]}
        gate, _ = self.gate(inventory, {})
        self.assertEqual(gate["services"][0]["consumers"], [
            {"workload": "orders", "kind": "service_account",
             "namespace": "orders", "source_path": None, "detection": "irsa"}])

    def test_an_unattributed_gating_service_is_called_out_in_the_notes(self):
        gate, notes = self.gate({"data_dependencies": [dep("orphan")]}, {})
        self.assertTrue(any("no attributed consumer" in n for n in notes),
                        f"notes were {notes}")

    def test_keep_in_aws_stops_counting_as_owed(self):
        inventory = {"data_dependencies": [dep("dyn", disposition="keep-in-aws")]}
        _, notes = self.gate(inventory, {})
        self.assertIn("0 graded 'migrate'", notes[0])


class PublishDataExportsTest(unittest.TestCase):

    def setUp(self):
        self.bucket = FakeBucket()

    def publish(self, inventory, migrations):
        return exports.publish_data_exports(self.bucket, inventory, migrations,
                                            key_of, status_of)

    def test_a_clean_publish_writes_only_the_data_slice(self):
        exports.publish(self.bucket, "discovery",
                        {"component_seed_index": {"a": {}}}, ["discovery: n"])
        self.assertEqual(self.publish({"data_dependencies": [dep("o")]}, {}), "")
        doc = self.bucket.exports_doc()
        self.assertEqual(doc["component_seed_index"], {"a": {}})
        self.assertEqual(doc["derivation_notes"][0], "discovery: n")
        self.assertEqual(doc["generations"], {"discovery": 1, "translation": 0,
                                              "deployment": 0, "data": 1})
        self.assertEqual(doc["data_gate"]["services"][0]["identifier"], "o")

    def test_an_unreadable_outcome_store_keeps_the_last_published_slice(self):
        self.publish({"data_dependencies": [dep("o")]}, {})
        before = self.bucket.exports_doc()

        warning = self.publish({"data_dependencies": [dep("o")]}, None)

        self.assertIn("could not be read", warning)
        self.assertIn("refresh_exports", warning)
        self.assertEqual(self.bucket.exports_doc(), before, (
            "deriving from an unreadable store republishes every gating "
            "service as outstanding and holds ship gates on completions "
            "that are already recorded"))

    def test_a_vanished_section_keeps_the_last_published_slice(self):
        """The blast radius that made this a guard rather than a note.

        `carry_scan_sections` logs and returns on ANY inventory read failure,
        so `write_discovery_inventory` can reach the publish with no section
        at all. Publishing scanned=false there lays "nobody looked" over a
        good slice, and the developer-side refusal on `scanned: false` then
        holds EVERY component in the estate at its ship gate — undoable only
        by a platform engineer.
        """
        self.publish({"data_dependencies": [dep("o")]}, {})
        before = self.bucket.exports_doc()
        self.assertTrue(before["data_gate"]["scanned"])

        warning = self.publish({}, {})

        self.assertIn("no data_dependencies section", warning)
        self.assertIn("refresh_exports", warning)
        self.assertEqual(self.bucket.exports_doc(), before)

    def test_an_estate_that_never_scanned_publishes_nothing_at_all(self):
        # The other side of the same rule: no slice, rather than a slice
        # saying "never scanned". The developer-side refusal for an ABSENT
        # slice covers it — the same answer, without the clobber.
        warning = self.publish({}, {})
        self.assertIn("no data_dependencies section", warning)
        self.assertNotIn(exports.EXPORTS_BLOB, self.bucket.objects)

    def test_an_empty_scanned_section_does_publish(self):
        # The positive control. "Scanned and found nothing" must reach the
        # developer side, or every component in a data-free estate is held.
        self.assertEqual(self.publish({"data_dependencies": []}, {}), "")
        gate = self.bucket.exports_doc()["data_gate"]
        self.assertTrue(gate["scanned"])
        self.assertEqual(gate["services"], [])

    def test_a_failed_write_warns_rather_than_raising(self):
        self.bucket.upload_errors[exports.EXPORTS_BLOB] = RuntimeError("boom")
        warning = self.publish({"data_dependencies": []}, {})
        self.assertIn("boom", warning)
        self.assertIn("last published slice", warning)


class RefreshDocumentTest(unittest.TestCase):

    def setUp(self):
        self.bucket = FakeBucket()

    def per_source(self, seed_index):
        return {
            "discovery": ({"source_repo": {"url": "u", "branch": "b", "path": None},
                           "component_seed_index": seed_index},
                          ["discovery: note"]),
            "translation": ({"storage_class_menu": None, "gateway": None,
                             "gsa_bindings": None}, []),
            "deployment": ({"artifact_registry": None, "node_shapes": None,
                            "project": None, "cluster": None, "workload_pool": None,
                            "staging_bucket": None}, []),
        }

    def test_refresh_creates_the_document_and_bumps_only_real_changes(self):
        doc, changed = exports.refresh_document(self.bucket, self.per_source({}))
        self.assertEqual(changed, ["discovery"],
                         "all-null slices match the skeleton and must not bump")
        self.assertEqual(doc["generations"],
                         {"discovery": 1, "translation": 0, "deployment": 0,
                          "data": 0})

    def test_an_unchanged_recompute_bumps_nothing(self):
        exports.refresh_document(self.bucket, self.per_source({}))
        doc, changed = exports.refresh_document(self.bucket, self.per_source({}))
        self.assertEqual(changed, [])
        self.assertEqual(doc["generations"]["discovery"], 1)

    def test_a_changed_slice_bumps_its_source(self):
        exports.refresh_document(self.bucket, self.per_source({}))
        doc, changed = exports.refresh_document(
            self.bucket, self.per_source({"a.yaml": {}}))
        self.assertEqual(changed, ["discovery"])
        self.assertEqual(doc["generations"]["discovery"], 2)

    def test_notes_are_replaced_wholesale(self):
        exports.refresh_document(self.bucket, self.per_source({}))
        per_source = self.per_source({})
        per_source["discovery"] = (per_source["discovery"][0], ["discovery: fresher note"])
        doc, _ = exports.refresh_document(self.bucket, per_source)
        self.assertEqual(doc["derivation_notes"], ["discovery: fresher note"])

    def test_a_conflict_is_retried_once_then_propagates(self):
        self.bucket.conflicts[exports.EXPORTS_BLOB] = 1
        doc, changed = exports.refresh_document(self.bucket, self.per_source({}))
        self.assertEqual(doc["generations"]["discovery"], 1)
        self.bucket.conflicts[exports.EXPORTS_BLOB] = 2
        with self.assertRaises(exceptions.PreconditionFailed):
            exports.refresh_document(self.bucket, self.per_source({"b.yaml": {}}))

    def test_an_omitted_source_keeps_its_fields_notes_and_generation(self):
        # A degraded recompute environment (missing checkout, unreadable unit
        # blob) omits the source; the stored slice must survive untouched.
        exports.publish(self.bucket, "translation",
                        {"storage_class_menu": ["gp3-encrypted"]},
                        ["translation: note from the healthy hook"])
        per_source = self.per_source({})
        del per_source["translation"]

        doc, changed = exports.refresh_document(self.bucket, per_source)

        self.assertEqual(doc["storage_class_menu"], ["gp3-encrypted"],
                         "a skipped source must not regress to null")
        self.assertIn("translation: note from the healthy hook",
                      doc["derivation_notes"])
        self.assertEqual(doc["generations"]["translation"], 1)
        self.assertEqual(changed, ["discovery"])


if __name__ == "__main__":
    unittest.main()
