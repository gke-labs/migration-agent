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

"""Unit tests for the pure image-extraction logic. No GCS, no subprocesses."""

import os
import tempfile
import unittest

from servers.phases.discovery.discovery_init_1 import images


def _write(root, rel_path, content):
    full = os.path.join(root, rel_path)
    os.makedirs(os.path.dirname(full), exist_ok=True)
    with open(full, "w") as f:
        f.write(content)


class ExtractLiteralImageRefsTest(unittest.TestCase):

    def test_yaml_forms(self):
        with tempfile.TemporaryDirectory() as root:
            _write(root, "deploy.yaml", """
apiVersion: apps/v1
kind: Deployment
spec:
  template:
    spec:
      containers:
      - image: 123456789012.dkr.ecr.us-east-1.amazonaws.com/payments/api:1.4.2
      - name: sidecar
        image: "nginx:1.25"
      initContainers:
      - image: 'busybox:stable'
""")
            refs = images.extract_literal_image_refs(root)
            found = {r for r, _ in refs}
            self.assertIn("123456789012.dkr.ecr.us-east-1.amazonaws.com/payments/api:1.4.2", found)
            self.assertIn("nginx:1.25", found)
            self.assertIn("busybox:stable", found)
            self.assertTrue(all(f == "deploy.yaml" for _, f in refs))

    def test_terraform_and_embedded_ecr(self):
        with tempfile.TemporaryDirectory() as root:
            _write(root, "main.tf", """
resource "aws_ecs_task_definition" "t" {
  image = "123456789012.dkr.ecr.us-west-2.amazonaws.com/web:2.0"
}
locals {
  script = "docker pull 123456789012.dkr.ecr.us-west-2.amazonaws.com/tools/dbmigrate@sha256:abc123"
}
""")
            refs = images.extract_literal_image_refs(root)
            found = {r for r, _ in refs}
            self.assertIn("123456789012.dkr.ecr.us-west-2.amazonaws.com/web:2.0", found)
            self.assertIn("123456789012.dkr.ecr.us-west-2.amazonaws.com/tools/dbmigrate@sha256:abc123", found)

    def test_templated_values_skipped(self):
        with tempfile.TemporaryDirectory() as root:
            _write(root, "chart/templates/deploy.yaml",
                   'image: "{{ .Values.image.repository }}:{{ .Values.image.tag }}"\n')
            _write(root, "vars.tf", 'image = "${var.registry}/app:latest"\n')
            self.assertEqual(images.extract_literal_image_refs(root), [])

    def test_block_style_image_mapping_not_matched(self):
        # "image:" opening a block mapping must not capture the next line's key.
        with tempfile.TemporaryDirectory() as root:
            _write(root, "values.yaml", "image:\n  repository: myrepo/app\n  tag: '1.0'\n")
            self.assertEqual(images.extract_literal_image_refs(root), [])

    def test_skips_git_dir_and_other_extensions(self):
        with tempfile.TemporaryDirectory() as root:
            _write(root, ".git/config.yaml", "image: hidden:1\n")
            _write(root, "readme.md", "image: notscanned:1\n")
            self.assertEqual(images.extract_literal_image_refs(root), [])

    def test_skips_vendored_directories_like_the_manifest_indexer(self):
        # Same pruning as files.SKIP_DIRS: a `terraform init`-ed checkout or a
        # committed vendor tree must not contribute refs or render targets the
        # scope-review manifest never showed.
        with tempfile.TemporaryDirectory() as root:
            _write(root, ".terraform/modules/eks/main.tf", 'image = "vendored:1"\n')
            _write(root, "vendor/app/deploy.yaml", "image: vendored:2\n")
            _write(root, "node_modules/pkg/chart/Chart.yaml", "name: pkg\nversion: 0.1.0\n")
            _write(root, "k8s/deploy.yaml", "image: mine:1\n")
            self.assertEqual(
                images.extract_literal_image_refs(root),
                [("mine:1", os.path.join("k8s", "deploy.yaml"))])
            self.assertEqual(images.detect_render_targets(root), [])


class DetectRenderTargetsTest(unittest.TestCase):

    def test_helm_chart_with_values_files(self):
        with tempfile.TemporaryDirectory() as root:
            _write(root, "charts/web/Chart.yaml", "name: web\n")
            _write(root, "charts/web/values.yaml", "")
            _write(root, "charts/web/values-prod.yaml", "")
            _write(root, "charts/web/values.staging.yaml", "")
            _write(root, "charts/web/notvalues.yaml", "")
            targets = images.detect_render_targets(root)
            self.assertEqual(len(targets), 1)
            t = targets[0]
            self.assertEqual(t["type"], "helm")
            self.assertEqual(t["root"], os.path.join("charts", "web"))
            self.assertEqual(t["values_files"], ["values-prod.yaml", "values.staging.yaml", "values.yaml"])
            self.assertEqual(t["status"], "pending")

    def test_vendored_chart_excluded(self):
        with tempfile.TemporaryDirectory() as root:
            _write(root, "app/Chart.yaml", "name: app\n")
            _write(root, "app/charts/postgresql/Chart.yaml", "name: postgresql\n")
            targets = images.detect_render_targets(root)
            self.assertEqual([t["root"] for t in targets], ["app"])

    def test_root_level_chart_with_vendored_subchart(self):
        with tempfile.TemporaryDirectory() as root:
            _write(root, "Chart.yaml", "name: rootchart\n")
            _write(root, "charts/postgresql/Chart.yaml", "name: postgresql\n")
            targets = images.detect_render_targets(root)
            self.assertEqual([t["root"] for t in targets], ["."])

    def test_kustomize_base_and_overlay_are_separate_targets(self):
        with tempfile.TemporaryDirectory() as root:
            _write(root, "base/kustomization.yaml", "resources: []\n")
            _write(root, "overlays/prod/kustomization.yml", "resources: [../../base]\n")
            targets = images.detect_render_targets(root)
            roots = sorted(t["root"] for t in targets)
            self.assertEqual(roots, ["base", os.path.join("overlays", "prod")])
            self.assertTrue(all(t["type"] == "kustomize" for t in targets))


class ParseAndClassifyTest(unittest.TestCase):

    def test_parse_image_ref(self):
        cases = [
            ("nginx:1.25", ("nginx", "1.25", None, "tag")),
            ("nginx", ("nginx", None, None, "none")),
            ("repo/app@sha256:abc", ("repo/app", None, "sha256:abc", "digest")),
            ("repo/app:v1@sha256:abc", ("repo/app", "v1", "sha256:abc", "tag_and_digest")),
            ("localhost:5000/app", ("localhost:5000/app", None, None, "none")),
            ("localhost:5000/app:v2", ("localhost:5000/app", "v2", None, "tag")),
        ]
        for ref, expected in cases:
            with self.subTest(ref=ref):
                self.assertEqual(images.parse_image_ref(ref), expected)

    def test_classify_registry(self):
        cases = [
            ("123456789012.dkr.ecr.us-east-1.amazonaws.com/a/b:1", "ecr"),
            ("public.ecr.aws/karpenter/controller:v1", "ecr"),
            ("us-central1-docker.pkg.dev/proj/repo/app:1", "gcr_ar"),
            ("gcr.io/proj/app:1", "gcr_ar"),
            ("ghcr.io/org/app:1", "ghcr"),
            ("quay.io/org/app:1", "quay"),
            ("docker.io/library/nginx:1", "dockerhub"),
            ("nginx:1.25", "dockerhub"),
            ("library/redis", "dockerhub"),
            ("registry.example.com/app:1", "other"),
        ]
        for ref, expected in cases:
            with self.subTest(ref=ref):
                self.assertEqual(images.classify_registry(ref), expected)


class WalkYamlForImagesTest(unittest.TestCase):

    def test_deployment_and_crd_embedded_pod_spec(self):
        deployment = {
            "kind": "Deployment",
            "spec": {"template": {"spec": {
                "containers": [{"image": "app:1"}],
                "initContainers": [{"image": "init:1"}],
                "ephemeralContainers": [{"image": "debug:1"}],
            }}},
        }
        crd_instance = {
            "kind": "MyOperator",
            "spec": {"workload": {"podTemplate": {"containers": [{"image": "operator-managed:2"}]}},
                     "ui": {"logo": {"image": "not-a-string-no-wait-it-is.png"}}},
        }
        found = images.walk_yaml_for_images([deployment, crd_instance, None])
        self.assertIn("app:1", found)
        self.assertIn("init:1", found)
        self.assertIn("debug:1", found)
        self.assertIn("operator-managed:2", found)
        # Tree walk knowingly over-collects: any string under an 'image' key counts.
        self.assertIn("not-a-string-no-wait-it-is.png", found)

    def test_templated_skipped(self):
        doc = {"spec": {"containers": [{"image": "{{ .Values.image }}"}]}}
        self.assertEqual(images.walk_yaml_for_images([doc]), [])


class MergeImagesTest(unittest.TestCase):

    def test_dedupe_and_provenance_union(self):
        inventory = {"images": []}
        images.merge_images(inventory, [
            ("app:1", {"kind": "literal", "file": "a.yaml"}),
            ("app:1", {"kind": "literal", "file": "b.yaml"}),
            ("app:1", {"kind": "literal", "file": "a.yaml"}),  # exact dup dropped
        ])
        images.merge_images(inventory, [
            ("app:1", {"kind": "rendered", "render_target": "charts/web", "values_file": "values.yaml"}),
        ])
        self.assertEqual(len(inventory["images"]), 1)
        entry = inventory["images"][0]
        self.assertEqual(entry["registry"], "dockerhub")
        self.assertEqual(entry["pinned_by"], "tag")
        self.assertEqual(len(entry["provenance"]), 3)
        kinds = {p["kind"] for p in entry["provenance"]}
        self.assertEqual(kinds, {"literal", "rendered"})


if __name__ == "__main__":
    unittest.main()
