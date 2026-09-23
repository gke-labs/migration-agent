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

"""Container image extraction for discovery.

Pure logic — no GCS, no subprocesses. Walks the source checkout for literal
image references and for render targets (Helm chart roots, Kustomize roots)
whose manifests must be rendered before their image references are visible.
The rendering itself lives in render.py.
"""

import os
import re

from .files import SKIP_DIRS

# Files larger than this are skipped during literal extraction; a lockfile or
# vendored bundle this size is not where image references live.
MAX_FILE_BYTES = 2 * 1024 * 1024

SCAN_EXTENSIONS = (".tf", ".yaml", ".yml")

# YAML `image:` keys, plain or quoted, including list items ("- image: ...").
# Whitespace after the colon is same-line only ([ \t], not \s): a block-style
# mapping ("image:\n  repository: ...") must not capture the next line's key.
_YAML_IMAGE_RE = re.compile(
    r"""^[ \t]*(?:-[ \t]*)?['"]?image['"]?[ \t]*:[ \t]*['"]?([^\s'"#]+)""", re.MULTILINE
)
# Terraform `image = "..."` assignments.
_TF_IMAGE_RE = re.compile(r'\bimage\s*=\s*"([^"]+)"')
# ECR references anywhere in the text (superset of the eks-discovery.md
# category-9 pattern), catching refs embedded in scripts or JSON strings.
_ECR_ANYWHERE_RE = re.compile(
    r"""\b\d{12}\.dkr\.ecr\.[a-z0-9-]+\.amazonaws\.com/[^\s'"]+"""
)

_ECR_HOST_RE = re.compile(r"^\d{12}\.dkr\.ecr\.[a-z0-9-]+\.amazonaws\.com$")

_HELM_VALUES_RE = re.compile(r"^values([.-].+)?\.ya?ml$")

_KUSTOMIZATION_NAMES = ("kustomization.yaml", "kustomization.yml", "Kustomization")


def _looks_templated(ref: str) -> bool:
    """True for values that still contain template syntax and are not real refs."""
    return "{{" in ref or "${" in ref


def extract_literal_image_refs(root_dir: str) -> list[tuple[str, str]]:
    """Finds image references stated literally in checked-in files.

    Returns (ref, repo-relative file path) pairs, in discovery order, with
    duplicates preserved (dedup happens in merge_images so provenance unions).
    Templated values ({{ .Values... }}, ${var...}) are skipped — rendering
    covers those.
    """
    refs = []
    for dirpath, dirnames, filenames in os.walk(root_dir):
        # Same pruning as the manifest indexer (files.SKIP_DIRS): a vendored
        # module's image refs describe software the user does not deploy, and
        # the manifest the user scoped against never showed those directories.
        dirnames[:] = [d for d in dirnames if d not in SKIP_DIRS]
        for filename in sorted(filenames):
            if not filename.endswith(SCAN_EXTENSIONS):
                continue
            full_path = os.path.join(dirpath, filename)
            try:
                if os.path.getsize(full_path) > MAX_FILE_BYTES:
                    continue
                with open(full_path, "r", encoding="utf-8", errors="replace") as f:
                    content = f.read()
            except OSError:
                continue

            rel_path = os.path.relpath(full_path, root_dir)
            seen_in_file = set()
            for pattern in (_YAML_IMAGE_RE, _TF_IMAGE_RE, _ECR_ANYWHERE_RE):
                for match in pattern.finditer(content):
                    ref = match.group(1) if pattern.groups else match.group(0)
                    ref = ref.rstrip(",;")
                    if not ref or _looks_templated(ref) or ref in seen_in_file:
                        continue
                    seen_in_file.add(ref)
                    refs.append((ref, rel_path))
    return refs


def detect_render_targets(root_dir: str) -> list[dict]:
    """Finds Helm chart roots and Kustomize roots under root_dir.

    Returns render_targets entries per the inventory schema, status "pending".
    A directory under another chart's charts/ folder is a vendored dependency,
    not a target of its own. Every kustomization directory is a target — bases
    included; a base that fails to render standalone is recorded render_failed
    and its images arrive via the overlays that reference it.
    """
    targets = []
    chart_roots = []

    for dirpath, dirnames, filenames in os.walk(root_dir):
        dirnames[:] = [d for d in dirnames if d not in SKIP_DIRS]
        rel_dir = os.path.relpath(dirpath, root_dir)

        if "Chart.yaml" in filenames:
            chart_roots.append(rel_dir)
            values_files = sorted(f for f in filenames if _HELM_VALUES_RE.match(f))
            targets.append({
                "id": rel_dir,
                "type": "helm",
                "root": rel_dir,
                "values_files": values_files,
                "status": "pending",
                "reason": None,
            })

        if any(name in filenames for name in _KUSTOMIZATION_NAMES):
            targets.append({
                "id": rel_dir,
                "type": "kustomize",
                "root": rel_dir,
                "values_files": [],
                "status": "pending",
                "reason": None,
            })

    def _is_vendored(target):
        if target["type"] != "helm":
            return False
        target_norm = os.path.normpath(target["root"])
        for chart in chart_roots:
            chart_norm = os.path.normpath(chart)
            if target_norm != chart_norm:
                vendored_prefix = os.path.normpath(os.path.join(chart, "charts")) + os.sep
                if target_norm.startswith(vendored_prefix):
                    return True
        return False

    return [t for t in targets if not _is_vendored(t)]


def classify_registry(ref: str) -> str:
    """Buckets an image ref by registry host for the replication step."""
    host, sep, _ = ref.partition("/")
    # No path separator means no registry host at all (nginx:1.25) — Docker Hub.
    if not sep:
        return "dockerhub"
    if _ECR_HOST_RE.match(host) or host == "public.ecr.aws":
        return "ecr"
    if host.endswith("pkg.dev") or host == "gcr.io" or host.endswith(".gcr.io"):
        return "gcr_ar"
    if host == "ghcr.io":
        return "ghcr"
    if host == "quay.io" or host.endswith(".quay.io"):
        return "quay"
    if host in ("docker.io", "index.docker.io", "registry-1.docker.io"):
        return "dockerhub"
    # First component that is not host-like (library/redis) is a Docker Hub
    # namespace, not a registry.
    if "." not in host and ":" not in host and host != "localhost":
        return "dockerhub"
    return "other"


def parse_image_ref(ref: str) -> tuple[str, str | None, str | None, str]:
    """Splits a ref into (repository, tag, digest, pinned_by).

    The digest sits after '@'; the tag is after the last ':' that follows the
    last '/', so registry ports (host:5000/app) are not mistaken for tags.
    """
    digest = None
    rest = ref
    if "@" in rest:
        rest, digest = rest.split("@", 1)

    tag = None
    last_slash = rest.rfind("/")
    last_colon = rest.rfind(":")
    if last_colon > last_slash:
        rest, tag = rest[:last_colon], rest[last_colon + 1:]

    if tag and digest:
        pinned_by = "tag_and_digest"
    elif digest:
        pinned_by = "digest"
    elif tag:
        pinned_by = "tag"
    else:
        pinned_by = "none"
    return rest, tag, digest, pinned_by


def walk_yaml_for_images(documents) -> list[str]:
    """Collects every string value under an 'image' key, recursively.

    Walks the full tree rather than known paths so containers, initContainers,
    ephemeralContainers and pod templates embedded in CRD specs are all caught
    (see workload-translation.md: images hide in non-obvious places).
    """
    found = []

    def _walk(node):
        if isinstance(node, dict):
            for key, value in node.items():
                if key == "image" and isinstance(value, str) and value and not _looks_templated(value):
                    found.append(value)
                else:
                    _walk(value)
        elif isinstance(node, list):
            for item in node:
                _walk(item)

    for doc in documents:
        _walk(doc)
    return found


def merge_images(inventory: dict, refs_with_provenance: list[tuple[str, dict]]) -> None:
    """Merges (ref, provenance-entry) pairs into inventory["images"] in place.

    Dedupes by full ref; provenance entries union (exact-duplicate entries are
    dropped so re-scans stay idempotent).
    """
    by_ref = {img["ref"]: img for img in inventory["images"]}
    for ref, provenance in refs_with_provenance:
        entry = by_ref.get(ref)
        if entry is None:
            repository, tag, digest, pinned_by = parse_image_ref(ref)
            entry = {
                "ref": ref,
                "registry": classify_registry(ref),
                "repository": repository,
                "tag": tag,
                "digest": digest,
                "pinned_by": pinned_by,
                "provenance": [],
            }
            by_ref[ref] = entry
            inventory["images"].append(entry)
        if provenance not in entry["provenance"]:
            entry["provenance"].append(provenance)
