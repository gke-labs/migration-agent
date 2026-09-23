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

"""Shared Kubernetes manifest checks for phase pipelines.

Like agent_workers.py, this sits at the phases root because more than one
phase needs it: the translation steps gate their generated manifests with it
(worker output at generation time, the materialized units at validation
time), and the [PLANNED] workload phase — whose primary output IS Kubernetes
YAML — will need the same gate in front of its PR.
"""

import yaml

# Far beyond any hand-reviewable GitOps file, but small enough that parsing
# cannot become a memory event.
MAX_MANIFEST_BYTES = 1_000_000


class _NoAliasSafeLoader(yaml.SafeLoader):
    """SafeLoader that refuses aliases.

    safe_load prevents object construction but still expands anchor/alias
    references, so a few-hundred-byte billion-laughs document can balloon to
    gigabytes inside this process. Worker output is downstream of scanned
    customer IaC, which §10 treats as potentially hostile, so the gate
    refuses aliases outright — GitOps manifests don't legitimately need them,
    and failing safe costs one worker retry.
    """

    def compose_node(self, parent, index):
        if self.check_event(yaml.events.AliasEvent):
            raise yaml.YAMLError("YAML aliases are not supported in generated manifests")
        return super().compose_node(parent, index)


def load_manifest_documents(content: str) -> list:
    """Parses one manifest stream under the full hardened posture.

    The one shared parse for untrusted manifest content (worker output,
    customer IaC, chart/kustomize renders): size-capped, aliases refused,
    safe loader, multi-document. Returns the non-null documents; raises
    ValueError with a one-line reason on any refusal, so callers degrade
    visibly instead of half-parsing.
    """
    content_bytes = len(content.encode("utf-8"))
    if content_bytes > MAX_MANIFEST_BYTES:
        raise ValueError(
            f"manifest is too large ({content_bytes} bytes > {MAX_MANIFEST_BYTES})")
    try:
        docs = list(yaml.load_all(content, Loader=_NoAliasSafeLoader))
    except yaml.YAMLError as e:
        raise ValueError(f"not parseable as YAML: {e}")
    return [doc for doc in docs if doc is not None]


def manifest_structure_error(content: str) -> str:
    """Structural check on one Kubernetes manifest file. Returns '' or an error.

    Pure Python and offline: every YAML document must be a mapping carrying
    the object envelope (apiVersion, kind, metadata.name). Not schema
    validation — kubectl and the OpenAPI schemas stay out of this gate so it
    needs no cluster and no credentials (DESIGN §13). kind: List and
    metadata.generateName are rejected with the rest, which fails safe:
    GitOps manifests need one fixed-name object per document, and the worker
    retries with the error.
    """
    try:
        docs = load_manifest_documents(content)
    except ValueError as e:
        return str(e)
    if not docs:
        return "contains no YAML documents"
    for index, doc in enumerate(docs, start=1):
        if not isinstance(doc, dict):
            return f"document {index} is not a mapping"
        for field in ("apiVersion", "kind"):
            if not str(doc.get(field) or "").strip():
                return f"document {index} is missing '{field}'"
        metadata = doc.get("metadata")
        name = metadata.get("name") if isinstance(metadata, dict) else None
        if not str(name or "").strip():
            return f"document {index} (kind {doc.get('kind')}) is missing 'metadata.name'"
    return ""
