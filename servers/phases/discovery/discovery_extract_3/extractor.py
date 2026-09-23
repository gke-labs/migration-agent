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

"""Small-context LLM extraction workers.

Each worker is a one-shot Claude Agent SDK query: one chunk of IaC content
plus the inventory JSON schema in, one schema-conforming inventory fragment
out. Workers get no tools and no conversation history, so their context stays
tiny and their behavior is testable in isolation. The LLM call is confined to
_run_worker so it can be substituted.

Credentials are resolved from the environment by the Agent SDK at call time
(ANTHROPIC_API_KEY, or CLAUDE_CODE_USE_VERTEX=1 with gcloud ADC, etc.) —
check_llm_auth() is the preflight that turns a missing setup into an
actionable error instead of a mid-run crash.
"""

import asyncio
import json
import logging
import os

from servers.phases import agent_workers

logger = logging.getLogger("migration-dag")

EXTRACT_MODEL = os.environ.get("GKE_AGENTIC_MIGRATION_EXTRACT_MODEL", "claude-haiku-4-5")
REPORT_MODEL = os.environ.get("GKE_AGENTIC_MIGRATION_REPORT_MODEL", "claude-opus-5")
WORKER_TIMEOUT_SECONDS = float(os.environ.get("GKE_AGENTIC_MIGRATION_WORKER_TIMEOUT", "240"))
WORKER_CONCURRENCY = int(os.environ.get("GKE_AGENTIC_MIGRATION_WORKER_CONCURRENCY", "4"))

EXTRACT_SYSTEM_RULES = """You are an EKS-to-GKE migration inventory extractor.
You receive a bounded chunk of IaC configuration files (Terraform, Helm,
Kubernetes manifests) and a JSON schema. Extract only facts present in the
chunk into a JSON object conforming to the schema. Rules:
- Output ONLY a JSON object. No prose, no markdown fences.
- Omit fields you have no evidence for; never guess or fabricate values.
- Set triggers (karpenter, privileged_daemonsets, gpu_tpu, vpc_peering) to
  true only with direct evidence in this chunk; omit them otherwise.
- Record every EKS addon or platform component you see (aws_eks_addon
  resources, helm_release charts, telltale annotations) in "addons" with the
  evidence; use the canonical addon names from the detection list if given.
- Fill "storage" whenever the chunk shows storage: set ebs_csi / efs_csi for
  the CSI drivers (ebs.csi.aws.com / efs.csi.aws.com provisioners or their
  addon charts) and list every StorageClass name in storage_classes.
  Recording the driver as an addon does NOT fill this section.
- Fill "network.ingress_hosts" with every externally served hostname the
  chunk shows (Ingress rule hosts, ALB/ELB host rules, Gateway listener
  hostnames): one bare hostname per entry, no paths and no objects. They
  become the migrated Gateway's listener hostnames verbatim, so a host you
  cannot read off the chunk is a host to omit.
- File workload facts in the top-level "workloads" section (irsa_bindings as
  "namespace/sa" strings); a per-cluster copy is additional detail, not a
  substitute.
- File every hostPath volume a pod spec mounts in "workloads.host_path_volumes"
  as "namespace/workload: path" strings (the owning workload's name, not the
  volume's) — read-only mounts included; the assessment grades hostPath at
  blocker level.
- Record cluster security posture when the chunk declares it: the
  encryption_config KMS key in the cluster's "secrets_kms_key_arn", and
  access_config.authentication_mode when it is one of the schema's literal
  values (API, API_AND_CONFIG_MAP, CONFIG_MAP). Omit the mode when the IaC
  declares an expression, and never infer a value for an absent attribute.
- Record every karpenter.sh NodePool or Provisioner the chunk declares — as
  plain YAML, inside a kubectl_manifest / kubernetes_manifest body or heredoc,
  or in a Helm values file — in "autoscaling.karpenter_nodepools": its name,
  kind and apiVersion, and its requirements, taints, labels, limits, weight,
  disruption settings and nodeClassRef (as "node_class_ref") as written,
  under the schema's own field names (disruption.consolidation_policy,
  consolidate_after, expire_after, budgets; a Provisioner's consolidation
  and ttl fields under disruption.provisioner), and the file it was read
  from in the entry's own "source_files" (the merger tells two same-named
  pools apart by directory). Leave capacity_types, instance_families,
  architectures and requirements_unreduced empty; they are derived.
- Record the file paths you used in "sources"."""


# Shared worker primitives (see servers/phases/agent_workers.py). Kept as
# module-level names so tests can patch e.g. extractor._run_worker.
check_llm_auth = agent_workers.check_llm_auth
parse_worker_json = agent_workers.parse_worker_json


async def _run_worker(prompt: str, model: str) -> str:
    """Runs one tool-less, single-turn Agent SDK query and returns its text."""
    return await agent_workers.run_worker(prompt, model)


def validate_fragment(fragment: dict, schema: dict) -> str:
    """Validates a fragment against the inventory schema. Returns '' or error text."""
    try:
        import jsonschema
    except ImportError:
        logger.warning("jsonschema not installed; skipping fragment validation")
        return ""
    try:
        jsonschema.validate(fragment, schema)
    except jsonschema.ValidationError as e:
        return f"{list(e.absolute_path)}: {e.message}"
    return ""


def build_extract_prompt(chunk_content: str, schema: dict) -> str:
    return (
        f"{EXTRACT_SYSTEM_RULES}\n\n"
        f"JSON schema for your output:\n{json.dumps(schema, indent=2)}\n\n"
        f"Configuration chunk:\n{chunk_content}"
    )


async def extract_fragment(chunk_content: str, schema: dict, model: str = None) -> dict:
    """Extracts one inventory fragment from one chunk, with a single retry."""
    model = model or EXTRACT_MODEL
    prompt = build_extract_prompt(chunk_content, schema)

    last_error = None
    for attempt in range(2):
        raw = await agent_workers.call_worker(
            _run_worker, prompt, model, WORKER_TIMEOUT_SECONDS, "extraction")
        try:
            fragment = parse_worker_json(raw)
        except (ValueError, json.JSONDecodeError) as e:
            last_error = f"output was not valid JSON: {e}"
        else:
            validation_error = validate_fragment(fragment, schema)
            if not validation_error:
                return fragment
            last_error = f"output failed schema validation: {validation_error}"
        prompt = (
            f"{build_extract_prompt(chunk_content, schema)}\n\n"
            f"Your previous attempt was rejected: {last_error}. "
            "Return ONLY a corrected JSON object."
        )
        logger.warning(f"Extraction attempt {attempt + 1} rejected: {last_error}")

    raise ValueError(f"Extraction failed after retry: {last_error}")


async def extract_all(chunks_with_content: list, schema: dict, concurrency: int = None,
                      on_fragment=None) -> dict:
    """Fans out extraction over [(chunk, content)] pairs with bounded concurrency.

    Returns {"fragments": {chunk_id: fragment}, "errors": {chunk_id: message}}.
    on_fragment(chunk_id, fragment), when given, is called the moment each
    fragment is ready — the caller persists it so progress is visible while
    the remaining workers are still running. Callback failures never fail the
    extraction (the caller re-persists whatever the callback missed).
    """
    semaphore = asyncio.Semaphore(concurrency or WORKER_CONCURRENCY)
    fragments = {}
    errors = {}

    async def worker(chunk, content):
        async with semaphore:
            try:
                fragment = await extract_fragment(content, schema)
            except Exception as e:
                errors[chunk["chunk_id"]] = str(e) or type(e).__name__
                logger.error(f"Chunk {chunk['chunk_id']} extraction failed: {e!r}")
                return
            fragments[chunk["chunk_id"]] = fragment
            if on_fragment is not None:
                try:
                    on_fragment(chunk["chunk_id"], fragment)
                except Exception as e:
                    logger.warning(f"on_fragment callback failed for {chunk['chunk_id']}: {e}")

    await asyncio.gather(*(worker(c, content) for c, content in chunks_with_content))
    return {"fragments": fragments, "errors": errors}
