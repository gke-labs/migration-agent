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

"""Per-unit translation workers.

One subagent per translation unit: the unit's inventory slice, the relevant
landing-zone decisions, and any reviewer feedback in — Terraform and/or
Kubernetes manifest files plus an explicit tradeoffs write-up out. Workers are
tool-less single-turn Agent SDK queries (servers/phases/agent_workers.py); the
LLM call is confined to _run_worker so tests and evals can substitute it.
"""

import asyncio
import inspect
import json
import logging
import os
import re

from servers.phases import agent_workers, k8s_manifests

logger = logging.getLogger("migration-dag")

TRANSLATE_MODEL = os.environ.get("GKE_AGENTIC_MIGRATION_TRANSLATE_MODEL", "claude-opus-5")
# Full-cluster Terraform from a large model routinely runs past 10 minutes;
# the e2e run against a real estate showed 600s cutting off legitimate work.
TRANSLATE_TIMEOUT_SECONDS = float(os.environ.get("GKE_AGENTIC_MIGRATION_TRANSLATE_TIMEOUT", "1800"))
TRANSLATE_CONCURRENCY = int(os.environ.get("GKE_AGENTIC_MIGRATION_TRANSLATE_CONCURRENCY", "3"))

# Shared worker primitives, kept as module-level names so tests can patch
# translator._run_worker / translator.check_llm_auth.
check_llm_auth = agent_workers.check_llm_auth
parse_worker_json = agent_workers.parse_worker_json


async def _run_worker(prompt: str, model: str) -> str:
    return await agent_workers.run_worker(prompt, model)


TRANSLATE_RULES = """You are an EKS-to-GKE migration engineer producing Terraform
and Kubernetes manifests.
You receive ONE translation unit: a bounded problem from an approved discovery
inventory, with the relevant landing-zone decisions. Produce production-quality
output that solves this unit, plus an honest tradeoffs write-up.

Output ONLY a JSON object (no prose, no markdown fences) with this shape:
{
  "files": [{"path": "<unit-relative path ending in .tf, .yaml, or .yml>", "content": "<HCL or Kubernetes YAML>"}],
  "tradeoffs": "<markdown: every non-obvious choice, the alternatives you rejected and why, behavioral differences vs the EKS source, and cost/quota implications>",
  "assumptions": ["<assumption the reviewer must verify>", ...],
  "open_questions": ["<question only the client can answer>", ...]
}

Rules:
- Pick each file's form by the API the resource targets: Terraform (.tf, for the
  google/google-beta providers) for resources provisioned through GCP APIs;
  Kubernetes YAML (.yaml/.yml) for in-cluster objects (StorageClass, Namespace,
  ResourceQuota, Gateway, HTTPRoute, ServiceAccount, ...). Never wrap an
  in-cluster object in a kubernetes_manifest Terraform resource.
- Every YAML document must be exactly one Kubernetes object carrying apiVersion,
  kind, and metadata.name.
- Never invent project IDs, regions, or CIDRs: declare Terraform variables
  (var.project_id, var.region, var.network, ...) in a variables.tf file and
  reference them. A value the unit's own inputs record (e.g.
  inputs.target_project — the workspace's recorded target GCP project) is
  a fact, not an invention: restate it verbatim where the unit brief says
  to, including as such a variable's literal default.
- Honor the landing-zone decisions in the unit: the recorded choice the
  brief names (`inputs.decision`, and per id in `inputs.decisions`), the
  derived cluster mode where the brief threads it (`inputs.cluster_mode`),
  and the derived values under `inputs.derived_decisions`. The base cluster
  and VPC are already designed and PR'd by the landing-zone phase; do not
  re-emit them — translate only this unit's workload-specific resources.
- Prefer current, non-deprecated resource arguments; note in tradeoffs where a
  feature needs google-beta.
- Comments in HCL only where a constraint is not expressible in code.
- If a component or mapping you need is not covered by the unit inputs, put
  it in open_questions instead of inventing a mapping.
- The tradeoffs section is for the human reviewer: write it as prose, specific
  to this unit, not boilerplate."""


# Knowledge documents delivered with the prompt, per unit kind. A brief is
# the planner's statement of the facts; the document is the durable mapping
# the worker applies to them, kept out of the brief because a brief rides
# into the plan summary, the Gate C elicitation prompt, state.json and the
# review UI, and a multi-page mapping in every one of those is noise. Kept
# out of TRANSLATE_RULES because the rules are unit-agnostic. Attaching it
# here means a mapping edit reaches the next worker run without a re-plan —
# which is what makes "a new CoreDNS plugin is a documentation edit" true.
# Validated at start-up (validate_family_knowledge): a missing document
# would send a worker the raw Corefile with no instructions. Read from disk on
# every prompt build, not cached — a worker run is rare and the file is
# small, and a cache would make "the edit reaches the next run" false until
# a restart.
_PHASES_ROOT = os.path.join(os.path.dirname(os.path.realpath(__file__)), "..", "..")
FAMILY_KNOWLEDGE = {
    "cluster-dns": os.path.join("landingzone", "knowledge", "cluster-dns-translation.md"),
    "compute-class": os.path.join("landingzone", "knowledge", "gke-compute-classes.md"),
}


class FamilyKnowledgeError(Exception):
    """A unit kind's knowledge document is missing or empty. Fatal at start-up."""


def load_family_knowledge(kind: str, refresh: bool = False) -> str | None:
    """The knowledge document for a unit kind, or None when the kind has none.

    `refresh` is accepted for call-site symmetry with the other start-up
    validators; every call reads the file.
    """
    relative = FAMILY_KNOWLEDGE.get(kind)
    if relative is None:
        return None
    path = os.path.normpath(os.path.join(_PHASES_ROOT, relative))
    try:
        with open(path, "r", encoding="utf-8") as f:
            text = f.read()
    except OSError as e:
        raise FamilyKnowledgeError(
            f"unit kind {kind!r}: cannot read its knowledge document {relative}: {e}")
    if not text.strip():
        raise FamilyKnowledgeError(
            f"unit kind {kind!r}: its knowledge document {relative} is empty")
    return text


def validate_family_knowledge() -> None:
    """Start-up check: every kind in FAMILY_KNOWLEDGE has a readable document."""
    for kind in FAMILY_KNOWLEDGE:
        load_family_knowledge(kind, refresh=True)
    logger.debug(f"Family knowledge validation passed ({len(FAMILY_KNOWLEDGE)} kind(s))")


def build_translation_prompt(unit: dict, decisions: dict = None) -> str:
    payload = {
        "unit": {k: unit[k] for k in ("unit_id", "kind", "title", "inputs", "notes") if k in unit},
        "landing_zone_decisions": decisions or {},
    }
    prompt = f"{TRANSLATE_RULES}\n\nTranslation unit:\n{json.dumps(payload, indent=2, default=str)}"

    knowledge = load_family_knowledge(str(unit.get("kind") or ""))
    if knowledge:
        prompt += ("\n\n--- Unit knowledge (the mapping this unit kind applies; the "
                   "output contract in it is machine-checked at validation) ---\n"
                   + knowledge)

    if unit.get("feedback"):
        prompt += (
            "\n\nReviewer feedback on the previous attempt (address it explicitly "
            f"and mention how in tradeoffs):\n{unit['feedback']}"
        )
    return prompt


def _hcl_structure_error(content: str) -> str:
    """Checks brace structure on HCL with strings and comments stripped.

    A raw character count rejects legitimate HCL like replace(var.x, "{", "")
    and accepts garbage like '}{'; stripping quoted strings and comments and
    tracking depth (never negative, zero at end) avoids both failure modes.
    Heredocs are not parsed — a heredoc containing unbalanced braces may still
    be rejected, which fails safe (worker retries with the error).
    """
    stripped = re.sub(r'"(?:\\.|[^"\\])*"', '""', content)
    stripped = re.sub(r"(?m)(?:#|//).*$", "", stripped)
    stripped = re.sub(r"/\*.*?\*/", "", stripped, flags=re.DOTALL)
    depth = 0
    for char in stripped:
        if char == "{":
            depth += 1
        elif char == "}":
            depth -= 1
            if depth < 0:
                return "closing brace before any opening brace"
    if depth != 0:
        return f"unbalanced braces (depth {depth} at end of file)"
    return ""


def validate_translation(result: dict) -> str:
    """Structural validation of a worker result. Returns '' or an error string."""
    if not isinstance(result, dict):
        return "result is not a JSON object"
    files = result.get("files")
    if not isinstance(files, list) or not files:
        return "'files' must be a non-empty list"
    for entry in files:
        if not isinstance(entry, dict) or not entry.get("path") or not entry.get("content"):
            return "every file needs 'path' and non-empty 'content'"
        path = str(entry["path"])
        if path.startswith(("/", "..")):
            return f"file path '{path}' must be unit-relative"
        if path.endswith(".tf"):
            structure_error = _hcl_structure_error(str(entry["content"]))
        elif path.endswith((".yaml", ".yml")):
            structure_error = k8s_manifests.manifest_structure_error(str(entry["content"]))
        else:
            return f"file '{path}' must end in .tf, .yaml, or .yml"
        if structure_error:
            return f"file '{path}': {structure_error}"
    if not str(result.get("tradeoffs", "")).strip():
        return "'tradeoffs' must be a non-empty explanation"
    for key in ("assumptions", "open_questions"):
        if key in result and not isinstance(result[key], list):
            return f"'{key}' must be a list"
    return ""


async def translate_unit(unit: dict, decisions: dict = None, model: str = None) -> dict:
    """Translates one unit, with a single retry on invalid output."""
    model = model or TRANSLATE_MODEL
    prompt = build_translation_prompt(unit, decisions)

    last_error = None
    for attempt in range(2):
        raw = await agent_workers.call_worker(
            _run_worker, prompt, model, TRANSLATE_TIMEOUT_SECONDS, "translation")
        try:
            result = parse_worker_json(raw)
        except (ValueError, json.JSONDecodeError) as e:
            last_error = f"output was not valid JSON: {e}"
        else:
            validation_error = validate_translation(result)
            if not validation_error:
                return result
            last_error = f"output failed validation: {validation_error}"
        prompt = (
            f"{build_translation_prompt(unit, decisions)}\n\n"
            f"Your previous attempt was rejected: {last_error}. "
            "Return ONLY a corrected JSON object."
        )
        logger.warning(f"Translation attempt {attempt + 1} for {unit['unit_id']} rejected: {last_error}")

    raise ValueError(f"Translation failed after retry: {last_error}")


async def translate_all(
    units: list, decisions: dict = None, concurrency: int = None, on_result=None
) -> dict:
    """Fans translation out over units with bounded concurrency.

    Returns {"results": {unit_id: result}, "errors": {unit_id: message}}.

    on_result, if given, is called as on_result(unit, result, error) the moment
    each worker finishes — result set and error None on success, result None and
    error a message on failure. It lets the caller persist each unit as it lands
    (so a live progress view fills in during the multi-minute fan-out) rather
    than waiting for the whole batch. It may be a coroutine function (awaited so
    a blocking persist runs off the event loop) or a plain function. It is
    best-effort: an exception in the callback is logged and swallowed so one
    unit's persistence never fails the run or another unit.
    """
    semaphore = asyncio.Semaphore(concurrency or TRANSLATE_CONCURRENCY)
    results = {}
    errors = {}

    async def _emit(unit, result, error):
        if on_result is None:
            return
        try:
            outcome = on_result(unit, result, error)
            if inspect.isawaitable(outcome):
                await outcome
        except Exception as e:
            logger.error(f"on_result callback for {unit.get('unit_id')} failed: {e!r}")

    async def worker(unit):
        async with semaphore:
            try:
                result = await translate_unit(unit, decisions)
            except Exception as e:
                message = str(e) or type(e).__name__
                errors[unit["unit_id"]] = message
                logger.error(f"Unit {unit['unit_id']} translation failed: {e!r}")
                await _emit(unit, None, message)
            else:
                results[unit["unit_id"]] = result
                await _emit(unit, result, None)

    await asyncio.gather(*(worker(u) for u in units))
    return {"results": results, "errors": errors}
