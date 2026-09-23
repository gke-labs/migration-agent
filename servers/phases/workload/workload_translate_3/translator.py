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

"""Per-unit workload translation workers (STATE_WKLD_TRANSLATE).

Mirror of translation_translate_1/translator.py for the developer pipeline:
one tool-less, single-turn worker per unit. Workload workers emit Kubernetes
YAML ONLY — a .tf path is a named rejection, so the
coverage map's workload x terraform cell stays empty by construction.

Chart-strategy output is never gated on raw Go templates: the
unit's chart-relative files are overlaid on a copy of the SOURCE chart and
the edited chart is freshly rendered (`helm template`, in-repo values only),
then the RENDERED documents pass the shared structural gate. Kustomize
strategy gets the same treatment via `kubectl kustomize` over the copied
local graph. A missing helm/kubectl binary is RenderToolMissing — a hard
tool error naming the binary, never a skipped gate and never a worker
rejection (it would burn the retry on something no retry can fix).

Which files ride which gate is decided by a CLOSED path-shape table
(carrier_relative / kustomize markers), never by parsing content: anything
outside the table is a plain manifest file and gets the per-file structural
gate, so a flattened render can never silently ride the chart render.
"""

import asyncio
import inspect
import json
import logging
import os
import re
import shutil
import subprocess
import tempfile

import yaml

from servers.phases import agent_workers, k8s_manifests
from servers.phases.workload.workload_plan_2 import planner

logger = logging.getLogger("migration-dag")

# The same knobs the platform translator reads (§2.1: do not invent a
# parallel set) — one env var tunes both fan-outs.
TRANSLATE_MODEL = os.environ.get("GKE_AGENTIC_MIGRATION_TRANSLATE_MODEL", "claude-opus-5")
TRANSLATE_TIMEOUT_SECONDS = float(
    os.environ.get("GKE_AGENTIC_MIGRATION_TRANSLATE_TIMEOUT", "1800"))
TRANSLATE_CONCURRENCY = int(os.environ.get("GKE_AGENTIC_MIGRATION_TRANSLATE_CONCURRENCY", "3"))
RENDER_TIMEOUT_S = 120

# Cap on the chart/kustomize source files inlined into a worker prompt.
MAX_SOURCE_BYTES = 300_000

check_llm_auth = agent_workers.check_llm_auth
parse_worker_json = agent_workers.parse_worker_json


async def _run_worker(prompt: str, model: str) -> str:
    return await agent_workers.run_worker(prompt, model)


class RenderToolMissing(RuntimeError):
    """helm/kubectl is not installed — a hard tool error naming the binary."""


TRANSLATE_RULES = """You are an EKS-to-GKE migration engineer translating ONE
workload unit of a component's Kubernetes manifests. You receive the unit's
source documents (facts from the approved plan), the plan's family brief in
`notes`, and — for chart/kustomize sources — the source files themselves.

Output ONLY a JSON object (no prose, no markdown fences) with this shape:
{
  "files": [{"path": "<unit-relative path ending in .yaml or .yml>", "content": "<Kubernetes YAML>"}],
  "tradeoffs": "<markdown: every non-obvious choice, the alternatives you rejected and why, behavioral differences vs the EKS source>",
  "assumptions": ["<assumption the reviewer must verify>", ...],
  "open_questions": ["<question only the client can answer>", ...]
}

Rules:
- Kubernetes YAML ONLY. NEVER emit Terraform (.tf) or any other file form —
  workload units have no cloud-API surface; a .tf path is rejected outright.
- Every plain manifest document must be exactly one Kubernetes object
  carrying apiVersion, kind, and metadata.name.
- In PLAIN manifest files, do NOT rewrite image references, IRSA/Workload
  Identity annotations, storageClassName values, or Karpenter placement keys
  (karpenter.sh/nodepool, karpenter.sh/provisioner-name,
  karpenter.sh/capacity-type nodeSelectors and affinities, karpenter.sh/*
  tolerations) yourself: the server's deterministic pass owns those rewrites
  there (it consumes the exports maps and the compute_classes menu). Translate the surrounding document and leave those fields at
  their source values. Chart/kustomize SOURCE files are the exception —
  the pass cannot edit them, so the chart-strategy contract below assigns
  those rewrites to YOU.
- Never invent image addresses, GSA emails, storage class names, ComputeClass
  or node pool names, project IDs, or hostnames. A published exports literal your brief restates is a
  TRANSCRIPTION, not an invention — write it exactly. Anything the unit
  inputs do not state goes to open_questions instead of a guess."""

CHART_RULES = """
Chart-strategy contract (this unit's plan cites a Helm chart source):
- Strategy is YOUR judgment, recorded in tradeoffs: (a) keep the chart and
  edit it, or (b) flatten the deterministic render into plain manifests.
- Under (a), the chart directory is the shared CARRIER: the wkld-manifests
  unit emits the FULL edited chart (Chart.yaml, values files, templates/);
  wkld-identity/wkld-storage emit ONLY the files they edit, with paths
  relative to the carrier root (e.g. templates/serviceaccount.yaml), and
  cross-cite the carrier in tradeoffs. When the plan cites MORE than one
  chart, prefix every chart file with its chart path.
- Carrier files are YOURS to rewrite (the plain-manifest ban above does not
  apply inside them): the deterministic pass cannot edit chart/kustomize
  sources, so transcribe the exports literals the unit brief lists —
  replicated image_map dest_refs; for a ServiceAccount whose gsa_bindings
  email is published, replace eks.amazonaws.com/role-arn with
  iam.gke.io/gcp-service-account and drop the IRSA companion annotations;
  for a pod that selects a Karpenter pool the exports compute_classes menu
  lists, swap karpenter.sh/nodepool for cloud.google.com/compute-class with
  the same name, capacity-type spot for cloud.google.com/gke-spot "true",
  capacity-type on-demand for a required nodeAffinity
  cloud.google.com/gke-spot DoesNotExist (GKE labels spot nodes only), and
  remove karpenter.sh/* tolerations —
  into the source files so the RENDERED documents carry them. A value the
  brief does not publish stays at its source value plus an open question.
- Either way the edited chart must re-render deterministically from in-repo
  values alone (`helm template`, no --set) — the gate renders it and checks
  every rendered document, and the validate step re-runs the deterministic
  pass detect-only over the rendered output: a left-over value the pass
  would have rewritten is a BLOCKING finding.
Kustomize-strategy contract (the plan cites a kustomization source): edits
are files of the kustomization directory itself (kustomization.yaml plus the
resource files it names, dir-relative paths). Edits to bases OUTSIDE that
directory are not expressible here — flatten the render instead and say so
in tradeoffs."""


# --- per-fact knowledge --------------------------------------------------
# The workload counterpart of the platform translator's FAMILY_KNOWLEDGE,
# keyed on a persisted unit INPUT rather than a unit kind: the four workload
# families are fixed, and what varies is which facts a unit carries. A unit
# whose inputs hold a non-empty `pod_dns_facts` gets the pod DNS mapping
# document appended to its prompt; the brief carries the facts and the
# fence, the document carries what each fact maps to, and the validate
# contract (poddns_contract.py) checks the output against the facts without
# knowing the mapping. A new DNS case is therefore a documentation edit.
# Validated at start-up, read from disk on every prompt build (not cached),
# for the same reasons the platform side gives.
_PHASES_ROOT = os.path.join(os.path.dirname(os.path.realpath(__file__)), "..", "..")
FACT_KNOWLEDGE = {
    "pod_dns_facts": os.path.join("workload", "knowledge", "pod-dns-translation.md"),
    # A render source the plan could not read may hold pod specs the worker
    # flattens itself; it needs the same mapping. Same document, attached once.
    "pod_dns_unread": os.path.join("workload", "knowledge", "pod-dns-translation.md"),
}


class FactKnowledgeError(Exception):
    """A fact key's knowledge document is missing or empty. Fatal at start-up."""


def load_fact_knowledge(key: str) -> str | None:
    """The knowledge document attached for a unit input key, or None."""
    relative = FACT_KNOWLEDGE.get(key)
    if relative is None:
        return None
    path = os.path.normpath(os.path.join(_PHASES_ROOT, relative))
    try:
        with open(path, "r", encoding="utf-8") as f:
            text = f.read()
    except OSError as e:
        raise FactKnowledgeError(
            f"unit input {key!r}: cannot read its knowledge document {relative}: {e}")
    if not text.strip():
        raise FactKnowledgeError(
            f"unit input {key!r}: its knowledge document {relative} is empty")
    return text


def validate_fact_knowledge() -> None:
    """Start-up check: every key in FACT_KNOWLEDGE has a readable document."""
    for key in FACT_KNOWLEDGE:
        load_fact_knowledge(key)
    logger.debug(f"Fact knowledge validation passed ({len(FACT_KNOWLEDGE)} key(s))")


def unit_knowledge(unit: dict) -> list:
    """[(input keys joined by '/', document text)] for every knowledge
    document some non-empty input of the unit is keyed on, each document
    once, in FACT_KNOWLEDGE order."""
    inputs = (unit or {}).get("inputs") or {}
    by_document = {}
    for key, relative in FACT_KNOWLEDGE.items():
        if inputs.get(key):
            by_document.setdefault(relative, []).append(key)
    return [("/".join(keys), load_fact_knowledge(keys[0]))
            for relative, keys in by_document.items()]


def build_translation_prompt(unit: dict, worker_input: dict) -> str:
    """worker_input is the prebuilt fact payload (build_worker_input)."""
    rules = TRANSLATE_RULES
    if worker_input.get("chart_sources") or worker_input.get("kustomize_sources"):
        rules += CHART_RULES
    prompt = (f"{rules}\n\nTranslation unit:\n"
              f"{json.dumps(worker_input, indent=2, default=str)}")
    for key, knowledge in unit_knowledge(unit):
        prompt += (f"\n\n--- Unit knowledge for inputs.{key} (the mapping these "
                   "facts apply; the output contract in it is machine-checked "
                   "at validation) ---\n" + knowledge)
    if unit.get("feedback"):
        prompt += (
            "\n\nReviewer feedback on the previous attempt (address it "
            f"explicitly and mention how in tradeoffs):\n{unit['feedback']}")
    return prompt


# --- strategy detection (closed path-shape tables, never content) ----------

_VALUES_RE = re.compile(r"^values(\.[A-Za-z0-9_-]+)?\.ya?ml$")
KUSTOMIZATION_MARKERS = ("kustomization.yaml", "kustomization.yml")


def carrier_relative(path: str) -> bool:
    """Is this path a Helm-reserved shape (an edit against a chart carrier)?

    Closed list: Chart.yaml, values files, and anything under templates/,
    crds/ or charts/. Fail-closed on purpose — a path outside these shapes
    is a plain manifest and helm would silently ignore it at the chart root,
    so routing it through the render gate would leave it ungated.
    """
    head = path.split("/", 1)[0]
    if head in ("templates", "crds", "charts") and "/" in path:
        return True
    return path == "Chart.yaml" or bool(_VALUES_RE.match(path))


def unit_render_cites(unit: dict) -> dict:
    """{"helm": [chart paths], "kustomize": [dirs]} the unit's documents were
    rendered from — the plan's record of which carriers this unit may edit."""
    cites = {"helm": set(), "kustomize": set()}
    for doc in (unit.get("inputs") or {}).get("documents") or []:
        rendered = doc.get("rendered_from") or {}
        if rendered.get("type") == "helm":
            cites["helm"].add(rendered.get("chart_path"))
        elif rendered.get("type") == "kustomize":
            cites["kustomize"].add(rendered.get("kustomize_dir"))
    return {k: sorted(v) for k, v in cites.items()}


def split_output_files(unit: dict, files: list) -> tuple:
    """Routes worker files to their gates. Returns (groups, error).

    groups = {"charts": {chart_path: {rel: content}},
              "kustomize": {dir: {rel: content}},
              "plain": [(path, content)]}.
    A chart file is one whose path is `<cited chart>/<carrier-relative>` or,
    when exactly ONE chart is cited, a bare carrier-relative path. With more
    than one cited chart a bare carrier path is ambiguous — a named
    rejection, never a guess. Kustomize files are `<cited dir>/<rel>` (or
    dir-relative for a single cite) where rel is a kustomization marker or
    any dir-relative file (the render gate judges them). Everything else is
    plain and gets the per-file structural gate.
    """
    cites = unit_render_cites(unit)
    groups = {"charts": {}, "kustomize": {}, "plain": []}
    for entry in files:
        path, content = str(entry["path"]), str(entry["content"])
        owner = _route_chart(path, cites["helm"])
        if owner == "AMBIGUOUS":
            return None, (
                f"file '{path}' is a chart-relative edit but the plan cites "
                f"{len(cites['helm'])} charts — prefix the path with the "
                "chart path it edits")
        if owner is not None:
            chart, rel = owner
            groups["charts"].setdefault(chart, {})[rel] = content
            continue
        routed = _route_kustomize(path, cites["kustomize"])
        if routed is not None:
            source_dir, rel = routed
            groups["kustomize"].setdefault(source_dir, {})[rel] = content
            continue
        groups["plain"].append((path, content))
    return groups, ""


def _route_chart(path: str, charts: list):
    """(chart, carrier-relative rel) | "AMBIGUOUS" | None (not a chart file)."""
    for chart in sorted((c for c in charts if c and c != "."),
                        key=len, reverse=True):
        if path.startswith(chart + "/") and carrier_relative(path[len(chart) + 1:]):
            return chart, path[len(chart) + 1:]
    if carrier_relative(path):
        if len(charts) == 1:
            return charts[0], path
        if len(charts) > 1:
            return "AMBIGUOUS"
    return None


def _route_kustomize(path: str, dirs: list):
    """(kustomize dir, dir-relative rel) or None.

    Prefixed paths (`<cited dir>/<rel>`) are that directory's edits; a bare
    path is one only when it IS the kustomization marker of a single cited
    directory. Anything else is a plain file — a resource file the render
    would ignore must fall to the per-file gate, not ride the render.
    """
    for source_dir in sorted((d for d in dirs if d and d != "."),
                             key=len, reverse=True):
        if path.startswith(source_dir + "/"):
            return source_dir, path[len(source_dir) + 1:]
    if len(dirs) == 1 and os.path.basename(path) in KUSTOMIZATION_MARKERS \
            and "/" not in path:
        return dirs[0], path
    return None


# --- re-render gate --------------------------------------------


def _run_render_strict(cmd: list, cwd: str) -> tuple:
    """(stdout, error string). FileNotFoundError -> RenderToolMissing: the
    gate must never be skipped because a binary is absent."""
    try:
        res = subprocess.run(cmd, cwd=cwd, capture_output=True, text=True,
                             check=True, timeout=RENDER_TIMEOUT_S)
    except FileNotFoundError:
        raise RenderToolMissing(
            f"the '{cmd[0]}' binary is not installed on this workstation; "
            "the re-render gate cannot run without it")
    except subprocess.CalledProcessError as e:
        return None, (e.stderr or e.stdout or "").strip()[:800] or f"{cmd[0]} failed"
    except subprocess.TimeoutExpired:
        return None, f"{cmd[0]} timed out after {RENDER_TIMEOUT_S}s"
    return res.stdout, None


def render_chart(root: str, chart_dir: str, release: str = None) -> tuple:
    """`helm template` with in-repo values only (no --set), fixed release:
    planner.helm_release_name of the SOURCE chart path. A caller rendering a
    materialized copy under another directory name (validate's `chart/`)
    passes the source path's release so the names match the plan's."""
    release = release or planner.helm_release_name(chart_dir)
    return _run_render_strict(
        ["helm", "template", release, chart_dir or ".", "--include-crds"], root)


def render_kustomize(root: str, rel_dir: str) -> tuple:
    return _run_render_strict(["kubectl", "kustomize", rel_dir or "."], root)


def copy_tree_files(src: str, dst: str) -> None:
    """Copies regular files only — no symlinks, nothing follows a link out of
    the tree (customer IaC is untrusted input)."""
    for dirpath, dirnames, filenames in os.walk(src, followlinks=False):
        dirnames[:] = sorted(d for d in dirnames if d != ".git")
        rel = os.path.relpath(dirpath, src)
        target = dst if rel == "." else os.path.join(dst, rel)
        os.makedirs(target, exist_ok=True)
        for name in sorted(filenames):
            full = os.path.join(dirpath, name)
            if not os.path.islink(full) and os.path.isfile(full):
                shutil.copy(full, os.path.join(target, name))


def copy_source_file(source_root: str, rel: str, dst_root: str) -> None:
    """Copies one source-root-relative regular file into the same relative
    place under dst_root. Symlinks and escapes are skipped, and an existing
    destination (already carried by a directory copy) is left alone."""
    real_root = os.path.realpath(source_root)
    src = os.path.join(source_root, rel)
    if os.path.islink(src) or not os.path.isfile(src) \
            or not os.path.realpath(src).startswith(real_root + os.sep):
        return
    dst = os.path.join(dst_root, rel)
    if os.path.exists(dst):
        return
    os.makedirs(os.path.dirname(dst), exist_ok=True)
    shutil.copy(src, dst)


def overlay_files(root: str, files: dict) -> str:
    """Writes {rel: content} under root, realpath-escape-guarded.
    Returns '' or the offending path (the caller names the rejection)."""
    real_root = os.path.realpath(root)
    for rel, content in sorted(files.items()):
        full = os.path.realpath(os.path.join(root, rel))
        if full != real_root and not full.startswith(real_root + os.sep):
            return rel
        os.makedirs(os.path.dirname(full), exist_ok=True)
        with open(full, "w", encoding="utf-8") as f:
            f.write(content)
    return ""


def _source_dirs_for(kind: str, source_root: str, rel_dir: str) -> tuple:
    """(dirs, files) the render actually reads: the chart dir itself, or the
    kustomization's whole local graph.

    Bases live outside the overlay directory and the copy must carry them —
    and so must the graph's FILES: `resources: - ../../shared/ns.yaml`
    resolves a plain file in a directory that has no kustomization.yaml of
    its own, so it is in graph['files'] and in NEITHER graph['dirs'] nor the
    overlay dir. build_worker_input already shows the worker those files;
    copying only the dirs made the gate reject an overlay the worker was
    shown complete, burning its single retry on an unfixable rejection.
    """
    if kind == "helm":
        return [rel_dir], []
    graph = planner.resolve_kustomize_graph(source_root, rel_dir)
    return graph["dirs"], graph["files"]


def rerender_error(kind: str, source_root: str, rel_dir: str, files: dict,
                   renderers: dict = None) -> str:
    """The chart/kustomize re-render gate over one edited source.

    Copies the SOURCE tree(s) to a temp dir, overlays the unit's edited
    files, renders deterministically (in-repo values only, nothing fetched)
    and structural-checks every rendered document. Returns '' or the
    rejection. Raw Go templates never reach the YAML loader: only the
    rendered stream is parsed.
    """
    renderers = renderers or {"helm": render_chart, "kustomize": render_kustomize}
    label = rel_dir if rel_dir and rel_dir != "." else ""
    src = os.path.join(source_root, label) if label else source_root
    if not os.path.isdir(src):
        return (f"{kind} source '{rel_dir or '.'}' does not exist under the "
                "source root; the re-render gate cannot run")
    tmp = tempfile.mkdtemp(prefix="wkld-rerender-")
    try:
        graph_dirs, graph_files = _source_dirs_for(kind, source_root, rel_dir)
        for graph_dir in graph_dirs:
            g_label = graph_dir if graph_dir and graph_dir != "." else ""
            copy_tree_files(
                os.path.join(source_root, g_label) if g_label else source_root,
                os.path.join(tmp, g_label) if g_label else tmp)
        for graph_file in graph_files:
            copy_source_file(source_root, graph_file, tmp)
        target = os.path.join(tmp, label) if label else tmp
        escaped = overlay_files(target, files)
        if escaped:
            return f"file '{escaped}' escapes the {kind} source directory"
        text, error = renderers[kind](tmp, rel_dir if label else ".")
        if error is not None:
            return (f"the edited {kind} source '{rel_dir or '.'}' does not "
                    f"render deterministically: {error}")
        structure = k8s_manifests.manifest_structure_error(text)
        if structure:
            return (f"the re-rendered output of '{rel_dir or '.'}' fails the "
                    f"structural gate: {structure}")
        return ""
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


def helm_partial_error(chart: str, files: dict) -> str:
    """'' or the rejection for an emitted Helm PARTIAL (templates/_*).

    carrier_relative routes everything under templates/ to the render gate,
    but helm never emits underscore-prefixed files into the rendered stream —
    so a partial's content is structurally ungated end to end: the render
    gate never sees it and the plain-file gate was skipped. This is the helm
    analogue of the kustomize fail-open corner (DESIGN §14 issue 19), and it
    is closed the only fail-closed way available: refuse the emission.

    Only YAML-extension partials reach here; `templates/_helpers.tpl` is
    already refused by the .yaml/.yml extension rule above.
    """
    offenders = sorted(
        rel for rel in files
        if rel.startswith("templates/")
        and os.path.basename(rel).startswith("_"))
    if not offenders:
        return ""
    return (
        f"file(s) {', '.join(f'{chart}/{o}' if chart not in ('', '.') else o for o in offenders)} "
        "are Helm partials: helm skips underscore-prefixed template files "
        "when it renders, so nothing would ever gate their content. Emit the "
        "change in the template(s) that INCLUDE the partial instead, or "
        "flatten this chart to plain manifests")


# --- envelope validation -----------------------------------------------------


def validate_workload_translation(result, unit: dict,
                                  render_ctx: dict = None) -> str:
    """Structural validation of a workload worker result. '' or an error.

    render_ctx: {"source_root": str, "renderers": {...}} — required whenever
    the unit's plan cites a chart/kustomize source; tests inject renderers.
    """
    if not isinstance(result, dict):
        return "result is not a JSON object"
    files = result.get("files")
    if not isinstance(files, list) or not files:
        return "'files' must be a non-empty list"
    for entry in files:
        if not isinstance(entry, dict) or not entry.get("path") \
                or not entry.get("content"):
            return "every file needs 'path' and non-empty 'content'"
        path = str(entry["path"])
        normalized = os.path.normpath(path)
        if os.path.isabs(normalized) or normalized.split(os.sep)[0] == "..":
            return f"file path '{path}' must be unit-relative"
        if path.endswith(".tf"):
            return (f"file '{path}' is Terraform — workload units emit "
                    "Kubernetes YAML only, never .tf (the workload x "
                    "terraform cell of the coverage map is empty by "
                    "construction)")
        if not path.endswith((".yaml", ".yml")):
            return f"file '{path}' must end in .yaml or .yml"
    groups, split_error = split_output_files(unit, files)
    if split_error:
        return split_error
    for path, content in groups["plain"]:
        structure = k8s_manifests.manifest_structure_error(content)
        if structure:
            return f"file '{path}': {structure}"
    if groups["charts"] or groups["kustomize"]:
        if not render_ctx or not render_ctx.get("source_root"):
            return ("this unit edits a chart/kustomize source but no source "
                    "root is available for the re-render gate")
        renderers = (render_ctx or {}).get("renderers")
        for chart, chart_files in sorted(groups["charts"].items()):
            partial = helm_partial_error(chart, chart_files)
            if partial:
                return partial
            error = rerender_error("helm", render_ctx["source_root"], chart,
                                   chart_files, renderers)
            if error:
                return error
        for rel_dir, dir_files in sorted(groups["kustomize"].items()):
            error = rerender_error("kustomize", render_ctx["source_root"],
                                   rel_dir, dir_files, renderers)
            if error:
                return error
    if not str(result.get("tradeoffs", "")).strip():
        return "'tradeoffs' must be a non-empty explanation"
    for key in ("assumptions", "open_questions"):
        if key in result and not isinstance(result[key], list):
            return f"'{key}' must be a list"
    return ""


# --- worker input (the plan holds locators; content loads here) -------------


def _stream_docs(source_root: str, path: str, rendered_from, cache: dict) -> list:
    """The parsed documents of one plan-recorded stream, cached per fan-out."""
    key = json.dumps(rendered_from or {"file": path}, sort_keys=True)
    if key in cache:
        return cache[key]
    if rendered_from is None:
        real_root = os.path.realpath(source_root)
        full = os.path.realpath(os.path.join(source_root, path))
        if full != real_root and not full.startswith(real_root + os.sep):
            raise ValueError(f"plan path '{path}' escapes the source root")
        with open(full, "r", encoding="utf-8") as f:
            text = f.read()
    elif rendered_from.get("type") == "helm":
        text, error = render_chart(source_root, rendered_from["chart_path"])
        if error is not None:
            raise ValueError(f"chart '{rendered_from['chart_path']}' no "
                             f"longer renders: {error}")
    else:
        text, error = render_kustomize(source_root,
                                       rendered_from["kustomize_dir"])
        if error is not None:
            raise ValueError(f"kustomization '{rendered_from['kustomize_dir']}'"
                             f" no longer renders: {error}")
    cache[key] = k8s_manifests.load_manifest_documents(text)
    return cache[key]


def _source_files(source_root: str, rel_dirs: list, walker) -> dict:
    """{path: content} for the cited source dirs' YAML files, size-capped —
    oversized files are named, never silently dropped."""
    out, budget = {}, MAX_SOURCE_BYTES
    for rel_dir in rel_dirs:
        for rel in walker(rel_dir):
            full = os.path.join(source_root, rel)
            if os.path.islink(full) or not os.path.isfile(full):
                continue
            size = os.path.getsize(full)
            if size > budget:
                out[rel] = f"<omitted: {size} bytes exceeds the prompt budget>"
                continue
            with open(full, "r", encoding="utf-8", errors="replace") as f:
                out[rel] = f.read()
            budget -= size
    return out


def build_worker_input(unit: dict, plan: dict, source_root: str,
                       cache: dict = None) -> dict:
    """The fact payload one worker receives. Raises ValueError when a source
    the plan recorded cannot be loaded (an honest per-unit error, no guess).
    """
    cache = cache if cache is not None else {}
    documents = []
    for locator in (unit.get("inputs") or {}).get("documents") or []:
        docs = _stream_docs(source_root, locator["path"],
                            locator.get("rendered_from"), cache)
        index = locator.get("doc_index", 0)
        if not 0 <= index < len(docs):
            raise ValueError(
                f"stream '{locator['path']}' has {len(docs)} document(s) but "
                f"the plan recorded doc_index {index} — the source changed "
                "since the plan was built; re-plan before translating")
        documents.append({
            **{k: locator.get(k) for k in ("path", "doc_index", "kind",
                                           "namespace", "name",
                                           "classification", "rendered_from")},
            "content": yaml.safe_dump(docs[index], sort_keys=False),
        })
    cites = unit_render_cites(unit)

    def _chart_walker(rel_dir):
        base = source_root if rel_dir in ("", ".") \
            else os.path.join(source_root, rel_dir)
        prefix = "" if rel_dir in ("", ".") else rel_dir + "/"
        for dirpath, dirnames, filenames in os.walk(base, followlinks=False):
            dirnames[:] = sorted(d for d in dirnames if d != ".git")
            for name in sorted(filenames):
                if name.endswith((".yaml", ".yml")):
                    rel = os.path.relpath(os.path.join(dirpath, name), base)
                    yield prefix + rel.replace(os.sep, "/")

    def _kustomize_walker(rel_dir):
        return planner.resolve_kustomize_graph(source_root, rel_dir)["files"]

    payload = {
        "unit": {k: unit.get(k) for k in ("unit_id", "family", "title", "notes")},
        "documents": documents,
        "carriers": [c for c in plan.get("carriers") or []
                     if c.get("chart_path") in cites["helm"]],
    }
    # The persisted facts a knowledge document is keyed on ride with the
    # unit so the worker reads them structured, not only as brief prose.
    for key in FACT_KNOWLEDGE:
        if (unit.get("inputs") or {}).get(key):
            payload["unit"][key] = unit["inputs"][key]
    if cites["helm"]:
        payload["chart_sources"] = _source_files(
            source_root, cites["helm"], _chart_walker)
    if cites["kustomize"]:
        payload["kustomize_sources"] = _source_files(
            source_root, cites["kustomize"], _kustomize_walker)
    return payload


# --- the worker loop (one retry, bounded fan-out) ---------------------------


async def translate_unit(unit: dict, worker_input: dict,
                         render_ctx: dict = None, model: str = None,
                         post_pass=None) -> dict:
    """Translates one unit, with a single retry on invalid output.

    post_pass(unit, result) -> result runs AFTER the gates pass and before
    the result is returned — the deterministic transforms hook.
    RenderToolMissing propagates immediately: a missing binary is not the
    worker's fault, so no retry is spent on it.
    """
    model = model or TRANSLATE_MODEL
    prompt = build_translation_prompt(unit, worker_input)
    last_error = None
    for attempt in range(2):
        raw = await agent_workers.call_worker(
            _run_worker, prompt, model, TRANSLATE_TIMEOUT_SECONDS,
            "workload-translation")
        try:
            result = parse_worker_json(raw)
        except (ValueError, json.JSONDecodeError) as e:
            last_error = f"output was not valid JSON: {e}"
        else:
            validation_error = validate_workload_translation(
                result, unit, render_ctx)
            if not validation_error:
                return post_pass(unit, result) if post_pass else result
            last_error = f"output failed validation: {validation_error}"
        prompt = (
            f"{build_translation_prompt(unit, worker_input)}\n\n"
            f"Your previous attempt was rejected: {last_error}. "
            "Return ONLY a corrected JSON object.")
        logger.warning(f"Workload translation attempt {attempt + 1} for "
                       f"{unit['unit_id']} rejected: {last_error}")
    raise ValueError(f"Workload translation failed after retry: {last_error}")


async def translate_all(units: list, worker_inputs: dict,
                        render_ctx: dict = None, concurrency: int = None,
                        on_result=None, post_pass=None) -> dict:
    """Fans workload translation out with bounded concurrency.

    Returns {"results": {unit_id: result}, "errors": {unit_id: message}}.
    on_result(unit, result, error) fires the moment each worker finishes so
    the caller persists incrementally; it is best-effort — a callback
    exception is logged and swallowed, never failing the run or another
    unit (the platform translator's semantics, copied deliberately).
    """
    semaphore = asyncio.Semaphore(concurrency or TRANSLATE_CONCURRENCY)
    results, errors = {}, {}

    async def _emit(unit, result, error):
        if on_result is None:
            return
        try:
            outcome = on_result(unit, result, error)
            if inspect.isawaitable(outcome):
                await outcome
        except Exception as e:
            logger.error(
                f"on_result callback for {unit.get('unit_id')} failed: {e!r}")

    async def worker(unit):
        async with semaphore:
            try:
                result = await translate_unit(
                    unit, worker_inputs[unit["unit_id"]], render_ctx,
                    post_pass=post_pass)
            except Exception as e:
                message = str(e) or type(e).__name__
                errors[unit["unit_id"]] = message
                logger.error(f"Unit {unit['unit_id']} workload translation "
                             f"failed: {e!r}")
                await _emit(unit, None, message)
            else:
                results[unit["unit_id"]] = result
                await _emit(unit, result, None)

    await asyncio.gather(*(worker(u) for u in units))
    return {"results": results, "errors": errors}
