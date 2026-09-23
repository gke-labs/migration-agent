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

"""MCP tool for the workload validate step (STATE_WKLD_VALIDATE).

The Gate E analog minus everything Terraform: the done
units materialize into the target clone under workloads/<component>/
(carrier first, then owned-file edits — materialize.py), and the gates are
manifest-shaped ONLY: the shared structural check, the chart/kustomize
re-render gate over the materialized output, and the deterministic
detect-only re-check (transforms.apply_transforms over the rendered and
plain shipped documents — a left-over `change` is blocking; DESIGN §14
issue 20). No terraform, no LLM auto-fix loop — a failing unit returns to
review with the finding (the deliberate asymmetry with the platform
validate step).

Also here: the exports staleness cross-check (every blob's stamped
generations against the CURRENT exports.json), the
ship-completeness check (parked units listed, never blocking), and — when
everything is clean — the dispatch-loop walk through the ship elicitation
(STATE_WKLD_APPROVED) and the per-component PR submission.

THE DATA GATE IS NOT A VALIDATION FINDING. A component whose database is
still in AWS has nothing wrong with its manifests, so it must not take the
on_failure edge to STATE_WKLD_REVIEW: there is no unit to revise, no plan to
rebuild, and the developer cannot clear it from any state in their graph —
both exits are platform-side tools. It is a PARK. The gates all run, the
report and the comparisons are persisted, and then the walk simply does not
raise the ship elicitation; the component stays at STATE_WKLD_VALIDATE and
re-running this tool re-checks. That mirrors what the platform walk does at
STATE_DEPLOYMENT_DATA_MIGRATION over the same fact, and what the planner
already does with a unit whose attach point has not published.
"""

import json
import logging
import os
import shutil
import uuid

from google.api_core import exceptions
from mcp.server.fastmcp import Context

from servers.dag.state_management import (
    get_bucket_name,
    load_dag,
    authorize_and_rehydrate_workload,
)
import servers.dag.state_management as state_mgr
from servers.dag.server import exports as exports_lib
from servers.dag.server import git_client
from servers.dag.dispatch import run_dispatch_loop
from servers.phases.landingzone import workspace
from servers.phases.landingzone.workspace import target_clone_path
from servers.phases.translation.translation_validate_3 import validation
from servers.phases import k8s_manifests

from .. import datagate
from ..workload_plan_2 import planner
from ..workload_scope_1 import seed as seed_lib
from ..workload_translate_3 import transforms
from ..workload_translate_3 import translator
from ..workload_translate_3.tools import unit_blob_path
from . import materialize
from . import poddns_contract

logger = logging.getLogger("migration-dag")

VALIDATE_STATE = "STATE_WKLD_VALIDATE"


def report_blob_path(component: str) -> str:
    return f"workloads/{component}/validation-report.json"


def comparisons_blob_path(component: str) -> str:
    return f"workloads/{component}/comparisons.json"


def _confirmed_scope_paths(bucket, component: str, exports_doc) -> tuple:
    """The files this component ships, for the data gate to attribute against.

    The CONFIRMED scope, not the draft in the state variables: what the
    developer signed off is what the component ships.

    A DEGRADED scope is re-resolved here rather than given up on.
    `submit_workload_scope` persists `resolved_paths: null` whenever it ran
    while `exports.component_seed_index` was absent or empty — routine
    pipeline timing, not an error — and the developer graph has no edge back
    to STATE_WKLD_SCOPE afterwards, so that null is permanent. Taking it at
    face value would disable the gate for the whole component, for ever, and
    silently: not one service unheld but every one of them. The confirmed
    `included`/`excluded` globs are still recorded, and by validate time the
    index has almost always published, so this re-runs the very resolution
    the confirmation would have done. Read-only and never persisted — the
    scope the developer signed off is the globs, and this is only the gate
    reading them.

    Returns (paths, unreadable, reresolved). The three outcomes are NOT
    interchangeable, and collapsing them into a bare None is what made this
    fail open: a transient 503 on scope.json read as "this component has no
    file list", which cleared the gate and shipped the component while
    telling the developer something false about a scope that was fine.
    `unreadable` holds instead, because a re-run clears it — the same
    discipline the sibling exports.json read on this path already had.
    `reresolved` says the paths came from globs nobody verified against the
    index, which suppresses the `elsewhere` claim downstream.

    `paths` None with `unreadable` False is the honest empty case: a degraded
    scope whose globs still resolve to nothing. `datagate.verdict` reads that
    as "cannot tell here" — every consumer-bearing service lands in
    `untestable`, which reports and holds nobody (DESIGN §14 issue 33).
    """
    try:
        doc = json.loads(bucket.blob(
            f"workloads/{component}/scope.json").download_as_text())
        if not isinstance(doc, dict):
            # Inside the try on purpose. A body of `null` or `[]` parses
            # fine and then `doc.get` raises AttributeError — out of the
            # tool, unhandled, AFTER the whole validation run and the report
            # write, naming no object. It is the same class of input as the
            # truncated body one line up, which is classified properly, so
            # it gets the same answer.
            raise ValueError(
                f"scope.json is {type(doc).__name__}, not an object")
    except exceptions.NotFound:
        # ABSENT is a definite answer, not a failed read: re-running will not
        # produce the object, so holding on it would wedge the component with
        # no remedy to name. It is also anomalous rather than routine —
        # plan_workload_translation refuses without a confirmed scope — so
        # the honest reading is "no file list here", which reports and ships
        # like every other thing this gate cannot test.
        logger.warning(f"data gate: no scope.json for {component}")
        return None, False, False
    except Exception as e:
        # Everything else — a 503, a 429, a truncated body, malformed JSON —
        # is a read that FAILED. Clearing the component on one of those is
        # the fail-open this distinction exists to close.
        logger.warning(f"data gate: scope.json unreadable ({e})")
        return None, True, False
    paths = doc.get("resolved_paths")
    if isinstance(paths, list):
        return paths, False, False
    index = (exports_doc or {}).get("component_seed_index")
    if not doc.get("included") or not index:
        return None, False, False
    try:
        resolved = seed_lib.resolve_component_scope(list(index.keys()), doc)
    except Exception as e:
        logger.warning(f"data gate: could not re-resolve a degraded scope ({e})")
        return None, False, False
    return (resolved, False, True) if resolved else (None, False, False)


def _completeness_findings(plan: dict) -> list:
    """Decision 10: every unit that is not a placeholder, not skipped and
    not parked must be done before the ship elicitation may rise."""
    return [
        {"unit_id": u["unit_id"], "status": u["status"],
         "error": ("unit is not done — revise, re-run, or skip it before "
                   "the component can ship")}
        for u in plan.get("units", [])
        if u["status"] not in ("done", "skipped", "parked")]


def _brief_generations(generations):
    """The generations map minus the sources a brief cannot depend on.

    Today that is `data` alone. Its slice republishes every time an operator
    reports a database landed or flipped one to keep-in-aws — a cadence
    measured in weeks of a customer's data migration, not in phase
    completions — and none of it is brief input: no unit's manifests change
    because a Postgres finished copying. Left in the comparison, the gate
    would demand a full replan of every in-flight component on each report,
    which costs more than the staleness it would be protecting against.

    Filtering both sides rather than the stored one also means a stamp taken
    before this source existed still compares equal.
    """
    if not isinstance(generations, dict):
        return generations
    return {k: v for k, v in generations.items()
            if k != exports_lib.DATA_SOURCE}


def _staleness_findings(done_entries: list, current_generations,
                        exports_present: bool = True) -> list:
    """Decision 8: a blob whose stamped generations differ from the CURRENT
    exports.json names the unit and both values — never silently passed.

    The remedy names the action that clears it:
    approve_workload_translation(action='replan'). Retranslating normally
    cannot — every blob is stamped with the PLAN's frozen exports_stamp, so a
    retranslate re-writes the same stale stamp; only a re-plan rebuilds the
    stamp (and the briefs) from the current exports. The ONE exception is the
    gateway unpark at translate entry, which refreshes the stamp itself and
    re-queues the done units so they re-run against it — and refuses outright
    when anything beyond `gateway` moved, precisely so it can never advance a
    stamp over a brief it cannot rebuild.
    """
    findings = []
    current_generations = _brief_generations(current_generations)
    for entry in done_entries:
        stamped = _brief_generations(entry.get("exports_generations"))
        if stamped == current_generations:
            continue
        if not exports_present:
            detail = ("the unit was translated against exports generations "
                      f"{stamped}, but this ledger has no readable "
                      "exports.json at all — the platform pipeline has not "
                      "published one, or it was deleted. Restore or "
                      "re-publish the exports before shipping")
        else:
            detail = ("the unit was translated against exports generations "
                      f"{stamped}, but exports.json now records "
                      f"{current_generations} — clear it with "
                      "approve_workload_translation(action='replan'), which "
                      "rebuilds the plan's briefs and exports_stamp from the "
                      "current exports and re-translates the affected units "
                      "(retranslating alone re-writes the SAME stale stamp)")
        findings.append({
            "unit_id": entry["unit"]["unit_id"],
            "blob_generations": stamped,
            "current_generations": current_generations,
            "exports_present": exports_present,
            "error": detail})
    return findings


def _ledger_findings(done_entries: list) -> list:
    """A plan unit marked done whose ledger blob is not a successful result.

    The plan's status is not proof: the blob can be overwritten by a
    superseded run (DESIGN §14 issue 2) or truncated. Without this check the
    unit materializes ZERO files, raises nothing, and the component ships a
    PR silently missing that unit's output.
    """
    findings = []
    for entry in done_entries:
        unit_id = (entry.get("unit") or {}).get("unit_id", "(unnamed)")
        result = entry.get("result")
        status = (entry.get("unit") or {}).get("status")
        if status != "done":
            reason = f"the blob records status {status!r}, not 'done'"
        elif not isinstance(result, dict):
            reason = "the blob carries no result object"
        elif not result.get("files"):
            reason = "the blob's result carries no files"
        else:
            continue
        findings.append({
            "unit_id": unit_id,
            "error": (f"the plan marks '{unit_id}' done but {reason} — "
                      "materializing it would silently ship nothing for this "
                      "unit. Re-run run_workload_translation for it, or skip "
                      "it deliberately with skip_workload_units")})
    return findings


def _render_findings(clone_dir: str, render_dirs: list) -> tuple:
    """(findings, rendered) — the re-render gate over the MATERIALIZED
    chart/kustomize output. `rendered` pairs each structurally-passing
    entry with its rendered text, so the transforms detect gate re-checks
    the SAME bytes this gate saw instead of rendering twice.
    RenderToolMissing propagates — a missing binary aborts the tool with a
    hard error naming it, never a skipped gate."""
    findings, rendered = [], []
    for entry in render_dirs:
        if entry["kind"] == "helm":
            # The materialized copy of a root chart lives under `chart/`;
            # the release name must stay the SOURCE path's or every
            # `.Release.Name`-derived name differs from the plan's render.
            text, error = translator.render_chart(
                clone_dir, entry["dir"],
                release=planner.helm_release_name(entry.get("source") or ""))
        else:
            text, error = translator.render_kustomize(clone_dir, entry["dir"])
        if error is not None:
            findings.append({**entry, "error": error})
            continue
        structure = k8s_manifests.manifest_structure_error(text)
        if structure:
            findings.append({**entry, "error": f"rendered output: {structure}"})
            continue
        rendered.append((entry, text))
    return findings, rendered


def _transform_gate(clone_dir: str, rendered: list, unit_dirs: list,
                    exports_doc) -> dict:
    """The detect-only deterministic re-check over the SHIPPED tree (DESIGN
    §14 issue 20's validate half): every rendered chart/kustomize document
    and every materialized plain manifest goes through
    transforms.apply_transforms against the CURRENT exports. A `change`
    finding is a value the deterministic pass would still rewrite sitting
    in shipped output — BLOCKING, because on plain files the pass already
    ran (an idempotence regression) and in chart sources the rewrite is the
    worker's briefed transcription duty. open_question/warning/ok findings
    stay non-blocking: an honest gap (unpublished map, unmapped ref) must
    not deadlock review. Returns {"checked", "blocking", "advisory"}."""
    blocking, counts = [], {"checked": 0, "advisory": 0}

    def check(source_label, text):
        counts["checked"] += 1
        try:
            docs = k8s_manifests.load_manifest_documents(text)
        except ValueError as e:
            blocking.append({"source": source_label, "error": str(e)})
            return
        _, findings = transforms.apply_transforms(docs, exports_doc)
        for f in findings:
            if f["category"] == "change":
                blocking.append({
                    "source": source_label, "transform": f["transform"],
                    "locator": f["locator"],
                    "error": (f"{f['detail']} — the deterministic pass "
                              "would still rewrite this in the shipped "
                              "output; the carrier transcription (or the "
                              "plain-file post-pass) missed it, or exports "
                              "moved since the unit was translated — "
                              "re-translate the unit")})
            elif f["category"] in ("open_question", "warning"):
                counts["advisory"] += 1

    for entry, text in rendered:
        check(entry["dir"], text)
    for unit_dir in unit_dirs:
        base = os.path.join(clone_dir, unit_dir)
        for dirpath, dirnames, filenames in os.walk(base):
            dirnames.sort()
            for name in sorted(filenames):
                if not name.endswith((".yaml", ".yml")):
                    continue
                full = os.path.join(dirpath, name)
                rel = os.path.relpath(full, clone_dir)
                try:
                    with open(full, "r", encoding="utf-8") as f:
                        check(rel, f.read())
                except (OSError, UnicodeDecodeError) as e:
                    blocking.append({
                        "source": rel,
                        "error": f"unreadable materialized file: {e}"})
    return {"checked": counts["checked"], "blocking": blocking,
            "advisory": counts["advisory"]}


def _target_repo_coordinates(bucket, variables: dict, exports_doc) -> tuple:
    """(url, branch, source_note) — the ship coordinates, in priority order:
    exports.target_repo, then the component's own variables, then a
    best-effort read of platform/onboarding/state.json (where
    configure_repositories actually records them — an admin affordance:
    the §4.5 ledger IAM 403s developers on platform/, by design, which is
    exactly why exports.target_repo is the developer channel). exports is
    AUTHORITATIVE over the variables: the variables tier is only the cache
    the previous resolution wrote back, so reading it first would freeze
    the first-ever answer — the platform fixing a wrong URL or moving the
    GitOps branch (configure_repositories + refresh_exports) could then
    never reach a component that had already cloned once, and the
    self-heal in _ensure_target_clone would be structurally unreachable.
    A 403 and a 404 are different facts and are never conflated."""
    published = (exports_doc or {}).get("target_repo")
    if isinstance(published, dict) and published.get("url") \
            and published.get("branch"):
        return published["url"], published["branch"], "exports.target_repo"
    url = variables.get("target_repo_url")
    branch = variables.get("target_branch")
    if url and branch:
        return url, branch, "component variables (cached resolution)"
    try:
        root = json.loads(bucket.blob("platform/onboarding/state.json")
                          .download_as_text())
    except exceptions.Forbidden:
        return None, None, (
            "exports.target_repo is not published and this session cannot "
            "read platform/onboarding/state.json (a developer grant — by "
            "design)")
    except exceptions.NotFound:
        return None, None, (
            "exports.target_repo is not published and no "
            "platform/onboarding/state.json exists in this ledger")
    except Exception as e:
        return None, None, (
            "exports.target_repo is not published and "
            f"platform/onboarding/state.json is unreadable ({e})")
    onboarding = root.get("variables") or {}
    if onboarding.get("target_repo_url") and onboarding.get("target_branch"):
        return (onboarding["target_repo_url"], onboarding["target_branch"],
                "platform/onboarding/state.json (admin read)")
    return None, None, (
        "exports.target_repo is not published and "
        "platform/onboarding/state.json records no target repository — "
        "configure_repositories has not recorded one")


def _ensure_target_clone(bucket, variables: dict, clone_dir: str,
                         exports_doc) -> str:
    """Makes `clone_dir` a real clone of the target repo on the PR branch,
    or leaves a scratch directory and says so. Returns the history note.

    Self-healing, not merely idempotent: a work tree whose origin still
    matches the current coordinates is reused (re-validating after a
    finding must not throw away the branch), while a scratch directory left
    by a run before the coordinates were published, or a clone whose origin
    no longer matches them, is re-cloned HERE — the failing precondition is
    repaired by the retry instead of being reported for a human to fix. The
    scratch flag is recomputed every run: a stale True never outlives the
    successful clone that disproves it (and never lies to the ship gate)."""
    url, branch, source = _target_repo_coordinates(bucket, variables,
                                                   exports_doc)
    if not url or not branch:
        os.makedirs(clone_dir, exist_ok=True)
        variables["workload_clone_is_scratch"] = True
        return (f"No target repository is resolvable ({source}), so "
                f"{clone_dir} is a SCRATCH directory: the gates still run, "
                "but this is a BLOCKING finding — the platform side runs "
                "configure_repositories and then refresh_exports (exports."
                "target_repo is the developer-readable channel), after "
                "which re-running run_workload_validation clones and ships.")
    note_head = ""
    if git_client.is_git_work_tree(clone_dir):
        origin = git_client.remote_origin_url(clone_dir)
        if origin == url:
            variables["target_repo_url"] = url
            variables["target_branch"] = branch
            variables.pop("workload_clone_is_scratch", None)
            return f"Target clone reused at {clone_dir} (origin verified)"
        note_head = (f"Existing clone at {clone_dir} has origin "
                     f"{origin or '(none)'}, not the current {url} "
                     f"({source}); re-cloning. ")
    variables["target_repo_url"] = url
    variables["target_branch"] = branch
    try:
        git_client.clone_repository(url, branch, clone_dir)
        git_client.create_and_checkout_branch(
            clone_dir, variables["workload_branch_name"])
    except Exception as e:
        os.makedirs(clone_dir, exist_ok=True)
        variables["workload_clone_is_scratch"] = True
        return (note_head + f"Could not clone {url}@{branch} into "
                f"{clone_dir} ({e}); continuing over a SCRATCH directory — "
                "a BLOCKING finding; re-run run_workload_validation to "
                "retry (the coordinates are re-read every run).")
    variables.pop("workload_clone_is_scratch", None)
    return (note_head + f"Cloned {url}@{branch} into {clone_dir} on branch "
            f"{variables['workload_branch_name']}")


def _parent_refs_error(doc: dict, expected: dict):
    """One HTTPRoute's deviation from the verbatim parentRefs contract, or
    None. sectionName is refused outright: listener names are unpublished,
    so a route naming one is a guess by construction."""
    if not expected.get("name") or not expected.get("namespace"):
        return ("shipped while exports.gateway publishes no complete "
                "attach point — a live routing unit is only reachable "
                "through a published gateway, so this blob predates a "
                "gateway retraction; re-plan (or re-join) the component")
    refs = (doc.get("spec") or {}).get("parentRefs") \
        if isinstance(doc.get("spec"), dict) else None
    want = f"{expected['namespace']}/{expected['name']}"
    if not isinstance(refs, list) or len(refs) != 1 \
            or not isinstance(refs[0], dict):
        return (f"spec.parentRefs must be exactly one reference to "
                f"{want}, copied verbatim from exports.gateway")
    ref = refs[0]
    if ref.get("name") != expected["name"] \
            or ref.get("namespace") != expected["namespace"]:
        return (f"parentRefs names {ref.get('namespace')}/{ref.get('name')}"
                f", not exports.gateway's {want} — the verbatim-copy "
                "contract forbids inventing or 'correcting' the attach "
                "point")
    if "sectionName" in ref:
        return ("parentRefs carries a sectionName, but listener names are "
                "not published — attachment must stay listener-agnostic")
    return None


def _rendered_documents(rendered: list) -> list:
    """[(label, doc)] over the re-rendered chart/kustomize streams, for the
    contracts that read documents; a stream that does not parse is the
    re-render gate's finding, not theirs."""
    out = []
    for entry, text in rendered:
        try:
            docs = k8s_manifests.load_manifest_documents(text)
        except ValueError:
            continue
        out.extend((entry["dir"], d) for d in docs if isinstance(d, dict))
    return out


def _routing_findings(done_entries: list, exports_doc) -> list:
    """The consumer-side half of the exports.gateway marker contract (the
    producer side is machine-gated platform-side): every HTTPRoute a done
    wkld-routing unit ships must carry parentRefs == exports.gateway,
    exactly one entry, no sectionName. The brief mandates the verbatim
    copy; this gate catches the drift the brief cannot."""
    gateway = (exports_doc or {}).get("gateway")
    expected = gateway if isinstance(gateway, dict) else {}
    findings = []
    for entry in done_entries:
        unit = (entry or {}).get("unit") or {}
        if unit.get("family") != "wkld-routing":
            continue
        unit_id = str(unit.get("unit_id") or "wkld-routing")
        for f in ((entry.get("result") or {}).get("files")) or []:
            try:
                docs = k8s_manifests.load_manifest_documents(
                    str(f.get("content") or ""))
            except ValueError:
                continue  # the structural gate owns unparseable output
            for doc in docs:
                if not isinstance(doc, dict) or doc.get("kind") != "HTTPRoute":
                    continue
                error = _parent_refs_error(doc, expected)
                if error:
                    name = (doc.get("metadata") or {}).get("name") \
                        or f.get("path")
                    findings.append({"unit_id": unit_id,
                                     "route": str(name), "error": error})
    return findings


def _ensure_pr_workspace(component: str, variables: dict) -> list:
    """Allocate-or-heal the PR branch and clone-path coordinates. Returns
    history notes.

    Allocation belongs to validate entry — the developer graph has no
    design step to do it (§6.3). The healing half: a recorded clone path is
    trusted only under THIS machine's scratch root. A path recorded by
    another machine (a resumed workspace, a takeover) or hand-edited into
    the ledger is re-derived from the branch uuid instead of being used
    verbatim — clone_repository clears its target directory, so honoring an
    arbitrary recorded path would be an arbitrary recursive delete."""
    notes = []
    if not variables.get("workload_branch_uuid"):
        variables["workload_branch_uuid"] = str(uuid.uuid4())
    if not variables.get("workload_branch_name"):
        variables["workload_branch_name"] = (
            f"migration/workload-{component}-"
            f"{variables['workload_branch_uuid']}")
    allocated = target_clone_path(variables["workload_branch_uuid"])
    recorded = variables.get("workload_clone_path")
    if not recorded:
        variables["workload_clone_path"] = allocated
        return ["Workload PR workspace allocated at validate entry (no "
                f"clone path was recorded earlier): {allocated}"]
    scratch_root = os.path.realpath(workspace.SCRATCH_DIR)
    real = os.path.realpath(recorded)
    if not real.startswith(scratch_root + os.sep):
        variables["workload_clone_path"] = allocated
        notes.append(
            f"Recorded clone path {recorded} is outside this machine's "
            f"scratch root; re-derived to {allocated} (a workspace resumed "
            "from another machine, or a hand-edited ledger value — never "
            "cloned into or cleared verbatim).")
    return notes


def _parked_units(plan: dict) -> list:
    return [u for u in plan.get("units", []) if u["status"] == "parked"]


def build_pr_body(plan: dict, done_entries: list) -> str:
    """The PR description: tradeoffs, assumptions, open questions and the
    parked list, per unit — the reviewer-facing contract of decision 13."""
    lines = [f"Translated workload component `{plan.get('component')}` "
             "(GKE Agentic Migration, developer pipeline).", ""]
    for entry in done_entries:
        unit = entry["unit"]
        result = entry.get("result") or {}
        lines.append(f"## {unit['unit_id']} — {unit.get('title', '')}")
        if result.get("tradeoffs"):
            lines += ["### Tradeoffs", str(result["tradeoffs"])]
        if result.get("assumptions"):
            lines += ["### Assumptions to verify"] + [
                f"- {a}" for a in result["assumptions"]]
        if result.get("open_questions"):
            lines += ["### Open questions"] + [
                f"- {q}" for q in result["open_questions"]]
        lines.append("")
    parked = _parked_units(plan)
    if parked:
        lines.append("## Parked units (not in this PR)")
        for unit in parked:
            lines.append(
                f"- {unit['unit_id']}: parked — exports.gateway is not "
                "published, so the routing facts have no attach point yet; "
                "the unit unparks at translate entry (a re-join re-enters "
                "the component) once the platform Gateway ships.")
    return "\n".join(lines)


def build_comparisons(done_entries: list, placed: dict) -> list:
    """Before/after per unit: the plan's classified inputs on one side, the
    materialized files on the other (the platform comparisons shape)."""
    comparisons = []
    for entry in done_entries:
        unit = entry["unit"]
        result = entry.get("result") or {}
        comparisons.append({
            "unit_id": unit["unit_id"],
            "family": unit.get("family"),
            "title": unit.get("title"),
            "before": {"inputs": unit.get("inputs") or {},
                       "notes": unit.get("notes") or []},
            "after": {"paths": placed.get(unit["unit_id"], []),
                      "files": result.get("files", [])},
            "tradeoffs": result.get("tradeoffs", ""),
            "assumptions": result.get("assumptions", []),
            "open_questions": result.get("open_questions", []),
            "transform_findings": result.get("transform_findings", []),
        })
    return comparisons


async def run_workload_validation(ctx: Context = None) -> str:
    """Validates the component's translated units and, if clean, drives the
    ship approval and the per-component PR in the same call.

    Materializes done units into the target clone (carrier first, then
    owned-file edits), runs the manifest structural gate, the
    chart/kustomize re-render gate and the deterministic detect-only
    re-check over the materialized output — NOTHING else (no terraform, no
    fix loop) — cross-checks every blob's stamped exports generations
    against the current exports.json, checks ship-completeness (parked
    units listed, never blocking), resolves the target repository
    (exports.target_repo first — authoritative — then the cached component
    variables, then an admin read of the onboarding state) and self-heals
    the clone from those coordinates
    — a scratch clone is a BLOCKING finding, never a silent pass — and
    persists the validation report + before/after comparisons. Findings
    return the DAG to STATE_WKLD_REVIEW; clean walks the ship elicitation
    and, on approve, submits the PR branch
    migration/workload-<component>-<uuid>.
    """
    logger.info("run_workload_validation called.")
    try:
        state_dict, generation, config = authorize_and_rehydrate_workload(None)
    except Exception as e:
        return f"ERROR: {e}"
    if state_dict["current_state"] != VALIDATE_STATE:
        return (f"ERROR: Invalid state for run_workload_validation: "
                f"{state_dict['current_state']}")

    component = config["component"]
    variables = state_dict["variables"]
    plan = variables.get("workload_plan")
    if not plan or not plan.get("units"):
        return "ERROR: No workload plan found. Run plan_workload_translation first."
    done_ids = [u["unit_id"] for u in plan["units"] if u["status"] == "done"]
    if not done_ids:
        return "ERROR: No done units to validate. Run run_workload_translation first."

    bucket = state_mgr.gcs_client.bucket(get_bucket_name(config["ledger_uri"]))
    done_entries = []
    for unit_id in done_ids:
        try:
            done_entries.append(json.loads(bucket.blob(
                unit_blob_path(component, unit_id)).download_as_text()))
        except Exception as e:
            return f"ERROR: Could not read unit blob for '{unit_id}': {e}"

    # exports.json first: the clone coordinates (target_repo), the
    # staleness cross-check and the transforms detect gate all read this
    # ONE document — loading it before the clone is what lets a developer
    # session (403 on platform/) resolve the target repository at all.
    # The second element is the exports blob's GCS generation, NOT a note —
    # do not conflate it with the exports CONTENT generations map below.
    # A 403 on exports.json itself is an IAM defect (missing conditional
    # developer read grant), named as such rather than raised.
    try:
        exports_doc, _exports_blob_generation = exports_lib.load_exports(bucket)
    except exceptions.Forbidden:
        return ("ERROR: this session was denied reading exports.json (403). "
                "The ledger is missing the conditional exports read grant — "
                "ask an admin to re-run provision_ledger_iam (registering "
                "any member re-runs it). Nothing was validated or written.")

    # Target clone + branch coordinates. The developer pipeline has no
    # design step to allocate them (the platform's landingzone_design_2
    # does), so validate entry allocates — and then CLONES, which is the
    # part that used to be missing: a bare makedirs left a plain directory
    # that satisfied the ship action's isdir() guard and then failed inside
    # git.Repo() with an opaque "Git commit failed", making STATE_WKLD_DONE
    # unreachable. When no target repository is resolvable the clone
    # honestly cannot happen; the gates still run over a scratch directory,
    # and that is a BLOCKING finding below — the ship elicitation must not
    # rise (and burn its retry) over a PR that cannot be opened.
    # Where this run's history starts, so a data-gate park that repeats the
    # previous one verbatim can leave nothing behind. See the park branch.
    history_mark = len(state_dict["history"])
    for note in _ensure_pr_workspace(component, variables):
        state_dict["history"].append(note)
    clone_dir = variables["workload_clone_path"]
    clone_note = _ensure_target_clone(bucket, variables, clone_dir,
                                      exports_doc)
    state_dict["history"].append(clone_note)
    clone_findings = ([{"error": clone_note}]
                      if variables.get("workload_clone_is_scratch") else [])

    source_root = variables.get("workload_source_root")
    try:
        materialized = materialize.materialize_component(
            clone_dir, component, plan, done_entries, source_root or "")
    except (ValueError, OSError) as e:
        return f"ERROR: Failed to materialize units into the clone: {e}"

    render_dirs = materialized["render_dirs"]
    needed = {"helm" if e["kind"] == "helm" else "kubectl"
              for e in render_dirs}
    missing = sorted(b for b in needed if not shutil.which(b))
    if missing:
        return ("ERROR: the re-render gate needs "
                + " and ".join(f"'{b}'" for b in missing)
                + " on PATH — install and re-run run_workload_validation; "
                "the gate is never skipped.")

    manifests = validation.check_unit_manifests(
        clone_dir, materialized["unit_dirs"])
    try:
        renders, rendered_texts = _render_findings(clone_dir, render_dirs)
    except translator.RenderToolMissing as e:
        return f"ERROR: {e}"
    transform_gate = _transform_gate(clone_dir, rendered_texts,
                                     materialized["unit_dirs"], exports_doc)
    current_generations = (exports_doc or {}).get("generations")
    staleness = _staleness_findings(done_entries, current_generations,
                                    exports_present=exports_doc is not None)
    routing = _routing_findings(done_entries, exports_doc)
    pod_dns = poddns_contract.check_component(
        done_entries, _rendered_documents(rendered_texts),
        plan_units=plan.get("units") or [])
    ledger = _ledger_findings(done_entries)
    completeness = _completeness_findings(plan)
    conflicts = materialized["conflicts"]
    strategy = materialized["strategy"]

    # The data gate. Evaluated before the report is written so its verdict is
    # persisted with everything else, and acted on only after the report and
    # the comparisons are safely stored — a component held here has done all
    # of this work and must not have to redo it to find out it is still held.
    scope_paths, scope_unreadable, scope_reresolved = _confirmed_scope_paths(
        bucket, component, exports_doc)
    data_verdict = datagate.verdict(
        exports_doc, scope_paths,
        (exports_doc or {}).get("component_seed_index"),
        scope_unreadable=scope_unreadable,
        scope_reresolved=scope_reresolved)

    parked = [u["unit_id"] for u in _parked_units(plan)]
    report = {
        "component": component,
        "manifests": manifests,
        "renders": {"checked": len(render_dirs), "invalid": renders},
        "staleness": staleness,
        "ledger": ledger,
        "conflicts": conflicts,
        "strategy": strategy,
        "completeness": completeness,
        "parked": parked,
        "transforms": transform_gate,
        "routing": routing,
        "pod_dns": pod_dns,
        "clone": {"path": clone_dir,
                  "scratch": bool(variables.get("workload_clone_is_scratch")),
                  "note": clone_note, "findings": clone_findings},
        # Recorded in the report but NOT in all_valid: the data gate parks,
        # it does not fail the component back to review. See the module
        # docstring.
        "data": data_verdict,
        "all_valid": not (manifests["invalid"] or renders or staleness
                          or ledger or conflicts or strategy or completeness
                          or transform_gate["blocking"] or clone_findings
                          or routing or pod_dns["findings"]),
    }
    try:
        bucket.blob(report_blob_path(component)).upload_from_string(
            json.dumps(report, indent=2), content_type="application/json")
    except exceptions.Forbidden:
        return state_mgr.workload_write_denied(config, component)
    except Exception as e:
        return f"ERROR: Failed to persist the validation report: {e}"

    try:
        dag = load_dag(bucket, f"workloads/{component}/dag.json")
    except Exception as e:
        return f"ERROR: {e}"
    state_def = dag["states"][VALIDATE_STATE]
    counts = (
        f"{manifests['checked']} manifest file(s) checked, "
        f"{len(manifests['invalid'])} invalid; {len(render_dirs)} render "
        f"source(s), {len(renders)} failing; {len(staleness)} stale, "
        f"{len(ledger)} unreadable blob(s), {len(conflicts)} two-unit edits, "
        f"{len(strategy)} mixed carrier strateg"
        f"{'y' if len(strategy) == 1 else 'ies'}, "
        f"{len(completeness)} not done; deterministic re-check: "
        f"{len(transform_gate['blocking'])} blocking over "
        f"{transform_gate['checked']} shipped source(s); "
        f"{len(routing)} routing contract finding(s); "
        f"{len(pod_dns['findings'])} pod DNS contract finding(s) over "
        f"{pod_dns['checked']} pod spec(s)"
        + (f" ({len(pod_dns['skipped'])} unit(s) without pod_dns_facts, not "
           "conservation-checked)" if pod_dns["skipped"] else "")
        + (f" (facts incomplete for {len(pod_dns['incomplete'])} unit(s): a render "
           "source did not render at plan time)" if pod_dns["incomplete"] else "")
        + "; clone: "
        f"{'SCRATCH (blocking)' if clone_findings else 'ok'}; "
        f"parked: {', '.join(parked) or 'none'}; data gate: "
        f"{len(data_verdict['blocking'])} outstanding service(s) attributed "
        f"to this component")

    if not report["all_valid"]:
        dest = state_def["transitions"]["on_failure"]
        state_dict["history"].append(
            f"Workload validation failed ({counts}); returning to review")
        state_dict["history"].append(
            f"Transitioned {VALIDATE_STATE} -> {dest} via run_workload_validation")
        state_dict["current_state"] = dest
        try:
            bucket.blob(f"workloads/{component}/state.json").upload_from_string(
                json.dumps(state_dict, indent=2),
                content_type="application/json", if_generation_match=generation)
        except exceptions.PreconditionFailed:
            return "ERROR: Concurrent update conflict. Your changes were not saved."
        except exceptions.Forbidden:
            return state_mgr.workload_write_denied(config, component)
        failing = "; ".join(
            [f"{m['file']}: {m['error'][:200]}" for m in manifests["invalid"]]
            + [f"{r['dir']}: {r['error'][:200]}" for r in renders]
            + [f"{s['unit_id']}: {s['error'][:200]}" for s in staleness]
            + [f"{l['unit_id']}: {l['error'][:200]}" for l in ledger]
            + [f"{c['path']}: edited by {', '.join(c['units'])}" for c in conflicts]
            + [f"{s['chart_path']}: {s['error'][:240]}" for s in strategy]
            + [f"{f['unit_id']}: {f['error'][:160]}" for f in completeness]
            + [f"{t['source']}: {t['error'][:240]}"
               for t in transform_gate["blocking"]]
            + [f"{r['unit_id']}/{r['route']}: {r['error'][:240]}"
               for r in routing]
            + [f"{p['unit_id']}/{p['subject']}: {p['error'][:240]}"
               for p in pod_dns["findings"]]
            # Never truncated: this is our own one-liner and its tail IS
            # the remedy (the platform-side tool pair).
            + [c["error"] for c in clone_findings])
        return (f"Validation FAILED ({counts}).\nFindings: {failing}\n"
                f"Current State: {dest} — revise or skip the failing units, "
                "then re-approve.")

    # Everything the run produced is stored BEFORE the data gate is allowed
    # to hold the component: a park is a wait, not a failure, and the
    # comparisons a reviewer reads while waiting must exist.
    comparisons = build_comparisons(done_entries, materialized["placed"])
    try:
        bucket.blob(comparisons_blob_path(component)).upload_from_string(
            json.dumps(comparisons, indent=2), content_type="application/json")
    except Exception as e:
        logger.error(f"Failed to persist comparisons: {e}")
    variables["workload_pr_body"] = build_pr_body(plan, done_entries)

    data_refusal = datagate.refusal(data_verdict, component)
    data_residue = datagate.residue(data_verdict)
    if data_refusal:
        # The state does NOT move: STATE_WKLD_VALIDATE is where the component
        # waits, and its own expected_tool_call is the re-check. No transition
        # line goes into the history for the same reason — nothing
        # transitioned — but the hold is recorded, because six weeks of
        # holding with no trace is what made the platform side need a state
        # of its own in the first place.
        # A park is a POLL, and a poll that says exactly what the last one
        # said leaves nothing behind. The step's instructions tell the agent
        # to re-run this tool to re-check and a data migration takes weeks,
        # so appending unconditionally would grow a document every
        # authenticated call reads, without adding a fact. Compared over
        # everything this run appended — the clone note comes back each time
        # too, so keying on the held line alone deduplicated nothing.
        # Anything that CHANGED (a settled service, a re-clone) still lands.
        added = state_dict["history"][history_mark:] + [
            f"Workload output validated ({counts}); ship held by the data "
            f"gate ({len(data_verdict['blocking'])} outstanding data "
            "service(s))"]
        previous = state_dict["history"][history_mark - len(added):history_mark]
        if previous == added and history_mark >= len(added):
            del state_dict["history"][history_mark:]
        else:
            state_dict["history"].append(added[-1])
        try:
            bucket.blob(f"workloads/{component}/state.json").upload_from_string(
                json.dumps(state_dict, indent=2),
                content_type="application/json", if_generation_match=generation)
        except exceptions.PreconditionFailed:
            return "ERROR: Concurrent update conflict. Your changes were not saved."
        except exceptions.Forbidden:
            return state_mgr.workload_write_denied(config, component)
        return (f"Validation passed ({counts}), but the component is HELD.\n"
                f"{data_refusal}\n"
                + (f"{data_residue}\n" if data_residue else "")
                + f"Current State: {VALIDATE_STATE} — the report and "
                f"before/after comparisons are under workloads/{component}/ "
                "in the ledger. Re-run run_workload_validation to re-check.")

    dest = state_def["transitions"]["on_success"]
    state_dict["history"].append(f"Workload output validated ({counts})")
    state_dict["history"].append(
        f"Transitioned {VALIDATE_STATE} -> {dest} via run_workload_validation")
    state_dict["current_state"] = dest

    # The walk raises the ship elicitation (STATE_WKLD_APPROVED) and, on
    # approve, runs the submit_workload_pr action. A PR failure re-raises
    # the ship approval (the graph's deliberate divergence — §2.4); a
    # decline returns to the unit review.
    # The residue rides the ship elicitation itself, not the return value.
    # What the gate saw and could not hold on — a service with no attributed
    # consumer, a component whose files carry no indexed names — is exactly
    # the thing a developer should weigh BEFORE approving; returned with the
    # response it arrives after the pull request is already open.
    _, message, error = await run_dispatch_loop(
        ctx, state_dict, dag, config, f"Workload output validated ({counts}).",
        first_notice=(f"Before you approve — the data gate could not clear "
                      f"everything:\n{data_residue}" if data_residue else None))
    if error:
        return error

    end_state = state_dict["current_state"]
    try:
        bucket.blob(f"workloads/{component}/state.json").upload_from_string(
            json.dumps(state_dict, indent=2), content_type="application/json",
            if_generation_match=generation)
    except exceptions.PreconditionFailed:
        return "ERROR: Concurrent update conflict. Your changes were not saved."
    except exceptions.Forbidden:
        return state_mgr.workload_write_denied(config, component)

    # No residue here: this path always raised the ship elicitation, which
    # carried it as `first_notice` at the moment it could still change the
    # answer. Repeating it in the response would have the agent relay the
    # same paragraph twice, the second time about a decision already taken.
    # The durable copy is the report blob's `data` section.
    if end_state == "STATE_WKLD_REVIEW":
        return (f"Validation passed ({counts}).\nCurrent State: {end_state}\n"
                f"{message}\nThe report and before/after comparisons are "
                f"under workloads/{component}/ in the ledger.")
    if end_state == "STATE_WKLD_DONE":
        return (f"Validation passed ({counts}) and the pull request was "
                f"opened — component '{component}' is DONE (the developer "
                f"graph's first terminal state).\nCurrent State: {end_state}"
                f"\n{message}")
    return (f"Validation passed ({counts}).\nCurrent State: {end_state}\n"
            f"{message}\nCall get_next_stage to continue.")


def register(mcp):
    """Register this step's tools on the FastMCP server."""
    mcp.tool()(run_workload_validation)
