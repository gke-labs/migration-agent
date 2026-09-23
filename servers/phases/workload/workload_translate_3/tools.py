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

"""MCP tool for the workload translate step (STATE_WKLD_TRANSLATE).

run_workload_translation mirrors the platform's run_translation fan-out,
scoped to one component: the lease marker and the monotonic run counter live
in workloads/<component>/state.json variables, the CAS is the
generation-matched write of that state object, and unit blobs persist under
workloads/<component>/units/<unit_id>.json stamped with the run token AND
the plan's exports generations.

The four-conjunct blob-reuse predicate runs BEFORE the
lease+CAS claim, against the pre-claim persisted run counter — the settled
ordering: the counter is bumped at claim time before
any blob is written, so no persisted blob ever carries a token above the
persisted counter, and a blob passing conjunct (c) against the counter also
passes it against counter + 1. Do not reorder.

The deterministic transforms run server-side as a post-pass
over each unit's gate-passing PLAIN manifest files before persistence;
chart/kustomize-strategy files are chart sources, not manifest documents, so
the pass cannot rewrite them deterministically — that gap is recorded on the
unit as an open question instead of a silent skip.
"""

import asyncio
import json
import logging
import os
import shutil
import time

import yaml

from google.api_core import exceptions
from mcp.server.fastmcp import Context

from servers.dag.state_management import (
    get_bucket_name,
    load_dag,
    authorize_and_rehydrate_workload,
)
import servers.dag.state_management as state_mgr
from servers.dag.server import exports as exports_lib
from servers.phases import k8s_manifests

from servers.phases.workload.workload_plan_2 import planner

from . import translator, transforms

logger = logging.getLogger("migration-dag")

TRANSLATE_STATE = "STATE_WKLD_TRANSLATE"
LEASE_SECONDS = float(os.environ.get("GKE_AGENTIC_MIGRATION_TRANSLATE_LEASE", "3600"))
PENDING_STATUSES = ("planned", "revise", "error")


def unit_blob_path(component: str, unit_id: str) -> str:
    return f"workloads/{component}/units/{unit_id}.json"


def progress_blob_path(component: str) -> str:
    return f"workloads/{component}/translation-progress.json"


def _unit_blob_body(unit: dict, result, error, run: int, generations) -> str:
    """One unit's ledger artifact. Stamps the run token (progress and the
    superseded-run arithmetic) and the PLAN's exports generations — conjunct
    (d) of the reuse predicate and the validate step's staleness cross-check
    both read that stamp."""
    if error is None:
        payload = {"unit": {**unit, "status": "done", "error": None},
                   "result": result, "run": run,
                   "exports_generations": generations}
    else:
        payload = {"unit": {**unit, "status": "error", "error": error},
                   "result": None, "run": run,
                   "exports_generations": generations}
    return json.dumps(payload, indent=2)


def blob_reusable(payload, unit: dict, run_counter: int,
                  plan_generations) -> tuple:
    """The four-conjunct reuse predicate. Returns (reusable, reason).

    (a) the blob exists AND records a successful result (an error blob is a
        failure report, not reusable work);
    (b) the unit is not marked 'revise' (the reviewer invalidated the blob);
    (c) the blob's stamped run token <= the pre-claim persisted run counter
        (a superseded run's blob is reusable data; an unstamped blob is not);
    (d) the blob's stamped exports generations deep-equal the plan's stamp
        (a stale-exports blob NEVER is — the plan's briefs no longer match
        the facts the blob was produced against).
    """
    if not isinstance(payload, dict) or not isinstance(payload.get("result"), dict) \
            or (payload.get("unit") or {}).get("status") != "done":
        return False, "no persisted successful result"
    if unit.get("status") == "revise":
        return False, "the unit is marked revise"
    if "run" not in payload or not isinstance(payload["run"], int):
        return False, "the blob carries no run token stamp"
    if payload["run"] > run_counter:
        return False, (f"blob run token {payload['run']} exceeds the "
                       f"persisted run counter {run_counter}")
    if "exports_generations" not in payload:
        return False, "the blob carries no exports generations stamp"
    if payload["exports_generations"] != plan_generations:
        return False, (f"blob exports generations "
                       f"{payload['exports_generations']} != plan stamp "
                       f"{plan_generations}")
    return True, "reused"


def transforms_post_pass(unit: dict, result: dict, exports_doc) -> dict:
    """The deterministic pass, applied to gate-passing output.

    Plain manifest files are parsed (they just passed the hardened gate),
    rewritten through transforms.apply_transforms, and re-serialized; every
    flag routes into the envelope: change -> a tradeoffs addendum,
    open_question -> open_questions, warning -> assumptions; ok/warning
    verdicts additionally ride result["transform_findings"] for the
    validation report. Chart/kustomize files are SOURCES, not documents —
    the pass cannot rewrite them deterministically, so that gap is recorded
    as an open question, never silently skipped.
    """
    cites = translator.unit_render_cites(unit)
    new_files, findings, carrier_paths = [], [], []
    for entry in result.get("files") or []:
        path, content = str(entry["path"]), str(entry["content"])
        if translator._route_chart(path, cites["helm"]) is not None \
                or translator._route_kustomize(path, cites["kustomize"]) is not None:
            carrier_paths.append(path)
            new_files.append({"path": path, "content": content})
            continue
        docs = k8s_manifests.load_manifest_documents(content)
        new_docs, file_findings = transforms.apply_transforms(docs, exports_doc)
        if any(f["category"] == "change" for f in file_findings):
            content = yaml.safe_dump_all(new_docs, sort_keys=False)
        new_files.append({"path": path, "content": content})
        findings.extend(file_findings)
    out = {**result, "files": new_files, "transform_findings": findings}
    open_questions = list(out.get("open_questions") or [])
    assumptions = list(out.get("assumptions") or [])
    for finding in findings:
        line = f"[deterministic pass] {finding['detail']}"
        if finding["category"] == "open_question":
            open_questions.append(line)
        elif finding["category"] == "warning":
            assumptions.append(line)
    changes = [f["detail"] for f in findings if f["category"] == "change"]
    if changes:
        out["tradeoffs"] = (str(out.get("tradeoffs") or "")
                            + "\n\nDeterministic pass (server-side) changes:\n"
                            + "\n".join(f"- {c}" for c in changes))
    if carrier_paths:
        open_questions.append(
            "[deterministic pass] chart/kustomize source files are not "
            "rewritten by the deterministic pass ("
            + ", ".join(sorted(carrier_paths)) + "): the translation worker "
            "owns those rewrites (its brief lists the exports literals to "
            "transcribe), and the validate step re-renders the shipped "
            "sources and BLOCKS any left-over value this pass would have "
            "rewritten.")
    out["open_questions"] = open_questions
    out["assumptions"] = assumptions
    return out


def _preflight_binaries(plan: dict) -> str:
    """'' or an error naming the missing render binary. The gate must be
    able to run before any worker is paid for (a missing binary mid-fan-out
    would fail every chart unit after the LLM spend)."""
    sources = plan.get("sources") or {}
    needs = []
    if sources.get("charts") or plan.get("carriers"):
        needs.append("helm")
    if sources.get("kustomize"):
        needs.append("kubectl")
    missing = [b for b in needs if not shutil.which(b)]
    if missing:
        return ("ERROR: the re-render gate needs "
                + " and ".join(f"'{b}'" for b in missing)
                + " on PATH for this plan's chart/kustomize sources. Install "
                "the missing binar" + ("ies" if len(missing) > 1 else "y")
                + " and re-run run_workload_translation — the gate is never "
                "skipped.")
    return ""


def release_lease(bucket, component: str, run_token: int) -> None:
    """Best-effort clear of variables.workload_run_active after the fan-out.

    The lease is only ever cleared in memory and then written out with the
    final state, so EVERY post-claim early return used to leak it — and
    nothing can clear a leaked marker except waiting out LEASE_SECONDS
    (an hour by default) or hand-editing the ledger, which is precisely the
    crashed-run recovery the reuse predicate exists to make cheap. Clears
    only OUR token: a later run's claim must never be stolen.
    """
    try:
        blob = bucket.blob(f"workloads/{component}/state.json")
        blob.reload()
        doc = json.loads(blob.download_as_text())
        lease = (doc.get("variables") or {}).get("workload_run_active")
        if not isinstance(lease, dict) or lease.get("run") != run_token:
            return
        doc["variables"].pop("workload_run_active", None)
        blob.upload_from_string(json.dumps(doc, indent=2),
                                content_type="application/json",
                                if_generation_match=blob.generation)
    except Exception as e:
        logger.warning(f"Could not release the translation lease for "
                       f"{component} run {run_token}: {e}")


def _with_unpark(message: str, unpark_note: str, persisted: bool) -> str:
    """Every return path BELOW the unpark carries its outcome.

    A successful unpark persists state.json and plan.json before the run is
    claimed, so a response that omits it tells the developer nothing happened
    when the plan was in fact re-stamped and units re-queued — the opposite of
    DESIGN §6.3's "reported in the tool response: visible, never silent".
    A DECLINED unpark wrote nothing, and says so instead: the same note with
    the persistence sentence would be a lie about the ledger.
    """
    if not unpark_note:
        return message
    if not persisted:
        return f"{message}\n{unpark_note}(Nothing was unparked or written.)"
    return (f"{message}\n{unpark_note}"
            "(The unpark above persisted; it is not rolled back by this "
            "outcome.)")


def _lease_conflict(variables: dict) -> str:
    """The in-flight-run refusal, or "" when no live lease is held.

    The lease stops a second fan-out while one is in flight (and expires so a
    crashed run never wedges the step — DESIGN §14 issue 2's caveat is
    inherited: a run outliving its lease can have overwritten blobs before
    its final check). Checked before ANY ledger mutation, so a refused call
    leaves the component exactly as it found it.
    """
    lease = variables.get("workload_run_active")
    if not isinstance(lease, dict):
        return ""
    # A claimed_at in the FUTURE (clock skew between the machine that claimed
    # and this one, or an NTP step) must count as held, not as expired:
    # `0 <= age` failed open and let a second fan-out start while the first
    # was still writing unit blobs.
    age = time.time() - float(lease.get("claimed_at") or 0)
    if age >= LEASE_SECONDS:
        return ""
    skew = (f" — its claimed_at is {int(-age)}s in the FUTURE, so this "
            "machine's clock disagrees with the claimant's" if age < 0 else "")
    return (f"ERROR: Workload translation run {lease.get('run')} appears to "
            f"be in flight (claimed {int(abs(age))}s ago){skew}. Not starting "
            "a second fan-out — nothing was unparked, claimed or written; "
            "wait for it, or retry after the lease expires "
            f"({int(LEASE_SECONDS)}s) if it crashed.")


def _reuse_pass(bucket, component, pending: list, run_counter: int,
                plan_generations) -> tuple:
    """Applies the four-conjunct predicate per pending unit BEFORE the claim.
    Returns (still_pending, reused_ids, reasons) — reasons name every refusal
    so the staleness arm can assert conjunct (d) fired."""
    still_pending, reused, reasons = [], [], {}
    for unit in pending:
        blob = bucket.blob(unit_blob_path(component, unit["unit_id"]))
        payload = None
        try:
            payload = json.loads(blob.download_as_text())
        except exceptions.NotFound:
            reasons[unit["unit_id"]] = "no persisted blob"
        except Exception as e:
            reasons[unit["unit_id"]] = f"blob unreadable: {e}"
        if payload is not None:
            ok, reason = blob_reusable(payload, unit, run_counter,
                                       plan_generations)
            reasons[unit["unit_id"]] = reason
            if ok:
                reused.append(unit["unit_id"])
                continue
        still_pending.append(unit)
    return still_pending, reused, reasons


async def run_workload_translation(ctx: Context = None) -> str:
    """Translates every pending unit of the component's approved plan.

    One worker per unit produces Kubernetes YAML plus the tradeoffs
    envelope; the deterministic transforms run as a server-side post-pass;
    results persist incrementally to workloads/<component>/units/. Persisted
    blobs from a previous run are REUSED (never re-translated) when the
    four-conjunct predicate holds. Parked units (wkld-routing while
    exports.gateway is null) never run a worker — but when the current
    exports publishes a gateway, parked routing units UNPARK here at entry
    (plan refreshed and re-stamped, visibly) before anything is claimed.
    Advances to STATE_WKLD_REVIEW once at least one active unit is done.
    """
    logger.info("run_workload_translation called.")
    auth_error = translator.check_llm_auth()
    if auth_error:
        return f"ERROR: {auth_error}"
    try:
        state_dict, generation, config = authorize_and_rehydrate_workload(None)
    except Exception as e:
        return f"ERROR: {e}"
    if state_dict["current_state"] != TRANSLATE_STATE:
        return (f"ERROR: Invalid state for run_workload_translation: "
                f"{state_dict['current_state']}")

    component = config["component"]
    variables = state_dict["variables"]
    plan = variables.get("workload_plan")
    if not plan or not plan.get("units"):
        return ("ERROR: No workload plan found. Run plan_workload_translation "
                "first (the plan review approves what this step translates).")
    source_root = variables.get("workload_source_root")
    if not source_root or not os.path.isdir(source_root):
        return (f"ERROR: the recorded source root {source_root!r} is not a "
                "directory on this workstation. The workers read the scoped "
                "files from YOUR clone; re-run plan_workload_translation "
                "with source_root=<path> from the machine that has it.")

    binary_error = _preflight_binaries(plan)
    if binary_error:
        return binary_error

    bucket = state_mgr.gcs_client.bucket(get_bucket_name(config["ledger_uri"]))
    state_path = f"workloads/{component}/state.json"

    # The in-flight lease is checked BEFORE the unpark, not after it. The
    # unpark PERSISTS state.json and plan.json; running it first meant a call
    # that was about to be refused had already re-stamped the plan under a
    # running fan-out (whose blobs carry the pre-unpark generations) and
    # returned an error that said nothing about the mutation.
    lease_error = _lease_conflict(variables)
    if lease_error:
        return lease_error

    # Unpark at translate entry: parked routing units whose
    # attach point has since published are refreshed to planned through the
    # planner's pure derivation, the units already done are demoted so they
    # genuinely re-run against the refreshed stamp, and the plan is re-stamped
    # to the CURRENT exports. Gateway absent/partial -> nothing changes and
    # parked stays excluded; exports moved beyond `gateway` -> refused, with
    # the re-plan path named (the other briefs cannot be rebuilt here).
    # A 403 is an IAM defect (missing conditional developer read grant on
    # exports.json), returned in-band before anything is unparked.
    try:
        exports_doc, _ = exports_lib.load_exports(bucket)
    except exceptions.Forbidden:
        return ("ERROR: this session was denied reading exports.json (403). "
                "The ledger is missing the conditional exports read grant — "
                "ask an admin to re-run provision_ledger_iam (registering "
                "any member re-runs it). Nothing was unparked or written.")
    unpark_note = ""
    unpark_persisted = False
    refreshed_plan, unparked, refusal = planner.refresh_parked_routing(
        plan, exports_doc)
    if refusal:
        unpark_note = f"- Unpark DECLINED: {refusal}\n"
    if unparked:
        old_stamp = (plan.get("exports_stamp") or {}).get("generations")
        unparked_ids = sorted(
            new["unit_id"] for new, old in
            zip(refreshed_plan["units"], plan["units"])
            if new["status"] == "planned" and old["status"] == "parked")
        redone_ids = planner.redone_at_unpark(plan, refreshed_plan)
        plan = refreshed_plan
        new_stamp = (plan.get("exports_stamp") or {}).get("generations")
        variables["workload_plan"] = plan
        redone_phrase = ("; re-queued " + ", ".join(redone_ids)
                         + " (their blobs carry the old generations)"
                         if redone_ids else "")
        state_dict["history"].append(
            f"Unparked {', '.join(unparked_ids)} at translate entry: "
            f"exports.gateway is published; plan re-stamped "
            f"{json.dumps(old_stamp)} -> {json.dumps(new_stamp)}"
            + redone_phrase)
        state_blob = bucket.blob(state_path)
        try:
            state_blob.upload_from_string(
                json.dumps(state_dict, indent=2),
                content_type="application/json",
                if_generation_match=generation)
        except exceptions.PreconditionFailed:
            return ("ERROR: the component state changed while unparking "
                    f"{', '.join(unparked_ids)}. Nothing was claimed and no "
                    "worker ran — re-run run_workload_translation.")
        except exceptions.Forbidden:
            return state_mgr.workload_write_denied(config, component)
        generation = state_blob.generation
        unpark_persisted = True  # state.json is written: the note is true now
        # state.json is the authority for the plan; this blob is the copy the
        # v0.4 join guard reads. A failure here is therefore not silent: it
        # leaves the guard seeing a parked unit that state.json says is
        # planned, so the caller is told (the next unpark is a no-op, so the
        # divergence is self-healing rather than damaging).
        plan_write_warning = ""
        try:
            bucket.blob(f"workloads/{component}/plan.json").upload_from_string(
                json.dumps(plan, indent=2), content_type="application/json")
        except Exception as e:
            logger.error(f"Failed to persist unparked plan blob: {e}")
            plan_write_warning = (
                f"  WARNING: the plan.json copy could not be written ({e}). "
                "state.json holds the unparked plan and is authoritative; the "
                "v0.4 join guard will keep reading the stale copy until a "
                "later write succeeds.\n")
        unpark_note = (
            f"- Unparked at translate entry: {', '.join(unparked_ids)} — "
            f"exports.gateway is now published; plan exports stamp "
            f"{json.dumps(old_stamp)} -> {json.dumps(new_stamp)}.\n"
            + (f"  Re-queued (blobs stamped with the old generations, so they "
               f"fail reuse conjunct (d) and re-run): {', '.join(redone_ids)}\n"
               if redone_ids else "")
            + plan_write_warning)

    units = plan["units"]
    parked = [u["unit_id"] for u in units if u["status"] == "parked"]
    pending = [u for u in units if u["status"] in PENDING_STATUSES]
    already_done = [u for u in units if u["status"] == "done"]
    if not pending and not already_done:
        return _with_unpark(
            "ERROR: No active units to translate (everything is skipped "
            + (f"or parked: {', '.join(parked)}" if parked else "")
            + "). Adjust the plan first.", unpark_note, unpark_persisted)

    plan_generations = (plan.get("exports_stamp") or {}).get("generations")

    # Four-conjunct reuse BEFORE the claim (see the module docstring for why
    # the pre-claim counter is the right operand for conjunct c).
    run_counter = int(variables.get("workload_run", 0))
    pending, reused_ids, reuse_reasons = _reuse_pass(
        bucket, component, pending, run_counter, plan_generations)

    # CAS claim, mirrored from run_translation but scoped to the component
    # state object. Two calls can both pass the state and lease checks above;
    # the generation-matched write makes two racing claims produce exactly
    # one winner.
    run_token = run_counter + 1
    variables["workload_run"] = run_token
    variables["workload_run_active"] = {"run": run_token,
                                        "claimed_at": time.time()}
    try:
        bucket.blob(state_path).upload_from_string(
            json.dumps(state_dict, indent=2),
            content_type="application/json", if_generation_match=generation)
    except exceptions.PreconditionFailed:
        return _with_unpark(
            "ERROR: Another workload translation run claimed this component "
            "first (or the state changed). Not starting a second fan-out — "
            "re-run once the other run finishes.", unpark_note, unpark_persisted)
    except exceptions.Forbidden:
        return _with_unpark(
            state_mgr.workload_write_denied(config, component), unpark_note, unpark_persisted)
    except Exception as e:
        return _with_unpark(
            f"ERROR: Failed to claim the translation run: {e}", unpark_note, unpark_persisted)

    # Progress blob: reused units count as done-before-start; blobs land one
    # by one under the run token so a progress reader tells this run's blobs
    # from a superseded run's.
    try:
        bucket.blob(progress_blob_path(component)).upload_from_string(
            json.dumps({
                "run": run_token,
                "total_units": len(pending) + len(already_done) + len(reused_ids),
                "reused": sorted(reused_ids),
                "parked": sorted(parked),
                "pending_ids": sorted(u["unit_id"] for u in pending),
            }, indent=2), content_type="application/json")
    except Exception as e:
        logger.warning(f"Failed to persist workload progress: {e}")

    # Worker inputs load the plan's locators into content; a unit whose
    # source no longer loads fails HERE, honestly, before any LLM spend.
    # (exports_doc was read once at translate entry, before the claim.)
    worker_inputs, input_errors, cache = {}, {}, {}
    for unit in pending:
        try:
            worker_inputs[unit["unit_id"]] = translator.build_worker_input(
                unit, plan, source_root, cache)
        except (ValueError, OSError) as e:
            input_errors[unit["unit_id"]] = str(e)
    pending_runnable = [u for u in pending
                        if u["unit_id"] not in input_errors]

    persisted = set()

    async def persist_unit(unit, result, error):
        unit_id = unit["unit_id"]
        blob = bucket.blob(unit_blob_path(component, unit_id))
        await asyncio.to_thread(
            blob.upload_from_string,
            _unit_blob_body(unit, result, error, run_token, plan_generations),
            content_type="application/json")
        persisted.add(unit_id)

    def post_pass(unit, result):
        return transforms_post_pass(unit, result, exports_doc)

    outcome = {"results": {}, "errors": {}}
    if pending_runnable:
        outcome = await translator.translate_all(
            pending_runnable, worker_inputs,
            render_ctx={"source_root": source_root},
            on_result=persist_unit, post_pass=post_pass)
    outcome["errors"].update(input_errors)

    # Re-read before applying: a run that lost the race (another session
    # advanced the state, or a later run superseded this token after a lease
    # expiry) discards its results instead of applying them over artifacts a
    # reviewer may already be reading. The per-unit blobs already landed
    # incrementally above — that is exactly what the reuse predicate makes
    # safe to leave behind (a superseded run's blob is reusable data).
    try:
        state_dict, generation, config = authorize_and_rehydrate_workload(None)
    except Exception as e:
        release_lease(bucket, component, run_token)
        return _with_unpark(
            "ERROR: Translation finished but the component state could not "
            f"be re-read: {e}", unpark_note, unpark_persisted)
    variables = state_dict["variables"]
    if state_dict["current_state"] != TRANSLATE_STATE:
        release_lease(bucket, component, run_token)
        return _with_unpark(
            f"ERROR: Another session advanced '{component}' to "
            f"{state_dict['current_state']} while translation ran. This "
            "run's results were not applied to the plan or state.",
            unpark_note, unpark_persisted)
    if int(variables.get("workload_run", 0)) != run_token:
        # Safe even when the superseding run holds the lease: release_lease
        # only clears a lease whose run token is this run's.
        release_lease(bucket, component, run_token)
        return _with_unpark(
            f"ERROR: Workload run {variables.get('workload_run')} superseded "
            f"run {run_token} while it ran (e.g. after a lease expiry). This "
            "run's results were not applied to the plan or state.",
            unpark_note, unpark_persisted)

    # The RE-READ plan is the authority for anything a concurrent review
    # action changed while the workers ran: skip_workload_units is
    # deliberately callable from STATE_WKLD_TRANSLATE, and overwriting a
    # 'skipped' (or 'parked') status here silently resurrected the unit as
    # done — it then materialized into the clone and shipped in the PR while
    # the summary reported it as skipped.
    plan = variables.get("workload_plan") or plan
    errors = dict(outcome["errors"])
    discarded = []
    for unit in plan["units"]:
        unit_id = unit["unit_id"]
        if unit["status"] in ("skipped", "parked") \
                and (unit_id in reused_ids or unit_id in outcome["results"]
                     or unit_id in errors):
            discarded.append(f"{unit_id} ({unit['status']})")
            continue
        if unit_id in reused_ids:
            unit.update({"status": "done", "error": None, "feedback": None})
        elif unit_id in outcome["results"]:
            if unit_id not in persisted:
                # Belt-and-suspenders: re-persist only if the incremental
                # write failed, so a persist error still marks the unit and
                # never silently loses a successful translation.
                try:
                    bucket.blob(unit_blob_path(component, unit_id)) \
                        .upload_from_string(
                            _unit_blob_body(unit, outcome["results"][unit_id],
                                            None, run_token, plan_generations),
                            content_type="application/json")
                except Exception as e:
                    errors[unit_id] = f"translated but failed to persist: {e}"
                    unit.update({"status": "error", "error": errors[unit_id]})
                    continue
            unit.update({"status": "done", "error": None, "feedback": None})
        elif unit_id in errors:
            unit.update({"status": "error", "error": errors[unit_id]})
            if unit_id not in persisted:
                try:
                    bucket.blob(unit_blob_path(component, unit_id)) \
                        .upload_from_string(
                            _unit_blob_body(unit, None, errors[unit_id],
                                            run_token, plan_generations),
                            content_type="application/json")
                except Exception as e:
                    logger.error(f"Failed to persist error blob for "
                                 f"{unit_id}: {e}")

    variables["workload_plan"] = plan
    done = [u for u in plan["units"] if u["status"] == "done"]
    variables.pop("workload_run_active", None)

    if not done:
        state_dict["history"].append(
            "run_workload_translation: no units completed; staying in "
            + TRANSLATE_STATE)
        try:
            bucket.blob(state_path).upload_from_string(
                json.dumps(state_dict, indent=2),
                content_type="application/json",
                if_generation_match=generation)
        except (exceptions.PreconditionFailed, exceptions.Forbidden):
            release_lease(bucket, component, run_token)
        # A discarded unit is not a failure: it translated fine and was
        # dropped because a reviewer changed its status mid-run. Saying
        # "no unit completed ({})" alone sent the developer looking for a
        # worker error that never happened.
        detail = f"errors: {json.dumps(errors)[:500]}" if errors \
            else "no unit produced output"
        if discarded:
            detail += ("; discarded because their status changed while the "
                       "workers ran: " + ", ".join(sorted(discarded)))
        return _with_unpark(
            f"ERROR: No unit completed ({detail}). State unchanged — fix the "
            "cause and re-run run_workload_translation.", unpark_note, unpark_persisted)

    try:
        dag = load_dag(bucket, f"workloads/{component}/dag.json")
    except Exception as e:
        release_lease(bucket, component, run_token)
        return _with_unpark(f"ERROR: {e}", unpark_note, unpark_persisted)
    dest = dag["states"][TRANSLATE_STATE]["transitions"]["on_tool_call_received"]
    state_dict["history"].append(
        f"Workload translation run {run_token}: {len(done)} done "
        f"({len(reused_ids)} reused), {len(errors)} failed")
    state_dict["history"].append(
        f"Transitioned {TRANSLATE_STATE} -> {dest} via run_workload_translation")
    state_dict["current_state"] = dest

    try:
        bucket.blob(f"workloads/{component}/plan.json").upload_from_string(
            json.dumps(plan, indent=2), content_type="application/json")
    except Exception as e:
        logger.error(f"Failed to persist plan blob: {e}")
    try:
        bucket.blob(state_path).upload_from_string(
            json.dumps(state_dict, indent=2),
            content_type="application/json", if_generation_match=generation)
    except exceptions.PreconditionFailed:
        release_lease(bucket, component, run_token)
        return _with_unpark("ERROR: Concurrent update conflict. Your changes "
                            "were not saved.", unpark_note, unpark_persisted)
    except exceptions.Forbidden:
        release_lease(bucket, component, run_token)
        return _with_unpark(
            state_mgr.workload_write_denied(config, component), unpark_note, unpark_persisted)

    failed = [u["unit_id"] for u in plan["units"] if u["status"] == "error"]
    skipped = [u["unit_id"] for u in plan["units"] if u["status"] == "skipped"]
    summary = (
        f"SUCCESS: Workload translation run complete for '{component}'. "
        f"Current State: {dest}.\n"
        f"- Units: {len(done)} done ({len(reused_ids)} reused, not re-run), "
        f"{len(failed)} failed, {len(skipped)} skipped, {len(parked)} parked\n"
        f"- Generated code is in the ledger under workloads/{component}/"
        "units/, not in this summary.\n")
    summary += unpark_note
    if reused_ids:
        summary += ("- Reused (four-conjunct predicate): "
                    + ", ".join(sorted(reused_ids)) + "\n")
    if parked:
        summary += ("- Parked (no worker ran; excluded from ship-"
                    "completeness): " + ", ".join(sorted(parked)) + "\n")
    if failed:
        summary += (
            f"- Failed: {', '.join(failed)} — send back with "
            "request_workload_unit_revision, exclude with "
            "skip_workload_units, or re-run after fixing the cause.\n")
    if discarded:
        summary += (
            "- Discarded (status changed while the workers ran; this run's "
            "output was NOT applied and the unit blob left behind is not "
            "materialized): " + ", ".join(sorted(discarded)) + "\n")
    summary += (
        "Next: get_workload_results() for per-unit status, assumptions and "
        "open questions — the human reads code and tradeoffs from the unit "
        "blobs.")
    return summary


def register(mcp):
    """Register this step's tools on the FastMCP server."""
    mcp.tool()(run_workload_translation)
