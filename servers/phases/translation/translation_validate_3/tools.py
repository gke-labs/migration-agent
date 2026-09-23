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

"""MCP tools for translation step 3 (validate): prove the generated code compiles.

Owns the STATE_TRANSLATION_VALIDATE agent task. run_generated_validation
materializes the done units into the target clone beside the landing-zone
draft, generates the root module that references every Terraform-bearing
unit directory (root_wiring — without it the root pass compiles nothing it
does not reference), runs `terraform validate` over the root and every unit
directory with a bounded LLM auto-fix loop, then prints the recorded values
behind the root's variables into migration.auto.tfvars (tfvars_printer,
after the fix loop so a declaration the root's fix worker added is seen —
a no-default variable with no recorded source is a finding, never a
plan-time prompt or an invented default), structurally checks the units'
Kubernetes manifests (pure Python, offline — terraform never reads them),
enforces the coverage-omission gate (coverage_gate — a facts-present
coverage-map row with no artifact and no explicit skip fails the run naming
the row; the GKE-cluster row is proven by scanning the clone), writes the
validation report and the before/after comparison the reviewer reads in the
Review UI, and — when everything passes — walks straight on through the ship
approval elicitation and the PR submission in the same call. Failures that
survive the fix loop return the DAG to the unit review with the report.
"""

import asyncio
import json
import logging
import os
import re
import shutil

from google.api_core import exceptions
from mcp.server.fastmcp import Context

from servers.dag.server import exports as exports_lib
from servers.dag.state_management import (
    get_bucket_name,
    load_dag,
    authorize_and_rehydrate,
)
import servers.dag.state_management as state_mgr
from servers.dag.dispatch import run_dispatch_loop

from ..translation_translate_1.tools import UNIT_BLOB_PREFIX
from . import (clusterdns_contract, computeclass_contract, coverage_gate, gateway_contract, ksa_contract,
               root_wiring, tfvars_printer, validation)

logger = logging.getLogger("migration-dag")

VALIDATION_REPORT_BLOB = "platform/translation/validation-report.json"
COMPARISONS_BLOB = "platform/translation/comparisons.json"
# Where the generated units land inside the target clone, beside the
# landing-zone draft at the root.
UNITS_SUBDIR = "translation-units"

# `terraform init` (run by the validator) drops a .terraform/ provider cache
# and local state into every directory it touches. Those are machine-local
# artifacts — hundreds of MB of provider binaries — and must never be staged
# into the customer's pull request. A .gitignore at the clone root keeps
# `git add -A` (in the PR submission) from sweeping them in, while still
# committing the .terraform.lock.hcl dependency locks.
GITIGNORE_CONTENT = "\n".join([
    "# Added by GKE Agentic Migration's Terraform validation step.",
    ".terraform/",
    "*.tfstate",
    "*.tfstate.*",
    ".terraform.tfstate.lock.info",
    "crash.log",
    "crash.*.log",
    "",
])


# The clone is the customer's repository, and the canonical Terraform
# .gitignore excludes `*.tfvars` — which would silently drop the printed
# migration.auto.tfvars from the PR while the report said the values
# shipped. Ensured separately from the block above (a clone that already
# ignores .terraform/ still needs it) and appended LAST, because git's
# last matching rule is the one that decides.
TFVARS_NEGATION = "\n".join([
    "# The migration agent's printed values must ship in the PR even where",
    "# an existing rule excludes *.tfvars (the canonical Terraform ignore does).",
    f"!{tfvars_printer.TFVARS_FILENAME}",
    "",
])


def _write_gitignore(clone_dir: str) -> None:
    """Excludes Terraform local state; force-includes the printed tfvars."""
    path = os.path.join(clone_dir, ".gitignore")
    existing = ""
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            existing = f.read()
    content = existing
    if ".terraform/" not in content:
        content = (content + "\n" if content.strip() else "") + GITIGNORE_CONTENT
    if f"!{tfvars_printer.TFVARS_FILENAME}" not in content:
        if content and not content.endswith("\n"):
            content += "\n"
        content += TFVARS_NEGATION
    if content != existing:
        with open(path, "w", encoding="utf-8") as f:
            f.write(content)


def _unit_dir(unit_id: str) -> str:
    slug = re.sub(r"[^a-zA-Z0-9._-]+", "-", unit_id).strip("-") or "unit"
    return f"{UNITS_SUBDIR}/{slug}"


def materialize_units(clone_dir: str, done_units: list) -> dict:
    """Writes each done unit's files into its own directory in the clone.

    Returns {unit_id: unit_dir_rel}. Paths are unit-relative and guarded, so a
    worker-authored path can never escape its unit directory.

    Directories under translation-units/ that this run does NOT place are
    pruned: the clone persists across runs, so a unit that was done last run
    and skipped this run would otherwise sit on disk referenced by nothing,
    validated by nothing, and swept into the customer's PR by the submit
    action's `git add -A`. The shipped tree must be exactly the units the
    root wiring and the validation passes covered.
    """
    placed = {}
    for entry in done_units:
        unit_id = entry["unit"]["unit_id"]
        rel_dir = _unit_dir(unit_id)
        target = os.path.realpath(os.path.join(clone_dir, rel_dir))
        clone_real = os.path.realpath(clone_dir)
        if not target.startswith(clone_real + os.sep):
            raise ValueError(f"unit directory escapes the clone: {rel_dir!r}")
        # Clear the unit's directory first so a file a previous run emitted but
        # this run no longer produces (e.g. after a revision) does not linger
        # and get validated and shipped as stale code.
        if os.path.isdir(target):
            shutil.rmtree(target)
        os.makedirs(target, exist_ok=True)
        for f in (entry.get("result") or {}).get("files", []):
            rel = str(f.get("path", ""))
            full = os.path.realpath(os.path.join(target, rel))
            if not full.startswith(target + os.sep) and full != target:
                raise ValueError(f"unit file escapes its directory: {rel!r}")
            os.makedirs(os.path.dirname(full), exist_ok=True)
            with open(full, "w", encoding="utf-8") as fh:
                fh.write(str(f.get("content", "")))
        placed[unit_id] = rel_dir
    units_root = os.path.join(clone_dir, UNITS_SUBDIR)
    keep = {os.path.basename(rel) for rel in placed.values()}
    if os.path.isdir(units_root):
        for name in sorted(os.listdir(units_root)):
            stale = os.path.join(units_root, name)
            if name not in keep and os.path.isdir(stale):
                shutil.rmtree(stale)
    return placed


def build_comparisons(done_units: list, report: dict, placed: dict) -> list:
    """The before/after the reviewer signs off: discovered AWS inputs on one
    side, the generated (and possibly auto-fixed) GCP code on the other."""
    fixed_dirs = {f["dir"]: f["attempts"] for f in report.get("fixed", [])}
    comparisons = []
    for entry in done_units:
        unit = entry["unit"]
        result = entry.get("result") or {}
        rel_dir = placed.get(unit["unit_id"], "")
        comparisons.append({
            "unit_id": unit["unit_id"],
            "kind": unit.get("kind"),
            "title": unit.get("title"),
            "before": {
                "inputs": unit.get("inputs") or {},
                "notes": unit.get("notes") or [],
            },
            "after": {
                "dir": rel_dir,
                "files": result.get("files", []),
            },
            "tradeoffs": result.get("tradeoffs", ""),
            "assumptions": result.get("assumptions", []),
            "open_questions": result.get("open_questions", []),
            "autofix_attempts": fixed_dirs.get(rel_dir, 0),
        })
    return comparisons


async def run_generated_validation(ctx: Context = None) -> str:
    """Validates the generated code and, if clean, drives ship approval + PR.

    Materializes every done unit into the target clone beside the landing-zone
    draft, wires the Terraform-bearing units into a generated root module,
    runs terraform validate per directory with a bounded LLM auto-fix loop,
    prints the recorded values behind the root's variables into
    migration.auto.tfvars (after the fix loop, so the settled variable set is
    what is printed; a no-default root variable with no recorded source fails
    the run naming the declaration — never a plan-time value prompt),
    structurally checks the units' Kubernetes manifests (offline, no fix
    loop), enforces the coverage-omission gate (a facts-present coverage-map
    row owned by landing-zone/platform-translation with no artifact and no
    explicit skip is a failure naming the row — the GKE-cluster row is
    proven by scanning the materialized clone for google_container_cluster),
    persists the validation report and the before/after comparison for
    the Review UI, then: all valid → the ship-approval elicitation is raised
    (the user's answer decides) and, on approve, the PR is opened in the
    target repository. Any directory or manifest still failing returns the DAG
    to STATE_TRANSLATION_REVIEW with the report.
    """
    logger.info("run_generated_validation called.")
    try:
        state_dict, generation, config = authorize_and_rehydrate(None)
    except Exception as e:
        return f"ERROR: {e}"

    current_state = state_dict["current_state"]
    if current_state != "STATE_TRANSLATION_VALIDATE":
        return f"ERROR: Invalid state for run_generated_validation: {current_state}"

    terraform_bin = shutil.which("terraform")
    if not terraform_bin:
        return "ERROR: terraform is not on PATH — install it, then re-run run_generated_validation."

    clone_dir = state_dict["variables"].get("target_clone_path")
    if not clone_dir or not os.path.isdir(clone_dir):
        return (
            "ERROR: Target clone directory does not exist. Re-clone the target "
            f"repository at {clone_dir!r} (branch "
            f"{state_dict['variables'].get('lz_branch_name')!r}) and re-run."
        )

    plan = state_dict["variables"].get("translation_plan") or {}
    done_ids = [u["unit_id"] for u in plan.get("units", []) if u["status"] == "done"]
    if not done_ids:
        return "ERROR: No done units to validate. Run run_translation first."

    bucket_name = get_bucket_name(config["ledger_uri"])
    bucket = state_mgr.gcs_client.bucket(bucket_name)

    done_units = []
    for unit_id in done_ids:
        try:
            done_units.append(json.loads(
                bucket.blob(f"{UNIT_BLOB_PREFIX}/{unit_id}.json").download_as_text()))
        except Exception as e:
            return f"ERROR: Could not read unit blob for '{unit_id}': {e}"

    try:
        placed = materialize_units(clone_dir, done_units)
    except Exception as e:
        return f"ERROR: Failed to materialize units into the clone: {e}"

    # Wire the materialized units into a generated root module BEFORE
    # validating: terraform compiles a module nothing references, so until
    # this file exists the root pass below reports success over code it never
    # read (DESIGN §14 issue 14). A failure here is fatal rather than logged —
    # validating without the wiring is the vacuous pass, not a degraded one.
    # The recorded facts (workspace settings, the design's cluster literals)
    # are collected first: the wiring wires a DEFAULTED unit variable too when
    # its name carries a recorded value, so the printed value — not the unit's
    # baked-in default — is what plan/apply reads.
    facts = tfvars_printer.collect_values(config, clone_dir)
    try:
        wiring = root_wiring.write(clone_dir, placed,
                                   known_values=frozenset(facts))
    except Exception as e:
        return f"ERROR: Failed to generate the root module wiring: {e}"

    # Keep terraform's local provider cache and state out of the PR.
    try:
        _write_gitignore(clone_dir)
    except Exception as e:
        logger.warning(f"Could not write .gitignore into the clone: {e}")

    # Unit directories first, the clone root (the landing-zone draft plus
    # the wiring) last. The root now compiles the units, so a unit-level
    # error must meet its OWN pass's fix worker — the only one handed that
    # unit's files — before the root pass reads the repaired code. Root
    # first would burn the root's fix attempts on an error its worker
    # structurally cannot see and leave `.` recorded as failing after the
    # unit's own pass fixed it (run_validation never re-validates a dir).
    tf_dirs = sorted(placed.values()) + ["."]
    try:
        report = await validation.run_validation(clone_dir, tf_dirs, terraform_bin)
    except Exception as e:
        # The auto-fix loop already absorbs per-directory worker failures; this
        # guards anything else (a terraform crash, a disk error) so the tool
        # returns a clean ERROR instead of an unhandled traceback.
        logger.exception("Terraform validation run failed")
        return f"ERROR: Terraform validation failed unexpectedly: {e}"

    # The units' Kubernetes manifests get their own offline gate: terraform
    # validate never reads a .yaml file, so a broken manifest would otherwise
    # ship in the PR unexamined.
    try:
        manifests = validation.check_unit_manifests(clone_dir, sorted(placed.values()))
    except Exception as e:
        logger.exception("Manifest structure check failed")
        return f"ERROR: Manifest structure check failed unexpectedly: {e}"
    report["manifests"] = manifests
    if manifests["invalid"]:
        report["all_valid"] = False

    # What the root pass actually reached, recorded for the reviewer — and a
    # check that it still reaches it: the wiring file is one of the root
    # directory's .tf files, so the auto-fix loop could have answered a unit
    # error by deleting or rewriting the module block that surfaced it.
    # A customer-owned file at the wiring name (never overwritten), a
    # unit whose Terraform no pass compiles, and a unit whose required
    # variable is named like a module meta-argument (no call can pass it,
    # so it cannot be wired) are findings for the same reason: each leaves
    # unit code the green report would not have read.
    report["root_wiring"] = wiring
    wiring["dropped"] = ([] if wiring["foreign_file"]
                         else root_wiring.dropped_modules(clone_dir, wiring))
    if (wiring["dropped"] or wiring["foreign_file"] or wiring["nested_tf"]
            or wiring["unsatisfied"]):
        report["all_valid"] = False

    # Print the recorded values behind the root's variables into
    # migration.auto.tfvars — the link that turns a green validate into a
    # plan that prompts for nothing. Deliberately AFTER the auto-fix loop:
    # the root's fix worker writes root-level .tf files, so the variable
    # set is only settled now — values printed before the run would miss a
    # declaration the worker added (the wiring byte-recheck's reason, one
    # file over). The facts are RE-collected for the same reason: the fix
    # loop can rewrite the very literals they came from, and a pre-fix
    # value would ship stale under a recorded-fact comment. Fatal on
    # failure for the wiring's reason too: shipping variables without
    # their recorded values is the 2026-08-15 hand-authored-tfvars bridge
    # again, not a degraded pass.
    try:
        facts = tfvars_printer.collect_values(config, clone_dir)
        tfvars = tfvars_printer.emit(clone_dir, facts)
    except Exception as e:
        return f"ERROR: Failed to emit the tfvars file: {e}"

    # The printer's findings gate exactly like the wiring's: a no-default
    # root variable with no recorded source is a `terraform plan` value
    # prompt shipped to the customer (the 2026-08-15 bridge), and a
    # customer-owned file at the tfvars name means the recorded values
    # were NOT written.
    report["tfvars"] = tfvars
    if tfvars["missing"] or tfvars["foreign_file"]:
        report["all_valid"] = False

    # Auto-fixed unit directories: fold the corrected files back into the unit
    # blobs so the ledger (and the Review UI) shows the code that validated.
    # The fix loop only ever touches .tf files, so the corrected set is MERGED
    # over the unit's existing file list — replacing the list wholesale would
    # drop the unit's manifests from the ledger, and the next materialize
    # (ship declined, PR retry, revise cycle) would then drop them from the
    # clone and the PR while the manifest gate reads "checked: 0".
    fixed_dirs = {f["dir"] for f in report.get("fixed", [])}
    dir_to_unit = {rel: uid for uid, rel in placed.items()}
    for rel in fixed_dirs & set(dir_to_unit):
        unit_id = dir_to_unit[rel]
        entry = next(u for u in done_units if u["unit"]["unit_id"] == unit_id)
        merged = {f["path"]: f["content"]
                  for f in (entry.get("result") or {}).get("files", [])}
        merged.update(validation._read_tf_files(os.path.join(clone_dir, rel)))
        entry.setdefault("result", {})["files"] = [
            {"path": p, "content": c} for p, c in sorted(merged.items())]
        entry["unit"]["autofixed"] = True
        try:
            bucket.blob(f"{UNIT_BLOB_PREFIX}/{unit_id}.json").upload_from_string(
                json.dumps(entry, indent=2), content_type="application/json")
        except Exception as e:
            logger.error(f"Failed to persist auto-fixed unit {unit_id}: {e}")

    # The workload-identity unit's ksa_annotations output is a machine-read
    # contract (the exports gsa_bindings derivation parses it — DESIGN §4.6),
    # so its presence and shape are gated here like everything else the unit
    # ships — and so is emptiness over recorded IRSA bindings: the undecided-
    # project escape routes to review loudly, never past this gate silently.
    # Deliberately AFTER the auto-fix fold-back: the check must attest
    # to the content that actually validated and ships, not the pre-fix blob —
    # an auto-fix that drops or de-literalizes the output must be caught.
    contract = ksa_contract.check_units(done_units)
    report["ksa_annotations_contract"] = contract
    if contract["findings"]:
        report["all_valid"] = False

    # Same arrangement for the gateway unit's marked-Gateway output: the
    # exports gateway derivation reads the marker and metadata.namespace,
    # and an unmarked Gateway ships clean while publishing nothing.
    gateway = gateway_contract.check_units(done_units)
    report["shared_gateway_contract"] = gateway
    if gateway["findings"]:
        report["all_valid"] = False

    # And for the cluster-dns unit: the worker translated a Corefile it was
    # handed verbatim, against a knowledge document; this gate checks the
    # target shape, the owner boundary and that no IP or name was invented
    # or dropped — without knowing what any CoreDNS plugin means — and sweeps
    # every other unit for the objects only that family may ship.
    cluster_dns = clusterdns_contract.check_units(done_units)
    report["cluster_dns_contract"] = cluster_dns
    if cluster_dns["findings"]:
        report["all_valid"] = False

    # And for the compute-class units: one ComputeClass per typed Karpenter
    # NodePool, judged against the pool's own facts and the family table the
    # knowledge document carries (shape, spot flags, families, forbidden
    # fields, nothing dropped, the derived decision that planned it); every
    # other unit is swept for a ComputeClass it must not ship.
    compute_class = computeclass_contract.check_units(done_units)
    report["compute_class_contract"] = compute_class
    if compute_class["findings"]:
        report["all_valid"] = False

    # The enforced coverage-omission gate (2026-08-15 audit: a landing zone
    # shipped with no google_container_cluster and validate passed). A
    # facts-present landing-zone/platform-translation map row with no
    # artifact behind it — no done citing unit, no explicitly skipped
    # citation, no clone-scanned resource, no documented pin — fails the run
    # naming the row. Fatal on internal error like the root wiring: a gate
    # that degrades to a log line is the vacuous pass again.
    try:
        cover = coverage_gate.check(
            plan, state_dict["variables"].get("discovery_inventory") or {},
            clone_dir)
    except Exception as e:
        logger.exception("Coverage omission gate failed")
        return f"ERROR: Coverage omission gate failed unexpectedly: {e}"
    report["coverage_omission"] = cover
    if cover["findings"]:
        report["all_valid"] = False

    comparisons = build_comparisons(done_units, report, placed)
    try:
        bucket.blob(VALIDATION_REPORT_BLOB).upload_from_string(
            json.dumps(report, indent=2), content_type="application/json")
        bucket.blob(COMPARISONS_BLOB).upload_from_string(
            json.dumps(comparisons, indent=2), content_type="application/json")
    except Exception as e:
        return f"ERROR: Failed to persist the validation report: {e}"

    try:
        platform_dag = load_dag(bucket, "platform_dag.json")
    except Exception as e:
        return f"ERROR: {e}"
    state_def = platform_dag["states"][current_state]

    counts = (f"{len(report['clean'])} clean, {len(report['fixed'])} auto-fixed, "
              f"{len(report['remaining'])} still failing")
    if manifests["checked"]:
        counts += (f"; {manifests['checked'] - len(manifests['invalid'])}/"
                   f"{manifests['checked']} manifests structurally valid")
    if wiring["file"]:
        counts += (f"; {len(wiring['modules'])}/{len(placed)} unit directories "
                   f"wired into the generated {wiring['file']}")
    elif placed:
        # No wiring file on disk (nothing to wire, or a foreign file blocked
        # it): say so, so a tf-bearing-but-unreached unit is never silent.
        counts += f"; 0/{len(placed)} unit directories wired (no wiring file)"
    if wiring["unwired"]:
        counts += ("; unwired (no Terraform of their own): "
                   + ", ".join(wiring["unwired"]))
    if tfvars["printed"] or tfvars["missing"]:
        counts += (f"; tfvars: {len(tfvars['printed'])} recorded value(s) "
                   f"printed, {len(tfvars['missing'])} root variable(s) with "
                   "no recorded source")
    counts += (f"; coverage: {cover['checked']} facts-present row(s) checked, "
               f"{len(cover['findings'])} finding(s) (omissions and cluster fields)"
               + (f", {len(cover['cluster_fields']['notes'])} cluster_dns value(s) "
                  "behind an expression (accepted, not verified)"
                  if cover["cluster_fields"]["notes"] else ""))
    # What the field scan accepted without proof rides the ship elicitation
    # (first_notice), not the return value: the user weighs it BEFORE
    # approving the PR. On the failure path the FAILED reply below carries it.
    unverified_notice = (
        "Before you approve — the validate gate accepted a cluster_dns value "
        "it could not read; confirm it resolves to \"CLOUD_DNS\":\n"
        + "\n".join(f"- {n}" for n in cover["cluster_fields"]["notes"])
        if cover["cluster_fields"]["notes"] else None)

    if not report["all_valid"]:
        dest = state_def["transitions"]["on_failure"]
        state_dict["history"].append(
            f"Generated code validation failed ({counts}); returning to review")
        state_dict["history"].append(f"Transitioned {current_state} -> {dest} via run_generated_validation")
        state_dict["current_state"] = dest
        data = json.dumps(state_dict, indent=2)
        try:
            bucket.blob("platform/onboarding/state.json").upload_from_string(
                data, content_type="application/json", if_generation_match=generation)
        except exceptions.PreconditionFailed:
            return "ERROR: Concurrent update conflict. Your changes were not saved."
        remaining = "; ".join(
            [f"{r['dir']}: {r['error'][:200]}" for r in report["remaining"]]
            + [f"{m['file']}: {m['error'][:200]}" for m in manifests["invalid"]]
            + [f"{c['unit_id']}: {c['error'][:200]}"
               for c in contract["findings"] + gateway["findings"] + cluster_dns["findings"]
               + compute_class["findings"]]
            + [f"coverage map row '{c['row_key']}': {c['error'][:300]}"
               for c in cover["findings"]]
            + [f"not a failure, a cluster field note the gate accepted without "
               f"proof (confirm with the user): {n}"
               for n in cover["cluster_fields"]["notes"]]
            + ([f"{root_wiring.WIRING_FILENAME}: the auto-fix pass removed or "
                f"rewrote the module block(s) {', '.join(wiring['dropped'])}, "
                "so the root validate no longer reaches those units as "
                "generated"]
               if wiring["dropped"] else [])
            + ([f"{root_wiring.WIRING_FILENAME}: the target repository "
                "already contains a file by this name that the agent did not "
                "generate; it was left untouched, so the root pass does not "
                "reach the units — ask the reviewer whether to move or "
                "remove it"]
               if wiring["foreign_file"] else [])
            + [f"{placed[u]}: this unit's Terraform sits only in "
               "subdirectories, so no validate pass compiles it (its own "
               "pass finds no configuration and the root cannot wire it) — "
               "revise the unit to carry .tf files at its top level"
               for u in wiring["nested_tf"]]
            + [f"{placed[u['unit_id']]}: required variable(s) "
               f"{', '.join(u['variables'])} are named like module "
               "meta-arguments, so no module call can pass them and the "
               "unit cannot be wired — revise the unit to rename them"
               for u in wiring["unsatisfied"]]
            + [f"{m['file']}:{m['line']}: root variable '{m['name']}' has "
               "no recorded source — the ledger records no value for it, "
               "so `terraform plan` would halt on a value prompt. Record "
               "the fact (workspace settings, the landing-zone design) or "
               "revise the unit so the value is not required; the agent "
               "does not invent values or defaults"
               for m in tfvars["missing"]]
            + ([f"{tfvars_printer.TFVARS_FILENAME}: the target repository "
                "already contains a file by this name that the agent did "
                "not generate; it was left untouched, so the recorded "
                "values were NOT written — ask the reviewer whether to "
                "move or remove it"]
               if tfvars["foreign_file"] else []))
        return (
            f"Validation FAILED after the auto-fix pass ({counts}).\n"
            f"Still failing: {remaining}\n"
            f"Current State: {dest} — revise or skip the failing units (a "
            "failure at '.' is the clone root: the landing-zone draft plus "
            "the generated wiring, not a unit), then re-approve."
        )

    dest = state_def["transitions"]["on_success"]
    state_dict["history"].append(f"Generated code validated ({counts})")
    state_dict["history"].append(f"Transitioned {current_state} -> {dest} via run_generated_validation")
    state_dict["current_state"] = dest

    # Platform translation completed: publish the translation slice of
    # exports.json (storage_class_menu, gateway, gsa_bindings — DESIGN §4.6)
    # now that the validation report and comparisons are persisted.
    # Best-effort: a failure warns in the response, never blocks the ship.
    exports_warning = exports_lib.publish_translation_exports(
        bucket, done_units, ksa_contract.parse_ksa_annotations,
        state_dict["variables"].get("translation_plan"))
    exports_note = (f"\n{exports_warning}" if exports_warning
                    else "\nexports.json refreshed (translation fields).")

    # The walk raises the ship-approval elicitation (Gate E) and, on approve,
    # runs the PR submission — one call from validation to shipped.
    _, message, error = await run_dispatch_loop(
        ctx, state_dict, platform_dag, config,
        f"Generated code validated ({counts}).", first_notice=unverified_notice)
    if error:
        return error

    end_state = state_dict["current_state"]
    data = json.dumps(state_dict, indent=2)
    try:
        bucket.blob("platform/onboarding/state.json").upload_from_string(
            data, content_type="application/json", if_generation_match=generation)
    except exceptions.PreconditionFailed:
        return "ERROR: Concurrent update conflict. Your changes were not saved."

    if end_state == "STATE_TRANSLATION_VALIDATE":
        # Ship approval succeeded but the PR submission failed; the graph is
        # back at this step to retry. Surface the reason; re-run this tool
        # once the cause (git access, branch, network) is resolved.
        return (
            f"Validation passed ({counts}), but opening the pull request did not complete.\n"
            f"{message}\n"
            f"Current State: {end_state} — re-run run_generated_validation to retry the PR once resolved."
            f"{exports_note}"
        )
    if end_state == "STATE_TRANSLATION_REVIEW":
        # Ship declined: the graph is back at the unit review.
        return (
            f"Validation passed ({counts}).\n"
            f"Current State: {end_state}\n{message}\n"
            "The before/after comparison and per-unit tradeoffs are in the Review UI."
            f"{exports_note}"
        )
    # The PR opened and the graph parked at the next phase's entry.
    # Translation's story ends here; this tool deliberately names no state
    # beyond its own phase, so the graph can grow without touching it.
    return (
        f"Validation passed ({counts}) and the pull request was opened — "
        f"translation is complete.\n"
        f"Current State: {end_state} — call get_next_stage to continue.\n{message}"
        f"{exports_note}"
    )


def register(mcp):
    """Register this step's tools on the FastMCP server."""
    mcp.tool()(run_generated_validation)
