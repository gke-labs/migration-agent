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

"""Image-scan and render actions for the discovery image pipeline.

Three INTERNAL_TASK_SERVER_MUTATION actions, each (variables, config) ->
(transition_key, message) per the engine contract:

  scan_image_references   STATE_DISCOVERY_IMAGE_SCAN
  render_image_targets    STATE_DISCOVERY_RENDER
  mark_render_declined    STATE_DISCOVERY_RENDER_DECLINED

Rendering is best-effort and per-target isolated: a chart that fails to
render is recorded render_failed with a reason and never blocks the others
or the migration. Rendering produces manifests; images are only ever
referenced, never built.
"""

import datetime
import logging
import os
import shutil
import subprocess

import yaml
from google.api_core import exceptions

from servers.dag.state_management import (
    get_bucket_name,
    load_inventory,
    save_inventory,
)
import servers.dag.state_management as state_mgr

from . import images

logger = logging.getLogger("migration-dag")

# Per-subprocess budget. A chart that takes longer than this to template is a
# problem to surface, not to wait out.
RENDER_TIMEOUT_S = 120
# Rendered output beyond this is not parsed; the target is marked failed.
MAX_RENDER_OUTPUT_BYTES = 5 * 1024 * 1024


def _bucket(config):
    return state_mgr.gcs_client.bucket(get_bucket_name(config["ledger_uri"]))


def _run(cmd: list[str], cwd: str | None = None) -> tuple[bool, str]:
    """Runs a render command. Returns (ok, stdout-or-error-summary)."""
    try:
        res = subprocess.run(
            cmd, cwd=cwd, capture_output=True, text=True, check=True,
            timeout=RENDER_TIMEOUT_S,
        )
    except subprocess.CalledProcessError as e:
        return False, (e.stderr or e.stdout or "").strip()[:500] or f"{cmd[0]} failed"
    except subprocess.TimeoutExpired:
        return False, f"timed out after {RENDER_TIMEOUT_S}s"
    except Exception as e:
        return False, str(e)
    if len(res.stdout) > MAX_RENDER_OUTPUT_BYTES:
        return False, f"rendered output exceeds {MAX_RENDER_OUTPUT_BYTES} bytes"
    return True, res.stdout


def _harvest(rendered_yaml: str) -> list[str]:
    """Parses rendered multi-doc YAML and collects image refs, tolerating
    per-document parse errors."""
    docs = []
    try:
        docs = list(yaml.safe_load_all(rendered_yaml))
    except yaml.YAMLError:
        # Fall back to splitting on document markers so one bad doc does not
        # discard the rest of the render.
        for chunk in rendered_yaml.split("\n---"):
            try:
                docs.append(yaml.safe_load(chunk))
            except yaml.YAMLError:
                continue
    return images.walk_yaml_for_images(docs)


def _helm_needs_dependency_build(chart_dir: str) -> bool:
    chart_file = os.path.join(chart_dir, "Chart.yaml")
    try:
        with open(chart_file, "r", encoding="utf-8") as f:
            chart = yaml.safe_load(f) or {}
    except (OSError, yaml.YAMLError):
        return False
    return bool(chart.get("dependencies")) and not os.path.isdir(
        os.path.join(chart_dir, "charts")
    )


def _render_helm_target(target: dict, root_dir: str, helm_bin: str) -> list[tuple[str, dict]]:
    """Renders one chart once per values file (defaults-only if none), unioning
    the harvested refs. Mutates target status/reason. Returns (ref, provenance)."""
    chart_dir = os.path.join(root_dir, target["root"])

    if _helm_needs_dependency_build(chart_dir):
        ok, err = _run([helm_bin, "dependency", "build", chart_dir])
        if not ok:
            target["status"] = "render_failed"
            target["reason"] = f"helm dependency build failed: {err}"
            return []

    harvested = []
    failures = []
    runs = target.get("values_files") or [None]
    for values_file in runs:
        cmd = [helm_bin, "template", "discovery", chart_dir]
        if values_file:
            cmd += ["--values", os.path.join(chart_dir, values_file)]
        ok, out = _run(cmd)
        if not ok:
            failures.append(f"{values_file or '(defaults)'}: {out}")
            continue
        provenance = {
            "kind": "rendered",
            "render_target": target["id"],
            "values_file": values_file,
        }
        harvested.extend((ref, provenance) for ref in _harvest(out))

    if len(failures) == len(runs):
        target["status"] = "render_failed"
        target["reason"] = "; ".join(failures)[:1000]
    else:
        target["status"] = "rendered"
        target["reason"] = ("; ".join(failures)[:1000]) if failures else None
    return harvested


def _render_kustomize_target(target: dict, root_dir: str, kustomize_cmd: list[str]) -> list[tuple[str, dict]]:
    """Renders one kustomization directory. Mutates target status/reason."""
    ok, out = _run(kustomize_cmd + [os.path.join(root_dir, target["root"])])
    if not ok:
        target["status"] = "render_failed"
        target["reason"] = out
        return []
    target["status"] = "rendered"
    target["reason"] = None
    provenance = {"kind": "rendered", "render_target": target["id"], "values_file": None}
    return [(ref, provenance) for ref in _harvest(out)]


def action_scan_image_references(variables: dict, config: dict) -> tuple[str, str]:
    """Builds a fresh inventory: literal image refs plus pending render targets.

    A fresh scan overwrites the whole blob — rediscovery semantics. Stores the
    target counts in variables for the render-approval prompt.

    The data-dependency scan is deliberately NOT here: it runs after the scope
    is confirmed (STATE_DISCOVERY_DATA_SCAN, discovery_datascan_3/) so it can honor
    the operator's exclusions, which this pre-scope pass cannot.
    """
    # discovery_root_dir is set by discover_configuration_files to the indexed
    # directory. source_path is NOT a usable fallback: it is repo-relative.
    root_dir = variables.get("discovery_root_dir")
    if not root_dir or not os.path.isdir(root_dir):
        return "on_failure", f"Image scan failed: source directory not found: {root_dir!r}"

    try:
        literal_refs = images.extract_literal_image_refs(root_dir)
        targets = images.detect_render_targets(root_dir)

        inventory = {
            "schema_version": "1.0",
            "generated_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "source": {
                "root_dir": root_dir,
                "source_repo_url": variables.get("source_repo_url"),
                "source_branch": variables.get("source_branch"),
            },
            "images": [],
            "render_targets": targets,
        }
        images.merge_images(
            inventory,
            [(ref, {"kind": "literal", "file": path}) for ref, path in literal_refs],
        )

        bucket = _bucket(config)
        _, generation = load_inventory(bucket)
        save_inventory(bucket, inventory, generation)
    except exceptions.PreconditionFailed:
        return "on_failure", "Concurrent update conflict writing the image inventory."
    except Exception as e:
        logger.exception("Image scan failed")
        return "on_failure", f"Image scan failed: {e}"

    helm_count = sum(1 for t in targets if t["type"] == "helm")
    kustomize_count = sum(1 for t in targets if t["type"] == "kustomize")
    variables["helm_chart_count"] = helm_count
    variables["kustomize_root_count"] = kustomize_count

    summary = f"Found {len(inventory['images'])} literal image reference(s)."
    if not targets:
        return "on_no_render_targets", (
            f"{summary} No Helm charts or Kustomize roots require rendering; "
            "the image inventory is complete."
        )
    return "on_success", (
        f"{summary} {helm_count} Helm chart(s) and {kustomize_count} Kustomize "
        "root(s) require rendering to complete the image inventory."
    )


def action_render_image_targets(variables: dict, config: dict) -> tuple[str, str]:
    """Renders every pending target, merging harvested image refs into the
    inventory. Partial failure is success with per-target reasons; on_failure
    only when nothing could be attempted."""
    # discovery_root_dir is set by discover_configuration_files to the indexed
    # directory. source_path is NOT a usable fallback: it is repo-relative.
    root_dir = variables.get("discovery_root_dir")
    if not root_dir or not os.path.isdir(root_dir):
        return "on_failure", f"Render failed: source directory not found: {root_dir!r}"

    try:
        bucket = _bucket(config)
        inventory, generation = load_inventory(bucket)
    except Exception as e:
        return "on_failure", f"Render failed: could not read inventory: {e}"
    if inventory is None:
        return "on_failure", "Render failed: no inventory found; run the scan first."

    pending = [t for t in inventory["render_targets"] if t["status"] == "pending"]
    if not pending:
        return "on_success", "No pending render targets; nothing to render."

    helm_bin = shutil.which("helm")
    kubectl_bin = shutil.which("kubectl")
    kustomize_bin = shutil.which("kustomize")
    kustomize_cmd = None
    if kubectl_bin:
        kustomize_cmd = [kubectl_bin, "kustomize"]
    elif kustomize_bin:
        kustomize_cmd = [kustomize_bin, "build"]

    attempted = 0
    harvested = []
    for target in pending:
        if target["type"] == "helm":
            if not helm_bin:
                target["status"] = "render_failed"
                target["reason"] = "helm binary not found on this workstation"
                continue
            attempted += 1
            harvested.extend(_render_helm_target(target, root_dir, helm_bin))
        else:
            if not kustomize_cmd:
                target["status"] = "render_failed"
                target["reason"] = "kubectl/kustomize binary not found on this workstation"
                continue
            attempted += 1
            harvested.extend(_render_kustomize_target(target, root_dir, kustomize_cmd))

    images.merge_images(inventory, harvested)

    try:
        save_inventory(bucket, inventory, generation)
    except exceptions.PreconditionFailed:
        return "on_failure", "Concurrent update conflict writing the image inventory."
    except Exception as e:
        return "on_failure", f"Failed to save inventory after rendering: {e}"

    rendered = sum(1 for t in pending if t["status"] == "rendered")
    failed = sum(1 for t in pending if t["status"] == "render_failed")
    summary = (
        f"Rendered {rendered}/{len(pending)} target(s)"
        + (f", {failed} failed (recorded in the inventory with reasons)" if failed else "")
        + f". Image inventory now holds {len(inventory['images'])} reference(s)."
    )
    if attempted == 0:
        return "on_failure", (
            f"No render target could be attempted: required binaries are missing. {summary} "
            "The migration continues with literal image references only."
        )
    return "on_success", summary


def action_mark_render_declined(variables: dict, config: dict) -> tuple[str, str]:
    """Stamps every pending target unrendered_declined so the inventory records
    exactly what the human opted out of."""
    try:
        bucket = _bucket(config)
        inventory, generation = load_inventory(bucket)
    except Exception as e:
        return "on_failure", f"Could not read inventory: {e}"
    if inventory is None:
        return "on_failure", "No inventory found; run the scan first."

    declined = 0
    for target in inventory["render_targets"]:
        if target["status"] == "pending":
            target["status"] = "unrendered_declined"
            target["reason"] = "user declined rendering"
            declined += 1

    try:
        save_inventory(bucket, inventory, generation)
    except exceptions.PreconditionFailed:
        return "on_failure", "Concurrent update conflict writing the image inventory."
    except Exception as e:
        return "on_failure", f"Failed to save inventory: {e}"

    return "on_success", (
        f"Recorded {declined} unrendered target(s) as declined. The migration "
        "continues with literal image references only; the unrendered targets "
        "are listed in the inventory for later follow-up."
    )


# Dispatch table for the discovery drain loop and main.run_internal_mutation.
ACTIONS = {
    "scan_image_references": action_scan_image_references,
    "render_image_targets": action_render_image_targets,
    "mark_render_declined": action_mark_render_declined,
}
