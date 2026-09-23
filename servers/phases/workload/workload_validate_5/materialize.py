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

"""Materialization of a component's done units into the target clone.

Carrier discipline: a chart chosen for the
chart-preserving strategy ships ONCE under the component output root
(workloads/<component>/<chart-path>/) — the SOURCE chart tree is copied
first, then each unit's owned-file edits are applied in deterministic plan
order (wkld-manifests, the carrier owner, is first in every plan). A file
is an EDIT when its written content differs from the source file (the
owner's re-emission of an untouched file is carriage, not an edit); a file
edited by two units is a validation finding, never a merge. Plain manifest
files land under workloads/<component>/<unit_id>/. Every write is
realpath-escape-guarded (the materialize_units posture).

Kustomize sources materialize as their whole local graph in
source-root-relative layout under the component root, so ../base references
keep resolving; the re-render gate then runs over the materialized overlay.
"""

import os
import shutil

from servers.phases.workload.workload_plan_2 import planner
from servers.phases.workload.workload_translate_3 import translator


def component_rel(component: str) -> str:
    return f"workloads/{component}"


def _label_dir(rel_dir: str) -> str:
    """Where a source dir lands under the component root. A repo whose root
    IS the chart materializes as 'chart/' — the component root itself also
    holds the unit directories."""
    return "chart" if rel_dir in ("", ".") else rel_dir


def _safe_join(root: str, rel: str) -> str:
    real_root = os.path.realpath(root)
    full = os.path.realpath(os.path.join(root, rel))
    if full != real_root and not full.startswith(real_root + os.sep):
        raise ValueError(f"path '{rel}' escapes the component root")
    return full


def _source_content(source_root: str, rel: str):
    """The source file's content, or None (new file / unreadable)."""
    full = os.path.join(source_root, rel)
    if os.path.islink(full) or not os.path.isfile(full):
        return None
    try:
        with open(full, "r", encoding="utf-8") as f:
            return f.read()
    except (OSError, UnicodeDecodeError):
        return None


def _copy_source_dir(source_root: str, rel_dir: str, root: str,
                     dst_rel: str) -> None:
    label = "" if rel_dir in ("", ".") else rel_dir
    src = os.path.join(source_root, label) if label else source_root
    real_src = os.path.realpath(src)
    real_source_root = os.path.realpath(source_root)
    if real_src != real_source_root \
            and not real_src.startswith(real_source_root + os.sep):
        raise ValueError(f"source dir '{rel_dir}' escapes the source root")
    if not os.path.isdir(src):
        raise ValueError(f"source dir '{rel_dir or '.'}' does not exist "
                         "under the source root")
    translator.copy_tree_files(src, _safe_join(root, dst_rel) if dst_rel
                               else root)


def _copy_source_file(source_root: str, rel: str, root: str) -> None:
    """Copies one kustomize-graph FILE into the same relative place under the
    component root. The destination escape guard is here; the source guard
    and the symlink/regular-file rules live in translator.copy_source_file."""
    _safe_join(root, rel)
    translator.copy_source_file(source_root, rel, root)


def _carrier_strategy_findings(plan: dict, per_unit: list,
                               charts_used: list) -> list:
    """Decision 6, the half the conflict list cannot see: the carrier's OWNER
    unit decides flatten-vs-chart-edit for the whole chart.

    Copying the source chart tree whenever ANY unit cites the chart shipped
    the verbatim un-migrated chart next to a flattened translation — two
    Deployments, one of them still pointing at ECR — with no conflict and
    all_valid true. When a satellite unit emits carrier-relative files while
    the owner did not (it flattened, or it is skipped/parked), that is a
    finding naming both, not a silent copy.
    """
    owners = {c.get("chart_path"): c.get("owner")
              for c in plan.get("carriers") or []}
    emitters = {unit["unit_id"]: set(groups["charts"])
                for unit, groups in per_unit}
    findings = []
    for chart in charts_used:
        owner = owners.get(chart)
        satellites = sorted(u for u, charts in emitters.items()
                            if chart in charts and u != owner)
        if owner and chart in emitters.get(owner, ()):
            continue
        findings.append({
            "chart_path": chart, "owner": owner, "units": satellites,
            "error": (
                f"unit(s) {', '.join(satellites) or '(none)'} emitted files "
                f"against the chart carrier '{chart}', but its owner "
                f"{owner or '(unrecorded)'} did not — the owner flattened the "
                "chart to plain manifests, or is skipped/parked. Materializing "
                "both would ship the un-migrated source chart alongside the "
                "translation (duplicate objects, the source copy still "
                "pointing at the source registry). Agree on ONE strategy: "
                "retranslate the satellite unit(s) as plain manifests, or "
                "retranslate the owner against the chart.")})
    return findings


def _split_done_entries(plan: dict, done_entries: list) -> list:
    """[(unit, groups)] in plan unit order — the deterministic apply order
    (wkld-manifests, the carrier owner, is first in every plan)."""
    order = {u["unit_id"]: i for i, u in enumerate(plan.get("units", []))}
    entries = sorted(done_entries,
                     key=lambda e: order.get(e["unit"]["unit_id"], len(order)))
    per_unit = []
    for entry in entries:
        unit = entry["unit"]
        files = (entry.get("result") or {}).get("files", [])
        groups, error = translator.split_output_files(unit, files)
        if error:
            raise ValueError(f"unit {unit['unit_id']}: {error}")
        per_unit.append((unit, groups))
    return per_unit


def materialize_component(clone_dir: str, component: str, plan: dict,
                          done_entries: list, source_root: str) -> dict:
    """Materializes done units under <clone>/workloads/<component>/.

    Returns {"component_rel", "unit_dirs", "render_dirs", "conflicts",
    "strategy", "placed"} — unit_dirs/render_dirs are clone-relative (the
    gates' input), conflicts are the two-unit-edit findings,
    strategy are the mixed-carrier findings (a satellite unit editing a
    chart its owner did not keep), placed maps unit_id -> written
    clone-relative paths. Raises ValueError on any path escape or on output
    the split cannot route.
    """
    comp_rel = component_rel(component)
    clone_real = os.path.realpath(clone_dir)
    root = os.path.realpath(os.path.join(clone_dir, comp_rel))
    if not root.startswith(clone_real + os.sep):
        raise ValueError(f"component root escapes the clone: {comp_rel!r}")
    # Clear first: a file a previous run emitted but this run no longer
    # produces must not linger, get validated, and ship as stale code.
    if os.path.isdir(root):
        shutil.rmtree(root)
    os.makedirs(root, exist_ok=True)

    per_unit = _split_done_entries(plan, done_entries)
    charts_used = sorted({c for _, g in per_unit for c in g["charts"]})
    kdirs_used = sorted({d for _, g in per_unit for d in g["kustomize"]})
    strategy = _carrier_strategy_findings(plan, per_unit, charts_used)

    render_dirs = []
    for chart in charts_used:
        dst_rel = _label_dir(chart)
        _copy_source_dir(source_root, chart, root, dst_rel)
        render_dirs.append({"kind": "helm", "dir": f"{comp_rel}/{dst_rel}",
                            "source": chart})
    for kdir in kdirs_used:
        graph = planner.resolve_kustomize_graph(source_root, kdir)
        for graph_dir in graph["dirs"]:
            _copy_source_dir(source_root, graph_dir, root,
                             "" if graph_dir in ("", ".") else graph_dir)
        # The graph's FILES matter as much as its dirs: `resources:
        # - ../../shared/ns.yaml` resolves a plain file in a directory with
        # no kustomization.yaml of its own, so copying only `dirs` produced a
        # tree kubectl could not render ("evalsymlink failure ... /shared")
        # even though build_worker_input had shown the worker that file.
        for graph_file in graph["files"]:
            _copy_source_file(source_root, graph_file, root)
        render_dirs.append({
            "kind": "kustomize",
            "dir": comp_rel if kdir in ("", ".") else f"{comp_rel}/{kdir}",
            "source": kdir})

    # Owned-file edits, in plan order. A write whose content equals the
    # source file is carriage (the owner re-emitting an untouched chart
    # file), not an edit — and carriage over an already-copied source tree is
    # a no-op by construction, so it is SKIPPED rather than written: writing
    # it silently reverted an earlier unit's edit of the same path while
    # staying out of the conflict list (content == source, so no writer was
    # recorded). Everything else records its writer, and a second writer on
    # the same path is the decision-6 finding.
    writers, placed, unit_dirs = {}, {}, []
    for unit, groups in per_unit:
        unit_id = unit["unit_id"]
        placed.setdefault(unit_id, [])
        for chart, files in sorted(groups["charts"].items()):
            base = _label_dir(chart)
            prefix = "" if chart in ("", ".") else chart + "/"
            for rel, content in sorted(files.items()):
                target_rel = f"{base}/{rel}"
                if _source_content(source_root, prefix + rel) == content:
                    continue
                _write(root, target_rel, content)
                placed[unit_id].append(f"{comp_rel}/{target_rel}")
                writers.setdefault(target_rel, []).append(unit_id)
        for kdir, files in sorted(groups["kustomize"].items()):
            base = "" if kdir in ("", ".") else kdir
            for rel, content in sorted(files.items()):
                target_rel = f"{base}/{rel}" if base else rel
                src_rel = f"{kdir}/{rel}" if base else rel
                if _source_content(source_root, src_rel) == content:
                    continue
                _write(root, target_rel, content)
                placed[unit_id].append(f"{comp_rel}/{target_rel}")
                writers.setdefault(target_rel, []).append(unit_id)
        if groups["plain"]:
            unit_dirs.append(f"{comp_rel}/{unit_id}")
        for path, content in groups["plain"]:
            _write(root, f"{unit_id}/{path}", content)
            placed[unit_id].append(f"{comp_rel}/{unit_id}/{path}")

    conflicts = [
        {"path": f"{comp_rel}/{rel}", "units": sorted(set(unit_ids))}
        for rel, unit_ids in sorted(writers.items())
        if len(set(unit_ids)) > 1]
    return {"component_rel": comp_rel, "unit_dirs": sorted(unit_dirs),
            "render_dirs": render_dirs, "conflicts": conflicts,
            "strategy": strategy, "placed": placed}


def _write(root: str, rel: str, content: str) -> None:
    full = _safe_join(root, rel)
    os.makedirs(os.path.dirname(full), exist_ok=True)
    with open(full, "w", encoding="utf-8") as f:
        f.write(content)
