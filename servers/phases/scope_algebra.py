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

"""Pure scope algebra, shared across phases.

Lifted from servers/phases/discovery/discovery_scope_2/scope.py (which
remains as a thin re-export): the workload phase needs the same pattern
matching and include/exclude update semantics, and importing it through the
discovery package would traverse discovery/__init__.py and its GCS/MCP tool
imports — exactly the package-init coupling DESIGN §9.4 calls out as
blocking pure-test collection. The one addition over the lifted code is
apply_scope_update's include_first mode for default-OUT (select-from-nothing)
scopes; the default keeps discovery's default-IN behaviour bit-for-bit.

A scope is {"root_dir", "excluded": [...], "included": [...]}. Patterns match
an exact path, a directory prefix ("vendor/"), or a glob ("**/test_*.yaml").
Include wins over exclude, and include entries may also name paths outside the
original manifest (extra files or directories to bring into scope).
"""

import fnmatch
import os


def normalize_pattern(pattern: str) -> str:
    normalized = pattern.strip().replace(os.sep, "/")
    while normalized.startswith("./"):
        normalized = normalized[2:]
    return normalized.rstrip("/") if normalized != "/" else ""


def matches(path: str, pattern: str) -> bool:
    """True if path (manifest-relative or absolute) matches the scope pattern."""
    path_n = path.replace(os.sep, "/")
    pattern_n = normalize_pattern(pattern)
    if not pattern_n:
        return False
    if path_n == pattern_n:
        return True
    if path_n.startswith(pattern_n + "/"):
        return True
    return fnmatch.fnmatch(path_n, pattern_n)


def is_excluded(path: str, scope: dict) -> bool:
    """Include wins over exclude; otherwise any excluded pattern removes the path."""
    if any(matches(path, inc) for inc in scope.get("included", [])):
        return False
    return any(matches(path, exc) for exc in scope.get("excluded", []))


def filter_files(files: list, scope: dict) -> tuple:
    """Splits manifest file entries into (kept, removed) under the scope."""
    kept, removed = [], []
    for entry in files:
        (removed if is_excluded(entry["path"], scope) else kept).append(entry)
    return kept, removed


def apply_scope_update(scope: dict, exclude: list = None, include: list = None,
                       include_first: bool = False) -> tuple:
    """Returns (new_scope, notes). Pure: the input scope is not mutated.

    - exclude entries append to "excluded" (deduped); an entry identical to an
      existing include is dropped from "included" first.
    - include entries that exactly match an excluded pattern remove that
      pattern (un-exclude). Under the default default-IN algebra (discovery:
      a scope subtracts from everything) un-excluding restores the path, so
      the pattern is NOT also appended to "included"; all other include
      entries append to "included".
    - include_first=True is for the INVERTED default-OUT algebra (a component
      scope selects from nothing — seed.resolve_component_scope): there,
      un-excluding alone selects nothing, so every include pattern also lands
      in "included".
    """
    new_scope = {
        "root_dir": scope["root_dir"],
        "excluded": list(scope.get("excluded", [])),
        "included": list(scope.get("included", [])),
    }
    notes = []

    for raw in exclude or []:
        pattern = normalize_pattern(raw)
        if not pattern:
            continue
        if pattern in new_scope["included"]:
            new_scope["included"].remove(pattern)
            notes.append(f"'{pattern}' removed from includes")
        if pattern not in new_scope["excluded"]:
            new_scope["excluded"].append(pattern)
            notes.append(f"'{pattern}' excluded")

    for raw in include or []:
        pattern = normalize_pattern(raw)
        if not pattern:
            continue
        if pattern in new_scope["excluded"]:
            new_scope["excluded"].remove(pattern)
            notes.append(f"'{pattern}' un-excluded")
            if not include_first:
                continue  # default-in: un-excluding alone restores the path
        if pattern not in new_scope["included"]:
            new_scope["included"].append(pattern)
            notes.append(f"'{pattern}' included")

    return new_scope, notes


def summarize_scope(manifest: dict, scope: dict, max_listed: int = 25) -> str:
    """Human-readable summary of the effective scope over a manifest."""
    import json

    kept, removed = filter_files(manifest.get("files", []), scope)
    summary = {
        "root_dir": scope["root_dir"],
        "in_scope_files": len(kept),
        "excluded_files": len(removed),
        "excluded_patterns": scope.get("excluded", []),
        "included_extras": scope.get("included", []),
        "excluded_sample": [f["path"] for f in removed[:max_listed]],
    }
    if len(removed) > max_listed:
        summary["excluded_sample"].append(f"... and {len(removed) - max_listed} more")
    return json.dumps(summary, indent=2)


def index_included_paths(scope: dict, index_fn) -> tuple:
    """Indexes include entries that point at paths outside the manifest.

    index_fn is index_configuration_files (injected for testability). Entries
    that resolve inside root_dir are left alone — the filter already handles
    those. Returns (extra_entries, notes); extra entries carry absolute paths,
    which the chunk reader handles transparently.
    """
    root_dir = scope["root_dir"]
    extras, notes = [], []
    for entry in scope.get("included", []):
        candidate = entry if os.path.isabs(entry) else os.path.join(root_dir, entry)
        candidate = os.path.abspath(candidate)
        inside_root = candidate.startswith(os.path.abspath(root_dir) + os.sep)
        if inside_root and os.path.exists(candidate):
            continue  # un-exclude/override of an in-root path; filter covers it
        if os.path.isdir(candidate):
            sub = index_fn(candidate)
            for f in sub["files"]:
                extras.append({**f, "path": os.path.join(sub["root_dir"], f["path"])})
            notes.append(f"'{entry}': added {len(sub['files'])} files")
        elif os.path.isfile(candidate):
            extras.append({"path": candidate, "size": os.path.getsize(candidate), "kind": "other"})
            notes.append(f"'{entry}': added 1 file")
        else:
            notes.append(f"'{entry}': path not found, ignored")
    return extras, notes
