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

"""Pure seed-index logic for the workload scope step.

The seed is the exports `component_seed_index` (D2/D3): {path: {"kinds",
"namespaces", "team_labels", "names"}}. `names` is not read here — it exists
for the data gate's consumer join (`workload/datagate`) — but it is part of
the entry shape this module receives. Everything here derives from that index alone
— literally-true claims, conditioned on what the index records, never
unconditional statements (the _storage_notes grammar).
"""

from servers.phases import scope_algebra

# Cluster-scoped kinds the seed may list. Platform-owned per the coverage
# map's workload rows; the scope step ADVISES excluding files carrying them
# — v1 does not machine-enforce (the planner's residual bucket catches leaks).
CLUSTER_SCOPED_KINDS = {
    "StorageClass",
    "Namespace",
    "CustomResourceDefinition",
    "NodePool",
    "NodeClass",
    "EC2NodeClass",
    "Provisioner",
}

_NAMESPACE_CAP = 8
_KIND_CAP = 10


def cluster_scoped_in(entry: dict) -> list:
    """The entry's kinds that are cluster-scoped (platform-owned)."""
    return sorted(k for k in (entry.get("kinds") or []) if k in CLUSTER_SCOPED_KINDS)


def terrain_note(seed_index: dict) -> str:
    """Which ownership signals discriminate in THIS estate (D13).

    Derived facts only; every claim is conditioned on what the index records.
    Presented to the developer BEFORE any filter suggestion.
    """
    entries = seed_index or {}
    total = len(entries)
    if total == 0:
        return ("Terrain: the component seed index is empty — no ownership "
                "signals can be derived from it.")
    namespaces = sorted({ns for e in entries.values()
                         for ns in (e.get("namespaces") or [])})
    labeled = {p: e for p, e in entries.items() if e.get("team_labels")}
    label_values = sorted({v for e in labeled.values() for v in e["team_labels"]})
    no_ns = [p for p, e in entries.items() if not e.get("namespaces")]
    kinds = {}
    for e in entries.values():
        for k in e.get("kinds") or []:
            kinds[k] = kinds.get(k, 0) + 1
    cluster_scoped = sorted(k for k in kinds if k in CLUSTER_SCOPED_KINDS)

    lines = [f"Terrain ({total} file(s) in the seed index):"]
    if namespaces:
        shown = ", ".join(namespaces[:_NAMESPACE_CAP])
        more = (f" (+{len(namespaces) - _NAMESPACE_CAP} more)"
                if len(namespaces) > _NAMESPACE_CAP else "")
        lines.append(f"- namespaces recorded: {len(namespaces)} ({shown}{more})")
    else:
        lines.append("- no entry records a namespace")
    if no_ns:
        lines.append(f"- {len(no_ns)} file(s) carry no namespace metadata "
                     "(e.g. helm-chart or terraform kinded files)")
    if labeled:
        lines.append(f"- team labels present on {len(labeled)} of {total} "
                     f"file(s); values: {', '.join(label_values)}")
    else:
        lines.append("- no entry records a *team*-suffixed label")
    top = sorted(kinds.items(), key=lambda kv: (-kv[1], kv[0]))[:_KIND_CAP]
    if top:
        lines.append("- kinds: " + ", ".join(f"{k} x{n}" for k, n in top))
    if cluster_scoped:
        lines.append(f"- cluster-scoped kinds present ({', '.join(cluster_scoped)}): "
                     "platform-owned per the coverage map — advise excluding "
                     "the files carrying them")
    with_labels = " and team labels" if labeled else ""
    if len(namespaces) > 1:
        lines.append(f"- discriminating signals here: {len(namespaces)} distinct "
                     f"namespaces — namespace is a usable ownership signal in "
                     f"this estate{with_labels}; path prefixes always apply. "
                     "Namespace is a signal, not a component boundary: several "
                     "teams can share one namespace.")
    elif len(namespaces) == 1:
        lines.append(f"- discriminating signals here: every namespaced entry "
                     f"sits in one namespace ({namespaces[0]}) — namespace does "
                     f"not discriminate in this estate; path prefixes{with_labels} do")
    else:
        lines.append("- discriminating signals here: no namespace metadata is "
                     f"recorded — path prefixes{with_labels} are the usable signals")
    return "\n".join(lines)


def token_guess(seed_index: dict, component: str) -> tuple:
    """(matched paths, note) for the component-id token match. ALWAYS a guess.

    Tokens are the id's hyphen-split parts minus the 'component' suffix
    token, substring-matched across path / namespaces / team-label values.
    Zero matches is stated honestly: it never means no candidates exist.
    """
    tokens = [t for t in (component or "").split("-") if t and t != "component"]
    if not tokens:
        return [], ("Token guess: the component id yields no usable tokens, so "
                    "no guess is offered. This says nothing about which "
                    "candidates exist — browse the unfiltered index.")
    matched = []
    for path, entry in sorted((seed_index or {}).items()):
        hay = ([path] + list(entry.get("namespaces") or [])
               + list(entry.get("team_labels") or []))
        if any(t in h for t in tokens for h in hay):
            matched.append(path)
    shown = ", ".join(repr(t) for t in tokens)
    if matched:
        note = (f"Token guess (a GUESS, not a fact): {len(matched)} file(s) "
                f"mention {shown} in their path, namespaces or team labels. "
                "Verify with the user before scoping on it.")
    else:
        note = (f"Token guess (a GUESS, not a fact): no seed entry mentions "
                f"{shown} — the guess matched nothing. That does NOT mean no "
                "candidates exist; the unfiltered index follows.")
    return matched, note


def filter_seed(seed_index: dict, path_glob=None, namespace=None,
                team_label=None, token=None) -> dict:
    """AND-combined filters over the seed index. Returns {path: entry}.

    path_glob uses the shared scope-algebra matcher (exact path, directory
    prefix, or glob); namespace/team_label are exact membership; token is a
    substring across path, namespaces and team-label values.
    """
    result = {}
    for path, entry in (seed_index or {}).items():
        if path_glob and not scope_algebra.matches(path, path_glob):
            continue
        if namespace and namespace not in (entry.get("namespaces") or []):
            continue
        if team_label and team_label not in (entry.get("team_labels") or []):
            continue
        if token:
            hay = ([path] + list(entry.get("namespaces") or [])
                   + list(entry.get("team_labels") or []))
            if not any(token in h for h in hay):
                continue
        result[path] = entry
    return result


def resolve_component_scope(seed_paths, scope: dict) -> list:
    """In-scope = matches >=1 `included` pattern AND matches no `excluded` one.

    This deliberately INVERTS the discovery algebra's include-wins/default-in
    rule because the base semantics invert: a discovery scope subtracts from
    everything, a component scope selects from nothing. Reuses the algebra's
    pattern matcher; exclusion carves out of the selection.
    """
    included = (scope or {}).get("included") or []
    excluded = (scope or {}).get("excluded") or []
    resolved = []
    for path in sorted(seed_paths or []):
        if not any(scope_algebra.matches(path, inc) for inc in included):
            continue
        if any(scope_algebra.matches(path, exc) for exc in excluded):
            continue
        resolved.append(path)
    return resolved
