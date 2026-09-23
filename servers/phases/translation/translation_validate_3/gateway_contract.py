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

"""The gateway unit's shared-Gateway output contract.

The exports gateway derivation (DESIGN §4.6) publishes `exports.gateway`
only from a Gateway API manifest carrying `gkma.dev/shared-gateway: "true"`,
and reads the namespace off `metadata.namespace` alone. Both requirements
live in the unit's brief, and a brief is a prompt: a worker that follows
every other instruction and drops the annotation ships a Gateway that
validates clean, publishes nothing, and parks every developer HTTPRoute
with no signal anywhere. This gate makes the same requirements machine-read,
exactly as ksa_contract does for the workload-identity unit's
`ksa_annotations` output.

Deliberately narrow: only units of kind `gateway` are checked, and only the
properties the exports derivation actually depends on. The scan itself is
`exports.gateway_documents`, so this gate and the derivation can never
disagree about what counts as a Gateway.

One deliberate widening (2026-08-15 audit, family 1): exports derives the
attach point from unit FILES, not the cluster, so a Gateway nothing can
attach to — `from: Same` or no allowedRoutes at all, a Selector no shipped
Namespace satisfies, or a namespace no shipped manifest creates — would
publish cleanly and park every developer HTTPRoute with no signal anywhere.
Over a well-formed marked Gateway this gate therefore also judges the attach
half, against the product-owned access label (exports.GATEWAY_ACCESS_LABEL:
one definition for the two briefs and this gate) and the Namespace documents
the run actually ships. Namespaces the run does not ship are not judged.
"""

from servers.dag.server import exports as exports_lib

UNIT_KIND = "gateway"
MARKER = exports_lib.SHARED_GATEWAY_MARKER
ACCESS_LABEL = exports_lib.GATEWAY_ACCESS_LABEL
ACCESS_VALUE = exports_lib.GATEWAY_ACCESS_VALUE


def _display(doc: dict) -> str:
    return f"{doc.get('namespace') or '?'}/{doc.get('name') or '?'}"


def _namespace_documents(done_units: list) -> list:
    """[{unit_id, name, labels}] for every v1 Namespace the run ships.

    The same file walk and hardened YAML posture as the Gateway scan
    (exports' helpers), over ALL units — the platform namespace is the
    gateway unit's to emit and the workload namespaces are the tenancy
    unit's, and this gate must see both sides. Parse-broken files are
    skipped here: naming them is the per-unit checks' business.
    """
    found = []
    for entry in done_units or []:
        unit = (entry or {}).get("unit") or {}
        unit_id = str(unit.get("unit_id") or unit.get("kind") or "?")
        files = exports_lib._unit_files(entry, (".yaml", ".yml"))
        for path, content in sorted(files.items()):
            docs, error = exports_lib._load_yaml_docs(content)
            if error:
                continue
            for doc in docs:
                if not (isinstance(doc, dict)
                        and doc.get("kind") == "Namespace"
                        and str(doc.get("apiVersion") or "") == "v1"):
                    continue
                metadata = doc.get("metadata") or {}
                name = metadata.get("name")
                if not (isinstance(name, str) and name.strip()):
                    continue
                labels = metadata.get("labels")
                found.append({
                    "unit_id": unit_id,
                    "name": name.strip(),
                    "labels": labels if isinstance(labels, dict) else {},
                })
    return found


def check_unit(entry: dict) -> list:
    """Contract errors for ONE gateway unit's outputs (empty when clean)."""
    marked, unmarked, parse_errors = exports_lib.gateway_documents(entry)
    errors = [f"{e}; the exports derivation cannot read this unit"
              for e in parse_errors]
    if not marked and not unmarked:
        return errors + [
            "the unit ships no Gateway API Gateway manifest (.yaml with "
            "apiVersion gateway.networking.k8s.io/*); exports.gateway stays "
            "null and every developer HTTPRoute has no attach point"]
    if not marked:
        return errors + [
            "Gateway " + ", ".join(_display(g) for g in unmarked)
            + f' carries no {MARKER}: "true" annotation or label, so the '
            "exports derivation reads it as an app-scoped gateway and "
            "publishes null; the shared entry point must be marked"]
    if len(marked) > 1:
        return errors + [
            f"{len(marked)} Gateway manifests carry the {MARKER} marker ("
            + ", ".join(_display(g) for g in marked)
            + "); the brief orders exactly one and the derivation refuses "
            "to guess between them"]
    if unmarked:
        errors.append(
            "the unit ships extra Gateway manifests beside the marked one ("
            + ", ".join(_display(g) for g in unmarked)
            + "); the brief orders exactly ONE Gateway")
    if not str(marked[0].get("namespace") or "").strip():
        errors.append(
            f"the marked Gateway {marked[0].get('name') or '?'} sets no "
            "metadata.namespace; exports.gateway.namespace is read from that "
            "field alone, so a namespace applied out of band publishes as "
            "null and no HTTPRoute parentRef resolves")
    return errors


def _policy_errors(doc: dict, display: str, namespaces: list) -> list:
    """Attach-policy errors for the one well-formed marked Gateway.

    The brief orders every listener's allowedRoutes.namespaces to be
    `from: All`, or `from: Selector` with matchLabels of exactly the
    product access label. `from: Same` — also the Gateway API default
    when the policy is absent — accepts a cross-namespace HTTPRoute and
    never attaches it, which is why absence fails too. A Selector is
    judged against the Namespace documents the run ships; namespaces the
    run does not ship are none of this gate's business.
    """
    listeners = (doc.get("spec") or {}).get("listeners")
    if not isinstance(listeners, list) or not listeners:
        return [f"the marked Gateway {display} declares no listeners, so "
                "nothing can ever attach to the published entry point"]
    errors = []
    for index, entry in enumerate(listeners):
        listener = entry if isinstance(entry, dict) else {}
        name = str(listener.get("name") or f"#{index}")
        allowed = listener.get("allowedRoutes")
        policy = (allowed or {}).get("namespaces") if isinstance(
            allowed, dict) else None
        frm = (policy or {}).get("from") if isinstance(policy, dict) else None
        if frm == "All":
            continue
        if frm in (None, "Same"):
            what = ("sets no explicit allowedRoutes.namespaces policy"
                    if frm is None else "sets `from: Same`")
            errors.append(
                f"listener {name} {what}: the API server accepts every "
                "developer HTTPRoute and attaches none (Same is the Gateway "
                "API default) — set `from: All`, or `from: Selector` on "
                f'`{ACCESS_LABEL}: "{ACCESS_VALUE}"`')
            continue
        if frm != "Selector":
            errors.append(
                f"listener {name} sets allowedRoutes.namespaces.from to "
                f"{frm!r}, which is neither of the two shapes the brief "
                "orders — set `from: All` or the labeled `from: Selector`")
            continue
        errors.extend(_selector_errors(policy, name, namespaces))
    return errors


def _selector_errors(policy: dict, name: str, namespaces: list) -> list:
    """`from: Selector` judged against the run's shipped Namespaces."""
    selector = policy.get("selector")
    selector = selector if isinstance(selector, dict) else {}
    match = selector.get("matchLabels")
    if (not isinstance(match, dict) or not match
            or selector.get("matchExpressions")):
        return [
            f"listener {name} uses a Selector this gate cannot verify "
            "(matchExpressions, or no matchLabels): the brief orders "
            f'matchLabels of exactly `{ACCESS_LABEL}: "{ACCESS_VALUE}"`']
    errors = []
    for ns in namespaces:
        missing = [f'{key}: "{value}"' for key, value in sorted(match.items())
                   if ns["labels"].get(key) != value]
        if missing:
            errors.append(
                f"listener {name}'s Selector does not match the shipped "
                f"Namespace {ns['name']} (unit {ns['unit_id']} — missing "
                + ", ".join(missing) + f"): HTTPRoutes in {ns['name']} "
                "will be accepted and never attach")
    return errors


def check_units(done_units: list) -> dict:
    """Contract check over the done unit blobs the validate step ships.

    Only `gateway`-kind units are checked. Returns {"checked": [unit_id],
    "findings": [{"unit_id", "error"}]} — a legacy blob without the family
    is simply not checked, never a crash.

    The cross-unit attach half (module docstring) runs only over a
    per-unit-clean gateway unit — a malformed Gateway goes back to review
    on its shape findings first — and its findings carry that unit's id:
    the brief that owns the attach contract is the one to revise.
    """
    report = {"checked": [], "findings": []}
    namespaces = None
    for entry in done_units or []:
        unit = (entry or {}).get("unit") or {}
        if unit.get("kind") != UNIT_KIND:
            continue
        unit_id = str(unit.get("unit_id") or UNIT_KIND)
        report["checked"].append(unit_id)
        errors = check_unit(entry)
        if namespaces is None:
            namespaces = _namespace_documents(done_units)
        if not errors:
            marked, _, _ = exports_lib.gateway_documents(entry)
            errors.extend(_policy_errors(
                marked[0]["doc"], _display(marked[0]), namespaces))
            errors.extend(_existence_errors(marked[0], namespaces))
        errors.extend(_collision_errors(namespaces))
        for error in errors:
            report["findings"].append({"unit_id": unit_id, "error": error})
    return report


def _existence_errors(marked: dict, namespaces: list) -> list:
    """Some shipped manifest must create the Gateway's own namespace."""
    ns = str(marked.get("namespace") or "").strip()
    if not ns or ns in {n["name"] for n in namespaces}:
        return []
    return [
        "no manifest shipped by this run creates the Gateway's namespace "
        f'— `kubectl apply` of the PR fails with `namespaces "{ns}" not '
        "found`, while exports (derived from the unit files, not the "
        "cluster) still publishes the attach point, so developer "
        "HTTPRoutes apply cleanly and never attach; emit the Namespace "
        "manifest beside the Gateway"]


def _collision_errors(namespaces: list) -> list:
    """One Namespace name created by two units is one object, two owners."""
    owners = {}
    for ns in namespaces:
        owners.setdefault(ns["name"], set()).add(ns["unit_id"])
    return [
        f'Namespace "{name}" is created by more than one unit '
        f"({', '.join(sorted(units))}) — one name, one owner: the platform "
        "namespace is the gateway unit's to emit and the recorded workload "
        "namespaces are the tenancy unit's"
        for name, units in sorted(owners.items()) if len(units) > 1]
