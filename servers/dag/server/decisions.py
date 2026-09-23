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

"""The landing-zone decision registry, read out of the knowledge document.

`## The four target-shape decisions` in
servers/phases/landingzone/knowledge/gke-landing-zone.md is a markdown table:
one decision id, its discovery trigger, its choices in order (the first is the
default recommendation), the "take it when" prose per choice, and the cluster
mode each choice implies. That table is the authority on what a decision is
and which tokens it accepts, and this module is how the server reads it — the
same arrangement as blocker_criteria.py and coverage_map.py: humans and the
machine read one table, so the choice set cannot drift between the document
and a hardcoded copy (DESIGN §14 issue 12), and adding a choice is a
documentation edit plus the module guidance and validate contract that choice
needs.

The module also owns the ONE projection from recorded choices to a cluster
mode (`cluster_mode`). Before it existed four readers each interpreted the
tokens on their own (the node-pool skip, the cluster-dns brief, the exports
cluster type, the cluster-dns validate contract) with different inputs, and
disagreed on ordinary designs. Every reader now calls this projection or
`mode_of`; code that legitimately needs one specific choice imports its
constant from here, and a test scans the tree for token literals anywhere
else.

Import-light on purpose (stdlib only, python 3.9 syntax): the Kokoro
presubmit loads it with only `servers/dag` on sys.path, and the parse is lazy
so a test that injects a synthetic table never drags in the real one.
Failure to parse is fatal at start-up, matching the other two registries.
"""

import logging
import os
import re

logger = logging.getLogger("migration-dag")

_HERE = os.path.dirname(os.path.realpath(__file__))

# servers/dag/server/ -> repository root, the same anchor dag_validation uses.
REPO_ROOT = os.path.abspath(os.path.join(_HERE, "..", "..", ".."))

KNOWLEDGE_DOC = os.path.join(
    "servers", "phases", "landingzone", "knowledge", "gke-landing-zone.md"
)

# The tokens code may key on. Every constant is checked against the parsed
# table at start-up, so the document stays the authority for the set and the
# code cannot key on a token the document dropped. Readers that need only the
# mode call cluster_mode()/mode_of(); readers that need one specific choice
# (the ComputeClass family's planned/skipped switch, the NAP ceiling contract)
# import the constant.
KARPENTER_NAP = "GKE_STANDARD_NAP"
KARPENTER_AUTOPILOT = "GKE_AUTOPILOT"
KARPENTER_COMPUTECLASS = "GKE_STANDARD_COMPUTECLASS"
PRIVILEGED_STANDARD = "GKE_STANDARD"
PRIVILEGED_BYPASS = "GKE_AUTOPILOT_BYPASS"
GPU_STANDARD = "GKE_STANDARD_SPECIALIZED"
GPU_AUTOPILOT = "GKE_AUTOPILOT_SPECIALIZED"
PEERING_PUBLIC = "PUBLIC_AUTHORIZED_NETS"
PEERING_PRIVATE = "PRIVATE_ONLY_PEERING"

# Constants that MUST exist in the table.
REQUIRED_TOKENS = (
    KARPENTER_NAP, KARPENTER_AUTOPILOT, KARPENTER_COMPUTECLASS,
    PRIVILEGED_STANDARD, PRIVILEGED_BYPASS,
    GPU_STANDARD, GPU_AUTOPILOT, PEERING_PUBLIC, PEERING_PRIVATE,
)

MODES = ("standard", "autopilot")
IMPLIES_VALUES = frozenset(
    MODES + tuple("advisory:" + m for m in MODES) + ("—", "-", "none"))

_DECISIONS_SECTION_RE = re.compile(r"^#{2,4}\s+The\s+(?:four\s+)?target-shape\s+decisions\b", re.I)
_DEFAULTS_SECTION_RE = re.compile(r"^#{2,4}\s+Standing\s+defaults\b", re.I)
_NEXT_SECTION_RE = re.compile(r"^#{1,4}\s+")
_SEPARATOR_RE = re.compile(r"^\|[\s:|-]+\|$")
_TOKEN_RE = re.compile(r"^[A-Z][A-Z0-9_]*$")
_ID_RE = re.compile(r"^[a-z][a-z0-9_]*$")

# The predicate grammar a continuation row's trigger cell may carry, as one
# backticked expression. Three atoms, joined by ` or `; anything else fails
# start-up. Review-facing and machine-read at once: the "take it when" a
# reviewer reads is the predicate the planner evaluates.
_PATH = r"[a-z_][a-z0-9_]*(?:\[\])?(?:\.[a-z_][a-z0-9_]*(?:\[\])?)*"
_ATOM_COUNT_RE = re.compile(r"^count\((%s)\)\s*>\s*(\d+)$" % _PATH)
_ATOM_LEN_RE = re.compile(r"^any\((%s)\[\]\.([a-z_][a-z0-9_]*),\s*len\s*>\s*(\d+)\)$" % _PATH)
_ATOM_CONTAINS_RE = re.compile(
    r"^any\((%s)\[\]\.([a-z_][a-z0-9_]*),\s*contains\s+([A-Za-z0-9_./-]+)\)$" % _PATH)
_BACKTICKED_RE = re.compile(r"^`([^`]+)`$")

_cache = None


class DecisionsError(Exception):
    """Raised when the decision table cannot be read. Fatal at start-up."""


def _split_row(line):
    return [cell.strip() for cell in line.strip().strip("|").split("|")]


def _strip_ticks(cell):
    return cell.replace("`", "").strip()


def _table_rows(lines, section_re, source, title):
    """(header cells, body rows) of the first table after the section heading."""
    start = None
    for i, line in enumerate(lines):
        if section_re.match(line):
            start = i + 1
            break
    if start is None:
        raise DecisionsError(f"{source}: no '{title}' section; the decision registry has no source")
    header, rows, header_seen = None, [], False
    for line in lines[start:]:
        if _NEXT_SECTION_RE.match(line):
            break
        if not line.strip().startswith("|"):
            if header_seen and rows:
                break
            continue
        if _SEPARATOR_RE.match(line.strip()):
            header_seen = True
            continue
        cells = _split_row(line)
        if not header_seen:
            header = cells
            continue
        rows.append((cells, line.strip()))
    if header is None or not rows:
        raise DecisionsError(f"{source}: the '{title}' table has no rows")
    return header, rows


def _column(header, needle, source, title):
    for i, name in enumerate(header):
        if needle in _strip_ticks(name).casefold():
            return i
    raise DecisionsError(f"{source}: the '{title}' table has no '{needle}' column")


def _parse_predicate(text, source, line):
    """Parses one backticked predicate expression into atoms, or raises."""
    atoms = []
    for part in re.split(r"\s+or\s+", text.strip()):
        part = part.strip()
        m = _ATOM_COUNT_RE.match(part)
        if m:
            atoms.append({"op": "count", "path": m.group(1), "n": int(m.group(2))})
            continue
        m = _ATOM_LEN_RE.match(part)
        if m:
            atoms.append({"op": "len", "path": m.group(1), "field": m.group(2), "n": int(m.group(3))})
            continue
        m = _ATOM_CONTAINS_RE.match(part)
        if m:
            atoms.append({"op": "contains", "path": m.group(1), "field": m.group(2),
                          "literal": m.group(3)})
            continue
        raise DecisionsError(
            f"{source}: unparseable predicate atom {part!r} in the decision table row "
            f"{line!r}; the grammar is count(<path>) > n, any(<path>[].<field>, len > n), "
            "any(<path>[].<field>, contains <literal>), joined by ' or '")
    return {"text": text.strip(), "atoms": atoms}


def parse_decision_table(text, source):
    """Extracts the decision registry from the knowledge document.

    Returns {"decisions": {id: {"id", "trigger", "choices": [{"token",
    "take_when", "implies", "predicate"}]}}, "defaults": [{"setting",
    "default", "deviate_when", "fact_path", "expected"}]}.

    Carry-forward rule: a row with an empty decision_id cell continues the
    previous decision; its trigger cell, when non-empty, is that choice's
    predicate (a single backticked expression in the grammar above). The
    first row of a decision carries the trigger prose and the default choice.
    """
    lines = text.splitlines()
    header, rows = _table_rows(lines, _DECISIONS_SECTION_RE, source, "target-shape decisions")
    c_id = _column(header, "decision_id", source, "target-shape decisions")
    c_trig = _column(header, "trigger", source, "target-shape decisions")
    c_choice = _column(header, "choice", source, "target-shape decisions")
    c_when = _column(header, "take it when", source, "target-shape decisions")
    c_mode = _column(header, "implies mode", source, "target-shape decisions")
    width = max(c_id, c_trig, c_choice, c_when, c_mode) + 1

    decisions, order = {}, []
    current = None
    seen_tokens = {}
    for cells, line in rows:
        if len(cells) != width:
            raise DecisionsError(
                f"{source}: decision table row has {len(cells)} cells, expected {width} "
                f"(a '|' inside a prose cell?): {line!r}")
        did = _strip_ticks(cells[c_id])
        trigger = cells[c_trig].strip()
        token = _strip_ticks(cells[c_choice])
        take_when = cells[c_when].strip()
        implies = _strip_ticks(cells[c_mode]).casefold()
        if did:
            if not _ID_RE.match(did):
                raise DecisionsError(f"{source}: malformed decision_id {did!r}")
            if did in decisions:
                raise DecisionsError(f"{source}: decision {did!r} listed twice")
            current = {"id": did, "trigger": trigger, "choices": []}
            decisions[did] = current
            order.append(did)
            predicate = None
        else:
            if current is None:
                raise DecisionsError(
                    f"{source}: continuation row with no preceding decision: {line!r}")
            predicate = None
            if trigger:
                m = _BACKTICKED_RE.match(trigger)
                if not m:
                    raise DecisionsError(
                        f"{source}: a continuation row's trigger cell must be one backticked "
                        f"predicate expression: {line!r}")
                predicate = _parse_predicate(m.group(1), source, line)
        if not _TOKEN_RE.match(token):
            raise DecisionsError(f"{source}: malformed choice token {token!r} in {line!r}")
        if token in seen_tokens:
            raise DecisionsError(
                f"{source}: choice token {token!r} appears under {seen_tokens[token]!r} and "
                f"{current['id']!r}; tokens are unique across decisions")
        seen_tokens[token] = current["id"]
        if implies not in IMPLIES_VALUES:
            raise DecisionsError(
                f"{source}: unknown 'Implies mode' value {cells[c_mode]!r} for {token!r} "
                f"(expected one of: standard, autopilot, advisory:standard, "
                f"advisory:autopilot, —)")
        if implies in ("-", "none"):
            implies = "—"
        current["choices"].append({
            "token": token, "take_when": take_when, "implies": implies,
            "predicate": predicate,
        })
    for did in order:
        if not decisions[did]["choices"]:
            raise DecisionsError(f"{source}: decision {did!r} has no choices")
        first = decisions[did]["choices"][0]
        if first["predicate"] is not None:
            raise DecisionsError(f"{source}: the default choice of {did!r} carries a predicate")

    return {"decisions": decisions, "order": order,
            "defaults": _parse_defaults(lines, source)}


def _parse_defaults(lines, source):
    header, rows = _table_rows(lines, _DEFAULTS_SECTION_RE, source, "Standing defaults")
    c_setting = _column(header, "setting", source, "Standing defaults")
    c_default = _column(header, "default", source, "Standing defaults")
    c_deviate = _column(header, "deviate", source, "Standing defaults")
    c_fact = _column(header, "fact path", source, "Standing defaults")
    c_expected = _column(header, "expected", source, "Standing defaults")
    width = len(header)
    out = []
    for cells, line in rows:
        if len(cells) != width:
            raise DecisionsError(
                f"{source}: Standing defaults row has {len(cells)} cells, expected {width}: {line!r}")
        fact_path = _strip_ticks(cells[c_fact]) or None
        expected = _strip_ticks(cells[c_expected]) or None
        if (fact_path is None) != (expected is None):
            raise DecisionsError(
                f"{source}: Standing defaults row {cells[c_setting]!r} fills only one of "
                "'Fact path' / 'Expected'; fill both or neither")
        out.append({"setting": cells[c_setting], "default": cells[c_default],
                    "deviate_when": cells[c_deviate], "fact_path": fact_path,
                    "expected": expected})
    return out


def load_registry(repo_root=REPO_ROOT, refresh=False):
    """The parsed registry, read once per process (lazily, never at import)."""
    global _cache
    if _cache is not None and not refresh:
        return _cache
    path = os.path.join(repo_root, KNOWLEDGE_DOC)
    try:
        with open(path, "r", encoding="utf-8") as f:
            text = f.read()
    except OSError as e:
        raise DecisionsError(f"cannot read the landing-zone knowledge document {path}: {e}")
    registry = parse_decision_table(text, KNOWLEDGE_DOC)
    _cache = registry
    logger.debug(f"Loaded {len(registry['decisions'])} landing-zone decisions from {KNOWLEDGE_DOC}")
    return registry


def validate_decisions(repo_root=REPO_ROOT):
    """Start-up check: the table parses and every declared constant is in it."""
    registry = load_registry(repo_root, refresh=True)
    tokens = set(all_tokens(registry))
    missing = [t for t in REQUIRED_TOKENS if t not in tokens]
    if missing:
        raise DecisionsError(
            f"{KNOWLEDGE_DOC}: the decision table no longer carries the token(s) code keys on: "
            f"{', '.join(missing)}")
    logger.debug("Decision registry validation passed")


# --- read side ---------------------------------------------------------------

def _registry(registry):
    return registry if registry is not None else load_registry()


def all_tokens(registry=None):
    reg = _registry(registry)
    return [c["token"] for did in reg["order"] for c in reg["decisions"][did]["choices"]]


def choices(registry=None):
    """{decision_id: (token, ...)} in table order — the VALID_CHOICES shape."""
    reg = _registry(registry)
    return {did: tuple(c["token"] for c in reg["decisions"][did]["choices"])
            for did in reg["order"]}


def default_choice(decision_id, registry=None):
    reg = _registry(registry)
    return reg["decisions"][decision_id]["choices"][0]["token"]


def _token_index(registry):
    return {c["token"]: (did, c) for did in registry["order"]
            for c in registry["decisions"][did]["choices"]}


def mode_of(token, registry=None):
    """(mode | None, advisory): the cluster mode a token implies.

    Never raises: a `—` token, an unknown string, or None reads as (None, False).
    An advisory token reads as its base mode with advisory=True.
    """
    reg = _registry(registry)
    if not isinstance(token, str):
        return None, False
    entry = _token_index(reg).get(token)
    if entry is None:
        return None, False
    implies = entry[1]["implies"]
    if implies in MODES:
        return implies, False
    if implies.startswith("advisory:"):
        return implies[len("advisory:"):], True
    return None, False


def voting_ids(registry=None):
    reg = _registry(registry)
    return [did for did in reg["order"]
            if any(c["implies"] in MODES for c in reg["decisions"][did]["choices"])]


def advisory_ids(registry=None):
    reg = _registry(registry)
    return [did for did in reg["order"]
            if any(c["implies"].startswith("advisory:") for c in reg["decisions"][did]["choices"])]


def flat_choices(decisions):
    """lz_decisions as a flat {id: value} map; a resolved {choice: ...} dict is unwrapped."""
    flat = {}
    for key, value in (decisions or {}).items():
        flat[key] = value.get("choice") if isinstance(value, dict) else value
    return flat


def _recorded(flat, decision_id, registry):
    """The recorded token for an id, or None when unrecorded.

    "Recorded" means the flattened value is one of the table's tokens for that
    id; any other value (only a hand-edited ledger holds one) is unrecorded.
    """
    value = flat.get(decision_id)
    if isinstance(value, str) and value in choices(registry).get(decision_id, ()):
        return value
    return None


def triggers_available(triggers, registry=None):
    """Whether a triggers value is usable by the projection: a dict carrying
    every voting id. Anything else is treated as unavailable as a whole (the
    manual inventory path stores an agent dict verbatim), and the projection
    falls back to choice-only voting. One definition, shared by the design
    step's warning and the projection."""
    return isinstance(triggers, dict) and all(did in triggers for did in voting_ids(registry))


def project(decisions, triggers, registry=None):
    """The single projection from recorded choices (+ triggers) to a mode.

    Returns {"mode", "reason", "considered": [(id, token, implied_mode,
    advisory)], "fallback": bool, "fallback_reason": None | "unavailable" |
    "mute"}. Rules (coverage-guards v9, G0):
    - Triggers are available iff `triggers` is a dict carrying every voting
      id. Otherwise the whole dict is treated as unavailable and the
      projection is choice-only voting over every recorded voting id —
      exactly the pre-registry exports semantics.
    - With triggers: a voting id votes when its trigger fired or its recorded
      choice is not the table default; an advisory id is considered under the
      same rule (an absent advisory trigger key counts as unfired) but never
      changes the result. If no voting id was considered, fall back to
      choice-only voting and consider no advisory.
    - Voters agreeing -> that mode. Conflicting -> None, "disagree: ...".
      No voting id recorded -> None, "none recorded".
    """
    reg = _registry(registry)
    flat = flat_choices(decisions)
    voters = voting_ids(reg)
    advisories = advisory_ids(reg)
    recorded = {did: _recorded(flat, did, reg) for did in voters + advisories}
    available = triggers_available(triggers, reg)

    def implied(did):
        return mode_of(recorded[did], reg)[0]

    considered = []
    fallback = False
    fallback_reason = None
    if available:
        for did in voters:
            token = recorded[did]
            if token is None:
                continue
            fired = triggers.get(did) is True
            if fired or token != default_choice(did, reg):
                considered.append((did, token, implied(did), False))
        if considered:
            for did in advisories:
                token = recorded[did]
                if token is None:
                    continue
                fired = triggers.get(did) is True
                if fired or token != default_choice(did, reg):
                    considered.append((did, token, implied(did), True))
        else:
            fallback, fallback_reason = True, "mute"
    else:
        fallback, fallback_reason = True, "unavailable"
    if fallback:
        considered = [(did, recorded[did], implied(did), False)
                      for did in voters if recorded[did] is not None]

    voted_modes = {m for (_d, _t, m, adv) in considered if not adv}
    if not voted_modes:
        return {"mode": None, "reason": "none recorded", "considered": considered,
                "fallback": fallback, "fallback_reason": fallback_reason}
    if len(voted_modes) > 1:
        names = ", ".join(f"{d}={t}" for (d, t, _m, _a) in considered)
        return {"mode": None, "reason": "disagree: " + names, "considered": considered,
                "fallback": fallback, "fallback_reason": fallback_reason}
    return {"mode": voted_modes.pop(), "reason": None, "considered": considered,
            "fallback": fallback, "fallback_reason": fallback_reason}


def cluster_mode(decisions, triggers, registry=None):
    """(mode | None, reason): see project()."""
    result = project(decisions, triggers, registry)
    return result["mode"], result["reason"]


def advisory_mismatches(decisions, triggers, registry=None):
    """[(id, token, implied_mode)] of considered advisories whose implied mode
    differs from the voted mode. Empty when the projection is None (the
    single disagree record already names every considered id) or under the
    fallback."""
    result = project(decisions, triggers, registry)
    if result["mode"] is None or result["fallback"]:
        return []
    return [(d, t, m) for (d, t, m, adv) in result["considered"]
            if adv and m is not None and m != result["mode"]]


def karpenter_replacement(decisions, triggers, registry=None):
    """(replacement | None, reason): what replaces Karpenter's node
    provisioning on the target — "nap", "computeclass" or "none" — or None
    when the cluster mode is unresolved. Derived from the projection and the
    recorded `karpenter` choice, never recorded itself; the planner keys the
    compute-class family on this value, so no planner line names a token.

    The `karpenter` choice counts only when it was considered by the
    projection (its trigger fired, its choice is not the default, or the
    projection ran without triggers): a defaulted `GKE_STANDARD_NAP` on an
    estate without Karpenter replaces nothing.
    """
    reg = _registry(registry)
    result = project(decisions, triggers, reg)
    if result["mode"] is None:
        return None, result["reason"]
    if result["mode"] == "autopilot":
        return "none", "Autopilot owns node provisioning"
    if result["fallback_reason"] == "mute":
        # Every recorded voting choice is a default on an unfired trigger:
        # the estate has no Karpenter, so nothing is replaced (v9 row 12).
        if _recorded(flat_choices(decisions), "karpenter", reg) is None:
            return "none", "no Karpenter replacement was decided"
        return "none", "the karpenter decision is a default on an unfired trigger: nothing to replace"
    considered = {did: token for (did, token, _m, _adv) in result["considered"]}
    token = considered.get("karpenter")
    if token == KARPENTER_NAP:
        return "nap", "NAP is the recorded Karpenter replacement"
    if token == KARPENTER_COMPUTECLASS:
        return "computeclass", ("node pools are auto-created per ComputeClass "
                                "(nodePoolAutoCreation.enabled: true, GKE 1.33.3+); "
                                "cluster-level NAP is not required")
    if token is None and _recorded(flat_choices(decisions), "karpenter", reg) is not None:
        return "none", "the karpenter decision is a default on an unfired trigger: nothing to replace"
    return "none", "no Karpenter replacement was decided"


def nap_enabled(decisions, triggers, registry=None):
    """(bool | None, reason): whether cluster-level node auto-provisioning is
    the Karpenter replacement. `karpenter_replacement() == "nap"`; None when
    the mode is unresolved. The GKE-version half stays an open question on
    the unit, never a constant here."""
    replacement, reason = karpenter_replacement(decisions, triggers, registry)
    if replacement is None:
        return None, reason
    return replacement == "nap", reason


# --- predicates and recommendations -----------------------------------------

def _resolve_path(inventory, path):
    """Dotted-path resolution with `[]` fan-out (coverage._resolve_path's rule,
    copied because this module must stay import-light)."""
    def resolve(node, segments):
        if not segments:
            return node
        head, rest = segments[0], segments[1:]
        if head.endswith("[]"):
            value = node.get(head[:-2]) if isinstance(node, dict) else None
            if not isinstance(value, (list, tuple)):
                return None
            return [resolve(item, rest) for item in value]
        if not isinstance(node, dict) or head not in node:
            return None
        return resolve(node[head], rest)
    return resolve(inventory or {}, str(path).split("."))


def _eval_atom(atom, inventory):
    """(fired: bool, evidence: [str]) or None when the typed path is absent."""
    value = _resolve_path(inventory, atom["path"])
    if value is None:
        return None
    if atom["op"] == "count":
        if not isinstance(value, list):
            return None
        fired = len(value) > atom["n"]
        return fired, [f"count({atom['path']}) = {len(value)}"] if fired else []
    if not isinstance(value, list):
        return None
    hits = []
    for item in value:
        if not isinstance(item, dict):
            continue
        field = item.get(atom["field"])
        name = item.get("name") or "?"
        if atom["op"] == "len":
            if isinstance(field, (list, tuple)) and len(field) > atom["n"]:
                hits.append(f"{name}.{atom['field']} has {len(field)} entries")
        elif atom["op"] == "contains":
            if (isinstance(field, (list, tuple)) and atom["literal"] in [str(v) for v in field]) \
                    or (isinstance(field, str) and field == atom["literal"]):
                hits.append(f"{name}.{atom['field']} contains {atom['literal']}")
    return bool(hits), hits


def evaluate_predicate(predicate, inventory):
    """(fired | None, evidence). None when every atom's typed path is absent."""
    any_present, fired, evidence = False, False, []
    for atom in predicate["atoms"]:
        result = _eval_atom(atom, inventory)
        if result is None:
            continue
        any_present = True
        if result[0]:
            fired = True
            evidence.extend(result[1])
    if not any_present:
        return None, []
    return fired, evidence


def recommend(decision_id, inventory, registry=None):
    """(token | None, reason): the first choice whose predicate fires on the
    inventory, in table order. None when no choice carries a predicate, the
    typed field is absent (absent is not "homogeneous"), or nothing fires.
    The recommendation is computed; the answer stays the human's."""
    reg = _registry(registry)
    decision = reg["decisions"].get(decision_id)
    if decision is None:
        return None, f"unknown decision {decision_id!r}"
    saw_predicate, any_present = False, False
    for choice in decision["choices"]:
        if choice["predicate"] is None:
            continue
        saw_predicate = True
        fired, evidence = evaluate_predicate(choice["predicate"], inventory)
        if fired is not None:
            any_present = True
        if fired:
            return choice["token"], ("predicate fired: " + choice["predicate"]["text"]
                                     + " (" + "; ".join(evidence) + ")")
    if not saw_predicate:
        return None, "no choice of this decision carries a predicate"
    if not any_present:
        return None, "the typed field the predicates read is absent from the inventory"
    return None, "no predicate fired on the recorded facts"
