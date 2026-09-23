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

"""Pure core of the developer join/claim flow (no GCS imports).

join_ledger's developer path is thin glue in main.py over these functions —
the E2 pattern (pure core + glue), so slug rules, version comparison, the
upgrade mapping and the claim decision table are unit-testable without a
bucket. See DESIGN §6.3 for the graph ladder this implements.
"""

import copy
import re
from datetime import datetime, timezone

# Component ids become GCS path segments now (workloads/<component>/) and PR
# branch tokens at the validate step, so the rule is an RFC-1123 label narrowed to start with
# a letter: lowercase letters/digits/hyphens, ends with a letter or digit,
# length 2-63. No dots, no slashes.
COMPONENT_SLUG_RE = re.compile(r"^[a-z][a-z0-9-]{0,61}[a-z0-9]$")

SLUG_RULE = (
    "component ids are lowercase RFC-1123 labels starting with a letter: "
    "letters, digits and hyphens only, ending with a letter or digit, "
    "2-63 characters (e.g. 'orders-component')"
)

# Explicit old-state -> new-state mapping per bundled version. Every future
# version bump MUST add its entry, even an empty one (v0.3 adds
# {"STATE_WKLD_AWAIT_PIPELINE": "STATE_WKLD_TRANSLATE"} per the spec ladder).
# States not named map to themselves. v0.2: a component parked on v0.1 moves
# into the new planning pipeline; every other state maps to itself.
# The entries COMPOSE: an upgrade applies each mapping in (copy, bundled] in
# ascending order, so an entry describes one hop and never has to restate the
# hops before it. An omitted intermediate version is therefore a silent
# no-op hop, which is why declaring every version is mandatory.
STATE_MAPPINGS = {
    "0.1": {},
    "0.2": {"STATE_WKLD_AWAIT_PIPELINE": "STATE_WKLD_PLAN"},
    # v0.3 (the execution slice): a component parked awaiting the pipeline
    # enters it at the translate step; every other state maps to itself.
    # Composition note: a v0.1 copy at AWAIT_PIPELINE goes through the v0.2
    # hop to STATE_WKLD_PLAN first (it has no approved plan yet), so this
    # entry only moves components that PARKED on v0.2 with a plan approved.
    "0.3": {"STATE_WKLD_AWAIT_PIPELINE": "STATE_WKLD_TRANSLATE"},
    # v0.4 (routing goes live): a GUARDED entry — a dict value maps only
    # when its `when` guard fact is True. A component that reached DONE with
    # parked units whose attach point HAS SINCE PUBLISHED re-enters at
    # translate (its units unpark there); DONE without such units, and every
    # other state, maps to itself. The guard is evaluated by the join glue
    # from the PERSISTED plan.json plus exports.json; an unreadable input
    # evaluates to None -> self plus a visible warning (never crash, never
    # re-enter blindly).
    "0.4": {"STATE_WKLD_DONE": {"to": "STATE_WKLD_TRANSLATE",
                                "when": "has_unparkable_units"}},
}

# The guard vocabulary a mapping entry may name. The join glue supplies the
# facts (guard token -> True/False, or None when unknowable); the mapping
# applier only consumes them, so this module stays pure. VALIDATED at map
# time: a token the glue does not supply is a programming bug in the graph,
# and degrading it to the "could not be evaluated" warning sent debuggers
# looking for a corrupt plan.json instead.
GUARD_TOKENS = ("has_unparkable_units",)


def has_unparkable_units(plan, exports) -> bool | None:
    """The `has_unparkable_units` guard fact, or None when unknowable.

    True only when the component has a parked unit AND the exports document
    carries the attach point that unit is parked for. Keying on the parked
    unit ALONE spends the re-entry on components whose Gateway is still
    unpublished — the expected state while the platform 2d unit is unbuilt —
    where translate unparks nothing, runs no worker, and still walks the tail
    again to open a duplicate no-op PR.

    plan absent/unreadable -> None (the caller passes what it read). exports
    absent is NOT unknowable: no exports means no attach point, so the
    honest answer is False.
    """
    if not isinstance(plan, dict) or not isinstance(plan.get("units"), list):
        return None
    parked = any(isinstance(u, dict) and u.get("status") == "parked"
                 for u in plan["units"])
    return bool(parked and gateway_published(exports))


def gateway_published(exports) -> bool:
    """True when exports.gateway carries a COMPLETE {name, namespace} attach
    point. Mirrors workload_plan_2.planner.gateway_attach_gaps — the dag
    server must not import the phase packages, so the rule is restated (a
    partial gateway cannot go live: parentRefs is copied verbatim)."""
    if not isinstance(exports, dict):
        return False
    gateway = exports.get("gateway")
    if not isinstance(gateway, dict):
        return False
    return all(isinstance(gateway.get(f), str) and gateway[f].strip()
               for f in ("name", "namespace"))


def needs_guard_facts(bundled_dag: dict, copy_dag: dict, state_dict: dict) -> bool:
    """True when this join would consult a guarded entry for this component's
    current state — the join glue reads plan.json/exports.json only then
    (every other join stays a zero-extra-read path)."""
    low = parse_version(copy_dag.get("version"))
    high = parse_version(bundled_dag.get("version"))
    state = state_dict.get("current_state")
    if high <= low:
        # Standing guards (see apply_standing_guards): the bundled version's
        # guarded entries are re-evaluated even when no version hop is due.
        return isinstance(STATE_MAPPINGS.get(str(bundled_dag.get("version")),
                                             {}).get(state), dict)
    for version in sorted((v for v in STATE_MAPPINGS
                           if low < parse_version(v) <= high),
                          key=parse_version):
        entry = STATE_MAPPINGS[version].get(state)
        if isinstance(entry, dict):
            return True
        if isinstance(entry, str):
            state = entry
    return False


def apply_standing_guards(bundled_dag: dict, state_dict: dict,
                          guard_facts: dict) -> tuple:
    """(new_state_dict, moved, notes): re-evaluates the BUNDLED version's
    guarded entries when no version hop is due.

    A guarded entry is not a one-time migration hop, it is a standing
    re-entry rule. Piggybacking it on the version bump made it fire at most
    ONCE per component: `_claim_component` rewrites the ledger dag.json copy
    to the bundled version in the same join, so a component whose guard was
    False at upgrade time (v0.4 DONE + parked routing while the platform
    Gateway was still unpublished — the expected ordering) could never
    re-enter afterwards, and its parked unit was stranded forever. Standing
    evaluation costs one guard read on the joins that could act on it and
    terminates naturally: the re-entry clears the parked unit, so the next
    join's guard is False.
    """
    state = state_dict.get("current_state")
    entry = STATE_MAPPINGS.get(str(bundled_dag.get("version")), {}).get(state)
    if not isinstance(entry, dict):
        return state_dict, False, []
    notes = []
    new_state = _map_entry(entry, state, guard_facts, notes)
    if new_state == state:
        return state_dict, False, notes
    _require_mapped_state(bundled_dag, state, new_state,
                          str(bundled_dag.get("version")))
    moved = copy.deepcopy(state_dict)
    moved["current_state"] = new_state
    moved.setdefault("history", []).append(
        f"Re-entry: guarded mapping {state} -> {new_state} fired on "
        f"'{entry.get('when')}' at join (graph version unchanged)")
    notes.append(f"re-entered {state} -> {new_state}: the "
                 f"'{entry.get('when')}' guard holds")
    return moved, True, notes

# Non-terminal parking states: claimable without a takeover (the refusal
# clause of decision 11 covers only non-initial, non-parked states).
# STATE_WKLD_AWAIT_PIPELINE stays listed for v0.1/v0.2 ledger copies, whose
# claim decisions run before the upgrade mapping is applied.
PARKING_STATES = {"STATE_WKLD_AWAIT_PIPELINE"}


def validate_component_id(component) -> str | None:
    """None when the id is a valid slug, else an error string naming the rule."""
    if not isinstance(component, str) or not COMPONENT_SLUG_RE.match(component):
        return f"invalid component id {component!r}: {SLUG_RULE}"
    return None


def parse_version(text) -> tuple:
    """"0.1" -> (0, 1). Unparseable -> (0, 0): a hand-edited ledger copy must
    degrade to "older than everything", never crash the join."""
    parsed = _try_parse_version(text)
    return parsed if parsed is not None else (0, 0)


def _try_parse_version(text):
    try:
        parts = str(text).strip().split(".")
        parsed = tuple(int(p) for p in parts)
        return parsed if parsed else None
    except (ValueError, AttributeError):
        return None


def _map_entry(entry, state, guard_facts, notes) -> str:
    """One hop's target for `state`. A plain string maps unconditionally;
    a {"to", "when"} dict maps only when the named guard fact is True.
    Guard False -> self; guard None (unknowable: plan.json absent or
    unreadable) -> self plus a visible warning, never a blind re-entry."""
    if not isinstance(entry, dict):
        return entry
    token = entry.get("when")
    if token not in GUARD_TOKENS:
        # Same class as an unmapped target state one function down, and the
        # same treatment: a token the join glue does not supply (a typo, or a
        # STATE_MAPPINGS entry added without its glue change) would otherwise
        # take the fact-is-None branch and degrade PERMANENTLY and silently
        # into "plan.json absent or unreadable" — a debugging dead end.
        raise ValueError(
            f"mapping entry {state!r} -> {entry.get('to')!r} names guard "
            f"{token!r}, which is not in GUARD_TOKENS {list(GUARD_TOKENS)}; "
            "every guarded entry must name a fact the join glue supplies")
    fact = (guard_facts or {}).get(token)
    if fact is True:
        return entry.get("to")
    if fact is None:
        notes.append(
            f"guarded mapping {state} -> {entry.get('to')} NOT applied: the "
            f"'{entry.get('when')}' guard could not be evaluated (plan.json "
            "absent or unreadable); the state was kept as-is")
    return state


def _apply_mappings(state, copy_raw, bundled_raw, guard_facts=None) -> tuple:
    """Walks EVERY declared version in (copy, bundled] in order, applying
    each mapping to the running state. Returns (state, versions, notes).

    Transitive by construction: a component parked since v0.1 rejoining a
    v0.3 graph is upgraded through the v0.2 mapping and then the v0.3 one.
    Reading only STATE_MAPPINGS[bundled] would silently skip the v0.2 hop
    and leave the component on a state the bundled graph no longer has.
    """
    low, high = parse_version(copy_raw), parse_version(bundled_raw)
    steps = sorted((v for v in STATE_MAPPINGS if low < parse_version(v) <= high),
                   key=parse_version)
    notes = []
    for version in steps:
        entry = STATE_MAPPINGS[version].get(state)
        if entry is not None:
            state = _map_entry(entry, state, guard_facts, notes)
    return state, steps, notes


def _require_mapped_state(bundled_dag: dict, old_state, new_state,
                          mapping_key: str) -> None:
    """A mapped-to state missing from the bundled graph is a programming bug
    in the mapping, not ledger data to tolerate."""
    if new_state not in (bundled_dag.get("states") or {}):
        raise ValueError(
            f"upgrade mapping for version {mapping_key} sends state "
            f"{old_state!r} to {new_state!r}, which is not in the bundled graph"
        )


def upgrade_component_state(bundled_dag: dict, copy_dag: dict, state_dict: dict,
                            guard_facts: dict = None) -> tuple:
    """Applies the version-bump state mapping. Pure.

    Returns (new_state_dict, upgraded, notes). When the bundled graph version
    is <= the ledger copy's, the state dict is returned unchanged. Otherwise
    current_state is mapped through EVERY declared mapping between the copy's
    version and the bundled one, in ascending order (absent key -> maps to
    itself), so a copy several versions behind is upgraded hop by hop rather
    than through the last mapping alone. A mapped-to state missing from the
    bundled graph is a hard error — that is a programming bug in the mapping,
    not ledger data to tolerate. A history line names both versions and both
    states. guard_facts feeds the guarded entries (see _map_entry); the
    caller supplies {"has_parked_units": bool | None} read from the
    persisted plan.json.
    """
    notes = []
    bundled_raw = bundled_dag.get("version")
    copy_raw = copy_dag.get("version")
    if _try_parse_version(copy_raw) is None:
        notes.append(f"ledger dag.json version {copy_raw!r} is unparseable; treated as 0.0")
    if parse_version(bundled_raw) <= parse_version(copy_raw):
        return state_dict, False, notes

    mapping_key = str(bundled_raw)
    if mapping_key not in STATE_MAPPINGS:
        raise ValueError(
            f"developer DAG version {mapping_key!r} declares no STATE_MAPPINGS "
            "entry; every version bump must declare its state mapping"
        )
    old_state = state_dict.get("current_state")
    new_state, steps, guard_notes = _apply_mappings(
        old_state, copy_raw, bundled_raw, guard_facts)
    notes.extend(guard_notes)
    _require_mapped_state(bundled_dag, old_state, new_state, mapping_key)
    if len(steps) > 1:
        notes.append("applied " + str(len(steps)) + " version mappings in "
                     "order: " + " -> ".join(f"v{v}" for v in steps))
    upgraded = copy.deepcopy(state_dict)
    upgraded["current_state"] = new_state
    upgraded.setdefault("history", []).append(
        f"Upgraded developer DAG copy v{copy_raw} -> v{bundled_raw}; "
        f"state {old_state} -> {new_state}"
    )
    return upgraded, True, notes


def classify_state(dag: dict, state_dict: dict) -> str:
    """"initial" | "done" | "parked" | "midflight" — the claim-decision input.

    "done" is read from the graph (type == "TERMINAL"), not from a hardcoded
    name, so the next terminal the ladder grows is classified without a code
    change. A shipped component is emphatically not mid-flight: refusing the
    next developer with "mid-flight and claimed by <other>" was factually
    wrong and forced a recorded "takeover" to pick up finished work.

    The start state counts as "initial" only while the component carries no
    work: the whole scope draft is iterated INSIDE the start state, so once
    variables.workload_scope exists, losing the claim silently would lose a
    drafting session — the component is mid-flight even though the graph has
    not moved. Last-writer-wins is for true join races over an untouched
    claim, not for established work.
    """
    state_name = state_dict.get("current_state")
    if state_name in PARKING_STATES:
        return "parked"
    if ((dag.get("states") or {}).get(state_name) or {}).get("type") \
            == "TERMINAL":
        return "done"
    variables = state_dict.get("variables") or {}
    if state_name == dag.get("start_state") and not variables.get("workload_scope"):
        return "initial"
    return "midflight"


def decide_claim(recorded_claimant, caller: str, state_class: str, reclaim: bool) -> tuple:
    """The decision-11 claim table. Returns (decision, message).

    decision: "allow" | "takeover" | "refuse". Last-writer-wins applies only
    to the raced initial claim; a parked or already-shipped (terminal)
    component is a handover, not a race; a mid-flight component is refused
    unless reclaim was passed deliberately.
    """
    if not recorded_claimant:
        return "allow", "new claim"
    if recorded_claimant == caller:
        return "allow", "resumed"
    if state_class == "initial":
        return "allow", (
            f"claim changed from {recorded_claimant} (raced initial claim, "
            "last-writer-wins)"
        )
    if state_class == "parked":
        return "allow", f"claim handed over from {recorded_claimant} (component parked)"
    if state_class == "done":
        return "allow", (
            f"claim handed over from {recorded_claimant} (component already "
            "shipped — its PR is open and the graph is terminal)")
    if reclaim:
        return "takeover", f"Claim takeover: {recorded_claimant} -> {caller}"
    return "refuse", (
        f"Component is mid-flight and claimed by {recorded_claimant}. "
        "Coordinate with them, or take it over deliberately with "
        "join_ledger(..., reclaim_component=True) — the takeover is recorded "
        "in the component history."
    )


def initial_state_dict(dag: dict, component: str, email: str, now_iso: str = None) -> dict:
    """The seeded workloads/<component>/state.json for a fresh claim."""
    now = now_iso or datetime.now(timezone.utc).isoformat()
    return {
        "current_state": dag["start_state"],
        "history": [f"Component {component} claimed by {email} at {now}"],
        "variables": {
            "component": component,
            "claim": {"claimant": email, "claimed_at": now},
        },
    }


def admin_binding_step(bucket_name: str, component: str, email: str) -> str:
    """The reported admin step that binds the component's managed folder.

    The server cannot verify the grant exists (it holds no setIamPolicy on
    the bucket for the caller), so the step is always reported at claim time
    and re-reported when a write comes back 403.
    """
    return (
        "Admin step required before ledger writes succeed for this component:\n"
        f"1. Ensure {email} is in the workspace registry's 'developers' role "
        "(the register_ledger_member flow during blocker triage, or an admin "
        "edit of workspace_registry.yaml followed by a provision_ledger_iam "
        "re-run).\n"
        "2. Create the component's managed folder and bind the claimant:\n"
        f"   gcloud storage managed-folders create gs://{bucket_name}/workloads/{component}/\n"
        "   gcloud storage managed-folders add-iam-policy-binding \\\n"
        f"       gs://{bucket_name}/workloads/{component}/ \\\n"
        f"       --member=user:{email} --role=roles/storage.objectAdmin"
    )
