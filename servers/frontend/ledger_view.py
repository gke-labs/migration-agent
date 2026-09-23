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

"""Pure view-model logic for the review frontend.

Everything here is deterministic and side-effect free: the DAG JSON, the
onboarding state, and the workspace registry come in as plain dicts; ordered
node/edge/artifact structures come out. HTTP glue lives in server.py, storage
access behind the Ledger backends. Keeping this pure is what makes the
frontend testable without GCS or a browser.
"""

import re

# States whose "phase" field is missing in older DAG documents get one
# inferred by name so the UI can always draw phase bands.
_PHASE_FALLBACK_PATTERNS = (
    (re.compile(r"^STATE_(CONFIGURE|CHECK|ELICIT_SSM|CREATE_SSM|VERIFY)"), "setup"),
    (re.compile(r"^STATE_DISCOVERY"), "discovery"),
    (re.compile(r"^STATE_(ASSESSMENT|BLOCKER_RESOLUTION|CONFIRM_NEW_MEMBER|REGISTER_MEMBER)"), "assessment"),
    # The landing zone phase spans the whole target build: the design/plan states
    # and the translation states (generate → validate → ship) are one band.
    (re.compile(r"^STATE_(LZ|TRANSLATION)"), "landingzone"),
    (re.compile(r"^STATE_DEPLOYMENT"), "deployment"),
)

# HITL and server-mutation transitions log "Transitioned A -> B" with no
# " via <tool>" suffix, so the tool part must be optional here.
_HISTORY_TRANSITION = re.compile(r"Transitioned (\S+) -> (\S+)")


def _phase_of(name: str, state_def: dict) -> str:
    if state_def.get("phase"):
        return state_def["phase"]
    for pattern, phase in _PHASE_FALLBACK_PATTERNS:
        if pattern.match(name):
            return phase
    return "other"


def _display_name(name: str) -> str:
    words = name.removeprefix("STATE_").split("_")
    keep_upper = {"SSM", "TF", "PR", "HITL", "LZ"}
    return " ".join(w if w in keep_upper else w.capitalize() for w in words)


def build_dag_view(dag: dict) -> dict:
    """Orders states depth-first from start_state and classifies every edge.

    Returns {nodes, edges, phases}: nodes carry an index used for layout;
    edges carry back/self flags (an edge to an equal-or-earlier index is a
    back edge, rendered differently); phases are contiguous index spans.
    """
    states = dag.get("states", {})
    order = []
    seen = set()

    def visit(name):
        if name in seen or name not in states:
            return
        seen.add(name)
        order.append(name)
        for target in (states[name].get("transitions") or {}).values():
            visit(target)

    visit(dag.get("start_state", ""))
    for name in states:  # anything unreachable still gets drawn, at the end
        visit(name)

    # A short branch that only rejoins earlier territory (e.g. the render
    # decline path) is the last thing the depth-first walk reaches, which
    # would strand it — and a fragment of its phase band — after the graph's
    # tail. Pull any such node back next to the state that branches to it.
    for name in list(order):
        succs = [t for t in (states[name].get("transitions") or {}).values() if t != name]
        preds = [p for p in order
                 if p != name and name in (states[p].get("transitions") or {}).values()]
        if not succs or not preds:
            continue
        pos = order.index(name)
        if all(order.index(s) < pos for s in succs):
            anchor = max(order.index(p) for p in preds)
            if anchor < pos - 1:
                order.remove(name)
                order.insert(anchor + 1, name)

    index = {name: i for i, name in enumerate(order)}
    nodes = []
    edges = []
    for name in order:
        state_def = states[name]
        nodes.append({
            "name": name,
            "display_name": _display_name(name),
            "index": index[name],
            "type": state_def.get("type", "UNKNOWN"),
            "hitl": bool(state_def.get("hitl")),
            "phase": _phase_of(name, state_def),
            "step": state_def.get("step"),
            "instructions": state_def.get("instructions"),
            "expected_tool_call": state_def.get("expected_tool_call"),
            "action": state_def.get("action"),
            "prompt_template": state_def.get("prompt_template"),
            "terminal_status": state_def.get("status") if state_def.get("type") == "TERMINAL" else None,
        })
        for trigger, target in (state_def.get("transitions") or {}).items():
            if target not in index:
                continue
            edges.append({
                "from": name,
                "to": target,
                "trigger": trigger,
                "self": target == name,
                "back": target != name and index[target] <= index[name],
            })

    phases = []
    for node in nodes:
        if phases and phases[-1]["phase"] == node["phase"]:
            phases[-1]["end"] = node["index"]
        else:
            phases.append({"phase": node["phase"], "start": node["index"], "end": node["index"]})

    return {"nodes": nodes, "edges": edges, "phases": phases}


def parse_visited(history: list) -> list:
    """State names touched so far, in first-visit order, from ledger history lines."""
    visited = []
    for line in history or []:
        match = _HISTORY_TRANSITION.search(str(line))
        if match:
            for name in (match.group(1), match.group(2)):
                if name not in visited:
                    visited.append(name)
    return visited


def registry_members(registry: dict) -> list:
    """Flattened, sorted, unique member emails across every registry role."""
    emails = set()
    for _role, lst in (registry.get("roles") or {}).items():
        for email in (lst or []):
            if email:
                emails.add(email)
    return sorted(emails)


def resolve_roles(config: dict, registry: dict, user_email: str) -> dict:
    """Resolves the acting role (local session) and registered role (ledger registry)."""
    acting = config.get("resolved_role") or "unknown"
    if acting == "platform_engineers":
        acting = "platform"

    registered = None
    for role_name, emails in (registry.get("roles") or {}).items():
        if user_email and user_email in (emails or []):
            registered = "platform" if role_name == "platform_engineers" else role_name
            break

    return {
        "user_email": user_email,
        "acting_role": acting,
        "registered_role": registered,
        "workspace_name": registry.get("workspace_name") or config.get("workspace_name"),
        "gcp_project": registry.get("gcp_project") or config.get("gcp_project"),
    }


# ---------------------------------------------------------------------------
# Artifact registry: which review artifacts belong to which DAG states.
#
# source is exactly one of:
#   {"blob": <exact ledger path>}       content fetched via GET /api/blob
#   {"prefix": <ledger path prefix>}    items listed, each fetched via /api/blob
#   {"variable": <state variable key>}  content inlined from state.json variables
#   {"variables": [keys...]}            composite dict inlined from state.json
#   {"endpoint": <api path>}            content computed server-side per fetch
#
# render is a client-side hint: json | markdown | unit-bundle | link.
# Optional "caveats" maps a state name to a warning shown with the artifact in
# that state (e.g. a blob that may be from a previous run while a re-run is in
# flight).
# ---------------------------------------------------------------------------

ARTIFACT_REGISTRY = [
    {
        "id": "repo-config", "label": "Repository configuration", "render": "json",
        "source": {"variables": ["source_repo_url", "source_branch", "source_path",
                                 "target_repo_url", "target_write_permission_verified"]},
        "states": ["STATE_CONFIGURE_REPOSITORIES", "STATE_CHECK_SSM_REGISTRATION",
                   "STATE_ELICIT_SSM_CREATION_APPROVAL", "STATE_CREATE_SSM_REPOSITORY",
                   "STATE_VERIFY_REPOSITORIES", "STATE_DISCOVERY_LIVE", "STATE_DISCOVERY"],
    },
    {
        # The live AWS estate scan, from STATE_DISCOVERY_LIVE onward. A prefix:
        # the Live IR (live_discovery.json — every object projected to its key
        # names and structural fields) plus one CSV per table. Absent until
        # the scan runs, or when it was skipped — rendered as no artifact.
        "id": "live-ir",
        "label": "Live AWS estate (clusters, workloads, cloud plane)",
        "render": "json",
        "source": {"prefix": "platform/discovery/live/"},
        "states": ["STATE_DISCOVERY_LIVE", "STATE_DISCOVERY",
                   "STATE_DISCOVERY_SCOPING", "STATE_DISCOVERY_DATA_SCAN",
                   "STATE_DISCOVERY_DATA_REVIEW", "STATE_DISCOVERY_RUNNING",
                   "STATE_ASSESSMENT"],
        "caveats": {
            "STATE_DISCOVERY_LIVE":
                "The operational reality scanned from AWS. Produced by the "
                "live scan this state runs; absent until it has run or been "
                "skipped.",
            **{state: "The operational reality scanned from AWS. Absent when "
                      "the live scan was skipped (no live AWS access): "
                      "variables.live_discovery in state.json records the "
                      "reason."
               for state in ("STATE_DISCOVERY", "STATE_DISCOVERY_SCOPING",
                             "STATE_DISCOVERY_DATA_SCAN",
                             "STATE_DISCOVERY_DATA_REVIEW",
                             "STATE_DISCOVERY_RUNNING", "STATE_ASSESSMENT")},
        },
    },
    {
        "id": "manifest", "label": "Configuration file manifest", "render": "json",
        "source": {"blob": "platform/discovery/manifest.json"},
        "states": ["STATE_DISCOVERY", "STATE_DISCOVERY_SCOPING"],
    },
    {
        "id": "scope", "label": "Discovery scope (client include/exclude)", "render": "json",
        "source": {"variable": "discovery_scope"},
        "states": ["STATE_DISCOVERY_SCOPING", "STATE_DISCOVERY_DATA_SCAN",
                   "STATE_DISCOVERY_DATA_REVIEW", "STATE_DISCOVERY_RUNNING",
                   "STATE_ASSESSMENT"],
    },
    {
        "id": "extraction-progress", "label": "Extraction progress (resources found so far)",
        "render": "extraction-progress",
        "source": {"endpoint": "/api/progress/extraction"},
        "states": ["STATE_DISCOVERY_RUNNING"],
        "caveats": {"STATE_DISCOVERY_RUNNING":
                    "Until run_discovery_extraction starts, this may still show the "
                    "previous run (e.g. before a scope amendment)."},
    },
    {
        "id": "extraction-fragments", "label": "Extraction fragments (per chunk)", "render": "json",
        "source": {"prefix": "platform/discovery/fragments/"},
        "states": ["STATE_DISCOVERY_RUNNING"],
    },
    {
        "id": "inventory", "label": "Discovery inventory (all discovered resources)", "render": "inventory",
        "source": {"blob": "platform/discovery/inventory.json"},
        # The deployment states show it again for the per-image replication
        # outcomes replicate_images writes back into images[].
        "states": ["STATE_DISCOVERY_SCOPING", "STATE_DISCOVERY_DATA_SCAN",
                   "STATE_DISCOVERY_DATA_REVIEW", "STATE_DISCOVERY_RUNNING",
                   "STATE_ASSESSMENT",
                   "STATE_DEPLOYMENT_INIT",
                   "STATE_DEPLOYMENT_DATA_MIGRATION",
                   "STATE_DEPLOYMENT_COMPLETED"],
        "caveats": {"STATE_DISCOVERY_DATA_SCAN":
                    "Only the deterministic scan sections are filled in yet — "
                    "the LLM extraction has not run.",
                    "STATE_DISCOVERY_DATA_REVIEW":
                    "data_dependencies is what is under review here; the rest "
                    "of the inventory arrives with the LLM extraction, which "
                    "runs once the mapping is approved.",
                    "STATE_DISCOVERY_SCOPING":
                    "Before extraction runs, this holds only the image scan "
                    "(images and render targets).",
                    "STATE_DISCOVERY_RUNNING":
                    "Until run_discovery_extraction completes, this may still show a previous run "
                    "(e.g. before a scope amendment or rediscovery)."},
    },
    {
        # The reviewer's own decisions, in the state where they are being made.
        # Absent until one is recorded, which the view renders as no artifact.
        "id": "data-corrections",
        "label": "Data mapping corrections (durable across re-scans)", "render": "json",
        "source": {"blob": "platform/discovery/data_consumer_overrides.json"},
        # The deployment step is one of those states now: `keep-in-aws`, its
        # documented exit from a service that turns out not to be movable,
        # writes this object.
        "states": ["STATE_DISCOVERY_DATA_SCAN", "STATE_DISCOVERY_DATA_REVIEW",
                   "STATE_DISCOVERY_RUNNING", "STATE_ASSESSMENT",
                   "STATE_DEPLOYMENT_DATA_MIGRATION"],
    },
    {
        "id": "readiness-report", "label": "Readiness report", "render": "markdown",
        "source": {"blob": "platform/discovery/readiness-report.md"},
        "states": ["STATE_DISCOVERY_RUNNING", "STATE_ASSESSMENT", "STATE_BLOCKER_RESOLUTION"],
        "caveats": {"STATE_DISCOVERY_RUNNING":
                    "Until run_discovery_extraction completes, this may still show a previous run."},
    },
    {
        "id": "pull-request", "label": "Pull request (target repository)", "render": "link",
        "source": {"variable": "pull_request_url"},
        "states": ["STATE_TRANSLATION_SUBMIT_PR", "STATE_DEPLOYMENT_INIT",
                   "STATE_DEPLOYMENT_DATA_MIGRATION",
                   "STATE_DEPLOYMENT_COMPLETED"],
    },
    {
        # Written by replicate_images in self-service mode; an unregistered
        # render kind lands in app.js's raw-<pre> fallback, which is right
        # for a shell script.
        "id": "replication-runbook", "label": "Image replication runbook (self-service)",
        "render": "text",
        "source": {"blob": "platform/deployment/replication-runbook.sh"},
        "states": ["STATE_DEPLOYMENT_INIT",
                   "STATE_DEPLOYMENT_DATA_MIGRATION",
                   "STATE_DEPLOYMENT_COMPLETED"],
        "caveats": {"STATE_DEPLOYMENT_INIT":
                    "Only present once self-service replication has been chosen; "
                    "may show a previous attempt until replicate_images re-runs."},
    },
    {
        # The worklist the data migration step exists to produce. Registered
        # for the same reason the replication runbook is: it is the artifact of
        # the step a reviewer is standing in, and a step whose subject cannot
        # be opened from the UI is a step the UI does not really show.
        "id": "data-migration-runbook",
        "label": "Data migration runbook (what still has to move)",
        # Markdown, like `readiness-report` and unlike the replication runbook
        # this entry was modelled on — that one is a shell script. `runbook()`
        # emits headings, bullets, bold, inline code and a blockquote, and
        # `render: "text"` has no branch in renderBlobInto, so it fell through
        # to a monospace dump of the markup at the one state where this is the
        # PRIMARY artifact.
        "render": "markdown",
        "source": {"blob": "platform/deployment/data-migration-runbook.md"},
        "states": ["STATE_DEPLOYMENT_DATA_MIGRATION",
                   "STATE_DEPLOYMENT_COMPLETED"],
        "caveats": {"STATE_DEPLOYMENT_DATA_MIGRATION":
                    "Absent until the step's first tool call writes it; every "
                    "call that changes what it says rewrites it.",
                    "STATE_DEPLOYMENT_COMPLETED":
                    "Written as it stood when the step closed; nothing "
                    "rewrites it afterwards."},
    },
    {
        # The adapted procedures, one per data service. A prefix rather than a
        # blob: how many there are depends on the estate, and which of them
        # exist depends on which offers the operator took. Registered because
        # this is what somebody actually opens to run a move — the worklist
        # says what is owed, these say how.
        "id": "data-migration-procedures",
        "label": "Data migration procedures (adapted per service)",
        "render": "markdown",
        "source": {"prefix": "platform/deployment/runbooks/"},
        "states": ["STATE_DEPLOYMENT_DATA_MIGRATION",
                   "STATE_DEPLOYMENT_COMPLETED"],
        "caveats": {"STATE_DEPLOYMENT_DATA_MIGRATION":
                    "One per service the agent has adapted a runbook for; "
                    "empty until the first offer is accepted.",
                    "STATE_DEPLOYMENT_COMPLETED":
                    "Kept as written. A procedure here is a plan somebody "
                    "made, not a record that the move ran — the outcomes are "
                    "in the runbook and the outcome store."},
    },
    {
        # The outcomes themselves, which the runbook renders from. Kept apart
        # from the inventory on purpose (the data scan rebuilds that section on
        # every run), so this is the only place the record lives.
        "id": "data-migrations", "label": "Data migration outcomes",
        "render": "json",
        "source": {"blob": "platform/deployment/data_migrations.json"},
        "states": ["STATE_DEPLOYMENT_DATA_MIGRATION",
                   "STATE_DEPLOYMENT_COMPLETED"],
        # Absent until the FIRST report, which on a real migration is weeks
        # after the step is entered — and permanently absent for a workspace
        # that closed with every owed service excused, or an estate with no
        # `migrate` service at all. Without this the card renders a 404 for
        # most of the waiting period the step exists for.
        "caveats": {"STATE_DEPLOYMENT_DATA_MIGRATION":
                    "Absent until the first service is reported; nothing "
                    "writes it while the step is only waiting.",
                    "STATE_DEPLOYMENT_COMPLETED":
                    "Absent if nothing was ever reported — a step that closed "
                    "with every owed service excused never writes it."},
    },
    {
        # The state variable is authoritative: review-time edits (skips,
        # unskips, revision feedback) mutate only the variable — the
        # plan.json blob is rewritten just at plan time and run completion.
        "id": "translation-plan", "label": "Translation plan (units + statuses)", "render": "json",
        "source": {"variable": "translation_plan"},
        "states": ["STATE_LZ_TRANSLATION_PLAN", "STATE_LZ_TRANSLATION_PLAN_REVIEW", "STATE_TRANSLATION_RUNNING",
                   "STATE_TRANSLATION_REVIEW", "STATE_TRANSLATION_VALIDATE", "STATE_TRANSLATION_APPROVED",
                   "STATE_DEPLOYMENT_INIT",
                   "STATE_DEPLOYMENT_DATA_MIGRATION",
                   "STATE_DEPLOYMENT_COMPLETED"],
    },
    {
        "id": "translation-progress", "label": "Translation progress (units produced so far)",
        "render": "translation-progress",
        "source": {"endpoint": "/api/progress/translation"},
        "states": ["STATE_TRANSLATION_RUNNING"],
        "caveats": {"STATE_TRANSLATION_RUNNING":
                    "Until run_translation starts, this may still show the previous run "
                    "(e.g. before a revision). A revised unit reads as in-progress until this "
                    "run re-produces it."},
    },
    {
        "id": "translation-units", "label": "Translated units (generated code + tradeoffs)", "render": "unit-bundle",
        "source": {"prefix": "platform/translation/units/"},
        "states": ["STATE_TRANSLATION_RUNNING", "STATE_TRANSLATION_REVIEW", "STATE_TRANSLATION_VALIDATE",
                   "STATE_TRANSLATION_APPROVED", "STATE_DEPLOYMENT_INIT",
                   "STATE_DEPLOYMENT_DATA_MIGRATION",
                   "STATE_DEPLOYMENT_COMPLETED"],
        "caveats": {
            "STATE_TRANSLATION_RUNNING":
                "Units appear here as their workers finish. A unit sent back for revision keeps "
                "showing its earlier attempt until this run re-produces it (watch the progress "
                "tab for which units this run has completed).",
            "STATE_TRANSLATION_REVIEW":
                "Cross-check each unit's status in the translation plan — a blob for a revised or "
                "failed unit may still show an earlier attempt.",
        },
    },
    {
        # Written by run_generated_validation: per-directory terraform-validate
        # outcome (clean / auto-fixed / still failing) over the landing-zone
        # draft at the clone root and each translated unit directory, plus the
        # structural check over the units' K8s manifests.
        "id": "validation-report", "label": "Validation report (terraform + manifests)", "render": "validation-report",
        "source": {"blob": "platform/translation/validation-report.json"},
        # Includes REVIEW: a failed validation returns the graph there (on_failure)
        # with the report the reviewer needs to see the directories still failing.
        "states": ["STATE_TRANSLATION_VALIDATE", "STATE_TRANSLATION_REVIEW",
                   "STATE_TRANSLATION_APPROVED", "STATE_DEPLOYMENT_INIT",
                   "STATE_DEPLOYMENT_DATA_MIGRATION",
                   "STATE_DEPLOYMENT_COMPLETED"],
        "caveats": {
            "STATE_TRANSLATION_VALIDATE":
                "Until run_generated_validation completes, this may still show a previous run "
                "(e.g. before units were revised and re-approved).",
        },
    },
    {
        # The before/after the reviewer signs off before the PR opens: the
        # discovered AWS inputs on one side, the generated (and possibly
        # auto-fixed) GCP code on the other, with per-unit tradeoffs.
        "id": "comparisons", "label": "Before / after (AWS → GCP tradeoffs)", "render": "comparisons",
        "source": {"blob": "platform/translation/comparisons.json"},
        # run_generated_validation writes this at VALIDATE, then raises the ship
        # elicitation without persisting APPROVED first — so the ledger still
        # reads VALIDATE while the reviewer is signing off. It lands at REVIEW on
        # a validation failure or a ship decline, and COMPLETED once the PR opens.
        "states": ["STATE_TRANSLATION_VALIDATE", "STATE_TRANSLATION_REVIEW",
                   "STATE_TRANSLATION_APPROVED", "STATE_DEPLOYMENT_INIT",
                   "STATE_DEPLOYMENT_DATA_MIGRATION",
                   "STATE_DEPLOYMENT_COMPLETED"],
        "caveats": {
            "STATE_TRANSLATION_VALIDATE":
                "Until run_generated_validation completes, this may be absent or "
                "show a previous run (before units were revised and re-approved).",
        },
    },
]

# Variables larger than this are summarized rather than inlined into
# /api/state responses (the blob-backed artifact covers the full content).
INLINE_VARIABLE_LIMIT = 200_000


# The artifact shown first (default subtab) on each state's detail page —
# the thing the reviewer most likely came to see. States not listed keep
# registry order.
PRIMARY_ARTIFACT = {
    "STATE_DISCOVERY_LIVE": "live-ir",
    "STATE_DISCOVERY_DATA_SCAN": "inventory",
    "STATE_DISCOVERY_DATA_REVIEW": "inventory",
    "STATE_DISCOVERY_RUNNING": "extraction-progress",
    "STATE_ASSESSMENT": "readiness-report",
    "STATE_BLOCKER_RESOLUTION": "readiness-report",
    "STATE_TRANSLATION_RUNNING": "translation-progress",
    "STATE_TRANSLATION_REVIEW": "translation-units",
    "STATE_TRANSLATION_VALIDATE": "validation-report",
    "STATE_TRANSLATION_APPROVED": "comparisons",
    "STATE_DEPLOYMENT_INIT": "pull-request",
    # The worklist, not the whole inventory: what is owed is this step's
    # subject, and the inventory is one click away in the same view set.
    "STATE_DEPLOYMENT_DATA_MIGRATION": "data-migration-runbook",
    "STATE_DEPLOYMENT_COMPLETED": "comparisons",
}


def artifacts_for_state(state_name: str) -> list:
    matches = [a for a in ARTIFACT_REGISTRY if state_name in a["states"]]
    primary = PRIMARY_ARTIFACT.get(state_name)
    return sorted(matches, key=lambda a: 0 if a["id"] == primary else 1)


def allowed_blob_exact() -> set:
    return {a["source"]["blob"] for a in ARTIFACT_REGISTRY if "blob" in a["source"]}


def allowed_blob_prefixes() -> tuple:
    return tuple(a["source"]["prefix"] for a in ARTIFACT_REGISTRY if "prefix" in a["source"])


def is_blob_allowed(path: str) -> bool:
    """Only artifact blobs are served — never state.json, the registry, or the DAG."""
    if not path or ".." in path or path.startswith("/"):
        return False
    if path in allowed_blob_exact():
        return True
    return any(path.startswith(prefix) and len(path) > len(prefix)
               for prefix in allowed_blob_prefixes())
