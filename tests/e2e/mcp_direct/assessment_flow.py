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

"""E2E: the assessment review, its approval elicitation, and the blocker guardrail.

Covers demo steps 2 and 3. Six claims, none of which a unit test can make on
its own, because each depends on the elicitation actually crossing the MCP
transport and on the ledger being a real bucket:

  1. submit_assessment raises a *real* elicitation — the pause and explain —
     rather than the model being asked to remember to pause. It is the only
     approval between extraction and the landing zone: one review covers what
     used to be a discovery review step plus an assessment gate, and the DAG
     only moves on once the client accepts it.
  2. STATE_ASSESSMENT serves migration-assessment.md. The document moved out of
     the discovery phase in v1.5, so this also checks that per-phase knowledge
     resolution finds it in its new home.
  3. Landing zone design stays locked while any blocker lacks an owner or a
     target close date, and unlocks on the assignment that completes the set.
  4. An owner who is not in the workspace registry triggers the team question,
     and answering it registers them — in the registry file *and* in the
     bucket's IAM, without which they would be registered and locked out.
  5. The step the gate unlocks can obtain what it is told to use: the first
     resolve_lz_decision allocates the branch and the target clone path, and
     returns them. The agent may not read the ledger or the server's files,
     so a value that only lands in the ledger has not been delivered.
  6. A category outside the Step 4 table is rejected before the elicitation
     is raised, so a mislabelled blocker list can never reach the user for
     approval — while the seven categories the flow submits (the two
     originals plus the five 2026-08 additions) pass the same live parse.

Claim 5 pins a contract that was once broken: resolve_lz_decision used to
write the three workspace variables to the ledger and return only a one-line
confirmation, while step 2 of that step's instructions.md tells the agent to
"read them back" — observed twice in end-to-end runs, where the agent went
looking for the values in the server's own files instead. The response now
carries the coordinates; this claim is what keeps a refactor from quietly
dropping them again.

Claim 4 grants a real IAM binding to a real principal, so it only runs when
E2E_NEW_OWNER names an address the project can actually bind. Without it the
test still covers the other claims and says plainly what it skipped.

Env:
  E2E_MCP_SERVER          path to the mcp-server launcher
  E2E_WORKSPACE           workspace_name
  E2E_GCP_PROJECT         gcp_project
  E2E_LEDGER_BUCKET       ledger bucket URI (gs://...)
  E2E_ADMINS              semicolon-separated admin emails
  E2E_PLATFORM_ENGINEERS  semicolon-separated platform engineer emails
  E2E_NEW_OWNER           optional; an unregistered but bindable email
"""

import asyncio
import datetime
import json
import os
import sys

import yaml
from google.cloud import storage
from mcp import ClientSession, StdioServerParameters, types
from mcp.client.stdio import stdio_client

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import session_cache  # noqa: E402  (shared snapshot/restore of ~/.ledger_config*)

INIT_TIMEOUT_S = 300
TOOL_TIMEOUT_S = 180

STATE_BLOB = "platform/onboarding/state.json"
REGISTRY_BLOB = "workspace_registry.yaml"

ASSESSMENT_MARKER = "--- Phase knowledge: migration-assessment ---"

# Verbatim from the Step 4 table of migration-assessment.md. The server parses
# that table at start-up and rejects any category not in it, so a drifting
# string here is a real failure and not a test-data nit.
CATEGORY_CNI = "Custom CNI (non-VPC-CNI) in production use"
CATEGORY_RWX = "RWX PVC not on EFS"
CATEGORY_HOSTPATH = "Local `hostPath` volumes in workloads"
CATEGORY_VPC_CNI = "VPC CNI-specific features in use (ENI per pod, security groups for pods)"
CATEGORY_XACCT = "On-prem-pinned dependency or cross-account RDS without a replica path"
CATEGORY_AWS_AUTH = "Cluster authentication via legacy `aws-auth` ConfigMap (CONFIG_MAP mode)"
CATEGORY_KMS = "Secrets-at-rest encryption parity (EKS KMS `encryption_config`)"

# One blocker per category family under test: the two originals plus the five
# rows added in 2026-08 for the conditions the scoring rubric already named.
# submit_assessment accepts all seven in one call or the elicitation never
# fires, so acceptance is asserted by the flow itself.
BLOCKERS = [
    {
        "id": "B-001",
        "title": "Cilium runs as the cluster CNI",
        "category": CATEGORY_CNI,
        "affected_workloads": ["kube-system/cilium"],
        "rationale": "GKE Dataplane V2 replaces Cilium; the CNI cannot be lifted as-is.",
        "resolution_path": "Adopt Dataplane V2 and port the NetworkPolicy set.",
    },
    {
        "id": "B-002",
        "title": "Shared media volume is RWX on EBS",
        "category": CATEGORY_RWX,
        "affected_workloads": ["media/render-farm"],
        "rationale": "ReadWriteMany is not served by the EBS CSI driver on GKE.",
        "resolution_path": "Move the volume to Filestore and repoint the PVC.",
    },
    {
        "id": "B-003",
        "title": "node-agent mounts the host /proc",
        "category": CATEGORY_HOSTPATH,
        "affected_workloads": ["acme-shop/node-agent"],
        "rationale": "The DaemonSet reads node metrics from a local hostPath mount.",
        "resolution_path": "Reproduce on GKE Standard where the path is allowed; Autopilot restricts it.",
    },
    {
        "id": "B-004",
        "title": "Orders pods use security groups for pods",
        "category": CATEGORY_VPC_CNI,
        "affected_workloads": ["acme-shop/orders"],
        "rationale": "Pod-level security groups have no feature-for-feature GKE equivalent.",
        "resolution_path": "Map the pod security groups to Dataplane V2 network policies and test.",
    },
    {
        "id": "B-005",
        "title": "Order archive is a cross-account RDS replica",
        "category": CATEGORY_XACCT,
        "affected_workloads": ["acme-shop/orders"],
        "rationale": "The primary lives in another account with no replica path to GCP.",
        "resolution_path": "Keep the database in place over hybrid connectivity during co-existence.",
    },
    {
        "id": "B-006",
        "title": "Cluster authenticates via the legacy aws-auth ConfigMap",
        "category": CATEGORY_AWS_AUTH,
        "affected_workloads": [],
        "rationale": "CONFIG_MAP mode maps IAM principals in-cluster; GKE uses IAM plus RBAC.",
        "resolution_path": "Translate the aws-auth mappings to GKE IAM and RBAC bindings.",
    },
    {
        "id": "B-007",
        "title": "Cluster secrets are envelope-encrypted with an AWS KMS key",
        "category": CATEGORY_KMS,
        "affected_workloads": [],
        "rationale": "GKE parity needs CMEK application-layer secrets encryption in the design.",
        "resolution_path": "Verify the landing zone design enables CMEK secrets encryption.",
    },
]


def future_date(days: int = 30) -> str:
    return (datetime.date.today() + datetime.timedelta(days=days)).isoformat()


def text_of(result) -> str:
    return result.content[0].text if result.content else ""


class Client:
    """One MCP session, recording every elicitation and scripting the answers.

    Answers are queued rather than derived from the prompt: the point is to
    assert on what the server *asked*, and a callback that pattern-matches the
    message would hide a prompt that came out wrong.
    """

    def __init__(self):
        self.elicitations = []
        self.answers = []

    def expect(self, content):
        self.answers.append(content)

    async def callback(self, context, params):
        self.elicitations.append(params)
        print(f"[assessment_flow] elicitation: {params.message!r}")
        if not self.answers:
            print("[assessment_flow] unexpected elicitation -> declining")
            return types.ElicitResult(action="decline")
        return types.ElicitResult(action="accept", content=self.answers.pop(0))


async def call(session, tool, args=None):
    result = await asyncio.wait_for(session.call_tool(tool, args or {}), TOOL_TIMEOUT_S)
    text = text_of(result)
    if result.isError:
        raise RuntimeError(f"{tool} errored: {text}")
    return text


def bucket_of(project: str, bucket_uri: str):
    name = bucket_uri[5:] if bucket_uri.startswith("gs://") else bucket_uri
    return storage.Client(project=project).bucket(name)


def park_ledger_on_review(bucket) -> None:
    """Puts the ledger on STATE_ASSESSMENT with a plausible inventory.

    Walking there for real would mean driving repository configuration and a
    full discovery scan first, none of which this test is about.
    """
    blob = bucket.blob(STATE_BLOB)
    state = json.loads(blob.download_as_text())
    state["current_state"] = "STATE_ASSESSMENT"
    state.setdefault("variables", {})
    state["variables"]["discovery_inventory"] = {
        "clusters": [{"name": "prod"}],
        "triggers": {"karpenter": True, "privileged_daemonsets": False,
                     "gpu_tpu": False, "vpc_peering": False},
    }
    blob.upload_from_string(json.dumps(state, indent=2), content_type="application/json")
    print("[assessment_flow] ledger parked on STATE_ASSESSMENT")


def registry_roles(bucket) -> dict:
    return (yaml.safe_load(bucket.blob(REGISTRY_BLOB).download_as_text()) or {}).get("roles", {})


def current_state(bucket) -> str:
    return json.loads(bucket.blob(STATE_BLOB).download_as_text())["current_state"]


def blockers_in_ledger(bucket) -> list:
    state = json.loads(bucket.blob(STATE_BLOB).download_as_text())
    return state.get("variables", {}).get("blockers") or []


def variables_in_ledger(bucket) -> dict:
    state = json.loads(bucket.blob(STATE_BLOB).download_as_text())
    return state.get("variables", {}) or {}


async def run() -> int:
    server_cmd = os.environ["E2E_MCP_SERVER"]
    workspace = os.environ["E2E_WORKSPACE"]
    project = os.environ["E2E_GCP_PROJECT"]
    bucket_uri = os.environ["E2E_LEDGER_BUCKET"]
    admins = [e for e in os.environ["E2E_ADMINS"].split(";") if e]
    platform_engineers = [e for e in os.environ["E2E_PLATFORM_ENGINEERS"].split(";") if e]
    new_owner = os.environ.get("E2E_NEW_OWNER", "").strip()

    owner_a = admins[0]
    failures = []
    # bootstrap_migration deletes both session-cache paths and join_ledger
    # rewrites the scoped one — snapshot both, put back the pre-run state.
    snap = session_cache.snapshot()
    client = Client()
    # Pass the invoking environment through (the MCP SDK spawns the server
    # with a sanitized minimal env by default), so auth fallbacks like
    # GKE_MIGRATION_USER_EMAIL reach the server — as the phase-harness does.
    env = dict(os.environ)
    # Headless run: no one watches the review UI, so skip it entirely.
    env.setdefault("GKE_AGENTIC_MIGRATION_FRONTEND", "0")
    params = StdioServerParameters(command=server_cmd, env=env)

    try:
        async with stdio_client(params) as (read, write):
            async with ClientSession(read, write,
                                     elicitation_callback=client.callback) as session:
                await asyncio.wait_for(session.initialize(), INIT_TIMEOUT_S)

                await call(session, "bootstrap_migration")
                client.expect({"approved": True})  # ledger creation
                await call(session, "initialize_ledger", {
                    "workspace_name": workspace,
                    "gcp_project": project,
                    "ledger_bucket": bucket_uri,
                    "roles": {
                        "admins": admins,
                        "platform_engineers": platform_engineers,
                        "developers": [],
                    },
                })
                await call(session, "join_ledger", {"ledger_uri": bucket_uri})

                bucket = bucket_of(project, bucket_uri)
                park_ledger_on_review(bucket)

                # --- Claim 2: the assessment guide is served here -------------
                stage = await call(session, "get_next_stage")
                if ASSESSMENT_MARKER not in stage:
                    failures.append("STATE_ASSESSMENT did not serve migration-assessment.md; the "
                                    "document did not follow the phase move")

                # --- Claim 6: a category outside the Step 4 table is rejected -
                # --- before any elicitation is raised -------------------------
                before = len(client.elicitations)
                rejected = await call(session, "submit_assessment", {
                    "blockers": [{**BLOCKERS[0], "id": "B-BOGUS",
                                  "category": "Vibes were off"}],
                })
                print(f"[assessment_flow] bogus category: {rejected}")
                if "not in the Step 4 blocker table" not in rejected:
                    failures.append("a category outside the Step 4 table was not rejected "
                                    f"with the taxonomy error: {rejected!r}")
                if len(client.elicitations) != before:
                    failures.append("a rejected blocker list still raised the approval "
                                    "elicitation; validation must come first")
                if current_state(bucket) != "STATE_ASSESSMENT":
                    failures.append("a rejected blocker list moved the DAG: now "
                                    f"{current_state(bucket)}")

                # --- Claims 1 + 3: one review, a real elicitation, and the ----
                # --- blockers routing to resolution ---------------------------
                before = len(client.elicitations)
                client.expect({"approved": True})
                submitted = await call(session, "submit_assessment", {"blockers": BLOCKERS})
                print(f"[assessment_flow] submit_assessment: {submitted}")
                gate_prompts = client.elicitations[before:]
                if not gate_prompts:
                    failures.append("submit_assessment raised no elicitation; the "
                                    "pause-and-explain approval is not being put to the user")
                else:
                    message = gate_prompts[0].message
                    if workspace not in message:
                        failures.append("the approval prompt did not interpolate the workspace "
                                        f"name, so prompt_template is still inert: {message!r}")
                    if "{" in message:
                        failures.append(f"the approval prompt has an unrendered placeholder: {message!r}")
                if "STATE_BLOCKER_RESOLUTION" not in submitted:
                    failures.append(f"{len(BLOCKERS)} blockers should route to blocker "
                                    f"resolution: {submitted!r}")

                for blocker in BLOCKERS[:-1]:
                    assigned = await call(session, "assign_blocker_owner", {
                        "blocker_id": blocker["id"],
                        "owner_email": owner_a,
                        "target_close_date": future_date(14),
                    })
                    print(f"[assessment_flow] assign {blocker['id']}: {assigned}")
                    if current_state(bucket) != "STATE_BLOCKER_RESOLUTION":
                        failures.append(f"blockers past {blocker['id']} still unowned, but the "
                                        f"DAG left blocker resolution: now {current_state(bucket)}")
                        break

                if not new_owner:
                    print("[assessment_flow] SKIP claim 4: E2E_NEW_OWNER unset, so the "
                          "unregistered-owner path would grant IAM to a made-up principal. "
                          "Closing the last blocker with a registered owner instead.")
                    second_owner, expect_team_question = owner_a, False
                else:
                    second_owner, expect_team_question = new_owner, True
                    if new_owner in registry_roles(bucket).get("developers", []):
                        failures.append(f"{new_owner} is already a developer; the test needs an "
                                        "owner the registry does not know")

                # --- Claim 4: unregistered owner -> team question -> register -
                last_id = BLOCKERS[-1]["id"]
                before = len(client.elicitations)
                if expect_team_question:
                    client.expect({"team": "application"})
                second = await call(session, "assign_blocker_owner", {
                    "blocker_id": last_id,
                    "owner_email": second_owner,
                    "target_close_date": future_date(30),
                })
                print(f"[assessment_flow] assign {last_id}: {second}")
                asked = client.elicitations[before:]

                if expect_team_question:
                    if not asked:
                        failures.append("an unregistered owner did not raise the team question")
                    else:
                        message = asked[0].message
                        if new_owner not in message:
                            failures.append("the team question did not name the owner: "
                                            f"{message!r}")
                        schema = asked[0].requestedSchema or {}
                        team = (schema.get("properties") or {}).get("team") or {}
                        if sorted(team.get("enum") or []) != ["application", "platform"]:
                            failures.append("the team question is not a platform/application "
                                            f"choice: {schema!r}")
                    developers = registry_roles(bucket).get("developers") or []
                    if new_owner not in developers:
                        failures.append(f"{new_owner} answered 'application' but was not added "
                                        f"to the registry's developers: {developers}")
                elif asked:
                    failures.append("a registered owner should not raise the team question")

                # --- The gate opens on the assignment that completes the set --
                if "STATE_LZ_DESIGN" not in second:
                    failures.append("the last blocker was owned and dated, but landing zone "
                                    f"design did not unlock: {second!r}")
                if current_state(bucket) != "STATE_LZ_DESIGN":
                    failures.append("the ledger disagrees with the tool about the unlock: "
                                    f"{current_state(bucket)}")
                unowned = [b["id"] for b in blockers_in_ledger(bucket)
                           if not (b.get("owner") and b.get("target_close_date"))]
                if unowned:
                    failures.append(f"landing zone unlocked with unowned blockers: {unowned}")

                # --- Claim 5: the unlocked step can obtain what it must use ---
                # landingzone_design_2/instructions.md tells the agent that the
                # first resolve_lz_decision allocates the workspace and to "read
                # them back". The agent may not read the ledger or the server's
                # files (the standing rules), so the tool response is the only
                # channel it has. Asserting on the ledger instead would pass
                # while the agent is still unable to see any of it.
                decision = await call(session, "resolve_lz_decision", {
                    "decision_id": "karpenter",
                    "choice": "GKE_STANDARD_NAP",
                })
                print(f"[assessment_flow] resolve_lz_decision: {decision}")
                allocated = variables_in_ledger(bucket)
                missing = [name for name in ("lz_branch_name", "target_clone_path")
                           if not allocated.get(name)]
                if missing:
                    failures.append(f"resolve_lz_decision did not allocate {missing} in the ledger")
                unreachable = [name for name in ("lz_branch_name", "target_clone_path")
                               if allocated.get(name) and allocated[name] not in decision]
                if unreachable:
                    failures.append(
                        f"resolve_lz_decision allocated {unreachable} but did not return "
                        f"{'them' if len(unreachable) > 1 else 'it'}, so the agent cannot clone "
                        f"the target repository: no tool exposes the workspace variables and the "
                        f"standing rules forbid reading the ledger. Response was {decision!r}")
    finally:
        session_cache.restore(snap, "assessment_flow")

    if client.answers:
        failures.append(f"{len(client.answers)} scripted answer(s) were never asked for; the "
                        "server skipped an elicitation this test expected")

    if failures:
        for f in failures:
            print(f"[assessment_flow] FAILED: {f}")
        return 1

    covered = "review elicitation + blocker guardrail" + (
        " + new-member registration" if new_owner else " (new-member registration skipped)")
    print(f"[assessment_flow] PASSED: {covered}, "
          f"{len(client.elicitations)} elicitation(s) answered")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(run()))
