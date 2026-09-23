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

"""Driving the graph through the nodes the agent does not act on.

A tool call lands the graph on a new state, but that state is often not one the
agent can do anything with: a human has to approve something, or the server has
to go and mutate something. This module walks those nodes until the graph reaches
somewhere the agent is needed again.

Both kinds of node used to be handled by a `while True` copy-pasted into each
tool that could reach one, with the prompt text hardcoded beside it. That is why
`prompt_template` sat unused in the graph for so long — nothing read it. Here the
template is the prompt.

Lives below main.py rather than in it because the phase packages need it and
main.py imports them. The internal-task actions genuinely belong to main.py, so
they arrive by injection: main.py calls set_mutation_runner at import.
"""

import asyncio
import logging
from typing import Optional

from pydantic import BaseModel, Field
from typing import Literal

logger = logging.getLogger("migration-dag")


class ApprovalSchema(BaseModel):
    approved: bool = Field(description="Approve ledger creation?")


class SSMApprovalSchema(BaseModel):
    approved: bool = Field(description="Approve Secure Source Manager repository creation?")


class LZApprovalSchema(BaseModel):
    approved: bool = Field(description="Approve the generated GKE landing zone design and Terraform code?")


class TranslationShipSchema(BaseModel):
    approved: bool = Field(
        description="Ship the validated Terraform? Approving opens the Pull Request "
                    "in the target repository — review the before/after comparison "
                    "and per-unit tradeoffs in the Review UI first. Declining "
                    "returns to the unit review."
    )


class WorkloadShipSchema(BaseModel):
    approved: bool = Field(
        description="Ship this component's validated Kubernetes manifests? "
                    "Approving opens the per-component Pull Request "
                    "(migration/workload-<component>-<uuid>) — review the "
                    "unit summaries, tradeoffs, parked units and open "
                    "questions in the tool output and the ledger comparisons "
                    "first. Declining returns to the unit review."
    )


class RenderApprovalSchema(BaseModel):
    approved: bool = Field(description="Approve local rendering of Helm charts and Kustomize roots?")


class ReplicationModeSchema(BaseModel):
    mode: Literal["agent_mediated", "self_service"] = Field(
        default="self_service",
        description="How to replicate the discovered ECR images to Artifact Registry. "
                    "'agent_mediated': the server runs skopeo copies using credentials "
                    "already present in the local environment — it verifies they are "
                    "present first and reports exactly what is missing, but never runs "
                    "logins or sets credentials up. 'self_service' (default): the server "
                    "writes a runbook of skopeo copy commands to the ledger for you to "
                    "run yourself; the server never reads from AWS."
    )


class NewMemberSchema(BaseModel):
    team: Literal["platform", "application"] = Field(
        description="Which team does this blocker owner belong to? 'platform' registers them as a "
                    "platform engineer (write access to platform/); 'application' registers them as "
                    "a developer."
    )


# Response schema and decline message per HITL_ELICITATION state. These cannot
# live on the state definition: dag_schema.json closes HITL_ELICITATION to
# type/prompt_template/transitions, and widening it for two fields the server
# resolves anyway is more surface than the problem warrants.
ELICITATIONS = {
    "STATE_ELICIT_SSM_CREATION_APPROVAL": (SSMApprovalSchema, "User rejected SSM repository creation."),
    "STATE_LZ_APPROVED": (LZApprovalSchema, "User rejected GKE Landing Zone design."),
    "STATE_TRANSLATION_APPROVED": (TranslationShipSchema, "Ship declined; back to the unit review."),
    "STATE_WKLD_APPROVED": (WorkloadShipSchema, "Ship declined; back to the unit review."),
    "STATE_CONFIRM_NEW_MEMBER": (NewMemberSchema, "User declined to register the blocker owner."),
    "STATE_DISCOVERY_RENDER_APPROVAL": (
        RenderApprovalSchema,
        "Rendering declined; discovery continues with literal image references only."),
    "STATE_DEPLOYMENT_IMAGE_REPLICATION": (
        ReplicationModeSchema,
        "Image replication skipped; the images remain in ECR and the inventory is unchanged."),
}
DEFAULT_ELICITATION = (ApprovalSchema, "User rejected the request.")

# How many times run_dispatch_loop re-raises the SAME elicitation state within
# one drain. A mutation whose on_failure edge points back at the elicitation
# that approved it (the developer graph's SUBMIT_PR -> APPROVED) would
# otherwise loop forever on a deterministic failure. One retry is the intended
# affordance: the first failure is often a transient push race, the second is
# a cause the user has to go and fix.
ELICITATION_RETRIES = 1


_mutation_runner = None


def set_mutation_runner(fn) -> None:
    """Registers the INTERNAL_TASK_SERVER_MUTATION dispatcher (main.run_internal_mutation)."""
    global _mutation_runner
    _mutation_runner = fn


def render_prompt(state_name: str, state_def: dict, state_dict: dict, config: dict) -> str:
    """Renders a HITL state's prompt_template against the ledger.

    The template is formatted with the state variables plus the two workspace
    identifiers that live in the local config rather than the ledger. A template
    naming a field that does not exist falls back to the raw template: a
    placeholder typo should degrade to an odd-looking prompt, not abort a
    migration mid-run.
    """
    template = state_def.get("prompt_template") or ""
    fields = dict(state_dict.get("variables") or {})
    fields["project_id"] = config.get("gcp_project")
    fields["workspace_name"] = config.get("workspace_name")
    try:
        return template.format(**fields)
    except (KeyError, IndexError) as e:
        logger.error(
            f"{state_name}: prompt_template references {e} which is not a ledger variable; "
            "sending it unrendered"
        )
        return template


async def run_elicitation(ctx, state_name: str, state_def: dict, schema_cls,
                          state_dict: dict, config: dict,
                          notice: Optional[str] = None) -> tuple[bool, Optional[dict]]:
    """Puts a HITL state's question to the user and returns (approved, response).

    Approval is the user accepting the form. A schema carrying an explicit
    `approved` field can still decline within an accepted form; a schema without
    one (the response is data, not a yes/no) treats acceptance as approval.

    `notice` is prepended to the rendered prompt. It carries why the state is
    being raised a second time: prompt_template is static, so without it a
    re-entry after a failed mutation asks the identical question and the user
    has no way to tell the retry from the first ask.
    """
    from mcp import types
    from mcp.shared.message import ServerMessageMetadata

    session = ctx.request_context.session
    related_request_id = ctx.request_id

    progress_token = None
    if ctx.request_context.meta:
        progress_token = ctx.request_context.meta.progressToken

    logger.debug(f"Custom elicit at {state_name}: progress_token={progress_token}")
    meta = types.RequestParams.Meta(progressToken=progress_token) if progress_token else None

    prompt = render_prompt(state_name, state_def, state_dict, config)
    if notice:
        prompt = f"{notice}\n\n{prompt}"

    params = types.ElicitRequestFormParams(
        message=prompt,
        requestedSchema=schema_cls.model_json_schema(),
        _meta=meta,
    )

    res = await session.send_request(
        types.ServerRequest(types.ElicitRequest(params=params)),
        types.ElicitResult,
        metadata=ServerMessageMetadata(related_request_id=related_request_id),
    )

    if res.action != "accept":
        return False, None
    if res.content is None:
        # An accept carrying no form body is a bare approval.
        return True, None

    validated = schema_cls.model_validate(res.content)
    return getattr(validated, "approved", True), validated.model_dump()


async def run_dispatch_loop(ctx, state_dict: dict, platform_dag: dict, config: dict,
                            message: str,
                            first_notice: str = None) -> tuple[list, str, Optional[str]]:
    """Drives the graph from its current state through every server-side and
    human-in-the-loop node, stopping at the next node that needs the agent.

    `first_notice` rides on the FIRST elicitation this walk raises, and only
    that one — a caller fact the user has to read before answering, as
    opposed to the retry notice below, which is a fact about the previous
    attempt. `message` cannot carry it: the walk returns that to the caller
    AFTER the elicitation and everything the answer triggered, so a warning
    put there reaches the user once the decision it was meant to inform has
    already been acted on. The workload ship gate is the case that needed
    it: outstanding data services no component could be attributed to have to
    be visible while the developer decides to ship, not in the report of the
    pull request that shipped.

    Returns (transitions_run, message, error). A non-None error means the run
    could not continue and the caller must return it without saving — a failed
    elicitation leaves the ledger untouched rather than half-advanced.

    Every server-side outcome in the walk is kept: a drain can cross several
    mutations (provision, then replicate) and a decline, and the last message
    must not erase the reasons that came before it — a provisioning failure
    stays visible even when replication reports after it.
    """
    transitions_run = []
    collected = []
    asked = {}

    while True:
        current_state = state_dict["current_state"]
        state_def = platform_dag["states"][current_state]
        state_type = state_def["type"]

        if state_type in ("TERMINAL", "AGENT_TASK"):
            break

        if state_type == "HITL_ELICITATION":
            # Bounded re-entry. A mutation whose on_failure points back at
            # the elicitation that approved it (the developer graph's
            # SUBMIT_PR -> APPROVED edge) spins here forever on a
            # deterministic failure — and re-asks the IDENTICAL static
            # prompt, since the failure text only reaches `collected`. One
            # retry is the intended affordance; past that, stop and let the
            # caller persist the state with every reason collected.
            asked[current_state] = asked.get(current_state, 0) + 1
            if asked[current_state] > ELICITATION_RETRIES + 1:
                # Never persist ON the elicitation state: a HITL node
                # accepts no tool call, so a component parked there is
                # unrecoverable without hand-editing the ledger (the
                # developer graph's APPROVED wedge). Follow the state's own
                # escape edge — what a human decline would have done — and
                # THEN stop, so the persisted state names reachable actions.
                exhausted = (
                    f"{current_state} has been raised "
                    f"{ELICITATION_RETRIES + 1} times in this run and what "
                    "followed it failed every time — not asking again.")
                escape_key = next(
                    (key for key in ("on_exhausted", "on_reject")
                     if state_def["transitions"].get(key)
                     and state_def["transitions"][key] != current_state),
                    None)
                if escape_key:
                    dest = state_def["transitions"][escape_key]
                    state_dict["history"].append(
                        f"Elicitation retries exhausted at {current_state}; "
                        f"following '{escape_key}'")
                    state_dict["history"].append(
                        f"Transitioned {current_state} -> {dest}")
                    state_dict["current_state"] = dest
                    transitions_run.append(
                        f"{current_state} -> {dest} "
                        f"({escape_key}: retries exhausted)")
                    collected.append(
                        exhausted + f" Moved to {dest} via {escape_key} so "
                        "the workspace persists on a state whose tools are "
                        "callable. Fix the cause reported above, then "
                        "continue from there.")
                else:
                    collected.append(
                        exhausted + " Fix the cause reported above and "
                        "re-run the step's tool.")
                break
            schema_cls, reject_message = ELICITATIONS.get(current_state, DEFAULT_ELICITATION)
            # Second and later asks name what went wrong after the first one.
            # The caller's notice rides the first ask and is then spent: on a
            # retry the reason for re-asking is what the user needs, and
            # stacking both would bury it.
            notice = None
            if asked[current_state] > 1 and collected:
                notice = (f"Retry {asked[current_state] - 1} of "
                          f"{ELICITATION_RETRIES}. The previous attempt "
                          f"failed: {collected[-1]}")
            elif first_notice:
                notice, first_notice = first_notice, None
            try:
                approved, content = await run_elicitation(
                    ctx, current_state, state_def, schema_cls, state_dict,
                    config, notice=notice)
            except Exception as e:
                logger.exception(f"Interactive elicitation failed at {current_state}")
                return transitions_run, message, f"ERROR: Elicitation request failed: {e}"

            # Kept so the mutation state that follows can read what the user
            # answered: an internal task sees only the variables. Always
            # overwritten, never merely added: these persist to the ledger
            # when a later state parks, and a bare accept (or a decline) must
            # not leave a previous run's answer behind to be replayed — e.g.
            # last run's explicit agent_mediated opt-in surviving into this
            # run's bare-accept default.
            state_dict["variables"].setdefault("elicitation_responses", {})[current_state] = content or {}

            transition_key = "on_approve" if approved else "on_reject"
            dest = state_def["transitions"][transition_key]
            state_dict["history"].append(f"Interactive HITL response: '{transition_key}'")
            state_dict["history"].append(f"Transitioned {current_state} -> {dest}")
            state_dict["current_state"] = dest
            transitions_run.append(f"{current_state} -> {dest}")

            if not approved:
                # A decline keeps draining rather than returning: its target
                # can be a mutation with work of its own (e.g. recording a
                # declined render), and nothing else would ever run it —
                # get_next_stage reports states, it does not execute them.
                # A reject leg that leads straight to an agent task parks
                # there immediately, so this changes nothing for those.
                collected.append(reject_message)

            continue

        if state_type == "INTERNAL_TASK_SERVER_MUTATION":
            if _mutation_runner is None:
                raise RuntimeError(
                    "no mutation runner registered; main.py must call "
                    "dispatch.set_mutation_runner at import"
                )
            action = state_def["action"]
            # The mutation does blocking network I/O (git clone/commit/push, SSM
            # LRO polling with time.sleep). run_dispatch_loop is async and drives
            # the request handler's event loop, so run it off-thread to keep the
            # loop free for live progress updates and other requests.
            transition_key, msg = await asyncio.to_thread(
                _mutation_runner, action, state_dict["variables"], config)
            dest = state_def["transitions"][transition_key]

            state_dict["history"].append(f"Action '{action}' returned '{transition_key}': {msg}")
            state_dict["history"].append(f"Transitioned {current_state} -> {dest}")
            state_dict["current_state"] = dest
            transitions_run.append(f"{current_state} -> {dest}")
            collected.append(msg)

            if transition_key == "on_pending":
                break

    if collected:
        message = "\n- ".join(collected)
    return transitions_run, message, None
