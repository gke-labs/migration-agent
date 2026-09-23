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

"""Validation for DAG definitions.

Two layers, both run at server start-up and in CI:

  1. dag_schema.json — the shape of the document: state types, per-type required
     fields, transition naming, and the path/name patterns for the phase fields.
  2. Structural checks — things a JSON Schema cannot express: that transitions
     point at states that exist, that the start state exists, and that every
     referenced instructions and knowledge file is actually present on disk.

Start-up enforcement is the point. Without it a mistyped instructions path
degrades to an empty string and reaches the agent as a state with no procedure —
a silent failure that surfaces as confused behaviour several turns later.

Repo-hygiene checks that are not runtime correctness — an unreferenced step
folder, a phase package no state points at — belong in CI rather than here. A
server with an unused folder runs correctly; it should not refuse to start.
"""

import json
import logging
import os

logger = logging.getLogger("migration-dag")

_HERE = os.path.dirname(os.path.realpath(__file__))
SCHEMA_PATH = os.path.join(_HERE, "schema", "dag_schema.json")

# servers/dag/server/ -> repository root. Content paths in a DAG are relative to
# this, and are resolved against it exactly as the server does when serving them.
REPO_ROOT = os.path.abspath(os.path.join(_HERE, "..", "..", ".."))


class DagValidationError(Exception):
    """Raised when a DAG definition is malformed. Fatal at start-up."""


def _load_schema() -> dict:
    with open(SCHEMA_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


def check_schema(dag: dict, source: str) -> list:
    """Validates the document against dag_schema.json. Returns a list of errors."""
    try:
        import jsonschema
    except ImportError as e:
        raise DagValidationError(
            f"jsonschema is required to validate DAG definitions but is not installed: {e}"
        )

    validator = jsonschema.Draft202012Validator(_load_schema())
    errors = []
    for err in sorted(validator.iter_errors(dag), key=lambda e: list(e.absolute_path)):
        where = "/".join(str(p) for p in err.absolute_path) or "<root>"
        errors.append(f"{source}: {where}: {err.message}")
    return errors


def check_structure(dag: dict, source: str, repo_root: str = REPO_ROOT) -> list:
    """Validates references the schema cannot check. Returns a list of errors."""
    errors = []
    states = dag.get("states", {})

    start = dag.get("start_state")
    if start and start not in states:
        errors.append(f"{source}: start_state '{start}' is not a defined state")

    for name, state in states.items():
        for outcome, target in (state.get("transitions") or {}).items():
            if target not in states:
                errors.append(
                    f"{source}: {name}.transitions.{outcome} -> '{target}' is not a defined state"
                )

        # The dispatch loop's exhaustion arm persists off a HITL state via
        # its escape edge (on_exhausted, else on_reject); without one the
        # workspace would park ON the elicitation — a state no tool accepts,
        # unrecoverable short of hand-editing the ledger.
        if state.get("type") == "HITL_ELICITATION":
            transitions = state.get("transitions") or {}
            escape = transitions.get("on_exhausted") or transitions.get("on_reject")
            if not escape or escape == name:
                errors.append(
                    f"{source}: {name}: HITL_ELICITATION has no escape edge "
                    "(on_exhausted or on_reject) leading off the state — "
                    "elicitation-retry exhaustion would wedge the workspace"
                )

        # Checked in its own right, not just via the content paths below: a
        # state may declare a phase and carry no files yet.
        phase = state.get("phase")
        if phase and not os.path.isdir(os.path.join(repo_root, "servers", "phases", phase)):
            errors.append(f"{source}: {name}: phase '{phase}' has no package directory")

        # Referenced content must exist on disk, and must resolve inside the
        # repository — the same confinement the server applies when serving it.
        for rel_path, what in _content_paths(state, phase):
            resolved = os.path.normpath(os.path.join(repo_root, rel_path))
            if not resolved.startswith(repo_root + os.sep):
                errors.append(f"{source}: {name}: {what} escapes the repository: {rel_path}")
            elif not os.path.isfile(resolved):
                errors.append(f"{source}: {name}: {what} not found: {rel_path}")

        if state.get("step") and state.get("instructions"):
            expected = state["step"].rstrip("/") + "/instructions.md"
            if state["instructions"] != expected:
                errors.append(
                    f"{source}: {name}: instructions '{state['instructions']}' "
                    f"does not sit in step folder '{state['step']}'"
                )

    return errors


def _content_paths(state: dict, phase):
    """Yields (repo-relative path, description) for every file a state references."""
    if state.get("instructions"):
        yield state["instructions"], "instructions"
    if state.get("step"):
        yield state["step"] + "/instructions.md", "step folder"
    if phase:
        for doc in state.get("knowledge") or []:
            yield os.path.join("servers", "phases", phase, "knowledge", doc), f"knowledge doc '{doc}'"


def report_unconverted(dag: dict, source: str) -> None:
    """Logs AGENT_TASK states that carry no instructions.

    Not an error yet: phase conversion is incomplete, and until it finishes the
    dag_executor skill supplies a fallback procedure for these states. When the
    last one is converted this becomes a schema requirement and the fallback
    goes away.
    """
    missing = sorted(
        name
        for name, state in dag.get("states", {}).items()
        if state.get("type") == "AGENT_TASK" and not state.get("instructions")
    )
    if missing:
        logger.warning(
            f"{source}: {len(missing)} AGENT_TASK state(s) serve no instructions and rely on the "
            f"dag_executor fallback: {', '.join(missing)}"
        )


def validate_dag(dag: dict, source: str, repo_root: str = REPO_ROOT) -> None:
    """Validates a DAG definition, raising DagValidationError on any problem."""
    errors = check_schema(dag, source) + check_structure(dag, source, repo_root)
    if errors:
        for e in errors:
            logger.error(f"DAG validation: {e}")
        raise DagValidationError(
            f"{source} failed validation with {len(errors)} error(s): " + "; ".join(errors[:5])
        )
    report_unconverted(dag, source)
    logger.debug(f"{source}: DAG validation passed ({len(dag.get('states', {}))} states)")
