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

"""Shared LLM worker primitives for phase pipelines.

Discovery extraction and report generation run the same worker shape: a
tool-less, single-turn agent query with a small, fully-specified context,
returning text or JSON. This module owns the call, the JSON parsing, and the
credential preflight so each phase only owns its prompts and schemas.

Two backends, selected by GKE_AGENTIC_MIGRATION_WORKER_SDK:
  - "antigravity" (default): Google Antigravity SDK (pip install
    google-antigravity), all tools denied so the worker stays a pure text
    call. Credentials: GEMINI_API_KEY, or GOOGLE_GENAI_USE_VERTEXAI=True with
    GOOGLE_CLOUD_PROJECT / GOOGLE_CLOUD_LOCATION and gcloud ADC. The SDK
    chooses its own Gemini model — it exposes no model parameter, so the
    GKE_AGENTIC_MIGRATION_*_MODEL variables do not apply to it. When the package or its
    credentials are missing, or a call fails, workers fall back to the
    Claude backend automatically.
  - "claude": Claude Agent SDK one-shot query, and the automatic backup for
    the default backend. Credentials resolve from the environment at call
    time (ANTHROPIC_API_KEY, or CLAUDE_CODE_USE_VERTEX=1 with gcloud ADC,
    etc.). Models come from GKE_AGENTIC_MIGRATION_EXTRACT_MODEL / GKE_AGENTIC_MIGRATION_REPORT_MODEL.
    Set GKE_AGENTIC_MIGRATION_WORKER_SDK=claude to skip Antigravity entirely.
"""

import asyncio
import json
import logging
import os
import re

logger = logging.getLogger("migration-dag")

# The CLI subprocess can die on transient transport failures (e.g. a stalled
# Vertex stream mid-generation surfaces as "Claude Code returned an error
# result" after the CLI exhausts its own retries). Those get fresh attempts.
WORKER_TRANSIENT_RETRIES = int(os.environ.get("GKE_AGENTIC_MIGRATION_WORKER_TRANSIENT_RETRIES", "2"))
WORKER_RETRY_BACKOFF_SECONDS = float(os.environ.get("GKE_AGENTIC_MIGRATION_WORKER_RETRY_BACKOFF", "15"))

# Which agent SDK executes workers: "antigravity" (default; falls back to
# Claude when unusable) or "claude" (Claude only).
WORKER_SDK = os.environ.get("GKE_AGENTIC_MIGRATION_WORKER_SDK", "antigravity")

AUTH_ENV_VARS = (
    "ANTHROPIC_API_KEY",
    "ANTHROPIC_AUTH_TOKEN",
    "CLAUDE_CODE_USE_VERTEX",
    "CLAUDE_CODE_USE_BEDROCK",
    "CLAUDE_CODE_USE_ANTHROPIC_AWS",
    "CLAUDE_CODE_USE_FOUNDRY",
)

ANTIGRAVITY_AUTH_ENV_VARS = (
    "GEMINI_API_KEY",
    "GOOGLE_GENAI_USE_VERTEXAI",
    "GOOGLE_GENAI_USE_ENTERPRISE",
)


def check_llm_auth() -> str:
    """Returns an error string if no usable worker backend is configured, else ''."""
    if WORKER_SDK == "claude":
        return _claude_error()
    if WORKER_SDK != "antigravity":
        return f"Unknown GKE_AGENTIC_MIGRATION_WORKER_SDK '{WORKER_SDK}'. Use 'antigravity' or 'claude'."

    antigravity_error = _antigravity_error()
    if not antigravity_error:
        return ""
    claude_error = _claude_error()
    if not claude_error:
        logger.warning(
            f"Antigravity backend unavailable ({antigravity_error}) — "
            "workers will run on the Claude Agent SDK backup."
        )
        return ""
    return (
        f"Neither worker backend is usable. Antigravity: {antigravity_error} "
        f"Claude backup: {claude_error}"
    )


def _claude_error() -> str:
    """'' if the Claude Agent SDK backend is usable, else an actionable error."""
    try:
        import claude_agent_sdk  # noqa: F401
    except ImportError:
        return (
            "The 'claude-agent-sdk' package is not installed in the MCP server "
            "environment. Add it to servers/dag/requirements.txt and reinstall."
        )

    if any(os.environ.get(var) for var in AUTH_ENV_VARS):
        return ""

    # Fall back to an existing interactive login on this workstation.
    home = os.path.expanduser("~")
    credential_paths = (
        os.path.join(home, ".claude", ".credentials.json"),
        os.path.join(home, ".config", "anthropic"),
    )
    if any(os.path.exists(p) for p in credential_paths):
        return ""

    return (
        "No LLM credentials configured for workers. Either set "
        "ANTHROPIC_API_KEY, or set CLAUDE_CODE_USE_VERTEX=1 (plus "
        "ANTHROPIC_VERTEX_PROJECT_ID and CLOUD_ML_REGION) and run "
        "'gcloud auth application-default login'."
    )


def _antigravity_error() -> str:
    """'' if the Antigravity backend is usable, else an actionable error."""
    try:
        import google.antigravity  # noqa: F401
    except ImportError:
        return (
            "The 'google-antigravity' package is not installed. Run "
            "'pip install google-antigravity' (the PyPI wheel ships the "
            "required runtime binary)."
        )

    if any(os.environ.get(var) for var in ANTIGRAVITY_AUTH_ENV_VARS):
        return ""

    # Note: gcloud ADC on disk is deliberately NOT treated as sufficient —
    # the Antigravity SDK only uses it when GOOGLE_GENAI_USE_VERTEXAI (or
    # _ENTERPRISE) is set, and otherwise demands GEMINI_API_KEY at call time.
    return (
        "No credentials configured for the Antigravity worker backend. Either "
        "set GEMINI_API_KEY, or set GOOGLE_GENAI_USE_VERTEXAI=True (plus "
        "GOOGLE_CLOUD_PROJECT and GOOGLE_CLOUD_LOCATION) and run "
        "'gcloud auth application-default login'."
    )


async def run_worker(prompt: str, model: str, max_turns: int = 1) -> str:
    """Runs one tool-less, single-turn worker: Antigravity first, Claude backup.

    With the default backend, Antigravity handles the call when it is
    installed and credentialed; otherwise — or if its call raises and the
    Claude backend is usable — the same prompt runs on the Claude Agent SDK.
    GKE_AGENTIC_MIGRATION_WORKER_SDK=claude skips Antigravity entirely.
    """
    if WORKER_SDK == "antigravity" and not _antigravity_error():
        try:
            return await _run_antigravity_worker(prompt)
        except Exception as e:
            if _claude_error():
                raise
            logger.warning(f"Antigravity worker failed ({e!r}); retrying on the Claude backup.")
    return await _run_claude_worker(prompt, model, max_turns=max_turns)


WORKER_SYSTEM_PROMPT = (
    "You are a headless text-generation worker with NO tools: never attempt "
    "tool calls, file operations, or shell commands — they will be denied and "
    "waste your only turn. Reply with the requested output directly as plain "
    "text in your final message."
)


async def _run_claude_worker(prompt: str, model: str, max_turns: int = 1) -> str:
    from claude_agent_sdk import query, ClaudeAgentOptions

    result_text = ""
    async for message in query(
        prompt=prompt,
        options=ClaudeAgentOptions(
            # tools=[] removes the built-in toolset entirely; allowed_tools is
            # only an auto-approval list and restricts nothing on its own.
            # setting_sources=[] keeps the operator's ~/.claude settings (and
            # their permission allow rules) out of the worker. Workers read
            # untrusted customer IaC, so the no-tools guarantee must not rest
            # on the system prompt or on whoever runs the server.
            tools=[],
            setting_sources=[],
            allowed_tools=[],
            max_turns=max_turns,
            model=model,
            system_prompt=WORKER_SYSTEM_PROMPT,
        ),
    ):
        result = getattr(message, "result", None)
        if isinstance(result, str):
            result_text = result
    return result_text


async def _run_antigravity_worker(prompt: str) -> str:
    # deny("*") on top of the SDK's read-only default: workers are pure
    # prompt->text calls and must never touch tools. The SDK selects its own
    # Gemini model (it exposes no model parameter).
    from google.antigravity import Agent, LocalAgentConfig
    from google.antigravity.hooks.policy import deny

    config = LocalAgentConfig(policies=[deny("*")])
    async with Agent(config) as agent:
        response = await agent.chat(prompt)
        return await response.text()


async def call_worker(worker_fn, prompt: str, model: str, timeout_seconds: float, what: str) -> str:
    """Runs worker_fn(prompt, model) with a hard timeout and transient retries.

    The timeout abandons the worker task instead of awaiting its cancellation:
    when the CLI is wedged on a stalled stream, cancellation itself can block
    until the CLI gives up (observed at well past the configured timeout), so
    the task is left to die on its own and the timeout error is raised
    immediately. Worker exceptions are treated as transient CLI/transport
    failures and get fresh attempts; a timeout does not retry (one stalled
    generation already spent the full budget).
    """
    last_error = None
    for attempt in range(WORKER_TRANSIENT_RETRIES + 1):
        task = asyncio.ensure_future(worker_fn(prompt, model))
        done, _ = await asyncio.wait({task}, timeout=timeout_seconds)
        if not done:
            task.cancel()
            task.add_done_callback(
                lambda t: t.exception() if not t.cancelled() else None
            )
            raise ValueError(f"{what} worker timed out after {timeout_seconds:.0f}s")
        try:
            return task.result()
        except Exception as e:
            last_error = e
            logger.warning(f"{what} worker attempt {attempt + 1} failed: {e!r}")
            if attempt < WORKER_TRANSIENT_RETRIES:
                await asyncio.sleep(WORKER_RETRY_BACKOFF_SECONDS * (attempt + 1))
    raise ValueError(
        f"{what} worker failed after {WORKER_TRANSIENT_RETRIES + 1} attempts: {last_error}"
    )


def parse_worker_json(text: str) -> dict:
    """Extracts a JSON object from worker output, tolerating markdown fences.

    Plain JSON is tried first so valid output whose string values contain
    ``` fences (e.g. a tradeoffs field with code blocks) is never mangled by
    the fence-extraction fallback.
    """
    cleaned = text.strip()
    try:
        parsed = json.loads(cleaned)
        if isinstance(parsed, dict):
            return parsed
    except json.JSONDecodeError:
        pass

    fence = re.search(r"```(?:json)?\s*(.*?)```", cleaned, re.DOTALL)
    if fence:
        candidate = fence.group(1).strip()
        try:
            parsed = json.loads(candidate)
            if isinstance(parsed, dict):
                return parsed
        except json.JSONDecodeError:
            cleaned = candidate

    start = cleaned.find("{")
    end = cleaned.rfind("}")
    if start == -1 or end == -1 or end <= start:
        raise ValueError(f"No JSON object found in worker output: {text[:200]!r}")
    return json.loads(cleaned[start : end + 1])
