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

"""Terraform validation with a bounded LLM auto-fix loop.

Validates the materialized tree (the landing-zone draft at the clone root and
each translated unit directory) with `terraform init -backend=false &&
terraform validate`. A directory that fails is handed to an LLM worker
together with the exact validate error; the worker returns corrected HCL, we
rewrite the directory and validate again, up to a bounded number of attempts.

`autofix_dir` is the pure loop — it takes injected `validate_fn`/`fix_fn`
coroutines so the attempt accounting (clean / fixed / failed, N-attempt cap)
is testable without terraform or an LLM; `run_validation` binds the real
terraform subprocess and the real worker to it over a clone on disk.

Kubernetes manifests materialized beside the Terraform get their own offline
gate, `check_unit_manifests`: terraform never reads them, so without it the
YAML half of a unit would ship unexamined. It is the same shared pure-Python
structural check (servers/phases/k8s_manifests.py) the translator applies to
worker output — no kubectl, no cluster, no credentials — and it has no fix
loop: a manifest that fails here goes back to the reviewer, not to a repair
worker.
"""

import asyncio
import json
import logging
import os
import subprocess

from servers.phases import agent_workers, k8s_manifests

logger = logging.getLogger("migration-dag")

FIX_MODEL = os.environ.get("GKE_AGENTIC_MIGRATION_VALIDATE_FIX_MODEL", "claude-opus-5")
# Terraform authored by a large model can need a couple of focused corrections;
# each worker call carries its own hard timeout + transient retries (the Vertex
# stream on this workstation stalls on long generations).
FIX_TIMEOUT_SECONDS = float(os.environ.get("GKE_AGENTIC_MIGRATION_VALIDATE_FIX_TIMEOUT", "900"))
MAX_FIX_ATTEMPTS = int(os.environ.get("GKE_AGENTIC_MIGRATION_VALIDATE_FIX_ATTEMPTS", "2"))

FIX_RULES = """You are a Terraform repair worker. A `terraform validate` run failed
on ONE directory. You receive the exact error and every .tf file in that
directory. Return corrected files that make `terraform validate` pass.

Output ONLY a JSON object (no prose, no markdown fences):
{"files": [{"path": "<dir-relative path ending in .tf>", "content": "<full corrected HCL>"}]}

Rules:
- Return the COMPLETE content of every file you change; omit files you leave
  unchanged. Never truncate.
- Fix only what the error requires. Do not redesign working resources or rename
  things gratuitously.
- If a required provider or version block is missing, add a versions.tf.
- Never invent project IDs, regions, or CIDRs — keep referencing the existing
  variables."""


def terraform_validate(terraform_bin: str, tf_dir: str) -> str:
    """Returns '' on success, else the validation error text."""
    env = dict(os.environ)
    env.setdefault("TF_PLUGIN_CACHE_DIR",
                   os.path.expanduser("~/.gke-agentic-migration/tf-plugin-cache"))
    os.makedirs(env["TF_PLUGIN_CACHE_DIR"], exist_ok=True)
    try:
        subprocess.run(
            [terraform_bin, "init", "-backend=false", "-input=false"],
            cwd=tf_dir, capture_output=True, text=True, check=True, env=env,
        )
        subprocess.run(
            [terraform_bin, "validate"],
            cwd=tf_dir, capture_output=True, text=True, check=True, env=env,
        )
        return ""
    except subprocess.CalledProcessError as e:
        return (e.stderr or e.stdout or "").strip() or "terraform exited non-zero"
    except Exception as e:  # noqa: BLE001 - reported upstream verbatim
        logger.exception("Failed during Terraform validation")
        return str(e)


async def autofix_dir(files: dict, validate_fn, fix_fn, max_attempts: int = MAX_FIX_ATTEMPTS) -> tuple:
    """Validate → fix → re-validate loop for one directory's files.

    - files: {rel_path: content} for the directory
    - validate_fn(files) -> error str (async; '' means valid)
    - fix_fn(files, error) -> corrected {rel_path: content} (async)

    Returns (final_files, outcome) where outcome is a dict:
      {"status": "clean"|"fixed"|"failed", "attempts": n,
       "original_error": str, "final_error": str}
    """
    error = await validate_fn(files)
    if not error:
        return files, {"status": "clean", "attempts": 0, "original_error": "", "final_error": ""}

    original_error = error
    for attempt in range(1, max(0, max_attempts) + 1):
        files = await fix_fn(files, error)
        error = await validate_fn(files)
        if not error:
            return files, {"status": "fixed", "attempts": attempt,
                           "original_error": original_error, "final_error": ""}

    return files, {"status": "failed", "attempts": max(0, max_attempts),
                   "original_error": original_error, "final_error": error}


def _read_tf_files(dir_path: str, recursive: bool = True) -> dict:
    """{rel_path: content} for the .tf files under dir_path.

    recursive=False reads only the directory's own .tf files — used for the
    clone root, whose subtrees (the unit directories) are validated and fixed
    as their own entries and must not be swept into the root's fix loop.

    .terraform/ is always skipped: after `terraform init` it holds downloaded
    module sources, which are not the directory's own code — sweeping them in
    would hand them to the fix worker and (via the fold-back) persist them
    into the unit blobs.
    """
    files = {}
    if not recursive:
        for name in sorted(os.listdir(dir_path)):
            if name.endswith(".tf") and os.path.isfile(os.path.join(dir_path, name)):
                with open(os.path.join(dir_path, name), "r", encoding="utf-8") as f:
                    files[name] = f.read()
        return files
    for root, dirs, names in os.walk(dir_path):
        dirs[:] = [d for d in dirs if d != ".terraform"]
        for name in names:
            if name.endswith(".tf"):
                full = os.path.join(root, name)
                rel = os.path.relpath(full, dir_path)
                with open(full, "r", encoding="utf-8") as f:
                    files[rel] = f.read()
    return files


def _write_tf_files(dir_path: str, files: dict) -> None:
    """Writes {rel_path: content} back into dir_path, guarding path escapes."""
    root_real = os.path.realpath(dir_path)
    for rel, content in files.items():
        full = os.path.realpath(os.path.join(dir_path, rel))
        if full != root_real and not full.startswith(root_real + os.sep):
            raise ValueError(f"refusing to write outside the directory: {rel!r}")
        os.makedirs(os.path.dirname(full), exist_ok=True)
        with open(full, "w", encoding="utf-8") as f:
            f.write(content)


async def _worker_fix(files: dict, error: str, model: str,
                      allow_subdirs: bool = True) -> dict:
    """Asks a worker to repair the directory; merges its files over the current set.

    allow_subdirs=False is the clone root's setting: the root's file set is
    its own top-level .tf files only, so a nested path returned there could
    only be INVENTED unit code — written into a unit directory behind the
    unit's own pass, never folded back into the unit blob, and shipped while
    the Review UI shows the original. Unit code is repaired only by that
    unit's own pass.
    """
    payload = {
        "validate_error": error,
        "files": [{"path": p, "content": c} for p, c in sorted(files.items())],
    }
    prompt = f"{FIX_RULES}\n\nFailed directory:\n{json.dumps(payload, indent=2, default=str)}"
    raw = await agent_workers.call_worker(
        agent_workers.run_worker, prompt, model, FIX_TIMEOUT_SECONDS, "terraform-autofix")
    result = agent_workers.parse_worker_json(raw)
    merged = dict(files)
    for entry in result.get("files", []) or []:
        path = str(entry.get("path", "")).strip()
        content = entry.get("content")
        if not path or content is None:
            continue
        if path.startswith(("/", "..")) or path.endswith(".tf") is False:
            logger.warning(f"autofix worker returned an unusable path {path!r}; ignoring it")
            continue
        if not allow_subdirs and ("/" in path or os.sep in path):
            logger.warning(
                f"autofix worker for the clone root returned the nested path "
                f"{path!r}; ignoring it — unit code is repaired only by the "
                "unit's own pass")
            continue
        merged[path] = content
    return merged


def check_unit_manifests(clone_dir: str, unit_dirs: list) -> dict:
    """Structural check over the Kubernetes manifests in the unit directories.

    Walks each unit directory (clone-relative) for .yaml/.yml files and applies
    the translator's manifest structure check to each. Only the unit
    directories are scanned — the clone root is the landing-zone draft plus
    whatever the target repository already contains, and neither is this
    gate's to judge.

    Returns {"checked": n, "clean": [file, ...], "invalid": [{"file", "error"}]}
    with clone-relative paths, deterministically ordered.
    """
    report = {"checked": 0, "clean": [], "invalid": []}
    for rel in unit_dirs:
        dir_path = os.path.join(clone_dir, rel)
        for root, dirs, names in os.walk(dir_path):
            # This gate runs after `terraform init`, which drops remote module
            # sources — their CI workflows and .yml fixtures included — under
            # .terraform/. Those files are machine-local, gitignored, and never
            # ship; judging them would dead-end validation on a directory the
            # reviewer cannot fix.
            dirs[:] = sorted(d for d in dirs if d != ".terraform")
            for name in sorted(names):
                if not name.endswith((".yaml", ".yml")):
                    continue
                full = os.path.join(root, name)
                rel_file = os.path.relpath(full, clone_dir)
                with open(full, "r", encoding="utf-8") as f:
                    content = f.read()
                report["checked"] += 1
                error = k8s_manifests.manifest_structure_error(content)
                if error:
                    report["invalid"].append({"file": rel_file, "error": error})
                else:
                    report["clean"].append(rel_file)
    return report


async def run_validation(clone_dir: str, tf_dirs: list, terraform_bin: str,
                         model: str = None, max_fix_attempts: int = None) -> dict:
    """Validates each terraform dir under the clone, auto-fixing failures.

    Rewrites any directory the worker repairs on disk (so the subsequent commit
    captures the fixes) and returns a report:
      {"dirs": [{dir, status, attempts, original_error, final_error}],
       "fixed": [...], "remaining": [...], "clean": [...], "all_valid": bool}
    """
    model = model or FIX_MODEL
    attempts_cap = MAX_FIX_ATTEMPTS if max_fix_attempts is None else max_fix_attempts

    async def validate_fn_for(dir_path):
        async def _validate(files):
            _write_tf_files(dir_path, files)
            return await asyncio.to_thread(terraform_validate, terraform_bin, dir_path)
        return _validate

    def fix_fn_for(rel):
        # The clone root's worker may not write nested paths (see _worker_fix).
        allow_subdirs = rel != "."

        async def fix_fn(files, error):
            # A fix worker can time out or return unparseable output. Treat that
            # as "no correction available" (return the files unchanged) rather
            # than letting the exception unwind the whole validation run: the
            # directory simply re-validates, still fails, and is reported in
            # `remaining` so the run finishes cleanly and routes the reviewer
            # back with the report.
            try:
                return await _worker_fix(files, error, model, allow_subdirs)
            except Exception as e:  # noqa: BLE001 - surfaced as an unfixed directory
                logger.error(f"autofix worker failed; leaving the directory unfixed: {e!r}")
                return files
        return fix_fn

    report = {"dirs": [], "fixed": [], "remaining": [], "clean": [], "all_valid": True}
    for rel in tf_dirs:
        dir_path = os.path.join(clone_dir, rel)
        files = _read_tf_files(dir_path, recursive=(rel != "."))
        validate_fn = await validate_fn_for(dir_path)
        final_files, outcome = await autofix_dir(files, validate_fn, fix_fn_for(rel), attempts_cap)
        _write_tf_files(dir_path, final_files)

        entry = {"dir": rel, **outcome}
        report["dirs"].append(entry)
        if outcome["status"] == "clean":
            report["clean"].append(rel)
        elif outcome["status"] == "fixed":
            report["fixed"].append({"dir": rel, "attempts": outcome["attempts"],
                                    "original_error": outcome["original_error"]})
        else:
            report["all_valid"] = False
            report["remaining"].append({"dir": rel, "error": outcome["final_error"]})
    return report
