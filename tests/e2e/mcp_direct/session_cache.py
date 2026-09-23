# Copyright 2026 Google LLC
# Licensed under the Apache License, Version 2.0.

"""Snapshot/restore of the developer's local ledger session caches.

The server under test writes the cwd-scoped cache
~/.ledger_config.d/<sha256(cwd)[:16]>.yaml (the MCP server child inherits
this harness's cwd) and bootstrap_migration deletes both that file and the
legacy machine-global ~/.ledger_config.yaml. These runs execute against the
real HOME, so the harness must put back exactly what the developer had:
restore an overwritten file's content, and remove any file the run created.
"""

import hashlib
import os

LEGACY_CONFIG = os.path.expanduser("~/.ledger_config.yaml")


def scoped_config() -> str:
    """The scoped cache path for THIS process's cwd (the server child
    inherits it) — same derivation as servers/dag/state_management.py."""
    digest = hashlib.sha256(os.getcwd().encode("utf-8")).hexdigest()[:16]
    return os.path.join(os.path.expanduser("~/.ledger_config.d"), f"{digest}.yaml")


def snapshot() -> dict:
    """Byte content of every session-cache file the run may touch (None =
    absent before the run)."""
    snap = {}
    for path in (LEGACY_CONFIG, scoped_config()):
        if os.path.exists(path):
            with open(path, "rb") as f:
                snap[path] = f.read()
        else:
            snap[path] = None
    return snap


def restore(snap: dict, tag: str) -> None:
    """Puts every snapshotted path back to its pre-run state."""
    for path, content in snap.items():
        if content is None:
            if os.path.exists(path):
                os.remove(path)
                print(f"[{tag}] removed {path} (did not exist before the run)")
        else:
            os.makedirs(os.path.dirname(path), exist_ok=True)
            with open(path, "wb") as f:
                f.write(content)
            print(f"[{tag}] restored pre-run {path}")
