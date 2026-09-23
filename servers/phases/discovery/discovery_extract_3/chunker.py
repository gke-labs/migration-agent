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

"""Deterministic chunking of the discovery manifest.

Groups manifest files into bounded chunks so each extraction worker gets one
small, coherent slice (files from the same directory stay together where
possible). Chunk IDs are content-addressed (path + size) so re-runs skip
chunks whose fragments already exist in the ledger.
"""

import hashlib
import os

DEFAULT_MAX_CHUNK_BYTES = 24_000


def chunk_manifest(manifest: dict, max_chunk_bytes: int = DEFAULT_MAX_CHUNK_BYTES) -> list:
    """Groups manifest files into chunks of at most max_chunk_bytes.

    Files are processed grouped by directory, then by path, so chunking is
    deterministic. A file larger than max_chunk_bytes gets its own chunk
    (the extractor truncates its content and records that in the fragment).

    Returns a list of {"chunk_id", "files": [manifest entries], "total_bytes"}.
    """
    entries = sorted(manifest["files"], key=lambda f: (os.path.dirname(f["path"]), f["path"]))

    chunks = []
    current = []
    current_bytes = 0
    current_dir = None

    def flush():
        nonlocal current, current_bytes
        if current:
            chunks.append(_make_chunk(current))
            current = []
            current_bytes = 0

    for entry in entries:
        entry_dir = os.path.dirname(entry["path"])
        dir_changed = current_dir is not None and entry_dir != current_dir
        if current and (current_bytes + entry["size"] > max_chunk_bytes or dir_changed and current_bytes > max_chunk_bytes // 2):
            flush()
        current.append(entry)
        current_bytes += entry["size"]
        current_dir = entry_dir
        if current_bytes >= max_chunk_bytes:
            flush()

    flush()
    return chunks


def _make_chunk(files: list) -> dict:
    key = "|".join(f"{f['path']}:{f['size']}" for f in files)
    chunk_id = hashlib.sha1(key.encode("utf-8")).hexdigest()[:16]
    return {
        "chunk_id": chunk_id,
        "files": list(files),
        "total_bytes": sum(f["size"] for f in files),
    }


def read_chunk_contents(chunk: dict, root_dir: str, max_chunk_bytes: int = DEFAULT_MAX_CHUNK_BYTES) -> str:
    """Reads a chunk's files and returns them concatenated with path headers.

    Total output is capped at ~2x max_chunk_bytes as a safety margin; anything
    beyond that is truncated with an explicit marker so the worker knows.
    """
    parts = []
    budget = max_chunk_bytes * 2
    used = 0
    for f in chunk["files"]:
        full_path = os.path.join(root_dir, f["path"])
        try:
            with open(full_path, "r", encoding="utf-8", errors="replace") as fh:
                content = fh.read(budget - used + 1)
        except OSError as e:
            parts.append(f"=== Path: {f['path']} ===\n[UNREADABLE: {e}]")
            continue
        if used + len(content) > budget:
            content = content[: max(0, budget - used)] + "\n[TRUNCATED]"
        used += len(content)
        parts.append(f"=== Path: {f['path']} ===\n{content}")
        if used >= budget:
            parts.append("[CHUNK TRUNCATED: byte budget exhausted]")
            break
    return "\n\n".join(parts)
