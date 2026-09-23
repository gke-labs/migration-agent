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

"""Local filesystem scanning for the discovery collect step.

index_configuration_files builds a lightweight manifest (paths, sizes, kinds)
that never loads file contents into anyone's context; the extraction step
reads contents chunk-by-chunk later. find_configuration_files is the legacy
concatenating scanner, kept for ad-hoc tooling.
"""

import os

CONFIG_EXTENSIONS = ('.tf', '.tfvars', '.yaml', '.yml', '.json')

# Directories holding vendored or generated copies of configuration rather than
# the estate's own source. Scanning them double-counts resources: a vendored
# terraform-aws-modules/eks under .terraform/ declares its own aws_eks_cluster,
# which would land in the inventory as a cluster the user does not operate.
SKIP_DIRS = frozenset({'.git', '.terraform', 'node_modules', 'vendor', '__pycache__', '.venv'})
SKIP_FILES = {'package-lock.json', 'yarn.lock', '.terraform.lock.hcl'}

# Suffixes the legacy concatenating scanner (find_configuration_files) returns.
CONFIG_SUFFIXES = ('.tf', '.yaml', '.yml')


def validate_explicit_root(root_dir: str) -> str:
    """Normalizes an agent-supplied scan root, refusing dangerous ones.

    The MCP server's working directory is not the agent's, so a relative path
    like "." would index whatever directory the server happens to run in.
    Roots that would sweep far more than an IaC checkout (the filesystem
    root, the home directory, or an ancestor of it) are refused outright.
    Returns the resolved absolute path; raises ValueError otherwise.
    """
    expanded = os.path.expanduser(root_dir or "")
    if not os.path.isabs(expanded):
        raise ValueError(
            f"root_dir must be an absolute path, got '{root_dir}'. Relative "
            "paths resolve against the MCP server's working directory, not "
            "yours. Omit root_dir to let the server clone the source "
            "repository configured during onboarding."
        )
    path = os.path.realpath(expanded)
    home = os.path.realpath(os.path.expanduser("~"))
    if path == os.sep or path == home or home.startswith(path + os.sep):
        raise ValueError(
            f"Refusing to index '{root_dir}': it is the filesystem root, a "
            "home directory, or an ancestor of one. Point root_dir at a "
            "repository checkout, or omit it to let the server clone the "
            "configured source repository."
        )
    return path


def classify_kind(path: str) -> str:
    """Best-effort classification of a configuration file."""
    lower = path.lower()
    if lower.endswith(('.tf', '.tfvars')):
        return 'terraform'
    if os.path.basename(lower) in ('chart.yaml', 'values.yaml') or '/charts/' in lower.replace(os.sep, '/'):
        return 'helm'
    if lower.endswith(('.yaml', '.yml')):
        return 'k8s-manifest'
    if lower.endswith('.json'):
        return 'json-config'
    return 'other'


def index_configuration_files(root_dir: str, max_file_bytes: int = 1_000_000) -> dict:
    """Walks root_dir and returns a manifest of configuration files.

    Returns {"root_dir", "files": [{"path", "size", "kind"}], "skipped": [...],
    "total_bytes"} where "path" is relative to root_dir. Contents are never
    read here.
    """
    if not os.path.exists(root_dir) or not os.path.isdir(root_dir):
        raise ValueError(f"Directory '{root_dir}' does not exist or is not a directory.")

    files = []
    skipped = []
    total_bytes = 0

    for root, dirs, names in os.walk(root_dir):
        dirs[:] = sorted(d for d in dirs if d not in SKIP_DIRS)
        for name in sorted(names):
            if not name.endswith(CONFIG_EXTENSIONS) or name in SKIP_FILES:
                continue
            full_path = os.path.join(root, name)
            rel_path = os.path.relpath(full_path, root_dir)
            try:
                size = os.path.getsize(full_path)
            except OSError as e:
                skipped.append({"path": rel_path, "reason": f"unreadable: {e}"})
                continue
            if size > max_file_bytes:
                skipped.append({"path": rel_path, "reason": f"too large ({size} bytes)"})
                continue
            files.append({"path": rel_path, "size": size, "kind": classify_kind(rel_path)})
            total_bytes += size

    return {
        "root_dir": os.path.abspath(root_dir),
        "files": files,
        "skipped": skipped,
        "total_bytes": total_bytes,
    }


def find_configuration_files(root_dir: str) -> str:
    """
    Recursively scans the directory to find all .tf, .yaml, or .yml files.
    Returns concatenated contents with '=== Path: <path> ===' headers.

    The whole tree is returned. Discovery builds a model of the estate from this
    text and the inventory records no coverage marker, so anything withheld here
    is silently missing from the inventory rather than reported as unscanned.

    Legacy helper: superseded by index_configuration_files + chunked extraction,
    retained for ad-hoc use.
    """
    if not os.path.exists(root_dir) or not os.path.isdir(root_dir):
        return f"ERROR: Directory '{root_dir}' does not exist or is not a directory."

    found_contents = []

    for root, dirs, files in os.walk(root_dir):
        dirs[:] = sorted(d for d in dirs if d not in SKIP_DIRS)
        for f in sorted(files):
            if not f.endswith(CONFIG_SUFFIXES):
                continue
            full_path = os.path.join(root, f)
            try:
                with open(full_path, "r", encoding="utf-8") as file:
                    content = file.read()
                found_contents.append(f"=== Path: {full_path} ===\n{content}")
            except Exception as e:
                found_contents.append(f"=== Path: {full_path} ===\nERROR READING FILE: {e}")

    if not found_contents:
        return "No .tf or .yaml/.yml files found."

    return "\n\n".join(found_contents)
