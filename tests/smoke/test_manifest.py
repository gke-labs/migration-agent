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

"""Smoke test: static validation of the plugin manifests and skills.

Checks that the plugin is structurally importable by an MCP client harness:
  - .claude-plugin/plugin.json parses and has the expected identity fields
  - .mcp.json parses and points at an existing, executable MCP server launcher
  - every skills/*/SKILL.md has parseable YAML frontmatter with name/description
  - the bootstrapping skill is present

Usage: test_manifest.py <repo_root>
"""

import json
import os
import sys

import yaml

FAILURES = []


def fail(msg: str):
    FAILURES.append(msg)
    print(f"  FAIL: {msg}")


def ok(msg: str):
    print(f"  ok: {msg}")


def check_plugin_json(repo_root: str):
    path = os.path.join(repo_root, ".claude-plugin", "plugin.json")
    if not os.path.isfile(path):
        fail(f"missing {path}")
        return
    with open(path) as f:
        data = json.load(f)
    if data.get("name") != "gke-agentic-migration":
        fail(f"plugin.json name is {data.get('name')!r}, expected 'gke-agentic-migration'")
    else:
        ok(f"plugin.json: name={data['name']} version={data.get('version')}")
    if not data.get("version"):
        fail("plugin.json has no version")


def check_mcp_json(repo_root: str):
    path = os.path.join(repo_root, ".mcp.json")
    if not os.path.isfile(path):
        fail(f"missing {path}")
        return
    with open(path) as f:
        data = json.load(f)
    server = data.get("mcpServers", {}).get("migration-dag")
    if not server:
        fail(".mcp.json does not define mcpServers.migration-dag")
        return
    command = server.get("command", "")
    resolved = command.replace("${CLAUDE_PLUGIN_ROOT}", repo_root)
    if not os.path.isfile(resolved):
        fail(f"MCP server command does not exist: {resolved}")
    elif not os.access(resolved, os.X_OK):
        fail(f"MCP server command is not executable: {resolved}")
    else:
        ok(f".mcp.json: migration-dag -> {command}")


def check_skills(repo_root: str):
    skills_dir = os.path.join(repo_root, "skills")
    if not os.path.isdir(skills_dir):
        fail("skills/ directory missing")
        return
    names = sorted(os.listdir(skills_dir))
    seen = []
    for entry in names:
        skill_md = os.path.join(skills_dir, entry, "SKILL.md")
        if not os.path.isfile(skill_md):
            continue
        with open(skill_md) as f:
            content = f.read()
        if not content.startswith("---"):
            fail(f"skills/{entry}/SKILL.md has no YAML frontmatter")
            continue
        try:
            frontmatter = yaml.safe_load(content.split("---")[1])
        except yaml.YAMLError as e:
            fail(f"skills/{entry}/SKILL.md frontmatter does not parse: {e}")
            continue
        if not frontmatter.get("name") or not frontmatter.get("description"):
            fail(f"skills/{entry}/SKILL.md frontmatter missing name/description")
            continue
        seen.append(entry)
    ok(f"{len(seen)} skills with valid frontmatter: {', '.join(seen)}")
    if "bootstrapping" not in seen:
        fail("bootstrapping skill not found or invalid")


def main():
    if len(sys.argv) != 2:
        print("usage: test_manifest.py <repo_root>", file=sys.stderr)
        return 2
    repo_root = os.path.abspath(sys.argv[1])
    print(f"[test_manifest] repo={repo_root}")
    check_plugin_json(repo_root)
    check_mcp_json(repo_root)
    check_skills(repo_root)
    if FAILURES:
        print(f"[test_manifest] FAILED ({len(FAILURES)} problem(s))")
        return 1
    print("[test_manifest] PASSED")
    return 0


if __name__ == "__main__":
    sys.exit(main())
