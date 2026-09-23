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

"""The workload-identity unit's ksa_annotations output contract.

The exports gsa_bindings derivation (DESIGN §4.6) parses this one output
block out of the unit's Terraform, so its shape is machine-validated here at
the same gate that checks everything else the unit ships: exactly one
`output "ksa_annotations"` whose value is a literal map from
"namespace/ksa-name" to a GSA email — quoted strings on both sides, no
references, no interpolation, no expressions.

This is deliberately a narrow parser for exactly that block, not a wider
HCL check: the existing brace-depth check stays as shallow as it is, and
anything this contract does not promise stays unparsed.
"""

import re

OUTPUT_NAME = "ksa_annotations"
WI_UNIT_KIND = "workload-identity"

_OUTPUT_RE = re.compile(r'output\s+"' + OUTPUT_NAME + r'"\s*\{')
# Anchored to an attribute position so a description string containing
# "value = ..." prose cannot be mistaken for the value attribute.
_VALUE_RE = re.compile(r'(?m)^[ \t]*value[ \t]*=[ \t]*')
_MAP_ENTRY_RE = re.compile(r'"(?P<key>[^"]*)"\s*=\s*"(?P<value>[^"]*)"')
# The real user-managed GSA email grammar: both the account id and the
# project id are lowercase [a-z]([a-z0-9-]*[a-z0-9]), 6-30 chars. Anything
# else — uppercase placeholder tokens (PROJECT_ID), angle brackets,
# underscores — is a stand-in, not an email, and fails the contract (a
# literal "...@PROJECT_ID.iam.gserviceaccount.com" shipped in an early
# end-to-end run; a consumer would have injected it into manifests).
_EMAIL_RE = re.compile(
    r"^[a-z][a-z0-9-]{4,28}[a-z0-9]@"
    r"[a-z][a-z0-9-]{4,28}[a-z0-9]\.iam\.gserviceaccount\.com$")
_HEREDOC_RE = re.compile(r'<<-?\s*"?(?P<tag>[A-Za-z_][A-Za-z0-9_]*)"?')
# The KSA-resource ban (DESIGN §14 issue 18). Deliberately textual, like the
# rest of this module: a `kind: ServiceAccount` line in a YAML stream and a
# kubernetes ServiceAccount resource block in HCL, plus the kubernetes_manifest
# escape hatch. A false positive here is a reviewer reading one file; the false
# negative is two owners shipping the same object.
_YAML_KSA_RE = re.compile(
    r'(?m)^\s*kind\s*:\s*["\']?ServiceAccount["\']?\s*(?:#.*)?$')
_TF_KSA_RE = re.compile(
    r'resource\s+"kubernetes_service_account(?:_v1)?"\s+"[^"]*"')
_TF_MANIFEST_RE = re.compile(r'resource\s+"kubernetes_manifest"\s+"[^"]*"\s*\{')
_MANIFEST_KSA_RE = re.compile(
    r'"?kind"?\s*[=:]\s*"ServiceAccount"')


def _strip_line_comment(line: str) -> str:
    """Removes a #-or-// comment from one line, quote-aware."""
    in_string = False
    i = 0
    while i < len(line):
        char = line[i]
        if in_string:
            if char == "\\":
                i += 2
                continue
            if char == '"':
                in_string = False
        elif char == '"':
            in_string = True
        elif char == "#" or line.startswith("//", i):
            return line[:i]
        i += 1
    return line


def _heredoc_opener(line: str):
    """(tag, index) of a heredoc opener OUTSIDE string literals, else None.

    Same quote-awareness as the comment scan, one level up: a "<<word"
    inside a quoted string (a kubectl one-liner, a doc string) must not
    open phantom heredoc mode and blank the rest of the file.
    """
    in_string = False
    i = 0
    while i < len(line) - 1:
        char = line[i]
        if in_string:
            if char == "\\":
                i += 2
                continue
            if char == '"':
                in_string = False
        elif char == '"':
            in_string = True
        elif char == "<" and line[i + 1] == "<":
            match = _HEREDOC_RE.match(line, i)
            if match:
                return match.group("tag"), i
        i += 1
    return None


def _strip_tf_comments(text: str) -> str:
    """Blanks comments and heredoc bodies before any scanning.

    Workers routinely leave commented-out drafts, quote the contract's
    example block, or embed text in heredocs; none of those are
    declarations, and a lone brace inside one must not derail the scan.
    """
    lines = []
    heredoc_end = None
    for line in text.splitlines():
        if heredoc_end is not None:
            if line.strip() == heredoc_end:
                heredoc_end = None
            lines.append("")
            continue
        stripped = _strip_line_comment(line)
        opener = _heredoc_opener(stripped)
        if opener is not None:
            heredoc_end, start = opener[0], opener[1]
            lines.append(stripped[:start])
            continue
        lines.append(stripped)
    return "\n".join(lines)


def _scan_braces(text: str, open_index: int):
    """Returns the index just past the brace block opened at open_index, or
    None if it never closes. Quote-aware, so a '{' inside a string does not
    change the depth."""
    depth = 0
    in_string = False
    i = open_index
    while i < len(text):
        char = text[i]
        if in_string:
            if char == "\\":
                i += 2
                continue
            if char == '"':
                in_string = False
        elif char == '"':
            in_string = True
        elif char == "{":
            depth += 1
        elif char == "}":
            depth -= 1
            if depth == 0:
                return i + 1
        i += 1
    return None


def _malformed(error: str, file: str = None) -> dict:
    return {"status": "malformed", "bindings": None, "error": error, "file": file}


def parse_ksa_annotations(tf_files: dict) -> dict:
    """Finds and parses the contract output across a unit's .tf files.

    tf_files: {path: content}. Returns {"status": "ok" | "absent" |
    "malformed", "bindings": {"ns/sa": "email"} | None, "error": str | None,
    "file": the declaring file | None}.
    """
    found = []  # (file, block_body)
    for path in sorted(tf_files):
        content = _strip_tf_comments(tf_files[path] or "")
        for match in _OUTPUT_RE.finditer(content):
            open_index = content.index("{", match.start())
            end = _scan_braces(content, open_index)
            if end is None:
                return _malformed(f'output "{OUTPUT_NAME}" block never closes', path)
            found.append((path, content[open_index + 1:end - 1]))
    if not found:
        return {"status": "absent", "bindings": None, "error": None, "file": None}
    if len(found) > 1:
        files = ", ".join(sorted({f for f, _ in found}))
        return _malformed(f'output "{OUTPUT_NAME}" is declared {len(found)} times ({files})')

    path, body = found[0]
    value_match = _VALUE_RE.search(body)
    if not value_match:
        return _malformed(f'output "{OUTPUT_NAME}" has no value attribute', path)
    rest = body[value_match.end():].lstrip()
    if not rest.startswith("{"):
        snippet = rest.splitlines()[0][:80] if rest else ""
        return _malformed(
            f'output "{OUTPUT_NAME}" value is not a literal map (starts with {snippet!r})', path)
    map_end = _scan_braces(rest, 0)
    if map_end is None:
        return _malformed(f'output "{OUTPUT_NAME}" value map never closes', path)
    return _parse_map_body(rest[1:map_end - 1], path)


def _parse_map_body(body: str, path: str) -> dict:
    """Literal '"ns/sa" = "email"' pairs, nothing else.

    Comments were stripped before this ran, and a pair may wrap across
    lines; anything left over that is not a quoted pair, whitespace, or a
    comma — a reference, a for-expression, a function call — is malformed.
    """
    residue = _MAP_ENTRY_RE.sub("", body)
    leftover = re.search(r"[^\s,]+", residue)
    if leftover:
        return _malformed(
            f"value map entry is not a literal \"namespace/ksa-name\" = "
            f"\"<gsa email>\" pair (near {leftover.group(0)[:80]!r})", path)
    bindings = {}
    for entry in _MAP_ENTRY_RE.finditer(body):
        key, value = entry.group("key"), entry.group("value")
        if "${" in key or "${" in value:
            return _malformed(f"interpolation is not literal: {key!r} = {value!r}", path)
        parts = key.split("/")
        if len(parts) != 2 or not all(parts):
            return _malformed(
                f'map key {key!r} is not "namespace/ksa-name"', path)
        if not _EMAIL_RE.match(value):
            return _malformed(
                f"map value {value!r} for key {key!r} is not a real "
                '"<account-id>@<project-id>.iam.gserviceaccount.com" email '
                "(lowercase 6-30 char ids; placeholder tokens like "
                "PROJECT_ID are rejected)", path)
        if key in bindings:
            return _malformed(f"map key {key!r} is declared twice", path)
        bindings[key] = value
    return {"status": "ok", "bindings": bindings, "error": None, "file": path}


def _empty_map_error(unit: dict) -> str:
    """Names which half of the empty-map escape failed: the pipeline never
    supplying a target project is a different defect from a worker declining
    a project it was given, and review needs to know which it is fixing."""
    inputs = (unit or {}).get("inputs") or {}
    count = len(inputs.get("irsa_bindings") or [])
    project = inputs.get("target_project")
    if project:
        return (f'output "{OUTPUT_NAME}" is an empty map while the unit\'s '
                f"inputs record {count} IRSA binding(s) and target project "
                f"'{project}' — the empty-map escape is only for a genuinely "
                "undecidable project; write the literal GSA emails")
    return (f'output "{OUTPUT_NAME}" is an empty map while the unit\'s '
            f"inputs record {count} IRSA binding(s) — no target project was "
            "supplied to the unit, so every one of those ServiceAccounts "
            "would ship with no Google identity; request_unit_revision "
            "naming the target GCP project (and the literal GSA emails it "
            "implies), or skip the unit explicitly")


def _unit_files(unit_entry: dict, suffixes: tuple) -> dict:
    """{path: content} of one unit blob's files with these suffixes."""
    files = {}
    for f in ((unit_entry or {}).get("result") or {}).get("files") or []:
        if not isinstance(f, dict):
            continue
        file_path = str(f.get("path") or "")
        if file_path.endswith(suffixes):
            files[file_path] = str(f.get("content") or "")
    return files


def ksa_resource_findings(unit_entry: dict) -> list:
    """[error string] for every KSA RESOURCE the workload-identity unit ships.

    The shed (DESIGN §14 issue 18) is a brief instruction, and the platform
    translator's own rules tell workers to emit in-cluster objects — including
    ServiceAccount — as Kubernetes YAML, so nothing stopped a worker from
    re-emitting the KSA the developer phase's wkld-identity unit owns. Two
    claimants for one object is exactly the straddle the shed removed, so the
    absence is machine-checked here, next to the output-half contract.

    Both emission routes are covered: a Kubernetes YAML document, and the
    Terraform ones (kubernetes_service_account[_v1] and a kubernetes_manifest
    carrying kind = "ServiceAccount").
    """
    findings = []
    for path, content in sorted(_unit_files(
            unit_entry, (".yaml", ".yml")).items()):
        if _YAML_KSA_RE.search(content or ""):
            findings.append(
                f"{path}: ships a Kubernetes ServiceAccount document. The "
                "KSA belongs to the developer phase's wkld-identity unit "
                "(coverage map, k8s/workload row); this unit ships the GSA "
                "and IAM bindings plus the ksa_annotations output only.")
    for path, content in sorted(_unit_files(unit_entry, (".tf",)).items()):
        stripped = _strip_tf_comments(content or "")
        for match in _TF_KSA_RE.finditer(stripped):
            findings.append(
                f"{path}: declares {match.group(0).strip()} — the KSA "
                "belongs to the developer phase's wkld-identity unit; this "
                "unit ships the GSA, the IAM bindings and the "
                "ksa_annotations output only.")
        for match in _TF_MANIFEST_RE.finditer(stripped):
            end = _scan_braces(stripped, stripped.index("{", match.start()))
            body = stripped[match.start():end if end else len(stripped)]
            if _MANIFEST_KSA_RE.search(body):
                findings.append(
                    f"{path}: a kubernetes_manifest resource carries kind "
                    '"ServiceAccount" — the KSA belongs to the developer '
                    "phase's wkld-identity unit.")
    return findings


def unit_tf_files(unit_entry: dict) -> dict:
    """{path: content} of the .tf files in one persisted unit blob.

    Tolerates legacy blob shapes (no result, no files list): missing pieces
    yield an empty dict, which parse_ksa_annotations reports as absent."""
    files = {}
    for f in ((unit_entry or {}).get("result") or {}).get("files") or []:
        if not isinstance(f, dict):
            continue
        file_path = str(f.get("path") or "")
        if file_path.endswith(".tf"):
            files[file_path] = str(f.get("content") or "")
    return files


def check_units(done_units: list) -> dict:
    """Contract check over the done unit blobs the validate step ships.

    Only workload-identity units are checked. Both halves of the unit's
    contract are checked here: the ksa_annotations output it MUST declare,
    and the KSA resource it must NOT ship (the shed of DESIGN §14 issue 18 —
    otherwise a brief-level instruction with nothing enforcing it).

    Returns {"checked": [unit_id], "findings": [{"unit_id", "error"}]} — an
    absent output on a legacy blob is a finding like any other, never a crash.
    An empty map while the unit's inputs record IRSA bindings is a finding
    too: the undecided-project escape stays the worker's honest move (never
    invent an email), but it must reach review loudly — shipped silently,
    every one of those ServiceAccounts lands on GKE with no Google identity
    and the exports gsa_bindings channel publishes null.
    """
    report = {"checked": [], "findings": []}
    for entry in done_units or []:
        unit = (entry or {}).get("unit") or {}
        if unit.get("kind") != WI_UNIT_KIND:
            continue
        unit_id = str(unit.get("unit_id") or "workload-identity")
        report["checked"].append(unit_id)
        for error in ksa_resource_findings(entry):
            report["findings"].append({"unit_id": unit_id, "error": error})
        result = parse_ksa_annotations(unit_tf_files(entry))
        if result["status"] == "absent":
            report["findings"].append({
                "unit_id": unit_id,
                "error": (f'no output "{OUTPUT_NAME}" in the unit\'s .tf files '
                          "(a legacy unit predating the contract, or the worker "
                          "dropped it); the exports gsa_bindings derivation "
                          "cannot read this unit"),
            })
        elif result["status"] == "malformed":
            report["findings"].append({"unit_id": unit_id, "error": result["error"]})
        elif result["bindings"] == {} and (unit.get("inputs") or {}).get("irsa_bindings"):
            report["findings"].append(
                {"unit_id": unit_id, "error": _empty_map_error(unit)})
    return report
