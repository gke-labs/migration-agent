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

"""Cluster DNS configuration harvest for discovery.

Pure logic — no GCS, no subprocesses, no LLM. Locates every CoreDNS artifact
in the source checkout and records its text **verbatim**, with evidence, in
`inventory["cluster_dns"]`. It never interprets a Corefile. The translation
worker reads the recorded text against the cluster DNS knowledge document
and writes the GKE side; the validate gate checks what it wrote. That split
is the point: a CoreDNS plugin this file has never heard of arrives as text
and needs no code change here.

What counts as a source is what carries a Corefile or the add-on settings
that wrap one: the managed add-on's `configuration_values` (on the
`aws_eks_addon` resource or inside an EKS module's `cluster_addons` /
`addons` / `eks_addons` map), a
`coredns` or `node-local-dns` ConfigMap in `kube-system` (as a Terraform
resource or as plain YAML). Everything else this walk notices — a managed add-on left at its
defaults, a Corefile behind a variable the files do not resolve, a Helm chart
whose values are not read, a CoreDNS Deployment with pod settings — is a
scan note, not a source. The section stays `{}` unless a source carries
text, because a recorded source object makes the coverage row facts-present
and plans a unit, and a unit with nothing to emit cannot pass the worker
contract. "Text" is any configuration the worker has to answer for: a
Corefile, or add-on settings such as `replicaCount` without one — those
translate too, into a dropped-with-tradeoff line, which is an emitted
answer. What does not count is an add-on with no configuration at all.

The Terraform lexer is `datastores.py`'s (`scan_source`,
`iter_terraform_blocks`); the manifest loader is `k8s_manifests`'. Text is
read from the ORIGINAL content by offset — the lexer's `text` view blanks
heredoc bodies, which is exactly the part a Corefile lives in.
"""

import os
import re
from typing import NamedTuple

from servers.phases import k8s_manifests
from servers.phases.scope_algebra import is_excluded

from . import consumers, datastores
from .files import SKIP_DIRS, SKIP_FILES

TF_EXTENSIONS = (".tf",)
YAML_EXTENSIONS = (".yaml", ".yml")

# A Corefile is a few hundred bytes; the add-on's whole configuration a few
# KB. The caps keep one pathological file from turning the inventory — which
# rides into every later agent context — into a dump.
MAX_TEXT_BYTES = 16 * 1024
MAX_SECTION_BYTES = 64 * 1024

ADDON_NAME = "coredns"
CONFIGMAP_NAMES = ("coredns", "node-local-dns")
# `kubernetes_config_map_v1_data` is the resource that customizes a ConfigMap the
# add-on owns (`force = true` over the existing object) — on EKS that is the
# standard way to edit the coredns Corefile from Terraform.
CONFIGMAP_RESOURCE_TYPES = ("kubernetes_config_map", "kubernetes_config_map_v1",
                            "kubernetes_config_map_v1_data")
HELM_CHARTS = ("coredns", "node-local-dns", "node-local-dns-cache")
KUBE_SYSTEM = "kube-system"

# What is and is not parsed, persisted with the section for the same reason
# datastores.py persists its own: an empty section with no record of why is
# indistinguishable from an estate that never touched its cluster DNS.
COVERAGE_NOTE = (
    "cluster_dns covers Terraform .tf files and plain Kubernetes YAML only — "
    "Helm chart templates, Kustomize generators and patches, .tf.json, "
    "CloudFormation, CDK and eksctl are not parsed, so an empty section for "
    "one of those means not scanned, not absent.")

# A key at the left of `=`, bare or quoted: `data = {` and `"Corefile" = <<EOT`
# both occur in real kubernetes_config_map declarations.
# Matched with `pattern.match(text, pos)`, which anchors at `pos` on its own —
# a `^` would only match at the real start of the string.
_KEY_ASSIGN_RE = re.compile(
    r'[ \t]*"?([A-Za-z_][A-Za-z0-9_.-]*)"?[ \t]*=(?![=>])[ \t]*')
# A reference the scan does not follow: a variable, a local, another module,
# a data source, a for_each value, or a call that reads a file. Matched in the
# MASK view of an expression — strings, comments and heredoc bodies blanked —
# so a hostname such as `data.corp.example.com` inside a Corefile cannot trip
# it. `jsonencode({...})` and `yamlencode({...})` are NOT references: their
# object literal is text the worker can read as well as JSON, and is recorded
# raw. (Inside a `"${...}"` template the mask is blank, so the same pattern
# runs over the text there — the one place a hostname could be mistaken for a
# reference, and the cost is a conservative "unread" note.)
_REFERENCE_RE = re.compile(
    r'(?<![\w.])(?:var|local|module|data|each|count|path)\.[A-Za-z_][\w.-]*|'
    r'\b(?:file|templatefile|filebase64)\s*\(')
# The keys a module-form add-on map is written under: `cluster_addons` in
# terraform-aws-modules/eks up to v20, `addons` from v21, `eks_addons` in
# aws-ia/eks-blueprints-addons.
MODULE_ADDON_ARGS = ("cluster_addons", "addons", "eks_addons")


# A `namespace =` line anywhere in a resource body: when the metadata reader
# returned no literal namespace and this matches, the namespace is an
# expression rather than absent.
_NAMESPACE_DECLARED_RE = re.compile(r"^[ \t]*namespace[ \t]*=", re.MULTILINE)


class _Value(NamedTuple):
    """One right-hand side, read. `text` is None when it was not recorded and
    `unread` then says what it is read from; `references` lists what a
    recorded text still reaches for outside the file."""
    text: str | None
    form: str
    unread: str | None
    references: list


class HarvestResult(NamedTuple):
    section: dict
    notes: list


def _line_of(content: str, offset: int) -> int:
    return content.count("\n", 0, offset) + 1


def _line_end(content: str, at: int) -> int:
    found = content.find("\n", at)
    return len(content) if found == -1 else found


_ESCAPES = {"n": "\n", "t": "\t", "r": "\r"}
_ESCAPE_RE = re.compile(r"\\(u[0-9A-Fa-f]{4}|.)", re.DOTALL)


def _unescape(raw: str) -> str:
    """The escapes a quoted HCL string can carry, decoded in one pass.

    One pass, because `\\\\n` is a backslash followed by an `n` — the JSON
    newline escape inside a quoted configuration_values — and sequential
    replacements would turn it into a real newline. `$${` is left alone
    here: `_template_value` has to see it before deciding what is an
    interpolation, and turns it into `${` on the way out.
    """
    def one(match):
        esc = match.group(1)
        if len(esc) == 5 and esc[0] == "u":
            return chr(int(esc[1:], 16))
        return _ESCAPES.get(esc, esc)
    return _ESCAPE_RE.sub(one, raw)


def _dedent(body: str) -> str:
    """What Terraform does to a `<<-` body: strip the common leading space."""
    lines = body.split("\n")
    indents = [len(line) - len(line.lstrip(" \t"))
               for line in lines if line.strip()]
    cut = min(indents) if indents else 0
    return "\n".join(line[cut:] if line.strip() else line.lstrip(" \t")
                     for line in lines)


def _expression_end(mask: str, start: int) -> int:
    """Offset just past a possibly multi-line expression starting at `start`.

    Counts brackets in the mask, so a brace inside a string or a heredoc body
    is text; ends at the first newline reached with every bracket closed.
    """
    depth = 0
    for i in range(start, len(mask)):
        ch = mask[i]
        if ch in "([{":
            depth += 1
        elif ch in ")]}":
            if depth == 0:
                return i          # the closer of whatever encloses the value
            depth -= 1
        elif ch in ",\n" and depth == 0:
            return i              # next entry of an object, or end of line
    return len(mask)


def _find_assignment(text: str, mask: str, start: int, stop: int,
                     names: tuple) -> tuple | None:
    """First `name = ...` at the span's OWN depth. Returns (name, rhs_offset).

    Depth is counted in the mask, line by line, the way `_top_level_args`
    does it, so a `name` inside a nested `metadata { }` or a `labels = { }`
    map is not mistaken for the block's own argument. The right-hand side
    offset indexes the original content.
    """
    depth = 0
    at = start
    while at < stop:
        end = text.find("\n", at, stop)
        if end == -1:
            end = stop
        if depth == 0:
            for seg in _entry_starts(mask, at, end):
                match = _KEY_ASSIGN_RE.match(text, seg, end)
                if match and match.group(1) in names:
                    return match.group(1), match.end()
        line_mask = mask[at:end]
        depth = max(depth + line_mask.count("{") - line_mask.count("}"), 0)
        at = end + 1
    return None


def _entry_starts(mask: str, start: int, stop: int) -> list:
    """Where entries begin on one line: its start, and after every comma at
    the line's own depth — `data = { other = "y", Corefile = "x" }` carries
    two keys on one line, and the second is the one that matters."""
    starts, depth = [start], 0
    for i in range(start, stop):
        ch = mask[i]
        if ch in "([{":
            depth += 1
        elif ch in ")]}":
            depth -= 1
        elif ch == "," and depth == 0:
            starts.append(i + 1)
    return starts


def _template_end(mask: str, rhs: int) -> int:
    """Offset just past a quoted string that spans lines.

    The lexer blanks a string in the mask, closing quote included, so the
    string ends where the blank run does — at the next structural character
    (the next key, or the block's closing brace).
    """
    i = rhs + 1
    while i < len(mask) and mask[i] in " \t\r\n":
        i += 1
    return i


def _object_span(mask: str, start: int, stop: int) -> tuple | None:
    """The `{ ... }` of the first object literal in the span: (open + 1, close)."""
    open_at = mask.find("{", start, stop)
    if open_at == -1:
        return None
    close_at = datastores._block_body(mask, open_at)
    return open_at + 1, min(close_at, stop)


def _carries_literal(text: str, mask: str, start: int, stop: int) -> bool:
    """Whether an expression holds any text of its own: an object literal
    (a `{` in the mask) or a heredoc opener. A quoted string does not count
    here — a plain string value never reaches this check, and a string inside
    an expression is an index or an argument (`data["Corefile"]`,
    `file("x")`), not the text."""
    return ("{" in mask[start:stop]
            or datastores._HEREDOC_RE.search(text, start, stop) is not None)


def _references(content: str, mask: str, start: int, stop: int) -> list:
    """The references the mask view of [start, stop) contains, as written."""
    found = []
    for match in _REFERENCE_RE.finditer(mask, start, stop):
        if match.group(0).rstrip().endswith("("):
            close = datastores._block_body(
                mask.replace("(", "{").replace(")", "}"), match.end() - 1)
            found.append(content[match.start():min(close + 1, stop)])
        else:
            full = _REFERENCE_RE.match(content, match.start())
            found.append(full.group(0) if full else match.group(0))
    return list(dict.fromkeys(found))


def _template_value(raw: str) -> _Value:
    """The content of a quoted string, still escaped: plain text, or a template.

    `"${jsonencode({...})}"` — the TF 0.11 wrapper — is one interpolation
    around an expression; the expression is what gets recorded, and it is
    lexed from the raw characters so a nested `"a\\nb"` keeps its escape as
    written. A plain string is unescaped on the way out. Any other use of
    `${` is an interpolated string the scan cannot resolve.
    """
    body = raw.replace("$${", "").replace("%%{", "")
    if "${" not in body and "%{" not in body:
        return _Value(_unescape(raw).replace("$${", "${").replace("%%{", "%{"),
                      "string", None, [])
    stripped = raw.strip()
    spans = datastores.interpolation_spans(stripped, 0, len(stripped))
    if spans and spans[0] == (0, len(stripped)):
        # One interpolation wrapping the whole value — nested quotes and all.
        # The inner text is Terraform, so it is lexed on its own and judged
        # by the expression rules: a literal must be present, references
        # are found in the mask, the corefile key decides.
        inner = stripped[2:-1].strip()
        text, mask, _notes, heredocs = datastores.scan_source(inner)
        return _expression_value(inner, text, mask, 0, heredocs)
    refs = list(dict.fromkeys(m.group(0) for m in _REFERENCE_RE.finditer(stripped)))
    what = "an interpolated string"
    if refs:
        what += f" ({', '.join(refs)})"
    return _Value(None, "string", what, [])


def _read_value(content: str, text: str, mask: str, rhs: int,
                heredocs: list) -> _Value:
    """The right-hand side at `rhs`.

    Forms: `string`, `heredoc`, `expression`. A value is recorded only when
    the text it carries is in this file: a heredoc (its `${...}`
    interpolations are listed as references, the body is still the record),
    a plain string, or an expression whose object literal is readable. An
    expression that reaches for a variable, a local, another module or a
    file is not recorded — unless it also carries a readable `corefile`
    key, in which case the whole expression is kept and the references are
    reported beside it, because a Corefile written next to
    `replicaCount = var.replicas` is still a Corefile.
    """
    line_end = _line_end(content, rhs)

    heredoc = datastores._HEREDOC_RE.match(content, rhs)
    if heredoc:
        body_start = line_end + 1
        for start, stop in heredocs:
            if start == body_start:
                body = content[start:stop]
                refs = [content[a:b] for a, b in
                        datastores.interpolation_spans(content, start, stop)]
                if content.startswith("<<-", rhs):
                    body = _dedent(body)
                # What the cluster saw: Terraform emits `${` for `$${`.
                return _Value(body.replace("$${", "${").replace("%%{", "%{"),
                              "heredoc", None, list(dict.fromkeys(refs)))
        return _Value(None, "heredoc", "a heredoc that is never closed", [])

    string = datastores._STRING_RE.match(text[rhs:line_end].strip())
    if string:
        return _template_value(string.group(1))
    if content.startswith('"', rhs):
        # A quoted template spanning lines: the text view blanks its
        # continuation, so the single-line match above cannot see its end.
        raw = content[rhs:_template_end(mask, rhs)].rstrip()
        if raw.count('"') >= 2:
            raw = raw[:raw.rindex('"') + 1]
            return _template_value(raw[1:-1])
        return _Value(None, "string", "a quoted string that is never closed", [])

    return _expression_value(content, text, mask, rhs, heredocs)


def _expression_value(content: str, text: str, mask: str, rhs: int,
                      heredocs: list) -> _Value:
    """The expression branch of `_read_value`, shared with the `"${...}"`
    wrapper (which lexes its inner text and comes through here)."""
    end = _expression_end(mask, rhs)
    raw = content[rhs:end].rstrip()
    refs = _references(content, mask, rhs, end)
    if not _carries_literal(text, mask, rhs, end):
        # `aws_ssm_parameter.coredns.value`, `terraform.workspace`, a bare call:
        # an expression with no object literal, no string and no heredoc in
        # it holds no text of its own, whatever prefix it starts with.
        return _Value(None, "expression", raw, refs or [raw])
    for start, stop in heredocs:
        if rhs <= start < end:
            refs.extend(content[a:b] for a, b in
                        datastores.interpolation_spans(content, start, stop))
    refs = list(dict.fromkeys(refs))
    # The `corefile` key decides: an object literal whose Corefile is itself
    # an attribute of some other resource carries no Corefile, whatever
    # prefix that attribute starts with — so the key's own value is read
    # with the same rules before the object is judged.
    corefile = None
    span = _object_span(mask, rhs, end)
    if span is not None:
        inner = _find_assignment(text, mask, span[0], span[1], ("corefile", "Corefile"))
        if inner is not None:
            corefile = _read_value(content, text, mask, inner[1], heredocs)
            if corefile.text is None:
                return _Value(None, "expression", corefile.unread,
                              refs or [corefile.unread])
    if not refs:
        return _Value(raw, "expression", None, [])
    if corefile is not None and not any(
            r for r in corefile.references if not r.startswith(("${", "%{"))):
        return _Value(raw, "expression", None, refs)
    return _Value(None, "expression", ", ".join(refs), refs)


def _fit(text: str, label: str, notes: list) -> str:
    if len(text.encode("utf-8")) <= MAX_TEXT_BYTES:
        return text
    cut = text.encode("utf-8")[:MAX_TEXT_BYTES].decode("utf-8", errors="ignore")
    notes.append(f"{label}: text longer than {MAX_TEXT_BYTES} bytes was "
                 "truncated; the rest was not recorded")
    return cut


def _values_of(declared: dict, name: str) -> str:
    raw = declared.get(name)
    return f", values: {raw}" if isinstance(raw, str) else ""


def walk_in_scope(root_dir: str, scope: dict, extensions: tuple, notes: list,
                  chart_roots: list = (), purpose: str = "cluster DNS configuration",
                  oversized_hint=None, skip_suffixes: tuple = ()):
    """Yields (rel_path, content) for in-scope files with the extensions.

    Same pruning and the same size guard as the datastore walk. Files under a
    Helm chart root are skipped and counted: a template's `{{ }}` would not
    survive the YAML loader, and nothing renders charts for this scan — the
    image scan renders them for image references only, in memory.

    Shared with the address-space harvest (addressspace.py), which walks the
    same checkout under the same scope; `purpose` names the harvest in the
    notes so a reader can tell whose files went unread. `oversized_hint`, a
    compiled pattern, limits the "skipped, larger than" note to files whose
    first 64 KB match it: a walk over every .json in a checkout meets lock
    files and dashboard exports that no reader wants a note about.
    `skip_suffixes` leaves a suffix to another walk: `.tfvars.json` is read
    by the Terraform walk, so the JSON walk must not count it again.
    """
    excluded, under_charts = 0, 0
    roots = tuple(os.path.normpath(r) for r in chart_roots or () if r)
    whole_repo = "." in roots
    for dirpath, dirnames, filenames in os.walk(root_dir):
        dirnames[:] = sorted(d for d in dirnames if d not in SKIP_DIRS)
        for filename in sorted(filenames):
            if not filename.endswith(extensions) or filename in SKIP_FILES:
                continue
            if skip_suffixes and filename.endswith(skip_suffixes):
                continue
            full_path = os.path.join(dirpath, filename)
            rel_path = os.path.relpath(full_path, root_dir)
            if scope and is_excluded(rel_path, scope):
                excluded += 1
                continue
            norm = os.path.normpath(rel_path)
            if whole_repo or any(norm == r or norm.startswith(r + os.sep) for r in roots):
                under_charts += 1
                continue
            try:
                if os.path.getsize(full_path) > datastores.MAX_FILE_BYTES:
                    if oversized_hint is not None:
                        with open(full_path, "r", encoding="utf-8-sig", errors="replace") as f:
                            head = f.read(64 * 1024)
                        if not oversized_hint.search(head):
                            continue
                    notes.append(f"{rel_path}: skipped, larger than "
                                 f"{datastores.MAX_FILE_BYTES} bytes")
                    continue
                with open(full_path, "r", encoding="utf-8-sig", errors="replace") as f:
                    content = f.read()
            except OSError as e:
                notes.append(f"{rel_path}: unreadable ({e.__class__.__name__})")
                continue
            yield rel_path, content
    kind = ("Terraform" if extensions[0].startswith(".tf")
            else "YAML" if extensions[0] in YAML_EXTENSIONS else "JSON")
    if excluded:
        notes.append(f"{excluded} {kind} file(s) the confirmed scope excludes "
                     f"were not read for {purpose}")
    if under_charts:
        notes.append(f"{under_charts} {kind} file(s) under Helm chart roots "
                     f"({', '.join(roots)}) were not read for {purpose}: "
                     "chart templates are not parsed, and no rendered form of "
                     "them is read either")


def _source(kind: str, name: str, rel_path: str, evidence: list, **fields) -> dict:
    directory = os.path.dirname(rel_path) or "."
    entry = {"kind": kind, "name": name, "address": None, "path": None,
             "directory": directory, "form": None, "addon_version": None,
             "resolve_conflicts_on_update": None, "text": None,
             "evidence": list(evidence)}
    entry.update(fields)
    return entry


def _is_empty_configuration(text: str) -> bool:
    """`""`, `"{}"`, `jsonencode({})`: a configuration that configures nothing."""
    bare = re.sub(r"\s+", "", text)
    return bare in ("", "{}", "null", "jsonencode({})", "yamlencode({})")


def _record(sources: list, notes: list, kind: str, name: str, rel_path: str,
            where: str, value: _Value, **fields) -> None:
    """One source from one read value, or the note that stands in for it."""
    label = f"{fields.get('address') or rel_path} ({where})"
    if value.text is not None and _is_empty_configuration(value.text):
        what = ("coredns add-on whose configuration_values is empty"
                if kind in ("eks_addon", "eks_module_addon")
                else f"ConfigMap {name} whose Corefile is empty")
        notes.append(f"{label}: {what} — nothing to carry over")
        return
    if value.text is None:
        notes.append(f"{label}: the {'Corefile' if kind != 'eks_addon' and kind != 'eks_module_addon' else 'coredns configuration'} "
                     f"is read from {value.unread}, which the scan does not follow — "
                     "not recorded; ask for it")
        return
    if value.references:
        notes.append(f"{label}: the recorded text also references "
                     f"{', '.join(value.references)}; the values behind them were "
                     "not recorded")
    sources.append(_source(kind, name, rel_path, [where], form=value.form,
                           text=_fit(value.text, label, notes), **fields))


def _string_or_none(value):
    """The schema types these fields as string-or-null; a numeric or boolean
    literal in their place is nobody's version and must not fail the whole
    inventory write."""
    return value if isinstance(value, str) else None


def _literal_at(content, text, mask, start, stop, heredocs, name):
    """A string-literal argument inside a span, or None."""
    found = _find_assignment(text, mask, start, stop, (name,))
    if found is None:
        return None
    value = _read_value(content, text, mask, found[1], heredocs)
    return value.text if value.form == "string" and value.text is not None else None


def extract_terraform_sources(root_dir: str, scope: dict = None,
                              chart_roots: list = ()) -> tuple[list, list]:
    """CoreDNS sources declared in Terraform.

    Five shapes carry one: the `aws_eks_addon` resource, the same add-on
    declared through an EKS module's `cluster_addons` / `addons` /
    `eks_addons` map, a `kubernetes_config_map` (or `_v1`, `_v1_data`)
    resource, a `kubernetes_manifest` whose inline object is the ConfigMap,
    and a `kubectl_manifest` whose `yaml_body` carries it. Returns
    (sources, notes). A `helm_release` of a CoreDNS chart and an add-on left
    at its defaults are notes, not sources. `chart_roots` is accepted for
    symmetry and unused: the skip is a YAML concern.
    """
    sources, notes = [], []
    # No chart roots here: the skip exists because a template is not YAML,
    # which says nothing about a .tf file that happens to sit under one.
    for rel_path, content in walk_in_scope(root_dir, scope, TF_EXTENSIONS, notes):
        text, mask, lex_notes, heredocs = datastores.scan_source(content)
        for note in lex_notes:
            notes.append(f"{rel_path}: {note}")
        blocks, truncated = datastores.iter_terraform_blocks(
            text, mask, kinds=("resource", "module"))
        if truncated:
            notes.append(f"{rel_path}: a block is never closed, so that block "
                         "and everything after it were not read")
        for kind, first, second, args, declared, body_start, body_end in blocks:
            where = f"{rel_path}:{_line_of(content, body_start)}"
            body_text, body_mask = text[body_start:body_end], mask[body_start:body_end]

            if kind == "module":
                address = f"module.{first}"
                found = _find_assignment(text, mask, body_start, body_end, MODULE_ADDON_ARGS)
                if found is None:
                    continue
                if not content.startswith("{", found[1]):
                    notes.append(
                        f"{address} ({where}): {found[0]} is an expression, so "
                        "whether it configures coredns, and with what, was not read")
                    continue
                map_end = datastores._block_body(mask, found[1])
                entry = _find_assignment(text, mask, found[1] + 1, map_end, (ADDON_NAME,))
                if entry is None:
                    continue
                if not content.startswith("{", entry[1]):
                    notes.append(
                        f"{address} ({where}): {found[0]}.coredns is an expression, "
                        "so its configuration was not read")
                    continue
                entry_end = datastores._block_body(mask, entry[1])
                config = _find_assignment(text, mask, entry[1] + 1, entry_end,
                                          ("configuration_values",))
                if config is None:
                    notes.append(
                        f"{address} ({where}): coredns add-on declared through "
                        f"{found[0]} with no configuration_values — default "
                        "configuration, nothing to carry over")
                    continue
                _record(sources, notes, "eks_module_addon", ADDON_NAME, rel_path, where,
                        _read_value(content, text, mask, config[1], heredocs),
                        address=address,
                        addon_version=_literal_at(content, text, mask, entry[1] + 1,
                                                  entry_end, heredocs, "addon_version"),
                        resolve_conflicts_on_update=_literal_at(
                            content, text, mask, entry[1] + 1, entry_end, heredocs,
                            "resolve_conflicts_on_update"))
                continue

            resource_type = first
            address = f"{resource_type}.{second}"
            if resource_type == "aws_eks_addon":
                addon_name = args.get("addon_name")
                if addon_name != ADDON_NAME:
                    if addon_name is None and "configuration_values" in declared:
                        notes.append(
                            f"{address} ({where}): addon_name is not a literal, so "
                            "whether it deploys coredns, and with what "
                            "configuration, was not read")
                    continue
                found = _find_assignment(text, mask, body_start, body_end,
                                         ("configuration_values",))
                if found is None:
                    notes.append(
                        f"{address} ({where}): managed coredns add-on with no "
                        "configuration_values — default configuration, nothing "
                        "to carry over")
                    continue
                _record(sources, notes, "eks_addon", ADDON_NAME, rel_path, where,
                        _read_value(content, text, mask, found[1], heredocs),
                        address=address, addon_version=_string_or_none(args.get("addon_version")),
                        resolve_conflicts_on_update=_string_or_none(
                            args.get("resolve_conflicts_on_update")))

            elif resource_type in CONFIGMAP_RESOURCE_TYPES:
                meta = consumers._metadata_args(body_text, body_mask)
                if meta.get("name") not in CONFIGMAP_NAMES:
                    continue
                if meta.get("namespace") not in (None, KUBE_SYSTEM):
                    continue
                if meta.get("namespace") is None and _NAMESPACE_DECLARED_RE.search(body_text):
                    notes.append(
                        f"{address} ({where}): the ConfigMap's namespace is not a "
                        "literal; recorded on the assumption that it is kube-system")
                data = _find_assignment(text, mask, body_start, body_end, ("data",))
                if data is None or not content.startswith("{", data[1]):
                    notes.append(f"{address} ({where}): ConfigMap {meta['name']} "
                                 "declares no literal data map; its Corefile was "
                                 "not read")
                    continue
                map_end = datastores._block_body(mask, data[1])
                corefile = _find_assignment(text, mask, data[1] + 1, map_end,
                                            ("Corefile",))
                if corefile is None:
                    notes.append(f"{address} ({where}): ConfigMap {meta['name']} "
                                 "carries no Corefile key")
                    continue
                _record(sources, notes, "kubernetes_config_map", meta["name"], rel_path,
                        where, _read_value(content, text, mask, corefile[1], heredocs),
                        address=address)

            elif resource_type == "kubectl_manifest":
                found = _find_assignment(text, mask, body_start, body_end, ("yaml_body",))
                if found is None:
                    continue
                value = _read_value(content, text, mask, found[1], heredocs)
                if value.text is None:
                    if any(name in body_text for name in CONFIGMAP_NAMES):
                        notes.append(
                            f"{address} ({where}): yaml_body is read from "
                            f"{value.unread}, which the scan does not follow — if "
                            "it carries the coredns ConfigMap, it was not recorded")
                    continue
                if not any(name in value.text for name in CONFIGMAP_NAMES):
                    continue
                try:
                    docs = k8s_manifests.load_manifest_documents(value.text)
                except ValueError as e:
                    notes.append(f"{address} ({where}): yaml_body mentions coredns but "
                                 f"was not read ({e})")
                    continue
                before = len(sources)
                _configmaps_from_documents(docs, rel_path, where, sources, notes,
                                           address=address)
                if value.references and len(sources) > before:
                    notes.append(f"{address} ({where}): the recorded text also references "
                                 f"{', '.join(value.references)}; the values behind them "
                                 "were not recorded")

            elif resource_type == "kubernetes_manifest":
                manifest = _find_assignment(text, mask, body_start, body_end, ("manifest",))
                if manifest is None:
                    continue
                if not content.startswith("{", manifest[1]):
                    if any(name in body_text for name in CONFIGMAP_NAMES) or "Corefile" in body_text:
                        raw = content[manifest[1]:_expression_end(mask, manifest[1])].rstrip()
                        notes.append(
                            f"{address} ({where}): manifest is read from {raw}, which "
                            "the scan does not follow — if it carries the coredns "
                            "ConfigMap, it was not recorded")
                    continue
                m_end = datastores._block_body(mask, manifest[1])
                kind_literal = _literal_at(content, text, mask, manifest[1] + 1, m_end,
                                           heredocs, "kind")
                if kind_literal != "ConfigMap":
                    continue
                meta = _find_assignment(text, mask, manifest[1] + 1, m_end, ("metadata",))
                if meta is None or not content.startswith("{", meta[1]):
                    continue
                meta_end = datastores._block_body(mask, meta[1])
                name = _literal_at(content, text, mask, meta[1] + 1, meta_end, heredocs, "name")
                namespace = _literal_at(content, text, mask, meta[1] + 1, meta_end,
                                        heredocs, "namespace")
                if name not in CONFIGMAP_NAMES or namespace not in (None, KUBE_SYSTEM):
                    continue
                if namespace is None and _find_assignment(
                        text, mask, meta[1] + 1, meta_end, ("namespace",)) is not None:
                    notes.append(
                        f"{address} ({where}): the ConfigMap's namespace is not a "
                        "literal; recorded on the assumption that it is kube-system")
                data = _find_assignment(text, mask, manifest[1] + 1, m_end, ("data",))
                if data is None or not content.startswith("{", data[1]):
                    notes.append(f"{address} ({where}): ConfigMap {name} declares no "
                                 "literal data map; its Corefile was not read")
                    continue
                map_end = datastores._block_body(mask, data[1])
                corefile = _find_assignment(text, mask, data[1] + 1, map_end, ("Corefile",))
                if corefile is None:
                    notes.append(f"{address} ({where}): ConfigMap {name} carries no "
                                 "Corefile key")
                    continue
                _record(sources, notes, "kubernetes_config_map", name, rel_path, where,
                        _read_value(content, text, mask, corefile[1], heredocs),
                        address=address)

            elif resource_type == "helm_release":
                chart = args.get("chart")
                if isinstance(chart, str) and chart.rsplit("/", 1)[-1] in HELM_CHARTS:
                    notes.append(
                        f"{address} ({where}): Helm release of chart {chart}"
                        f"{_values_of(declared, 'values')} — its values were not "
                        "read; the cluster DNS configuration it carries is unknown")
    return sources, notes


def _doc_identity(doc) -> tuple:
    if not isinstance(doc, dict):
        return None, None, None
    meta = doc.get("metadata") if isinstance(doc.get("metadata"), dict) else {}
    return doc.get("kind"), meta.get("name"), meta.get("namespace")


def extract_manifest_sources(root_dir: str, scope: dict = None,
                             chart_roots: list = ()) -> tuple[list, list]:
    """CoreDNS sources in plain Kubernetes YAML: the ConfigMaps in kube-system.

    A CoreDNS Deployment or a node-local-dns DaemonSet is a note: pod settings
    do not carry over to a managed data plane, but the reader should know
    they were there.
    """
    sources, notes = [], []
    for rel_path, content in walk_in_scope(root_dir, scope, YAML_EXTENSIONS, notes, chart_roots):
        if not any(name in content for name in CONFIGMAP_NAMES):
            continue
        try:
            docs = k8s_manifests.load_manifest_documents(content)
        except ValueError as e:
            notes.append(f"{rel_path}: mentions coredns but was not read ({e})")
            continue
        _configmaps_from_documents(docs, rel_path, rel_path, sources, notes)
    return sources, notes


def _configmaps_from_documents(docs: list, rel_path: str, label: str,
                               sources: list, notes: list, address: str = None) -> None:
    """The CoreDNS ConfigMaps among parsed manifest documents, appended.

    Shared by the YAML walk and by a `kubectl_manifest` resource's
    `yaml_body`; `label` is the file or the block the documents came from.
    """
    for index, doc in enumerate(docs, start=1):
        kind, name, namespace = _doc_identity(doc)
        if name not in CONFIGMAP_NAMES or namespace not in (None, KUBE_SYSTEM):
            continue
        where = f"{label} (document {index})" if len(docs) > 1 or address is None else label
        if kind == "ConfigMap":
            data = doc.get("data") if isinstance(doc.get("data"), dict) else {}
            corefile = data.get("Corefile")
            if not isinstance(corefile, str):
                notes.append(f"{where}: ConfigMap {name} carries no Corefile key")
                continue
            if _is_empty_configuration(corefile):
                notes.append(f"{where}: ConfigMap {name} whose Corefile is empty — "
                             "nothing to carry over")
                continue
            sources.append(_source(
                "configmap", name, rel_path, [where], address=address,
                path=None if address else rel_path, form="manifest",
                text=_fit(corefile, where, notes)))
        elif kind in ("Deployment", "DaemonSet"):
            spec = doc.get("spec") if isinstance(doc.get("spec"), dict) else {}
            replicas = spec.get("replicas")
            notes.append(
                f"{where}: a {name} {kind} is declared in the repository"
                + (f" (replicas: {replicas})" if replicas is not None else "")
                + "; its pod settings do not carry over to a managed DNS "
                "data plane")


def merge_sources(sources: list) -> list:
    """One entry per (kind, name, directory, address or path); evidence unions.

    `name` is in the key because one manifest file can carry both the
    coredns and the node-local-dns ConfigMaps."""
    merged = {}
    for source in sources:
        key = (source["kind"], source["name"], source["directory"],
               source.get("address") or source.get("path"))
        if key in merged:
            kept = merged[key]
            for item in source["evidence"]:
                if item not in kept["evidence"]:
                    kept["evidence"].append(item)
            continue
        merged[key] = source
    return list(merged.values())


def harvest_cluster_dns(root_dir: str, scope: dict = None,
                        chart_roots: list = ()) -> HarvestResult:
    """The single entry point the scan step calls.

    Returns the section (`{"sources": [...]}` or `{}`) and the notes to
    persist beside it. The section cap is applied last, across both walks:
    a source whose text would push the section past it is demoted to a note
    rather than recorded without its text.
    """
    tf_sources, notes = extract_terraform_sources(root_dir, scope, chart_roots)
    yaml_sources, yaml_notes = extract_manifest_sources(root_dir, scope, chart_roots)
    notes.extend(yaml_notes)
    kept, total = [], 0
    for source in merge_sources(tf_sources + yaml_sources):
        size = len((source.get("text") or "").encode("utf-8"))
        if total + size > MAX_SECTION_BYTES:
            notes.append(
                f"{source.get('address') or source.get('path')}: not recorded, the "
                f"cluster_dns section is capped at {MAX_SECTION_BYTES} bytes")
            continue
        total += size
        kept.append(source)
    notes.append(COVERAGE_NOTE)
    section = {"sources": kept} if kept else {}
    return HarvestResult(section, notes)
