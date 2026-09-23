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

"""The cluster-dns unit's output contract.

The worker was handed the source Corefile verbatim (`inputs.cluster_dns`,
copied by discovery and never interpreted) and the mapping document
(`landingzone/knowledge/cluster-dns-translation.md`, attached to its
prompt), and it wrote the GKE side: a `kube-dns` ConfigMap on Standard,
Cloud DNS zones on the VPC. This gate checks what it wrote **without knowing
what any CoreDNS plugin means** — that is the division of labour the series
is built on (DESIGN §13): the LLM translates, code validates, and the
CoreDNS-side knowledge stays a document. Three check groups:

1. **Target schema.** The shape Cloud DNS for GKE actually reads: one
   `kube-dns` ConfigMap in `kube-system` whose data is `stubDomains` (a JSON
   object of domain → IPs) and/or `upstreamNameservers` (a JSON list of at
   most three IPs), no `Corefile`; managed zones private, with a dotted
   `dns_name`; every record set naming a zone the unit declares, in the one
   form that can be checked (`google_dns_managed_zone.<name>.name`).
2. **Owner boundary.** Nothing the landing zone or GKE owns: no cluster
   resource, no `dns_config` / `addons_config`, no `google_dns_policy` (VPC-wide,
   one per network, and alternative name servers would bypass the unit's own
   zones), no CoreDNS or node-local-dns workload or ConfigMap, no `kube-system`
   Namespace — and a `kube-dns` ConfigMap only when the unit's cluster mode
   is Standard (`inputs.cluster_mode`, the registry projection the planner
   threads in; a stored plan from before the projection is read through
   `decisions.mode_of(inputs.decision)`), since Cloud DNS reads it on
   Standard only and an unrecorded mode may be Autopilot. The boundary runs
   the other way too: every unit of
   another kind is swept for a kube-dns ConfigMap, a CoreDNS object, a
   forwarding zone or a DNS policy, so one family answers for CoreDNS —
   while a private or peering zone stays the network unit's business.
3. **Literal conservation.** Every IP and every DNS name in the generated
   files comes from the source text or the unit inputs (nothing invented);
   every IP in the source text is in the files or in the result's prose
   (nothing silently dropped). Source-side DNS names are deliberately not
   checked: Corefile plugin arguments are full of dotted non-hostnames.

Known blind spots, by design: a value filed under the wrong key — a stub
domain's resolvers listed as upstream nameservers — passes, because every
literal is present; and a record-holding zone may be named for any parent of
a source name, since the hosts mapping calls for exactly that. Stub domains
and forwarding zones are held to the exact source name. The rest is what the
unit review and the review UI's before/after comparison are for.

Deliberately narrow, like ksa_contract and gateway_contract: only units of
kind `cluster-dns` get the full contract; other units get only the
foreign-object sweep, and a legacy blob without a kind is swept, never
crashed on.
"""

import ipaddress
import json
import re

from servers.dag.server import decisions as decisions_lib
from servers.dag.server import exports as exports_lib

from . import root_wiring

UNIT_KIND = "cluster-dns"
CONFIGMAP_NAME = "kube-dns"
NAMESPACE = "kube-system"
ALLOWED_DATA_KEYS = ("stubDomains", "upstreamNameservers")
# kube-dns reads at most three upstream nameservers from the ConfigMap, and
# Cloud DNS for GKE honours the same map. Verified against the kube-dns
# ConfigMap documentation when this gate was written; if the limit moves,
# this is the one constant to move with it.
MAX_UPSTREAMS = 3
# The cluster mode is `inputs.cluster_mode` ("standard" | "autopilot" | null),
# the one registry projection (servers/dag/server/decisions.py) the planner
# threads into every new plan. A stored plan from before the projection has
# no such key and carries a mode-bearing token in `inputs.decision`; that is
# read through decisions.mode_of, so this gate never interprets a token.
STANDARD_MODE = "standard"
AUTOPILOT_MODE = "autopilot"

# Objects GKE or the landing zone owns; a unit that ships one is reaching
# across the boundary the brief draws.
FORBIDDEN_OBJECTS = {
    ("Deployment", "coredns"), ("DaemonSet", "node-local-dns"),
    ("ConfigMap", "coredns"), ("ConfigMap", "node-local-dns"),
    ("Namespace", "kube-system"),
}
_FORBIDDEN_HCL = (
    (re.compile(r'resource\s+"google_container_cluster"'), "a google_container_cluster"),
    (re.compile(r"\bdns_config\s*\{"), "a dns_config block"),
    (re.compile(r"\baddons_config\s*\{"), "an addons_config block"),
    # A DNS policy is VPC-wide and one per network; alternative name servers
    # replace the whole resolution order, so the unit's own private zones
    # would stop answering. Landing-zone territory, never this unit's.
    (re.compile(r'resource\s+"google_dns_policy"'), "a google_dns_policy"),
)
# Terraform namespaces: a dotted reference inside a "${...}" string is not a
# hostname, and neither is a prose abbreviation with a one-letter tail.
_HCL_NAMESPACES = ("var", "local", "module", "data", "each", "count", "path", "self", "terraform")
_INTERPOLATION_RE = re.compile(r"\$\{[^}]*\}")
# `variables.tf`, `kube-dns.yaml`, `design-decisions.md` in a description are
# file names, not hosts — when they are exactly two labels; `.md`, `.sh`,
# `.py` and `.tf` are also country codes, and `corp.example.md` is a name.
_FILE_SUFFIXES = ("tf", "tfvars", "yaml", "yml", "md", "json", "txt", "sh", "py", "html", "csv")

# Addresses that are nobody's data: loopback, the NodeLocal and metadata
# server addresses, the unspecified address.
IGNORED_IPS = {"127.0.0.1", "0.0.0.0", "169.254.20.10", "169.254.169.254",
               "169.254.169.253",   # the AWS VPC resolver's link-local address: no GKE meaning
               "::1", "::"}
# Domain suffixes generated files legitimately carry without a source:
# Kubernetes and GCP API groups, the product's own labels, the in-cluster
# zone, reverse zones.
ALLOWED_NAME_SUFFIXES = (
    "googleapis.com", "gserviceaccount.com", "kubernetes.io", "k8s.io", "gke.io",
    "gkma.dev", "cluster.local", "in-addr.arpa", "ip6.arpa", "google.com",
    "terraform.io", "hashicorp.com",
)

# The trailing lookahead refuses a further dotted label (a reverse zone, a
# version string) but accepts a sentence-final period: the drop check reads
# prose, and "the VPC resolver at 10.0.0.2." must count.
_IPV4_RE = re.compile(r"(?<![\w.])(?:\d{1,3}\.){3}\d{1,3}(?!\w|\.\w)")
_IPV6_RE = re.compile(r"(?<![\w:])(?:[0-9A-Fa-f]{0,4}:){2,7}[0-9A-Fa-f]{0,4}(?![\w:])")
_DNS_RE = re.compile(
    r"(?<![\w.-])((?:[a-z0-9](?:[a-z0-9-]*[a-z0-9])?\.)+[a-z]{2,}[a-z0-9-]*)\.?(?![\w-])")
# One or more labels: `consul` is a common single-label stub domain, and the
# same customization shipped as a forwarding zone `consul.` passes below.
_LABEL_RE = re.compile(r"^(?:[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\.)*[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\.?$")
_HCL_STRING_RE = re.compile(r'"((?:[^"\\]|\\.)*)"')
# A heredoc body is data the strip blanks; it is scanned from the raw file so
# an address written inside one is judged like any other string.
_HEREDOC_BODY_RE = re.compile(r"<<-?[ \t]*([A-Za-z_][A-Za-z0-9_]*)[ \t]*\r?\n(.*?)\n[ \t]*\1[ \t]*(?=\r?\n|$)", re.S)
_ZONE_RE = re.compile(r'resource\s+"google_dns_managed_zone"\s+"([^"]+)"\s*\{')
_RECORD_RE = re.compile(r'resource\s+"google_dns_record_set"\s+"([^"]+)"\s*\{')


def _ip(value: str) -> str | None:
    try:
        return ipaddress.ip_address(value.strip()).compressed
    except ValueError:
        return None


def _ips_in(text: str) -> set:
    found = set()
    for pattern in (_IPV4_RE, _IPV6_RE):
        for match in pattern.finditer(text or ""):
            ip = _ip(match.group(0))
            if ip and ip not in IGNORED_IPS:
                found.add(ip)
    return found


def _is_domain(value: str) -> bool:
    return bool(_LABEL_RE.match(value.strip().lower())) and _ip(value) is None


def _allowed_name(name: str) -> bool:
    return any(name == suffix or name.endswith("." + suffix) for suffix in ALLOWED_NAME_SUFFIXES)


def _names_in(strings) -> set:
    """Dotted DNS names in a set of string scalars, lowercased, no trailing dot.

    Scalars only — never identifiers — so `google_dns_managed_zone.x.name`
    in HCL or a dotted YAML key cannot look like a hostname.
    """
    found = set()
    for value in strings:
        # An interpolation becomes one placeholder label: "${var.network}"
        # is then just `x` (no name), while "${var.env}.corp.example.net." is
        # `x.corp.example.net` — a templated domain, judged like any other.
        text = _INTERPOLATION_RE.sub("x", str(value).lower())
        for match in _DNS_RE.finditer(text):
            name = match.group(1).rstrip(".")
            labels = name.split(".")
            if (_ip(name) or labels[0] in _HCL_NAMESPACES
                    or (len(labels) == 2 and labels[-1] in _FILE_SUFFIXES)
                    or _allowed_name(name)):
                continue
            found.add(name)
    return found


def _yaml_scalars(node, out: list) -> None:
    if isinstance(node, dict):
        for value in node.values():
            _yaml_scalars(value, out)
    elif isinstance(node, list):
        for value in node:
            _yaml_scalars(value, out)
    elif isinstance(node, str):
        out.append(node)


def _hcl_strings(content: str) -> list:
    """Quoted strings of an HCL file, lexed from the raw text.

    Not from `_strip_hcl`'s output (it turns the braces of
    `"${var.env}.corp.example.net."` into spaces and hides the templated
    domain), and not by a comment regex (a `//` inside a URL string, or a
    DOTALL `.*`, blanks the rest of the file). A small walker: comments
    (`#`, `//`, `/* */`) are skipped outside strings, heredoc bodies are
    skipped (scanned separately), and a string ends at its closing quote —
    with `${ ... }` nesting tracked so a quote inside an interpolation does
    not end it.
    """
    strings, i, n = [], 0, len(content)
    while i < n:
        ch = content[i]
        if ch == '"':
            i += 1
            start, depth = i, 0
            while i < n:
                c = content[i]
                if c == "\\":
                    i += 2
                    continue
                if content.startswith("${", i) or content.startswith("%{", i):
                    depth += 1
                    i += 2
                    continue
                if depth and c == "}":
                    depth -= 1
                elif depth and c == '"':
                    # a nested string inside the interpolation: skip it whole
                    i += 1
                    while i < n and content[i] != '"':
                        i += 2 if content[i] == "\\" else 1
                elif not depth and c == '"':
                    break
                i += 1
            strings.append(content[start:i])
            i += 1
        elif ch == "#" or content.startswith("//", i):
            while i < n and content[i] != "\n":
                i += 1
        elif content.startswith("/*", i):
            close = content.find("*/", i + 2)
            i = n if close == -1 else close + 2
        elif ch == "<" and content.startswith("<<", i):
            match = re.compile(r"<<-?[ \t]*([A-Za-z_][A-Za-z0-9_]*)[ \t]*\r?\n").match(content, i)
            if match:
                terminator = re.compile(r"^[ \t]*" + re.escape(match.group(1)) + r"[ \t]*$", re.M)
                end = terminator.search(content, match.end())
                i = n if end is None else end.end()
            else:
                i += 2
        else:
            i += 1
    return strings


def _hcl_block_body(stripped: str, open_at: int) -> str:
    depth = 0
    for i in range(open_at, len(stripped)):
        if stripped[i] == "{":
            depth += 1
        elif stripped[i] == "}":
            depth -= 1
            if depth == 0:
                return stripped[open_at + 1:i]
    return stripped[open_at + 1:]


_JSON_ESCAPE_RE = re.compile(r"\\(u[0-9A-Fa-f]{4}|[nrtbf\\\"/])")
_JSON_ESCAPES = {"n": "\n", "r": "\r", "t": "\t", "b": "\b", "f": "\f",
                 "\\": "\\", '"': '"', "/": "/"}


def _source_text(unit: dict) -> str:
    """The recorded source texts, with JSON string escapes decoded.

    The managed add-on's configuration_values is JSON whose `corefile` value
    is one escaped string: `}\\ncorp.acme.internal:53 {`. Left as written,
    the `n` of the `\\n` is a word character right before the domain, so the
    name regex's boundary never fires and a faithful stub domain reads as
    invented — and an address at the start of a `hosts` line was neither
    "known" nor "dropped" for the same reason. Decoding the escapes gives the
    Corefile the worker actually read — the string the JSON means. The short
    escapes create no letter, digit or dot; a `\\uXXXX` yields exactly the
    character the source encoded, which is the character the worker saw.
    """
    section = ((unit.get("inputs") or {}).get("cluster_dns") or {})
    raw = "\n".join(str(s.get("text") or "") for s in section.get("sources") or []
                    if isinstance(s, dict))
    def one(match):
        esc = match.group(1)
        if len(esc) == 5 and esc[0] == "u":
            return chr(int(esc[1:], 16))
        return _JSON_ESCAPES[esc]
    return _JSON_ESCAPE_RE.sub(one, raw)


def _mode(unit: dict):
    """The unit's cluster mode: `inputs.cluster_mode` when the key is present
    (null included — a present null is a recorded "no mode", never a reason
    to fall through), else the legacy `inputs.decision` token's base mode."""
    inputs = unit.get("inputs") or {}
    if "cluster_mode" in inputs:
        mode = inputs.get("cluster_mode")
        return mode if mode in (STANDARD_MODE, AUTOPILOT_MODE) else None
    return decisions_lib.mode_of(inputs.get("decision"))[0]


def _documents(entry: dict) -> tuple:
    """([(path, doc)], [parse errors]) over the unit's YAML files."""
    docs, errors = [], []
    for path, content in sorted(exports_lib._unit_files(entry, (".yaml", ".yml")).items()):
        loaded, error = exports_lib._load_yaml_docs(content)
        if error:
            errors.append(f"{path}: {error}; this gate cannot read it")
            continue
        docs.extend((path, doc) for doc in loaded if isinstance(doc, dict))
    return docs, errors


def _identity(doc: dict) -> tuple:
    meta = doc.get("metadata") if isinstance(doc.get("metadata"), dict) else {}
    return str(doc.get("kind") or ""), str(meta.get("name") or ""), meta.get("namespace")


def _stub_domain_keys(doc: dict) -> set:
    data = doc.get("data") if isinstance(doc.get("data"), dict) else {}
    try:
        parsed = json.loads(data.get("stubDomains") or "{}")
    except ValueError:
        return set()
    return {str(k).lower().rstrip(".") for k in parsed} if isinstance(parsed, dict) else set()


def _name_known(name: str, known: set, allow_parent: bool) -> bool:
    """Label-aligned membership: `corp.example.com` is known when the source
    names it, or — for a zone that merely holds records — when the source
    names a child of it (`db.corp.example.com`). Never a substring:
    `xample.com` is not in `example.com`."""
    if name in known:
        return True
    return allow_parent and any(k.endswith("." + name) for k in known)


def _configmap_errors(path: str, doc: dict) -> list:
    """The shape Cloud DNS for GKE reads from the kube-dns ConfigMap."""
    errors = []
    _kind, _name, namespace = _identity(doc)
    if namespace != NAMESPACE:
        errors.append(f"{path}: the kube-dns ConfigMap sets metadata.namespace to "
                      f"{namespace!r}; Cloud DNS reads it from {NAMESPACE} only")
    data = doc.get("data")
    data = data if isinstance(data, dict) else {}
    if "Corefile" in data:
        errors.append(f"{path}: the kube-dns ConfigMap carries a Corefile key; Cloud DNS "
                      "does not read one, and a stray Corefile misleads the next reader")
    for key in data:
        if key not in ALLOWED_DATA_KEYS and key != "Corefile":
            errors.append(f"{path}: the kube-dns ConfigMap carries data.{key}, which Cloud "
                          f"DNS does not read; only {', '.join(ALLOWED_DATA_KEYS)} are honoured")
    if "stubDomains" in data:
        errors.extend(_stub_domain_errors(path, data["stubDomains"]))
    if "upstreamNameservers" in data:
        errors.extend(_upstream_errors(path, data["upstreamNameservers"]))
    return errors


def _parse_json(path: str, key: str, value) -> tuple:
    if not isinstance(value, str):
        return None, [f"{path}: data.{key} must be a JSON string, not "
                      f"{type(value).__name__}"]
    try:
        return json.loads(value), []
    except ValueError as e:
        return None, [f"{path}: data.{key} is not valid JSON ({e})"]


def _stub_domain_errors(path: str, raw) -> list:
    parsed, errors = _parse_json(path, "stubDomains", raw)
    if errors:
        return errors
    if not isinstance(parsed, dict):
        return [f"{path}: data.stubDomains must be a JSON object of domain → list of IPs"]
    for domain, servers in parsed.items():
        if not _is_domain(str(domain)):
            errors.append(f"{path}: stubDomains key {domain!r} is not a DNS name")
        if not isinstance(servers, list) or not servers:
            errors.append(f"{path}: stubDomains[{domain!r}] must be a non-empty list of IPs")
            continue
        for server in servers:
            if not isinstance(server, str) or _ip(server) is None:
                errors.append(f"{path}: stubDomains[{domain!r}] carries {server!r}, "
                              "which is not an IP address")
    return errors


def _upstream_errors(path: str, raw) -> list:
    parsed, errors = _parse_json(path, "upstreamNameservers", raw)
    if errors:
        return errors
    if not isinstance(parsed, list) or not parsed:
        return [f"{path}: data.upstreamNameservers must be a non-empty JSON list of IPs"]
    if len(parsed) > MAX_UPSTREAMS:
        errors.append(f"{path}: upstreamNameservers lists {len(parsed)} servers; kube-dns "
                      f"honours at most {MAX_UPSTREAMS}")
    for server in parsed:
        if not isinstance(server, str) or _ip(server) is None:
            errors.append(f"{path}: upstreamNameservers carries {server!r}, which is not "
                          "an IP address")
    return errors


def _terraform_errors(tf_files: dict) -> tuple:
    """(errors, string scalars, strict names) over the unit's .tf files,
    lexically. Strict names — a forwarding zone's dns_name and every record
    set's name — must match the source exactly; only a record-holding zone's
    dns_name may be a parent of a source name."""
    errors, strings, zones, records, forwarding = [], [], {}, [], set()
    for path, content in sorted(tf_files.items()):
        stripped = root_wiring._strip_hcl(content)
        strings.extend(_hcl_strings(content))
        strings.extend(m.group(2) for m in _HEREDOC_BODY_RE.finditer(content))
        for pattern, what in _FORBIDDEN_HCL:
            if pattern.search(stripped):
                errors.append(f"{path}: declares {what}; the cluster and its DNS provider "
                              "are the landing zone's, not this unit's")
        for match in _ZONE_RE.finditer(stripped):
            body = _hcl_block_body(stripped, match.end() - 1)
            zones[match.group(1)] = path
            if not re.search(r'\bvisibility\s*=\s*"private"', body):
                errors.append(f"{path}: google_dns_managed_zone.{match.group(1)} does not set "
                              'visibility = "private"; a forwarding zone is a private zone '
                              "too, and a public zone is not how a Corefile customization "
                              "comes across")
            dns_name = re.search(r'\bdns_name\s*=\s*"([^"]*)"', body)
            if dns_name and re.search(r"\bforwarding_config\s*\{", body):
                forwarding.add(dns_name.group(1).lower().rstrip("."))
            if dns_name is None:
                assignment = re.search(r"\bdns_name\s*=\s*(.+)", body)
                errors.append(f"{path}: google_dns_managed_zone.{match.group(1)} "
                              + (f"sets dns_name = {assignment.group(1).strip()}, which is not a literal"
                                 if assignment else "sets no dns_name")
                              + " — the domain comes from the source text and the gate reads it "
                              "off the resource; one resource per zone, no for_each or variable")
            elif not dns_name.group(1).endswith("."):
                errors.append(f"{path}: google_dns_managed_zone.{match.group(1)} sets "
                              f"dns_name = \"{dns_name.group(1)}\" without the trailing dot "
                              "Cloud DNS requires")
        for match in _RECORD_RE.finditer(stripped):
            body = _hcl_block_body(stripped, match.end() - 1)
            record_name = re.search(r'\bname\s*=\s*"([^"]*)"', body)
            if record_name:
                # A record answers for exactly the name the source gave it;
                # only the zone around it may be a parent.
                forwarding.add(record_name.group(1).lower().rstrip("."))
            assignment = re.search(r"\bmanaged_zone\s*=\s*(.+)", body)
            ref = re.match(r"google_dns_managed_zone\.([A-Za-z0-9_-]+)\.name\s*$",
                           assignment.group(1).strip()) if assignment else None
            records.append((path, match.group(1),
                            assignment.group(1).strip() if assignment else None,
                            ref.group(1) if ref else None))
    for path, name, raw, zone in records:
        if zone is not None and zone in zones:
            continue
        errors.append(
            f"{path}: google_dns_record_set.{name} must name a zone this unit declares "
            f"(managed_zone = google_dns_managed_zone.<name>.name); it "
            + (f"sets managed_zone = {raw}" if raw else "sets no managed_zone")
            + " — a record in a zone this unit does not own is the landing zone's to place")
    return errors, strings, forwarding


def check_unit(entry: dict) -> list:
    """Contract errors for ONE cluster-dns unit's outputs (empty when clean)."""
    unit = (entry or {}).get("unit") or {}
    result = (entry or {}).get("result") or {}
    source = _source_text(unit)
    # The inputs are the "known values" whitelist (project, network names);
    # the scan notes are prose about the scan and stay out of it, so a note
    # that one day quotes an address cannot excuse an invented one.
    inputs_text = json.dumps({k: v for k, v in (unit.get("inputs") or {}).items()
                              if k != "scan_notes"}, default=str).lower()
    mode = _mode(unit)

    docs, errors = _documents(entry)
    configmaps, scalars = [], []
    for path, doc in docs:
        kind, name, _ns = _identity(doc)
        if (kind, name) in FORBIDDEN_OBJECTS:
            errors.append(f"{path}: ships a {kind} named {name}; GKE owns it under Cloud DNS, "
                          "and the landing zone owns kube-system")
        elif _ns == NAMESPACE and (kind, name) != ("ConfigMap", CONFIGMAP_NAME):
            # GKE still runs its own kube-dns Service, Deployment, ServiceAccount
            # and autoscaler there under Cloud DNS; a unit object with one of
            # those names would patch the live one on apply.
            errors.append(f"{path}: ships a {kind} named {name} in {NAMESPACE}; the only "
                          f"object this unit may place there is the {CONFIGMAP_NAME} ConfigMap "
                          "— everything else in that namespace is GKE's")
        if kind == "ConfigMap" and name == CONFIGMAP_NAME:
            configmaps.append((path, doc))
        elif kind == "ConfigMap" and (kind, name) not in FORBIDDEN_OBJECTS:
            errors.append(f"{path}: ships a ConfigMap named {name}; the only ConfigMap this "
                          f"unit may emit is {CONFIGMAP_NAME} in {NAMESPACE} — anything else "
                          "is a stray Corefile that misleads the next reader")
        _yaml_scalars(doc, scalars)

    if len(configmaps) > 1:
        errors.append("the unit ships more than one kube-dns ConfigMap ("
                      + ", ".join(p for p, _ in configmaps) + "); one object, one owner")
    for path, doc in configmaps:
        errors.extend(_configmap_errors(path, doc))
    if configmaps and mode != STANDARD_MODE:
        reason = str((unit.get("inputs") or {}).get("decision_reason") or "")
        if mode == AUTOPILOT_MODE:
            what = "an Autopilot cluster"
        elif reason.startswith("disagree"):
            what = ("a cluster whose recorded landing-zone decisions disagree on the mode ("
                    + reason[len("disagree: "):] + ")")
        else:
            what = "a cluster whose mode is not recorded (inputs.cluster_mode is null)"
        errors.append(f"the unit ships a kube-dns ConfigMap for {what}; Cloud DNS reads "
                      "stubDomains and upstreamNameservers from that ConfigMap on Standard "
                      "only (inputs.cluster_mode = standard) — carry stub domains as Cloud "
                      "DNS private forwarding zones and raise pinned upstreams as an open "
                      "question")
    if configmaps and not source.strip():
        errors.append("the unit ships a kube-dns ConfigMap although its inputs carry no "
                      "Corefile text; an empty or invented resolver map would clear a "
                      "customer's hand-set resolvers")

    tf_files = exports_lib._unit_files(entry, (".tf",))
    tf_errors, tf_strings, tf_strict = _terraform_errors(tf_files)
    errors.extend(tf_errors)
    # Names that must match the source exactly: a stub domain's key and a
    # forwarding zone's dns_name decide which queries leave the cluster, so
    # `corp.example.com` for a source `internal.corp.example.com` would widen
    # the forward to a whole parent domain; a record set answers for its
    # name. Only a zone that holds records may be the common parent of the
    # names it holds (the hosts mapping).
    strict_names = set(tf_strict)
    for _path, doc in configmaps:
        strict_names |= _stub_domain_keys(doc)

    # Literal conservation, output → source: nothing invented.
    output_ips = _ips_in("\n".join(scalars + tf_strings))
    source_ips = _ips_in(source)
    inputs_ips = _ips_in(inputs_text)
    for ip in sorted(output_ips):
        if ip not in source_ips and ip not in inputs_ips:
            errors.append(f"the generated files carry the address {ip}, which does not come "
                          "from the source Corefile or the unit inputs — nothing is invented")
    known_names = _names_in([source]) | _names_in([inputs_text])
    for name in sorted(_names_in(scalars + tf_strings)):
        strict = name in strict_names
        if not _name_known(name, known_names, allow_parent=not strict):
            why = ("a stub domain, a forwarding zone or a record must name exactly the "
                   "domain the source gives, never a parent of it" if strict else
                   "nothing is invented")
            errors.append(f"the generated files carry the name {name}, which does not come "
                          f"from the source Corefile or the unit inputs — {why}")

    # Literal conservation, source → output: nothing silently dropped.
    files_text = "\n".join(
        f.get("content") or "" for f in (result.get("files") or []) if isinstance(f, dict))
    prose = "\n".join([
        str(result.get("tradeoffs") or ""),
        "\n".join(str(a) for a in result.get("assumptions") or []),
        "\n".join(str(q) for q in result.get("open_questions") or []),
    ])
    accounted = _ips_in(files_text) | _ips_in(prose)
    for ip in sorted(source_ips):
        if ip not in accounted:
            errors.append(f"the source Corefile names the address {ip}, and neither the "
                          "generated files nor the tradeoffs, assumptions or open questions "
                          "mention it — nothing is dropped silently")
    return errors


_DNS_RESOURCE_RE = re.compile(
    r'resource\s+"(google_dns_managed_zone|google_dns_policy)"\s+"([^"]+)"\s*\{')


def foreign_dns_errors(entry: dict) -> list:
    """Cluster-DNS objects shipped by a unit of ANOTHER kind.

    "Never let two families answer for CoreDNS" is a brief sentence in the
    cluster-addons unit; this makes it mechanical, the way gateway_contract
    sweeps Namespace manifests across all units: a kube-dns ConfigMap, a
    CoreDNS or node-local-dns object, a FORWARDING zone or a DNS policy from
    any other unit is a finding attributed to that unit. A private or peering
    zone and its record sets are the network unit's ordinary business and
    are not swept.
    """
    errors = []
    docs, _parse_errors = _documents(entry)
    for path, doc in docs:
        kind, name, namespace = _identity(doc)
        if (kind, name) == ("ConfigMap", CONFIGMAP_NAME):
            errors.append(f"{path}: ships ConfigMap {CONFIGMAP_NAME}"
                          + (f" in {namespace}" if namespace else "")
                          + f"; the resolver ConfigMap is the {UNIT_KIND} unit's alone — one "
                          "family answers for CoreDNS")
        elif (kind, name) in FORBIDDEN_OBJECTS and kind != "Namespace":
            # The kube-system Namespace is left to the tenancy family's own
            # rules: a recorded namespace it issues is not a DNS matter.
            errors.append(f"{path}: ships {kind} {name}"
                          + (f" in {namespace}" if namespace else "")
                          + "; CoreDNS and node-local-dns objects are GKE's under Cloud DNS "
                          "— no unit ships them")
    # Only what the mapping produces: a FORWARDING zone answers for a CoreDNS
    # stub domain, and a DNS policy is nobody's. A private or peering zone
    # (cross-VPC name sharing) is the network unit's ordinary business and a
    # record set in one is not a resolver matter — the sweep leaves them be.
    for path, content in sorted(exports_lib._unit_files(entry, (".tf",)).items()):
        stripped = root_wiring._strip_hcl(content)
        for match in _DNS_RESOURCE_RE.finditer(stripped):
            rtype, name = match.group(1), match.group(2)
            if rtype == "google_dns_policy":
                errors.append(f"{path}: declares google_dns_policy.{name}; a DNS policy is "
                              "VPC-wide and one per network — landing-zone territory, no unit's")
                continue
            body = _hcl_block_body(stripped, match.end() - 1)
            if re.search(r"\bforwarding_config\s*\{", body):
                errors.append(f"{path}: declares google_dns_managed_zone.{name} with a "
                              f"forwarding_config; a forwarding zone answers for a CoreDNS stub "
                              f"domain, which is the {UNIT_KIND} unit's alone — one family "
                              "answers for CoreDNS")
    return errors


def check_units(done_units: list) -> dict:
    """Contract check over the done unit blobs the validate step ships.

    Units of kind `cluster-dns` get the full contract; every other unit gets
    the foreign-object sweep. Returns {"checked": [unit_id],
    "findings": [{"unit_id", "error"}]} — `checked` names the cluster-dns
    units only, as before.
    """
    report = {"checked": [], "findings": []}
    for entry in done_units or []:
        unit = (entry or {}).get("unit") or {}
        unit_id = str(unit.get("unit_id") or unit.get("kind") or "?")
        if unit.get("kind") != UNIT_KIND:
            for error in foreign_dns_errors(entry):
                report["findings"].append({"unit_id": unit_id, "error": error})
            continue
        report["checked"].append(unit_id)
        for error in check_unit(entry):
            report["findings"].append({"unit_id": unit_id, "error": error})
    return report
