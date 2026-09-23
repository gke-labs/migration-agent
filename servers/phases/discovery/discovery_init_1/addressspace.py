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

"""Source network address space harvest for discovery.

Pure logic — no GCS, no subprocesses, no LLM. Reads the address space the
repository declares for the source estate and records it, with evidence, in
`inventory["address_space"]`: the VPC CIDRs (primary and secondary), the
subnet CIDRs with their tier and zone, what each cluster's network is (the
VPC it sits in, its service range, its IP family, the CIDRs allowed at its
public endpoint, and the remote node and pod ranges of a hybrid estate), and
the ranges the VPC routes to, whatever the target (a peering connection, a
transit gateway, a VPN, an appliance instance, a gateway, an endpoint).

The landing-zone design reads this section to propose target ranges that do
not overlap the source — the two networks are routable to each other for as
long as a migration runs. Before
this section existed the design had nothing to read, so it asked the user
for the ranges or invented them.

Three dialects are read: Terraform (`aws_vpc`, `aws_subnet`, the VPC and EKS
community modules, `aws_eks_cluster`, `aws_route` and the route table, VPN
static route and transit gateway route resources), eksctl `ClusterConfig`
YAML, and CloudFormation templates in YAML or JSON. A range is recorded only
when the files state it: a literal, a variable's default or a `locals`
value (followed through `var.` and `local.` references within one
directory, auto-loaded `.tfvars` and `.tfvars.json` values laid over the
defaults), a `cidrsubnet()` over one of those, or the primary range of a
VPC declared in the same directory named through its attribute
(`aws_vpc.main.cidr_block`, `module.vpc.vpc_cidr_block`): the one resource
attribute the scan follows, because it is the value the scan has just
recorded. Anything else — a variable with no default, a data source, any
other resource attribute, an attribute or index of a variable or local
(`var.network.cidr`, `var.cidrs[0]`), an IPAM pool, an eksctl `${ENV}` placeholder, a
CloudFormation intrinsic other than a `Ref` to a parameter with a `Default`
(followed, and the evidence says so; a value a parent stack passes for that
parameter is not read) — lands in `unresolved` with the expression,
never as a guess. `triage_unresolved` then says which of those entries can
change the target ranges and which the design can ignore (a subnet inside
a VPC whose range is stated or is itself among the questions, a
public-endpoint allow list); the scan
summary and the landing-zone proposal both use it, so the user is asked
the same questions once.

The Terraform lexer is `datastores.py`'s (`scan_source`,
`iter_terraform_blocks`); the file walk is `clusterdns.py`'s.
"""

import ipaddress
import json
import os
import re
from typing import NamedTuple

import yaml

from servers.phases import k8s_manifests

from . import datastores
from .clusterdns import TF_EXTENSIONS, YAML_EXTENSIONS, walk_in_scope

TFVARS_EXTENSIONS = (".tfvars",)
TFVARS_JSON_EXTENSIONS = (".tfvars.json",)
JSON_EXTENSIONS = (".json",)
PURPOSE = "the network address space"

# How far a `var.` or `local.` chain is followed before it is given up as
# unresolved. Real estates need two or three hops (module arg → local →
# variable default); the cap only stops a cycle.
MAX_RESOLVE_DEPTH = 8
# Entries kept per list. An estate with more subnets than this is not a
# subnet list a design reads; the overflow is a note.
MAX_ENTRIES = 500

COVERAGE_NOTE = (
    "address_space covers Terraform .tf and auto-loaded .tfvars files (HCL or "
    ".tfvars.json), eksctl "
    "ClusterConfig YAML and CloudFormation templates (YAML or JSON) only — "
    "Helm chart templates, Kustomize overlays, .tf.json, CDK and Pulumi are "
    "not parsed, so an empty section for one of those means not scanned, not "
    "absent. A managed prefix list is not followed: a route to one is listed "
    "under `unresolved`. Terragrunt `inputs` are not read either, so an estate whose ranges "
    "live only in terragrunt.hcl shows them as variables with no default. Of "
    "the resource attributes, only a declared VPC's own primary range "
    "(aws_vpc.<name>.cidr_block, module.<name>.vpc_cidr_block) is followed; an "
    "attribute or index of a variable or local (var.network.cidr, var.cidrs[0]) "
    "is not, nor an indexed reference (aws_vpc.this[0].cidr_block, "
    "module.vpc[0].vpc_id). A CloudFormation Ref to a parameter Default is "
    "followed; a value a parent stack passes for that parameter is not read, so "
    "a nested stack shows its defaults. A "
    "range the files build from a variable with no default, a data source, "
    "any other resource attribute, an IPAM pool or an environment placeholder "
    "is listed under `unresolved` with its expression, never guessed.")



def _is_catch_all(cidr: str) -> bool:
    """A default route or a split of one: 0.0.0.0/0, ::/0, or the
    0.0.0.0/1 and 128.0.0.0/1 pair a full-tunnel VPN or firewall pushes.
    A destination shorter than a /8 is not a network anyone is assigned;
    it says where everything else goes, not which ranges the source uses,
    and in the avoidance set it would leave no candidate block free."""
    try:
        return ipaddress.ip_network(cidr, strict=False).prefixlen < 8
    except ValueError:
        return False
# eksctl creates the VPC with this CIDR when the ClusterConfig declares
# neither `vpc.cidr` nor `vpc.id` (eksctl docs, "VPC networking").
EKSCTL_DEFAULT_VPC_CIDR = "192.168.0.0/16"

# The VPC module's subnet-list arguments and the tier each one declares.
MODULE_SUBNET_ARGS = {
    "private_subnets": "private",
    "public_subnets": "public",
    "intra_subnets": "intra",
    "database_subnets": "database",
    "elasticache_subnets": "elasticache",
    "redshift_subnets": "redshift",
    "outpost_subnets": "outpost",
}
# EKS module argument names across major versions: v20 prefixes them with
# `cluster_`, v21 drops the prefix.
EKS_MODULE_SERVICE_ARGS = ("cluster_service_ipv4_cidr", "service_ipv4_cidr")
EKS_MODULE_IP_FAMILY_ARGS = ("cluster_ip_family", "ip_family")
EKS_MODULE_PUBLIC_ARGS = ("cluster_endpoint_public_access_cidrs",
                          "endpoint_public_access_cidrs")
EKS_MODULE_NAME_ARGS = ("cluster_name", "name")
EKS_MODULE_REMOTE_ARGS = ("cluster_remote_network_config", "remote_network_config")
# A route names its target by exactly one of these; the route is recorded
# as reaching its destination "via" that kind of target. The list is the
# AWS provider's current `aws_route` target set plus what older files still
# carry: `instance_id` (an appliance; no longer an `aws_route` argument, still
# one of `aws_default_route_table`) and `vpn_gateway_id` (a virtual private
# gateway is named through `gateway_id` today), and the targets of a VPN
# static route and a transit gateway route.
ROUTE_TARGET_ARGS = ("vpc_peering_connection_id", "transit_gateway_id",
                     "vpn_gateway_id", "core_network_arn", "odb_network_arn",
                     "network_interface_id", "nat_gateway_id", "gateway_id",
                     "egress_only_gateway_id", "carrier_gateway_id", "local_gateway_id",
                     "vpc_endpoint_id", "instance_id", "vpn_connection_id",
                     "transit_gateway_attachment_id")
CFN_ROUTE_TARGETS = {
    "VpcPeeringConnectionId": "vpc_peering_connection_id",
    "TransitGatewayId": "transit_gateway_id",
    "InstanceId": "instance_id",
    "CoreNetworkArn": "core_network_arn",
    "NetworkInterfaceId": "network_interface_id",
    "NatGatewayId": "nat_gateway_id",
    "GatewayId": "gateway_id",
    "EgressOnlyInternetGatewayId": "egress_only_gateway_id",
    "CarrierGatewayId": "carrier_gateway_id",
    "LocalGatewayId": "local_gateway_id",
    "VpcEndpointId": "vpc_endpoint_id",
}

# A zone code: the Region code and a letter (us-east-1a), or the Region code
# and a location suffix for a Local Zone (us-west-2-lax-1a) or a Wavelength
# Zone (us-east-1-wl1-bos-wlz-1, or the newer us-east-1-foe-wlz-1a). eksctl
# accepts any of them as a subnet key.
_AZ_RE = re.compile(r"^[a-z]{2}(?:-[a-z]+)+-\d(?:[a-z]|-[a-z]{3}-\d[a-z]|-wl\d-[a-z]{3}-wlz-\d|-[a-z]{3}-wlz-\d[a-z])$")
_VAR_RE = re.compile(r"^var\.([A-Za-z_][A-Za-z0-9_-]*)$")
_LOCAL_RE = re.compile(r"^local\.([A-Za-z_][A-Za-z0-9_-]*)$")
# The one resource attribute a range expression is followed through: the
# primary range of a VPC declared in the same directory, as a resource or
# as the community module (whose output is `vpc_cidr_block`).
_VPC_ATTR_RE = re.compile(r"^(?:aws_vpc\.[A-Za-z_][\w-]*\.cidr_block"
                          r"|module\.[A-Za-z_][\w-]*\.vpc_cidr_block)$")
_CALL_RE = re.compile(r"^([a-z_]+)\((.*)\)$", re.DOTALL)
_PURE_INTERPOLATION_RE = re.compile(r'^"\$\{([^"{}]+)\}"$')
# A depth-0 `key = value` at the start of a line (`re.match` anchors at the
# position it is given). The key may be quoted, as map keys are.
_ASSIGN_RE = re.compile(r'[ \t]*"?([A-Za-z_][A-Za-z0-9_.-]*)"?[ \t]*=(?![=>])[ \t]*')
# A nested block header: `vpc_config {`, `dynamic "ingress" {`.
_BLOCK_START_RE = re.compile(r'[ \t]*([A-Za-z_][A-Za-z0-9_-]*)(?:[ \t]+"([^"\n]*)")?(?:[ \t]+"[^"\n]*")*[ \t]*\{')
_LOCALS_RE = re.compile(r"^[ \t]*locals[ \t]*\{", re.MULTILINE)
# Terraform addresses a reference can name. Module outputs keep their output
# name (`module.vpc.private_subnets`) because the tier is in it; resources
# keep type and name only.
_MODULE_OUTPUT_RE = re.compile(r"\bmodule\.([A-Za-z_][\w-]*)\.([A-Za-z_][\w-]*)")
_RESOURCE_REF_RE = re.compile(r"\b(data\.)?(aws_[a-z0-9_]+)\.([A-Za-z_][\w-]*)")
# A module source whose last meaningful segment IS vpc / eks: the community
# modules (`terraform-aws-modules/vpc/aws`) and local ones (`./modules/eks`).
# `terraform-aws-modules/vpc/aws` from the registry, `./modules/vpc` locally,
# `git::https://github.com/terraform-aws-modules/terraform-aws-vpc.git?ref=v5`
# from git.
_VPC_SOURCE_RE = re.compile(r"(^|/)(vpc|terraform-aws-vpc)(/aws)?(\.git)?(\?[^/]*)?$")
_EKS_SOURCE_RE = re.compile(r"(^|/)(eks|terraform-aws-eks)(/aws)?(\.git)?(\?[^/]*)?$")
# A module source that is a path in this checkout, not a registry or git address.
_LOCAL_SOURCE_RE = re.compile(r"^\.\.?/")
_INTERNAL_ELB_TAG_RE = re.compile(r"kubernetes\.io/role/internal-elb")
_PUBLIC_ELB_TAG_RE = re.compile(r"kubernetes\.io/role/elb")
_MAP_PUBLIC_RE = re.compile(r"^[ \t]*map_public_ip_on_launch[ \t]*=[ \t]*true\b", re.MULTILINE)


class HarvestResult(NamedTuple):
    section: dict
    notes: list


class _View(NamedTuple):
    """One copy of a local module's declarations: the chain of instance
    labels from the declaration out to a root module (direct instance
    first), the absolute-address prefix, the root directory that tells this
    copy from the others (None when there is only one), and the parent copy."""
    chain: tuple
    prefix: str
    root_dir: str | None
    parent: "object"


class _Scope:
    """The names one Terraform module directory can resolve: variable
    defaults, `locals`, the auto-loaded `.tfvars` values (which override a
    default, as Terraform does) and, for a local module seen through one of
    its instances, the arguments that instance passes (which override both)."""

    def __init__(self, defaults=None, locals_=None, tfvars=None, overrides=None,
                 resources=None, directory=None):
        self.defaults = defaults if defaults is not None else {}
        self.locals = locals_ if locals_ is not None else {}
        self.tfvars = tfvars if tfvars is not None else {}
        self.overrides = overrides or {}
        # The primary range of each VPC declared in the directory, once it
        # is resolved, under the attribute that names it
        # (`aws_vpc.main.cidr_block`, `module.vpc.vpc_cidr_block`), as the
        # literal text `_resolve` reads. One dict per copy of a local module
        # (its VPC can have a different range per instance), kept by
        # `_effective` across recomputations of the copy's scope.
        self.resources = resources if resources is not None else {}
        self.directory = directory
        # Outputs an instance argument on the way to this scope named and
        # did not find, as (directory, attribute): the VPC that produces
        # one may not have finished yet (resolve_terraform).
        self.pending_outputs = set()

    def variable(self, name: str):
        if name in self.overrides:
            return self.overrides[name]
        if name in self.tfvars:
            return self.tfvars[name]
        return self.defaults.get(name)

    def through(self, overrides: dict, resources: dict = None) -> "_Scope":
        """This scope as an instance of the module sees it."""
        return _Scope(self.defaults, self.locals, self.tfvars, overrides, resources, self.directory)


def _literal_text(value) -> str | None:
    """A resolved value written back as a literal `_resolve` reads, so an
    instance's argument can stand in for the module's variable."""
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, str):
        return json.dumps(value)
    if isinstance(value, list):
        items = [_literal_text(v) for v in value]
        return None if any(i is None for i in items) else "[" + ", ".join(items) + "]"
    return None


def _line_of(content: str, offset: int) -> int:
    return content.count("\n", 0, offset) + 1


# --- reading a block body --------------------------------------------------

def _expression_end(mask: str, start: int) -> int:
    """Index just past a right-hand side that starts at `start`: the first
    newline outside brackets, or the brace that closes the enclosing block.
    Counted in the mask, so brackets inside strings are text."""
    depth = 0
    for i in range(start, len(mask)):
        c = mask[i]
        if c in "[{(":
            depth += 1
        elif c in "]})":
            depth -= 1
            if depth < 0:
                return i
        elif c in "\n," and depth <= 0:
            # A depth-0 comma separates the members of a one-line object
            # constructor (`{ a = { ... }, b = { ... } }`); in a block body
            # it cannot occur outside brackets.
            return i
    return len(mask)


def _members(text: str, mask: str) -> list:
    """The depth-0 members of a block body, in order.

    Each is ("arg", name, start, stop) with the span of the raw right-hand
    side, or ("block", name, start, stop) with the span of the nested body.
    An object-valued argument (`remote_network_config = { ... }`) is an arg
    whose raw value starts with `{`; `_inner` turns either into a body span.
    """
    out = []
    i, n = 0, len(text)
    while i < n:
        match = _ASSIGN_RE.match(text, i)
        if match:
            end = _expression_end(mask, match.end())
            out.append(("arg", match.group(1), match.end(), end))
            if end < n and text[end] == ",":
                # The next member follows on the same line.
                i = end + 1
                continue
            i = end
        else:
            block = _BLOCK_START_RE.match(text, i)
            if block:
                open_at = block.end() - 1
                close_at = datastores._block_body(mask, open_at)
                name = block.group(1)
                if name == "dynamic" and block.group(2):
                    # `dynamic "route" {`: kept apart from a literal `route {}`
                    # so a caller can ask for either.
                    name = f"dynamic:{block.group(2)}"
                out.append(("block", name, open_at + 1, close_at))
                i = close_at + 1
        newline = text.find("\n", i)
        i = n if newline == -1 else newline + 1
    return out


class _Body:
    """One block body: its text and mask slices, and its members."""

    def __init__(self, text: str, mask: str, start: int, stop: int):
        self.text, self.mask = text[start:stop], mask[start:stop]
        self.start = start
        self.members = _members(self.text, self.mask)

    def arg(self, name: str) -> str | None:
        """Raw right-hand side of the first depth-0 argument `name`."""
        for kind, member, start, stop in self.members:
            if kind == "arg" and member == name:
                return self.text[start:stop].strip().rstrip(",").strip()
        return None

    def first_arg(self, names) -> tuple[str, str] | None:
        for name in names:
            raw = self.arg(name)
            if raw is not None:
                return name, raw
        return None

    def inner(self, name: str) -> "_Body | None":
        """The body of the first nested block or object-valued argument
        `name`, whichever form the file used."""
        for kind, member, start, stop in self.members:
            if member != name:
                continue
            if kind == "block":
                return _Body(self.text, self.mask, start, stop)
            raw_start = start
            while raw_start < stop and self.text[raw_start] in " \t":
                raw_start += 1
            if raw_start < stop and self.text[raw_start] == "{":
                close = datastores._block_body(self.mask, raw_start)
                return _Body(self.text, self.mask, raw_start + 1, close)
        return None

    def inners(self, name: str) -> list:
        """Every nested block `name`, for repeated blocks such as `route`."""
        return [_Body(self.text, self.mask, start, stop)
                for kind, member, start, stop in self.members
                if kind == "block" and member == name]

    def dynamics(self, name: str) -> list:
        """The `for_each` expression of every `dynamic "name"` block: what
        the files generate the blocks from, which the scan cannot expand."""
        return [_Body(self.text, self.mask, start, stop).arg("for_each") or "(no for_each)"
                for kind, member, start, stop in self.members
                if kind == "block" and member == f"dynamic:{name}"]


# --- resolving an expression --------------------------------------------------

def _split_top(raw: str) -> list:
    """Splits on the commas outside brackets and strings."""
    parts, depth, quoted, escaped, current = [], 0, False, False, []
    for c in raw:
        if quoted:
            current.append(c)
            if escaped:
                escaped = False
            elif c == "\\":
                escaped = True
            elif c == '"':
                quoted = False
            continue
        if c == '"':
            quoted = True
        elif c in "[{(":
            depth += 1
        elif c in "]})":
            depth -= 1
        elif c == "," and depth == 0:
            parts.append("".join(current))
            current = []
            continue
        current.append(c)
    tail = "".join(current)
    if tail.strip():
        parts.append(tail)
    return [p.strip() for p in parts if p.strip()]


def _cidrsubnet(prefix, newbits, netnum):
    """Terraform's cidrsubnet(), for literal arguments."""
    if not isinstance(prefix, str) or isinstance(newbits, bool) or isinstance(netnum, bool):
        return None
    if not isinstance(newbits, int) or not isinstance(netnum, int):
        return None
    try:
        network = ipaddress.ip_network(prefix, strict=False)
    except ValueError:
        return None
    new_prefix = network.prefixlen + newbits
    if newbits < 0 or new_prefix > network.max_prefixlen or not 0 <= netnum < 2 ** newbits:
        return None
    size = 2 ** (network.max_prefixlen - new_prefix)
    return str(ipaddress.ip_network((int(network.network_address) + netnum * size, new_prefix)))


def _resolve(raw: str | None, scope: _Scope, depth: int = 0, missing: set = None):
    """The literal value of a Terraform expression — a string, number, bool
    or list of those — or None when the files do not state it. With a
    `missing` set, every VPC output the expression names that the scope
    has no value for yet is added to it, as (directory, attribute)."""
    if raw is None or depth > MAX_RESOLVE_DEPTH:
        return None
    raw = raw.strip().rstrip(",").strip()
    if not raw:
        return None
    literal = datastores._literal(raw)
    if literal is not None:
        return literal
    # Terraform 0.11 style: `"${var.cidr}"` is the expression inside, when
    # nothing else is in the string.
    match = _PURE_INTERPOLATION_RE.match(raw)
    if match:
        return _resolve(match.group(1), scope, depth + 1, missing)
    if raw.startswith("[") and raw.endswith("]"):
        items = []
        for item in _split_top(raw[1:-1]):
            value = _resolve(item, scope, depth + 1, missing)
            if value is None:
                return None
            items.extend(value if isinstance(value, list) else [value])
        return items
    match = _VAR_RE.match(raw)
    if match:
        return _resolve(scope.variable(match.group(1)), scope, depth + 1, missing)
    match = _LOCAL_RE.match(raw)
    if match:
        return _resolve(scope.locals.get(match.group(1)), scope, depth + 1, missing)
    if _VPC_ATTR_RE.match(raw):
        value = scope.resources.get(raw)
        if value is None and missing is not None:
            missing.add((scope.directory, raw))
        return _resolve(value, scope, depth + 1, missing)
    match = _CALL_RE.match(raw)
    if match:
        name, inner = match.group(1), _split_top(match.group(2))
        if name in ("tolist", "toset", "flatten", "compact", "sort", "distinct") and len(inner) == 1:
            value = _resolve(inner[0], scope, depth + 1, missing)
            if not isinstance(value, list):
                return None
            # The order and the members matter downstream (`azs[index]` pairs
            # with them), so each does what Terraform does: sort() orders,
            # distinct() dedupes keeping first occurrences, toset() dedupes
            # (a set has no order; sorted here, as Terraform's examples show),
            # compact() drops null and
            # empty strings; tolist() and flatten() over a flat list of
            # strings are identities.
            if name == "sort":
                return sorted(value, key=str)
            if name == "distinct":
                return list(dict.fromkeys(value))
            if name == "toset":
                return sorted(set(value), key=str)
            if name == "compact":
                return [v for v in value if v not in (None, "")]
            return value
        if name == "cidrsubnet" and len(inner) == 3:
            return _cidrsubnet(*(_resolve(p, scope, depth + 1, missing) for p in inner))
        if name == "cidrsubnets" and len(inner) >= 2:
            prefix = _resolve(inner[0], scope, depth + 1, missing)
            bits = [_resolve(p, scope, depth + 1, missing) for p in inner[1:]]
            return _cidrsubnets(prefix, bits)
    return None


def _describe(raw: str, scope: "_Scope | None", depth: int = 0) -> str:
    """`raw`, with a `var.`/`local.` chain expanded to what it names."""
    raw = (raw or "").strip().rstrip(",").strip()
    if scope is None or depth >= 3:
        return raw
    match = _VAR_RE.match(raw) or _LOCAL_RE.match(raw)
    if not match:
        return raw
    definition = scope.variable(match.group(1)) if raw.startswith("var.") else scope.locals.get(match.group(1))
    if definition is None:
        return raw + (" (no default)" if raw.startswith("var.") else "")
    return f"{raw} = {_describe(definition, scope, depth + 1)}"


def _cidrsubnets(prefix, newbits_list):
    """Terraform's cidrsubnets(): consecutive subnets of the given sizes."""
    if not isinstance(prefix, str) or any(
            not isinstance(b, int) or isinstance(b, bool) or b <= 0 for b in newbits_list):
        return None
    try:
        network = ipaddress.ip_network(prefix, strict=False)
    except ValueError:
        return None
    out, cursor = [], int(network.network_address)
    end = int(network.broadcast_address) + 1
    for bits in newbits_list:
        new_prefix = network.prefixlen + bits
        if new_prefix > network.max_prefixlen:
            return None
        size = 2 ** (network.max_prefixlen - new_prefix)
        if cursor % size:
            cursor += size - cursor % size
        if cursor + size > end:
            return None
        out.append(str(ipaddress.ip_network((cursor, new_prefix))))
        cursor += size
    return out


def _cidr(value) -> str | None:
    """`value` as a normalized CIDR string, or None when it is not one."""
    if not isinstance(value, str) or "/" not in value:
        return None
    try:
        return str(ipaddress.ip_network(value.strip(), strict=False))
    except ValueError:
        return None


def _cidr_list(value) -> list | None:
    if isinstance(value, str):
        # A CloudFormation CommaDelimitedList default is one string.
        value = [v.strip() for v in value.split(",")] if "," in value else [value]
    if not isinstance(value, list):
        return None
    cidrs = [_cidr(v) for v in value]
    return None if any(c is None for c in cidrs) else cidrs


def _module_outputs(raw: str) -> list:
    return [f"module.{m}.{o}" for m, o in _MODULE_OUTPUT_RE.findall(raw or "")]


def _resource_refs(raw: str, types: tuple) -> list:
    return [f"{prefix}{rtype}.{name}" for prefix, rtype, name in _RESOURCE_REF_RE.findall(raw or "")
            if rtype in types]


def _vpc_ref(raw: str | None) -> str | None:
    """The VPC a `vpc_id` expression names: a VPC resource, a data source, or
    a module output (its output name dropped — the module IS the VPC)."""
    if not raw:
        return None
    refs = _resource_refs(raw, ("aws_vpc",))
    if refs:
        return refs[0]
    match = _MODULE_OUTPUT_RE.search(raw)
    return f"module.{match.group(1)}" if match else None


def _subnet_refs(raw: str | None) -> list:
    """The subnets a `subnet_ids` expression names, deduplicated in order."""
    refs = _resource_refs(raw or "", ("aws_subnet",)) + _module_outputs(raw or "")
    return list(dict.fromkeys(refs))


# --- the harvest --------------------------------------------------------------

def _identity(entry: dict) -> tuple:
    """What tells two same-named entries apart: the directory the
    declaration lives in, its address, and — for a view of a module
    instantiated more than once — the instance's directory."""
    return entry.get("directory", os.path.dirname(entry.get("path", ""))), entry["address"], entry.get("_instance_dir")


class _Harvest:
    """Collects declarations during the walk and resolves them at the end,
    once every directory's variables and locals are known."""

    def __init__(self):
        self.vpcs = []
        self.subnets = []
        self.clusters = []
        self.routes = []
        self.unresolved = []
        self.notes = []
        self.scopes = {}
        # Terraform declarations waiting on their directory's scope.
        self.pending = []
        # `.tfvars` files Terraform would only load with -var-file, counted
        # for one note.
        self.skipped_tfvars = 0
        self.tfvars_files = []
        # CloudFormation secondary ranges, attached once the template's VPCs
        # are all read (a VPCCidrBlock may precede its VPC in the file).
        self.cfn_secondary = []
        # Directories a local module instance points at (`source = "../../modules/vpc"`),
        # mapped to the instances: (label, instance directory, instance body).
        # A range such a directory reads from an input variable is stated at
        # the instance, so it is resolved there, through the value the
        # instance passes, rather than asked of the user.
        self.module_sources = {}
        # (instance directory, "module.<name>") -> the module's directory, for
        # following a cluster's `vpc_id = module.network.vpc_id` to the VPC
        # declared inside the module.
        self.module_targets = {}
        # Module directories whose input variables were actually consulted,
        # so the note about them is written only where it explains something.
        self.module_inputs_used = set()

    def scope(self, directory: str) -> _Scope:
        return self.scopes.setdefault(directory, _Scope(directory=directory))

    def unresolvable(self, address: str, path: str, argument: str, expression: str,
                     scope: "_Scope | None" = None):
        """Records a range the files do not state. With a scope, a `var.` or
        `local.` chain is spelled out to the expression it ends in, so the
        reader sees `local.private_subnets = [for ... cidrsubnet(...)]` and
        not just the name."""
        self.unresolved.append({
            "address": address, "path": path, "argument": argument,
            "expression": _describe(expression, scope)[:300],
        })

    # -- Terraform ------------------------------------------------------------

    def read_terraform(self, rel_path: str, content: str):
        directory = os.path.dirname(rel_path)
        scope = self.scope(directory)
        text, mask, lex_notes, _ = datastores.scan_source(content)
        for note in lex_notes:
            self.notes.append(f"{rel_path}: {note}")
        for match in _LOCALS_RE.finditer(text):
            open_at = match.end() - 1
            body = _Body(text, mask, open_at + 1, datastores._block_body(mask, open_at))
            for kind, name, start, stop in body.members:
                if kind == "arg":
                    scope.locals.setdefault(name, body.text[start:stop])
        blocks, truncated = datastores.iter_terraform_blocks(
            text, mask, kinds=("resource", "module", "variable"))
        if truncated:
            self.notes.append(f"{rel_path}: a block is never closed, so that block "
                              "and everything after it were not read")
        for kind, first, second, _args, _declared, body_start, body_end in blocks:
            body = _Body(text, mask, body_start, body_end)
            where = f"{rel_path}:{_line_of(content, body_start)}"
            if kind == "variable":
                default = body.arg("default")
                if default is not None:
                    scope.defaults.setdefault(first, default)
                continue
            if kind == "module":
                before = len(self.pending)
                self._module(first, body, rel_path, where, directory)
                self._annotate_iterator(before, body)
                continue
            handler = {
                "aws_vpc": self._aws_vpc,
                "aws_vpc_ipv4_cidr_block_association": self._aws_vpc_secondary,
                "aws_subnet": self._aws_subnet,
                "aws_eks_cluster": self._aws_eks_cluster,
                "aws_route": self._aws_route,
                "aws_vpn_connection_route": self._aws_route,
                "aws_ec2_transit_gateway_route": self._aws_route,
                "aws_route_table": self._aws_route_table,
                "aws_default_route_table": self._aws_route_table,
            }.get(first)
            if handler:
                before = len(self.pending)
                handler(f"{first}.{second}", second, body, rel_path, where, directory)
                self._annotate_iterator(before, body)

    def _annotate_iterator(self, before: int, body: "_Body"):
        """A block with count/for_each: a range written as each.value or
        indexed by count.index is a question that has to name what the
        block iterates over, not just the iterator variable."""
        iterator = body.arg("for_each") or body.arg("count")
        if iterator is not None:
            for _, _, record in self.pending[before:]:
                record["iterator"] = iterator

    def read_tfvars(self, rel_path: str, content: str):
        """Collects an auto-loaded .tfvars file; `apply_tfvars` lays them
        over the variables in Terraform's order once the walk is done."""
        name = os.path.basename(rel_path)
        if name not in ("terraform.tfvars", "terraform.tfvars.json") and not name.endswith(
                (".auto.tfvars", ".auto.tfvars.json")):
            self.skipped_tfvars += 1
            return
        self.tfvars_files.append((rel_path, content))

    def apply_tfvars(self):
        """Terraform loads terraform.tfvars, then terraform.tfvars.json, then
        every *.auto.tfvars and *.auto.tfvars.json in lexical order of their
        names, and a later file overrides an earlier one; for the root
        module only. A JSON file's values are written back as the literals
        `_resolve` reads. Runs after the walk, so every local module
        instance is known by then."""
        def order(item):
            rel_path = item[0]
            name = os.path.basename(rel_path)
            return (os.path.dirname(rel_path),
                    name not in ("terraform.tfvars", "terraform.tfvars.json"), name)
        for rel_path, content in sorted(self.tfvars_files, key=order):
            directory = os.path.dirname(rel_path)
            if directory in self.module_sources:
                # Terraform loads .tfvars for the root module only. A file
                # left in a module directory changes nothing when the module
                # is applied through its instances, so reading it would
                # record a range with evidence that Terraform never builds.
                self.notes.append(f"{rel_path}: not read; Terraform loads .tfvars files for "
                                  f"the root module only, and {directory or 'the repository root'} "
                                  "is instantiated as a module")
                continue
            scope = self.scope(directory)
            if rel_path.endswith(TFVARS_JSON_EXTENSIONS):
                try:
                    values = json.loads(content)
                except ValueError as e:
                    self.notes.append(f"{rel_path}: not parseable as JSON, so its values "
                                      f"were not read ({e})")
                    continue
                for key, value in (values.items() if isinstance(values, dict) else ()):
                    text = _literal_text(value)
                    if text is not None:
                        scope.tfvars[key] = text
                continue
            text, mask, _, _ = datastores.scan_source(content)
            body = _Body(text, mask, 0, len(text))
            for kind, key, start, stop in body.members:
                if kind == "arg":
                    scope.tfvars[key] = body.text[start:stop]

    def _aws_vpc(self, address, name, body, rel_path, where, directory):
        self.pending.append(("vpc", directory, {
            "name": name, "address": address, "path": rel_path, "where": where,
            "form": "resource", "cidr_raw": body.arg("cidr_block"),
            "secondary_raw": [], "ipam": body.arg("ipv4_ipam_pool_id") is not None,
        }))

    def _aws_vpc_secondary(self, address, name, body, rel_path, where, directory):
        self.pending.append(("secondary", directory, {
            "address": address, "path": rel_path, "where": where,
            "vpc_raw": body.arg("vpc_id"), "cidr_raw": body.arg("cidr_block"),
            "ipam": body.arg("ipv4_ipam_pool_id") is not None,
        }))

    def _aws_subnet(self, address, name, body, rel_path, where, directory):
        tier = None
        if _INTERNAL_ELB_TAG_RE.search(body.text):
            tier = "private"
        elif _PUBLIC_ELB_TAG_RE.search(body.text) or _MAP_PUBLIC_RE.search(body.text):
            tier = "public"
        self.pending.append(("subnet", directory, {
            "name": name, "address": address, "path": rel_path, "where": where,
            "form": "resource", "cidr_raw": body.arg("cidr_block"),
            "vpc_raw": body.arg("vpc_id"), "az_raw": body.arg("availability_zone"),
            "tier": tier,
        }))

    def _aws_eks_cluster(self, address, name, body, rel_path, where, directory):
        vpc_config = body.inner("vpc_config")
        network = body.inner("kubernetes_network_config")
        remote = body.inner("remote_network_config")
        record = {
            "address": address, "path": rel_path, "where": where, "form": "resource",
            "name_raw": body.arg("name"), "vpc_raw": None, "subnets_raw": None,
            "service_raw": None, "ip_family_raw": None, "public_raw": None,
            "remote_node_raw": None, "remote_pod_raw": None, "local_target": None,
        }
        if vpc_config is not None:
            record["subnets_raw"] = vpc_config.arg("subnet_ids")
            record["public_raw"] = vpc_config.arg("public_access_cidrs")
        if network is not None:
            record["service_raw"] = network.arg("service_ipv4_cidr")
            record["ip_family_raw"] = network.arg("ip_family")
        self._remote_networks(remote, record)
        self.pending.append(("cluster", directory, record))

    def _remote_networks(self, remote: "_Body | None", record: dict):
        if remote is None:
            return
        for block_name, key in (("remote_node_networks", "remote_node_raw"),
                                ("remote_pod_networks", "remote_pod_raw")):
            inner = remote.inner(block_name)
            if inner is not None:
                record[key] = inner.arg("cidrs")
            elif remote.dynamics(block_name):
                # Generated blocks: the ranges live in the for_each value.
                record[key] = remote.dynamics(block_name)[0]

    def _module(self, name, body, rel_path, where, directory):
        source = _resolve(body.arg("source"), _Scope()) or ""
        source = source if isinstance(source, str) else ""
        address = f"module.{name}"
        # The source's last path segment has to be exactly `vpc` or `eks`
        # (_VPC_SOURCE_RE, _EKS_SOURCE_RE): `eks-blueprints-addons` is not
        # `eks`, and a registry sub-module (`.../eks/aws//modules/karpenter`,
        # `.../vpc/aws//modules/vpc-endpoints`) is neither the VPC nor the
        # cluster, whatever its parent is called.
        local_target = None
        if _LOCAL_SOURCE_RE.match(source):
            # A module that instantiates itself (`source = "./"`), which no
            # plan can apply, is registered like any other; `_views` refuses
            # the instance, so following it ends.
            local_target = os.path.normpath(os.path.join(directory, source))
            if local_target == ".":
                # `source = "../.."` from envs/prod: the repository root, whose
                # declarations are keyed by the empty directory (the shape of a
                # module repository with an examples/ tree).
                local_target = ""
            self.module_sources.setdefault(local_target, []).append(
                (f"{address} ({rel_path})", directory, body))
            self.module_targets[(directory, address)] = local_target
        # A local module's declarations are read from its own directory, so
        # only its source name says whether the instance IS the VPC; a
        # registry or git module with a `cidr` argument is one by that alone.
        is_vpc = bool(_VPC_SOURCE_RE.search(source)) or (
            body.arg("cidr") is not None and local_target is None)
        is_eks = bool(_EKS_SOURCE_RE.search(source)) or (
            body.first_arg(EKS_MODULE_SERVICE_ARGS + EKS_MODULE_PUBLIC_ARGS) is not None
            or any(body.inner(name) is not None for name in EKS_MODULE_REMOTE_ARGS))
        if is_vpc:
            self.pending.append(("vpc", directory, {
                "name": name, "address": address, "path": rel_path, "where": where,
                "form": "module", "cidr_raw": body.arg("cidr"), "local_target": local_target,
                "secondary_raw": [body.arg("secondary_cidr_blocks")]
                if body.arg("secondary_cidr_blocks") is not None else [],
                "ipam": body.arg("ipv4_ipam_pool_id") is not None,
                "subnet_lists": {arg: body.arg(arg) for arg in MODULE_SUBNET_ARGS
                                 if body.arg(arg) is not None},
                "azs_raw": body.arg("azs"),
            }))
        if is_eks:
            record = {
                "address": address, "path": rel_path, "where": where, "form": "module",
                "name_raw": (body.first_arg(EKS_MODULE_NAME_ARGS) or (None, None))[1],
                "vpc_raw": body.arg("vpc_id"), "subnets_raw": body.arg("subnet_ids"),
                "local_target": local_target,
                "service_raw": (body.first_arg(EKS_MODULE_SERVICE_ARGS) or (None, None))[1],
                "ip_family_raw": (body.first_arg(EKS_MODULE_IP_FAMILY_ARGS) or (None, None))[1],
                "public_raw": (body.first_arg(EKS_MODULE_PUBLIC_ARGS) or (None, None))[1],
                "remote_node_raw": None, "remote_pod_raw": None,
            }
            self._remote_networks(next((body.inner(name) for name in EKS_MODULE_REMOTE_ARGS
                                        if body.inner(name) is not None), None), record)
            self.pending.append(("cluster", directory, record))

    def _aws_route(self, address, name, body, rel_path, where, directory):
        self._route(address, body, rel_path, where, directory)

    def _aws_route_table(self, address, name, body, rel_path, where, directory):
        for index, route in enumerate(body.inners("route")):
            self._route(f"{address}.route[{index}]", route, rel_path, where, directory,
                        destination_arg="cidr_block")
        for index, for_each in enumerate(body.dynamics("route")):
            # Generated routes: the destinations live in whatever for_each
            # iterates over, which the scan does not expand. A question, so
            # the design does not treat the table as having no routes.
            self.pending.append(("route", directory, {
                "address": f"{address}.route[dynamic {index}]", "path": rel_path, "where": where,
                "destination_raw": for_each, "via": "dynamic", "argument": "dynamic route for_each",
            }))
        # The attributes-as-blocks form, `route = [ { cidr_block = ... } ]`,
        # which the scan does not walk: a question for the table rather than
        # a table with no routes. `route = []` clears the table and is not.
        listed = body.arg("route")
        if listed is not None and listed.strip().rstrip(",").strip() not in ("[]", ""):
            self.pending.append(("route", directory, {
                "address": f"{address}.route", "path": rel_path, "where": where,
                "destination_raw": listed, "via": "list", "argument": "route (attribute form)",
            }))

    def _route(self, address, body, rel_path, where, directory,
               destination_arg="destination_cidr_block"):
        target = body.first_arg(ROUTE_TARGET_ARGS)
        destination = body.arg(destination_arg)
        argument = destination_arg
        if destination is None:
            # A managed prefix list carries the destinations; the scan does
            # not follow it, so the route is a question rather than nothing.
            destination = body.arg("destination_prefix_list_id")
            argument = "destination_prefix_list_id"
        if destination is None or target is None:
            return
        self.pending.append(("route", directory, {
            "address": address, "path": rel_path, "where": where, "argument": argument,
            "destination_raw": destination, "via": target[0][:-3] if target[0].endswith("_id") else target[0],
        }))

    def _instances(self, directory: str, label=None) -> list:
        """The instances of the local module in `directory`, or just the one
        with `label`."""
        return [i for i in self.module_sources.get(directory) or ()
                if label is None or i[0] == label]

    def _views(self, directory: str, visiting: tuple = ()) -> list:
        """The copies a declaration in `directory` stands for.

        One, normally. A local module instantiated more than once is that
        many copies of everything in it, and so is a module instantiated once
        from inside a module that is itself instantiated twice: a copy is a
        chain of instances from the declaration out to a root module. Every
        declaration is recorded once per copy, under Terraform's absolute
        address (`module.stack.module.vpc.aws_vpc.main`), and references
        between declarations resolve within the same copy. With a single
        copy the address is left bare.
        """
        if directory in self._views_cache:
            return self._views_cache[directory]
        views = []
        for label, instance_dir, _ in self.module_sources.get(directory) or ():
            if instance_dir in visiting or instance_dir == directory:
                # Two directories instantiating each other: Terraform never
                # plans such a pair, and following it would never end.
                continue
            segment = label.split(" ")[0] + "."
            for parent in self._views(instance_dir, visiting + (directory,)):
                # The copy marker: the parent's, or, when the parent is a
                # single-instance wrapper (collapsed: chain kept, no marker)
                # or a root module, the parent's own directory. Two copies
                # of one module reached through two such wrappers must not
                # share a marker, or they would fold into one entry.
                root = parent.root_dir if parent.chain and parent.root_dir is not None else instance_dir
                views.append(_View((label,) + parent.chain, parent.prefix + segment, root, parent))
        if len(views) <= 1:
            # One copy (or none): plain address, no copy identity — but the
            # chain still tells `_effective` which instance to read through.
            views = [_View(views[0].chain if views else (), "", None, views[0].parent if views else None)]
        self._views_cache[directory] = views
        return views

    def _effective(self, directory: str, chain: tuple, visiting: tuple = ()) -> tuple:
        """The scope a declaration in `directory` resolves in, seen through
        the chain of instances `chain` (the direct instance first): the
        module's own scope with the instance's arguments laid over its
        variables, each argument resolved in the instance's own effective
        scope, so a wrapper around a wrapper is followed as far as it goes.
        An argument the files do not resolve is laid over too, as a marker
        `_resolve` cannot read: the module's default must not stand in for a
        value the instance meant to supply.

        Returns (scope, instance label or None, instance directory or None,
        raw arguments by name).
        """
        base = self.scope(directory)
        found = self._instances(directory, chain[0] if chain else None)
        if not found or directory in visiting:
            return base, None, None, {}
        label, instance_dir, body = found[0]
        key = (directory, chain)
        if key in self._effective_cache:
            return self._effective_cache[key]
        instance_scope = self._effective(instance_dir, chain[1:], visiting + (directory,))[0]
        overrides, raw_args = {}, {}
        # Outputs named and not found, here or further out on the chain: an
        # argument cut from one may resolve on a later call, once the VPC
        # that produces it has finished (resolve_terraform works in rounds),
        # so a scope with any is rebuilt on every call rather than cached.
        pending = set(instance_scope.pending_outputs)
        for kind, name, start, stop in body.members:
            if kind != "arg":
                continue
            raw = body.text[start:stop].strip().rstrip(",").strip()
            raw_args[name] = raw
            missing = set()
            text = _literal_text(_resolve(raw, instance_scope, missing=missing))
            if text is None:
                pending |= missing
                text = (f"{_describe(raw, instance_scope)} (passed by the instance {label}, "
                        "not a value the files state)")
            overrides[name] = text
        # The copy's registered ranges outlive the rebuilds.
        scope = base.through(overrides, self._resources_cache.setdefault(key, {}))
        scope.pending_outputs = pending
        result = (scope, label, instance_dir, raw_args)
        if not pending:
            self._effective_cache[key] = result
        return result

    def _via_instance(self, raw, scope: _Scope, label, directory: str) -> str:
        """The evidence suffix when `raw` is a bare module input the instance
        `label` supplied; "" otherwise."""
        match = _VAR_RE.match((raw or "").strip().rstrip(",").strip())
        if not match or label is None or match.group(1) not in scope.overrides:
            return ""
        self.module_inputs_used.add(directory)
        value = _resolve(scope.overrides[match.group(1)], scope)
        return f", passed by the instance {label}: {match.group(1)} = {value}"

    def _origin(self, raw, scope: _Scope) -> str:
        """The evidence suffix saying where a resolved value came from when
        the line does not state it: "" for a literal; else the variable and
        whether its value is the instance's, an auto-loaded .tfvars file's
        (naming the default it overrode) or the default; a local; a VPC's
        stated range; or an expression over those. A Gate A/B reviewer can
        then tell a literal from a default that CI overrides with -var."""
        text = (raw or "").strip().rstrip(",").strip()
        if not text or datastores._literal(text) is not None:
            return ""
        if text.startswith("[") and text.endswith("]") and all(
                datastores._literal(item.strip()) is not None
                for item in _split_top(text[1:-1]) if item.strip()):
            return ""  # a list of literals is as stated as one literal
        match = _VAR_RE.match(text)
        if match:
            name = match.group(1)
            if name in scope.overrides:
                return f" ({text}, passed by the module instance)"
            if name in scope.tfvars:
                default = _literal_text(_resolve(scope.defaults.get(name), scope))
                return (f" ({text}, from an auto-loaded .tfvars file"
                        + (f", over the default {default}" if default is not None else "") + ")")
            return f" ({text}, the variable's default)"
        if _LOCAL_RE.match(text):
            return f" ({text}, a local)"
        if _VPC_ATTR_RE.match(text):
            return f" ({text}, that VPC's stated range)"
        return f" ({text[:120]}, an expression over stated values)"

    def _resolve_range(self, raw, scope: _Scope, address: str, path: str, argument: str,
                       expect_list: bool = False):
        """A CIDR (or list of CIDRs) from `raw` in `scope`. Records the
        unresolved entry itself and returns None when the files do not state
        it; returns None without a record when `raw` is None."""
        if raw is None:
            return None
        reader = _cidr_list if expect_list else _cidr
        value = reader(_resolve(raw, scope))
        if value is not None:
            return value
        iterator = getattr(self, "_iterator", None)
        if iterator is not None and re.search(r"\b(each|count)\.", raw):
            self.unresolvable(address, path, argument,
                              f"{raw.strip()} over for_each/count = {_describe(iterator, scope)}")
            return None
        self.unresolvable(address, path, argument, raw, scope)
        return None

    def resolve_terraform(self):
        # VPCs first, whatever file order the walk read them in: a secondary
        # range association or a subnet in an earlier-sorting file attaches
        # to its VPC entry rather than creating a placeholder beside it.
        self._effective_cache = {}
        self._resources_cache = {}
        self._views_cache = {}
        self.dirs_with_vpcs = {directory for kind, directory, record in self.pending if kind == "vpc"}
        self.dirs_with_clusters = {directory for kind, directory, record in self.pending
                                   if kind == "cluster"}
        # Declared VPCs first, then module instances from the leaf of the
        # instance tree outward (a wrapper's `module "vpc"` before the
        # `module "network"` that instantiates the wrapper, whatever the
        # directory names sort like), then everything that attaches to a VPC.
        module_vpc_dirs = {directory for kind, directory, record in self.pending
                           if kind == "vpc" and record["form"] == "module"}

        def depth(directory, seen=()):
            """How many local module levels sit below `directory`'s module
            instances; 0 when they are all registry or git modules."""
            if directory in seen:
                return 0
            below = [depth(target, seen + (directory,))
                     for (instance_dir, _), target in self.module_targets.items()
                     if instance_dir == directory and target in module_vpc_dirs]
            return 1 + max(below) if below else 0

        def rank(item):
            kind, directory, record = item
            if kind != "vpc":
                return (3, 0)
            if record["form"] == "resource":
                return (0, 0)
            target = record.get("local_target")
            return (1, 1 + depth(target) if target in module_vpc_dirs else 0)
        ordered = sorted(self.pending, key=rank)
        # VPCs first, all of them, in rounds: a VPC whose range is cut from
        # another VPC's output (a spoke passed `cidr =
        # cidrsubnet(module.hub.vpc_cidr_block, 2, 1)`, or declared through
        # a local that reads `aws_vpc.hub.cidr_block`) waits for the round
        # after that output is registered; finished earlier, it would be a
        # question, and which VPC finished first would depend on the walk
        # order of the directories. A round with no progress finishes what
        # is left as it stands (a cycle, or an output nothing registers):
        # questions, not a hang. Then everything that reads the ranges.
        vpcs = [item for item in ordered if item[0] == "vpc"]
        while vpcs:
            ready = [item for item in vpcs if not self._waits_for_an_output(item, vpcs)] or vpcs
            for kind, directory, record in ready:
                self._iterator = record.get("iterator")
                for view in self._views(directory):
                    self._finish_vpc(record, view)
            self._register_module_ranges()
            done = {id(item) for item in ready}
            vpcs = [item for item in vpcs if id(item) not in done]
        for kind, directory, record in ordered:
            if kind == "vpc":
                continue
            self._iterator = record.get("iterator")
            for view in self._views(directory):
                getattr(self, f"_finish_{kind}")(record, view)
        self._iterator = None

    def _waits_for_an_output(self, item, unfinished) -> bool:
        """True when the VPC record `item` cannot finish yet: it is a local
        module instance standing for a VPC that is still unfinished inside
        the module (the instance attaches to that entry), or its range, or
        an argument of an instance on the way out to a root module, reads
        the output of another unfinished VPC record — `aws_vpc.hub.cidr_block`,
        `module.hub.vpc_cidr_block` — wherever the reference sits in the
        expression. Decided by resolving, not by reading the text: `_resolve`
        reports the outputs it looked for and did not find, and the scope
        carries the ones its instance arguments missed."""
        _, directory, record = item
        outputs, pending_dirs = set(), set()
        for other in unfinished:
            if other is item:
                continue
            _, other_dir, other_record = other
            pending_dirs.add(other_dir)
            attribute = "vpc_cidr_block" if other_record["form"] == "module" else "cidr_block"
            outputs.add((other_dir, f"{other_record['address']}.{attribute}"))
        target = record.get("local_target")
        if record["form"] == "module" and target and pending_dirs & ({target} | self._reachable(target)):
            return True
        for (instance_dir, address), target in self.module_targets.items():
            if pending_dirs & ({target} | self._reachable(target)):
                outputs.add((instance_dir, f"{address}.vpc_cidr_block"))
        if not outputs:
            return False
        for view in self._views(directory):
            scope = self._effective(directory, view.chain)[0]
            missing = set(scope.pending_outputs)
            _resolve(record.get("cidr_raw"), scope, missing=missing)
            if missing & outputs:
                return True
        return False

    def _register_module_ranges(self):
        """`module.<label>.vpc_cidr_block` for every local module instance
        whose module declares the VPC (itself, or a module inside it): the
        range of the VPC entry this copy stands for, registered in the
        instance's scope so a subnet or route beside it resolves
        `cidrsubnet(module.network.vpc_cidr_block, ...)` whatever the module
        directory is called. A registry module's entry registers its own
        output in `_finish_vpc`."""
        by_identity = {_identity(v): v for v in self.vpcs}
        for (instance_dir, address), _target in self.module_targets.items():
            for view in self._views(instance_dir):
                identity = self._inner_vpc(instance_dir, view.prefix + address, view.root_dir)
                entry = by_identity.get(identity)
                if entry is None or not entry.get("cidr"):
                    continue
                scope = self._effective(instance_dir, view.chain)[0]
                scope.resources.setdefault(f"{address}.vpc_cidr_block", _literal_text(entry["cidr"]))

    def _reachable(self, directory: str, depth: int = 0) -> set:
        """Directories reachable from `directory` through local module
        instances declared in it."""
        out = set()
        if depth > 3:
            return out
        for (instance_dir, _), target in self.module_targets.items():
            if instance_dir == directory and target not in out:
                out.add(target)
                out |= self._reachable(target, depth + 1)
        return out

    def _inner_vpc(self, directory: str, vpc: str | None, root_dir=None) -> tuple:
        """The identity (directory, address, copy root) of the VPC `vpc`
        names from `directory`, `vpc` being the reference under its copy's
        prefix (`module.stack.module.network`): a local module instance
        becomes the VPC declared inside the module — this copy's own when
        there are several (its absolute address extends the instance's and
        it belongs to the same root), else the single VPC the module, or a
        module inside it, declares. Unchanged when `vpc` is not such an
        instance."""
        if not vpc:
            return directory, vpc, root_dir
        target = None
        for (instance_dir, bare), candidate in self.module_targets.items():
            if instance_dir == directory and (vpc == bare or vpc.endswith("." + bare)):
                target = candidate
                break
        if target is not None:
            copy_root = root_dir if root_dir is not None else directory
            mine = [v for v in self.vpcs
                    if v["address"].startswith(vpc + ".") and v.get("_instance_dir") == copy_root]
            if len(mine) == 1:
                return _identity(mine[0])
            by_prefix = [v for v in self.vpcs if v["address"].startswith(vpc + ".")]
            if len(by_prefix) == 1:
                return _identity(by_prefix[0])
            reachable = {target} | self._reachable(target)
            inner = [v for v in self.vpcs if v["directory"] in reachable and not v.get("_instance_dir")]
            if len(inner) == 1:
                return _identity(inner[0])
        return directory, vpc, root_dir

    def _local_ref(self, directory: str, raw, view, reader, depth: int = 0):
        """A reference (`vpc_id`, `subnet_ids`) as (directory it is followed
        from, references, copy root of that copy): from the instance's
        argument when the module reads it from an input (followed on through
        the chain when the instance passes an input of its own), else the
        local reference under this copy's prefix."""
        match = _VAR_RE.match((raw or "").strip().rstrip(",").strip())
        if match and view.chain and depth <= 3:
            _, label, instance_dir, raw_args = self._effective(directory, view.chain)
            if label is not None and match.group(1) in raw_args:
                self.module_inputs_used.add(directory)
                parent = view.parent or _View((), "", None, None)
                return self._local_ref(instance_dir, raw_args[match.group(1)], parent, reader, depth + 1)
        refs = reader(raw)
        if isinstance(refs, list):
            refs = [view.prefix + ref for ref in refs]
        elif refs:
            refs = view.prefix + refs
        return directory, refs, view.root_dir

    def _finish_vpc(self, record: dict, view):
        directory = os.path.dirname(record["path"])
        scope, label, _, _ = self._effective(directory, view.chain)
        address = view.prefix + record["address"]
        entry = {
            "name": record["name"], "address": address, "path": record["path"],
            "directory": directory, "form": record["form"],
            "cidr": None, "secondary_cidrs": [], "cluster_vpc": None, "evidence": [],
            "_instance_dir": view.root_dir,
        }
        arg = "cidr" if record["form"] == "module" else "cidr_block"
        inner_vpc = None
        target = record.get("local_target")
        if record["form"] == "module" and target and (
                ({target} | self._reachable(target)) & self.dirs_with_vpcs):
            # `module "vpc" { source = "../../modules/vpc" cidr = ... }`: the
            # VPC is the one declared inside the module, recorded from there
            # with the range this instance passes; an entry here would be the
            # same VPC a second time. Ranges the instance passes beyond the
            # CIDR (secondary blocks, subnet lists) still belong to it, so
            # they attach to the inner entry.
            identity = self._inner_vpc(directory, address, view.root_dir)
            found = next((v for v in self.vpcs if _identity(v) == identity), None)
            if found is not None:
                inner_vpc, entry = identity[1], found
        if inner_vpc:
            # The instance stands for the VPC inside the local module; its
            # `vpc_cidr_block` is registered by _register_module_ranges with
            # every other local module's, once the VPCs are all recorded.
            pass
        elif (cidr := _cidr(_resolve(record["cidr_raw"], scope))):
            entry["cidr"] = cidr
            # VPCs finish first (resolve_terraform), so a subnet's
            # `cidrsubnet(aws_vpc.main.cidr_block, ...)` or a route's
            # `aws_vpc.peer.cidr_block` in this directory resolves to it.
            attribute = "vpc_cidr_block" if record["form"] == "module" else "cidr_block"
            scope.resources[f"{record['address']}.{attribute}"] = _literal_text(cidr)
            via = self._via_instance(record["cidr_raw"], scope, label, directory)
            shown = (record["cidr_raw"].strip() + via if via
                     else f"{cidr}{self._origin(record['cidr_raw'], scope)}")
            entry["evidence"].append(f"{record['where']}: {arg} = {shown}")
        elif record["cidr_raw"] is not None:
            self._resolve_range(record["cidr_raw"], scope, address, record["path"], arg)
        elif record["ipam"]:
            entry["evidence"].append(f"{record['where']}: the CIDR is allocated from an IPAM pool, "
                                     "so the files do not state it")
        elif record["form"] == "module":
            entry["evidence"].append(f"{record['where']}: the instance passes no `cidr`, so the range "
                                     "is the module's own default or an argument the scan does not read")
        if not inner_vpc:
            self.vpcs.append(entry)
        if inner_vpc and entry.get("form") == "module":
            # The community module inside the wrapper reads the same
            # secondary blocks and subnet lists through this instance's
            # arguments and has recorded them (or their questions).
            return
        # A list the scan cannot resolve is filed under the VPC it belongs
        # to (the inner one, when the instance stands for a local module's
        # VPC), so a reader, and the design's triage, can see whose ranges
        # they are.
        list_address = inner_vpc or address
        list_path = entry["path"] if inner_vpc else record["path"]
        for raw in record["secondary_raw"]:
            cidrs = self._resolve_range(raw, scope, list_address, list_path,
                                        "secondary_cidr_blocks", expect_list=True)
            if cidrs:
                entry["secondary_cidrs"].extend(c for c in cidrs if c not in entry["secondary_cidrs"])
                entry["evidence"].append(f"{record['where']}: secondary_cidr_blocks = {cidrs}"
                                         f"{self._origin(raw, scope)}")
        azs = _resolve(record.get("azs_raw"), scope)
        azs = azs if isinstance(azs, list) else []
        for arg, raw in (record.get("subnet_lists") or {}).items():
            cidrs = self._resolve_range(raw, scope, list_address, list_path, arg, expect_list=True)
            if cidrs is None:
                continue
            for index, cidr in enumerate(cidrs):
                self.subnets.append({
                    "name": f"{record['name']}/{arg}[{index}]", "address": address,
                    "path": record["path"], "form": "module", "cidr": cidr,
                    "availability_zone": azs[index] if index < len(azs) and isinstance(azs[index], str) else None,
                    "tier": MODULE_SUBNET_ARGS[arg], "vpc": inner_vpc or address, "vpc_path": list_path,
                    "_vpc_dir": entry["directory"], "_vpc_inst": entry.get("_instance_dir"),
                    "_instance_dir": view.root_dir,
                    "evidence": [f"{record['where']}: {arg}[{index}] = {cidr}{self._origin(raw, scope)}"],
                })

    def _finish_secondary(self, record: dict, view):
        directory = os.path.dirname(record["path"])
        scope = self._effective(directory, view.chain)[0]
        vpc_dir, vpc, vpc_root = self._local_ref(directory, record["vpc_raw"], view, _vpc_ref)
        if vpc is None and record["vpc_raw"]:
            # `vpc_id = var.vpc_a_id`: not a reference the scan can follow,
            # but two associations on two such expressions are two VPCs.
            vpc = record["vpc_raw"].strip()[:80]
        identity = self._inner_vpc(vpc_dir, vpc, vpc_root)
        entry = next((v for v in self.vpcs if _identity(v) == identity), None)
        # A secondary range the files do not state is filed under the VPC
        # it extends, as `secondary_cidr_blocks`, the way a module's list
        # is: the triage then knows that VPC has a range nobody wrote down,
        # and a subnet cut from it (EKS custom networking) is a question,
        # not "inside a stated VPC range".
        address = view.prefix + record["address"]
        filed_under = identity[1] or address
        filed_path = entry["path"] if entry is not None else record["path"]
        if record["cidr_raw"] is None:
            if record.get("ipam"):
                self.unresolvable(filed_under, filed_path, "secondary_cidr_blocks",
                                  f"{address}: allocated from an IPAM pool (ipv4_ipam_pool_id), "
                                  "so the files do not state it")
            return
        cidr = self._resolve_range(record["cidr_raw"], scope, filed_under, filed_path,
                                   "secondary_cidr_blocks")
        if cidr is None:
            return
        if entry is None:
            # Built under the identity the lookup used, so a second
            # association naming the same undeclared VPC finds this entry.
            entry = {
                "name": identity[1] or record["address"], "address": identity[1],
                "path": record["path"], "directory": identity[0],
                "form": "resource", "cidr": None, "secondary_cidrs": [], "cluster_vpc": None,
                "evidence": [f"{record['where']}: the VPC itself is not declared in the scanned files"],
                "_instance_dir": identity[2],
            }
            self.vpcs.append(entry)
        if cidr not in entry["secondary_cidrs"]:
            entry["secondary_cidrs"].append(cidr)
        entry["evidence"].append(f"{record['where']}: {record['address']} cidr_block = {cidr}"
                                 f"{self._origin(record['cidr_raw'], scope)}")

    def _finish_subnet(self, record: dict, view):
        directory = os.path.dirname(record["path"])
        scope = self._effective(directory, view.chain)[0]
        address = view.prefix + record["address"]
        # An unresolved CIDR is a question, but the subnet still exists and
        # still links its cluster to its VPC: the entry stays, with cidr
        # null, so the cluster marking does not turn "unknown" into "false".
        cidr = self._resolve_range(record["cidr_raw"], scope, address, record["path"], "cidr_block")
        vpc_dir, vpc, vpc_root = self._local_ref(directory, record["vpc_raw"], view, _vpc_ref)
        # `vpc_id = module.vpc.vpc_id` with a local source: the VPC is the
        # one declared inside the module (VPCs finish first, so it is
        # recorded by now), and `vpc` has to be the address of a vpcs
        # entry for the triage to find the range that covers this subnet.
        vpc_dir, vpc, vpc_root = self._inner_vpc(vpc_dir, vpc, vpc_root)
        # The path of the VPC entry the reference resolved to: a namesake
        # VPC in this directory must not be mistaken for it downstream.
        vpc_entry = next((v for v in self.vpcs if _identity(v) == (vpc_dir, vpc, vpc_root)), None)
        az = _resolve(record["az_raw"], scope)
        self.subnets.append({
            "name": record["name"], "address": address, "path": record["path"],
            "form": "resource", "cidr": cidr,
            "availability_zone": az if isinstance(az, str) else None,
            "tier": record["tier"], "vpc": vpc, "vpc_path": vpc_entry["path"] if vpc_entry else None,
            "_vpc_dir": vpc_dir, "_vpc_inst": vpc_root,
            "_instance_dir": view.root_dir,
            "evidence": [f"{record['where']}: cidr_block = {cidr}{self._origin(record['cidr_raw'], scope)}" if cidr else
                         f"{record['where']}: cidr_block is an expression the scan does not "
                         "resolve (listed under unresolved)" if record["cidr_raw"] is not None else
                         f"{record['where']}: no cidr_block declared (an IPv6-only or "
                         "IPAM-allocated subnet)"],
        })

    def _finish_cluster(self, record: dict, view):
        directory = os.path.dirname(record["path"])
        target = record.get("local_target")
        if record["form"] == "module" and target and (
                ({target} | self._reachable(target)) & self.dirs_with_clusters):
            # `module "cluster" { source = "../../modules/cluster" ... }`: the
            # cluster is the one declared inside the module, recorded from
            # there with the values this instance passes.
            return
        scope = self._effective(directory, view.chain)[0]
        address = view.prefix + record["address"]
        vpc_dir, vpc, vpc_root = self._local_ref(directory, record["vpc_raw"], view, _vpc_ref)
        subnet_dir, subnets, subnet_root = self._local_ref(directory, record["subnets_raw"], view, _subnet_refs)
        name = _resolve(record["name_raw"], scope)
        entry = {
            "name": name if isinstance(name, str) else None,
            "address": address, "path": record["path"], "form": record["form"],
            "vpc": vpc, "vpc_path": None, "subnets": subnets or [],
            "service_ipv4_cidr": None, "ip_family": None, "public_access_cidrs": [],
            "remote_node_cidrs": [], "remote_pod_cidrs": [], "evidence": [],
            "_vpc_dir": vpc_dir, "_subnet_dir": subnet_dir, "_vpc_inst": vpc_root,
            "_subnet_inst": subnet_root, "_instance_dir": view.root_dir,
        }
        service = self._resolve_range(record["service_raw"], scope, address, record["path"],
                                      "service_ipv4_cidr")
        if service:
            entry["service_ipv4_cidr"] = service
            entry["evidence"].append(f"{record['where']}: service_ipv4_cidr = {service}"
                                     f"{self._origin(record['service_raw'], scope)}")
        family = _resolve(record["ip_family_raw"], scope)
        if isinstance(family, str):
            entry["ip_family"] = family.lower()
        for key, raw, arg in (("public_access_cidrs", record["public_raw"], "public_access_cidrs"),
                              ("remote_node_cidrs", record["remote_node_raw"], "remote_node_networks.cidrs"),
                              ("remote_pod_cidrs", record["remote_pod_raw"], "remote_pod_networks.cidrs")):
            cidrs = self._resolve_range(raw, scope, address, record["path"], arg, expect_list=True)
            if cidrs is None:
                continue
            entry[key] = cidrs
            entry["evidence"].append(f"{record['where']}: {arg} = {cidrs}{self._origin(raw, scope)}")
        self.clusters.append(entry)

    def _finish_route(self, record: dict, view):
        scope = self._effective(os.path.dirname(record["path"]), view.chain)[0]
        address = view.prefix + record["address"]
        destination = self._resolve_range(record["destination_raw"], scope, address,
                                          record["path"],
                                          record.get("argument", "destination_cidr_block"))
        if destination is None:
            return
        if _is_catch_all(destination):
            return
        self.routes.append({
            "destination": destination, "via": record["via"], "address": address,
            "path": record["path"],
            "evidence": [f"{record['where']}: destination {destination}"
                         f"{self._origin(record['destination_raw'], scope)} via {record['via']}"],
        })

    # -- eksctl ---------------------------------------------------------------

    def read_eksctl(self, rel_path: str, docs: list):
        for doc in docs:
            if not isinstance(doc, dict) or doc.get("kind") != "ClusterConfig":
                continue
            if not str(doc.get("apiVersion", "")).startswith("eksctl.io/"):
                continue
            self._eksctl_cluster(rel_path, doc)

    def _eksctl_value(self, rel_path: str, address: str, argument: str, value):
        """A literal from a ClusterConfig, or None (unresolved) when it is an
        `${ENV}` placeholder a pipeline fills in before eksctl reads the file."""
        if isinstance(value, str) and "${" in value:
            self.unresolvable(address, rel_path, argument, value)
            return None
        return value

    def _eksctl_cidr_list(self, rel_path, address, argument, value) -> list:
        value = self._eksctl_value(rel_path, address, argument, value)
        if value is None:
            return []
        if isinstance(value, list):
            value = [self._eksctl_value(rel_path, address, argument, v) for v in value]
            value = [v for v in value if v is not None]
        cidrs = _cidr_list(value)
        if cidrs is None:
            self.unresolvable(address, rel_path, argument, json.dumps(value))
            return []
        return cidrs

    def _eksctl_cluster(self, rel_path: str, doc: dict):
        meta = doc.get("metadata") if isinstance(doc.get("metadata"), dict) else {}
        address = f"ClusterConfig:{rel_path}"
        name = meta.get("name")
        # A `${ENV}` placeholder is not a name; nor is it a range, so it is
        # left null rather than listed under unresolved.
        name = name if isinstance(name, str) and "${" not in name else None
        vpc = doc.get("vpc") if isinstance(doc.get("vpc"), dict) else {}
        vpc_address = f"{address}#vpc"
        vpc_entry = {
            "name": name or os.path.splitext(os.path.basename(rel_path))[0],
            "address": vpc_address, "path": rel_path, "directory": os.path.dirname(rel_path),
            "form": "eksctl", "cidr": None, "secondary_cidrs": [], "cluster_vpc": True,
            "evidence": [],
        }
        subnets = vpc.get("subnets") if isinstance(vpc.get("subnets"), dict) else {}
        # eksctl imports an existing VPC when `vpc.id` is set, and also when
        # any subnet is named by id; the default CIDR applies only to a VPC
        # eksctl creates itself.
        existing_by_subnet = any(
            isinstance(spec, dict) and spec.get("id") is not None
            for tier in ("private", "public")
            if isinstance(subnets.get(tier), dict)
            for spec in subnets[tier].values())
        declared_cidr = self._eksctl_value(rel_path, vpc_address, "vpc.cidr", vpc.get("cidr"))
        if declared_cidr is not None:
            cidr = _cidr(declared_cidr)
            if cidr:
                vpc_entry["cidr"] = cidr
                vpc_entry["evidence"].append(f"{rel_path}: vpc.cidr = {cidr}")
            else:
                self.unresolvable(vpc_address, rel_path, "vpc.cidr", str(declared_cidr))
        elif vpc.get("id") is not None:
            vpc_entry["evidence"].append(
                f"{rel_path}: vpc.id = {vpc.get('id')} — an existing VPC whose CIDR the file does not state")
        elif existing_by_subnet:
            vpc_entry["evidence"].append(
                f"{rel_path}: subnets are named by id, so the cluster joins an existing VPC "
                "whose CIDR the file does not state")
        elif vpc.get("cidr") is None:
            # Absent, or written with no value (`cidr:`): eksctl treats both
            # as unset. A placeholder (`${VPC_CIDR}`) is a value, unresolved
            # above, and never the default.
            vpc_entry["cidr"] = EKSCTL_DEFAULT_VPC_CIDR
            vpc_entry["defaulted"] = True
            vpc_entry["evidence"].append(
                f"{rel_path}: no vpc.cidr and no vpc.id, so eksctl creates the VPC with its "
                f"default {EKSCTL_DEFAULT_VPC_CIDR}")
        # eksctl's secondary VPC ranges (custom networking). A placeholder
        # among them is filed under `secondary_cidr_blocks`, so the triage
        # treats this VPC as one with a range nobody stated, as it does for
        # the other two dialects.
        extra = self._eksctl_cidr_list(rel_path, vpc_address, "secondary_cidr_blocks", vpc.get("extraCIDRs"))
        if extra:
            vpc_entry["secondary_cidrs"] = extra
            vpc_entry["evidence"].append(f"{rel_path}: vpc.extraCIDRs = {extra}")
        self.vpcs.append(vpc_entry)

        subnet_addresses = []
        for tier in ("private", "public"):
            group = subnets.get(tier) if isinstance(subnets.get(tier), dict) else {}
            for key, spec in group.items():
                spec = spec if isinstance(spec, dict) else {}
                subnet_address = f"{vpc_address}.subnets.{tier}.{key}"
                cidr = None
                declared = self._eksctl_value(rel_path, subnet_address, "cidr", spec.get("cidr"))
                if declared is not None:
                    cidr = _cidr(declared)
                    if cidr is None:
                        self.unresolvable(subnet_address, rel_path, "cidr", str(declared))
                if cidr is None and spec.get("id") is None and spec.get("cidr") is None:
                    # Neither range nor id: `cidr:` with no value is unset,
                    # like an absent key; nothing here states a range.
                    continue
                az = spec.get("az") if isinstance(spec.get("az"), str) else (
                    key if _AZ_RE.match(str(key)) else None)
                subnet_addresses.append(subnet_address)
                self.subnets.append({
                    "name": str(key), "address": subnet_address, "path": rel_path,
                    "form": "eksctl", "cidr": cidr, "availability_zone": az, "tier": tier,
                    "vpc": vpc_address, "vpc_path": rel_path,
                    "evidence": [f"{rel_path}: vpc.subnets.{tier}.{key}"
                                 + (f".cidr = {cidr}" if cidr else
                                    f".id = {spec.get('id')}" if spec.get("id") is not None else
                                    ".cidr is a placeholder the scan does not resolve (listed under unresolved)")],
                })

        network = (doc.get("kubernetesNetworkConfig")
                   if isinstance(doc.get("kubernetesNetworkConfig"), dict) else {})
        remote = doc.get("remoteNetworkConfig") if isinstance(doc.get("remoteNetworkConfig"), dict) else {}
        entry = {
            "name": name, "address": address, "path": rel_path, "form": "eksctl",
            "vpc": vpc_address, "vpc_path": rel_path, "subnets": subnet_addresses,
            "service_ipv4_cidr": None, "ip_family": None, "public_access_cidrs": [],
            "remote_node_cidrs": [], "remote_pod_cidrs": [], "evidence": [],
        }
        service = self._eksctl_value(rel_path, address, "kubernetesNetworkConfig.serviceIPv4CIDR",
                                     network.get("serviceIPv4CIDR"))
        if service is not None:
            cidr = _cidr(service)
            if cidr:
                entry["service_ipv4_cidr"] = cidr
                entry["evidence"].append(f"{rel_path}: kubernetesNetworkConfig.serviceIPv4CIDR = {cidr}")
            else:
                self.unresolvable(address, rel_path, "kubernetesNetworkConfig.serviceIPv4CIDR", str(service))
        if isinstance(network.get("ipFamily"), str):
            entry["ip_family"] = network["ipFamily"].lower()
        if vpc.get("publicAccessCIDRs") is not None:
            entry["public_access_cidrs"] = self._eksctl_cidr_list(
                rel_path, address, "vpc.publicAccessCIDRs", vpc.get("publicAccessCIDRs"))
            if entry["public_access_cidrs"]:
                entry["evidence"].append(f"{rel_path}: vpc.publicAccessCIDRs = {entry['public_access_cidrs']}")
        for key, field in (("remoteNodeNetworks", "remote_node_cidrs"),
                           ("remotePodNetworks", "remote_pod_cidrs")):
            networks = remote.get(key) if isinstance(remote.get(key), list) else []
            for index, spec in enumerate(networks):
                cidrs = self._eksctl_cidr_list(
                    rel_path, address, f"remoteNetworkConfig.{key}[{index}].cidrs",
                    (spec or {}).get("cidrs") if isinstance(spec, dict) else None)
                entry[field].extend(c for c in cidrs if c not in entry[field])
                if cidrs:
                    entry["evidence"].append(f"{rel_path}: remoteNetworkConfig.{key}[{index}].cidrs = {cidrs}")
        self.clusters.append(entry)

    # -- CloudFormation ---------------------------------------------------------

    def read_cloudformation(self, rel_path: str, template: dict):
        resources = template.get("Resources")
        if not isinstance(resources, dict):
            return
        parameters = template.get("Parameters") if isinstance(template.get("Parameters"), dict) else {}
        defaults = {name: (spec or {}).get("Default") for name, spec in parameters.items()
                    if isinstance(spec, dict)}

        def value(logical: str, argument: str, raw):
            """A literal property value, following `Ref` to a parameter's
            Default; every other intrinsic is unresolved."""
            if isinstance(raw, dict):
                if set(raw) == {"Ref"} and isinstance(raw["Ref"], str):
                    if raw["Ref"] in defaults and defaults[raw["Ref"]] is not None:
                        return value(logical, argument, defaults[raw["Ref"]])
                    self.unresolvable(logical, rel_path, argument, json.dumps(raw))
                    return None
                self.unresolvable(logical, rel_path, argument, json.dumps(raw)[:200])
                return None
            if isinstance(raw, list):
                items = [value(logical, argument, item) for item in raw]
                return None if any(item is None for item in items) else items
            return raw

        def ref_of(raw) -> str | None:
            return raw["Ref"] if isinstance(raw, dict) and set(raw) == {"Ref"} else None

        def origin(raw) -> str:
            """The evidence suffix when the value came through a parameter's
            Default rather than the line itself: a parent stack or a pipeline
            may pass another value, which the scan does not read."""
            name = ref_of(raw)
            if name is not None and defaults.get(name) is not None:
                return f" (Ref {name}, the parameter's default)"
            return ""

        def cidr_of(logical: str, argument: str, raw, expect_list: bool = False):
            """A CIDR (or list) from a property: an intrinsic is recorded by
            value(); a literal that is not a CIDR (an empty parameter
            default, a typo) is recorded here, never dropped."""
            literal = value(logical, argument, raw)
            if literal is None:
                return None
            parsed = _cidr_list(literal) if expect_list else _cidr(literal)
            if parsed is None:
                self.unresolvable(logical, rel_path, argument, json.dumps(literal)[:200])
            return parsed

        for logical, resource in resources.items():
            if not isinstance(resource, dict):
                continue
            rtype = resource.get("Type")
            props = resource.get("Properties") if isinstance(resource.get("Properties"), dict) else {}
            where = f"{rel_path}: Resources.{logical}"
            if rtype == "AWS::EC2::VPC":
                entry = {
                    "name": logical, "address": logical, "path": rel_path,
                    "directory": os.path.dirname(rel_path), "form": "cloudformation",
                    "cidr": None, "secondary_cidrs": [], "cluster_vpc": None, "evidence": [],
                    # Two templates in one directory may share logical ids;
                    # the path is what tells their VPCs apart.
                    "_instance_dir": rel_path,
                }
                if "CidrBlock" in props:
                    cidr = cidr_of(logical, "CidrBlock", props["CidrBlock"])
                    if cidr:
                        entry["cidr"] = cidr
                        entry["evidence"].append(f"{where}.Properties.CidrBlock = {cidr}"
                                                 f"{origin(props['CidrBlock'])}")
                elif "Ipv4IpamPoolId" in props:
                    entry["evidence"].append(f"{where}: the CIDR is allocated from an IPAM pool, "
                                             "so the template does not state it")
                self.vpcs.append(entry)
            elif rtype == "AWS::EC2::VPCCidrBlock":
                vpc_ref = ref_of(props.get("VpcId"))
                if vpc_ref is None:
                    # `!ImportValue` or another intrinsic: the VPC is elsewhere,
                    # named by something the scan does not evaluate, but the
                    # range itself may be stated.
                    vpc_ref = json.dumps(props.get("VpcId"))[:80]
                # Filed under the VPC as `secondary_cidr_blocks` when not
                # stated, as in Terraform, so the triage sees the VPC has a
                # range nobody wrote down.
                if props.get("CidrBlock") is None:
                    if props.get("Ipv4IpamPoolId") is not None:
                        self.unresolvable(vpc_ref, rel_path, "secondary_cidr_blocks",
                                          f"{logical}: allocated from an IPAM pool (Ipv4IpamPoolId), "
                                          "so the template does not state it")
                    continue
                cidr = cidr_of(vpc_ref, "secondary_cidr_blocks", props["CidrBlock"])
                if cidr is None:
                    continue
                self.cfn_secondary.append((vpc_ref, cidr, where, origin(props["CidrBlock"])))
            elif rtype == "AWS::EC2::Subnet":
                # An intrinsic CIDR (`!Select [0, !Cidr [...]]`) is a question,
                # recorded by value(); the subnet entry stays, as in Terraform,
                # so the cluster keeps its link to the VPC.
                cidr = cidr_of(logical, "CidrBlock", props.get("CidrBlock")) if "CidrBlock" in props else None
                az = props.get("AvailabilityZone")
                tags = props.get("Tags") if isinstance(props.get("Tags"), list) else []
                keys = {str((t or {}).get("Key")) for t in tags if isinstance(t, dict)}
                tier = ("private" if "kubernetes.io/role/internal-elb" in keys
                        else "public" if "kubernetes.io/role/elb" in keys
                        or props.get("MapPublicIpOnLaunch") in (True, "true") else None)
                self.subnets.append({
                    "name": logical, "address": logical, "path": rel_path,
                    "form": "cloudformation", "cidr": cidr,
                    "availability_zone": az if isinstance(az, str) else None,
                    "tier": tier, "vpc": ref_of(props.get("VpcId")), "vpc_path": rel_path,
                    "_vpc_inst": rel_path, "_instance_dir": rel_path,
                    "evidence": [f"{where}.Properties.CidrBlock = {cidr}{origin(props.get('CidrBlock'))}" if cidr else
                                 f"{where}.Properties.CidrBlock is an intrinsic the scan does not "
                                 "evaluate (listed under unresolved)" if "CidrBlock" in props else
                                 f"{where}: no CidrBlock declared (an IPv6-only or IPAM-allocated subnet)"],
                })
            elif rtype == "AWS::EKS::Cluster":
                vpc_config = props.get("ResourcesVpcConfig") if isinstance(
                    props.get("ResourcesVpcConfig"), dict) else {}
                network = props.get("KubernetesNetworkConfig") if isinstance(
                    props.get("KubernetesNetworkConfig"), dict) else {}
                remote = props.get("RemoteNetworkConfig") if isinstance(
                    props.get("RemoteNetworkConfig"), dict) else {}
                # A name is not a range: an intrinsic here stays null and is
                # not a question for the user (as with eksctl's ${ENV} names).
                name = props.get("Name")
                if isinstance(name, dict) and set(name) == {"Ref"} and isinstance(defaults.get(name["Ref"]), str):
                    name = defaults[name["Ref"]]
                entry = {
                    "name": name if isinstance(name, str) else None,
                    "address": logical, "path": rel_path, "form": "cloudformation",
                    "vpc": None, "vpc_path": rel_path,
                    "subnets": [ref_of(s) for s in (vpc_config.get("SubnetIds") or [])
                                if ref_of(s)] if isinstance(vpc_config.get("SubnetIds"), list) else [],
                    "service_ipv4_cidr": None, "ip_family": None, "public_access_cidrs": [],
                    "remote_node_cidrs": [], "remote_pod_cidrs": [], "evidence": [],
                    "_vpc_inst": rel_path, "_subnet_inst": rel_path, "_instance_dir": rel_path,
                }
                if "ServiceIpv4Cidr" in network:
                    cidr = cidr_of(logical, "KubernetesNetworkConfig.ServiceIpv4Cidr",
                                   network["ServiceIpv4Cidr"])
                    if cidr:
                        entry["service_ipv4_cidr"] = cidr
                        entry["evidence"].append(f"{where}.Properties.KubernetesNetworkConfig.ServiceIpv4Cidr = {cidr}"
                                                 f"{origin(network['ServiceIpv4Cidr'])}")
                if isinstance(network.get("IpFamily"), str):
                    entry["ip_family"] = network["IpFamily"].lower()
                if "PublicAccessCidrs" in vpc_config:
                    cidrs = cidr_of(logical, "ResourcesVpcConfig.PublicAccessCidrs",
                                    vpc_config["PublicAccessCidrs"], expect_list=True)
                    if cidrs:
                        entry["public_access_cidrs"] = cidrs
                        entry["evidence"].append(f"{where}.Properties.ResourcesVpcConfig.PublicAccessCidrs = {cidrs}"
                                                 f"{origin(vpc_config['PublicAccessCidrs'])}")
                for key, field in (("RemoteNodeNetworks", "remote_node_cidrs"),
                                   ("RemotePodNetworks", "remote_pod_cidrs")):
                    for index, spec in enumerate(remote.get(key) or [] if isinstance(remote.get(key), list) else []):
                        cidrs = cidr_of(logical, f"RemoteNetworkConfig.{key}[{index}].Cidrs",
                                        (spec or {}).get("Cidrs") if isinstance(spec, dict) else None,
                                        expect_list=True)
                        if cidrs:
                            entry[field].extend(c for c in cidrs if c not in entry[field])
                            entry["evidence"].append(f"{where}.Properties.RemoteNetworkConfig.{key}[{index}].Cidrs = {cidrs}"
                                                     f"{origin((spec or {}).get('Cidrs') if isinstance(spec, dict) else None)}")
                self.clusters.append(entry)
            elif rtype == "AWS::EC2::Route":
                target = next((CFN_ROUTE_TARGETS[k] for k in CFN_ROUTE_TARGETS if k in props), None)
                if target is None:
                    continue
                if "DestinationCidrBlock" not in props:
                    if "DestinationPrefixListId" in props:
                        self.unresolvable(logical, rel_path, "DestinationPrefixListId",
                                          json.dumps(props["DestinationPrefixListId"])[:200])
                    continue
                destination = cidr_of(logical, "DestinationCidrBlock", props["DestinationCidrBlock"])
                if destination is None or _is_catch_all(destination):
                    continue
                via = target[:-3] if target.endswith("_id") else target
                self.routes.append({
                    "destination": destination, "via": via, "address": logical, "path": rel_path,
                    "evidence": [f"{where}.Properties.DestinationCidrBlock = {destination}"
                                 f"{origin(props['DestinationCidrBlock'])} via {via}"],
                })
        for vpc_ref, cidr, where, note in self.cfn_secondary:
            vpc = next((v for v in self.vpcs if v["address"] == vpc_ref and v["path"] == rel_path), None)
            if vpc is None:
                # A parameter, an import or an intrinsic names the VPC: it is
                # not declared in this template, but its extra range is, and
                # the target must keep clear of it all the same.
                vpc = {
                    "name": vpc_ref, "address": vpc_ref, "path": rel_path,
                    "directory": os.path.dirname(rel_path), "form": "cloudformation",
                    "cidr": None, "secondary_cidrs": [], "cluster_vpc": None,
                    "evidence": [f"{where}: the VPC itself is not declared in this template"],
                    "_instance_dir": rel_path,
                }
                self.vpcs.append(vpc)
            if cidr not in vpc["secondary_cidrs"]:
                vpc["secondary_cidrs"].append(cidr)
                vpc["evidence"].append(f"{where}.Properties.CidrBlock = {cidr}{note}")
        self.cfn_secondary = []

    # -- assembly ---------------------------------------------------------------

    def mark_cluster_vpcs(self):
        """Marks which VPCs a cluster sits in. Only decided when the files
        declare at least one cluster: with none, every VPC stays `null`
        (unknown) rather than `false`."""
        if not self.clusters:
            return
        # Keyed by directory and instance as well as address: `aws_vpc.main`
        # in envs/dev and in envs/prod are two VPCs, and so are two views of
        # one module's VPC; a cluster names the one beside it.
        by_identity = {_identity(s): s for s in self.subnets}
        by_vpc_identity = {_identity(v): v for v in self.vpcs}
        decided = set()
        for cluster in self.clusters:
            vpc_dir = cluster.get("_vpc_dir")
            if vpc_dir is None:  # eksctl / CloudFormation entries; "" is the root
                vpc_dir = os.path.dirname(cluster["path"])
            vpc_inst = cluster.get("_vpc_inst")
            vpc = cluster["vpc"]
            if vpc is None:
                subnet_dir = cluster.get("_subnet_dir")
                if subnet_dir is None:
                    subnet_dir = os.path.dirname(cluster["path"])
                for ref in cluster["subnets"]:
                    subnet = by_identity.get((subnet_dir, ref, cluster.get("_subnet_inst")))
                    module = _MODULE_OUTPUT_RE.match(ref)
                    if subnet is not None and subnet.get("vpc"):
                        vpc = subnet["vpc"]
                        vpc_dir = subnet.get("_vpc_dir", os.path.dirname(subnet["path"]))
                        vpc_inst = subnet.get("_vpc_inst")
                        break
                    if module is not None:
                        # `module.stack.module.vpc.private_subnets`: the
                        # instance is everything but the output name, and the
                        # copy is the one the subnets were followed from.
                        vpc, vpc_dir, vpc_inst = ref.rsplit(".", 1)[0], subnet_dir, cluster.get("_subnet_inst")
                        break
            # `vpc_id = module.network.vpc_id` with a local source: the VPC is
            # the one declared inside modules/network — this instance's view
            # of it, when there are several.
            identity = self._inner_vpc(vpc_dir, vpc, vpc_inst)
            cluster["vpc"] = identity[1]
            found = by_vpc_identity.get(identity)
            if found is not None:
                cluster["vpc_path"] = found["path"]
            if identity[1]:
                decided.add(identity)
        # A cluster whose VPC could not be followed — no reference at all, or
        # one that names nothing recorded (a data source, a module with two
        # VPCs) — leaves the question open: a VPC it does not visibly name
        # may still be its VPC, so only the ones a cluster names are decided,
        # and the rest stay null.
        recorded = {_identity(v) for v in self.vpcs}
        # A decided VPC with no stated range (an eksctl or CloudFormation
        # cluster joining an existing VPC by id) may be any of the VPCs
        # declared elsewhere, so it settles nothing for them either.
        unstated = any(_identity(v) in decided and v["cidr"] is None for v in self.vpcs)
        undecided = (any(c["vpc"] is None for c in self.clusters) or bool(decided - recorded)
                     or unstated)
        for vpc in self.vpcs:
            if vpc["cluster_vpc"] is None:
                named = _identity(vpc) in decided
                vpc["cluster_vpc"] = True if named else (None if undecided else False)

    def section(self) -> dict:
        for entries in (self.vpcs, self.subnets, self.clusters):
            for entry in entries:
                for key in [k for k in entry if k.startswith("_")]:
                    entry.pop(key)
        lists = {"vpcs": self.vpcs, "subnets": self.subnets, "clusters": self.clusters,
                 "routes": self.routes, "unresolved": self.unresolved}
        for name, items in lists.items():
            if len(items) > MAX_ENTRIES:
                self.notes.append(f"address_space.{name}: {len(items)} entries found, the first "
                                  f"{MAX_ENTRIES} recorded")
                del items[MAX_ENTRIES:]
        if not any(lists.values()):
            return {}
        return lists


class _CloudFormationLoader(yaml.SafeLoader):
    """SafeLoader that reads CloudFormation's short-form intrinsics (`!Ref`,
    `!Sub`, `!GetAtt`, ...) as their long-form mappings and refuses aliases,
    for the same reason k8s_manifests does."""

    def compose_node(self, parent, index):
        if self.check_event(yaml.events.AliasEvent):
            raise yaml.constructor.ConstructorError(
                None, None, "YAML aliases are not accepted", self.peek_event().start_mark)
        return super().compose_node(parent, index)


def _intrinsic(loader, tag_suffix, node):
    name = "Ref" if tag_suffix == "Ref" else f"Fn::{tag_suffix}"
    if isinstance(node, yaml.ScalarNode):
        value = loader.construct_scalar(node)
    elif isinstance(node, yaml.SequenceNode):
        value = loader.construct_sequence(node, deep=True)
    else:
        value = loader.construct_mapping(node, deep=True)
    return {name: value}


_CloudFormationLoader.add_multi_constructor("!", _intrinsic)


def _load_cloudformation(content: str) -> dict | None:
    """The template as a dict, or None when the text is not a CloudFormation
    template. Raises ValueError when it is one but cannot be read."""
    if len(content.encode("utf-8")) > k8s_manifests.MAX_MANIFEST_BYTES:
        raise ValueError(f"template is larger than {k8s_manifests.MAX_MANIFEST_BYTES} bytes")
    try:
        template = yaml.load(content, Loader=_CloudFormationLoader)
    except yaml.YAMLError as e:
        raise ValueError(f"not parseable as YAML: {e}")
    return template if _is_cloudformation(template) else None


def _reads_as_manifests(content: str) -> bool:
    """True when `content` is a Kubernetes manifest stream: at least one
    document, every one a mapping with apiVersion and kind."""
    try:
        docs = k8s_manifests.load_manifest_documents(content)
    except ValueError:
        return False
    return bool(docs) and all(isinstance(d, dict) and "apiVersion" in d and "kind" in d
                              for d in docs)


def _is_cloudformation(doc) -> bool:
    if not isinstance(doc, dict) or not isinstance(doc.get("Resources"), dict):
        return False
    # A mapping with a Resources mapping is a template; one whose resources
    # are all custom or third-party types is read and records nothing.
    return True


_CFN_HINT_RE = re.compile(r"AWSTemplateFormatVersion|AWS::EC2::VPC|AWS::EC2::Subnet|AWS::EC2::Route|"
                          r"AWS::EKS::Cluster")
_EKSCTL_HINT_RE = re.compile(r"apiVersion:\s*['\"]?eksctl\.io/")


# -- What an unresolved entry means downstream --------------------------------

# Arguments of an unresolved entry that never enter the avoidance set, in
# each dialect's spelling (Terraform, eksctl, CloudFormation).
NON_RANGE_ARGUMENTS = ("public_access_cidrs", "vpc.publicAccessCIDRs",
                       "ResourcesVpcConfig.PublicAccessCidrs")
# The argument that carries a VPC's primary range, and a cluster's service
# range, in each dialect's spelling.
PRIMARY_RANGE_ARGUMENTS = ("cidr_block", "cidr", "CidrBlock", "vpc.cidr")
SERVICE_RANGE_ARGUMENTS = ("service_ipv4_cidr", "kubernetesNetworkConfig.serviceIPv4CIDR",
                           "KubernetesNetworkConfig.ServiceIpv4Cidr")


def entry_key(entry: dict, address=None) -> tuple:
    """The identity the section can still tell apart: the place and the
    address. The harvester keeps same-named declarations apart by directory
    for Terraform (a module is a directory) and by file for eksctl and
    CloudFormation (a template is a file, and two in one directory may
    share logical ids)."""
    path = entry.get("path") or ""
    place = os.path.dirname(path) if path.endswith(".tf") else path
    return place, entry.get("address") if address is None else address


def asked_arguments(section: dict) -> dict:
    """The arguments `unresolved` already asks about, by entry key: what a
    reader must consult before calling a null field "unset" — a VPC whose
    `cidr_block` is a variable with no default is asked for under
    `unresolved`, not unstated."""
    asked = {}
    for entry in section.get("unresolved") or []:
        asked.setdefault(entry_key(entry), set()).add(entry.get("argument"))
    return asked


def triage_unresolved(section: dict) -> tuple:
    """Splits the section's unresolved entries into the ones that can change
    the target ranges and the ones that cannot: a subnet whose VPC has a
    stated primary range is inside it by AWS's own rule, a subnet whose
    VPC's own range is already among the questions is settled by that
    answer, and a public-endpoint allow list is not a routing range.
    Returns (blocking, covered). One rule for two readers: the scan summary asks the user only
    for the blocking entries, and the landing-zone proposal counts them the
    same way."""
    key = entry_key
    # Two copies of one local module instantiated from two root directories
    # share a key once the section is written (the copy marker is not in
    # it), so a key is stated only when every VPC entry under it is: a
    # subnet of the copy whose range nobody stated is a question, whatever
    # the other copy says.
    # A VPC with a secondary range the files do not state (an IPAM pool, a
    # list behind a variable with no default) covers no subnet: the subnet
    # may be cut from that range, which is the EKS custom-networking shape.
    unstated_secondary = {key(e) for e in section.get("unresolved") or []
                          if e.get("argument") == "secondary_cidr_blocks"}

    def is_stated(vpc):
        return bool(vpc.get("cidr")) and key(vpc) not in unstated_secondary

    by_key = {}
    for vpc in section.get("vpcs") or []:
        by_key.setdefault(key(vpc), []).append(is_stated(vpc))
    stated = {k for k, flags in by_key.items() if all(flags)}
    # A VPC whose own primary range is a question here: its answer settles
    # every subnet cut from it, so those are not asked one by one.
    asked = {key(e) for e in section.get("unresolved") or []
             if e.get("argument") in PRIMARY_RANGE_ARGUMENTS and key(e) in by_key}
    # A subnet's VPC may be declared in another directory (a subnet inside
    # a local module whose VPC is at the root, or the reverse): the address
    # then decides, but only when every VPC that carries it states its
    # range or has it among the questions. envs/dev and envs/prod each
    # instantiating modules/vpc with a literal are two stated carriers, and
    # a root subnet beside either is covered; the same pair with prod's
    # range from an IPAM pool decides nothing, as the subnet's VPC may be
    # the unstated one.
    carriers = {}
    for vpc in section.get("vpcs") or []:
        carriers.setdefault(vpc.get("address"), []).append(is_stated(vpc) or key(vpc) in asked)

    def vpc_is_stated(subnet):
        vpc = subnet.get("vpc")
        if subnet.get("vpc_path"):
            # The harvester recorded which VPC entry the reference resolved
            # to: that one decides, not a namesake in the subnet's place.
            vpc_key = key({"path": subnet["vpc_path"]}, vpc)
            return vpc_key in stated or vpc_key in asked
        if key(subnet, vpc) in stated or key(subnet, vpc) in asked:
            return True
        # Only Terraform references cross directories; an eksctl or
        # CloudFormation subnet's VPC is always in the same file.
        terraform = (subnet.get("path") or "").endswith(".tf")
        flags = carriers.get(vpc) or []
        return terraform and bool(flags) and all(flags)

    # Every subnet under a key, not the first: two copies of a shared
    # subnets module share a key once written, and each copy may sit in a
    # different VPC. An entry is covered only when every copy's VPC is.
    subnets = {}
    for subnet in section.get("subnets") or []:
        subnets.setdefault(key(subnet), []).append(subnet)
    blocking, covered = [], []
    for entry in section.get("unresolved") or []:
        argument, address = entry.get("argument"), entry.get("address") or ""
        copies = subnets.get(key(entry)) or []
        is_subnet_cidr = argument in ("cidr_block", "CidrBlock", "cidr")
        # An entry filed under a VPC's own key is that VPC's question (its
        # `cidr`, or a secondary list), never a subnet's: a module's subnet
        # entries carry the module's address, so the lookup above would
        # otherwise hand the VPC's own range a subnet to hide behind.
        own = key(entry) in by_key
        if argument in NON_RANGE_ARGUMENTS:
            covered.append(entry)
        elif argument in MODULE_SUBNET_ARGS and (key(entry) in stated or key(entry) in asked):
            covered.append(entry)
        elif is_subnet_cidr and not own and copies and all(vpc_is_stated(s) for s in copies):
            covered.append(entry)
        else:
            blocking.append(entry)
    return blocking, covered


def harvest_address_space(root_dir: str, scope: dict = None,
                          chart_roots: list = ()) -> HarvestResult:
    """The single entry point the scan step calls.

    Returns the section (`{"vpcs": [...], "subnets": [...], "clusters":
    [...], "routes": [...], "unresolved": [...]}` or `{}` when the files
    declare none of it) and the notes to persist beside it.
    """
    harvest = _Harvest()
    notes = harvest.notes
    for rel_path, content in walk_in_scope(root_dir, scope,
                                           TF_EXTENSIONS + TFVARS_EXTENSIONS + TFVARS_JSON_EXTENSIONS,
                                           notes, purpose=PURPOSE):
        if rel_path.endswith(TFVARS_EXTENSIONS + TFVARS_JSON_EXTENSIONS):
            harvest.read_tfvars(rel_path, content)
        else:
            harvest.read_terraform(rel_path, content)
    harvest.apply_tfvars()
    harvest.resolve_terraform()
    for target, instances in sorted(harvest.module_sources.items()):
        if target not in harvest.module_inputs_used:
            continue
        labels = ", ".join(label for label, _, _ in instances)
        notes.append(f"{target or 'the repository root'}: a local module, instantiated by {labels}; "
                     "a range it reads "
                     "from an input variable is resolved through the value the instance passes")
    if harvest.skipped_tfvars:
        notes.append(f"{harvest.skipped_tfvars} .tfvars file(s) Terraform does not load "
                     "automatically were not read: which one applies is decided at plan time "
                     "with -var-file")

    for rel_path, content in walk_in_scope(root_dir, scope, YAML_EXTENSIONS, notes,
                                           chart_roots, purpose=PURPOSE):
        # CloudFormation first: an exported eksctl stack carries eksctl tags
        # AND short-form intrinsics the manifest loader refuses, so the
        # template shape decides, not the tag.
        template, cfn_error = None, None
        if _CFN_HINT_RE.search(content):
            try:
                template = _load_cloudformation(content)
            except ValueError as e:
                cfn_error = e
        if template is not None:
            harvest.read_cloudformation(rel_path, template)
        elif _EKSCTL_HINT_RE.search(content):
            try:
                docs = k8s_manifests.load_manifest_documents(content)
            except ValueError as e:
                notes.append(f"{rel_path}: looks like an eksctl ClusterConfig but was not read ({e})")
                continue
            harvest.read_eksctl(rel_path, docs)
        elif cfn_error is not None and not _reads_as_manifests(content):
            # A manifest stream that mentions a CloudFormation type (a
            # ConfigMap carrying a template, a comment) is a manifest the
            # single-document loader refused, not a template left unread.
            notes.append(f"{rel_path}: looks like a CloudFormation template but was not read ({cfn_error})")

    for rel_path, content in walk_in_scope(root_dir, scope, JSON_EXTENSIONS, notes,
                                           chart_roots, purpose=PURPOSE,
                                           oversized_hint=_CFN_HINT_RE, skip_suffixes=TFVARS_JSON_EXTENSIONS):
        if not _CFN_HINT_RE.search(content):
            continue
        try:
            template = json.loads(content)
        except ValueError as e:
            notes.append(f"{rel_path}: looks like a CloudFormation template but was not read ({e})")
            continue
        if _is_cloudformation(template):
            harvest.read_cloudformation(rel_path, template)

    harvest.mark_cluster_vpcs()
    section = harvest.section()
    notes.append(COVERAGE_NOTE)
    return HarvestResult(section, notes)
