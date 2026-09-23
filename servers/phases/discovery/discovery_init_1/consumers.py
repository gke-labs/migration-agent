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

"""Which workload needs which data service.

Pure logic — no GCS, no subprocesses, no LLM. `datastores.py` answers "what
data services does this estate have"; this module answers "and who uses them",
filling the `consumers` list on each entry it can attribute.

The workload data gate is the reason the question matters. A gate that knows
an estate has four databases but not which component needs which one can only
stop everybody or nobody, and neither is useful — which is exactly what
`servers/phases/workload/datagate` does with an entry that reaches it with no
consumer attached: it reports the service and holds nobody.

Nothing in a repository states the relationship outright, so the two
mechanisms here follow references that are already written down rather than
guessing from names:

  Terraform wiring — a Kubernetes-deploying resource (`helm_release`,
  `kubernetes_secret`, ...) references a module output, and that output
  resolves to the datastore's own block. This is the shape of any estate that
  deploys its workloads through Terraform.

  IRSA — an IAM role names the Kubernetes service account allowed to assume it,
  and the resource ARN is named either by a policy attached to that role or by
  the role block's own arguments (`iam-role-for-service-accounts-eks` takes
  them directly and builds the policy internally). Two spellings of
  the subject are read: the fully-qualified `system:serviceaccount:<ns>:<sa>`,
  and the bare `<ns>:<sa>` that `iam-role-for-service-accounts-eks` takes in
  `namespace_service_accounts`. This is the AWS-sanctioned way for a pod to
  reach a data service, so it finds the endpoint-less ones (DynamoDB, Kinesis)
  that no amount of hostname reading would. Only an IAM policy is expanded, and
  only when it is not the role's permissions boundary: a role body also names
  its provider, its OIDC module and its boundary, and a shared boundary names
  most of the estate. EKS Pod Identity, which supersedes IRSA, is not read — it
  binds through its own resource and needs a hop this does not have.

Both chains read Terraform references AND literal ARNs. A reference resolves
to the address of a declared block; a literal `arn:aws:s3:::acme-invoice-archive`
in a policy document, an IRSA module's arguments or a Helm value is matched to
the referenced-only entry `datastores.py` recorded for it, so a bucket the
estate reaches but does not declare still gets its consumer. Only a recorded
ARN attributes — an ARN of something the scan did not record, wildcarded or
built from a variable, is not a grant here any more than there.

Both stop at the first thing they cannot resolve rather than guessing past it.
A datastore with no consumers is reported as exactly that: the entry stays,
carrying a note. Under-detection is the worse error here as it is next door in
`datastores.py`, but the mitigation differs — there the answer is to record
with a caveat, here it is to record nothing and say so, because a wrong
consumer would gate the wrong team's pipeline.

Not covered, deliberately. Name similarity (`module.orders_rds` beside
`src/orders`) is left out: it is the single highest-recall signal available
and would attribute most of a typical estate, but it is a guess, and this
section feeds a gate. Environment-variable handoffs (a Terraform output
mapping `BUCKET_NAME` to a bucket, a manifest reading `$BUCKET_NAME`) are real
and deterministic but are a convention of one workshop's tooling rather than a
Terraform pattern. `data "aws_iam_policy_document"` and
`aws_eks_pod_identity_association` are real gaps rather than choices. DESIGN.md
issue 27 carries all of these, along with the known over-attributions.
"""

import bisect
import os
import re

from servers.phases.scope_algebra import is_excluded

from .datastores import (
    DATASTORE_KINDS,
    MAX_FILE_BYTES,
    SCAN_EXTENSIONS,
    added_by_hand,
    content_was_skipped,
    value_end,
    heredoc_body_without_comments,
    find_literals,
    interpolation_spans,
    iter_terraform_blocks,
    scan_source,
)
from .files import SKIP_DIRS

# Block kinds this module reads. `output` is here and not in datastores.py's
# default because only the join resolves through outputs. `data` is
# deliberately absent: `data "aws_iam_policy_document"` would be worth
# following, but `_REF_RE` collapses `data.aws_iam_policy_document.carts` to a
# two-part address so the block could not be found anyway — and including the
# kind would let an unterminated `data` block truncate the consumer index at a
# point where the datastore scan reads on. See DESIGN.md issue 27.
_KINDS = ("resource", "module", "output")

# Terraform resources that put a workload, or a workload's configuration, into
# a cluster. A reference to a datastore from inside one of these is a
# statement that the thing being deployed needs that datastore.
#
# An explicit list rather than a `kubernetes_` prefix. The prefix also matches
# `kubernetes_storage_class`, `kubernetes_network_policy`,
# `kubernetes_cluster_role_binding` and a dozen more that are cluster
# machinery, not workloads: recording one produces a "consumer" naming
# something no gate can act on. `kubernetes_manifest` is out for a different
# reason — it wraps an arbitrary embedded object, so its block label names the
# manifest rather than the workload inside it. A namespace is a container, not
# a consumer.
_DEPLOY_TYPES = (
    "helm_release",
    "kubernetes_deployment", "kubernetes_deployment_v1",
    "kubernetes_stateful_set", "kubernetes_stateful_set_v1",
    "kubernetes_daemon_set", "kubernetes_daemon_set_v1",
    "kubernetes_job", "kubernetes_job_v1",
    "kubernetes_cron_job", "kubernetes_cron_job_v1",
    "kubernetes_pod", "kubernetes_pod_v1",
    # Configuration carriers: a datastore endpoint reaching one of these is
    # bound for whatever mounts it. `kind` records what it is, so a reader can
    # tell a Secret from a Deployment.
    "kubernetes_secret", "kubernetes_secret_v1",
    "kubernetes_config_map", "kubernetes_config_map_v1",
)

# Blocks that can carry an IRSA subject, and blocks that can grant access.
# Kept apart because the second hop must expand ONLY a policy: a role body also
# names its provider, its boundary and whatever module produced its OIDC URL,
# and expanding those hands every datastore they mention to the role's service
# account.
_ROLE_TYPES = ("aws_iam_role", "aws_iam_role_policy")
_POLICY_TYPES = ("aws_iam_policy", "aws_iam_role_policy")

# `system:serviceaccount:<namespace>:<name>`, the fully-qualified subject. A
# hand-rolled `assume_role_policy` condition and the older
# `iam-assumable-role-with-oidc` module input both use this exact form.
_SUBJECT_RE = re.compile(
    r"system:serviceaccount:([a-z0-9][a-z0-9-]*):([a-z0-9][a-z0-9._-]*)")

# The bare `"<namespace>:<name>"` form. `iam-role-for-service-accounts-eks`
# supersedes `iam-assumable-role-with-oidc` and is what current estates write,
# and it takes the subject unqualified inside `namespace_service_accounts`.
# Matching that string anywhere would be far too loose — `"redis:6379"` is a
# subject by that rule — so it is only read inside the argument that means it,
# whose extent is found by counting brackets in `_subjects_in`.
_NS_SA_ARG_OPEN_RE = re.compile(r"namespace_service_accounts[ \t]*=[ \t]*\[")
# The argument alone, whatever its value: `namespace_service_accounts =
# var.orders_sas` names no subject this scan can read, but it says the group
# is a slot FOR one — which is what tells another app's entry from a
# provider default or a trust policy's condition operator.
_SUBJECT_ARG_RE = re.compile(r"namespace_service_accounts[ \t]*[=:]")
_NS_SA_RE = re.compile(r'"([a-z0-9][a-z0-9-]*):([a-z0-9][a-z0-9._-]*)"')

# The characters a map key may be written with, shared by the two patterns
# that have to agree about it: the subscript that selects a key and the entry
# that declares one. Kept as one constant because every time the two were
# spelled separately they drifted, and a drift is silent — the map still
# parses, the key just reads as absent.
_KEY_CHARS = "A-Za-z0-9_.-"

# A Terraform reference: `module.x.out`, `aws_dynamodb_table.carts.name`, or
# the bare two-part address. The `resource.` prefix that newer HCL allows, and
# that eks-workshop-v2 writes, needs nothing: the match begins at the resource
# TYPE, and the `.` before it is a word boundary, so `resource.aws_s3_bucket.
# assets.arn` yields the same address, attribute and key as the bare spelling.
# An explicit optional group for it was carried here for a while and matched
# the same triples — worth stating, because it read like load-bearing support
# for a spelling that is in fact handled by the anchor.
_REF_RE = re.compile(
    r"\b((?:module|data|aws_[a-z0-9_]+)\.[A-Za-z_][A-Za-z0-9_-]*)"
    # An instance selector, from `count` or `for_each`, sits between the
    # address and the attribute: `module.deps["prod"].orders_endpoint`,
    # `module.deps[0].orders_endpoint`. Skipped rather than captured — every
    # instance of one module block is that one block, and datastores.py
    # records it once — but it has to be tolerated, because without it the
    # attribute is never read, `_resolve` takes the no-attribute path and the
    # walk stops at the wrapper while the chain plainly continues. A
    # per-environment layout writes exactly this.
    #
    # Bounded to one line so it cannot run away over a wrapped expression; a
    # selector split across lines reads as no attribute, which is where this
    # started.
    r"(?:\[[^]\n]*\])?"
    r"(?:\.([A-Za-z_][A-Za-z0-9_-]*))?"
    # A subscript on the attribute selects one key of a map output. Captured
    # because ignoring it makes `endpoints["orders"]` and `endpoints["catalog"]`
    # the same reference, and a grouped map is an ordinary way to publish one
    # endpoint per service.
    r"""(?:\[[ \t]*["']([""" + _KEY_CHARS + r"""]+)["'][ \t]*\])?""")

# `key = <value>` at the top level of a map output body.
#
# The key charset must match `_REF_RE`'s subscript charset, FIRST CHARACTER
# INCLUDED — one class, used in both, so the two cannot drift. A subscript
# accepts a leading digit, so a key must too, or `versions["5"]` reads as "map
# parsed, key absent" and expands nothing; the same held for a leading dot or
# dash until this was one class. A dot inside the key is legal in both for a
# related reason: accepting it in one but not the other made a dotted-key map
# parse as no map at all, and the caller then fell back to the whole body and
# handed every key's datastore to every key's consumer.
#
# `[=:](?!>)` — the negative lookahead rejects `=>`. A `for` expression wrapped
# across lines, which is what `terraform fmt` produces for a long one, puts
# `k => v` on a line of its own; without the lookahead that parses as a key
# called `k` with the value `> v`. The map then counts as "parsed", so a
# reference to a key it does not have expands NOTHING instead of falling back
# to the whole body — and the datastore goes on to claim no workload reaches
# it while the wiring chain plainly does. `k` is a loop variable, not a key.
_MAP_KEY_RE = re.compile(
    r'^["\']?([' + _KEY_CHARS + r']+)["\']?[ \t]*[=:](?!>)[ \t]*(.+)$',
    re.DOTALL)

# How many output hops to follow before giving up. Two is what real estates
# use (resource -> module output -> datastore); the extra headroom covers a
# module re-exporting a nested module's output, and the limit exists at all so
# a cycle in a malformed repository cannot hang the scan.
_MAX_HOPS = 4


def _is_deploy_type(resource_type: str) -> bool:
    return resource_type in _DEPLOY_TYPES


# A permissions-boundary assignment and everything up to the next top-level
# assignment, so a value wrapped over several lines is still subtracted.
#
# A boundary is a ceiling on permissions, not a grant, and a shared org-wide
# one names most of the estate — but it is an `aws_iam_policy` like any other,
# so the _POLICY_TYPES filter cannot catch it. The argument name is the only
# thing that distinguishes it, and the name varies: the `aws_iam_role` resource
# calls it `permissions_boundary` while the IRSA modules call it
# `role_permissions_boundary_arn`. The module form is the one real estates and
# this file's own fixture use, so matching an exact name fixed the rarer
# spelling and left the common one attributing the whole estate to one service
# account.
_BOUNDARY_BLOCK_RE = re.compile(
    r"^[ \t]*(?:[A-Za-z_][A-Za-z0-9_]*_)?permissions_boundary(?:_arn)?[ \t]*=.*?"
    r"(?=^[ \t]*[A-Za-z_][A-Za-z0-9_-]*[ \t]*=|\Z)",
    re.MULTILINE | re.DOTALL)

# Arguments whose references say nothing about data need. `depends_on` on a
# helm_release is an apply-ordering device — routinely written on cluster
# add-ons to serialise them behind the data layer — and
# `lifecycle.replace_triggered_by` is the same. A `description` naming a policy
# is prose. A reference from inside any of them must not attribute a consumer:
# it would hold a metrics-server release on an orders-database migration.
# Anchored to the line start OR to a `{` or `,` on the same line. A minified
# or generated policy writes the whole statement on one line —
# `[{ description = "we removed the grant on arn:… last year", Resource =
# "arn:…" }]` — and a line-anchored match reads neither the prose nor the
# note, so the retired bucket becomes a first-class dependency in silence.
# The lookbehind is one character wide, so `match.start()` still falls AFTER
# the delimiter and the delimiter still counts towards the depth walk.
# The depth walk is what keeps the widened anchor safe for the other three:
# a nested `depends_on` or `lifecycle` is still skipped, as it was.
# The quoted spelling of the key — `{ "description" = "…" }`, `"description":`
# in a JSON-shaped object — is blank in the mask, quotes and all. Matched in
# the raw body and accepted only where the mask still shows the `=`/`:` that
# follows it, exactly as `datastores._QUOTED_DESCRIPTION_ARG_RE` does: the two
# LITERAL views have to agree about what prose is, and while only the bare
# spelling was blanked here, a prose mention of an ARN recorded from a real
# grant elsewhere handed this role's service account the bucket.
_QUOTED_DESCRIPTION_RE = re.compile(
    r'(?:^[ \t]*|(?<=[{,])[ \t]*)"description"[ \t]*[=:]', re.MULTILINE)


class _QuotedKeyMatch:
    """A quoted `"description"` hit, shaped like a `_META_ARG_RE_OPEN` match
    (group 3 is the key) so the walk below treats both spellings alike."""

    def __init__(self, match):
        self._match = match

    def start(self) -> int:
        return self._match.start()

    def end(self) -> int:
        return self._match.end()

    def group(self, index: int = 0):
        if index == 0:
            return self._match.group(0)
        return "description" if index == 3 else None


_META_ARG_RE_OPEN = re.compile(
    r"(?:^[ \t]*|(?<=[{,])[ \t]*)"
    r"(?:(depends_on)[ \t]*=[ \t]*\[|(lifecycle)[ \t]*\{"
    r"|(description|tags)[ \t]*=)",
    re.MULTILINE)

_CLOSERS = {"[": "]", "{": "}", "(": ")"}


def _without_meta_arguments(raw_body: str, body_mask: str,
                            heredocs: list = (),
                            nested_prose: bool = False) -> str:
    """The body with ordering-only arguments blanked, length preserved.

    Extents are found by counting delimiters in the lexer's mask, not by a
    non-greedy regex. A regex ending at the first `]` stops early on
    `depends_on = [aws_eks_addon.this["vpc-cni"], module.orders_rds]` — the
    subscript closes it — and one ending at the first line-leading `}` stops
    on a nested `precondition {` block inside `lifecycle`. Both leave the real
    reference live, which is the whole failure this function exists to
    prevent. The mask already excludes brackets inside strings, so a subscript
    key or a quoted `}` cannot mislead the count.

    Applied at the block's OWN level only. Terraform allows `depends_on` and
    `lifecycle` nowhere else, and the names collide one level down with
    schema fields of the very resources this module treats as workloads: a
    container on a `kubernetes_deployment` has a real `lifecycle` block
    holding its `pre_stop` hook, and a ConfigMap can have a `data` key called
    `tags`. A depth-blind blanker deletes those, and the datastore the hook
    names then reports that no workload reaches it.

    `nested_prose` widens that for `description` alone, and only the LITERAL
    view asks for it. The two views disagree about a nested `description`
    because the thing they read out of it differs. An ARN written there is
    prose wherever it sits — `policy_statements = [{ description = "we
    removed the grant on arn:… last year" }]` — so the literal view must
    blank it or a workload is handed a bucket the Terraform says it stopped
    using. A REFERENCE written there is not prose by the same argument: a
    ConfigMap's `data = { description = aws_s3_bucket.archive.id }` is real
    configuration, exactly as its `tags` key is, and blanking it drops a
    consumer edge the reference chain had before literal ARNs existed.
    """
    out = list(raw_body)

    def blank(start: int, stop: int) -> None:
        for i in range(start, min(stop, len(out))):
            if out[i] != "\n":
                out[i] = " "

    # Depth is carried forward between matches rather than recounted from the
    # start at each one, so a body with many of them stays linear.
    depth, scanned = 0, 0
    quoted = [_QuotedKeyMatch(m) for m in _QUOTED_DESCRIPTION_RE.finditer(raw_body)
              if body_mask[m.end() - 1] in "=:"]
    for match in sorted(list(_META_ARG_RE_OPEN.finditer(body_mask)) + quoted,
                        key=lambda m: m.start()):
        segment = body_mask[scanned:match.start()]
        depth += segment.count("{") - segment.count("}")
        scanned = match.start()
        nested_description = (nested_prose
                              and (match.group(3) or "").strip() == "description")
        if depth != 0 and not nested_description:
            # One level down this is a schema field, not a meta-argument. A
            # match inside a span already blanked above lands here too, which
            # is right: blanking it twice would be harmless but pointless.
            #
            # `description` is the exception under `nested_prose`, which
            # only the literal view sets: a nested one is prose wherever it
            # sits — `policy_statements = [{ description = "we removed the
            # grant on arn:… last year" }]` is the shape — and that view is
            # what literal ARNs are attributed from, so reading it hands a
            # workload a bucket the Terraform says it stopped using.
            # `datastores.without_prose` blanks a description at any depth
            # for the same reason, and the two LITERAL views must agree
            # about what counts as prose.
            #
            # The reference view does not set it, and must not. A ConfigMap
            # `data` key called `description` holding
            # `aws_s3_bucket.archive.id` is real configuration and a real
            # consumer edge, indistinguishable in kind from the `tags` key
            # the paragraph above refuses to blank for exactly that reason.
            # Blanking it here dropped edges this scan made before literal
            # ARNs existed. `tags` is NOT excepted in either view: nothing
            # reads an ARN out of a nested `tags` that a grant does not also
            # name.
            continue
        if match.group(3):
            # `description`/`tags`. Either spelling is followed to the end
            # of its VALUE — a delimiter it opens directly (`= [`, `= (`,
            # `= {`), a heredoc, or a call like `= merge(local.tags, {...})`
            # that begins with an identifier and wraps across lines. The
            # end-of-line path this once took left a call's continuation
            # live, which mattered little while only references were read
            # from this view and matters now that literal ARNs are: a
            # description's example ARN would attribute a workload, and a
            # tag's would too. Neither is a grant.
            #
            # `datastores.without_prose` blanks the description and NOT
            # the tags, and the two views are right to differ here: this
            # one records grants, so a tag on an IAM role is not one,
            # while the harvest records evidence of a dependency, and a
            # tag naming an ARN is evidence. An ARN only a top-level tag
            # states is therefore a referenced entry no chain reaches,
            # which is what the unattributed note is for.
            rest = body_mask[match.end():]
            lead = len(rest) - len(rest.lstrip(" \t"))
            first = rest[lead:lead + 1]
            if first not in ("[", "(", "{"):
                # To the end of the VALUE, by delimiter depth in the mask —
                # not to the end of the line. `description = join("\n", [...])`
                # begins with an identifier, and stopping at the newline
                # leaves its continuation live. That mattered for IRSA
                # subjects before, and matters more now that this view is
                # also what literal ARNs are read from for attribution:
                # prose in a description would hand a role's grants to a
                # service account nobody deploys, or attribute a workload to
                # a bucket named in an example.
                stop = value_end(body_mask, match.end())
                # A heredoc value opens with `<<`, which is not a delimiter
                # the depth walk counts, so a description that IS a heredoc
                # still ends at its own line. Extended over every heredoc the
                # value opens — not the first alone — since one can follow
                # another. The spans come from the lexer, as everywhere else.
                extended = True
                while extended:
                    extended = False
                    for h_start, h_stop in heredocs:
                        if match.end() <= h_start <= stop + 1 and h_stop > stop:
                            stop, extended = h_stop, True
                blank(match.start(), stop)
                continue
            opener = first
            open_at = match.end() + lead
        else:
            opener = body_mask[match.end() - 1]
            open_at = match.end() - 1
        # A LOCAL counter. `depth` is carried across matches and is the
        # block-level depth every guard above reads; reusing it here reset
        # it to zero mid-block, and once a nested `description` could reach
        # this branch (its value opening a delimiter) every later match was
        # judged at the wrong depth — a top-level `depends_on` left live,
        # attributing a metrics-server release to an orders database, and a
        # nested `tags` blanked, deleting a real ConfigMap key.
        nesting, stop = 0, len(out)
        for i in range(open_at, len(body_mask)):
            if body_mask[i] == opener:
                nesting += 1
            elif body_mask[i] == _CLOSERS[opener]:
                nesting -= 1
                if nesting == 0:
                    stop = i + 1
                    break
        blank(match.start(), stop)
    return "".join(out)


_METADATA_RE = re.compile(r"^[ \t]*metadata[ \t]*\{", re.MULTILINE)
_META_ARG_RE = re.compile(
    r'^[ \t]*(name|namespace)[ \t]*=[ \t]*"([^"$\\]*)"[ \t]*,?[ \t]*$')


def _metadata_args(body_text: str, body_mask: str) -> dict:
    """`name` and `namespace` from a Kubernetes resource's `metadata` block.

    Every `kubernetes_*` resource puts its identity there rather than at the
    block's own level, so the top-level argument reader sees neither — it
    would report the Terraform block label as the workload name and a null
    namespace for every one of them.

    This counts braces in the mask the lexer already produced; it does not
    re-decide what is code. Only literal values are read, so a templated name
    stays absent rather than becoming a guess.
    """
    # The first metadata block at the block's OWN level. A Deployment nests a
    # second one inside `spec { template { ... } }`, and HCL does not order
    # blocks — a `spec` written before `metadata` would otherwise hand back the
    # pod template's name as the workload's.
    open_at = None
    for match in _METADATA_RE.finditer(body_text):
        candidate = match.end() - 1
        depth = 0
        for i in range(candidate):
            if body_mask[i] == "{":
                depth += 1
            elif body_mask[i] == "}":
                depth -= 1
        if depth == 0:
            open_at = candidate
            break
    if open_at is None:
        return {}
    depth = 0
    for i in range(open_at, len(body_mask)):
        if body_mask[i] == "{":
            depth += 1
        elif body_mask[i] == "}":
            depth -= 1
            if depth == 0:
                return _metadata_own_args(body_text[open_at + 1:i],
                                          body_mask[open_at + 1:i])
    return {}


def _metadata_own_args(inner_text: str, inner_mask: str) -> dict:
    """`name`/`namespace` at the metadata block's OWN level.

    Depth-tracked, for the same two reasons `_top_level_args` is: `labels = {
    name = "catalog-deployment" }` sits one level down and is the label, not
    the object's name, and an `annotations` map can carry a `namespace` key
    that is not the namespace. Reading a match at any depth names a workload
    that does not exist, which is worse than naming none, because the gate
    then cannot match it to a component.

    First-wins on top of that, which the depth guard does not cover: a
    repeated argument — generated Terraform, a merge resolved by
    concatenation — has to settle the same way it does in `_top_level_args`,
    or the two readers report different names for the same block.
    """
    args = {}
    depth = 0
    for text_line, mask_line in zip(inner_text.split("\n"), inner_mask.split("\n")):
        if depth == 0:
            match = _META_ARG_RE.match(text_line)
            if match and not text_line.strip().endswith("{"):
                args.setdefault(match.group(1), match.group(2))
        depth = max(depth + mask_line.count("{") - mask_line.count("}"), 0)
    return args


def _all_groups(body_mask: str) -> list:
    """Every `{...}` span in the mask, as (start, stop)."""
    stack, spans = [], []
    for i, char in enumerate(body_mask):
        if char == "{":
            stack.append(i)
        elif char == "}" and stack:
            spans.append((stack.pop() + 1, i))
    return spans


def _is_keyed_group(body_mask: str, start: int) -> bool:
    """Is this `{...}` the value of a named key, or an element of a list?

    `carts = { ... }` is one app of a per-app map; `[{ ... }, { ... }]` is a
    list of policy statements belonging to one app. `start` is the index
    just inside the brace, as `_all_groups` reports it, so the brace is at
    `start - 1` and what precedes it decides. Read from the mask, where a
    `=` or `:` inside a string is already blanked.

    Both HCL2 spellings of a key count: `objectelem` is
    `(Identifier | Expression) ("=" | ":") Expression`, so `carts: {...}`
    and `"carts": {...}` mean exactly what `carts = {...}` means, and taking
    only `=` read every colon-keyed app map as a list of statements — the
    whole body then became one app's scope and one service account
    collected every other app's datastore. A list element is preceded by
    `[` or `,`, never by either key character, including in the JSON
    spelling `jsonencode({ "Statement": [{...}, {...}] })`.
    """
    index = start - 2
    while index >= 0 and body_mask[index] in " \t\r\n":
        index -= 1
    return index >= 0 and body_mask[index] in "=:"


def _group_key(raw_body: str, body_mask: str, start: int):
    """The key a `{...}` is the value of — `carts` for `carts = {`, `"carts" =`
    or `carts: {` — or None for a list element or a key this cannot read.

    Read from the RAW body at the offsets the mask fixes: the mask blanks a
    string's contents, so a quoted key is not there, but the two views are
    the same length and the raw text at the same span is. That is what lets
    the per-app test ask the question the shape alone cannot answer — does
    the subject's slot key a GRANT sibling too? — instead of taking any keyed
    slot beside any keyed grant map for an app.
    """
    index = start - 2
    while index >= 0 and body_mask[index] in " \t\r\n":
        index -= 1
    if index < 0 or body_mask[index] not in "=:":
        return None
    index -= 1
    # Whitespace in BOTH views. The mask blanks a quoted string with its
    # quotes, so skipping mask blanks alone strode straight through a
    # quoted key and read whatever preceded it — the tail of the previous
    # line's expression, or nothing. The raw body still holds the quotes.
    while (index >= 0 and body_mask[index] in " \t\r\n"
           and raw_body[index] in " \t\r\n"):
        index -= 1
    return _key_ending_at(raw_body, index)


def _key_ending_at(raw_body: str, index: int):
    """The bare or quoted key whose last character is at `index` in the raw
    body, or None: `carts`, `"carts"` → `carts`."""
    if index < 0:
        return None
    if raw_body[index] == '"':
        opening = raw_body.rfind('"', 0, index)
        return raw_body[opening + 1:index] if opening >= 0 else None
    stop = index + 1
    while index >= 0 and (raw_body[index].isalnum() or raw_body[index] in "_-"):
        index -= 1
    return raw_body[index + 1:stop] or None


def _holding_argument(raw_body: str, body_mask: str, group: tuple, position: int):
    """The argument name a subject sits under inside its INNERMOST group —
    the last depth-zero key before `position` within `group` — or None.

    `namespace_service_accounts` for the upstream module, `subject` or
    `service_account` for a house wrapper, `oidc:sub` inside a trust policy's
    condition: the shape of the slot, read from the slot itself rather than
    assumed from one module's argument name. `group` must be the innermost
    span holding the position — for `main = { trust = { oidc = { subject =
    … } } }` the answer is `subject`, not `trust`. Depth is counted in the
    mask, so a brace inside a string does not open a level, and parentheses
    count too, so a call's arguments are not keys; a `:` after a depth-zero
    `?` is a conditional's, not a key's. The key text is read from the raw
    body, where a quoted key still has its quotes.
    """
    depth, key, ternary = 0, None, False
    for i in range(group[0], min(position, len(body_mask))):
        char = body_mask[i]
        if char in "{[(":
            depth += 1
        elif char in "}])":
            depth -= 1
        elif depth != 0:
            continue
        elif char == "?":
            ternary = True
        elif char in ",\n":
            ternary = False
        elif char in "=:":
            if char == ":" and ternary:
                continue
            ternary = False
            j = i - 1
            while (j >= 0 and body_mask[j] in " \t\r\n"
                   and raw_body[j] in " \t\r\n"):
                j -= 1
            found = _key_ending_at(raw_body, j) if j >= group[0] else None
            if found:
                key = found
    # A position that points at the argument NAME itself — how the
    # `namespace_service_accounts = [...]` form records its subject — holds
    # under that name, not under whatever key happened to precede it: read
    # as "the key before", `provider_arn` made a `defaults = { provider_arn
    # = "" }` sibling look like another app's slot.
    k = position
    while k < len(raw_body) and (raw_body[k].isalnum() or raw_body[k] in '_-:"'):
        k += 1
    j = k
    while j < len(body_mask) and body_mask[j] in " \t":
        j += 1
    if (k > position and j < len(body_mask) and body_mask[j] in "=:"
            and not (body_mask[j] == ":" and ternary)):
        own = _key_ending_at(raw_body, k - 1)
        if own:
            return own
    return key


# IAM condition operators, the keys of a trust policy's `Condition` map. A
# group keyed by one is never an app's slot, strong or weak: `StringEquals =
# { "…:sub" = "system:serviceaccount:carts:carts-sa" }` beside `StringLike =
# { "…:sub" = "system:serviceaccount:carts:*" }` is one app's trust widened
# by a wildcard, not two apps — and a trust policy is the role's, never an
# app's, however it is spelled. (A wildcard that still parses as a subject,
# `carts:canary-*`, is read as a SECOND subject in a second group and makes
# the role per-app under the older rule; that reading predates this change
# and is not what this exclusion is about.)
_CONDITION_OPERATOR_RE = re.compile(
    r"^(?:ForAnyValue:|ForAllValues:)?"
    r"(?:String|Arn|Numeric|Date|Bool|Binary|IpAddress|NotIpAddress|Null)")


def _carries_argument(raw_body: str, body_mask: str, span: tuple, name: str) -> bool:
    """Does this group state an argument called `name`, bare or quoted, at
    any depth — as a KEY, which the mask tells from the same text inside a
    string: the `=`/`:` after a real key is intact there, one inside
    `note = "namespace_service_accounts = [] disables"` is blank."""
    pattern = re.compile(r'(?<![A-Za-z0-9_-])' + re.escape(name) + r'[ \t]*[=:]'
                         r'|"' + re.escape(name) + r'"[ \t]*[=:]')
    return any(body_mask[match.end() - 1] in "=:"
               for match in pattern.finditer(raw_body, span[0], span[1]))


def _sibling_groups(spans: list) -> list:
    """Groups sharing a parent GROUP, as a list of sibling lists.

    A group's parent is the smallest span strictly containing it. Bounded work:
    the span list is small for a single block, and a block with hundreds of
    nested groups is not an IAM role.

    The block body itself is not a parent here. Two maps written as two
    top-level arguments — `role_policy_arns = {...}` beside
    `bucket_arns = {...}` — are two kinds of grant handed to the same app, and
    counting them as siblings makes a single-app module read as a per-app
    wrapper: the subject's own group carries no grant, the walk finds none
    outward either, and the block's every grant is dropped. A per-app map
    keys its apps under ONE argument, so its app groups always share a real
    parent.
    """
    families = {}
    for span in spans:
        start, stop = span
        parent = None
        for other in spans:
            if other == span:
                continue
            if other[0] <= start and stop <= other[1]:
                if parent is None or (other[1] - other[0]) < (parent[1] - parent[0]):
                    parent = other
        families.setdefault(parent, []).append(span)
    return [group for parent, group in families.items()
            if parent is not None and len(group) > 1]


def _enclosing_groups(body_mask: str, position: int) -> list:
    """Every `{...}` in the mask containing `position`, innermost first.

    A list, not just the innermost, because the caller has to walk outward: a
    subject is often one level deeper than the grant it belongs to —

        apps = {
          carts = { dynamodb_table_arns = [...]
                    oidc = { namespace_service_accounts = [...] } }
          orders = { ... }
        }

    — and stopping at the innermost group (`oidc`) finds no grant there, so an
    innermost-or-whole-body rule falls all the way back and hands carts the
    orders table. The first enclosing group that carries a grant is the right
    scope; only when none does is the whole body correct.

    Counted in the mask, which matters: a hand-written role puts its subject
    inside an `assume_role_policy` heredoc, and that JSON's braces are blanked
    there — so such a subject reports no enclosing group at all and correctly
    falls back to the whole body.
    """
    stack, enclosing = [], []
    for i, char in enumerate(body_mask):
        if char == "{":
            stack.append(i)
        elif char == "}" and stack:
            open_at = stack.pop()
            if open_at < position < i:
                enclosing.append((open_at + 1, i))
    return enclosing


def _subjects_in(raw_body: str, body_mask: str = "") -> list:
    """Every (namespace, service account, scope) an IRSA role body names.

    Two spellings, because the module that current estates use is not the one
    the fully-qualified form comes from. The bare form is read only inside
    `namespace_service_accounts`, since `"<a>:<b>"` on its own matches far too
    much to be safe.

    Each subject carries its own position and the groups enclosing it,
    innermost first, for the caller to pick a scope from. The position is what
    lets the caller tell a group that belongs to this subject alone from one
    it shares with another.

    A role's subjects may have no enclosing groups (a heredoc trust policy,
    whose braces the mask blanks) or several (a `jsonencode` one, whose braces
    are live). Either way they all sit in the SAME groups, which is how the
    caller tells one role shared by several service accounts from a wrapper
    module handing each app its own. For a role there are none and the whole body
    applies: one role shared by several service accounts really does give all
    of them everything it is granted. For a module call the enclosing groups
    matter, because one call routinely carries per-app configuration keyed by
    app —

        apps = {
          carts  = { namespace_service_accounts = [...], table_arn = ...carts... }
          orders = { namespace_service_accounts = [...], table_arn = ...orders... }
        }

    — and reading the whole body for every subject makes that a cross-product:
    carts-sa becomes a consumer of the orders table and vice versa.
    """
    # The mask is a same-length view of the body — every stage that produces
    # one preserves length — and the bracket count below indexes both. Falling
    # back rather than trusting that keeps a mismatch from raising out of the
    # whole scan, which is the one failure mode this module never chooses.
    if len(body_mask) != len(raw_body):
        body_mask = raw_body
    subjects = []
    for match in _SUBJECT_RE.finditer(raw_body):
        subjects.append((match.group(1), match.group(2), match.start(),
                         _enclosing_groups(body_mask, match.start())))
    for arg in _NS_SA_ARG_OPEN_RE.finditer(raw_body):
        # Extent by bracket counting, not a non-greedy match to the first `]`.
        # `["a:b", local.x["y"]]` closes early under a lazy pattern, and every
        # subject after the subscript goes unread — the same defect the
        # meta-argument blanking counts delimiters to avoid.
        #
        # A bracket counts only where the body and the mask agree on it. The
        # mask blanks one inside a quoted element, and the meta-argument
        # blanking has already removed one inside a `description`; a bracket
        # that survives both is structure. Counting the body alone closes the
        # list on the first quoted `]` and loses every subject after it.
        depth, stop = 0, len(raw_body)
        for i in range(arg.end() - 1, len(raw_body)):
            char = raw_body[i] if raw_body[i] == body_mask[i] else ""
            if char == "[":
                depth += 1
            elif char == "]":
                depth -= 1
                if depth == 0:
                    stop = i
                    break
        groups = _enclosing_groups(body_mask, arg.start())
        for inner in _NS_SA_RE.finditer(raw_body[arg.end():stop]):
            subjects.append((inner.group(1), inner.group(2), arg.start(), groups))
    return subjects


def _boundary_refs(raw_body: str) -> set:
    """Addresses named by a permissions-boundary argument on this block.

    Read from the raw body rather than from `declared`, which is parsed one
    line at a time: a boundary wrapped across lines would escape subtraction
    and take the whole estate with it. The scan is bounded to the argument's
    own line and its continuation up to the next assignment, so it cannot
    swallow a following grant.
    """
    refs = set()
    for match in _BOUNDARY_BLOCK_RE.finditer(raw_body or ""):
        refs.update(a for a, _attr, _key in _references(match.group(0)))
    return refs


def _references(raw: str) -> list:
    """Every Terraform address mentioned in a chunk of source, in order.

    Returns (address, attribute, key) triples. The attribute matters only for
    module references, where it names the output to resolve; the key is the
    subscript, when the reference selects one entry of a map output.
    """
    return [(m.group(1), m.group(2), m.group(3)) for m in _REF_RE.finditer(raw)]


class _Index:
    """Everything the join needs, built in one walk of the checkout.

    Keyed by directory throughout: Terraform resolves `module.x` against the
    directory the reference is written in, and two directories can each
    declare a `module "this"`. A flat key would silently join across them.
    """

    def __init__(self):
        # (dir, output_name) -> (body read for references, its mask slice)
        self.outputs = {}
        # (dir, module_alias) -> directory the module's source points at
        self.local_modules = {}
        # (dir, address) -> raw body, for IAM policies ONLY. Deliberately not
        # every block: see _ROLE_TYPES.
        self.policies = {}
        # (dir, address) -> the same policy's literal body: heredoc documents
        # restored whole, so the ARNs a policy states are readable. Same
        # length as the raw body, so a span into one is a span into the other.
        self.policy_literals = {}
        # [(dir, rel_path, resource_type, name, args, raw_body, metadata,
        #   literal body)]
        self.deploys = []
        # [(dir, rel_path, role_address, subjects, raw_body, boundary_refs,
        #   body mask, literal body)]
        self.roles = []
        # [(dir, declared args, own address or None)] for the blocks carrying
        # a role->policy edge: aws_iam_role_policy_attachment and the inline
        # aws_iam_role_policy.
        self.attachments = []
        # Scan-level notes. Only the unclosed-block case lands here: an
        # unreadable or oversized file is recorded in `truncated` without a
        # note, because the datastore walk reads the same files and has
        # already said so, and the agent relays every note verbatim.
        self.notes = []
        # Files whose blocks ran out mid-read. Repository-wide, not per
        # directory: a workload reaches a datastore through a module output
        # from a DIFFERENT directory by construction, so a truncation anywhere
        # can hide the reference to an entry declared anywhere else.
        self.truncated = set()
        # Files the confirmed scope excluded. Separate from `truncated`: an
        # exclusion is a decision, not a failure, and folding the two would
        # relabel every unattributed entry in an ordinary scoped scan as
        # "unknown" and drain the count of its meaning. Tracked at all because
        # the consumer of an in-scope datastore can sit in an excluded file —
        # the wiring chain crosses directories — and the scan must not then
        # claim nothing references it.
        self.excluded = set()
        # `_resolve`'s memo. Lives on the index because that is exactly what
        # the walk is a pure function of, so the cache is valid for as long as
        # the index is and no longer.
        self.resolved = {}

    def module_dir(self, directory: str, alias: str) -> str | None:
        return self.local_modules.get((directory, alias))


def build_index(root_dir: str, scope: dict = None) -> _Index:
    """Walks the checkout once and records what the join resolves through.

    Reads the same files `extract_datastores` reads, under the same
    scope rule: an operator who excluded a directory excluded it from the
    migration, and this feeds the same member-readable section.
    """
    index = _Index()
    for dirpath, dirnames, filenames in os.walk(root_dir):
        dirnames[:] = sorted(d for d in dirnames if d not in SKIP_DIRS)
        for filename in sorted(filenames):
            if not filename.endswith(SCAN_EXTENSIONS):
                continue
            full_path = os.path.join(dirpath, filename)
            rel_path = os.path.relpath(full_path, root_dir)
            if scope and is_excluded(rel_path, scope):
                index.excluded.add(rel_path)
                continue
            try:
                if os.path.getsize(full_path) > MAX_FILE_BYTES:
                    # Not noted here: extract_datastores walks the
                    # same files under the same limits and already recorded
                    # this one. The agent is told to relay every scan note
                    # verbatim, so a second wording of the same fact is noise
                    # a reader would double-count. It IS recorded as unread,
                    # though — the note is not what suppresses the "nothing
                    # references this" claim, this set is.
                    index.truncated.add(rel_path)
                    continue
                with open(full_path, "r", encoding="utf-8-sig",
                          errors="replace") as f:
                    content = f.read()
            except OSError:
                # Already noted by the datastore walk over the same file set,
                # but still a file whose references were never read.
                index.truncated.add(rel_path)
                continue

            directory = os.path.dirname(rel_path)
            # Lexer notes are not repeated here — the datastore walk reads the
            # same files and already recorded them — but a note saying part of
            # the file went unread has to reach `truncated`. An unterminated
            # heredoc or string stops the scan mid-file just as an unclosed
            # block does, and if it happens OUTSIDE a resource/module/output
            # block the kind filter skips that block before its extent is
            # measured, so `iter_terraform_blocks` never reports truncation.
            text, mask, lex_notes, heredocs = scan_source(content)
            if content_was_skipped(lex_notes):
                index.truncated.add(rel_path)
            narrowed, widest_span = _narrow_to_interpolations(content, heredocs)
            blocks, truncated = iter_terraform_blocks(text, mask, _KINDS)
            if truncated:
                # Recorded per file, not only as a note: an entry declared
                # before the break must not go on to claim that no workload
                # references it, because the workload may be in the part that
                # was never read. The set is what suppresses the claim; the
                # note below is only there to explain it.
                index.truncated.add(rel_path)
                # And only when this walk is the one that saw it. The datastore
                # walk reads the same file for `resource` and `module`, so an
                # unclosed block among those is already a note this run
                # carries — and the agent relays every note verbatim, so
                # restating it makes one malformed file read as two problems.
                # An unclosed `output` is the case only this walk sees: the
                # kind filter skips the block before its extent is measured,
                # so the datastore walk reads straight past it.
                if not iter_terraform_blocks(text, mask, DATASTORE_KINDS)[1]:
                    index.notes.append(
                        f"{rel_path}: an output block is never closed, so "
                        "consumers were not read from that point on")
            for kind, first, second, args, declared, start, stop in blocks:
                # Meta-arguments are blanked HERE, for every block kind, not in
                # the deploys loop. A `depends_on` on an IAM role reads as an
                # attachment and hands the role every datastore that policy
                # names; one on a policy adds a table it does not grant. The
                # _ROLE_TYPES/_POLICY_TYPES filter is the defence against
                # expanding non-grants, and an ordering argument walks straight
                # through it.
                block_heredocs = [(h_start - start, h_stop - start)
                                  for h_start, h_stop in heredocs
                                  if start <= h_start and h_stop <= stop]
                raw_body = _without_meta_arguments(
                    _reference_body(content, text, start, stop, narrowed,
                                    widest_span),
                    mask[start:stop], block_heredocs)
                # Meta-blanked for the same reason as the reference body: a
                # `description` mentioning an old `system:serviceaccount:...`
                # would otherwise mint a subject and hand that role's grants
                # to a service account nobody deploys.
                subject_body = _without_meta_arguments(
                    _subject_body(content, text, start, stop, heredocs),
                    mask[start:stop], block_heredocs, nested_prose=True)
                if kind == "output":
                    # The whole body, not `declared["value"]`: an assignment is
                    # read one line at a time, and an output whose value is a
                    # map or a multi-line expression would resolve to the `{`
                    # it opens with. Every reference in the block is a target;
                    # an output that composes two of them genuinely depends on
                    # both.
                    index.outputs[(directory, first)] = (raw_body,
                                                        mask[start:stop])
                    continue
                if kind == "module":
                    source = args.get("source")
                    if isinstance(source, str) and source.startswith((".", "/")):
                        resolved = os.path.normpath(
                            os.path.join(directory, source))
                        # `normpath` spells the checkout root ".", and every
                        # other directory key here comes from `os.path.dirname`,
                        # which spells it "". A module whose source points at
                        # the root — `source = "../.."` from `envs/prod` — would
                        # otherwise never find the outputs declared there, and
                        # the datastore behind them would claim no workload
                        # reaches it.
                        index.local_modules[(directory, first)] = (
                            "" if resolved == "." else resolved)
                    subjects = _subjects_in(subject_body, mask[start:stop])
                    if subjects:
                        index.roles.append(
                            (directory, rel_path, f"module.{first}", subjects,
                             raw_body, _boundary_refs(raw_body),
                             mask[start:stop], subject_body))
                    continue
                if kind != "resource":
                    continue
                address = f"{first}.{second}" if second else first
                if first in _POLICY_TYPES:
                    index.policies[(directory, address)] = raw_body
                    # The literal view is the subject view: whole heredocs,
                    # meta-arguments blanked. A policy document's ARNs are
                    # literals in exactly the way an IRSA subject is.
                    index.policy_literals[(directory, address)] = subject_body
                if first in ("aws_iam_role_policy_attachment",
                             "aws_iam_role_policy"):
                    # Both carry the role->policy edge the two blocks
                    # themselves do not. An attachment names the policy in
                    # `policy_arn`; an inline `aws_iam_role_policy` IS the
                    # policy, so it names itself.
                    index.attachments.append((directory, declared, address
                                              if first == "aws_iam_role_policy"
                                              else None))
                if _is_deploy_type(first):
                    index.deploys.append(
                        (directory, rel_path, first, second, args, raw_body,
                         _metadata_args(text[start:stop], mask[start:stop]),
                         subject_body))
                elif first in _ROLE_TYPES:
                    subjects = _subjects_in(subject_body, mask[start:stop])
                    if subjects:
                        index.roles.append(
                            (directory, rel_path, address, subjects, raw_body,
                             _boundary_refs(raw_body), mask[start:stop],
                             subject_body))
    return index


def _restore(content: str, text: str, start: int, stop: int, spans: list,
             widest: int = 0) -> str:
    """`text` for this block, with the given raw spans written back over it.

    Spans are file-wide and sorted, so the relevant ones are found by binary
    search rather than by scanning the whole list per block. Walking all of
    them per block is O(blocks x spans) — the cost hoisting the span
    computation out of the block loop was supposed to remove, reintroduced one
    function along.

    The lookback is the widest span in the list, so the search cannot start
    past a span that begins before this block and ends inside it. That does
    not happen in a well-formed file — `_BLOCK_RE` matches on `text`, where
    heredoc bodies are blanked, so no block header sits inside a heredoc and
    no span straddles a block boundary — and setting the lookback to zero
    leaves the suite green. It is kept for the unterminated case, where the
    lexer's last heredoc span runs to end of file and does cross every block
    after it. Bounding the lookback by the file-size limit instead of the
    widest span made the search return 0 every time and the walk linear again,
    which is the mistake this argument exists to prevent.
    """
    body = list(text[start:stop])
    index = bisect.bisect_left(spans, (start - widest, -1))
    for span_start, span_stop in spans[index:]:
        if span_start >= stop:
            break
        low, high = max(span_start, start), min(span_stop, stop)
        if low < high:
            body[low - start:high - start] = list(content[low:high])
    return "".join(body)


def _narrow_to_interpolations(content: str, heredocs: list) -> tuple[list, int]:
    """Every `${...}` span inside every heredoc in one file, and the widest.

    Computed once per file, not once per block: the spans depend only on the
    content, and rebuilding them inside the block loop makes the cost
    O(blocks x heredoc bytes). A file with a 50 KB embedded manifest and a few
    hundred blocks is exactly the shape `iter_terraform_blocks` already
    guards against for the same reason.
    """
    spans = []
    for h_start, h_stop in heredocs:
        spans.extend(interpolation_spans(content, h_start, h_stop))
    widest = max((stop - start for start, stop in spans), default=0)
    return spans, widest


def _reference_body(content: str, text: str, start: int, stop: int,
                    narrowed: list, widest: int = 0) -> str:
    """The block body that Terraform *references* may be read from.

    Built from `text`, not the raw content: a commented-out
    `module.dependencies.orders_db_endpoint` must not attribute a consumer,
    and the raw bytes cannot tell the difference.

    Heredoc bodies are blanked by `text` and an IAM policy document lives in
    one, so their `${...}` interpolations are written back — only those. A
    heredoc is data; restoring it wholesale would make a plain-prose mention
    ("see aws_db_instance.d for the connection details") read as a live
    reference.
    """
    return _restore(content, text, start, stop, narrowed, widest)


def _subject_body(content: str, text: str, start: int, stop: int,
                  heredocs: list) -> str:
    """The block body that an IRSA *subject* may be read from.

    A subject is the opposite case to a reference: `system:serviceaccount:ns:sa`
    is a literal string sitting in the policy document, not an interpolation,
    so narrowing to `${...}` would find none of them and the whole IRSA chain
    would go dark. Whole heredoc bodies are restored here for that reason —
    minus their `#` comment lines, which are blanked as `literal_text` blanks
    them: a decommissioned bucket's ARN left in a commented line of a values
    heredoc is not a grant, and this view is also what the literal-ARN
    attribution reads.

    Deliberately NOT the same rule as `_reference_body`, which leaves a
    `${...}` on a commented line live. The two views disagree because
    Terraform does: it interpolates a reference wherever it appears in a
    heredoc, `#` or not, so the dependency edge is real and the `#` is a
    comment only to whatever consumes the rendered text (issue 27 records
    that over-attribution). A literal string on a commented line creates no
    edge at all — it is text, and nothing resolves it — so reading it would
    invent a grant rather than follow one.
    """
    body = _restore(content, text, start, stop, heredocs)
    # Bisected rather than walked: visiting every heredoc in the file per
    # block is the quadratic shape `_restore`'s docstring warns about. The
    # one-span lookback is belt and braces — `_restore` (called with the
    # default `widest=0`) cannot have restored a span beginning before
    # `start`, so that span is still blank here and blanking it again is a
    # no-op — and it costs one comparison.
    index = max(bisect.bisect_left(heredocs, (start, -1)) - 1, 0)
    for h_start, h_stop in heredocs[index:]:
        if h_start >= stop:
            break
        low, high = max(h_start, start), min(h_stop, stop)
        if low < high:
            body = (body[:low - start]
                    + heredoc_body_without_comments(body[low - start:high - start])
                    + body[high - start:])
    return body


def _resolve(index: _Index, directory: str, address: str, attribute: str | None,
             hops: int = 0, key: str | None = None) -> list:
    """Follows a reference to the addresses it ultimately names.

    `module.dependencies.orders_db_endpoint` resolves through the local
    module's `outputs.tf` to `module.orders_rds`. A reference to something
    that is not a local module, or an output that cannot be found, resolves to
    itself — the caller decides whether that address is a datastore.

    Every hop is returned, not only the leaf. A local wrapper module can be
    recorded as a datastore in its own right — `source = "./modules/rds"`
    matches the `rds` pattern — while the resource inside it is recorded
    separately as a template. Returning only the leaf attaches the consumer to
    the template and leaves the real, named entry claiming that no workload
    references it, which is exactly the affirmative falsehood the unattributed
    note exists to avoid. Both are the same database; the entry that survives
    the merge should carry the consumer whichever one it is.

    Memoised on the whole argument tuple. The walk is pure given the index and
    it restarts from scratch at every referencing site, so without a cache the
    work is `references ** hops`: four levels of local wrappers whose outputs
    each re-export the child's whole map under forty keys, read from twenty
    releases, measured 50.1s without the cache and 2.3s with it, for the same
    answer. That runs inside `scan_data_dependencies`, which is one
    synchronous tool call.

    `hops` is part of the key because the limit truncates the answer, so the
    same reference reached at a different depth is a different result. The
    returned list is shared between callers, all of which only read it.
    """
    memo_key = (directory, address, attribute, hops, key)
    cached = index.resolved.get(memo_key)
    if cached is not None:
        return cached
    resolved = _resolve_uncached(index, directory, address, attribute, hops, key)
    index.resolved[memo_key] = resolved
    return resolved


def _resolve_uncached(index: _Index, directory: str, address: str,
                      attribute: str | None, hops: int, key: str | None) -> list:
    """`_resolve` without the cache. See its docstring."""
    if hops >= _MAX_HOPS:
        # The address itself is already known, so record it rather than
        # returning nothing: the limit exists to stop a cycle in a malformed
        # repository, not to discard a datastore reached at exactly the last
        # permitted hop.
        return [(directory, address)]
    if not address.startswith("module."):
        return [(directory, address)]
    here = (directory, address)
    alias = address.split(".", 1)[1]
    target_dir = index.module_dir(directory, alias)
    if target_dir is None or attribute is None:
        # A registry module, or a whole-module reference. The address stands
        # for the module itself, which is how datastores.py records a data
        # service declared as one.
        return [here]
    entry = index.outputs.get((target_dir, attribute))
    if entry is None:
        return [here]
    expression, expression_mask = entry
    consumed = False
    if key:
        # The reference selects one entry of a map output. Expanding the whole
        # body here is what makes `endpoints["orders"]` and
        # `endpoints["catalog"]` indistinguishable, and a grouped map with one
        # endpoint per service is an ordinary way to publish them — so every
        # consumer of any key would collect every datastore in the map, and an
        # orders migration would park the catalog team.
        selected = _map_entry(expression, expression_mask, key)
        if selected is not None:
            # "" means the map parsed and this key is not in it: expand
            # nothing rather than everything.
            expression = selected
            # The key has been used up. Carrying it on would re-apply it to
            # whatever this entry resolves to — and if that is itself a map
            # (`orders = module.orders_db.conf`, where `conf` is
            # `{ host = ..., port = ... }`) the stale key is absent there, the
            # new absent-key state expands nothing, and the chain dies.
            consumed = True
    resolved = [here]
    for ref_address, ref_attribute, ref_key in _references(expression):
        # The caller's key carries across a hop that does not re-subscript:
        # `output "endpoints" { value = module.inner.endpoints }` re-exports
        # the map wholesale, and dropping the key there expands the inner map
        # entirely — the same cross-attribution, one hop further out.
        resolved.extend(_resolve(index, target_dir, ref_address, ref_attribute,
                                 hops + 1,
                                 key=ref_key if consumed else (ref_key or key)))
    return resolved


def _summarise(items: list, limit: int = 5) -> str:
    """A comma list, capped. These go into notes the agent relays verbatim."""
    if len(items) <= limit:
        return ", ".join(items)
    return ", ".join(items[:limit]) + f" and {len(items) - limit} more"


# Every character that can precede a map literal, i.e. that puts what follows
# in value position. An assignment, a function argument, the next element of a
# list or argument list, and both arms of a conditional:
#
#     value    = { ... }          =
#     merge(     { ... }, ...)    (  and then ,
#     [          { ... } ]        [
#     cond ?     { ... } : { }    ?  and then :
#
# A block body is preceded by its own identifier instead — `precondition {`,
# `dynamic "x" {` — which is how the two are told apart. The set IS the
# contract: a delimiter missing from it makes a real map read as a block body
# and silently changes the answer, which is how the conditional form went
# wrong.
_VALUE_POSITION = "=(,[?:"


def _opens_a_map(mask: str, open_at: int) -> bool:
    """True when the `{` at `open_at` starts a map literal, not a block body.

    Read from the mask, so a brace inside a string cannot be mistaken for
    either.
    """
    i = open_at - 1
    while i >= 0 and mask[i] in " \t\r\n":
        i -= 1
    return i >= 0 and mask[i] in _VALUE_POSITION


def _map_entry(expression: str, mask: str, key: str) -> str | None:
    """The value assigned to `key` at the top level of a map output body.

    Walks the map's entries by counting delimiters rather than by matching
    lines, because the two shapes a line-based reader misses are both
    ordinary. A per-service map of OBJECTS puts the key line as `orders = {`
    and its contents one level further down; a map written on one line puts
    every key after the same `=`. In both, a line reader finds no entry, the
    caller falls back to the whole body, and every key's datastore is
    attributed to every key's consumer — the cross-team attribution the key
    capture exists to prevent, not the documented unresolvable-key fallback.

    EVERY top-level map literal is walked, not just the first — and only map
    literals, judged by the delimiter in front of the brace (`_VALUE_POSITION`).
    A nested block body like an output's `precondition` is skipped, or its
    arguments would read as map keys and make a non-map look parsed. Both arms
    of a conditional count, and their keys land in one pool: which arm a
    `var.enabled ? {...} : {...}` takes is not knowable here, so a reference to
    a key in either resolves to that key's value. A composed
    output — `merge({ orders = ... }, { catalog = ... })` — puts later keys in
    later groups, and stopping at the first close reports them as absent. That
    is indistinguishable from "not a map", so the caller falls back to the
    whole body and every key's datastore reaches every key's consumer. The
    asymmetry is the tell: the first key isolates and the rest silently do not.

    Delimiters are counted in the lexer's mask, so a `,` or `}` inside a
    quoted value cannot split an entry or close the group early.

    Three outcomes, and the middle one is the point:

      the value      the map assigns that key
      ""             the map was parsed, has keys, and yours is NOT one of them
      None           no top-level key was parsed at all — a `for` expression,
                     or a bare reference, so this is not a map this can read

    Only the last falls back to the whole body. Collapsing the middle case
    into it is what let a composed output — `merge(local.shared, { orders =
    ... })`, where some keys come from outside the literal groups — hand one
    team's workload every other team's datastore. If the map parsed and the
    key is absent, the honest answer is that this reference reaches nothing
    known, which is the under-detection this module prefers.
    """
    entries, at = [], 0
    while True:
        open_at = mask.find("{", at)
        if open_at == -1:
            break
        if not _opens_a_map(mask, open_at):
            # A nested BLOCK body, not a map literal. An `output` may carry a
            # `precondition`, and its `condition = ...` / `error_message = ...`
            # lines match the key pattern exactly — so the value would count as
            # a parsed map, an absent key would expand nothing instead of
            # falling back, and a bare re-export like
            # `value = module.db.endpoints` would lose its consumer entirely.
            at = open_at + 1
            continue
        entry_start, depth = open_at + 1, 1
        for i in range(open_at + 1, len(mask)):
            char = mask[i]
            if char in "{[(":
                depth += 1
            elif char in "}])":
                depth -= 1
                if depth == 0:
                    entries.append(expression[entry_start:i])
                    break
            elif depth == 1 and char in ",\n":
                entries.append(expression[entry_start:i])
                entry_start = i + 1
        else:
            # Unterminated group: nothing after it can be trusted either.
            break
        at = i + 1
    parsed_any = False
    for entry in entries:
        match = _MAP_KEY_RE.match(entry.strip())
        if not match:
            continue
        parsed_any = True
        if match.group(1) == key:
            return match.group(2)
    return "" if parsed_any else None


def _attached_policies(index: _Index, directory: str, role_address: str) -> set:
    """Policies bound to a role by a separate attachment resource.

    The textbook hand-written IRSA wiring puts the role→policy edge on a third
    block: the role body never names the policy and the policy body never names
    a subject, so following only the role's own references finds nothing. Both
    sides are matched by address, so a `role = aws_iam_role.carts.name` and a
    `policy_arn = aws_iam_policy.carts.arn` reconnect here.
    """
    attached = set()
    for attach_dir, declared, own_address in index.attachments:
        if attach_dir != directory:
            continue
        role_value = (declared or {}).get("role") or ""
        # Matched on the full address only. A bare-label match would pair
        # `module.carts` with `aws_iam_role.carts` — two different principals —
        # and `this` is a common enough label that the collision is routine.
        # `module.carts.iam_role_name` still resolves here, because a
        # reference's address is its first two parts.
        if not any(a == role_address for a, _, _k in _references(role_value)):
            continue
        if own_address:
            attached.add(own_address)
        attached.update(
            a for a, _, _k in _references((declared or {}).get("policy_arn") or ""))
    return attached


def _consumer(workload: str, kind: str, namespace: str | None,
              source_path: str | None, detection: str, evidence: str) -> dict:
    """One `consumers` entry, schema-shaped."""
    return {
        "workload": workload,
        "kind": kind,
        "namespace": namespace,
        "source_path": source_path,
        "detection": detection,
        "evidence": evidence,
    }


def _namespace_of(args: dict) -> str | None:
    """The namespace argument when it is a literal, else None.

    A namespace that comes from `kubernetes_namespace_v1.catalog.metadata[0]
    .name` is an expression; the block label it points at is a good guess and
    deliberately not made. A null is never a guess, here as in datastores.py.
    """
    namespace = args.get("namespace")
    return namespace if isinstance(namespace, str) else None


_PATH_CHART_RE = re.compile(r'\bchart\s*=\s*"(\$\{path\.module\}/[^"$]*)"')


def _chart_path(args: dict, rel_path: str, raw_body: str = "") -> str | None:
    """A local chart path, normalized against the file that declares it.

    `chart = "../../../src/catalog/chart"` is the strongest link this module
    produces to a *source directory*, which is what a developer's component
    scope is made of. A chart named from a registry (`bitnami/postgresql`) is
    not a path and yields None.
    """
    chart = args.get("chart")
    if chart is None and raw_body:
        # The parser nulls every interpolated string, rightly — but
        # `${path.module}/charts/x` is the ordinary in-module spelling and
        # resolves without a plan, so that one shape is read back from the
        # raw body. Nothing else interpolated is.
        found = _PATH_CHART_RE.search(raw_body)
        chart = found.group(1) if found else None
    if not isinstance(chart, str):
        return None
    # `${path.module}/charts/x` is the in-module spelling and names the
    # declaring directory — as local as `./charts/x`. `${path.root}` and
    # `${path.cwd}` are NOT resolved: this scan's root-module model is the
    # declaring file's directory, and mapping them to the repository root
    # gave a release in `envs/prod` a `source_path` that held someone else's
    # chart — wrong where the baseline was honestly unknown. A bare `charts/x`
    # is not told from a repository chart.
    base = os.path.dirname(rel_path)
    if chart.startswith("${path.module}/"):
        chart = "./" + chart[len("${path.module}/"):]
    if not chart.startswith("."):
        # An absolute path is a chart on the operator's disk, not in the
        # repository, and a registry name is not a path at all. Neither can be
        # a component scope, and the schema promises a repository-relative one.
        return None
    resolved = os.path.normpath(os.path.join(base, chart))
    if resolved in (".", "") or resolved.startswith(".."):
        # `../../..` climbing out of the checkout leaves nothing a component
        # scope could match against.
        return None
    return resolved


def attach_consumers(entries: list, root_dir: str,
                     scope: dict = None) -> tuple[list, set, set, list, list]:
    """Fills `consumers` on every entry the references can attribute.

    Returns (scan-level notes, paths the scan could not fully read, paths the
    confirmed scope excluded, every workload the walk saw, and the literal
    `key = "value"` pairs each deploying block states — the raw material
    `inferred.py` types into guesses, collected here because this walk
    already holds every deploying block's body and its consumer record).

    That last list is the pool of candidates the human review ranks when it
    asks who owns an unattributed entry. It is collected here rather than
    rebuilt there because it is free at this point — the walk has already
    found every deploying block and every IRSA subject — and because it must
    describe the same commit the entries do.

    Entries are matched on `(declaring directory, address)`. The directory has
    to be part of the key for the same reason `merge_datastores` includes it
    in its own: two modules both labelled `this` in different directories are
    two datastores, and must not collect each other's consumers.

    Referenced entries — known from an ARN, declared nowhere — are matched on
    the ARN itself, repository-wide: an ARN names one resource in one account
    wherever it is written.
    """
    index = build_index(root_dir, scope)
    notes = list(index.notes)
    config_pairs = []

    # Every workload seen, attributed or not, first spelling kept.
    pool, seen_workloads = [], set()

    def remember(consumer: dict) -> None:
        # `source_path` is in the key because a pool entry stands for a
        # deploying block, and two releases of one name in one namespace
        # deploying different charts are two of them. Collapsing them left one
        # entry, which made the lookup in `from_the_scan` look unambiguous and
        # copy the surviving block's chart path onto the other one's consumer
        # The identity axes had the same bug before this one; this is it
        # on the detail axis.
        key = (consumer["workload"], consumer["kind"], consumer["namespace"],
               consumer.get("source_path"))
        if key not in seen_workloads:
            seen_workloads.add(key)
            # A copy: the same dict is attached to every entry this consumer
            # reaches, and the pool is persisted separately.
            pool.append(dict(consumer))

    # (dir, address) -> the entries declared there. A list because one block
    # can be recorded more than once before merge_datastores dedupes.
    by_address = {}
    # handle (the referenced entry's address: its ARN or canonical endpoint)
    # -> the entries it names. A declared entry's `arn` is null until the
    # merge fills it from a referenced twin, so this is exactly the
    # referenced set; the twin collects its consumers by block address and
    # the two lists union when they fold.
    by_handle = {}
    for entry in entries:
        if entry.get("detection") == "referenced" and entry.get("address"):
            by_handle.setdefault(entry["address"], []).append(entry)
            continue
        address = entry.get("address")
        if not address:
            continue
        evidence = (entry.get("evidence") or [""])[0]
        by_address.setdefault(
            (os.path.dirname(evidence), address), []).append(entry)

    def literal_grants(literal: str) -> list:
        """The recorded ARNs and endpoints a literal slice states, in order."""
        if not by_handle or not literal:
            return []
        return [found.handle for found in find_literals(literal)
                if found.kind == "recorded" and found.handle in by_handle]

    def names_a_grant(text: str, directory: str, boundary_refs=(),
                      literal: str = "") -> bool:
        """True when this slice carries a grant of its own.

        A grant is a recorded datastore named directly — by reference or by
        literal ARN — or an IAM policy: the per-app spelling is as often
        `role_policy_arns = { policy = ... }` as a bare ARN, and a group that
        names its app's policy is just as much that app's alone.

        Deliberately not "references anything": `provider_arn =
        module.eks.oidc_provider_arn` grants nothing, and counting it makes
        every `iam-role-for-service-accounts-eks` group look self-sufficient,
        so the fall back to the whole body stops firing and the chain dies.
        """
        if literal_grants(literal):
            return True
        for address, attribute, key in _references(text):
            if address in boundary_refs:
                # A boundary is an aws_iam_policy but grants nothing, so
                # counting it would stop the walk on a group that has no grant
                # and leave the real one, further out, unread.
                continue
            if (directory, address) in index.policies:
                return True
            for target in _resolve(index, directory, address, attribute, key=key):
                if target in by_address:
                    return True
        return False

    def record(targets, consumer):
        for target in targets:
            for entry in by_address.get(target, []):
                # `setdefault`, not `[...]`. `_entry` always sets the key
                # today, but this function is where a future harvester of
                # ARNs or endpoints would feed entries built elsewhere, and a
                # KeyError here would abort the whole scan with nothing
                # written — the one way of failing this module never chooses.
                consumers = entry.setdefault("consumers", [])
                if consumer not in consumers:
                    # A COPY per entry. One deploying block reaching two
                    # datastores appended the same dict to both, and anything
                    # that later filled a field on one — the data review
                    # completing a chart path, say — wrote it onto the other
                    # as well. `copy.deepcopy` is memoised, so the aliasing
                    # survived into the scan's baseline while the review's
                    # JSON round-trip broke it, and the two paths stopped
                    # producing the same section.
                    consumers.append(dict(consumer))

    def record_arns(literal: str, consumer: dict) -> None:
        for handle in literal_grants(literal):
            for entry in by_handle.get(handle, []):
                consumers = entry.setdefault("consumers", [])
                if consumer not in consumers:
                    consumers.append(dict(consumer))

    for (directory, rel_path, resource_type, name, args, raw_body,
         metadata, literal_body) in index.deploys:
        # helm_release states name/namespace at the block's own level; every
        # kubernetes_* resource states them inside `metadata`. Whichever the
        # declaration used, the block label is only the last resort.
        declared_name = metadata.get("name") or args.get("name")
        # `name` is the block's second label, which `_BLOCK_RE` makes optional:
        # a malformed `resource "helm_release" {` has none. Falling through
        # would emit a consumer with a null workload, which the schema forbids
        # — and `save_inventory` validates, so one malformed block anywhere in
        # the checkout would throw the entire scan away with nothing written.
        # Every other malformed-input path here degrades to a note and carries
        # on, and this one must too.
        if not (isinstance(declared_name, str) and declared_name) and not name:
            declared_name = f"unnamed {resource_type} in {rel_path}"
        consumer = _consumer(
            declared_name if isinstance(declared_name, str) and declared_name
            else name,
            resource_type,
            metadata.get("namespace") or _namespace_of(args),
            _chart_path(args, rel_path, raw_body),
            "terraform_wiring",
            rel_path,
        )
        remember(consumer)
        for address, attribute, key in _references(raw_body):
            record(_resolve(index, directory, address, attribute, key=key), consumer)
        # A release handed a bucket's ARN in a `set {}` value, or a ConfigMap
        # carrying one in its data, states the dependency as plainly as a
        # reference does.
        record_arns(literal_body, consumer)
        # Deferred import for the same cycle reason as `harvest_datastores`.
        from .inferred import terraform_pairs
        if not resource_type.startswith("kubernetes_secret"):
            # A Secret's values are secrets by placement, in Terraform as in
            # a manifest: none is typed into a guess, whose identifier would
            # be the value in a member-readable section. Its literal ARNs
            # and endpoints were read above. A release's `set_sensitive {}`
            # blocks are the same claim made per value, and are blanked for
            # the typing pass only.
            for key, value in terraform_pairs(_without_sensitive_sets(literal_body)):
                config_pairs.append((dict(consumer), key, value, rel_path))

    for (directory, rel_path, role_address, subjects, raw_body,
         boundary_refs, body_mask, literal_body) in index.roles:
        # The role names the service account; the ARNs are in the policy. The
        # second hop expands ONLY blocks that are IAM policies and are not this
        # role's permissions boundary — a role body also names its provider,
        # its OIDC module and its boundary, and expanding those hands every
        # datastore they mention to this one service account.
        attached = _attached_policies(index, directory, role_address)
        # Subjects deduplicated by identity, keeping every position each was
        # named at. One service account listed under two OIDC providers of the
        # same role is one consumer, not two competing ones.
        by_identity = {}
        for namespace, account, position, groups in subjects:
            entry = by_identity.setdefault((namespace, account),
                                           {"positions": [], "groups": []})
            entry["positions"].append(position)
            entry["groups"].append(tuple(groups))

        # Does this block hand different subjects different things?
        #
        # If every subject sits in the same set of groups, it does not: one
        # role's trust policy naming three service accounts really does give
        # all three everything the role is granted, whether the policy is a
        # heredoc (no groups at all) or a jsonencode (all subjects inside the
        # same braces). Whole body for each, attachments included.
        #
        # If subjects sit in DIFFERENT groups, the block is a per-app wrapper
        # and each grant belongs to one app. Three consecutive review rounds
        # found a fresh cross-attribution in successive attempts to decide
        # which — so the rule is now conservative by construction: a subject
        # gets the innermost enclosing group that both carries a grant and
        # holds no other subject, and nothing at all if there is none. An
        # unattributed entry says so honestly; a misattributed one gates the
        # wrong team.
        group_sets = {tuple(sorted(set().union(*e["groups"]))) if e["groups"] else ()
                      for e in by_identity.values()}
        # Two ways to know. Subjects landing in DIFFERENT group sets is the
        # direct evidence. But that only fires when the scan recognised a
        # subject in more than one app, and `_NS_SA_RE` matches a quoted
        # literal only — so an app whose subject list is
        # `var.orders_service_accounts` is invisible while its grant is still
        # read out of the whole body, and the one app the scan CAN see
        # collects every sibling's datastore.
        #
        # So the block's shape is consulted too: sibling groups under one
        # parent GROUP that each carry a grant of their own are a per-app map,
        # however many subjects were recognised. A shared role's trust policy
        # has sibling groups as well (`Condition`, `StringEquals`), but they
        # name no grants, so this stays quiet on it — and two top-level
        # argument maps are not siblings at all here, or a single-app module
        # granting through two of them would read as per-app and lose both.
        # Only a module call can hand different apps different things. One
        # `aws_iam_role` is one set of permissions no matter how its trust
        # policy is spelled — so applying the per-app rule to a resource block
        # only misfires: two `inline_policy` blocks look like grant-bearing
        # siblings, and a `jsonencode` trust policy with two Statements looks
        # like differing group sets. Either then drops every grant the role
        # has, and the entry claims the IRSA chain never reached it.
        # A block is per-app when its subjects land in different groups, or
        # when sibling groups each carry a grant — by reference on their own
        # terms, by literal ARN only when those siblings are keyed map
        # entries rather than list elements. That last qualification is what
        # keeps a module call whose inline policy lists two statements naming
        # two queue ARNs from flipping to per-app the moment those queues are
        # recorded, which would strand a subject sitting at the block's own
        # level and lose the declared datastore it was attributed to before.
        # See `per_app_siblings`.
        all_positions = [p for e in by_identity.values() for p in e["positions"]]
        families = _sibling_groups(_all_groups(body_mask))

        def distinguishes_an_app(positions: list) -> bool:
            """Does the block give this subject a slot of its OWN?

            True when the group the subject sits in has a sibling that does
            NOT hold it. That is what an app map looks like from the subject
            side: `oidc_providers = { carts = {...}, orders = { ns_sa =
            var.orders } }` gives `carts-sa` the `carts` slot, and `orders`
            — recognised subject or not — is another app.

            Two shapes are NOT that, and keyed grant-bearing siblings alone
            read both as per-app:

            - `oidc_providers = { main = {...} }`, one app's statements
              keyed by label under `policy_statements`. The subject's group
              has no sibling at all.
            - `oidc_providers = { blue = {...}, green = {...} }` naming ONE
              service account in both — blue/green, two regions, or two
              clusters trusting one app. Every sibling holds the same
              subject, so none of them tells an app from another.

            Either way the subject strands: its group carries no grant, has
            no grant-bearing sibling to widen into, and the block yields
            nothing — the literal ARNs and the reference-chain grants
            alongside them. A declared table the Terraform plainly wires up
            then reports that nothing reaches it, which is not the honest
            under-detection this module prefers but a loss of an
            attribution the scan used to make.

            Asking after the key TEXTS instead — do the grant map's keys
            match the subject map's? — is not open to this code: the lexer
            blanks string contents, so a quoted key is not there to read.
            The shape is, and it answers the same question.
            """
            own = set(positions)

            def holds(span: tuple) -> bool:
                return any(span[0] <= p < span[1] for p in own)

            # Returns (has a slot, the keys of its slots). Only a KEYED group
            # can be a slot: an app map's entries are the values of named
            # keys, and a list element never is — the two `Statement`
            # objects of a trust policy, one holding the subject's `:sub`
            # condition and one not, are not two apps. And the key is kept
            # so the caller can ask whether the same key also keys a grant
            # sibling: a wrapper keys `oidc_providers` and `app_policies` by
            # the same app names, while `oidc_providers = { main, defaults }`
            # beside `policy_statements = { read, write }` shares no key —
            # nor does `Condition = { StringEquals, StringLike }` — and both
            # of those read as per-app on the shape alone, stranding the
            # subject and losing a declared table origin/main attributed.
            # Returns (strong, weak keys). A sibling that does not hold the
            # subject but carries a subject ARGUMENT of its own
            # (`namespace_service_accounts = var.orders_sas`) is another
            # app's slot outright — a STRONG slot, per-app whatever the keys
            # say, since reading it as single-app hands one service account
            # its neighbour's bucket. A sibling carrying none (`defaults = {
            # provider_arn = "" }`, `StringLike = {...}`) is a WEAK slot, and
            # counts only when its key also keys a grant sibling.
            # "Carries a subject argument" is read from the slot the subject
            # actually sits in — the argument name holding it there — not
            # assumed from one module's spelling: a house wrapper that says
            # `subject = var.orders_subject` in its second slot is as much
            # another app as one saying `namespace_service_accounts = var.x`,
            # and keying on the upstream name alone read it as single-app
            # and handed one service account the other app's bucket.
            spans = _all_groups(body_mask)

            def innermost(p: int) -> tuple:
                # The innermost KEYED group: a subject inside a list element
                # (`subjects = [{ name = "system:…" }]`) holds under the
                # key naming the list, `subjects`, not under the element's
                # own `name` — a generic key any sibling may carry, which
                # made a `defaults = { name = "…" }` a strong slot.
                holder = min((g for g in spans if g[0] <= p < g[1]),
                             key=lambda g: g[1] - g[0])
                while not _is_keyed_group(body_mask, holder[0]):
                    parents = [g for g in spans
                               if g[0] < holder[0] and g[1] > holder[1]]
                    if not parents:
                        break
                    holder = min(parents, key=lambda g: g[1] - g[0])
                return holder

            strong, keys = False, set()
            for family in families:
                for group in family:
                    if not holds(group) or not _is_keyed_group(body_mask, group[0]):
                        continue
                    key = _group_key(raw_body, body_mask, group[0])
                    if key and _CONDITION_OPERATOR_RE.match(key):
                        # A trust policy's condition map: never a slot.
                        continue
                    others = [s for s in family if s != group and not holds(s)]
                    if not others:
                        continue
                    held_under = {
                        _holding_argument(raw_body, body_mask, innermost(p), p)
                        for p in own if group[0] <= p < group[1]}
                    names = {n for n in held_under if n} or {"namespace_service_accounts"}
                    if any(_carries_argument(raw_body, body_mask, s, n)
                           for s in others for n in names):
                        strong = True
                    if key:
                        keys.add(key)
            return strong, keys

        # Gates the LITERAL clause only. References decided per-app-ness
        # before literal ARNs existed and keep deciding it on their own
        # terms; narrowing them here would drop attributions for a reason
        # that has nothing to do with them. The reference clause can strand a
        # subject the same way — `policy_statements = { read = { resources =
        # [aws_dynamodb_table.reads.arn] }, write = {...} }` under one
        # provider entry loses both tables — but that predates literal ARNs
        # and changing it is a decision about the reference chain, tracked
        # separately rather than folded in here.
        slots = [distinguishes_an_app(e["positions"]) for e in by_identity.values()]
        strong_slot = any(strong for strong, _ in slots)
        subject_slot_keys = set().union(*(keys for _, keys in slots)) if slots else set()

        def per_app_siblings() -> bool:
            """Sibling groups that each carry a grant of their own.

            References decide on their own terms. A literal ARN decides only
            when the siblings are KEYED — the values of named map entries
            (`carts = {...}`, `carts: {...}`) rather than elements of a list
            (`[{...}, {...}]`). That is what tells an `apps = { carts = {...},
            orders = {...} }` map, genuinely per-app, from
            `extra = jsonencode({Statement = [{Resource = arn:…a}, {Resource
            = arn:…b}]})`, which is two grant-bearing siblings inside a
            SINGLE app's inline policy — reading that as per-app strands the
            subject, which is why literal ARNs were kept out of this test at
            all.

            Keyed is necessary and not sufficient, because a map key is
            not always an app: `policy_statements = { read = {...}, write =
            {...} }` keys ONE app's statements by label. So a literal flip
            also needs `subject_among_siblings` — some recognised subject
            whose group has a sibling that does not hold it, which is how a
            per-app wrapper hands each app its own subject slot. See
            `distinguishes_an_app` for the two single-app shapes that a
            looser reading of the same idea lets through.

            An earlier shape of this asked instead WHERE the subject sits,
            requiring it inside a grant-bearing sibling. It looks equivalent
            and is not: a wrapper that keys grants under `app_policies` and
            subjects under `oidc_providers` has no subject inside either
            grant, so the block read as single-app and one service account
            collected every app's datastore. Asking only that the subject
            have a sibling somewhere keeps that wrapper working.
            """
            for family in families:
                # References first, and on their own terms: two siblings that
                # each name a recorded datastore by reference are a per-app
                # map whatever else the block holds, which is how this read
                # before literal ARNs existed. Requiring a subject inside one
                # of them breaks the ordinary wrapper that keys grants under
                # `app_policies` and subjects under `oidc_providers` — the
                # subject sits in neither bearing sibling, the block reads as
                # single-app, and one service account collects every app's
                # table.
                by_reference = [(start, stop) for start, stop in family
                                if names_a_grant(raw_body[start:stop], directory,
                                                 boundary_refs)]
                if len(by_reference) > 1:
                    return True
                # Literal grants need a second test, and only they do: two
                # `Statement` objects in ONE app's inline policy are
                # grant-bearing siblings too, and reading them as two apps
                # strands the subject.
                #
                # The discriminator is how the sibling is KEYED, not where a
                # subject sits. An app map's siblings are the values of named
                # keys (`carts = {`); a `jsonencode({Statement = [{…}, {…}]})`
                # list's are anonymous elements (`[{`, `, {`). Keying it on a
                # subject instead looked right while the subject happened to
                # live inside the app's own group, and misattributed every
                # app's datastore to one service account the moment the
                # wrapper kept subjects in a separate `oidc_providers` map —
                # which is the ordinary shape.
                by_literal = [(start, stop) for start, stop in family
                              if names_a_grant(raw_body[start:stop], directory,
                                               boundary_refs,
                                               literal_body[start:stop])
                              and _is_keyed_group(body_mask, start)]
                # `subject_among_siblings` is the second half of the keyed
                # test. Keyed siblings alone read `policy_statements = { read
                # = {...}, write = {...} }` as two apps: the subject then sits
                # under `oidc_providers = { main = {...} }`, whose group
                # carries no grant and has no sibling to widen into, so the
                # block loses EVERY grant it has — the literal ARNs and the
                # reference-chain grants alongside them, on a table the
                # Terraform plainly wires up. Losing an attribution the scan
                # used to make is not the honest under-detection this module
                # prefers; it is a declared datastore reporting that nothing
                # reaches it.
                if len(by_literal) > 1:
                    if strong_slot:
                        return True
                    # A weak slot needs its key to key a grant sibling too;
                    # an unreadable key on either side (an expression) is no
                    # match, and the block stays single-app — the direction
                    # that attributes rather than strands, which for a weak
                    # slot is the likelier truth.
                    literal_keys = {key for key in (
                        _group_key(raw_body, body_mask, start) for start, _ in by_literal)
                        if key}
                    if literal_keys & subject_slot_keys:
                        return True
            return False

        per_app = role_address.startswith("module.") and (
            len(group_sets) > 1 or per_app_siblings())

        for (namespace, account), info in by_identity.items():
            groups = info["groups"][0]
            own = set(info["positions"])
            consumer = _consumer(
                account, "service_account", namespace, None, "irsa", rel_path)
            remember(consumer)
            # What this subject may be granted: the group it was declared in
            # if that group names a datastore of its own, otherwise the whole
            # body.
            #
            # Scoping to the group is what stops a per-app module argument
            # cross-producting — `carts = { namespace_service_accounts = [...],
            # table_arn = ... }` beside an `orders = { ... }` must not hand
            # each service account the other's table.
            #
            # The test is "names a RECORDED DATASTORE", not "names anything".
            # `iam-role-for-service-accounts-eks` nests its subjects under
            # `oidc_providers` while the ARNs sit elsewhere in the block, and
            # that group almost always carries
            # `provider_arn = module.eks.oidc_provider_arn` — a reference that
            # grants nothing. A rule keyed on emptiness stops firing the moment
            # it appears, and the whole chain goes silent.
            # `literal_scope` is the same span of the literal view, kept in
            # step with `grant_scope`: the two bodies are the same length by
            # construction, so one pair of offsets slices both.
            # Innermost outward. A group shared with another subject stops
            # the walk with nothing: the parent of two app groups holds both
            # apps' grants, so widening into it is exactly how one app
            # collects its neighbour's datastore. A subject with no groups at
            # all in a per-app block is in the same position — the whole body
            # is every app's — so it gets nothing too.
            scoped_grant, scoped_literal = "", ""
            if per_app:
                for scope_start, scope_stop in groups:
                    if any(scope_start <= other < scope_stop
                           for other in all_positions if other not in own):
                        break
                    group = raw_body[scope_start:scope_stop]
                    group_literal = literal_body[scope_start:scope_stop]
                    if names_a_grant(group, directory, boundary_refs, group_literal):
                        scoped_grant, scoped_literal = group, group_literal
                        break
            grant_scope = scoped_grant if per_app else raw_body
            literal_scope = scoped_literal if per_app else literal_body
            policy_addresses = {a for a, _, _k in _references(grant_scope)}
            # Attachments are block-wide: `_attached_policies` matches on the
            # role address and cannot tell one app of a wrapper module from
            # another. Unioning them into a narrowed scope hands every app
            # every attached policy, undoing the narrowing entirely — so they
            # only apply when the scope IS the whole block.
            if grant_scope is raw_body:
                policy_addresses |= attached
            policy_addresses -= boundary_refs
            # A datastore the role block names DIRECTLY is a grant, whatever
            # kind of block it is. `iam-role-for-service-accounts-eks` — the
            # current IRSA module — takes the resource ARNs as its own
            # arguments (`mountpoint_s3_csi_bucket_arns = [...]`) and builds
            # the policy internally, so there is no policy block to expand and
            # the second hop finds nothing. That is the shape in this repo's
            # own eks-workshop-v2 fixture, where the S3 bucket was reported as
            # referenced by nothing while the module named its ARN.
            #
            # Safe to widen because `record` only fires on an address that IS
            # a recorded datastore: a role's provider, OIDC module and boundary
            # are none of those, so they still attribute nothing. What stays
            # narrow is the second hop — expanding another block's body — which
            # is where a shared boundary would hand over the whole estate.
            for address, attribute, key in _references(grant_scope):
                if address in boundary_refs:
                    continue
                record(_resolve(index, directory, address, attribute, key=key),
                       consumer)
            # The literal ARNs in the same scope. This is the whole of the
            # hand-written IRSA shape — a role whose inline policy grants
            # `s3:GetObject` on `arn:aws:s3:::acme-invoice-archive` — when the
            # bucket is declared in another repository: no reference to
            # follow, and the ARN is the only thing that names it.
            record_arns(literal_scope, consumer)
            for address in policy_addresses:
                policy_body = index.policies.get((directory, address))
                if not policy_body:
                    continue
                for inner, inner_attr, inner_key in _references(policy_body):
                    record(_resolve(index, directory, inner, inner_attr,
                                    key=inner_key), consumer)
                record_arns(index.policy_literals.get((directory, address), ""),
                            consumer)

    return notes, index.truncated, index.excluded, pool, config_pairs


TRUNCATED_NOTE = (
    "part of the Terraform could not be read to the end, so whether a workload "
    "references this is unknown — which is not the same as nothing using it")

REJECTED_NOTE = (
    "a reference chain did reach this, and a reviewer rejected the link at the "
    "data review — it has no consumer by decision, not for want of evidence. "
    "The rejection and its reason are recorded above")

# A `helm_release` value the operator marked sensitive: never typed into a
# guess (the value would become the identifier of a member-readable entry).
# Both shapes — the block `set_sensitive { … }` and the provider-3 attribute
# `set_sensitive = [{ … }]` — and `set_wo`, the write-only form.
_SENSITIVE_SET_RE = re.compile(r"\bset_(?:sensitive|wo)\b\s*=?\s*(?=[\[{])")


def _without_sensitive_sets(body: str) -> str:
    """`body` with every sensitive set's bracketed value blanked, braces
    balanced — a value holding `{` or `}` must not end the blank early."""
    out, i = [], 0
    for match in _SENSITIVE_SET_RE.finditer(body):
        start = match.end()
        if start < i:
            continue
        depth, j = 0, start
        while j < len(body):
            if body[j] in "[{":
                depth += 1
            elif body[j] in "]}":
                depth -= 1
                if depth == 0:
                    break
            j += 1
        out.append(body[i:match.start()])
        i = j + 1
    out.append(body[i:])
    return "".join(out)

ADDED_NOTE = (
    "added at the data review, and the scan does not look for its consumer — one "
    "named there and later withdrawn is recorded above; attach one with "
    "attach_data_consumer when the workload is known")

UNATTRIBUTED_NOTE = (
    "neither of the two reference chains this scan follows — a Kubernetes "
    "deploying resource reaching it through a module output, or an IRSA role "
    "granting access to it — reaches this from any workload it read. The link "
    "may be made some other way in the Terraform (through a `local`, or an "
    "endpoint passed into a child module), in a directory the confirmed scope "
    "excluded, or outside the repository entirely. A human has to attach it "
    "before it can gate")


def note_unattributed(inventory: dict, truncated: set = (),
                      excluded: set = (), rejected: set = ()) -> list:
    """Marks and counts the entries no chain reached. Runs AFTER the merge.

    `rejected` holds the ids of entries a human emptied at the data review.
    They are counted apart and get their own note, because this function's
    other two say something about the SCAN — "no chain reached it" and "a file
    could not be read" — and neither is true of an entry whose chain resolved
    and whose reviewer cut it. The rollup matters more than the note: it is
    persisted to `data_dependency_scan_notes`, carried to the assessment
    verbatim, and "could not be attributed from the Terraform alone" about a
    database the Terraform plainly wires up sends the next reader hunting for
    a link that exists and was deliberately removed.

    Attribution has to happen before merging, because entries are matched on
    the address of the block each came from. Counting has to happen after: the
    merge folds entries sharing a service, identifier and directory, so a
    pre-merge count is over a different denominator than the one the tool
    reports, and the two numbers reach the user side by side. Running after
    the merge also means an entry that gained consumers from a folded twin
    cannot end up carrying this note as well — the note is only ever written
    here, to an entry that has none by the time the section is final.
    """
    entries = inventory.get("data_dependencies") or []
    unattributed, unreadable, withdrawn, added = [], [], [], []
    for entry in entries:
        if entry.get("consumers"):
            continue
        notes_list = entry.setdefault("notes", [])
        if id(entry) not in (rejected or ()) and added_by_hand(entry):
            # The scan never produced this entry — a reviewer did, naming no
            # consumer — so "no chain reached it" is a claim about a scan
            # that never looked. Counted apart, as a rejection is; a
            # hand-added entry whose named consumer a reviewer then rejected
            # is the rejected case below, not this one.
            added.append(entry)
            if ADDED_NOTE not in notes_list:
                notes_list.append(ADDED_NOTE)
            continue
        if id(entry) in (rejected or ()):
            # A chain DID reach this one. The entry already carries the
            # reviewer's own note saying so and why; all this adds is the
            # distinction the counts need.
            withdrawn.append(entry)
            if REJECTED_NOTE not in notes_list:
                notes_list.append(REJECTED_NOTE)
            continue
        if truncated:
            # Some file could not be read to the end, so the workload that
            # references this entry may be in the part never scanned. Not
            # scoped to the entry's own directory: the terraform-wiring chain
            # runs from a deploying resource in one directory to a datastore
            # in another, so the truncation that hides a reference is almost
            # never in the same place as the entry it hides it from.
            unreadable.append(entry)
            if TRUNCATED_NOTE not in notes_list:
                notes_list.append(TRUNCATED_NOTE)
            continue
        unattributed.append(entry)
        if UNATTRIBUTED_NOTE not in notes_list:
            notes_list.append(UNATTRIBUTED_NOTE)
    notes = []
    if added:
        notes.append(
            f"{len(added)} data dependenc(y/ies) were added by hand at the data review "
            "with no consumer named: "
            + _summarise(sorted(e["identifier"] for e in added)))
    if withdrawn:
        notes.append(
            f"{len(withdrawn)} data dependenc(y/ies) have no consumer because a "
            "reviewer rejected the one the Terraform pointed at, not because "
            "the scan found none: "
            + _summarise(sorted(e["identifier"] for e in withdrawn)))
    if unattributed:
        # A list, not a set: two directories can each declare an unattributed
        # `orders-db`, and a de-duplicated list beside a count of entries
        # gives the reader two numbers that disagree.
        notes.append(
            f"{len(unattributed)} of {len(entries)} data dependenc(y/ies) could "
            "not be attributed to a workload from the Terraform alone: "
            + _summarise(sorted(e["identifier"] for e in unattributed)))
    if unreadable:
        notes.append(
            f"{len(unreadable)} data dependenc(y/ies) have no recorded consumer, "
            "but the consumer scan could not finish reading "
            + _summarise(sorted(truncated))
            + ", so whether a workload references them is unknown rather than "
            "answered: "
            + _summarise(sorted(e["identifier"] for e in unreadable)))
    # Either list, not just the first. A truncation anywhere routes EVERY
    # consumer-less entry to `unreadable`, so keying this on `unattributed`
    # alone made one malformed file suppress the exclusion caveat for the
    # whole scan — and the exclusion is still the likelier explanation, since
    # it is a place the scan was told not to look rather than one it failed to
    # finish.
    if (unattributed or unreadable) and excluded:
        # The count is deliberately not repeated. `extract_datastores`
        # already reports how many files the scope excluded, the agent relays
        # every note verbatim, and a reader who meets the same number in two
        # sentences can only conclude there were two exclusions. What this
        # note adds is the consequence, which is not stated anywhere else.
        notes.append(
            "a workload referencing one of the data dependencies named above "
            "may be in one of the Terraform files the confirmed scope "
            "excluded, which this scan did not read")
    return notes
