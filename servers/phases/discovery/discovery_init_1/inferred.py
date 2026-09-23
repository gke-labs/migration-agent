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

"""Bare-name guesses, and the YAML half of the data scan.

Pure logic — no GCS, no LLM. `datastores.py` records what the files state
exactly: a declaration, an ARN, an endpoint. This module handles the two
things they state only suggestively, and it does so by asking rather than
asserting.

A bare name. `INVOICE_BUCKET=acme-invoice-archive` in a container's env, or
`invoiceBucket: acme-invoice-archive` in a chart's values, names a bucket the
workload uses — probably. The value alone is untyped (`orders` is a database,
a namespace and a Helm release), so the KEY beside it does the typing, and
only a short list of key words is trusted to. What comes out is an entry with
`detection: "inferred"`, disposition `undecided`, and a note saying it is a
guess. It gates nothing until a human confirms it at the data review, and a
dismissed guess stays dismissed across re-scans. The holder of the key — the
Deployment, the chart, the ConfigMap — is recorded as its consumer, because
that half is not a guess: the value is literally in that workload's
configuration. What is uncertain is what the value IS.

A name that matches an entry the scan already recorded is not a new guess.
It is offered to the reviewer as the first candidate consumer for that entry,
with the reason, and never attached: this section gates a pipeline, and an
exact string match is still a match on a name (DESIGN.md issue 27).

YAML. The Terraform walk cannot see a chart's values file or a raw manifest,
and that is where an application states most of what it reaches. This module
reads them — under the confirmed scope, aliases refused, size-capped — for
three things: literal ARNs and endpoints (recorded exactly, as
`datastores.py` would, with the holder as consumer), and key-typed bare
names (recorded as guesses). Nothing else in a YAML file is read.
"""

import os
import re
from typing import NamedTuple

from servers.phases.k8s_manifests import MAX_MANIFEST_BYTES, load_manifest_documents
from servers.phases.scope_algebra import is_excluded

from . import datastores
from .files import SKIP_DIRS

YAML_EXTENSIONS = (".yaml", ".yml")
# CI configuration directories, whose YAML never states what a workload
# reaches. Other dot-directories are read: `.helm/`, `.k8s/`, `.deploy/`.
_CI_DIRS = frozenset((".github", ".circleci", ".gitlab", ".buildkite", ".drone",
                      ".travis", ".azure-pipelines", ".idea", ".vscode"))
# Tooling files that are YAML but never state what a workload reaches.
_TOOLING_YAML_RE = re.compile(
    r"^(?:docker-compose[^/]*|compose|mkdocs|codecov|\.?gitlab-ci|\.?travis|"
    r"\.?pre-commit-config|dependabot|renovate|\.?golangci|\.?yamllint|\.?markdownlint|"
    r"pnpm-workspace|\.?prettierrc|\.?eslintrc|\.?readthedocs|\.?goreleaser|"
    r"\.?dockerignore|azure-pipelines|bitbucket-pipelines|cloudbuild|skaffold|tilt)"
    r"\.ya?ml$")

# What a consumer records when the link comes from the workload's own
# configuration stating the resource — a Helm value, a manifest env var, a
# ConfigMap or Secret key — rather than from a Terraform reference chain.
CONFIG_DETECTION = "config_value"

# How an inferred entry is named to the review tools: `<service>:<identifier>`.
# It has no Terraform block and no ARN, and the tools take an address.
INFERRED_ADDRESS_SEP = ":"

GUESS_NOTE_PREFIX = "a guess: "

# Key words that type a value, most specific first. Deliberately short: every
# guess is a question a reviewer has to answer, and `TABLE` alone would ask
# about every SQL table name in the estate. `bucket` stands alone because
# nothing else is called one.
_KEY_SERVICES = (
    (re.compile(r"dynamo|ddb", re.I), "dynamodb"),
    (re.compile(r"kinesis", re.I), "kinesis"),
    (re.compile(r"sqs", re.I), "sqs"),
    (re.compile(r"sns", re.I), "sns"),
    (re.compile(r"secretsmanager|secret[_-]?(name|id)s?\b", re.I), "secretsmanager"),
    (re.compile(r"ssm[_-]?param|parameter[_-]?(store|name)", re.I), "ssm"),
    # `bucket` as a word of the key: `BUCKETING_STRATEGY` is not a bucket,
    # `bucketName` is (the key is matched on its words joined by `_`).
    (re.compile(r"(?:^|_)buckets?(?:_|$)", re.I), "s3"),
)
# A key whose suffix says the value is not a name: an ARN or URL (read as a
# literal instead), a host, a region, a prefix inside a bucket, a flag.
# Matched on the key's words joined by `_`, as a whole last word: `_EXPORT`
# is not `port`, `_ACCOUNT` is not `count`, `_TURKEY` is not `key`.
_NOT_A_NAME_KEY_RE = re.compile(
    r"(?:^|_)(arn|url|uri|host|hostname|endpoint|region|prefix|path|key|keys|"
    r"enabled|port|role|policy|count|size|ttl|timeout|version|"
    r"auth|cert|certificate|private|"
    # A knob, not a name: `S3_BUCKET_ACL=private`, `DYNAMODB_BILLING_MODE`,
    # `KINESIS_SHARD_ITERATOR_TYPE=LATEST`. Each false guess blocks the
    # sign-off until answered.
    r"acl|mode|type|protocol|class|format|encoding|level|strategy|tier|engine|"
    r"algorithm|encryption|sse|retries|retry|interval|seconds|ms|limit|max|min|"
    r"batch|concurrency|threshold|percent|ratio|flag|delay|attempts)s?$", re.I)
# A key that says its value is a credential, ANYWHERE in the key — as a word
# of it, not only its suffix: `BUCKET_PASSPHRASE`, `SQS_QUEUE_PASSWORD`,
# `DB_SECRET`. The value is not a name, and the section it would land in is
# published to every member of the workspace. `secret` counts unless the
# next word says the value is a NAME (`SECRET_NAME`, `secret_id`).
_CREDENTIAL_WORDS = frozenset((
    "password", "passwd", "passphrase", "pass", "pwd", "token", "tokens",
    "credential", "credentials", "apikey", "accesskey", "secretkey", "privatekey",
))
_WORD_SPLIT_RE = re.compile(r"[_\-.]+|(?<=[a-z0-9])(?=[A-Z])")
# Kubernetes' own Secret references, as charts spell them: a Kubernetes
# Secret, not an AWS Secrets Manager one, and nearly every chart carries one
# (`ingress.tls[].secretName`, Bitnami's `existingSecret`). Case-sensitive on
# purpose — `SECRET_NAME` in a container's env still names an AWS secret.
_K8S_SECRET_KEYS = frozenset((
    "secretName", "existingSecret", "existingSecretName", "existingSecretKey",
    "secretKeyRef", "secretKey", "tlsSecretName", "tlsSecret",
))


_CREDENTIAL_STEMS = ("password", "passwd", "passphrase", "credential", "creds",
                     "token", "apikey", "accesskey", "secretkey", "privatekey", "keyid")
# Whole words only: `AUTHOR_BUCKET` is a name.
_CREDENTIAL_WORDS = _CREDENTIAL_WORDS | frozenset(("auth", "hmac", "bearer"))
# Refused only as the LAST word, where the value IS the thing: `SNS_TOPIC_SIGNATURE`
# holds a signature, `SIGNATURE_BUCKET` and `OAUTH_STATE_BUCKET` hold names.
_CREDENTIAL_LAST_WORDS = frozenset(("signature", "signing", "oauth", "header"))
_ACCESS_KEY_ID_RE = re.compile(r"^(?:AKIA|ASIA|AROA|AIDA)[A-Z0-9]{16}$")
# A value that looks like key material rather than a name: long, base64
# alphabet, and either base64 symbols or a digit-heavy body. A CamelCase
# table name (`OrdersEventStore2024ProductionArchiveTable`) is long and mixed
# too but reads as words: few digits, no symbols.
_KEY_MATERIAL_RE = re.compile(r"^[A-Za-z0-9+/=]{32,}$")


def _credential_key(key: str) -> bool:
    """Does the key say its value is a credential? By its WORDS — a stem
    anywhere in a word (`PASSWORD2`, `BUCKETPASSWORD`), the short forms as
    whole words (`pass`, `pwd`, `pw`), `key` followed by `id`, `access`
    followed by `key`, and `secret` unless the next word says the value is a
    name. A missed guess costs one `add_data_dependency`; a published
    credential fragment cannot be taken back."""
    words = [w.lower() for w in _WORD_SPLIT_RE.split(key) if w]
    for index, word in enumerate(words):
        following = words[index + 1] if index + 1 < len(words) else ""
        if any(stem in word for stem in _CREDENTIAL_STEMS):
            return True
        if word in ("pass", "pwd", "pw") or word in _CREDENTIAL_WORDS:
            return True
        if word == "key" and following in ("id", "ids"):
            return True
        if word == "access" and following.startswith("key"):
            return True
        if word in ("secret", "secrets") and following not in ("name", "names", "id", "ids"):
            return True
    return bool(words) and words[-1] in _CREDENTIAL_LAST_WORDS


def _key_material(value: str) -> bool:
    if _ACCESS_KEY_ID_RE.match(value):
        return True
    if not _KEY_MATERIAL_RE.match(value):
        return False
    if any(c in "+/=" for c in value):
        return True
    # A name reads as WORDS: most of its characters sit in runs of three or
    # more lower-case letters (`OrdersAPIEventsProductionArchiveTable` is
    # 0.78 of them, acronyms and all). Base64 has hardly any such run
    # (0.1–0.3 measured over real keys). The cost is an ALL-CAPS name of 32+
    # characters with no separators, which reads as no words at all.
    worded = sum(len(run) for run in re.findall(r"[a-z]{3,}", value))
    return worded / len(value) < 0.4

# What a name may look like, per service. AWS's own rules, tightened where
# a looser grammar would accept ordinary words.
_NAME_GRAMMAR = {
    "s3": re.compile(r"^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$"),
    "sqs": re.compile(r"^[A-Za-z0-9_-]{1,80}(\.fifo)?$"),
    "sns": re.compile(r"^[A-Za-z0-9_-]{1,256}$"),
    "kinesis": re.compile(r"^[A-Za-z0-9_.-]{1,128}$"),
    "dynamodb": re.compile(r"^[A-Za-z0-9_.-]{3,255}$"),
    "secretsmanager": re.compile(r"^[A-Za-z0-9/_+=.@-]{1,512}$"),
    "ssm": re.compile(r"^[A-Za-z0-9_./-]{1,2048}$"),
}
_IPV4_RE = re.compile(r"^\d{1,3}(\.\d{1,3}){3}$")
_NOT_A_VALUE = frozenset(("true", "false", "yes", "no", "none", "null", "", "*"))


class Guess(NamedTuple):
    service: str
    identifier: str
    key: str
    holder: dict          # a schema-shaped consumer
    rel_path: str


def classify(key: str, value) -> tuple:
    """(service, identifier) when `key = value` reads as naming a data store,
    else None. Pure; the caller decides what to do with it."""
    if not isinstance(key, str) or not isinstance(value, str):
        return None
    value = value.strip()
    # Templated, interpolated, a URL, prose, a flag: not a name. A URL or an
    # ARN in a value is read by the literal scan instead.
    if (value.lower() in _NOT_A_VALUE or "{{" in value or "${" in value
            or "://" in value or " " in value or _IPV4_RE.match(value)
            # No queue, topic, stream, table or bucket is named by digits
            # alone: `SQS_MAX_MESSAGES=10` is a knob.
            or value.replace(".", "").replace("-", "").isdigit()
            # An AWS access key id, whatever the key beside it is called.
            or _ACCESS_KEY_ID_RE.match(value)):
        return None
    # Matched on the key's WORDS joined by `_` — `bucketName`, `s3_bucket_name`
    # and `BUCKET_NAME` read alike — so `bucket` is a word and `bucketing` is
    # not, whatever the case; the Kubernetes Secret-reference keys are tested
    # on the last dotted segment too, since a Terraform `set { name =
    # "ingress.tls.secretName" }` passes the whole path as the key.
    worded = "_".join(w.lower() for w in _WORD_SPLIT_RE.split(key) if w)
    if (_NOT_A_NAME_KEY_RE.search(worded) or _credential_key(key)
            or key in _K8S_SECRET_KEYS or key.rsplit(".", 1)[-1] in _K8S_SECRET_KEYS):
        return None
    for pattern, service in _KEY_SERVICES:
        if not pattern.search(worded):
            continue
        # A slash is a path inside a bucket or a URL fragment, except for the
        # two services whose names ARE paths.
        if "/" in value and service not in ("secretsmanager", "ssm"):
            return None
        if value.startswith("/") and service != "ssm":
            return None
        if not _NAME_GRAMMAR[service].match(value):
            return None
        if service == "s3" and ".." in value:
            return None
        # Base64 key material, judged once the service is known: a secret's
        # or a parameter's name is a PATH, and its slashes are not the base64
        # symbols that mark key material.
        if _key_material(value.replace("/", "") if service in ("secretsmanager", "ssm")
                         else value):
            return None
        return service, value
    return None


def inferred_address(service: str, identifier: str) -> str:
    return f"{service}{INFERRED_ADDRESS_SEP}{identifier}"


# --- Terraform side ---------------------------------------------------------

# `key = "value"` on one line, and the two-line `name = "K"` / `value = "V"`
# pair a `set {}` or `env {}` block spells. Read from the literal body, where
# comments are already blank and an interpolated value still shows its `${`.
_ASSIGN_RE = re.compile(
    r'(?<![A-Za-z0-9_.-])"?([A-Za-z_][A-Za-z0-9_.-]*)"?[ \t]*[=:][ \t]*"([^"\n]*)"')
_NAME_VALUE_RE = re.compile(
    r'\bname[ \t]*=[ \t]*"([^"\n]+)"[ \t]*\n[ \t]*value[ \t]*=[ \t]*"([^"\n]*)"')


def terraform_pairs(literal_body: str) -> list:
    """(key, value) pairs a deploying block's body states literally."""
    pairs = [(m.group(1), m.group(2)) for m in _NAME_VALUE_RE.finditer(literal_body)]
    pairs.extend((m.group(1), m.group(2)) for m in _ASSIGN_RE.finditer(literal_body))
    return pairs


# --- YAML side --------------------------------------------------------------

# Manifest kinds whose configuration states what a workload reaches. The same
# allowlist idea as `consumers._DEPLOY_TYPES`: a Namespace or a StorageClass
# is cluster machinery, and a consumer nobody can act on.
MANIFEST_KINDS = frozenset((
    "Deployment", "StatefulSet", "DaemonSet", "Job", "CronJob", "Pod",
    "ConfigMap", "Secret",
))
CHART_KIND = "helm_chart"


def _chart_of(rel_dir: str, root_dir: str) -> tuple:
    """(chart name, chart dir) when `rel_dir` is a chart's own directory or a
    values directory inside one (`charts/orders/values/`, `ci/`): the chart is
    the holder of every values file under it. A sub-chart has its own
    Chart.yaml and is found first; `templates/` and `crds/` are pruned before
    this is asked."""
    probe = rel_dir
    while True:
        chart_file = os.path.join(root_dir, probe, "Chart.yaml")
        if os.path.isfile(chart_file):
            break
        parent = os.path.dirname(probe)
        if not probe or parent == probe:
            return None
        probe = parent
    rel_dir = probe
    try:
        with open(chart_file, "r", encoding="utf-8-sig", errors="replace") as f:
            docs = load_manifest_documents(f.read())
    except (OSError, ValueError):
        return None
    for doc in docs:
        if isinstance(doc, dict) and isinstance(doc.get("name"), str):
            return doc["name"], rel_dir
    return None


def _scalars(node, path=()):
    """Every (key path, string) leaf under a parsed YAML node."""
    if isinstance(node, dict):
        for key, value in node.items():
            yield from _scalars(value, path + (str(key),))
    elif isinstance(node, list):
        for item in node:
            yield from _scalars(item, path)
    elif isinstance(node, str):
        yield path, node


def _env_pairs(node):
    """(name, value) from every `{name:, value:}` object in a document — the
    shape of a container env entry wherever it sits."""
    if isinstance(node, dict):
        if (isinstance(node.get("name"), str) and isinstance(node.get("value"), str)
                and len(node) <= 3):
            yield node["name"], node["value"]
        for value in node.values():
            yield from _env_pairs(value)
    elif isinstance(node, list):
        for item in node:
            yield from _env_pairs(item)


def _holder(workload: str, kind: str, namespace, source_path, evidence: str) -> dict:
    return {
        "workload": workload,
        "kind": kind,
        "namespace": namespace if isinstance(namespace, str) else None,
        "source_path": source_path,
        "detection": CONFIG_DETECTION,
        "evidence": evidence,
    }


class YamlScan(NamedTuple):
    referenced: list     # entries, detection "referenced", holders attached
    pairs: list          # (holder, key, value, rel_path)
    holders: list        # every holder seen, for the candidate pool
    notes: list
    files: int


def scan_yaml(root_dir: str, scope: dict = None) -> YamlScan:
    """Reads chart values and manifests for literals and key-typed names.

    Under the confirmed scope and the same directory pruning as the Terraform
    walk. A file that is not parseable YAML — a Helm template full of `{{`
    is the ordinary case — is skipped without a note: it is not a failure,
    and every chart has a directory of them. A file too large to parse is
    noted, because a manifest that size is unusual enough to say so.
    """
    referenced = {}
    pairs, holders, notes = [], [], []
    seen_holders = set()
    values_like, unparsed = set(), set()
    expressions, unreadable = set(), set()
    files = 0
    for dirpath, dirnames, filenames in os.walk(root_dir):
        # The CI directories too (`.github/workflows`, `.circleci`), never a
        # manifest or a chart's values — but only those: `.helm/` is werf's
        # chart convention and `.k8s/`, `.deploy/` hold manifests, and the
        # manifest the user scoped against lists them.
        dirnames[:] = sorted(d for d in dirnames
                             if d not in SKIP_DIRS and d not in _CI_DIRS)
        rel_dir = os.path.relpath(dirpath, root_dir)
        rel_dir = "" if rel_dir == "." else rel_dir
        chart = _chart_of(rel_dir, root_dir)
        if chart:
            # A chart's templates are Go templates, not YAML, whether or not
            # one happens to parse (`name: "{{ .Release.Name }}-config"`
            # does): read as a manifest it put template source into the
            # section as a workload name. Its `crds/` are cluster machinery.
            dirnames[:] = [d for d in dirnames if d not in ("templates", "crds")]
        for filename in sorted(filenames):
            if not filename.endswith(YAML_EXTENSIONS):
                continue
            if filename.startswith(".") or _TOOLING_YAML_RE.match(filename):
                # `.pre-commit-config.yaml`, `docker-compose.yml`,
                # `mkdocs.yml`, `.gitlab-ci.yml`: tooling, not the estate.
                continue
            rel_path = os.path.join(rel_dir, filename) if rel_dir else filename
            if scope and is_excluded(rel_path, scope):
                continue
            full_path = os.path.join(dirpath, filename)
            try:
                if os.path.getsize(full_path) > MAX_MANIFEST_BYTES:
                    notes.append(f"{rel_path}: skipped, larger than "
                                 f"{MAX_MANIFEST_BYTES} bytes")
                    continue
                with open(full_path, "r", encoding="utf-8-sig", errors="replace") as f:
                    content = f.read()
            except OSError as e:
                notes.append(f"{rel_path}: unreadable ({e.__class__.__name__})")
                continue
            try:
                docs = load_manifest_documents(content)
            except ValueError:
                # A Helm template full of `{{` is the ordinary case and is
                # not worth a note. A file the loader REFUSES — YAML anchors
                # and aliases are rejected on purpose — is said, since a
                # values file with anchors is common and would otherwise
                # vanish without a trace.
                if "{{" not in content and ("&" in content or "*" in content):
                    unparsed.add(rel_path)
                continue
            files += 1
            for doc in docs:
                if not isinstance(doc, dict):
                    continue
                metadata = doc.get("metadata")
                kind = doc.get("kind")
                if isinstance(kind, str) and isinstance(metadata, dict) \
                        and isinstance(metadata.get("name"), str):
                    if kind not in MANIFEST_KINDS:
                        continue
                    if "{{" in metadata["name"] or "{{" in str(metadata.get("namespace") or ""):
                        # A template outside a chart's own directory: still
                        # template source, never a workload name.
                        continue
                    holder = _holder(metadata["name"], kind, metadata.get("namespace"),
                                     None, rel_path)
                    # Not from a Secret, in any shape: its `stringData` may
                    # itself be `{name: …, value: …}`.
                    doc_pairs = [] if kind == "Secret" else list(_env_pairs(doc))
                    if kind == "ConfigMap":
                        # A ConfigMap's `data` is plain configuration.
                        source = doc.get("data")
                        if isinstance(source, dict):
                            doc_pairs.extend((str(k), v) for k, v in source.items()
                                             if isinstance(v, str))
                    # A Secret's values are secrets by placement, whatever
                    # the key is called, so none of them is typed into a
                    # guess — the guess's identifier would be the value, and
                    # the section is published to every workspace member.
                    # Its `stringData` IS still read for literal ARNs and
                    # endpoints, which are names, not secrets; the endpoint
                    # tokenizer strips any credentials from a URL first.
                    scalars = [v for _p, v in _scalars(doc)]
                elif (chart and not doc.get("kind")
                      and not (filename == "Chart.yaml" and rel_dir == chart[1])):
                    # A document WITH a `kind` but no name (a `List`, a
                    # kustomization) under a chart is not the chart's values:
                    # its scalars are not the chart's configuration.
                    holder = _holder(chart[0], CHART_KIND, None, chart[1], rel_path)
                    # The map shape (`invoiceBucket: …`) AND the env-list
                    # shape (`env: [{name: INVOICE_BUCKET, value: …}]`),
                    # which is how most charts state a bare name; read as a
                    # path leaf the latter yields `("name", …)` and
                    # `("value", …)`, neither of which types anything.
                    doc_pairs = ([(path[-1], v) for path, v in _scalars(doc) if path]
                                 + list(_env_pairs(doc)))
                    scalars = [v for _p, v in _scalars(doc)]
                else:
                    if filename != "Chart.yaml" and not doc.get("apiVersion"):
                        # A mapping with no `kind`, outside any chart's own
                        # directory: the values file a `helm_release` names
                        # (`envs/prod/values/orders-prod.yaml`), a chart's
                        # `ci/` values, an Ansible vars file. Not read — its
                        # holder would be a guess about a guess — and SAID,
                        # since the prod hostname often lives exactly there.
                        values_like.add(rel_path)
                    continue
                key = (holder["workload"], holder["kind"], holder["namespace"],
                       holder["source_path"])
                if key not in seen_holders:
                    seen_holders.add(key)
                    holders.append(dict(holder))
                for value in scalars:
                    for found in datastores.find_literals(value):
                        if found.kind == "expression":
                            expressions.add(found.token)
                            continue
                        if found.kind == "malformed":
                            unreadable.add(found.token)
                            continue
                        if found.kind != "recorded":
                            continue
                        entry = referenced.get(found.handle)
                        if entry is None:
                            entry = datastores._referenced_entry(found, rel_path)
                            referenced[found.handle] = entry
                        elif rel_path not in entry["evidence"]:
                            entry["evidence"].append(rel_path)
                        consumer = dict(holder)
                        if consumer not in entry["consumers"]:
                            entry["consumers"].append(consumer)
                for k, v in doc_pairs:
                    pairs.append((dict(holder), k, v, rel_path))
    def _listed(items):
        items = sorted(items)
        return ", ".join(items[:5]) + (f" and {len(items) - 5} more" if len(items) > 5 else "")
    if expressions:
        # The Terraform walk's note, for the YAML side: what it promises for
        # a variable-built literal it has to keep here too.
        notes.append(
            f"{len(expressions)} ARN(s) or endpoint(s) in chart values or manifests build "
            "the resource name from a variable and were not recorded — the real name is "
            "not knowable from the files alone: " + _listed(expressions))
    if unreadable:
        notes.append(
            f"{len(unreadable)} ARN- or endpoint-shaped string(s) in chart values or "
            "manifests could not be read and were not recorded: " + _listed(unreadable))
    if unparsed:
        notes.append(
            f"{len(unparsed)} YAML file(s) in scope could not be parsed (anchors and aliases "
            "are refused) and were not read for data services: " + _listed(unparsed))
    if values_like:
        listed = sorted(values_like)
        notes.append(
            f"{len(values_like)} YAML file(s) in scope are neither manifests nor under a "
            "chart's directory and were not read for data services — a values file a "
            "helm_release names from outside its chart: "
            + ", ".join(listed[:5]) + (f" and {len(listed) - 5} more" if len(listed) > 5 else "")
            + ". A hostname or bucket name stated only there is not in this section; "
            "record it at the data review with add_data_dependency")
    return YamlScan(list(referenced.values()), pairs, holders, notes, files)


def unify_chart_holders(scan: YamlScan, workloads: list) -> list:
    """Rewrites every `helm_chart` holder whose chart directory exactly one
    Terraform `helm_release` deploys (`source_path`) into that release — its
    name, kind and namespace — in place: the holders list, the pairs' copies
    and the consumers on the YAML-found entries alike. One workload, one row.
    Returns the scan notes it owes (a chart several releases deploy)."""
    notes = []
    by_path = {}
    for workload in workloads:
        if workload.get("kind") == "helm_release" and workload.get("source_path"):
            by_path.setdefault(workload["source_path"], []).append(workload)
    if not by_path:
        return notes
    # ONE release per chart directory, or none: two releases deploying
    # `charts/orders` (dev and prod) are two workloads reading one values
    # file, and picking the first by walk order handed the prod database
    # host to `orders-dev`. The chart stays the neutral holder, and the scan
    # says why.
    releases = {path: found[0] for path, found in by_path.items() if len(found) == 1}
    shared = {path: found for path, found in by_path.items() if len(found) > 1}
    if shared:
        notes.append(
            f"{len(shared)} chart director(y/ies) are deployed by more than one helm_release, "
            "so what their values state is attributed to the chart rather than to one release: "
            + "; ".join(f"{path} ({', '.join(sorted(w.get('workload') or '' for w in found))})"
                        for path, found in sorted(shared.items())))

    def unify(holder: dict) -> None:
        release = releases.get(holder.get("source_path")) if holder.get("kind") == CHART_KIND \
            else None
        if release is None:
            return
        holder["workload"] = release.get("workload")
        holder["kind"] = "helm_release"
        if release.get("namespace"):
            holder["namespace"] = release["namespace"]

    for holder in scan.holders:
        unify(holder)
    for holder, _key, _value, _path in scan.pairs:
        unify(holder)
    for entry in scan.referenced:
        for consumer in entry.get("consumers") or []:
            unify(consumer)
        # Two consumers may have become one.
        deduped = []
        for consumer in entry.get("consumers") or []:
            if consumer not in deduped:
                deduped.append(consumer)
        entry["consumers"] = deduped
    return notes


# --- guesses ----------------------------------------------------------------

def _known(entries: list) -> dict:
    """(service family, identifier) -> entry, over declared and referenced."""
    known = {}
    for entry in entries:
        if entry.get("detection") not in ("declared", "referenced"):
            continue
        if entry.get("identifier_is_fallback"):
            continue
        # Under every name the merge would match it by (`name_keys`): the
        # bare name of a secret recorded under its console form, an SSM
        # parameter with or without its leading slash. Keying on the exact
        # identifier alone produced a guess the merge then dropped as
        # answered — neither a hint nor a guess, and the holder's link gone.
        for key in datastores.name_keys(entry):
            known.setdefault(key, entry)
    return known


def guess_entries(pairs: list, entries: list) -> tuple:
    """(inferred entries, candidate hints, notes).

    `pairs` are (holder, key, value, rel_path) from both sides of the scan;
    `entries` are the declared and referenced entries already recorded, which
    a bare name must not duplicate. A name matching one of them yields a
    hint for the review's candidate list instead of an entry.
    """
    known = _known(entries)
    inferred = {}
    hints = []
    seen_hints = set()
    for holder, key, value, rel_path in pairs:
        typed = classify(key, value)
        if not typed:
            continue
        service, identifier = typed
        match = next((known[key] for key in datastores.name_keys(
            {"service": service, "identifier": identifier}) if key in known), None)
        if match is not None:
            hint_key = (match.get("address"), holder.get("workload"),
                        holder.get("kind"), holder.get("source_path"))
            if hint_key in seen_hints:
                continue
            seen_hints.add(hint_key)
            hints.append({
                "address": match.get("address"),
                "identifier": match.get("identifier"),
                "workload": holder.get("workload"),
                "kind": holder.get("kind"),
                "namespace": holder.get("namespace"),
                "source_path": holder.get("source_path"),
                "reason": (f"its configuration states the value '{identifier}' "
                           f"under the key {key} in {rel_path} — an exact match "
                           "on the name, offered as a candidate and not attached"),
            })
            continue
        address = inferred_address(service, identifier)
        entry = inferred.get(address)
        if entry is None:
            entry = {
                "service": service,
                "identifier": identifier,
                "address": address,
                "engine": None,
                "engine_version": None,
                "multi_az": None,
                "allocated_storage": None,
                "storage_type": None,
                "region": None,
                "account": None,
                "arn": None,
                "detection": "inferred",
                "declared_in_repo": False,
                "module_source": None,
                # Never a plan on its own: a guess gates nothing until a
                # human confirms it, and `undecided` is the disposition that
                # gates nothing.
                "disposition": "undecided",
                "identifier_is_fallback": False,
                "evidence": [],
                "consumers": [],
                "notes": [],
            }
            inferred[address] = entry
        if rel_path not in entry["evidence"]:
            entry["evidence"].append(rel_path)
        # The holder's own configuration states the value, whichever walk
        # found the holder — so the link is a config_value one even when the
        # holder came from a Terraform deploying block.
        consumer = dict(holder, detection=CONFIG_DETECTION, evidence=rel_path)
        if consumer not in entry["consumers"]:
            entry["consumers"].append(consumer)
        where = f"{key} in {rel_path}"
        wheres = entry.setdefault("_wheres", [])
        if where not in wheres:
            wheres.append(where)
        entry["notes"] = [
            f"{GUESS_NOTE_PREFIX}the key{'s' if len(wheres) > 1 else ''} "
            f"{', '.join(wheres)} hold{'s' if len(wheres) == 1 else ''} this "
            f"value and the key name says it is a {_what(service)}; nothing in "
            "the scanned files declares one by that name or names it by ARN or "
            "endpoint. It gates nothing until confirmed — confirm it "
            "(confirm_data_dependency) or dismiss it (dismiss_data_dependency) "
            "at the data review"]
    for entry in inferred.values():
        entry.pop("_wheres", None)
    notes = []
    if inferred:
        notes.append(
            f"{len(inferred)} data service(s) are guesses from a configuration "
            "key's name and value, recorded as 'inferred' with no disposition "
            "— each needs a yes or no at the data review: "
            + ", ".join(sorted(f"{e['service']} {e['identifier']}"
                               for e in inferred.values())[:5])
            + (f" and {len(inferred) - 5} more" if len(inferred) > 5 else ""))
    return list(inferred.values()), hints, notes


def _what(service: str) -> str:
    return {
        "s3": "bucket", "sqs": "queue", "sns": "topic", "kinesis": "stream",
        "dynamodb": "DynamoDB table", "secretsmanager": "Secrets Manager secret",
        "ssm": "SSM parameter",
    }.get(service, service)
