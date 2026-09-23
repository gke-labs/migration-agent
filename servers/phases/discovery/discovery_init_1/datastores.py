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

"""Managed data service extraction for discovery.

Pure logic — no GCS, no subprocesses, no LLM. Walks the source checkout for
Terraform declarations of managed data services (RDS, S3, DynamoDB, caches,
queues, streams) and records them into the inventory's `data_dependencies`.

This section is deterministic and scan-owned for the same reason `images` is:
the facts are exact strings, and the workload data gate holds a developer's
pipeline on them, which mixed scanner/model provenance could not support. Ownership is enforced by SCAN_OWNED_KEYS, which `carry_scan_sections`
restores over whatever an extraction re-run produced. (`data_dependencies` was
also dropped from INVENTORY_ANALYSIS_KEYS for consistency, but that set is
documentation only — nothing reads it.)

Both declaration forms are covered. Most real estates declare data services
through registry modules rather than raw resource blocks, and the module
publisher namespace is not a closed set (`cloudposse/elasticache-redis/aws`
sits beside `terraform-aws-modules/rds/aws`), so module detection matches the
`source` string by pattern rather than by publisher.

Which workload needs which of these is answered next door in `consumers.py`,
which `harvest_datastores` calls before merging.

A data service the estate reaches but does not declare is found by its ARN or
its endpoint: a literal `arn:aws:s3:::acme-invoice-archive` in an IAM policy,
an IRSA module's arguments or a Helm value, or an RDS hostname in a ConfigMap,
yields an entry with `detection: "referenced"` (see the literal sections
below). A bare name — a bucket name in an env var — cannot be typed without
guessing, so `inferred.py` records it as a guess for a human to answer rather
than as a fact; this section gates a pipeline.
"""

import copy
import os
import re
from typing import NamedTuple

from servers.phases.scope_algebra import is_excluded

from .files import SKIP_DIRS

# Terraform files only. Data services declared in CloudFormation, Crossplane or
# CDK are out of scope (no dialect support anywhere in the server), and an empty
# result must read as "did not look there" rather than "nothing there" — see
# the module note recorded by harvest_datastores.
SCAN_EXTENSIONS = (".tf",)

# A .tf file this large is generated or vendored, not hand-declared estate.
MAX_FILE_BYTES = 2 * 1024 * 1024

# Raw `resource "<type>"` blocks → normalized service name.
RESOURCE_SERVICES = {
    "aws_db_instance": "rds",
    # Deliberately not aws_rds_cluster_instance: an Aurora database is a
    # cluster plus its instances, so mapping both counts one database twice
    # — and the assessment prices Aurora at 8 days an entry.
    "aws_rds_cluster": "aurora",
    "aws_docdb_cluster": "docdb",
    "aws_neptune_cluster": "neptune",
    "aws_dynamodb_table": "dynamodb",
    "aws_elasticache_cluster": "elasticache",
    "aws_elasticache_replication_group": "elasticache",
    "aws_elasticache_serverless_cache": "elasticache",
    "aws_memorydb_cluster": "memorydb",
    "aws_s3_bucket": "s3",
    "aws_efs_file_system": "efs",
    "aws_fsx_lustre_file_system": "fsx",
    "aws_fsx_openzfs_file_system": "fsx",
    "aws_fsx_windows_file_system": "fsx",
    "aws_fsx_ontap_file_system": "fsx",
    "aws_kinesis_stream": "kinesis",
    "aws_kinesis_firehose_delivery_stream": "firehose",
    "aws_msk_cluster": "msk",
    "aws_msk_serverless_cluster": "msk",
    "aws_mq_broker": "mq",
    "aws_sqs_queue": "sqs",
    "aws_sns_topic": "sns",
    "aws_cloudwatch_event_bus": "eventbridge",
    "aws_opensearch_domain": "opensearch",
    "aws_elasticsearch_domain": "opensearch",
    "aws_redshift_cluster": "redshift",
    "aws_secretsmanager_secret": "secretsmanager",
    "aws_ssm_parameter": "ssm",
}

# Module `source` substrings → normalized service name, checked in order.
# Order is load-bearing: "rds-aurora" and "aurora" must both be tried before
# "rds", or `terraform-aws-modules/rds-aurora/aws` files as plain RDS.
MODULE_PATTERNS = (
    ("rds-aurora", "aurora"),
    ("aurora", "aurora"),
    ("documentdb", "docdb"),
    ("docdb", "docdb"),
    ("neptune", "neptune"),
    ("dynamodb", "dynamodb"),
    ("memorydb", "memorydb"),
    ("elasticache", "elasticache"),
    ("opensearch", "opensearch"),
    ("elasticsearch", "opensearch"),
    ("redshift", "redshift"),
    ("kinesis-firehose", "firehose"),
    ("firehose", "firehose"),
    ("kinesis", "kinesis"),
    ("msk-kafka", "msk"),
    ("msk", "msk"),
    ("secrets-manager", "secretsmanager"),
    ("secretsmanager", "secretsmanager"),
    ("eventbridge", "eventbridge"),
    ("rds", "rds"),
    ("efs", "efs"),
    ("fsx", "fsx"),
    ("sqs", "sqs"),
    ("sns", "sns"),
    # "s3" last: two characters, so it is the pattern most likely to collide.
    # Matching is boundary-aware (_module_pattern_matches), and a bucket that
    # looks like platform machinery is downgraded in _disposition_for rather
    # than dropped here.
    ("s3", "s3"),
)

# Argument names that carry the resource's own identity, per service, most
# specific first. Falls back to the Terraform block label, which always exists.
IDENTIFIER_ARGS = {
    # cluster_identifier is here because a Multi-AZ DB cluster refines from
    # aurora to rds and would otherwise lose the name its declaration states.
    "rds": ("identifier", "cluster_identifier", "name", "db_name"),
    "aurora": ("cluster_identifier", "name", "identifier"),
    "docdb": ("cluster_identifier", "name"),
    "neptune": ("cluster_identifier", "name"),
    "dynamodb": ("name", "table_name"),
    "elasticache": ("cluster_id", "replication_group_id", "name"),
    "memorydb": ("name", "cluster_name"),
    "s3": ("bucket", "bucket_prefix", "name"),
    "efs": ("creation_token", "name"),
    "fsx": ("name",),
    "kinesis": ("name", "stream_name"),
    "firehose": ("name",),
    "msk": ("cluster_name", "name"),
    "mq": ("broker_name", "name"),
    "sqs": ("name", "queue_name"),
    "sns": ("name", "topic_name"),
    "eventbridge": ("name", "bus_name"),
    "opensearch": ("domain_name", "name"),
    "redshift": ("cluster_identifier", "name"),
    "secretsmanager": ("name", "secret_name"),
    "ssm": ("name",),
}

# What the migration does with each service. Only "migrate" should gate a
# workload downstream: an empty cache is a working cache, and keeping a store
# on AWS is a legitimate choice the assessment is told to surface.
DISPOSITIONS = {
    "rds": "migrate",
    # Aurora has no exact GCP analogue — Aurora Postgres → AlloyDB is an engine
    # change and the assessment prices it at "8 days and quote", so it is an
    # escalation rather than a move we plan.
    "aurora": "escalate",
    "docdb": "escalate",
    "neptune": "escalate",
    "dynamodb": "escalate",
    # A cache can start cold; the warm-up cost is real but there is no data to
    # move. MemoryDB is deliberately NOT here — it is durable, and treating it
    # as rebuildable would silently discard data.
    "elasticache": "rebuild",
    "memorydb": "migrate",
    "s3": "migrate",
    "efs": "migrate",
    "fsx": "migrate",
    "kinesis": "replatform",
    "firehose": "replatform",
    "msk": "replatform",
    "mq": "replatform",
    "sqs": "replatform",
    "sns": "replatform",
    "eventbridge": "replatform",
    "opensearch": "replatform",
    "redshift": "escalate",
    # Secrets and config stores gate deliberately, and the instinct to exempt
    # them should be resisted. They look like plumbing for the databases beside
    # them — seven of retail-store-sample-app's thirteen entries are secrets
    # holding passwords for the other six — so it is tempting to record them
    # and let the workload through. But an application whose connection string
    # or password did not come across starts and immediately fails, and unlike
    # a cache there is nothing to rebuild it from. They move by hand rather
    # than by a generated runbook, which is what the gate is for: a human
    # resolves them before the component proceeds.
    "secretsmanager": "migrate",
    "ssm": "migrate",
}

# ---------------------------------------------------------------------------
# Referenced datastores: literal ARNs.
#
# A data service the estate reaches but does not declare — a bucket provisioned
# by another team's Terraform, a console-created database, a queue owned by a
# pipeline — leaves exactly one deterministic trace in the scanned files: its
# ARN, written literally into an IAM policy, an IRSA module's arguments, a Helm
# value or a ConfigMap. These are the dependencies that get forgotten, because
# nothing in the repository owns them. They are recorded with
# `detection: "referenced"`, and `merge_datastores` folds one onto the declared
# entry it names when the estate declares it too.
#
# Only a whole, literal ARN counts. A name built from a variable
# (`arn:aws:s3:::${var.bucket}`) is an expression and yields nothing but a
# note; a wildcard (`arn:aws:s3:::acme-*`) names a family, not a resource, and
# is noted rather than recorded — a policy granting `s3:*` on a prefix says the
# workload reaches some bucket, not which. Bare names — an env var holding
# `acme-invoice-archive` with no ARN around it — are never typed: `orders` is
# an RDS identifier and a namespace and a Helm release, and this section gates
# a pipeline.
# ---------------------------------------------------------------------------

# Where a literal ARN begins. Loose on purpose: the token is cut at the next
# delimiter and classified afterwards, so an ARN whose name is built from a
# variable is counted as one instead of silently half-matched.
# The partition is matched loosely, like everything else before the
# classification step: `arn:${data.aws_partition.current.partition}:s3:::b`
# is the partition-agnostic spelling every GovCloud- or China-capable module
# writes, and anchoring on a literal partition made it match nothing at all —
# no entry and, worse, no scan note either. `_ARN_PARTS_RE` still requires a
# real partition of anything that reaches `recorded`.
_ARN_START_RE = re.compile("\\barn:([^:\\s\"']+):([a-z0-9-]+):")
_ARN_PARTS_RE = re.compile(
    r"^arn:(aws|aws-cn|aws-us-gov):([a-z0-9-]+):([a-z0-9-]*|\*):(\d{12}|\*|):(.+)$")
# A backslash ends one too: an inline JSON policy string spells its quotes
# `\"`, and no AWS resource name contains one.
_ARN_DELIMITERS = " \t\r\n\"'`,])\\"
# The longest ARN AWS issues is a 2048-byte SSM parameter one; anything longer
# is not an ARN, and bounding the scan keeps it linear in the file.
_MAX_ARN_LENGTH = 2100
# Trailing prose punctuation: an ARN quoted at the end of a sentence in a
# heredoc's embedded YAML. No AWS resource name ends in these.
_ARN_TRAILING = ".;:"

_CLUSTER_ARN_NOTE = (
    "an RDS cluster ARN — Aurora, DocumentDB and Neptune share this namespace, "
    "and which engine it runs is not knowable from an ARN"
)
REFERENCED_NOTE = (
    "known only from a literal ARN or endpoint in the scanned files; no "
    "declaration there states this name, so it was either provisioned out of "
    "band — another repository, the console or a pipeline — or declared here "
    "under a name built from variables the files do not resolve. Its engine, "
    "size and multi-AZ setting are not knowable from these files"
)
# Notes that describe an entry BECAUSE it is referenced-only. Dropped when the
# entry folds onto a declared twin, whose declaration answers them.
_ASSIGNED_ID_NOTE = (
    "identified by an AWS-assigned id rather than a name, which a declaration "
    "never states — so if this estate declares this resource the two entries "
    "will not merge on their own: attach its consumer to the declared entry and "
    "annotate that they are one"
)
REFERENCED_NOTES = frozenset((REFERENCED_NOTE, _CLUSTER_ARN_NOTE, _ASSIGNED_ID_NOTE))


def _resolve_arn_resource(namespace: str, resource: str, region: str = "",
                          account: str = ""):
    """(service, identifier, canonical resource part, notes), or None.

    None means the ARN names something in a data service's namespace that is
    not a data store — an RDS subnet group, an S3 access point, an
    EventBridge rule — or a namespace this scan does not read at all. The
    canonical resource part is what two spellings of one resource collapse
    to: `bucket` and `bucket/*`, a table and its index, a topic and its
    subscription.
    """
    notes = []
    if namespace == "s3":
        # A bucket ARN never states a region or an account; every S3 ARN that
        # does names something else — an access point, a Storage Lens
        # configuration, a batch job, an access grant.
        if region or account:
            return None
        bucket = resource.split("/", 1)[0]
        return ("s3", bucket, bucket, notes) if bucket else None
    if namespace == "dynamodb":
        kind, _, rest = resource.partition("/")
        if kind != "table" or not rest:
            return None
        name = rest.split("/", 1)[0]
        return "dynamodb", name, f"table/{name}", notes
    if namespace == "rds":
        kind, _, name = resource.partition(":")
        if kind == "db" and name:
            return "rds", name, resource, notes
        if kind == "cluster" and name:
            return "aurora", name, resource, [_CLUSTER_ARN_NOTE]
        return None
    if namespace == "docdb-elastic":
        kind, _, rest = resource.partition("/")
        if kind != "cluster" or not rest:
            return None
        return "docdb", rest.split("/", 1)[0], resource, [_ASSIGNED_ID_NOTE]
    if namespace == "elasticache":
        kind, _, name = resource.partition(":")
        if kind in ("cluster", "replicationgroup", "serverlesscache") and name:
            return "elasticache", name, resource, notes
        return None
    if namespace == "memorydb":
        kind, _, name = resource.partition("/")
        return ("memorydb", name, resource, notes) if kind == "cluster" and name else None
    if namespace == "sqs":
        return ("sqs", resource, resource, notes) if resource else None
    if namespace == "sns":
        topic = resource.split(":", 1)[0]
        return ("sns", topic, topic, notes) if topic else None
    if namespace == "kinesis":
        # `stream/<name>` — and `stream/<name>/consumer/<consumer>:<ts>`, an
        # enhanced fan-out consumer, which is a dependency ON the stream, not
        # a second stream: kept whole it minted `clicks/consumer/app:158…`
        # as its own entry and attributed the Helm value to that instead.
        kind, _, rest = resource.partition("/")
        name = rest.split("/", 1)[0]
        return ("kinesis", name, f"stream/{name}", notes) if kind == "stream" and name else None
    if namespace == "firehose":
        kind, _, name = resource.partition("/")
        return (("firehose", name, resource, notes)
                if kind == "deliverystream" and name else None)
    if namespace == "kafka":
        # cluster/<name>/<uuid>; a topic, group or transactional id is
        # <kind>/<cluster name>/<uuid>/<...> and is a dependency on the cluster.
        parts = resource.split("/")
        if (len(parts) >= 3 and parts[1] and parts[2]
                and parts[0] in ("cluster", "topic", "group", "transactional-id")):
            # The UUID is dropped, as every other namespace drops its
            # sub-resource: `cluster/orders/*` is the spelling AWS's own IAM
            # documentation uses (the UUID is not known when the policy is
            # written), and keeping it would put a `*` in the entry's handle
            # and stop the two spellings composing.
            return "msk", parts[1], f"cluster/{parts[1]}", notes
        if len(parts) == 2 and parts[0] == "cluster" and parts[1]:
            # The canonical form this function itself returns, read back: an
            # entry's stored handle is `cluster/<name>` with the UUID already
            # dropped, and `_arn_name_keys` re-resolves it. Without this the
            # helper was blind to MSK — harmlessly, since an MSK identifier is
            # always the name inside its ARN, but by accident, not by rule.
            return "msk", parts[1], f"cluster/{parts[1]}", notes
        return None
    if namespace == "mq":
        kind, _, rest = resource.partition(":")
        if kind == "broker" and rest:
            # The broker id is dropped for the same reason the MSK UUID is.
            name = rest.split(":", 1)[0]
            return "mq", name, f"broker:{name}", notes
        return None
    if namespace == "events":
        kind, _, name = resource.partition("/")
        return ("eventbridge", name, resource, notes) if kind == "event-bus" and name else None
    if namespace == "es":
        kind, _, rest = resource.partition("/")
        if kind == "domain" and rest:
            name = rest.split("/", 1)[0]
            return "opensearch", name, f"domain/{name}", notes
        return None
    if namespace == "aoss":
        kind, _, name = resource.partition("/")
        return (("opensearch", name, resource, [_ASSIGNED_ID_NOTE])
                if kind == "collection" and name else None)
    if namespace == "redshift":
        kind, _, name = resource.partition(":")
        return ("redshift", name, resource, notes) if kind == "cluster" and name else None
    if namespace == "secretsmanager":
        kind, _, name = resource.partition(":")
        if kind != "secret" or not name:
            return None
        # `-??????` is AWS's own spelling for "this secret, whatever its
        # six-character suffix": exact, and stripped. `-*` is a prefix grant
        # like any other and stays a wildcard — `app-*` covers `app-db` and
        # `app-api` as readily as the suffix of `app`.
        if name.endswith("-??????"):
            name = name[:-len("-??????")]
        return ("secretsmanager", name, f"secret:{name}", notes) if name else None
    if namespace == "ssm":
        kind, _, rest = resource.partition("/")
        if kind != "parameter" or not rest:
            return None
        return "ssm", ("/" + rest if "/" in rest else rest), resource, notes
    if namespace == "elasticfilesystem":
        kind, _, name = resource.partition("/")
        return (("efs", name, resource, [_ASSIGNED_ID_NOTE])
                if kind == "file-system" and name else None)
    if namespace == "fsx":
        kind, _, name = resource.partition("/")
        return (("fsx", name, resource, [_ASSIGNED_ID_NOTE])
                if kind == "file-system" and name else None)
    return None


# The ARN namespaces `_resolve_arn_resource` reads. An ARN in any other
# namespace (iam, kms, ec2, logs, ...) is not a data store and is ignored
# without a note.
ARN_NAMESPACES = frozenset((
    "s3", "dynamodb", "rds", "docdb-elastic", "elasticache", "memorydb", "sqs",
    "sns", "kinesis", "firehose", "kafka", "mq", "events", "es", "aoss",
    "redshift", "secretsmanager", "ssm", "elasticfilesystem", "fsx",
))


class LiteralMatch(NamedTuple):
    """One literal ARN or endpoint found in a file, classified.

    `kind` is "recorded" for a whole literal ARN of a data store, "wildcard"
    when the resource name carries a wildcard, "expression" when it is built
    from a variable, "malformed" when it is ARN-shaped in a data service's
    namespace but not readable. Only "recorded" carries the remaining fields.
    `handle` is the entry's identity and its `address`: the normalised ARN,
    or the canonical endpoint — two spellings of one resource (a bucket and
    its object path, a writer and a reader endpoint) share one handle. `arn`
    is set only when the literal was an ARN.
    """
    kind: str
    token: str
    handle: str | None
    arn: str | None
    service: str | None
    identifier: str | None
    region: str | None
    account: str | None
    notes: list


def find_arns(text: str) -> list:
    """Every data-store ARN stated literally in `text`, classified.

    `text` should be the literal view of a file or block — string contents
    and heredoc bodies kept, comments blanked (`literal_text`). Two spellings
    of one resource yield the same `arn`.
    """
    found = []
    last_end = 0
    for match in _ARN_START_RE.finditer(text):
        if match.start() < last_end:
            # Inside the previous token: `arn:aws:s3:` repeated would
            # otherwise start a scan every eleven bytes, each to the end of
            # the file. One pass over the text, whatever it contains.
            continue
        namespace = match.group(2)
        if namespace not in ARN_NAMESPACES:
            continue
        end = match.start()
        limit = min(len(text), match.start() + _MAX_ARN_LENGTH)
        while end < limit and text[end] not in _ARN_DELIMITERS:
            end += 1
        last_end = end
        if end == limit and end < len(text) and text[end] not in _ARN_DELIMITERS:
            # Longer than any ARN can be: not one.
            found.append(LiteralMatch("malformed", text[match.start():match.start() + 64] + "…",
                                      None, None, None, None, None, None, []))
            continue
        token = text[match.start():end].rstrip(_ARN_TRAILING)
        if "$" in token or "{" in token or "}" in token:
            found.append(LiteralMatch("expression", token, None, None, None, None,
                                      None, None, []))
            continue
        parts = _ARN_PARTS_RE.match(token)
        if not parts:
            # ARN-shaped, in a data service's namespace, but not an ARN this
            # scan can read — an anonymised account, a missing field. Said
            # rather than dropped: a malformed ARN must not look like no ARN.
            found.append(LiteralMatch("malformed", token, None, None, None, None,
                                      None, None, []))
            continue
        partition, _namespace, region, account, resource = parts.groups()
        resolved = _resolve_arn_resource(namespace, resource,
                                         "" if region == "*" else region,
                                         "" if account == "*" else account)
        if resolved is None:
            continue
        service, identifier, canonical, notes = resolved
        if any(char in field for field in (identifier, canonical) for char in "*?"):
            # The canonical resource too, not the identifier alone: a
            # namespace whose canonical form keeps a sub-resource would
            # otherwise put a `*` into the entry's durable handle.
            found.append(LiteralMatch("wildcard", token, None, None, service,
                                      None, None, None, []))
            continue
        # `*` is "any", which is what an unstated field already means in an
        # ARN — and a bucket ARN states neither. Normalised out of the stored
        # handle so two spellings of one resource differ only where one of
        # them says something, and `_compose_arn` has no second spelling of
        # "unstated" to reconcile.
        arn = (f"arn:{partition}:{namespace}:{'' if region == '*' else region}:"
               f"{'' if account == '*' else account}:{canonical}")
        found.append(LiteralMatch(
            "recorded", token, arn, arn, service, identifier,
            region if region and region != "*" else None,
            # An S3 ARN never states one, and a `*` states nothing.
            account if account and account != "*" else None, notes))
    return found


# ---------------------------------------------------------------------------
# Endpoints. The other literal form a data service takes in an estate's files
# is its hostname or URL: an RDS endpoint in a ConfigMap, a queue URL in a
# Helm value, a cache endpoint in a Secret. These are as deterministic as an
# ARN — the AWS domain names the service, and the first label names the
# resource — and they are the form the ticket's "env hostnames" take. What
# they do NOT say is the account (except a queue URL), and for a cache the
# region is a short code this scan does not translate.
# ---------------------------------------------------------------------------

_AWS_REGION = r"[a-z]{2}(?:-[a-z]+)+-\d"
# (namespace, compiled pattern, builder). The pattern is matched against the
# whole token with its scheme stripped; the builder turns the groups into
# (service, identifier, canonical handle, region, account, notes).
_ENDPOINT_RULES = (
    ("rds", re.compile(
        r"^([a-z][a-z0-9-]*)\.(cluster(?:-ro)?-)?([a-z0-9]{12})\.(" + _AWS_REGION
        + r")\.rds\.amazonaws\.com(?::\d+)?$"),
     lambda m: (("aurora" if m.group(2) else "rds"), m.group(1),
                f"{m.group(1)}.{'cluster-' if m.group(2) else ''}{m.group(3)}."
                f"{m.group(4)}.rds.amazonaws.com",
                m.group(4), None, [_CLUSTER_ARN_NOTE] if m.group(2) else [])),
    ("docdb", re.compile(
        r"^([a-z][a-z0-9-]*)\.(?:cluster(?:-ro)?-)?([a-z0-9]{12})\.(" + _AWS_REGION
        + r")\.docdb\.amazonaws\.com(?::\d+)?$"),
     lambda m: ("docdb", m.group(1),
                f"{m.group(1)}.cluster-{m.group(2)}.{m.group(3)}.docdb.amazonaws.com",
                m.group(3), None, [])),
    ("neptune", re.compile(
        r"^([a-z][a-z0-9-]*)\.(?:cluster(?:-ro)?-)?([a-z0-9]{12})\.(" + _AWS_REGION
        + r")\.neptune\.amazonaws\.com(?::\d+)?$"),
     lambda m: ("neptune", m.group(1),
                f"{m.group(1)}.cluster-{m.group(2)}.{m.group(3)}.neptune.amazonaws.com",
                m.group(3), None, [])),
    ("elasticache", re.compile(
        # A cluster-mode node (`orders-0001-001.abc123.0001.use1…`) is the
        # group `orders`, not a second cache: the node suffix is dropped.
        r"^(?:(?:master|replica|clustercfg)\.)?([a-z][a-z0-9-]*?)(?:-\d{4}-\d{3})?"
        r"\.([a-z0-9]{6})\.(?:(?:ng\.)?\d{4}\.)?([a-z0-9]{4})\.cache\.amazonaws\.com(?::\d+)?$"),
     lambda m: ("elasticache", m.group(1),
                f"{m.group(1)}.{m.group(2)}.{m.group(3)}.cache.amazonaws.com",
                None, None, [])),
    ("elasticache", re.compile(
        r"^([a-z][a-z0-9-]*)\.serverless\.([a-z0-9]{4})\.cache\.amazonaws\.com(?::\d+)?$"),
     lambda m: ("elasticache", m.group(1),
                f"{m.group(1)}.serverless.{m.group(2)}.cache.amazonaws.com",
                None, None, [])),
    ("memorydb", re.compile(
        r"^clustercfg\.([a-z][a-z0-9-]*)\.([a-z0-9]{6})\.memorydb\.(" + _AWS_REGION
        + r")\.amazonaws\.com(?::\d+)?$"),
     lambda m: ("memorydb", m.group(1),
                f"clustercfg.{m.group(1)}.{m.group(2)}.memorydb.{m.group(3)}.amazonaws.com",
                m.group(3), None, [])),
    ("sqs", re.compile(
        r"^sqs\.(" + _AWS_REGION + r")\.amazonaws\.com/(\d{12})/"
        r"([A-Za-z0-9_-]{1,80}(?:\.fifo)?)/?$"),
     lambda m: ("sqs", m.group(3),
                f"https://sqs.{m.group(1)}.amazonaws.com/{m.group(2)}/{m.group(3)}",
                m.group(1), m.group(2), [])),
    ("s3", re.compile(
        # Virtual-host forms: `bucket.s3.amazonaws.com`, `bucket.s3.REGION…`,
        # `bucket.s3-REGION…`, and the website, accelerate and dualstack
        # subdomains (`bucket.s3-website-REGION…`, `bucket.s3-accelerate…`,
        # `bucket.s3.dualstack.REGION…`) — all one bucket.
        r"^([a-z0-9][a-z0-9.-]{1,61}[a-z0-9])\.s3(?:[.-](?:website[.-]|accelerate\.?|"
        r"dualstack\.)?(" + _AWS_REGION + r")?)?\.amazonaws\.com(?::\d+)?(?:/.*)?$"),
     lambda m: ("s3", m.group(1), f"s3://{m.group(1)}", m.group(2), None, [])),
    ("s3", re.compile(
        r"^s3(?:[.-](" + _AWS_REGION + r"))?\.amazonaws\.com/"
        r"([a-z0-9][a-z0-9.-]{1,61}[a-z0-9])(?:/.*)?$"),
     lambda m: ("s3", m.group(2), f"s3://{m.group(2)}", m.group(1), None, [])),
    ("s3", re.compile(r"^s3://([a-z0-9][a-z0-9.-]{1,61}[a-z0-9])(?:/.*)?$"),
     lambda m: ("s3", m.group(1), f"s3://{m.group(1)}", None, None, [])),
    ("opensearch", re.compile(
        r"^(?:search|vpc)-([a-z][a-z0-9-]{2,27})-[a-z0-9]{26}\.(" + _AWS_REGION
        + r")\.es\.amazonaws\.com(?:/.*)?$"),
     lambda m: ("opensearch", m.group(1),
                f"search-{m.group(1)}.{m.group(2)}.es.amazonaws.com",
                m.group(2), None, [])),
    ("msk", re.compile(
        r"^b-\d+\.([a-z0-9-]+)\.([a-z0-9]{6})\.c\d+\.kafka\.(" + _AWS_REGION
        + r")\.amazonaws\.com(?::\d+)?$"),
     lambda m: ("msk", m.group(1),
                f"{m.group(1)}.{m.group(2)}.kafka.{m.group(3)}.amazonaws.com",
                m.group(3), None, [])),
    ("mq", re.compile(
        r"^(b-[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})(?:-\d+)?"
        r"\.mq\.(" + _AWS_REGION + r")\.amazonaws\.com(?::\d+)?$"),
     lambda m: ("mq", m.group(1),
                f"{m.group(1)}.mq.{m.group(2)}.amazonaws.com", m.group(2), None,
                ["identified by its broker id; a broker endpoint does not carry "
                 "the broker's name"])),
    ("redshift", re.compile(
        r"^([a-z][a-z0-9-]*)\.([a-z0-9]{12})\.(" + _AWS_REGION
        + r")\.redshift\.amazonaws\.com(?::\d+)?$"),
     lambda m: ("redshift", m.group(1),
                f"{m.group(1)}.{m.group(2)}.{m.group(3)}.redshift.amazonaws.com",
                m.group(3), None, [])),
)
# Where an endpoint token may begin. Loose, like `_ARN_START_RE`: anything
# that mentions the AWS domain or the s3 scheme is cut into a token and
# classified afterwards, so an endpoint built from a variable is counted
# rather than half-matched.
_ENDPOINT_HINT_RE = re.compile(r"amazonaws\.com|\bs3[an]?://")
# Braces and `$` are NOT delimiters: `${var.db}.abc.us-east-1.rds.amazonaws.com`
# has to come out as one token so it can be classified as an expression.
_ENDPOINT_DELIMITERS = " \t\r\n\"'`,])[(=<>;\\"
# `postgres://`, `https://`, and the two-part `jdbc:postgresql://`.
_SCHEME_RE = re.compile(r"^[a-z][a-z0-9+.:-]*://")
# What a token has to mention for an interpolated one to be worth a scan note.
# `${var.account}.dkr.ecr.us-east-1.amazonaws.com/orders` is an image, not a
# data service, and counting it under "built from a variable" was noise.
_ENDPOINT_SERVICE_HINT_RE = re.compile(
    r"\.(?:rds|docdb|neptune|cache|redshift|es|kafka|mq)\.[^/]*amazonaws\.com"
    r"|memorydb\.[^/]*amazonaws\.com|(?:^|/)sqs\.[^/]*amazonaws\.com"
    r"|\.s3(?:[.-][^./]+)?\.amazonaws\.com|(?:^|/)s3(?:[.-][^./]+)?\.amazonaws\.com/"
    r"|s3://")


# Hosts in a data service's domain that are deliberately not recorded and not
# worth a note: an RDS Proxy fronts a database recorded from its own endpoint
# or declaration, and naming the proxy as a second database double-counts it.
_ENDPOINT_IGNORE_RE = re.compile(
    r"\.proxy-[a-z0-9]{12}\.[a-z0-9-]+\.rds\.amazonaws\.com"
    # A service principal (`monitoring.rds.amazonaws.com`, the trust policy
    # of every enhanced-monitoring role) has one label before the service; a
    # resource host has an id and a region there. And a wildcard host
    # (`*.s3.amazonaws.com` in a CSP header) names nothing.
    r"|(?:^|/)[a-z][a-z0-9-]*\.(?:rds|es|kafka|mq|redshift|docdb|neptune|cache|memorydb|sqs)"
    r"\.amazonaws\.com(?:$|[:/])"
    r"|(?:^|/)\*\.")
# Terraform's S3 getter (`s3::https://…/modules/vpc.zip`): a module archive
# the plan downloads, not data a workload reaches. Skipped whole.
_GETTER_PREFIX_RE = re.compile(r"^(?:s3|git|hg|gcs)::")


def _without_userinfo(token: str) -> str:
    """`token` from its host on: everything up to the LAST `@` that precedes
    the AWS domain is a URL's userinfo, however many `/`, `:` or `@` the
    password holds. Nothing after the domain is touched."""
    domain = token.find("amazonaws.com")
    if domain < 0:
        return token
    at = token.rfind("@", 0, domain)
    if at < 0:
        return token
    # Keep the scheme, drop the credentials: `postgres://` + host on.
    scheme = _SCHEME_RE.match(token)
    return (scheme.group(0) if scheme else "") + token[at + 1:]


def find_endpoints(text: str) -> list:
    """Every data-store endpoint or URL stated literally in `text`.

    A connection URL is read for its host: the scheme (`postgres://`,
    `jdbc:postgresql://`, `redis://`), any credentials and the path or
    database name are stripped, so `DATABASE_URL=postgres://u:p@orders.<id>
    .us-east-1.rds.amazonaws.com:5432/orders` records the same entry as
    `DB_HOST=orders.<id>.us-east-1.rds.amazonaws.com`.
    """
    found = []
    seen_at = set()
    for hint in _ENDPOINT_HINT_RE.finditer(text):
        start = hint.start()
        while start > 0 and text[start - 1] not in _ENDPOINT_DELIMITERS:
            start -= 1
        if start in seen_at:
            continue
        seen_at.add(start)
        end = hint.end()
        while end < len(text) and text[end] not in _ENDPOINT_DELIMITERS:
            end += 1
        token = text[start:end].rstrip(_ARN_TRAILING)
        if "${" not in token and "%{" not in token:
            # A YAML flow mapping inside a heredoc closes on the host:
            # `{host: orders.<id>…amazonaws.com}`. The brace is kept in the
            # token only so an interpolation can be recognised.
            token = token.rstrip("}])")
        if _GETTER_PREFIX_RE.match(token):
            continue
        if "s3.amazonaws.com/doc/2006-03-01" in token:
            # The S3 XML namespace, in every policy and CORS document ever
            # copied from the docs: not a bucket named `doc`.
            continue
        # What leaves this function as `token` — printed by the scan notes,
        # which are persisted — never carries a scheme's credentials or a
        # query string: `postgres://app:hunter2@…` is shown from the host on.
        # The userinfo ends at the LAST `@` before the AWS domain, because a
        # generated password routinely contains `/` and sometimes `@`
        # (`app:Zx9/qLm+w@…`, `app:p@ss@…`), and a stripper that stopped at
        # the first of either left the password in the token — and in the
        # note, which the ledger keeps.
        if token.startswith(("s3://", "s3a://", "s3n://")):
            # The scheme IS the hint for a bucket URL, so it stays; Hadoop's
            # `s3a://` and the older `s3n://` name the same bucket — and its
            # deprecated `s3a://KEY:SECRET@bucket` form carries credentials
            # before the bucket, stripped to the last `@` before the path.
            # A bucket name never holds an `@`; the secret before it may hold
            # `/`, so cut at the LAST `@` rather than the first `/`.
            shown = "s3://" + token.split("://", 1)[1].rpartition("@")[2]
        else:
            shown = _SCHEME_RE.sub("", _without_userinfo(token))
        # A query string is never part of the name, and `?password:…` in one
        # is a credential; cut before anything is printed or matched.
        shown = shown.split("?", 1)[0]
        # Judged on the host-onward form: `${DB_PASSWORD}` in the userinfo
        # is a literal host with a variable password, not a variable host.
        if "$" in shown or "{" in shown or "}" in shown:
            probe = re.sub(r"\$\{[^}]*\}", "x", shown)
            if _ENDPOINT_SERVICE_HINT_RE.search(probe):
                found.append(LiteralMatch("expression", shown, None, None, None, None,
                                          None, None, []))
            continue
        if shown.startswith("s3://"):
            candidates = [shown]
        else:
            bare = shown
            # The whole host-and-path first (a queue URL and an S3 URL carry
            # the name in the path), then the host alone for a connection
            # URL whose path is a database name.
            candidates = [bare]
            host = bare.split("/", 1)[0]
            if host != bare:
                candidates.append(host)
        hit = None
        for bare in candidates:
            hit = next(((m, build) for _ns, pattern, build in _ENDPOINT_RULES
                        if (m := pattern.match(bare))), None)
            if hit:
                break
        if not hit:
            if (_ENDPOINT_SERVICE_HINT_RE.search(shown)
                    and not _ENDPOINT_IGNORE_RE.search(shown)):
                # Names a data service's domain and still did not read — a
                # China-partition host (`amazonaws.com.cn`), a `KEY:host`
                # pair with no space, a shape this scan does not know. Said,
                # as an unreadable ARN is said, rather than dropped — from the
                # host on, never a fragment with an `@` in it.
                printed = shown.rpartition("@")[2]
                found.append(LiteralMatch(
                    "malformed", printed[:120] + ("…" if len(printed) > 120 else ""),
                    None, None, None, None, None, None, []))
            continue
        match, build = hit
        token = shown
        service, identifier, handle, region, account, notes = build(match)
        if "*" in identifier or "?" in identifier:
            found.append(LiteralMatch("wildcard", token, None, None, service,
                                      None, None, None, []))
            continue
        found.append(LiteralMatch("recorded", token, handle, None, service,
                                  identifier, region, account, list(notes)))
    return found


def find_literals(text: str) -> list:
    """ARNs and endpoints together — everything a file can state literally
    about a data store it does not declare."""
    return find_arns(text) + find_endpoints(text)


# A Terraform address as this scan writes one: `aws_db_instance.orders`,
# `module.orders_queue`, at most one index. Everything else an entry can be
# addressed by is a literal handle.
_BLOCK_ADDRESS_RE = re.compile(r"[a-z][a-z0-9_]*\.[A-Za-z0-9_-]+(?:\[[^\]]*\])?")


# What a hand-added entry's `evidence[0]` begins with (`overrides._entry_from`):
# the review, not a file. Read here so `consumers.note_unattributed` can tell
# such an entry from one the scan produced and no chain reached.
ADDED_EVIDENCE_PREFIX = "added at the data review"


def added_by_hand(entry: dict) -> bool:
    return str((entry.get("evidence") or [""])[0]).startswith(ADDED_EVIDENCE_PREFIX)


def inferred_handle(entry: dict):
    """The `<service>:<identifier>` handle an entry answers to besides its
    address: the address a guess or a hand-added entry carries, and the one a
    record made against such an entry names after a scan found the fact. None
    for an entry whose identifier is a Terraform address standing in for a
    name."""
    if entry.get("identifier_is_fallback") or not entry.get("service") \
            or not entry.get("identifier"):
        return None
    return f"{entry['service']}:{entry['identifier']}"


def is_inferred_handle(address) -> bool:
    """`<service>:<identifier>`, as opposed to an ARN, an endpoint or a block."""
    return (isinstance(address, str) and ":" in address and "://" not in address
            and not address.startswith("arn:") and is_literal_handle(address))


def is_literal_handle(address) -> bool:
    """Is this address a literal handle rather than a Terraform block address?

    A literal handle names one resource on its own — an ARN, a canonical
    endpoint (`orders.c9ak….us-east-1.rds.amazonaws.com`, `s3://bucket`, a
    queue URL), or the `<service>:<identifier>` handle a guess or a
    hand-added entry carries — so no directory travels with it and a
    correction keyed on it applies wherever the entry stands. A block address
    is unique only within a root module. Every reader that used to ask
    "does it start with `arn:`" asks this instead, because three of the four
    literal shapes do not.
    """
    if not isinstance(address, str) or not address:
        return False
    return _BLOCK_ADDRESS_RE.fullmatch(address) is None


# What an `aws` provider block says about the account the estate itself runs
# in: an `allowed_account_ids` list, or the account in an `assume_role`
# role ARN. `data "aws_caller_identity"` says nothing at plan time, so these
# two are the only literal statements of it the files can make.
_ALLOWED_ACCOUNTS_RE = re.compile(r"allowed_account_ids[ \t]*=[ \t]*\[([^\]]*)\]")
_ACCOUNT_ID_RE = re.compile(r'"(\d{12})"')
_AWS_REGION_PATTERN = r"[a-z]{2}(?:-[a-z]+)+-\d"
_ROLE_ARN_ACCOUNT_RE = re.compile(r"\barn:aws[a-z-]*:iam::(\d{12}):role/")

CROSS_ACCOUNT_NOTE_PREFIX = "in AWS account "
UNKNOWN_ACCOUNT_NOTE_PREFIX = "its ARN names AWS account "
AMBIGUOUS_ACCOUNT_NOTE_PREFIX = "named by ARNs in more than one AWS account, region or partition "


def _unknown_account_note(account: str, known: set = ()) -> str:
    return (
        f"{UNKNOWN_ACCOUNT_NOTE_PREFIX}{account}, and "
        + (f"the scanned Terraform states only {', '.join(sorted(known))} as its "
           "own, with at least one aws provider block silent about its account"
           if known else
           "the scanned Terraform states no account of its own (no "
           "allowed_account_ids or assume_role in an aws provider block)")
        + ", so whether that is this estate's account or another one is not "
        "knowable from the files — if this entry also folded onto a declaration, "
        "the ARN may name a same-named resource elsewhere"
    )


# A nested `replica { region = "eu-west-1" }` (Secrets Manager) or
# `replica { region_name = "eu-west-1" }` (DynamoDB global table): a region
# the estate declares THAT resource into, excusing its own ARN there from
# the cross-region verdict — and nothing else's.
_REPLICA_REGION_RE = re.compile(
    r'replica\s*\{[^{}]*?\bregion(?:_name)?\s*=\s*"([a-z]{2}(?:-gov)?-[a-z]+-\d)"', re.S)
_REPLICATED_TYPES = {"aws_secretsmanager_secret": "secretsmanager",
                     "aws_dynamodb_table": "dynamodb"}


def provider_regions(args: dict) -> tuple[set, bool]:
    """(the region an `aws` provider block states literally, whether it
    leaves its region unstated or stated with an expression).

    From the block's OWN arguments, as the lexer read them at depth zero —
    a `region` key inside `default_tags` is a tag, not the provider's
    region. Nearly every provider block states a region, and a declaration
    states none of its own — so this is the one fact the files almost
    always offer about where a declared resource lives, and an ARN in
    another region must not fold onto it.
    """
    value = args.get("region")
    if isinstance(value, str) and re.fullmatch(_AWS_REGION_PATTERN, value):
        return {value}, False
    return set(), True


def _partition_of(region: str) -> str:
    if region.startswith("cn-"):
        return "aws-cn"
    if region.startswith("us-gov-"):
        return "aws-us-gov"
    return "aws"


CROSS_REGION_NOTE_PREFIX = "in AWS region "
CROSS_PARTITION_NOTE_PREFIX = "in AWS partition "
DUPLICATE_NOTE_PREFIX = "possibly a duplicate: "
# Distinct from UNKNOWN_ACCOUNT_NOTE_PREFIX rather than a prefix of it: a
# `startswith` on one must never match the other.
UNKNOWN_REGION_NOTE_PREFIX = "its ARN places it in AWS "
# The estate's own replica in another region: answered like a foreign
# spelling (never folds, out of the ambiguity grouping), graded non-gating.
REPLICA_NOTE_PREFIX = "the replica in AWS region "


def provider_accounts(text: str, start: int, stop: int) -> tuple[set, bool]:
    """(the AWS account ids an `aws` provider block states literally, whether
    the block leaves its account unstated).

    The second value is the caveat. `provider "aws" { region = "us-east-1" }`
    — the default shape — names no account, and `assume_role { role_arn =
    var.deploy_role_arn }` names one the files cannot resolve. Either leaves
    the estate's account set incomplete, and an incomplete set beside one
    aliased provider that does state its account would turn every ARN in
    the estate's own account into a false cross-account verdict.
    """
    body = text[start:stop]
    accounts = set()
    unstated = False
    for match in _ALLOWED_ACCOUNTS_RE.finditer(body):
        elements = [e.strip() for e in match.group(1).split(",") if e.strip()]
        literal = _ACCOUNT_ID_RE.findall(match.group(1))
        accounts.update(literal)
        if len(literal) != len(elements):
            # `["111111111111", var.secondary_account_id]`: the list names an
            # account the files cannot resolve, and half a list is not a
            # complete set.
            unstated = True
    role_accounts = _ROLE_ARN_ACCOUNT_RE.findall(body)
    accounts.update(role_accounts)
    if re.search(r"\brole_arn\b", body) and not role_accounts:
        unstated = True
    return accounts, unstated or not accounts


def _cross_account_note(account: str, estate_accounts: set) -> str:
    return (
        f"{CROSS_ACCOUNT_NOTE_PREFIX}{account}, which no provider block in the "
        f"scanned Terraform names (the estate's own: "
        f"{', '.join(sorted(estate_accounts))}) — a cross-account dependency; the "
        "assessment grades a cross-account RDS without a replica path as its "
        "worst readiness band, and any cross-account store needs its owner's "
        "agreement before it can move"
    )


_HEREDOC_COMMENT_RE = re.compile(r"^[ \t]*#[^\n]*", re.MULTILINE)


def heredoc_body_without_comments(body: str) -> str:
    """A heredoc body with its `#` comment lines blanked, length preserved.

    The lexer does not enter its comment branch inside a heredoc — the body
    is data — but the data is YAML, a shell script or HCL-in-a-string often
    enough that a line opening with `#` is a comment there too, and a
    decommissioned bucket's ARN left in one must not become a live
    dependency. Only whole comment lines: a `#` mid-line is a fragment
    identifier in a URL as often as a comment.
    """
    return _HEREDOC_COMMENT_RE.sub(lambda m: " " * len(m.group(0)), body)


# Anchored to the line start OR to a `{` or `,` on the same line. A minified
# or generated policy writes the whole statement on one line —
# `[{ description = "we removed the grant on arn:… last year", Resource =
# "arn:…" }]` — and a line-anchored match reads neither the prose nor the
# note, so the retired bucket becomes a first-class dependency in silence.
# The lookbehind is one character wide, so `match.start()` still falls AFTER
# the delimiter and the delimiter still counts towards the depth walk.
_DESCRIPTION_ARG_RE = re.compile(
    r'(?:^[ \t]*|(?<=[{,])[ \t]*)description[ \t]*=', re.MULTILINE)
# The quoted spelling of the same key — `{ "description" = "…" }`, or the
# `"description": "…"` of a JSON policy written as an object — is invisible in
# the mask, where a string's contents are blank. Matched in the raw text and
# accepted only where the mask still shows the `=`/`:` that follows it: an
# `=` inside a string is blank there, so a key-shaped token inside prose
# cannot anchor a blanking.
_QUOTED_DESCRIPTION_ARG_RE = re.compile(
    r'(?:^[ \t]*|(?<=[{,])[ \t]*)"description"[ \t]*[=:]', re.MULTILINE)


def value_end(mask: str, start: int) -> int:
    """Where the value beginning at `start` ends: the first newline,
    depth-zero closer or depth-zero comma, counted in the lexer's mask.

    A description is as often `format("%s (e.g. arn:…)", …)` or a list
    wrapped by `terraform fmt` as it is one line, and stopping at the first
    newline would leave the rest of it live. Counted in the mask, so a
    bracket inside a quoted string is data and cannot move the depth — the
    same rule `_without_meta_arguments` follows on the consumer side.

    Three things end a value and all three are checked, because blanking
    past the end of one takes a sibling argument with it and the ARN in that
    sibling is a real grant. The newline is the ordinary case. A closer at
    depth zero belongs to the container the value sits in. A comma at depth
    zero is HCL's object-element separator — inside the value it would be
    within a delimiter and so at depth one or more — so
    `description = "orders read path", resources = ["arn:…"]` on one line
    ends at the comma.
    """
    depth = 0
    for i in range(start, len(mask)):
        char = mask[i]
        if char in "([{":
            depth += 1
        elif char in ")]}":
            if depth == 0:
                # Clamping here instead ran the walk on to the next
                # depth-zero newline and blanked a sibling —
                # `description = "..." }, { resources = [arn] }` on one line
                # took the grant with it.
                return i
            depth -= 1
        elif depth == 0 and char in ",\n":
            return i
    return len(mask)


def without_prose(text: str, mask: str, heredocs: list) -> tuple[str, list]:
    """(`text` with `description = ...` values blanked, the heredocs that
    are not descriptions). Length preserved.

    The literal scan reads the whole file so a `locals` block, a module
    argument or a variable's `default` is not missed, and the price of
    reading everything is reading prose: `description = "the orders table,
    e.g. arn:aws:dynamodb:us-east-1:123456789012:table/my-table"` would
    otherwise mint an escalation for a table that does not exist. Only the
    description argument is prose — a `default = "arn:aws:s3:::…"` is a
    dependency as real as any — and its value is followed to its end,
    whether that is the end of the line, the close of a call or list it
    opens, or the heredoc it opens. No block is blanked whole, so an
    unclosed block cannot switch the blanking off for the rest of the file.
    """
    out = list(text)
    kept = list(heredocs)
    # Matched in the MASK and sliced out of `text`, the two being the same
    # length. The old line-start anchor could not fire inside a quoted string
    # — a template cannot hold a real newline — but the `{`/`,` anchor can, so
    # `n = "prefix, description = see arn:…"` would have blanked the rest of a
    # string and dropped the ARN. In the mask a string's contents are already
    # blank, so no delimiter inside one can anchor anything. The consumer twin
    # already ran on its mask, and the two literal views have to agree about
    # what prose is.
    quoted = [m for m in _QUOTED_DESCRIPTION_ARG_RE.finditer(text)
              if mask[m.end() - 1] in "=:"]
    for match in sorted(list(_DESCRIPTION_ARG_RE.finditer(mask)) + quoted,
                        key=lambda m: m.start()):
        stop = value_end(mask, match.end())
        for i in range(match.start(), min(stop, len(out))):
            if out[i] != "\n":
                out[i] = " "
        if "<<" in text[match.start():stop]:
            # Every heredoc the value opens, not only one closing its first
            # line: `description = join("\n", [<<-EOT ... EOT])` opens one
            # inside the brackets, and a span left in the list is restored
            # from raw content by `literal_text`, undoing the blanking. A
            # body beginning at `stop + 1` is the last-token spelling.
            kept = [h for h in kept if not (match.start() < h[0] <= stop + 1)]
    return "".join(out), kept


def literal_text(content: str, text: str, heredocs: list) -> str:
    """`text` with heredoc bodies restored: every literal the file states.

    Comments stay blanked — Terraform comments by the lexer, `#` lines inside
    a heredoc by `heredoc_body_without_comments` — so a commented-out grant
    records nothing. Heredoc bodies otherwise come back whole, because an IAM
    policy document lives in one and its ARNs are data, not interpolations —
    the same choice `consumers.py` makes for IRSA subjects.
    """
    out = list(text)
    for start, stop in heredocs:
        out[start:stop] = list(heredoc_body_without_comments(content[start:stop]))
    return "".join(out)


def _referenced_entry(found: LiteralMatch, rel_path: str) -> dict:
    """One data_dependencies entry for a store nothing in the repo declares.

    `address` is the handle — the ARN, or the canonical endpoint. A
    referenced entry has no Terraform block, and the review's correction
    tools name an entry by its address — a reviewer who wants to attach a
    consumer to this bucket, or keep it in AWS, has to be able to say which
    one. The handle is the one stable, unique name it has.
    """
    notes = [REFERENCED_NOTE] + list(found.notes)
    disposition = _disposition_for(found.service, found.identifier, {}, None, notes)
    return {
        "service": found.service,
        "identifier": found.identifier,
        "address": found.handle,
        "engine": None,
        "engine_version": None,
        "multi_az": None,
        "allocated_storage": None,
        "storage_type": None,
        "region": found.region,
        "account": found.account,
        "arn": found.arn,
        # The canonical endpoint when that is what was seen. Kept when an ARN
        # sighting later takes over as the handle (`_FILL_FIELDS`), so a
        # correction or an outcome recorded under the URL still reaches the
        # entry — the same reason `arn` survives a fold onto a declaration.
        "endpoint": None if found.arn else found.handle,
        "console_form": None,
        "detection": "referenced",
        "declared_in_repo": False,
        "module_source": None,
        "disposition": disposition,
        "identifier_is_fallback": False,
        "evidence": [rel_path],
        "consumers": [],
        "notes": notes,
    }


def _listed(items, limit: int = 5) -> str:
    items = sorted(items)
    if len(items) <= limit:
        return ", ".join(items)
    return ", ".join(items[:limit]) + f" and {len(items) - limit} more"


# Arguments worth carrying onto the entry when the declaration states them
# literally. An expression (var.x, random_string.y.result) yields null plus a
# note — a null is never a guess.
_ENGINE_ARGS = ("engine",)
_ENGINE_VERSION_ARGS = ("engine_version",)
_MULTI_AZ_ARGS = ("multi_az",)
_STORAGE_ARGS = ("allocated_storage",)
_STORAGE_TYPE_ARGS = ("storage_type",)

_BLOCK_RE = re.compile(
    r'^[ \t]*([a-z_]+)[ \t]+"([^"]+)"(?:[ \t]+"([^"]+)")?[ \t]*\{',
    re.MULTILINE,
)
# Block kinds this module cares about. `iter_terraform_blocks` filters on the
# caller's behalf rather than in the regex: consumers.py needs `output` and
# `data` too, and one pattern that matches every labelled block keeps the two
# callers reading the same grammar.
#
# Named rather than private because consumers.py asks the same question of the
# same file — "would the datastore walk have seen this?" — to decide whether a
# truncation it hit is one this run has already reported.
DATASTORE_KINDS = ("resource", "module")
_ASSIGN_RE = re.compile(r'^[ \t]*([A-Za-z_][A-Za-z0-9_-]*)[ \t]*=[ \t]*(.+?)[ \t]*$')
_STRING_RE = re.compile(r'^"((?:[^"\\]|\\.)*)"$')
_NUMBER_RE = re.compile(r'^-?\d+(?:\.\d+)?$')
_HEREDOC_RE = re.compile(r"<<-?[ \t]*([A-Za-z_][A-Za-z0-9_]*)[ \t]*(?=\r?\n)")


def _literal(raw: str):
    """The literal value of a Terraform RHS, or None if it is an expression.

    Interpolated strings and template directives count as expressions — a
    half-resolved name is worse than an honest null. Comments are already gone
    by the time this runs (see `scan_source`), so there is nothing to strip: an
    earlier version stripped at `//` here and silently blanked every
    `git::https://` and registry-subpath module source.
    """
    raw = raw.strip().rstrip(",")
    match = _STRING_RE.match(raw)
    if match:
        value = match.group(1)
        # $${ and %%{ are HCL's escapes for a literal ${ and %{, so a value
        # containing only those is still a literal. Blank them before looking
        # for a real interpolation, then restore.
        probe = value.replace("$${", "").replace("%%{", "")
        if "${" in probe or "%{" in probe:
            return None
        value = value.replace('\\"', '"').replace("\\\\", "\\")
        return value.replace("$${", "${").replace("%%{", "%{")
    if _NUMBER_RE.match(raw):
        return float(raw) if "." in raw else int(raw)
    if raw in ("true", "false"):
        return raw == "true"
    return None


# Openings of the lexer notes that mean part of the file was NOT READ, as
# opposed to read differently. A caller that draws a conclusion from the
# absence of something has to tell those apart: after one of these, "nothing
# in this file references X" is unknowable rather than false. The unclosed
# block comment is deliberately absent — it over-reads rather than skipping,
# so the tail is still scanned.
CONTENT_SKIPPED_NOTE_PREFIXES = (
    "a heredoc opened with",
    "a quoted string or interpolation is never closed",
)


def content_was_skipped(notes: list) -> bool:
    """True when a lexer note says part of the file went unread."""
    return any(note.startswith(CONTENT_SKIPPED_NOTE_PREFIXES) for note in notes)


def interpolation_spans(content: str, start: int, stop: int) -> list:
    """`${...}` spans inside a heredoc body, as (start, stop) into `content`.

    A heredoc body is data — a policy document, a YAML fragment, a script —
    and the only live Terraform in it is an interpolation. Handing back the
    whole body instead would make a `#` comment in an embedded YAML document,
    or a plain-text mention in a description, read as a live reference.

    Both `${...}` and `%{...}` count: a values heredoc wrapping a reference in
    `%{ if var.enabled }` is still referencing it. `$${` and `%%{` are HCL's
    escapes for the literal forms and open nothing. Quoted strings inside the
    interpolation are skipped so a `}` in a string does not close it early.
    """
    spans = []
    i = start
    while i < stop:
        if content.startswith("$${", i) or content.startswith("%%{", i):
            i += 3
            continue
        if not (content.startswith("${", i) or content.startswith("%{", i)):
            i += 1
            continue
        depth, j, quote = 0, i + 1, None
        while j < stop:
            ch = content[j]
            if quote:
                if ch == "\\":
                    j += 2
                    continue
                if ch == quote:
                    quote = None
            elif ch in "\"'":
                quote = ch
            elif ch == "{":
                depth += 1
            elif ch == "}":
                depth -= 1
                if depth == 0:
                    spans.append((i, j + 1))
                    break
            j += 1
        else:
            # Unterminated: the rest of the body is the reference's best
            # available extent. Recording it keeps under-detection from
            # winning, and the caller only reads addresses out of it.
            spans.append((i, stop))
            break
        i = j + 1
    return spans


def scan_source(content: str) -> tuple[str, str, list, list]:
    """Classifies every byte once. Returns (text, mask, notes, heredocs).

    `text` keeps string contents and blanks comments and heredoc bodies — it is
    what values are read from. `mask` blanks strings as well, leaving only
    structural characters — it is what braces are counted in. `heredocs` is the
    (start, stop) span of each heredoc *body*, which both other views blank.

    A heredoc body is data — an IAM policy document, a YAML fragment — but two
    different things in it matter to `consumers.py`, and they need different
    treatment. A literal string like an IRSA subject is data and is read from
    the body as-is. A `${...}` is live Terraform; `interpolation_spans` narrows
    a body span to just those, so a reference is not picked up from surrounding
    prose. Both live here rather than in the caller, because this is the one
    function that knows where a heredoc begins — letting a second module
    re-derive it is the defect this one exists to prevent.

    Two outputs from one pass, because the alternative is what this file kept
    doing: four functions each deciding for itself where a string ends, and a
    fix to one of them setting up the next defect in another. A quoted template
    can hold `${...}`, which can hold another quoted string, which can hold
    `\"`; a heredoc body can hold `/*`, `*/`, `#` and `<<TAG`; `$${` is a
    literal `${`. Every consumer below now reads one answer instead of
    re-deriving its own.
    """
    text, mask = list(content), list(content)
    n = len(content)
    notes = []

    def blank(start: int, stop: int, in_text: bool) -> None:
        for j in range(start, min(stop, n)):
            if content[j] != "\n":
                mask[j] = " "
                if in_text:
                    text[j] = " "

    def line_end(at: int) -> int:
        found = content.find("\n", at)
        return n if found == -1 else found

    # End of the line on which the outermost quoted template opened. Past it,
    # a string is blanked in the text as well as the mask.
    #
    # `_top_level_args` and `_declared_args` read names from the text and take
    # depth from the mask, so the two have to agree about which lines are
    # structural. A template spanning lines — the TF-0.11 `"${ ... }"` wrapper
    # around a multi-line object — breaks that: the mask blanks its braces so
    # depth never rises, while the text still shows the inner `key = value`
    # lines, which are then read as the block's own arguments. That silently
    # turns an inner `replicate_source_db` into the enclosing database being
    # deleted as a replica, or an inner `source` into a queue graded as an RDS
    # instance. The opening line stays live so the assignment is still seen and
    # `_literal` still returns None for it; only the continuation is blanked,
    # and a multi-line value can never be a literal anyway.
    text_live_until = n
    i, stack, heredoc = 0, [], None
    heredocs, heredoc_start = [], 0
    while i < n:
        ch = content[i]
        in_text = bool(stack) and i > text_live_until

        if heredoc is not None:
            stop = line_end(i)
            if content[i:stop].strip() == heredoc:
                heredoc = None
                heredocs.append((heredoc_start, i))
            else:
                blank(i, stop, True)
            i = stop + 1

        elif stack and stack[-1] == "template":
            if ch == "\\":
                blank(i, i + 2, in_text)
                i += 2
            elif content.startswith("$${", i) or content.startswith("%%{", i):
                # HCL's escape for a literal ${ or %{ — not an interpolation.
                blank(i, i + 3, in_text)
                i += 3
            elif content.startswith("${", i) or content.startswith("%{", i):
                blank(i, i + 2, in_text)
                stack.append(0)
                i += 2
            elif ch == '"':
                blank(i, i + 1, in_text)
                stack.pop()
                if not stack:
                    text_live_until = n
                i += 1
            else:
                blank(i, i + 1, in_text)
                i += 1

        elif stack:                                   # inside ${ ... }
            if ch == '"':
                stack.append("template")
            elif ch == "{":
                stack[-1] += 1
            elif ch == "}":
                if stack[-1] == 0:
                    stack.pop()
                else:
                    stack[-1] -= 1
            blank(i, i + 1, in_text)
            i += 1

        elif ch == '"':
            blank(i, i + 1, False)
            stack.append("template")
            text_live_until = line_end(i)
            i += 1

        elif ch == "#" or content.startswith("//", i):
            stop = line_end(i)
            blank(i, stop, True)
            i = stop

        elif content.startswith("/*", i):
            close = content.find("*/", i + 2)
            if close == -1:
                notes.append(
                    "a block comment is never closed, so everything below it was "
                    "read as live Terraform — entries from that point on may not "
                    "exist")
                break
            blank(i, close + 2, True)
            i = close + 2

        else:
            match = _HEREDOC_RE.match(content, i) if ch == "<" else None
            if match:
                heredoc = match.group(1)
                stop = line_end(i)
                if stop == n:
                    break
                blank(i, stop, False)
                i = stop + 1
                heredoc_start = i
            else:
                i += 1

    if heredoc is not None:
        notes.append(
            f"a heredoc opened with <<{heredoc} is never closed, so the rest of "
            "that file was skipped — anything declared below it is missing")
        # Recorded to end of file anyway, though no consumer of the list can
        # currently see it: a block holding an unterminated heredoc never
        # closes, so `iter_terraform_blocks` reports truncation and drops it,
        # and no block that IS yielded overlaps this span. It is recorded
        # because the alternative — leaving `heredoc_start` set with nothing
        # written — is the shape two reviewers read as a stale span reaching
        # back over the file, and the span being in the list is what
        # `test_a_heredoc_opened_on_the_last_line_records_no_stale_span`
        # inspects to show it does not. The note is what the reader acts on.
        heredocs.append((heredoc_start, n))
    if stack:
        notes.append(
            "a quoted string or interpolation is never closed, so the rest of "
            "that file was read as string content and not scanned at all — "
            "anything declared below that point is missing entirely")
    return "".join(text), "".join(mask), notes, heredocs


def _block_body(mask: str, open_index: int) -> int:
    """Index of the brace closing the block whose `{` sits at open_index.

    Counts in the mask, so a brace inside a string, a comment or a heredoc is
    text rather than structure. Returns len(mask) for an unterminated block,
    which the caller reads as "the rest of the file".
    """
    depth = 0
    for i in range(open_index, len(mask)):
        if mask[i] == "{":
            depth += 1
        elif mask[i] == "}":
            depth -= 1
            if depth == 0:
                return i
    return len(mask)


def _declared_args(body_text: str, body_mask: str) -> dict:
    """Every argument assigned at the block's own level, name -> raw value.

    Kept separate from `_top_level_args` because presence is a fact even when
    the value is an expression: `replicate_source_db` almost always references
    the primary rather than naming it.
    """
    declared = {}
    depth = 0
    text_lines, mask_lines = body_text.split("\n"), body_mask.split("\n")
    for text_line, mask_line in zip(text_lines, mask_lines):
        if depth == 0:
            match = _ASSIGN_RE.match(text_line)
            if match and not text_line.strip().endswith("{"):
                declared.setdefault(match.group(1), match.group(2))
        depth = max(depth + mask_line.count("{") - mask_line.count("}"), 0)
    return declared


def _top_level_args(body_text: str, body_mask: str) -> dict:
    """Literal `key = value` pairs at the block's own level.

    Nested blocks are skipped by depth, counted in the mask, so an inner `name`
    is not mistaken for the resource's identity and an unbalanced brace inside
    a comment or a string does not shift every argument after it.
    """
    args = {}
    depth = 0
    text_lines, mask_lines = body_text.split("\n"), body_mask.split("\n")
    for text_line, mask_line in zip(text_lines, mask_lines):
        if depth == 0:
            match = _ASSIGN_RE.match(text_line)
            if match and not text_line.strip().endswith("{"):
                value = _literal(match.group(2))
                if value is not None:
                    args.setdefault(match.group(1), value)
        depth = max(depth + mask_line.count("{") - mask_line.count("}"), 0)
    return args


def iter_terraform_blocks(text: str, mask: str,
                          kinds: tuple = DATASTORE_KINDS) -> tuple[list, bool]:
    """Returns (blocks, truncated).

    Each block is (kind, type_or_source_label, name, literal args, raw declared
    args, body start, body stop). The two body offsets index into the same
    string `text` and `mask` were built from, so a caller that needs the raw
    body — heredocs and all — can slice the original content with them.
    `text` and `mask` come from one `scan_source` call: block headers and
    values are read from the text, the extent of each block is measured in the
    mask.

    `kinds` filters by block kind. The default is what this module declares an
    interest in; consumers.py passes a wider set. Each block's extent is
    measured from its own opening brace, so skipping an uninteresting one
    cannot shift the blocks after it.

    The filter runs BEFORE the extent is measured, which is not only a saving:
    it also means an unterminated block of an uninteresting kind never sets
    `truncated`. That is deliberate and load-bearing for consumers.py, which
    treats truncation as "part of this file went unread" — see its `_KINDS`
    comment.

    `truncated` is returned rather than signalled by a sentinel block, because
    "no top-level literal arguments" is an ordinary thing for a real block to
    have — a `resource` whose only argument is a nested `triggers = {` looks
    identical to an empty one, and treating that as truncation stopped the scan
    on well-formed files.
    """
    blocks, truncated = [], False
    for match in _BLOCK_RE.finditer(text):
        kind, first, second = match.group(1), match.group(2), match.group(3)
        if kind not in kinds:
            continue
        open_at = match.end() - 1
        close_at = _block_body(mask, open_at)
        if close_at == len(mask):
            # Unterminated. Every later block would rescan to end of file, so a
            # malformed file with many of them is quadratic — measured at ~20s
            # for 4000 blocks, and this runs inside the tool call. The file is
            # not parseable past here anyway.
            truncated = True
            break
        blocks.append((
            kind, first, second,
            _top_level_args(text[open_at + 1:close_at], mask[open_at + 1:close_at]),
            _declared_args(text[open_at + 1:close_at], mask[open_at + 1:close_at]),
            open_at + 1, close_at))
    return blocks, truncated


def service_for_resource_type(resource_type: str) -> str | None:
    """Normalized service for a raw `resource "<type>"` block, or None."""
    return RESOURCE_SERVICES.get(resource_type)


# Buckets that hold the migration's own machinery rather than the workloads'
# data: Terraform state, pipeline artifacts, access logs. Matched against a
# bucket's name and its Terraform label, so a raw `aws_s3_bucket` is judged the
# same way a module source is — the earlier guard only covered modules, which
# is the less common way a state or log bucket gets declared.
_INFRA_BUCKET_HINTS = ("tfstate", "terraform-state", "tf-state", "backend",
                       "artifact", "pipeline", "codepipeline", "buildlog",
                       "build-log", "access-log", "accesslog", "logs-bucket",
                       "cloudtrail", "elb-log",
                       # A Terraform module store on S3: tooling the plan
                       # reads, not data a workload reaches.
                       "helm-repo", "terraform-modules", "tf-modules")
# As WORDS of the name, not substrings: `acme-charts` and `helm-charts-prod`
# are chart repositories, `chartered-bank-statements` and `orgchart-data` are
# data.
_INFRA_BUCKET_WORD_RE = re.compile(r"(?:^|[-_.])(?:charts?|helm)(?:[-_.]|$)")


def _strip_getter_and_host(source: str | None) -> str | None:
    """Reduces a module source to the part that names the module.

    A `<getter>::` prefix says how to fetch it, a URL host says where it is
    kept, and a `?ref=` says which version — none of them describe what the
    module is. Without this, `git::https://artifactory.acme.com/tf/s3-bucket`
    reads as platform machinery (the `artifact` hint fires on the hostname),
    a branch called `feature/add-s3-backup` turns a VPC module into a bucket,
    and the path-style S3 getter puts the archive's bucket name in the path.

    The host is only removed when the source carries a `://`, so the two
    scheme-less forms keep theirs: a private registry address
    (`artifactory.acme.com/acme/s3-bucket/aws`) and an scp-style git source
    (`git@artifactory.acme.com:acme/s3-data.git`). Both still read as platform
    machinery. That is the same fallibility `_module_subpath` documents — the
    entry is recorded either way, carrying the note that says why — and the
    obvious fix (drop the leading dotted segment) mis-fires on `./modules/…`.

    Shared by `service_for_module_source` and `_disposition_for` deliberately:
    they are the only two readers of the raw source, and normalizing in one of
    them is how they end up disagreeing.
    """
    if not source:
        return source
    getter = ""
    if "::" in source:
        getter, source = source.split("::", 1)
    if "://" in source:
        rest = source.split("://", 1)[1]
        slash = rest.find("/")
        source = rest[slash:] if slash != -1 else ""
    for sep in ("?", "#"):
        marker = source.find(sep)
        if marker != -1:
            source = source[:marker]
    # An object-store getter points at a single archive, and in the path-style
    # URL form the bucket name sits in the path — `s3-eu-west-1.amazonaws.com/
    # rds-modules/vpc.zip` is a VPC module in a bucket called rds-modules. Only
    # the archive's own name describes it.
    if getter in ("s3", "gcs"):
        source = source.rsplit("/", 1)[-1]
    return source


def _module_subpath(source: str | None) -> str | None:
    """The part of a module source that names the module, not its home.

    `git::https://github.com/acme/pipeline-modules.git//s3-bucket` is a bucket
    module in a repository that happens to be called pipeline-modules; judging
    the whole string would condemn every S3 module the org owns. Only the
    subpath after `//` describes the module. Registry sources have no subpath,
    so they are judged whole — `org/s3-backend/aws` is a state-bucket module by
    its own name.
    """
    if not source:
        return None
    marker = source.rfind("//")
    # Skip a scheme's `//` (https://…), which is not a subpath separator.
    if marker > 0 and source[marker - 1] != ":":
        return source[marker + 2:]
    return source


def _looks_like_infrastructure_bucket(*candidates) -> bool:
    """True when any available signal says this bucket serves the platform
    rather than an application.

    Deliberately hint-based and therefore fallible in one direction only: a
    missed hint records a bucket that does not need migrating, which costs a
    reviewer a moment. A hint that fired wrongly would drop a bucket holding
    real customer data, so the list stays specific — "state" alone is not on
    it, because `realestate/s3-bucket/aws` and a bucket named `estate-docs`
    are both legitimate.
    """
    for candidate in candidates:
        if not candidate:
            continue
        lowered = str(candidate).lower()
        if any(hint in lowered for hint in _INFRA_BUCKET_HINTS):
            return True
        if _INFRA_BUCKET_WORD_RE.search(lowered):
            return True
    return False


def _module_pattern_matches(pattern: str, text: str) -> bool:
    """Whether `pattern` appears in `text` as a whole word.

    Bare substring matching is unsafe at these lengths: `rds` is inside
    `records`, `dashboards`, `standards` and `leaderboards`, so a route53
    records module was harvested as an unsized Postgres and priced at two
    days. Only a boundary makes three-character service names usable.
    """
    return re.search(r"(?<![a-z0-9])" + re.escape(pattern) + r"(?![a-z0-9])",
                     text) is not None


def service_for_module_source(source: str) -> str | None:
    """Normalized service for a module `source` string, or None.

    Matches by pattern rather than publisher: the namespace is open, and
    `cloudposse/elasticache-redis/aws` must resolve as readily as
    `terraform-aws-modules/elasticache/aws`.
    """
    if not source:
        return None
    source = _strip_getter_and_host(source)
    # The subpath names the module; the rest names where it lives, so the
    # subpath is tried first — an SQS module in a repository called
    # `rds-modules` is an SQS module. The fallback to the whole source is
    # still needed (`terraform-aws-modules/rds/aws//modules/db_instance` has
    # no service word in its subpath) and it can mislabel: a `vpc-endpoint`
    # submodule of an `rds-modules` repository reads as RDS. Under-detection
    # is the worse error here — a wrong service on a recorded entry is visible
    # to the platform engineer reading the runbook, a missing entry is not —
    # so the fallback stays and the mislabel is accepted. Note that nothing
    # marks an entry as having matched this way; the service alone is reported.
    subpath = _module_subpath(source)
    candidates = [subpath.lower()] if subpath and subpath != source else []
    candidates.append(source.lower())
    for candidate in candidates:
        for pattern, service in MODULE_PATTERNS:
            if _module_pattern_matches(pattern, candidate):
                return service
    return None


def _identifier(service: str, args: dict, address: str) -> tuple[str, bool, list]:
    """Best identity for the entry, plus whether it is a fallback and why.

    Real estates name most resources with an expression (`"${var.env}-catalog"`),
    so the fallback is the common path, not the exception. It uses the Terraform
    address (`module.catalog`, `aws_mq_broker.orders`) rather than a bare label:
    an address is meaningful to whoever has to go find the thing, and a bare
    label is often `this`, which several modules in one estate will share.
    """
    for arg in IDENTIFIER_ARGS.get(service, ()):
        value = args.get(arg)
        if isinstance(value, str) and value:
            return value, False, []
    return address, True, [
        f"identifier falls back to the Terraform address '{address}': the "
        "declaration names the resource with an expression, not a literal, so "
        "its real AWS name is not knowable from the files alone"
    ]


def _first(args: dict, names) -> object | None:
    for name in names:
        if name in args:
            return args[name]
    return None


# Engines that `aws_rds_cluster` carries when it is a Multi-AZ DB cluster
# rather than an Aurora one. Cloud SQL handles these natively, so grading them
# as Aurora would escalate a move the migration can actually plan.
_NON_AURORA_CLUSTER_ENGINES = ("mysql", "postgres", "postgresql")

# A reference to a database declared in this estate: `aws_db_instance.primary`,
# `aws_rds_cluster.main.id`, `module.db.identifier`. Anything else is either a
# primary opting out or a source outside the estate that nothing else counts.
_IN_ESTATE_REF_RE = re.compile(r"^(aws_(?:db_instance|rds_cluster)|module)\.[A-Za-z_]")


def _refine_service(service: str, resource_type: str | None, args: dict,
                    notes: list) -> str:
    """Corrects a service whose resource type is shared by two products.

    `aws_rds_cluster` is Aurora *and* the Multi-AZ DB cluster, distinguished
    only by `engine`. Left as Aurora, a plain MySQL cluster is escalated as
    having no clean GCP equivalent and priced at 8 days and a quote, when it
    is a 2-day homogeneous move to Cloud SQL. The schema already sets this
    precedent for DocumentDB and Neptune, which share the RDS ARN namespace
    and are told apart by the same field.
    """
    if resource_type == "aws_rds_cluster" and service == "aurora":
        engine = args.get("engine")
        if isinstance(engine, str) and engine.lower() in _NON_AURORA_CLUSTER_ENGINES:
            notes.append(
                f"graded as RDS rather than Aurora: this is a Multi-AZ DB "
                f"cluster (engine '{engine}'), which Cloud SQL supports natively"
            )
            return "rds"
    return service


def _is_read_replica(declared: dict) -> bool:
    """A database whose source is another database in the same estate.

    Recorded as its own entry it double-counts the primary — the symmetric
    case of the cluster-and-its-instances count, and equally invisible: two
    entries at 2 days each, and a runbook for a database nobody migrates.
    """
    raw = declared.get("replicate_source_db") or declared.get(
        "replication_source_identifier")
    if raw is None:
        return False
    raw = raw.strip().rstrip(",")
    literal = _literal(raw)
    if literal is not None:
        # An empty literal is the "no replication" sentinel every wrapper
        # module passes for a primary — `terraform-aws-modules/rds/aws`
        # defaulted `replicate_source_db` to "" before v5.
        if not literal:
            return False
        # A literal ARN names a primary in another account, so nothing in this
        # estate counts it. Record the entry rather than skipping it: the
        # assessment grades cross-account RDS without a replica path as the
        # worst readiness band, and skipping would delete the very case.
        return not literal.startswith("arn:")
    # An allowlist, not a deny list. Every round of "also exclude var., also
    # local., also try(" missed the next form — coalesce(), lookup(),
    # each.value — and each miss deletes a production database. Only a
    # reference to something this estate declares is evidence of a replica;
    # anything else (null, a variable, a function call, a data source, an
    # unrecognized expression) is read as a primary. A double-count is visible
    # in the estimate; a missing database is not.
    return bool(_IN_ESTATE_REF_RE.match(raw))


def _disposition_for(service: str, identifier: str, args: dict,
                     module_source: str | None, notes: list) -> str:
    """The table default, adjusted where the declaration itself disagrees.

    Two services cannot be judged from their type alone, and both mistakes are
    silent, so they are read from the arguments instead of assumed.
    """
    default = DISPOSITIONS.get(service, "undecided")

    # A Redis with snapshots configured is a datastore wearing a cache's name.
    # "An empty cache is a working cache" is only true when nothing persists,
    # and the declaration says which it is.
    if service == "elasticache":
        retention = args.get("snapshot_retention_limit")
        if isinstance(retention, (int, float)) and retention > 0:
            notes.append(
                f"graded as data to migrate, not a rebuildable cache: "
                f"snapshot_retention_limit is {int(retention)}, so this Redis "
                "persists"
            )
            return "migrate"
        if retention is None:
            notes.append(
                "graded as a rebuildable cache; snapshot_retention_limit is not "
                "stated literally, so persistence could not be ruled out from "
                "the files"
            )

    # A bucket holding Terraform state, pipeline artifacts or access logs is
    # the platform's own machinery. Recorded rather than dropped — a wrong
    # guess here should cost a reviewer a moment, not lose a bucket of
    # customer data — but it should not be planned as a migration.
    if service == "s3" and _looks_like_infrastructure_bucket(
            identifier, args.get("bucket"),
            _module_subpath(_strip_getter_and_host(module_source))):
        notes.append(
            "looks like platform machinery (Terraform state, pipeline "
            "artifacts or logs) rather than workload data — confirm before "
            "treating it as an application's dependency"
        )
        return "undecided"

    return default


def _entry(service: str, identifier: str, rel_path: str, args: dict,
           module_source: str | None, notes: list,
           identifier_is_fallback: bool = False, declared: dict = None,
           address: str | None = None) -> dict:
    """One data_dependencies entry, schema-shaped.

    `address` is the Terraform block address (`module.orders_rds`,
    `aws_mq_broker.mq`). It is what `identifier` falls back to when the files
    do not state a name, but it is persisted in its own right because it is
    the only stable join key: `consumers.py` resolves a reference chain to an
    address, and matching on `identifier` instead would fail exactly where the
    declaration is most informative — an RDS whose `identifier` came from
    `db_name` no longer looks like the block that declares it.
    """
    notes = list(notes)
    # One block can declare many resources. The scan records one entry either
    # way, so the count the assessment prices and the gate reads is wrong —
    # a for_each over a map of per-tenant databases is an ordinary pattern.
    for meta in ("count", "for_each"):
        if declared and meta in declared:
            notes.append(
                f"declared with {meta}, so this block may create several of "
                "these or none — recorded as a single entry, and the real "
                "number is not knowable from the files alone")
            break
    disposition = _disposition_for(service, identifier, args, module_source, notes)
    engine = _first(args, _ENGINE_ARGS)
    storage = _first(args, _STORAGE_ARGS)
    # An unsized relational store is a real gap for the downtime estimate, so
    # say why it is null rather than leaving the consumer to guess.
    if storage is None and service in ("rds", "docdb"):
        notes = notes + [
            "allocated_storage not stated literally; the downtime-window "
            "estimate has no size to work from"
        ]
    return {
        "service": service,
        "identifier": str(identifier),
        "address": address,
        "engine": engine if isinstance(engine, str) else None,
        "engine_version": _first(args, _ENGINE_VERSION_ARGS) if isinstance(
            _first(args, _ENGINE_VERSION_ARGS), str) else None,
        "multi_az": _first(args, _MULTI_AZ_ARGS) if isinstance(
            _first(args, _MULTI_AZ_ARGS), bool) else None,
        "allocated_storage": storage if isinstance(storage, (int, float)) else None,
        "storage_type": _first(args, _STORAGE_TYPE_ARGS) if isinstance(
            _first(args, _STORAGE_TYPE_ARGS), str) else None,
        "region": None,
        "account": None,
        "arn": None,
        "detection": "declared",
        "declared_in_repo": True,
        "module_source": module_source,
        "disposition": disposition,
        # True when `identifier` is a Terraform address standing in for a name
        # the files do not state. Consumers must not treat it as an AWS name,
        # and dedup scopes it to its directory (merge_datastores) — two modules
        # both labelled `this` are two resources, not one.
        "identifier_is_fallback": identifier_is_fallback,
        "console_form": None,
        "evidence": [rel_path],
        "consumers": [],
        "notes": notes,
    }


# Resource types the declared walk leaves out of RESOURCE_SERVICES on
# purpose, mapped to what they are counted through.
_COUNTED_THROUGH_A_PRIMARY = {
    "aws_rds_cluster_instance": "an Aurora cluster instance",
    "aws_docdb_cluster_instance": "a DocumentDB cluster instance",
    "aws_neptune_cluster_instance": "a Neptune cluster instance",
}


def _resource_kind(service: str, resource: str) -> str:
    """The kind an ARN's resource part names, where a parent and a child of
    one service share a namespace: `db` against `cluster` for RDS, `cluster`
    against `replicationgroup` for ElastiCache. Empty elsewhere, where the
    name alone is unambiguous.

    Without it the duplicate note below is the one place in this module that
    lets an instance identifier and a cluster identifier collide — the
    separate namespaces `_same_service` and `merge_datastores._key` both
    refuse to conflate.
    """
    if service in _RDS_FAMILY or service == "elasticache":
        head, separator, _rest = (resource or "").partition(":")
        return head if separator else ""
    return ""


def _entry_kind(entry: dict) -> str:
    """`_resource_kind` for an entry, from the ARN it was recorded under."""
    parts = (entry.get("arn") or "").split(":", 5)
    return _resource_kind(entry.get("service"), parts[5]) if len(parts) == 6 else ""


def _uncounted_key(service: str, kind: str, name: str) -> tuple:
    """One name for one kind of resource, under the RDS family's shared
    namespace: a module-declared Aurora replica records `rds` while the ARN
    that names it resolves to `aurora`."""
    return ("rds" if service in _RDS_FAMILY else service, kind, name)


def _remember_uncounted(seen: dict, service: str, args: dict, own_args: tuple,
                        kind: str, what: str) -> None:
    """Records the AWS name of a block counted through its primary, so a
    literal ARN naming the same thing can be flagged as possibly the same
    resource recorded twice.

    `own_args` are the arguments that name THIS block, and only those —
    never `IDENTIFIER_ARGS`, whose per-service list also holds the parent's
    name: a cluster instance states `cluster_identifier`, an ElastiCache
    member states `replication_group_id`, and falling back to either would
    suppress an ARN naming the parent. When none of them is literal nothing
    is recorded. A wrong match costs a note on an entry that says the two
    may be one thing, which a reviewer can dismiss; the parent's name would
    put that note on a resource that has nothing to do with the child.
    """
    for arg in own_args:
        value = args.get(arg)
        if isinstance(value, str) and value:
            seen.setdefault(_uncounted_key(service, kind, value), what)
            return


def extract_datastores(root_dir: str, scope: dict = None,
                       extra_referenced: list = ()) -> tuple[list, list, list, bool]:
    """Finds data services declared or referenced in Terraform under root_dir.

    `extra_referenced` are referenced entries another walk found — the YAML
    half's literals (`inferred.scan_yaml`) — joined to the Terraform ones
    BEFORE the account and region verdicts, so an endpoint in a ConfigMap
    manifest is judged by the same provider blocks as an ARN in a policy.

    `scope` is the confirmed discovery scope; excluded files are not read. An
    operator who removed a directory from discovery removed it from the
    migration, and this section is headed for the member-readable exports
    object, so an excluded file's metadata must not cross into it — the same
    rule the exports seed index already applies to the file manifest. A None
    scope scans everything, which is only correct before a scope exists.

    Returns (declared, referenced, notes, regions verified). Declared entries are in discovery
    order with duplicates preserved; merge_datastores dedupes so provenance
    unions. Referenced entries — data stores known only from a literal ARN —
    are one per ARN, their evidence already unioned, and each carrying its
    account notes. Notes record files that could not be read, which must
    stay visible — a scan that silently skipped half the estate looks
    identical to a clean one.
    """
    entries = []
    notes = []
    replicas = []
    members = []
    # AWS names of blocks the declared walk deliberately does not count on
    # their own — an ElastiCache group member, a read replica, an Aurora
    # cluster's instances. A literal ARN naming one of them is recorded like
    # any other and carries a note saying it may be that same resource; this
    # is what the note is keyed on.
    counted_through_a_primary = {}
    local_module_dirs = set()
    excluded_count = 0
    referenced = {}
    wildcard_arns, expression_arns, malformed_arns = set(), set(), set()
    estate_accounts = set()
    # Why the estate's account set may be incomplete: a file whose aws
    # provider does not state its account literally, a provider block that
    # was never closed, or any file the walk did not read to the end — an
    # excluded, oversized, unreadable or lexer-truncated file may hold the
    # provider that would have made an account the estate's own.
    accounts_incomplete = set()
    # ARNs a `description` stated, as {normalised ARN: the token as written}.
    # Whether one is prose-ONLY cannot be decided per file — the same ARN is
    # routinely documented in a `variable`'s description in one file and
    # granted for real in another — so the set is filtered against everything
    # the whole walk recorded, after the walk.
    prose_candidates = {}
    # The subset of those that are provider blocks silent about their
    # account (the rest were left unread); and the same pair for regions.
    # Files the walk did not read to the end, tracked on their own rather
    # than derived: a file can be both provider-silent and truncated.
    unread = set()
    estate_regions, regions_incomplete = set(), set()
    # (service, name) -> the regions a `replica {}` block declares THAT
    # resource into; excuses only its own ARN there (`_REPLICATED_TYPES`).
    replica_regions = {}
    for dirpath, dirnames, filenames in os.walk(root_dir):
        # Same pruning as the manifest indexer: a vendored module declares
        # infrastructure the user does not own, and the manifest the user
        # scoped against never showed those directories.
        dirnames[:] = sorted(d for d in dirnames if d not in SKIP_DIRS)
        for filename in sorted(filenames):
            if not filename.endswith(SCAN_EXTENSIONS):
                continue
            full_path = os.path.join(dirpath, filename)
            rel_path = os.path.relpath(full_path, root_dir)
            if scope and is_excluded(rel_path, scope):
                excluded_count += 1
                accounts_incomplete.add(rel_path)
                unread.add(rel_path)
                continue
            try:
                if os.path.getsize(full_path) > MAX_FILE_BYTES:
                    notes.append(f"{rel_path}: skipped, larger than {MAX_FILE_BYTES} bytes")
                    accounts_incomplete.add(rel_path)
                    unread.add(rel_path)
                    continue
                with open(full_path, "r", encoding="utf-8-sig", errors="replace") as f:
                    content = f.read()
            except OSError as e:
                notes.append(f"{rel_path}: unreadable ({e.__class__.__name__})")
                accounts_incomplete.add(rel_path)
                unread.add(rel_path)
                continue

            text, mask, lex_notes, heredocs = scan_source(content)
            for note in lex_notes:
                notes.append(f"{rel_path}: {note}")
            over_read = any(note.startswith("a block comment is never closed")
                            for note in lex_notes)
            if content_was_skipped(lex_notes) or over_read:
                accounts_incomplete.add(rel_path)
                unread.add(rel_path)
            # Literal ARNs, read from the whole file rather than block by
            # block: a grant lives in an IAM policy's heredoc, an IRSA
            # module's arguments, a Helm value or a `locals` block, and the
            # block-kind filter below would skip the last of those.
            # Prose is blanked BEFORE the heredocs are restored, and a
            # heredoc that a description opens is not restored at all.
            prose_free, live_heredocs = without_prose(text, mask, heredocs)
            if content_was_skipped(lex_notes):
                # An unterminated heredoc's span runs to end of file, and
                # restoring it would read the rest of the file from raw
                # content — resurrecting `//` comments and description prose
                # that the blanking passes never saw, in a file this scan has
                # just said it could not read to the end.
                live_heredocs = [h for h in live_heredocs if h[1] < len(content)]
            # Two passes at most, both hoisted out of any loop: the view is
            # rebuilt and re-scanned per file, never per ARN found in it. The
            # loop this was written inside made a 110 KB policy file take
            # twelve seconds and a 2 MB one — which MAX_FILE_BYTES allows —
            # take hours, inside one synchronous tool call.
            recorded = find_literals(literal_text(content, prose_free, live_heredocs))
            if prose_free != text:
                # A `description` key can hold a real ARN — a ConfigMap
                # `data` entry called `description`, say. Blanking prose is
                # still right (a description is prose wherever it sits, and
                # a wrong consumer is the worse error), but an ARN dropped
                # with nothing recorded is the silence the scan notes exist
                # to prevent, so what was skipped is named.
                kept = {f.handle for f in recorded}
                # `heredocs` minus the unread tail, NOT `live_heredocs`:
                # that list also has every heredoc a description opens
                # removed, and those hold exactly the ARNs this pass exists
                # to name. Only the to-EOF span of an unterminated heredoc
                # has to go, or live grants from a tail the file says was
                # never read would be reported as prose.
                readable = ([h for h in heredocs if h[1] < len(content)]
                            if content_was_skipped(lex_notes) else heredocs)
                for found in find_literals(literal_text(content, text, readable)):
                    if found.kind == "recorded" and found.handle not in kept:
                        prose_candidates.setdefault(found.handle, found.token)
            for found in recorded:
                if found.kind == "wildcard":
                    wildcard_arns.add(found.token)
                elif found.kind == "expression":
                    expression_arns.add(found.token)
                elif found.kind == "malformed":
                    malformed_arns.add(found.token)
                elif found.handle in referenced:
                    evidence = referenced[found.handle]["evidence"]
                    if rel_path not in evidence:
                        evidence.append(rel_path)
                else:
                    referenced[found.handle] = _referenced_entry(found, rel_path)
            # Provider blocks are read in their own pass, not by widening the
            # kinds below: an unclosed `provider` block would otherwise
            # truncate the datastore walk at a point it reads past today.
            # An unclosed block comment reads everything below it as live, so
            # a commented-out provider block would put a retired account into
            # the estate's set. Nothing from such a file joins the set.
            provider_blocks, provider_truncated = iter_terraform_blocks(
                text, mask, ("provider",)) if not over_read else ([], False)
            for kind, first, _second, _args, _declared, bs, be in provider_blocks:
                if first == "aws":
                    accounts, unstated = provider_accounts(text, bs, be)
                    estate_accounts |= accounts
                    if unstated:
                        accounts_incomplete.add(rel_path)
                    regions, region_unstated = provider_regions(_args)
                    estate_regions |= regions
                    if region_unstated:
                        regions_incomplete.add(rel_path)
            if not over_read:
                # A region this repository declares a resource INTO without a
                # provider there: a Secrets Manager `replica { region = … }`
                # or a DynamoDB `replica { region_name = … }` block. The ARN
                # of THAT replica is this estate's, and read against the
                # provider regions alone it was a cross-region dependency
                # that never folded and gated twice. Kept per resource, not
                # joined to the estate's set: one secret replicated to
                # eu-west-1 says nothing about a same-named queue there, and
                # widening the set folded that queue across the cross-region
                # rule. Literal names and regions only.
                for _kind, r_type, _label, r_args, _decl, r_bs, r_be in iter_terraform_blocks(
                        text, mask, ("resource",))[0]:
                    r_service = _REPLICATED_TYPES.get(r_type)
                    r_name = r_args.get("name")
                    if r_service and isinstance(r_name, str) and r_name:
                        replica_regions.setdefault((r_service, r_name), set()).update(
                            _REPLICA_REGION_RE.findall(text[r_bs:r_be]))
            if provider_truncated:
                accounts_incomplete.add(rel_path)
                unread.add(rel_path)
                notes.append(
                    f"{rel_path}: a provider block is never closed, so the "
                    "estate's own AWS account(s) may be incomplete; ARNs in "
                    "other accounts are noted as unknown rather than cross-account")
            blocks, truncated = iter_terraform_blocks(text, mask)
            if truncated:
                # A note, not `unread`: the provider pass has already read
                # this file whole, so the verdicts stand. What the unread
                # tail can hide is a declaration or a `replica {}` block, and
                # the safe direction for an ARN naming either is the one the
                # verdicts already take — cross-region, apart, gating —
                # rather than un-verdicting the whole estate.
                notes.append(
                    f"{rel_path}: a block is never closed, so that block and "
                    "everything after it were not read")
            for kind, first, second, args, declared, _bs, _be in blocks:
                if kind == "module":
                    raw_source = args.get("source")
                    if isinstance(raw_source, str) and raw_source.startswith((".", "/")):
                        # A local module definition lives at this path. What is
                        # declared inside it is a template instantiated N times
                        # from elsewhere, not N declarations — and not one
                        # either.
                        resolved = os.path.normpath(os.path.join(
                            os.path.dirname(rel_path), raw_source))
                        # "." means the module is the whole checkout, which
                        # marks everything and says nothing.
                        if resolved not in (".", ""):
                            local_module_dirs.add(resolved)
                resource_type = None
                if kind == "resource":
                    resource_type = first
                    service = service_for_resource_type(first)
                    module_source = None
                    address = f"{first}.{second}" if second else first
                else:
                    module_source = args.get("source")
                    service = service_for_module_source(module_source)
                    address = f"module.{first}"
                if not service:
                    if resource_type in _COUNTED_THROUGH_A_PRIMARY:
                        # `identifier` alone: `cluster_identifier` on one of
                        # these is the cluster it belongs to, which is the
                        # entry it is counted through.
                        # A cluster instance's own ARN is `db:<identifier>`,
                        # whatever engine the cluster runs.
                        _remember_uncounted(
                            counted_through_a_primary, "rds", args, ("identifier",),
                            "db", _COUNTED_THROUGH_A_PRIMARY[resource_type])
                    continue
                if service == "elasticache" and "replication_group_id" in declared \
                        and resource_type == "aws_elasticache_cluster":
                    # A member of a group already counted through the group
                    # itself — the same one-thing-one-entry rule as an Aurora
                    # cluster and its instances.
                    members.append(address)
                    # `cluster_id` alone: `replication_group_id` is the
                    # group's name, and the group is a recorded entry.
                    _remember_uncounted(counted_through_a_primary, service, args,
                                        ("cluster_id",), "cluster",
                                        "an ElastiCache replication group member")
                    continue
                if _is_read_replica(declared):
                    # Counted through its primary, which is in this estate by
                    # definition. Noted at scan level so the omission is visible.
                    replicas.append(address)
                    # A replica names ITSELF with these; its source is in
                    # `replicate_source_db` / `replication_source_identifier`.
                    # From the SERVICE, not the resource type: a module
                    # block has no resource type, and
                    # `terraform-aws-modules/rds/aws` — the plain-RDS module,
                    # whose documented replica input is
                    # `replicate_source_db` — declares an instance, whose ARN
                    # is `db:`. `_refine_service` has not run yet, so `rds`
                    # is an instance and every cluster service is a cluster,
                    # for the resource and the module spelling alike.
                    # `identifier` for an instance — `name` there is the
                    # deprecated DATABASE name, not the instance's. For a
                    # cluster, `cluster_identifier` (the resource) or `name`
                    # (`terraform-aws-modules/rds-aurora/aws`, which sets
                    # `cluster_identifier = var.name` internally).
                    _remember_uncounted(
                        counted_through_a_primary, service, args,
                        ("identifier",) if service == "rds"
                        else ("cluster_identifier", "name"),
                        "db" if service == "rds" else "cluster",
                        "a read replica")
                    continue
                id_notes = []
                service = _refine_service(service, resource_type, args, id_notes)
                identifier, is_fallback, fallback_notes = _identifier(
                    service, args, address)
                entry = _entry(service, identifier, rel_path, args, module_source,
                               id_notes + fallback_notes, is_fallback, declared,
                               address)
                if (service == "rds" and not is_fallback
                        and identifier not in (args.get("identifier"),
                                               args.get("cluster_identifier"))
                        and identifier in (args.get("db_name"), args.get("name"))):
                    # An INSTANCE whose identifier is the DATABASE name, taken
                    # because the instance identifier is an expression
                    # (`name` on `aws_db_instance` is the deprecated spelling
                    # of `db_name`). A `db:` ARN carries the instance
                    # identifier and never the database name, so this entry
                    # can match no ARN on its own — matched as equals,
                    # `db:orders` folded onto `prod-orders-primary`, and the
                    # instance's own ARN stood as a second gating database.
                    # Said, and kept apart. `rds` only: for Aurora, DocumentDB
                    # and Neptune `name` IS the cluster identifier
                    # (`rds-aurora` sets `cluster_identifier = var.name`), and
                    # marking those refused the fold the sample estate relies
                    # on and put a false note on every cluster.
                    entry["identifier_is_db_name"] = True
                    states_one = any(args.get(a) is not None
                                     for a in ("identifier", "cluster_identifier"))
                    entry["notes"].append(
                        f"identifier is the database name '{identifier}', not the "
                        "instance identifier, which "
                        + ("the declaration builds from an expression" if states_one
                           else "this block does not state — an override file that "
                                "re-declares only `db_name`, or a module argument")
                        + "; an ARN naming the instance will not merge with this "
                        "entry on its own — attach and annotate at the review if "
                        "one turns up")
                entries.append(entry)
    outside = [pattern for pattern in (scope or {}).get("included", [])
               if os.path.isabs(pattern) or pattern.startswith("..")]
    if outside:
        notes.append(
            f"{len(outside)} scope include(s) point outside the scanned root and "
            "were not searched for data services, though extraction does read "
            "them: " + ", ".join(sorted(outside))
        )
    if local_module_dirs:
        template_entries = []
        for entry in entries:
            evidence = (entry.get("evidence") or [""])[0]
            if any(evidence == d or evidence.startswith(d + os.sep)
                   for d in local_module_dirs):
                entry["notes"].append(
                    "declared inside a local module definition, so this is a "
                    "template rather than a deployment — the estate may have "
                    "several of these, or none, depending on how many times the "
                    "module is instantiated")
                template_entries.append(entry["identifier"])
        if template_entries:
            notes.append(
                f"{len(template_entries)} entr(y/ies) come from local module "
                "definitions rather than from instantiations, so the count is "
                "not the number of real resources: "
                + ", ".join(sorted(template_entries))
            )
    if members:
        notes.append(
            f"{len(members)} ElastiCache member cluster(s) were seen and not "
            "counted separately — they belong to a replication group already "
            "recorded: " + ", ".join(sorted(members))
        )
    if replicas:
        notes.append(
            f"{len(replicas)} read replica(s) were seen and not counted as "
            "separate dependencies — they migrate with their primary: "
            + ", ".join(sorted(replicas))
        )
    if excluded_count:
        notes.append(
            f"{excluded_count} Terraform file(s) were not scanned: the confirmed "
            "discovery scope excludes them"
        )
    # Which account a referenced resource lives in is in its ARN; whether that
    # is the estate's own account is only knowable when a provider block says
    # which account that is. Noted per entry when it differs, because that is
    # the cross-account case the assessment grades at its worst band. The
    # scan-level "cannot tell" note is written by the harvest, after the
    # merge, so it counts the referenced entries that actually survive.
    # A cross-account verdict — which also refuses the fold onto a same-named
    # declaration — needs the estate's account set to be COMPLETE: every aws
    # provider block stating its account literally. With any provider silent
    # the set may be missing the very account the ARN names, so the entry
    # gets the unknown-account note instead: it still folds, and the doubt
    # stays on it for the review to settle.
    # NOT `unread`: the provider pass reads every file on its own, before and
    # regardless of the datastore walk, and adds a file to
    # `accounts_incomplete` itself when a provider block there is unclosed or
    # states its account with an expression. Withholding the verdict for
    # every file the datastore walk left unread turned one stray unclosed
    # block anywhere into a fold of every confirmed cross-account ARN onto a
    # same-named declaration — the wrong-resource error — for nothing.
    for extra in extra_referenced:
        held = referenced.get(extra.get("address"))
        if held is None:
            referenced[extra["address"]] = extra
            continue
        for field in ("evidence", "consumers"):
            for value in extra.get(field) or []:
                if value not in held.setdefault(field, []):
                    held[field].append(value)
    complete = bool(estate_accounts) and not accounts_incomplete
    # Regions follow the same rule: a verdict needs every provider block in
    # scope to state its region literally and nothing left unread.
    regions_complete = bool(estate_regions) and not regions_incomplete and not unread
    for entry in referenced.values():
        region = entry.get("region")
        partition = (entry.get("arn") or "::").split(":")[1]
        estate_partitions = {_partition_of(r) for r in estate_regions}
        # Under every name the entry answers to (`name_keys`): the console
        # form `prod/db-AbC1dE` reaches the declared `prod/db`'s replica set
        # through its bare name, as the merge reaches the declaration.
        replicated_into = set().union(*(
            replica_regions.get((entry.get("service"), name), set())
            for _family, name in name_keys(entry)))
        # The NEGATIVE test, as the account verdict itself is: refused only
        # when the set is complete and the account is known to be another's.
        # Requiring positive proof — the account IN the set — left every
        # replica of an estate whose provider states no account (the default
        # shape) graded `migrate` and gating, its consumers never carried;
        # and requiring the partition in the estate's set, empty while the
        # provider's region is an expression, skipped this branch and let the
        # replica FOLD through the unknown-region branch below — the
        # withdrawn fold back through a side door. An unknown account keeps
        # its unknown-account note beside the replica one: "its replica, or a
        # same-named secret in an account the files do not name".
        foreign_account = (complete and entry.get("account")
                           and entry["account"] not in estate_accounts)
        foreign_partition = (regions_complete and partition
                             and partition not in estate_partitions)
        if (region and region in replicated_into
                and not foreign_account and not foreign_partition):
            # The estate's own replica: answered here, the way a foreign
            # spelling is, and never folded. Folding it onto the declaration
            # took three rounds of review to get wrong three different ways
            # — the region the declaration recorded, the ARN it kept, a
            # same-named secret in a third region absorbed — because a
            # replica is not a second spelling of one resource; it is a
            # second resource the primary's replication re-creates. So it
            # stands apart, non-gating, with the note saying what it is.
            # Its ACCOUNT has to be the estate's: a replica lives in the
            # primary's account, and an ARN in another account in that
            # region is the finance account's secret, which the account
            # verdict below exists to hold — answered as a replica it was
            # graded `rebuild` and gated nothing.
            # Not "known only from an ARN" either: the declaration states it.
            entry["notes"] = [n for n in entry["notes"] if n != REFERENCED_NOTE]
            entry["notes"].append(
                f"{REPLICA_NOTE_PREFIX}{region}: this estate declares this "
                f"{entry.get('service')} with a `replica {{}}` block for that region, so "
                "this is its replica, not a foreign resource — it is re-created by "
                "the primary's replication policy on the target rather than moved, "
                "and nothing waits on it; report the primary")
            if entry.get("service") == "secretsmanager":
                entry["disposition"] = "rebuild"
        elif regions_complete and region and region not in estate_regions:
            entry["notes"].append(
                f"{CROSS_REGION_NOTE_PREFIX}{region}, which no provider block in the "
                "scanned Terraform names (the estate's own: "
                f"{', '.join(sorted(estate_regions))}) — a cross-region dependency, "
                "not merged with a same-named resource declared here")
        elif not regions_complete and (region or (partition and partition != "aws")):
            # The account fallback covers a standalone entry and a folded one
            # alike; the region one covered only folds, so an out-of-region
            # dependency nothing declares — the very shape this section
            # exists to record — was reported with no region caveat at all.
            where = ([f"region {region}"] if region else []) + (
                [f"partition {partition}"] if partition and partition != "aws" else [])
            entry["notes"].append(
                f"{UNKNOWN_REGION_NOTE_PREFIX}{' and '.join(where)}, and the "
                "scanned Terraform does not state its own region(s) completely, "
                "so whether that is a region this estate uses is not knowable "
                "from the files")
        elif regions_complete and partition and partition not in estate_partitions:
            entry["notes"].append(
                f"{CROSS_PARTITION_NOTE_PREFIX}{partition}, while every "
                "provider block in the scanned Terraform is in "
                f"{', '.join(sorted(estate_partitions))} — not merged with a "
                "same-named resource declared here")
    # One name, several accounts, and no way to say which — if any — is the
    # estate's own: none of them may fold onto a same-named declaration, or
    # the walk order would decide which account the declaration "lives in".
    by_name = {}
    for entry in referenced.values():
        by_name.setdefault((entry["service"], entry["identifier"]), []).append(entry)
    for group in by_name.values():
        # The estate's own replicas are answered already (`REPLICA_NOTE_PREFIX`)
        # and must not make the primary's spelling ambiguous: paired with it,
        # the primary was flagged and refused its fold onto the declaration
        # whenever the replica's region was also a provider region.
        group = [e for e in group
                 if not any(n.startswith(REPLICA_NOTE_PREFIX) for n in e.get("notes") or [])]
        if complete:
            # With the estate's account set complete, an ARN in an account it
            # does not own is answered already — it gets the cross-account
            # note below and never folds — so it cannot make the estate's OWN
            # spelling of the name ambiguous. Grouping it in did exactly that:
            # `db:orders-replica` in the estate's account beside the same
            # name in a foreign one left the estate's ARN flagged, refused
            # its fold onto the declared replica, and recorded one database
            # twice, each gating.
            group = [e for e in group
                     if not e.get("account") or e["account"] in estate_accounts]
        if regions_complete:
            # The region axis, for the same reason: with every provider's
            # region stated and every file read, an ARN in a region no
            # provider names is a cross-region dependency already — noted,
            # never folded — and leaving it in the group flagged the estate's
            # own spelling beside it, so a parameter in `us-east-1` gated
            # twice because a policy also named its `eu-west-1` twin.
            # And the partition, which a region-less ARN (S3) states on its
            # own: `arn:aws-cn:s3:::x` beside the estate's `arn:aws:s3:::x`
            # is answered by the cross-partition note and must not flag
            # the estate's own bucket — the service most often granted by
            # literal ARN, and the one the region filter could not reach.
            partitions = {_partition_of(r) for r in estate_regions}
            group = [e for e in group
                     if (not e.get("region") or e["region"] in estate_regions)
                     and (e.get("arn") or "::").split(":")[1] in partitions | {""}]
        if len(group) < 2 or all(_compatible(a, b) for a in group for b in group):
            continue
        # The same name in two accounts or two regions — a DynamoDB global
        # table's replicas, or two estates' queues. A declaration states
        # neither, so it cannot say which of these it is, and the walk order
        # must not decide.
        places = sorted({f"{(e.get('arn') or '::').split(':')[1] or 'any partition'}/"
                         f"{e.get('region') or 'any region'}/"
                         f"{e.get('account') or 'any account'}"
                         + (f" ({e['endpoint']})" if e.get("endpoint") else "")
                         for e in group})
        for entry in group:
            entry["notes"].append(
                f"{AMBIGUOUS_ACCOUNT_NOTE_PREFIX}({', '.join(places)}); "
                "which of them, if any, is a resource this estate declares under "
                "the same name is not knowable from the files, so none is merged "
                "with a declaration")
    for entry in referenced.values():
        if not entry.get("account") or entry["account"] in estate_accounts:
            continue
        if complete:
            entry["notes"].append(
                _cross_account_note(entry["account"], estate_accounts))
        else:
            entry["notes"].append(
                _unknown_account_note(entry["account"], estate_accounts))
    # An ARN whose name and kind match a block the declared walk counts
    # through its primary — an ElastiCache group member, a read replica, an
    # Aurora/DocDB/Neptune cluster instance. It is NOTED, not dropped.
    #
    # Dropping it was the first shape of this, and it was wrong in the same
    # way each time: the declared walk knows a member belongs to a group
    # because the block says so, while here there is only a name, and the
    # ARN states an account and a region the declaration never does. So the
    # two may be one resource or two, the files cannot say, and deleting one
    # of them deletes exactly the out-of-band dependency this section exists
    # to record. Under-detection is the preferred error in this module; a
    # duplicate a reviewer merges is the cheap direction, and a note is how
    # they are told to look.
    duplicates = []
    for entry in referenced.values():
        if _foreign(entry):
            # Already placed somewhere the estate does not own: the local
            # child cannot be the same resource, and saying it might be
            # would contradict the note it already carries.
            continue
        what = counted_through_a_primary.get(
            _uncounted_key(entry["service"], _entry_kind(entry), entry["identifier"]))
        if not what:
            continue
        duplicates.append(f"{entry['service']} {entry['identifier']} ({entry['address']})")
        entry["notes"].append(
            f"{DUPLICATE_NOTE_PREFIX}{what} declared here shares this name and kind, "
            "and the scan counts that one through its primary rather than on its own "
            "— so this may be the same resource recorded twice. The files cannot "
            "settle it: the ARN states an account and a region the declaration does "
            "not. Merge them at the review if they are one."
        )
    if duplicates:
        notes.append(
            f"{len(duplicates)} literal ARN(s) may name a resource this scan already "
            "counts through its primary; each is recorded with a note rather than "
            "merged, because only a human can say whether they are the same thing: "
            + _listed(duplicates)
        )
    if (accounts_incomplete and estate_accounts
            and any(e.get("account") for e in referenced.values())):
        notes.append(
            f"{len(accounts_incomplete)} file(s) either declare an aws provider "
            "that does not state its account literally or were not read to the "
            "end (excluded, oversized, unreadable or truncated), so the estate's "
            "own accounts (" + ", ".join(sorted(estate_accounts))
            + ") may be incomplete; ARNs in other accounts are noted as "
            "unknown rather than cross-account: " + _listed(accounts_incomplete)
        )
    prose_arns = {token for arn, token in prose_candidates.items()
                  if arn not in referenced}
    if prose_arns:
        notes.append(
            f"{len(prose_arns)} ARN(s) appear only in a `description`, which is "
            "prose and is not read — an example in one would otherwise become a "
            "dependency, and on the consumer side would hand a workload a resource "
            "the text may say it no longer uses. If any of these is real, record it "
            "at the data review: " + _listed(prose_arns)
        )
    if malformed_arns:
        notes.append(
            f"{len(malformed_arns)} ARN- or endpoint-shaped string(s) in a data "
            "service's namespace could not be read and were not recorded: "
            + _listed(malformed_arns)
        )
    if wildcard_arns:
        notes.append(
            f"{len(wildcard_arns)} ARN(s) or endpoint(s) name a family of "
            "resources with a wildcard and were not recorded as data services — "
            "a grant on a prefix says a workload reaches some of them, not which: "
            + _listed(wildcard_arns)
        )
    if expression_arns:
        notes.append(
            f"{len(expression_arns)} ARN(s) or endpoint(s) build the resource "
            "name from a variable and were not recorded — the real name is not "
            "knowable from the files alone: " + _listed(expression_arns)
        )
    return entries, list(referenced.values()), notes, regions_complete


# Services one ARN namespace cannot tell apart. A declared `aws_rds_cluster`
# refines to rds, aurora, docdb or neptune from its engine; the ARN that names
# it says only `cluster:`. The two must still meet in the merge.
_RDS_FAMILY = frozenset(("rds", "aurora", "docdb", "neptune"))

TWINS_NOTE_MARK = " declared entries share this name ("

_FILL_FIELDS = ("engine", "engine_version", "multi_az", "allocated_storage",
                "storage_type", "region", "account", "arn", "endpoint", "module_source",
                # Which console spelling a secret absorbed: carried over when
                # a declaration folds a referenced entry that already merged
                # one, so the memory survives the fold that needs it.
                "console_form",
                # The Terraform address of the block the entry came from. A
                # referenced-only entry carries its ARN here instead, and a
                # declared twin's real address wins over it. Not what the
                # consumer join reads: that runs BEFORE this, on the unmerged
                # entries, which is the whole reason the field exists.
                "address")


def _same_service(one: dict, other: dict) -> bool:
    """Do two entries name the same kind of service?

    Equal services, or one RDS-family service against another when the
    referenced side is a `cluster:` ARN — the one form that genuinely cannot
    say whether it is Aurora, DocumentDB or Neptune, and that a declared
    `aws_rds_cluster` refines from its engine. A `db:` ARN names an
    instance and is never a cluster's twin.

    Equal services are not always enough either: ElastiCache has the second
    namespace pair `_resource_kind` computes, and `_same_elasticache_kind`
    applies it.
    """
    a, b = one.get("service"), other.get("service")
    if a not in _RDS_FAMILY or b not in _RDS_FAMILY:
        if a != b:
            return False
        # `opensearch` is the third service two ARN namespaces refine into —
        # `es` (`domain/x`) and `aoss` (`collection/x`) — and it gets no
        # equivalent guard on purpose: an `aoss` collection ARN carries the
        # id AWS assigned it, never the name a human chose, so the collision
        # this would protect against needs a domain and a collection whose
        # AWS-assigned id equals the domain's name. `_ASSIGNED_ID_NOTE`
        # already tells the reviewer the identifier is not a chosen name.
        return _same_elasticache_kind(one, other) if a == "elasticache" else True
    # Inside the RDS family the refined service name is not enough, in
    # either direction. An ARN says whether it names a cluster (`cluster:`)
    # or an instance (`db:`), a declaration says which it declares, and
    # instance identifiers and cluster identifiers are separate namespaces —
    # so `aws_db_instance "orders"` and a cluster called `orders` are two
    # databases even when both refine to `rds`, which a Multi-AZ DB cluster
    # (an `aws_rds_cluster` with a non-Aurora engine) does.
    referenced = [e for e in (one, other) if e.get("detection") == "referenced"]
    if len(referenced) != 1:
        return a == b
    named = referenced[0]
    declared = other if named is one else one
    if _CLUSTER_ARN_NOTE in (named.get("notes") or []):
        # `cluster:` cannot say whether it is Aurora, DocumentDB or Neptune,
        # so it may cross the family — but only onto a declared cluster.
        return _declared_cluster(declared)
    return a == b and not _declared_cluster(declared)


_ELASTICACHE_ADDRESSES = (
    ("aws_elasticache_cluster.", "cluster"),
    ("aws_elasticache_replication_group.", "replicationgroup"),
    ("aws_elasticache_serverless_cache.", "serverlesscache"),
)


def _declared_elasticache_kind(entry: dict) -> str:
    """Which ElastiCache namespace a declaration occupies, by block type.

    Empty for a module-declared cache, whose address names the module and
    not the kind — undecidable, so the fold is left alone there.
    """
    if entry.get("detection") != "declared":
        return ""
    address = entry.get("address") or ""
    for prefix, kind in _ELASTICACHE_ADDRESSES:
        if address.startswith(prefix):
            return kind
    return ""


def _same_elasticache_kind(one: dict, other: dict) -> bool:
    """ElastiCache's own separate namespaces, the pair `_resource_kind`
    already computes: `cluster:` names a standalone cache and
    `replicationgroup:` names a Redis group, exactly as `db:` and `cluster:`
    split RDS.

    A standalone cache called `redis` beside a replication group called
    `redis` is two caches. Folding one's ARN onto the other loses the second
    from the section a gate reads, attributes its consumers to the group,
    and leaves the surviving entry holding a durable correction handle that
    names the other thing. The rule was enforced for RDS and not here, which
    is the asymmetry this closes.
    """
    referenced = [e for e in (one, other) if e.get("detection") == "referenced"]
    if len(referenced) == 2:
        # Two ARNs, and each says its own kind. The RDS family is separated
        # here by accident — `db:` resolves to `rds` and `cluster:` to
        # `aurora`, so `_same_service`'s equality already parts them — but
        # both ElastiCache spellings resolve to `elasticache`, and nothing
        # else on this path can tell them apart: same region, same account,
        # same name. They folded, the replication group vanished from the
        # section a gate reads, its consumers were attributed to the
        # standalone cache, and the surviving handle named the other thing.
        kinds = {_entry_kind(one), _entry_kind(other)}
        return "" in kinds or len(kinds) == 1
    if len(referenced) != 1:
        return True
    named = referenced[0]
    declared = other if named is one else one
    kind = _entry_kind(named)
    declared_kind = _declared_elasticache_kind(declared)
    if not declared_kind:
        # A module-declared cache: the address names the module, so the block
        # type cannot say which namespace it occupies, and one ARN against it
        # is genuinely undecidable — permitted. A SECOND ARN of the other kind
        # is not: at most one of the two can be this block. Folds are judged
        # pairwise against the declaration, so each ARN arrives here alone and
        # the two-ARN rule above never sees the pair; reading the kind off the
        # ARN the declaration has already folded in is that rule in pairwise
        # form. The first ARN folds, the second stands as its own entry rather
        # than being dropped for want of a composable spelling. WHICH one
        # folds is `_specificity`'s order — stated fields, then the shorter
        # address — so `cluster:` systematically beats `replicationgroup:`
        # onto a module cache; for a module that in fact creates a group
        # (cloudposse/elasticache-redis) the wrong ARN folds and the right one
        # stands as referenced. Both survive and ElastiCache is `rebuild`, so
        # nothing gates on the choice, but it is biased, not random.
        declared_kind = _entry_kind(declared)
    return not (kind and declared_kind) or kind == declared_kind


_CLUSTER_ADDRESSES = ("aws_rds_cluster.", "aws_docdb_cluster.", "aws_neptune_cluster.")


def _declared_cluster(entry: dict) -> bool:
    """A declared entry that is a cluster: by service when the engine refined
    it (aurora, docdb, neptune), or by block type when it refined to `rds`
    (a Multi-AZ DB cluster is `aws_rds_cluster` with a non-Aurora engine)."""
    if entry.get("detection") != "declared":
        return False
    if entry.get("service") in ("aurora", "docdb", "neptune"):
        return True
    return (entry.get("address") or "").startswith(_CLUSTER_ADDRESSES)


def _referenced_only(note: str) -> bool:
    """A note that only makes sense while the entry is known from its ARN
    alone. The unknown-account note is NOT one of them: after a fold it is
    the one thing that says the ARN might name a same-named resource in
    another account, and the fold is exactly when that matters."""
    return note in REFERENCED_NOTES or note.startswith(
        (CROSS_ACCOUNT_NOTE_PREFIX, AMBIGUOUS_ACCOUNT_NOTE_PREFIX, CROSS_REGION_NOTE_PREFIX,
         CROSS_PARTITION_NOTE_PREFIX))


def _secret_arn_named(arn: str, name: str):
    """A Secrets Manager ARN with its resource name replaced, so two
    spellings of one secret differ only in region and account."""
    head, separator, _rest = (arn or "").rpartition(":secret:")
    return f"{head}{separator}{name}" if separator and name else arn


def _note_secret_merge(entry: dict, one: str, other: str) -> None:
    """Says that two spellings of a secret name were taken for one secret.

    The strip is a judgement about a suffix, not a fact the files state, so
    the entry records what it was built from — the merge used to be the one
    silent branch here while the refusal to merge was noisy.
    """
    note = (f"{SECRET_MERGED_NOTE_PREFIX}'{one}' and '{other}', taken for one "
            "secret because the longer name ends in what looks like the "
            "six-character suffix AWS appends. If they are two secrets, say so "
            "at the data review")
    if note not in entry.setdefault("notes", []):
        entry["notes"].append(note)


def _bare_secret_arn(arn: str, identifier: str, bare: str):
    """A Secrets Manager ARN with the random suffix taken off its name, so
    two spellings of one secret differ only in their region and account and
    `_compose_arn` can put them together."""
    if arn and identifier and identifier != bare and arn.endswith(identifier):
        return arn[:-len(identifier)] + bare
    return arn


def _secret_resources_agree(namespace: str, one: str, other: str) -> bool:
    """Are two Secrets Manager resource parts the console form and the bare
    form of ONE secret?

    One side is stripped, never both — the same asymmetry `_same_identifier`
    spells out and for the same reason. `acme/db-Prod01` and `acme/db-Dev001`
    are two real secrets whose endings both pass for AWS's random suffix, so
    reducing both to `acme/db` calls them equal; the merge deliberately keeps
    them apart, and agreeing here would let a correction keyed on one land on
    the other. Stripping one side matches only the pair the merge itself
    folds: a name and that same name plus a suffix.
    """
    if namespace != "secretsmanager":
        return False

    def name_of(resource):
        head, separator, name = (resource or "").partition(":")
        return name if head == "secret" and separator else None

    a, b = name_of(one), name_of(other)
    if a is None or b is None:
        return False
    return _strip_secret_suffix(a) == b or _strip_secret_suffix(b) == a


def spelling_is_ambiguous(entry: dict) -> bool:
    """Does more than one ARN spelling of this name stand in the section?

    An entry gets the ambiguous-account note when the scan saw the same name
    under ARNs it could not reconcile — two accounts, two regions, two
    partitions — and it then refuses to merge any of them. Every entry in
    that group carries the note, including the under-specified spelling.

    `arn_spellings_agree` must not be used across such a group. Its tolerance
    exists for one resource wearing two spellings across two scans, and it is
    sound only while at most one entry can carry an agreeing ARN. Here three
    can, and a `migrated` reported on `arn:aws:sqs:*:*:orders` then released
    the ship gate for two queues nobody had moved. Read from the note by the
    same argument `_foreign` reads from one: these are facts about the
    estate's accounts that the extraction established and no field carries.

    The account note alone was not enough. It groups on `(service,
    identifier)`, and `arn_spellings_agree` also matches ACROSS identifiers
    for Secrets Manager, so a bare secret name and two console forms in two
    accounts agreed while none of them was flagged.
    `_note_shared_spellings` closes that by asking the merged section the
    same question the guards ask.

    The other-console-form note is the one case flagged BEYOND that relation:
    a declaration that absorbed one console spelling beside a second one it
    refused. Their ARNs carry two suffixes and do not agree, so neither guard
    would cross them by spelling — but which of the two is the declared
    secret is exactly what the files cannot settle, and the records made
    against them are compared by identifier as well as by ARN.
    """
    return any(n.startswith((AMBIGUOUS_ACCOUNT_NOTE_PREFIX,
                             SHARED_SPELLING_NOTE_PREFIX,
                             SECRET_OTHER_FORM_NOTE_PREFIX))
               for n in entry.get("notes") or [])


def arn_spellings_agree(one: str, other: str) -> bool:
    """Are these two spellings of one resource's ARN?

    An entry's handle is one SPELLING of its ARN, and which one it carries
    can change from scan to scan: a wildcard-region policy joined later by a
    fully-qualified sighting, the reverse when the specific file leaves the
    confirmed scope at an `amend_discovery_scope`, or a secret's console form
    reduced to its bare name once the policy form turns up. `_specificity`
    fixes the ORDER so the choice does not depend on walk order, but it does
    not stop the choice changing when the set of sightings does — so a
    correction the reviewer keyed on the spelling they were shown has to
    still find the entry, and comparing handles for equality does not do it.
    The alternative failure is silent: the correction lands nowhere and the
    unplaced note tells the reviewer the block was deleted or excluded, while
    the resource sits one row away under another spelling.

    Unstated is not a contradiction; two different literals are. `*` and an
    empty field both mean "any", as everywhere else here — so
    `arn:aws:sqs:*:*:orders` agrees with `arn:aws:sqs:us-east-1:111…:orders`
    and neither agrees with the same queue's ARN in `eu-west-1`, which the
    cross-region rule says is a different queue.

    A false match here is worse than the lost correction it fixes: the
    correction does not vanish with a note, it lands on another resource and
    reports success. So this agrees only where the merge itself would fold,
    and the secret-name tolerance strips one side at a time for exactly that
    reason (`_secret_resources_agree`).
    """
    if not one or not other:
        return False
    if one == other:
        return True
    a, b = one.split(":", 5), other.split(":", 5)
    if len(a) != 6 or len(b) != 6 or a[1:3] != b[1:3]:
        return False
    for x, y in zip(a[3:5], b[3:5]):
        x = "" if x == "*" else x
        y = "" if y == "*" else y
        if x and y and x != y:
            return False
    return a[5] == b[5] or _secret_resources_agree(a[2], a[5], b[5])


def _better_field(one: str, other: str) -> str:
    """The more informative of two ARN fields, by a TOTAL order: a literal,
    then unstated, then `*`.

    Pairwise "take mine unless mine is unstated" is not an order — with one
    side `""` and the other `*` both are unstated, each defers to the other,
    and the answer depends on which was passed first. The handle then flips
    between scans, and a correction keyed on it cannot be replayed.
    """
    for candidate in (one, other):
        if candidate not in ("", "*"):
            return candidate
    return "" if "" in (one, other) else one


def _compose_arn(one: str, other: str):
    """One ARN from two spellings of a resource: field by field, a literal
    over a wildcard or an empty field. None when they are not two spellings
    of one ARN (different partitions, namespaces or resources)."""
    if not one or not other:
        return None
    a, b = one.split(":", 5), other.split(":", 5)
    if len(a) != 6 or len(b) != 6 or a[1:3] != b[1:3] or a[5] != b[5]:
        return None
    return ":".join(a[:3] + [_better_field(x, y) for x, y in zip(a[3:5], b[3:5])]
                    + [a[5]])


def _specificity(entry: dict) -> tuple:
    """Which of two ARN spellings of one resource is the entry's handle when
    they cannot be composed: the one stating the most (region, account),
    then the shorter, then the lexically larger — a fixed order, so the
    address does not depend on which file the walk met first, and a
    correction keyed on it survives a new sighting."""
    # An ARN before an endpoint: the ARN is the handle whenever one was seen,
    # as every reader and DESIGN.md say — without this `s3://acme-logs`, the
    # shorter spelling, beat `arn:aws:s3:::acme-logs` for a bucket.
    return (bool(entry.get("arn")),
            bool(entry.get("region")) + bool(entry.get("account")),
            -len(entry.get("address") or ""), entry.get("address") or "")


def _note_unverified_fold(current: dict, referenced: dict, regions_verified: bool) -> None:
    """After a referenced entry folded onto a declaration while the estate's
    regions could not be verified: the region the entry now carries, or a
    non-default partition, came from an ARN that may name a same-named
    resource elsewhere, and the declaration states neither of its own. Kept
    through the fold, like the unknown-account note.
    """
    if current.get("detection") != "declared" or regions_verified:
        return
    if any(note.startswith(UNKNOWN_REGION_NOTE_PREFIX)
           for note in current.get("notes") or []):
        # The referenced side already said it, and the fold carried the note
        # over; a second wording of one fact reads as two.
        return
    partition = (referenced.get("arn") or "::").split(":")[1]
    where = []
    if referenced.get("region"):
        where.append(f"region {referenced['region']}")
    if partition and partition != "aws":
        where.append(f"partition {partition}")
    if not where:
        return
    note = (f"{UNKNOWN_REGION_NOTE_PREFIX}{' and '.join(where)}, and the scanned "
            "Terraform does not state its own region(s) completely, so whether "
            "the ARN names this declared resource or a same-named one in another "
            "region or partition is not knowable from the files")
    if note not in current.setdefault("notes", []):
        current["notes"].append(note)


def _fold(current: dict, entry: dict) -> None:
    """Folds `entry` onto `current` in place: fill nulls, union the lists.

    "declared" outranks "referenced": a resource named by an ARN and also
    declared here is declared, and its properties are knowable — so none of
    the referenced side's notes survive the fold. They all describe the
    ARN-only view (provisioned out of band, engine unknown, a disposition
    graded from an ARN with no arguments), and the declaration answers every
    one of them; carrying a "not stated literally" note onto an entry whose
    declaration states it would have the entry contradict itself.
    """
    declared = "declared" in (current.get("detection"), entry.get("detection"))
    if (current.get("detection") == "declared" and entry.get("detection") == "referenced"
            and current.get("service") == "secretsmanager"):
        absorbed = (entry["identifier"]
                    if _strip_secret_suffix(entry.get("identifier") or "")
                    == current.get("identifier")
                    else entry.get("console_form"))
        if absorbed:
            # Which console spelling this declaration absorbed — recorded on
            # the FIRST fold, before the branch below, because that branch
            # runs only once the declaration already carries an ARN: a first
            # console form arrives through the plain field fill, and a memory
            # written on the second fold is written after the twin test that
            # needed it. A referenced entry that already merged one hands its
            # memory over the same way.
            current["console_form"] = absorbed
            # Said, as the undeclared branch says it. None of the referenced
            # side's notes survive a declared fold — its merge note included
            # — and `console_form` is printed nowhere, so without this the
            # strip was a judgement the review could neither see nor dispute.
            _note_secret_merge(current, current.get("identifier"), absorbed)
    if (current.get("detection") == "declared" and entry.get("detection") == "referenced"
            and current.get("arn") and entry.get("arn")):
        # A second spelling folding onto a declaration. Its `address` is the
        # block and never changes, but its `arn` must state everything the
        # spellings state between them: filling only when null would keep
        # whichever the walk met first, wildcards and all, and a correction
        # keyed on the other spelling would find nothing once the
        # declaration leaves the repository.
        current_arn, entry_arn = current["arn"], entry["arn"]
        if current.get("service") == "secretsmanager":
            # Both spellings reduced to the declared name before composing,
            # as the referenced-to-referenced branch below does: otherwise
            # the console form and the policy form differ in their resource
            # part, `_compose_arn` refuses, and the handle is whichever the
            # walk met first.
            current_arn = _secret_arn_named(current_arn, current.get("identifier"))
            entry_arn = _secret_arn_named(entry_arn, current.get("identifier"))
        composed = _compose_arn(current_arn, entry_arn)
        if composed:
            current["arn"] = composed
    if (not declared and current.get("detection") == "referenced"
            and entry.get("detection") == "referenced"):
        current_arn, entry_arn = current.get("arn"), entry.get("arn")
        if (current.get("service") == "secretsmanager"
                and (current.get("identifier") or "") != (entry.get("identifier") or "")):
            _note_secret_merge(current, current.get("identifier"),
                               entry.get("identifier"))
            # Two spellings of one secret: the console form carries the
            # random suffix, the policy form does not. The bare name is the
            # identity, and BOTH ARNs are reduced to it before composing —
            # taking the shorter spelling whole would throw away the region
            # and account the longer one states, and would make the handle
            # depend on which file the walk met first.
            bare = min((current.get("identifier") or "", entry.get("identifier") or ""),
                       key=len)
            current_arn = _bare_secret_arn(current_arn, current.get("identifier"), bare)
            entry_arn = _bare_secret_arn(entry_arn, entry.get("identifier"), bare)
            if current.get("address") == current.get("arn"):
                current["address"] = current_arn
            # The longer spelling is the console form — the only way two
            # differing identifiers reach this branch is one being the other
            # plus a suffix — and it is kept, since the bare name the entry
            # now carries no longer says which suffix was taken for it.
            current["console_form"] = max(
                (current.get("identifier") or "", entry.get("identifier") or ""), key=len)
            current["identifier"] = bare
            current["arn"] = current_arn
        # Two spellings of one resource: the handle states everything either
        # of them states, so it cannot depend on which the walk met first and
        # never asserts a wildcard for a field the entry knows.
        composed = _compose_arn(current_arn, entry_arn)
        if composed and current.get("arn") and current.get("address") == current.get("arn"):
            current["address"] = current["arn"] = composed
        elif _specificity(entry) > _specificity(current):
            for field in ("address", "arn"):
                if entry.get(field) is not None:
                    current[field] = entry[field]
    for field in _FILL_FIELDS:
        if current.get(field) is None and entry.get(field) is not None:
            current[field] = entry[field]
    # A standing entry that already says two declarations share its name has
    # had the referenced-only note removed on purpose; a re-merge of the same
    # ARN must not put it back.
    shares_a_name = any(TWINS_NOTE_MARK in n for n in current.get("notes") or [])
    for field in ("evidence", "consumers", "notes"):
        for value in entry.get(field) or []:
            if field == "notes" and shares_a_name and value == REFERENCED_NOTE:
                continue
            if field == "notes" and declared and entry.get("detection") == "referenced" \
                    and not value.startswith(UNKNOWN_ACCOUNT_NOTE_PREFIX):
                # None of the referenced side's notes but the unknown-account
                # one survive: they describe the ARN-only view, and the
                # declaration answers them.
                continue
            if field == "notes" and declared and _referenced_only(value):
                continue
            if value not in current.setdefault(field, []):
                current[field].append(value)
    if declared:
        current["detection"] = "declared"
        current["declared_in_repo"] = True
        current["notes"] = [n for n in current.get("notes") or []
                            if not _referenced_only(n)]


def _compatible(one: dict, other: dict) -> bool:
    """Could two literals name one resource? Same region, account and
    partition when both state one; an unstated value contradicts nothing —
    an endpoint states no account and a cache endpoint no region, and only
    an ARN states a partition."""
    for field in ("region", "account"):
        a, b = one.get(field), other.get(field)
        if a and b and a != b:
            return False
    partitions = {(e.get("arn") or "").split(":")[1]
                  for e in (one, other) if e.get("arn")}
    # `arn:aws:s3:::logs` and `arn:aws-cn:s3:::logs` are two buckets.
    return len(partitions) <= 1


def _foreign(entry: dict) -> bool:
    """A referenced entry that must not fold onto a declaration: its ARN
    names an account the estate's provider blocks say is not its own, or
    one of several accounts that share the name with no way to choose.
    Recognisable by the notes the extraction wrote, which are the facts
    about the estate's accounts the merge does not otherwise hold."""
    return any(n.startswith((CROSS_ACCOUNT_NOTE_PREFIX, AMBIGUOUS_ACCOUNT_NOTE_PREFIX,
                             CROSS_REGION_NOTE_PREFIX, CROSS_PARTITION_NOTE_PREFIX,
                             REPLICA_NOTE_PREFIX, AMBIGUOUS_ENDPOINT_NOTE_PREFIX))
               for n in entry.get("notes") or [])


_SECRET_SUFFIX_RE = re.compile(r"-[A-Za-z0-9]{6}$")
# AWS's suffix is six characters drawn at random from base62, and a name
# ending in an ordinary word or a date is not: `-Backup`, `-Master`,
# `-202401`, `-shard1`, `-2024Q1` are names, and stripping one merges two
# secrets. Requiring all three character classes separates them, at a price
# worth stating plainly: about three real suffixes in five carry all three,
# so the other two in five are not stripped either and the console form
# stands as a second entry. That is the accepted cost, not an oversight —
# see `_strip_secret_suffix`.
_RANDOM_SUFFIX_RE = re.compile(
    r"-(?=[A-Za-z0-9]{6}$)(?=[A-Za-z0-9]*[a-z])(?=[A-Za-z0-9]*[A-Z])"
    r"(?=[A-Za-z0-9]*\d)[A-Za-z0-9]{6}$")
SECRET_SUFFIX_NOTE_PREFIX = "may be the console form of the secret "
SECRET_MERGED_NOTE_PREFIX = "recorded from both "


def _strip_secret_suffix(identifier: str):
    """The name without AWS's six-character suffix, or None when the suffix
    does not look like one.

    Only a suffix carrying lower case, upper case AND a digit is taken for
    AWS's — `-AbC1dE` yes, `-Backup`, `-reader`, `-202401`, `-shard1` no.
    The earlier rule exempted six letters of ONE case, which protected
    `-reader` and stripped `-Backup`, merging two secrets silently in the
    section that gates a pipeline.

    The test is deliberately one-sided, and the arithmetic says how much:
    of the 62**6 suffixes AWS can draw, 33294892800 carry all three classes
    and the rest do not, so roughly two console-form ARNs in five are NOT
    folded onto the declaration they belong to and the section carries two
    entries for one secret. `SECRET_SUFFIX_NOTE_PREFIX` names the pair on
    the surviving entry whenever the bare name is also in the section, which
    is every case where the duplicate is real, so a reviewer can merge them.

    That is the trade, chosen knowingly. Loosening it to "two of the three
    classes" would accept about 99 per cent of real suffixes and also strip
    `-Backup` and `-Master`, which carry two — back to merging two secrets
    silently, this module's worst error. A duplicate entry is visible and
    correctable at the data review; a merge is neither.
    """
    if not _RANDOM_SUFFIX_RE.search(identifier or ""):
        return None
    return identifier[:-7]


def _same_identifier(one: dict, other: dict) -> bool:
    """Equal identifiers, with two service-specific tolerances.

    A Secrets Manager ARN as the console prints it carries the secret's
    six-character suffix (`prod/db-AbC1dE`), and a declaration never does;
    the two name one secret. An SSM parameter ARN cannot say whether the
    name began with a slash (`parameter/db-password` is `db-password` and
    `/db-password` alike). Tolerated for matching only — the identifier each
    entry records is left as found.
    """
    a, b = one.get("identifier") or "", other.get("identifier") or ""
    if a == b:
        return True
    if one.get("service") == "secretsmanager" == other.get("service"):
        # Only an ARN carries the suffix, so only a referenced side is
        # stripped — and only one side at a time: `orders-master` and
        # `orders-config` are six alphanumerics too, and stripping both would
        # merge two secrets. A declaration is never stripped.
        def stripped(e):
            return (_strip_secret_suffix(e.get("identifier") or "")
                    if e.get("detection") == "referenced" else None)
        return (stripped(one) is not None and stripped(one) == b) or \
               (stripped(other) is not None and stripped(other) == a)
    if one.get("service") == "ssm" == other.get("service"):
        return a.lstrip("/") == b.lstrip("/")
    return False


SECRET_OTHER_FORM_NOTE_PREFIX = "a console spelling of the secret "


def _console_form(entry: dict, other: dict):
    """The console spelling — the name with AWS's six-character suffix — that
    stands for this Secrets Manager entry IN THIS PAIR: the one it folded in,
    or, for a referenced entry nothing has merged yet, its own identifier
    when that is the suffixed side of the pair, i.e. it strips to `other`'s
    identifier. Looking suffixed is not enough: `app-AbC1dE` is the BARE
    side when the other entry is `app-AbC1dE-XyZ9wQ` or `app-AbC1dE` itself,
    and taking it for a console form refused the exact policy form of a
    declared secret whose name happens to end in six mixed characters — one
    secret became two gating entries. A declaration's identifier is never
    one: a declared name is never stripped, whatever it ends in.
    """
    if entry.get("service") != "secretsmanager":
        return None
    if entry.get("console_form"):
        return entry["console_form"]
    if entry.get("detection") != "referenced":
        return None
    identifier = entry.get("identifier") or ""
    return (identifier
            if _strip_secret_suffix(identifier) == (other.get("identifier") or "")
            else None)


def _another_console_form(one: dict, other: dict) -> bool:
    """Do these two secret entries carry DIFFERENT console spellings?

    A secret has exactly one suffix, so at most one console form can be the
    declared secret's — and `_same_identifier` cannot see that: it judges
    each console form against the declaration's bare name on its own, both
    match, and `_fold` then reduces both to that name, so two secrets became
    one entry with no note, on the one service whose disposition gates. The
    entry keeps the spelling it absorbed (`console_form`) and a second one
    with another suffix is refused here — the pairwise form of the rule, as
    `_same_elasticache_kind` reads the kind of the ARN a module-declared
    cache already folded in. The same memory stops a bare policy form
    bridging two suffixed forms into one referenced entry. A spelling equal
    to the one held (a second sighting of the same secret under another
    region spelling) still folds.
    """
    a, b = _console_form(one, other), _console_form(other, one)
    return bool(a and b and a != b)


OTHER_ENDPOINT_NOTE_PREFIX = "another endpoint of this name is recorded beside it: "
AMBIGUOUS_ENDPOINT_NOTE_PREFIX = "named by more than one endpoint "


def _note_ambiguous_endpoints(entries: list) -> None:
    """Two or more DIFFERENT canonical endpoints of one name in one merge:
    none may fold onto a same-named declaration, and each says so.

    The ARN rule's twin (`AMBIGUOUS_ACCOUNT_NOTE_PREFIX`): with a declared
    `orders` and two ConfigMaps naming `orders.<idA>…` and `orders.<idB>…`,
    both hosts are compatible with the declaration (an endpoint states no
    account), so whichever the sort met first folded and the other stood
    apart — the instance id's lexical order decided which ConfigMap's
    workload the declaration owed, and the declaration carried no flag. Walk
    order must not decide which host a declaration is; the files cannot say,
    so neither folds and the review is told. `_foreign` reads the note, which
    is what keeps them off the declaration in `_twins`.
    """
    # Only what the merge would otherwise let fold onto one declaration: the
    # same service and compatible fields, and neither side already placed
    # elsewhere by the account, region or partition verdicts (`_foreign`) —
    # the ARN grouping filters those out for the same reason, and grouping
    # them in refused the estate's own host its fold and recorded one
    # database twice. A DocumentDB host beside an RDS host of one name, or a
    # cluster endpoint beside an instance endpoint, are two services already.
    hosts = {}
    candidates = [e for e in entries
                  if e.get("detection") == "referenced" and e.get("endpoint")
                  and not _foreign(e)]
    for entry in candidates:
        for key in name_keys(entry):
            hosts.setdefault(key, []).append(entry)
    for entry in candidates:
        others = set()
        for key in name_keys(entry):
            for other in hosts.get(key) or ():
                if (other is not entry and other["endpoint"] != entry["endpoint"]
                        and _same_service(other, entry) and _compatible(other, entry)):
                    others.add(other["endpoint"])
        if not others:
            continue
        note = (f"{AMBIGUOUS_ENDPOINT_NOTE_PREFIX}({', '.join(sorted(others))} beside "
                f"{entry['endpoint']}); each hostname is its own resource, and which of "
                "them, if any, is a resource this estate declares under the same name "
                "is not knowable from the files, so none is merged with a declaration")
        if note not in entry.setdefault("notes", []):
            entry["notes"].append(note)


def _another_endpoint(one: dict, other: dict) -> bool:
    """Do these two entries carry DIFFERENT canonical endpoints?

    An endpoint states no account, so `_compatible` cannot tell a dev
    database from a prod one by the fields an ARN would state — but the host
    itself does: `orders.c9akdev00001.us-east-1.rds.amazonaws.com` and
    `orders.c9akprod0001.us-east-1.rds.amazonaws.com` are two instances, as
    their instance ids say, and folding them recorded one database with both
    ConfigMaps' consumers and lost the other from the section the gate reads.
    Two spellings of ONE resource already share a canonical handle (a reader
    and a writer endpoint, a bucket's three URL forms), so a differing
    endpoint is a differing resource. The same rule holds a declared entry
    that already absorbed one endpoint against a second — the shape
    `_another_console_form` gives a secret's suffix.
    """
    a, b = one.get("endpoint"), other.get("endpoint")
    return bool(a and b and a != b)


def name_keys(entry: dict) -> set:
    """The (service family, name) keys an entry can be matched under.

    More than one where `_same_identifier` is tolerant: a Secrets Manager
    ARN carries the random suffix a declaration never states, and an SSM
    parameter name may or may not lead with a slash. Indexing an entry under
    all of them, and looking a candidate up under all of its own, keeps the
    tolerant match while turning the twin search from a walk of the whole
    section into a dict lookup — the merge was quadratic in the number of
    entries, which a generated policy file reaches.
    """
    service = entry.get("service")
    family = "rds" if service in _RDS_FAMILY else service
    identifier = entry.get("identifier") or ""
    names = {identifier}
    if service == "secretsmanager":
        bare = _strip_secret_suffix(identifier)
        if bare:
            names.add(bare)
    elif service == "ssm":
        names.add(identifier.lstrip("/"))
        names.add("/" + identifier.lstrip("/"))
    return {(family, name) for name in names}


def _arn_name_keys(entry: dict) -> set:
    """The `name_keys` an entry's ARN implies, which need not be the ones its
    IDENTIFIER implies.

    They diverge exactly where a fold has happened: a declared secret keeps
    the bare name it was declared under (`acme/db`) while the ARN it folded
    in carries the console spelling (`secret:acme/db-Prod01`). `name_keys`
    strips at most one secret suffix from an identifier and never adds one,
    so the ARN's own name is unreachable from the identifier's keys and two
    entries `arn_spellings_agree` matches sit in disjoint buckets.
    """
    parts = (entry.get("arn") or "").split(":", 5)
    if len(parts) != 6:
        return set()
    resolved = _resolve_arn_resource(parts[2], parts[5], parts[3], parts[4])
    if not resolved:
        return set()
    service, identifier, _canonical, _notes = resolved
    return name_keys({"service": service, "identifier": identifier})


def _twins(existing: list, entry: dict, detection: str) -> list:
    """Entries of the given detection that name the same resource as `entry`.

    A literal identifier and a service the ARN namespace can express. A
    fallback identifier is a Terraform address, never an AWS name, so it
    cannot match an ARN; and the match is repository-wide rather than
    per-directory, because an ARN or an endpoint names one resource in one
    account wherever the file that states it happens to sit. Two literals — a
    queue's ARN in a policy and its URL in a ConfigMap — must not disagree on
    region or account. And a referenced entry whose ARN names an account
    the estate's provider blocks do not is never a declared entry's twin: a
    same-named queue in the finance account is a different queue, and folding
    it would erase the one cross-account dependency this section exists to
    surface.
    """
    def crosses_the_account_line(e: dict) -> bool:
        referenced, declared = ((entry, e) if entry.get("detection") == "referenced"
                                else (e, entry))
        return (declared.get("detection") == "declared"
                and referenced.get("detection") == "referenced"
                and _foreign(referenced))
    def foreign_on_one_side(e: dict) -> bool:
        # Two referenced spellings of one name, one of them in an account the
        # estate's provider blocks say is not its own: that one is a different
        # queue by the same verdict that stops it folding onto a declaration,
        # so it is not a sibling of the estate's own spelling either. Counting
        # it as one refused the unstated spelling its fold onto the estate's
        # ARN, and the estate's own queue stood twice, each flagged — the
        # duplicate the ambiguity grouping had just stopped producing.
        return (detection == "referenced"
                and _foreign(e) != _foreign(entry))
    return [e for e in existing
            if e is not entry
            and e.get("detection") == detection
            and not e.get("identifier_is_fallback")
            # A database name is not an instance identifier: no ARN twin.
            and not e.get("identifier_is_db_name")
            and not entry.get("identifier_is_db_name")
            and _same_identifier(e, entry)
            and not _another_console_form(e, entry)
            and not _another_endpoint(e, entry)
            and _same_service(e, entry)
            and _compatible(e, entry)
            and not crosses_the_account_line(e)
            and not foreign_on_one_side(e)]


def merge_datastores(inventory: dict, entries: list,
                     regions_verified: bool = True) -> None:
    """Merges entries into inventory["data_dependencies"] in place.

    Declared entries dedupe on (service, identifier, directory); evidence and
    consumers union, and a later entry only fills fields the earlier one left
    null. Re-scans are therefore idempotent, and a resource split across two
    files (the cluster in one, its instances in another) lands as one entry.

    Referenced entries dedupe on their handle (the ARN or the canonical
    endpoint), fold onto another referenced entry naming the same resource
    (a queue's ARN in a policy and its URL in a ConfigMap), and fold onto the
    one declared entry that names the same resource, so an estate that both
    declares a bucket and grants a role access to it records one bucket —
    declared, with its ARN filled in. When two declared entries share the
    name (the per-environment pattern) the literal cannot say which it means,
    and the referenced entry stands on its own with a note saying so.
    """
    def _key(entry: dict) -> tuple:
        # Identity is scoped to the declaring directory, for literal names as
        # well as fallback addresses. A Terraform root module is a directory,
        # so `envs/dev` and `envs/prod` each declaring a queue named "orders"
        # are two queues in two accounts, not one — and the per-environment
        # duplicate-name pattern is common enough that folding them would
        # under-count a real estate. Within one directory the same name is the
        # same resource, so `_override.tf` and split declarations still merge
        # — when both state the same identifier. An override that re-declares
        # only `db_name` or a size takes a different identifier and stands as
        # a second entry: a known edge, identical on main.
        evidence = entry.get("evidence") or [""]
        # Inside the RDS family the service name is not enough: a Multi-AZ
        # DB cluster refines to `rds` exactly as an instance does, and
        # `IDENTIFIER_ARGS` reads `cluster_identifier` for it — so
        # `aws_db_instance.orders` and `aws_rds_cluster.orders` in one
        # directory would key alike and one production database would
        # vanish from the count. Instance identifiers and cluster
        # identifiers are separate namespaces, which is the rule
        # `_same_service` applies on the referenced side.
        # ElastiCache carries the same split under one service name:
        # `aws_elasticache_cluster` names a standalone cache by `cluster_id`
        # and `aws_elasticache_replication_group` names a Redis group by
        # `replication_group_id`, `IDENTIFIER_ARGS` reads both, and the two
        # are separate AWS namespaces — so a cache and a group both called
        # `redis` in one directory keyed alike and one of them vanished from
        # the section the ship gate reads. A module-declared cache still
        # keys on "" because its address names the module and not the block
        # type: undecidable, and the same residue the referenced side leaves.
        service = entry.get("service")
        # An instance identified by its DATABASE name keys on its block
        # address, as a fallback identifier does: two instances in one root
        # module sharing a `db_name` are two databases, and keyed on the name
        # they collapsed into one — the note saying "an ARN will not merge
        # with this entry" then sat on the entry an ARN had just merged with.
        identity = (entry.get("address") if entry.get("identifier_is_db_name")
                    else entry.get("identifier"))
        return (service, identity,
                os.path.dirname(evidence[0]),
                _declared_cluster(entry) if service in _RDS_FAMILY
                else _declared_elasticache_kind(entry) if service == "elasticache"
                else False)

    # Declared first, then referenced MOST SPECIFIC FIRST. The order is part
    # of the answer, not a convenience: a spelling that leaves the region or
    # account unstated is compatible with every literal spelling of the same
    # name, so whether it folds — and onto WHICH account — depended on how
    # many literal siblings happened to have been processed when it arrived.
    # Renaming a file moved a service account's grant onto a queue in a
    # different AWS account and changed the entry count, which is the
    # wrong-consumer error this module calls its worst and contradicts the
    # rule it states: two ARNs naming one name in two accounts fold onto
    # nothing, and walk order must not decide which account a resource lives
    # in. Sorted, the unstated spelling arrives last, sees every literal one
    # this scan found, and stands on its own when there is more than one.
    entries = ([e for e in entries if e.get("detection") != "referenced"]
               + sorted((e for e in entries if e.get("detection") == "referenced"),
                        key=_specificity, reverse=True))
    existing = inventory.setdefault("data_dependencies", [])
    # Before anything folds: two hosts of one name are answered like two
    # accounts of one name — apart, flagged, off the declaration.
    _note_ambiguous_endpoints(list(existing) + entries)
    # (family, name) -> the entries that could be twins under it.
    by_name = {}

    def index(entry: dict) -> None:
        for key in name_keys(entry):
            by_name.setdefault(key, []).append(entry)

    def unindex(entry: dict) -> None:
        for key in name_keys(entry):
            bucket = by_name.get(key) or []
            by_name[key] = [e for e in bucket if e is not entry]

    def candidates(entry: dict) -> list:
        seen, out = set(), []
        for key in name_keys(entry):
            for other in by_name.get(key) or ():
                if id(other) not in seen:
                    seen.add(id(other))
                    out.append(other)
        return out

    def answers_a_guess(guess: dict) -> bool:
        """Does a declared or referenced entry already state this name? A
        fact about the name answers the question the guess would ask —
        whatever account or region the fact is placed in, since the guess
        states neither."""
        return any(e.get("detection") in ("declared", "referenced")
                   and not e.get("identifier_is_fallback")
                   and _same_identifier(e, guess)
                   for e in candidates(guess))

    def drop_answered_guesses(fact: dict) -> None:
        # The declaration or the literal answers the question the guess asked.
        for guess in [e for e in candidates(fact)
                      if e.get("detection") == "inferred" and _same_identifier(e, fact)]:
            existing.remove(guess)
            unindex(guess)
            by_guess.pop(guess.get("address"), None)

    for entry in existing:
        index(entry)
    by_key = {_key(e): e for e in existing if e.get("detection") != "referenced"}
    # handle -> the entry that now carries it. A declared entry that absorbed
    # a literal is reachable by that literal's handle too, so a later sighting
    # of the same ARN lands on the declaration.
    by_handle = {e.get("address"): e for e in existing
                 if e.get("detection") == "referenced" and e.get("address")}
    by_guess = {e.get("address"): e for e in existing
                if e.get("detection") == "inferred"}
    for entry in entries:
        if entry.get("detection") == "inferred":
            # A guess never stands beside a fact about the same name: the
            # harvest checks this before building one, and a later merge that
            # brings the declaration drops the guess the same way.
            if answers_a_guess(entry):
                continue
            current = by_guess.get(entry.get("address"))
            if current is None:
                by_guess[entry.get("address")] = entry
                existing.append(entry)
                index(entry)
            else:
                _fold(current, entry)
            continue
        if entry.get("detection") == "referenced":
            current = by_handle.get(entry.get("address"))
            if current is None:
                twins = _twins(candidates(entry), entry, "declared")
                if len(twins) == 1:
                    current = twins[0]
                else:
                    # The same resource under another spelling of its ARN —
                    # a wildcard region beside a literal one — is one entry,
                    # whether or not the name also has declared twins.
                    siblings = _twins(candidates(entry), entry, "referenced")
                    if len(siblings) == 1:
                        current = siblings[0]
                    elif len(twins) > 1:
                        # Something here DOES declare this name — twice — so
                        # the referenced-only note would be false beside it.
                        entry["notes"] = [n for n in entry.get("notes") or []
                                          if n != REFERENCED_NOTE]
                        entry.setdefault("notes", []).append(
                            f"{len(twins)}{TWINS_NOTE_MARK}"
                            + ", ".join(sorted(os.path.dirname(
                                (t.get("evidence") or [""])[0]) or "." for t in twins))
                            + "); which of them this ARN names is not knowable "
                            "from the files, so it is recorded separately")
            if current is None:
                if entry.get("service") == "secretsmanager":
                    identifier = entry.get("identifier") or ""
                    # A console form refused as a twin because the entry
                    # under its bare name already absorbed ANOTHER console
                    # form: said, in the same voice as every other refusal
                    # here, and `_note_shared_spellings` flags the pair.
                    bare = _strip_secret_suffix(identifier)
                    holder = next(
                        (e for e in by_name.get(("secretsmanager", bare)) or ()
                         if e is not entry and e.get("identifier") == bare
                         and e.get("console_form")
                         and _another_console_form(e, entry)),
                        None) if bare else None
                    # `acme/db-reader` beside a declared `acme/db`: the suffix
                    # is a word, so it was not stripped and this stands as
                    # its own secret — said, since the console form of the
                    # same secret would have merged.
                    loose = _SECRET_SUFFIX_RE.sub("", identifier)
                    # Through the index, not a walk of the section: every
                    # console-form ARN reaches this branch, so a generated
                    # policy naming thousands of secrets made the merge
                    # quadratic again.
                    if holder is not None:
                        # Both sides, as `_note_shared_spellings` marks both:
                        # the two ARNs do NOT agree (two suffixes), so that
                        # pass will not flag them, yet which console form is
                        # the declaration's is exactly what the files cannot
                        # settle — `spelling_is_ambiguous` reads this prefix.
                        entry.setdefault("notes", []).append(
                            f"{SECRET_OTHER_FORM_NOTE_PREFIX}'{bare}', which already "
                            f"folded in '{holder['console_form']}'. A secret has one "
                            "suffix, so this is a second secret, or the files cannot "
                            "say which of the two the declaration is — recorded "
                            "separately, and a correction or a reported migration "
                            "against one is never taken for the other")
                        held = (f"{SECRET_OTHER_FORM_NOTE_PREFIX}'{identifier}' is recorded "
                                f"beside it: this entry folded in '{holder['console_form']}', "
                                "a secret has one suffix, and which of the two is this "
                                "one the files cannot say — a correction or a reported "
                                "migration against one is never taken for the other")
                        if held not in holder.setdefault("notes", []):
                            holder["notes"].append(held)
                    elif loose != identifier and any(
                            e.get("identifier") == loose and e is not entry
                            for e in by_name.get(("secretsmanager", loose)) or ()):
                        entry.setdefault("notes", []).append(
                            f"{SECRET_SUFFIX_NOTE_PREFIX}'{loose}' recorded beside it, "
                            "or a different secret whose name ends in a word — the "
                            "suffix does not look like the random one AWS appends, "
                            "so the two are kept apart; if they are one secret, say so "
                            "at the review — attach its consumers to the entry you keep "
                            "and annotate the other, since the review has no merge tool")
                if entry.get("endpoint") and not any(
                        n.startswith((AMBIGUOUS_ENDPOINT_NOTE_PREFIX,
                                      AMBIGUOUS_ACCOUNT_NOTE_PREFIX))
                        for n in entry.get("notes") or []):
                    # Refused as a twin because an entry of this name already
                    # holds a DIFFERENT endpoint: said, so the section does
                    # not read as one database recorded twice by accident.
                    # (Two hosts met in one merge are already answered by
                    # `_note_ambiguous_endpoints`, on both sides.)
                    holder = next(
                        (e for e in candidates(entry)
                         if e is not entry and _same_service(e, entry)
                         and _same_identifier(e, entry) and _another_endpoint(e, entry)),
                        None)
                    if holder is not None:
                        entry.setdefault("notes", []).append(
                            f"{OTHER_ENDPOINT_NOTE_PREFIX}'{holder['endpoint']}' "
                            f"({holder.get('address')}). Two hostnames are two "
                            "resources — a dev and a prod database sharing a name — "
                            "so this one stands apart; a correction or a reported "
                            "migration against one is never taken for the other")
                by_handle[entry.get("address")] = entry
                drop_answered_guesses(entry)
                existing.append(entry)
                index(entry)
                continue
            _fold(current, entry)
            _note_unverified_fold(current, entry, regions_verified)
            by_handle[entry.get("address")] = current
            continue
        key = _key(entry)
        current = by_key.get(key)
        if current is None:
            by_key[key] = entry
            existing.append(entry)
            index(entry)
            drop_answered_guesses(entry)
            # A referenced entry an earlier merge left standing is this
            # block's ARN twin; it folds onto the declaration now that one
            # exists, so the section never holds one resource twice.
            twins = _twins(candidates(entry), entry, "referenced")
            if len(twins) == 1 and len(_twins(candidates(twins[0]), twins[0], "declared")) == 1:
                _fold(entry, twins[0])
                _note_unverified_fold(entry, twins[0], regions_verified)
                existing.remove(twins[0])
                unindex(twins[0])
                # Every handle the referenced entry held — its ARN and, for
                # a queue also seen by URL, that endpoint — now reaches the
                # declaration.
                for handle, holder in list(by_handle.items()):
                    if holder is twins[0]:
                        by_handle[handle] = entry
            continue
        _fold(current, entry)
    _note_shared_spellings(existing)


SHARED_SPELLING_NOTE_PREFIX = (
    "its ARN spelling also matches another entry in this section: ")


def _note_shared_spellings(existing: list) -> None:
    """Marks every entry whose ARN agrees with another entry's.

    The guards that stop a correction or a migration outcome crossing between
    two resources ask `spelling_is_ambiguous`, and that has to cover exactly
    the relation `arn_spellings_agree` matches on — no less. The
    ambiguous-ACCOUNT note does not: it is written by grouping on `(service,
    identifier)`, and the secret tolerance deliberately matches ACROSS
    identifiers, the bare policy form against the console form with AWS's
    suffix. So three secrets standing apart in two accounts carried no flag,
    one `mark_data_service_migrated` on the bare spelling reported all three
    migrated, and one `annotate` on it deleted a customer's `keep-in-aws` on
    another. Computed here, from the finished section, against the same
    predicate the guards use.

    Bucketed on `name_keys` UNION `_arn_name_keys`, so that the buckets cover
    what `arn_spellings_agree` can match — the secret tolerance is in there
    too — and this stays linear in the ordinary case instead of comparing
    every entry with every other. `name_keys` alone is not a superset: it is
    computed from the identifier, the guards compare the name inside the ARN,
    and a fold is exactly where those two diverge. A declared `acme/db` that
    folded in `secret:acme/db-Prod01` and a referenced
    `secret:acme/db-Prod01-Xy7Zq2` then never met, neither was flagged, and
    one `mark_data_service_migrated` against the referenced secret released
    the ship gate for the declared one — the failure this pass exists to stop.
    """
    buckets = {}
    for entry in existing:
        if not entry.get("arn"):
            continue
        if any(n.startswith(REPLICA_NOTE_PREFIX) for n in entry.get("notes") or []):
            # The estate's own replica is answered, not ambiguous: a
            # region-unstated spelling of the primary agrees with it by
            # `arn_spellings_agree`, and flagging the pair put a false flag
            # on the estate's own secret and refused it the spelling
            # tolerance.
            continue
        for key in name_keys(entry) | _arn_name_keys(entry):
            buckets.setdefault(key, []).append(entry)
    for with_arns in buckets.values():
        for index, entry in enumerate(with_arns):
            for other in with_arns[index + 1:]:
                if entry is other or not arn_spellings_agree(entry["arn"],
                                                             other["arn"]):
                    continue
                for one, two in ((entry, other), (other, entry)):
                    note = (f"{SHARED_SPELLING_NOTE_PREFIX}'{two['arn']}'. The "
                            "two are recorded separately because the files do "
                            "not settle whether they are one resource, so a "
                            "correction or a reported migration against one is "
                            "never taken for the other")
                    if note not in one.setdefault("notes", []):
                        one["notes"].append(note)


def is_replica(entry: dict) -> bool:
    """A referenced entry the scan answered as the estate's own replica."""
    return any(n.startswith(REPLICA_NOTE_PREFIX) for n in entry.get("notes") or [])


REPLICA_TWINS_NOTE_PREFIX = "its consumers stay here: "


def carry_replica_consumers(section: list) -> dict:
    """Copies each replica entry's consumers onto its primary declaration and
    returns what it added, `{id(primary): [copies]}`.

    Consumers attach by address before the merge and a replica never folds,
    so a role granted only the eu-west-1 ARN was attributed to the `rebuild`
    replica and the `migrate` primary never learned of it — the workload
    shipped with its secret unmoved, the case the whole section exists to
    hold. The withdrawn fold carried this implicitly; the answered shape does
    it here, with a note saying which ARN the workload came through and a
    `via_replica` handle pointing back, and leaves the replica's own list in
    place so it is not "unattributed".

    Runs inside `overrides.rebuild`, the one producer of the corrected
    section, BEFORE the corrections replay, and the copies are registered as
    DERIVED consumers of the primary. That is what makes a correction about
    a carried consumer behave like one about any other derived link: a
    rejection on the primary removes it under every rule `apply` has —
    directory, alias, kind, the stamps — and an attach on the primary
    restores the derived record rather than minting a `human_review` one.
    Keying the carry on the records' raw addresses instead re-implemented
    placement with a weaker rule, and honoured a dev-only rejection on prod.
    `reconcile_carried` then runs after the replay, so a rejection recorded
    on the REPLICA — the entry the chain derived the consumer from — also
    withdraws the copy. Run only in the harvest, the copies vanished from
    every section a later correction rebuilt.

    Exactly ONE declaration may receive the copy. Two root modules declaring
    the same replicated name are twins, and the fold refuses to choose
    between them; the carry chose by walk order and landed prod's grant on
    dev's secret. It now carries to neither and says so on the replica.
    """
    added = {}
    declared = {}
    for entry in section:
        if entry.get("detection") == "declared":
            declared.setdefault((entry.get("service"), entry.get("identifier")), []).append(entry)
    for entry in section:
        if not is_replica(entry):
            continue
        primaries = []
        for _family, name in name_keys(entry):
            for candidate in declared.get((entry.get("service"), name), ()):
                if candidate not in primaries:
                    primaries.append(candidate)
        if not primaries:
            continue
        if len(primaries) > 1:
            note = (f"{REPLICA_TWINS_NOTE_PREFIX}{len(primaries)} declarations share the "
                    "primary's name ("
                    + ", ".join(sorted(os.path.dirname((p.get("evidence") or [""])[0]) or "."
                                       for p in primaries))
                    + "); the scan does not choose between them — attach the workload "
                    "to the right one at the review")
            if note not in entry.setdefault("notes", []):
                entry["notes"].append(note)
            continue
        primary = primaries[0]
        have = {(c.get("workload"), c.get("kind"), c.get("namespace"))
                for c in primary.get("consumers") or []}
        for consumer in entry.get("consumers") or []:
            key = (consumer.get("workload"), consumer.get("kind"), consumer.get("namespace"))
            if key in have:
                continue
            have.add(key)
            copy = dict(consumer,
                        via_replica=entry.get("address"),
                        note=(f"reaches it through the {entry.get('region')} replica's "
                              f"ARN ({entry.get('arn')})"))
            primary.setdefault("consumers", []).append(copy)
            added.setdefault(id(primary), []).append(copy)
    return added


def reconcile_carried(section: list) -> None:
    """After the corrections replay: drops a carried consumer whose source is
    no longer on its replica entry — the reviewer rejected it there, which is
    the entry the chain derived it from — so one rejection withdraws both
    copies rather than leaving the primary listing a workload the replica
    says has no consumer by decision."""
    by_address = {e.get("address"): e for e in section if e.get("address")}
    for entry in section:
        kept = []
        for consumer in entry.get("consumers") or []:
            source = by_address.get(consumer.get("via_replica")) if consumer.get("via_replica") else None
            if source is not None and not any(
                    (c.get("workload"), c.get("kind"), c.get("namespace"))
                    == (consumer.get("workload"), consumer.get("kind"), consumer.get("namespace"))
                    for c in source.get("consumers") or []):
                continue
            kept.append(consumer)
        if len(kept) != len(entry.get("consumers") or []):
            entry["consumers"] = kept


class HarvestResult(NamedTuple):
    """What a harvest produced, and what the review needs to reproduce it.

    The last three fields exist so the review can rebuild the section the same
    way this function does rather than by mutating it in place — see
    `overrides.rebuild` and DESIGN.md issue 31. `scanned` is the section as the
    Terraform alone describes it, BEFORE any human correction is replayed over
    it; `truncated` and `excluded` are what `note_unattributed` needs to say
    "unknown" rather than "nothing" about an entry with no consumer.
    """
    notes: list
    workloads: list
    truncated: list
    excluded: list
    scanned: list
    # Candidate hints for the review: a workload whose configuration states a
    # recorded entry's name exactly, with the reason. Offered first in the
    # candidate list, never attached (inferred.py).
    hints: list


def harvest_datastores(inventory: dict, root_dir: str, scope: dict = None,
                       overrides: dict = None) -> HarvestResult:
    """Populates inventory["data_dependencies"] from root_dir.

    The single entry point the scan action calls. Declared and referenced
    entries come out of one walk; the join sees both — a declared entry is
    reached by address, a referenced one by its ARN — and merge_datastores
    then reconciles the two.

    Consumers are attached before the merge, not after, so each entry is
    matched on the address of the block it actually came from. Merging first
    would fold two blocks that share a service and identifier within one
    directory into a single entry holding only the first one's address, and
    the second block's consumers would then find nothing to attach to.

    `overrides` is the human review's durable corrections (overrides.py). They
    are replayed after the merge — the addresses the reviewer acted on are the
    merged section's — and before the unattributed notes, so an attached
    consumer clears "nothing references this" and a rejection that empties an
    entry earns it.
    """
    # Deferred: consumers.py reads this module's lexer, so importing it at
    # module level is a cycle. The honest fix is to lift the lexer into its own
    # module that both import — recorded as DESIGN.md issue 28 rather than done
    # here, because moving it while CL 1 is still in review would rebase badly.
    from . import consumers, inferred, overrides as overrides_lib

    # The YAML half first: chart values and manifests. Literals found there
    # are referenced entries like any other — handed to the Terraform walk so
    # they get the same account and region verdicts — with the file's
    # workload already attached; its key/value pairs join the Terraform ones
    # for typing below.
    yaml_scan = inferred.scan_yaml(root_dir, scope)
    declared, referenced, notes, regions_verified = extract_datastores(
        root_dir, scope, yaml_scan.referenced)
    notes.extend(yaml_scan.notes)
    # Declared first: a referenced entry folds onto the declared twin that
    # precedes it, and the join must have run over both before that happens.
    entries = declared + referenced
    consumer_notes, truncated, excluded, workloads, pairs = \
        consumers.attach_consumers(entries, root_dir, scope)
    notes.extend(consumer_notes)
    # A chart's values name the chart (`helm_chart`, by its Chart.yaml name and
    # directory); the Terraform release that deploys that directory is the
    # same workload under its own name and kind. Unified onto the release —
    # holders, the pairs' copies of them, and the consumers already placed on
    # the YAML-found entries — or the pool held `orders` twice, the hint said
    # `helm_chart` under an entry already listing `orders (helm_release)`, and
    # a hand attach of the release lost the scan's enrichment to the tie.
    notes.extend(inferred.unify_chart_holders(yaml_scan, workloads))
    pairs = pairs + yaml_scan.pairs
    seen = {(w.get("workload"), w.get("kind"), w.get("namespace"),
             w.get("source_path")) for w in workloads}
    for holder in yaml_scan.holders:
        key = (holder.get("workload"), holder.get("kind"), holder.get("namespace"),
               holder.get("source_path"))
        if key not in seen:
            seen.add(key)
            workloads.append(holder)
    # Guesses last, against everything recorded so far, so a bare name that
    # matches a declared or referenced entry becomes a hint rather than a
    # duplicate; the merge drops any guess a fact answers.
    guesses, hints, guess_notes = inferred.guess_entries(pairs, entries)
    entries.extend(guesses)
    notes.extend(guess_notes)
    merge_datastores(inventory, entries, regions_verified)
    # Replica consumers are carried onto the primary by `overrides.rebuild`,
    # after the corrections replay — see `carry_replica_consumers`.
    # Counted AFTER the merge, from the entries that survived it. A referenced
    # entry that folded onto its declaration is declared, and a note naming it
    # as undeclared would contradict the summary in the same response. The
    # estate's own replicas are answered, not out-of-band, for the same reason.
    surviving = [e for e in inventory.get("data_dependencies") or []
                 if e.get("detection") == "referenced" and not is_replica(e)]
    if surviving:
        notes.append(
            f"{len(surviving)} data service(s) are known only from a literal "
            "ARN or endpoint and were not matched to a declaration in the "
            "scanned Terraform — provisioned outside this repository, declared "
            "here under a variable-built name, or one of several same-named "
            "resources the literals cannot tell apart: "
            + _listed(f"{e['service']} {e['identifier']}" for e in surviving)
        )
    # "Unknown" is not "same": the files never say which account is the
    # estate's own, so the comparison cannot be made. Each such entry carries
    # the note — a folded one included, since the fold is exactly when an ARN
    # might name a same-named resource elsewhere — and the scan says so once.
    unknown_account = [e for e in inventory.get("data_dependencies") or []
                       if any(n.startswith(UNKNOWN_ACCOUNT_NOTE_PREFIX)
                              for n in e.get("notes") or [])]
    if unknown_account:
        notes.append(
            f"{len(unknown_account)} data service(s) are named by an ARN that "
            "carries an AWS account the scanned Terraform does not state as "
            "its own — and it does not state its own account(s) completely — "
            "so whether they are cross-account is not knowable from the files: "
            + _listed(f"{e['service']} {e['identifier']} ({e['account']})"
                      for e in unknown_account)
        )
    # Kept before the corrections go on: this is what the Terraform says on its
    # own, and the review rebuilds from it rather than editing the corrected
    # section in place. Deep-copied because the replay mutates entries.
    scanned = copy.deepcopy(inventory.get("data_dependencies") or [])
    # Through `rebuild`, not by calling apply and note_unattributed in
    # sequence here: the review builds its section with that one function, and
    # a second copy of the sequence is a second thing to keep in step. The
    # counting still happens after the merge and after the corrections —
    # `truncated` keeps an entry in a file that ran out mid-read from claiming
    # nothing references it, and `excluded` does the same for one whose
    # consumer may sit in a file the scope kept out.
    inventory["data_dependencies"], replay_notes = overrides_lib.rebuild(
        scanned, overrides, truncated, excluded)
    notes.extend(replay_notes)
    notes.append(
        "data_dependencies covers Terraform .tf files only — .tf.json, "
        "CloudFormation, Crossplane, CDK and eksctl are not parsed, so an empty "
        "result for one of those means not scanned, not absent."
    )
    # Persist them, not just return them. These notes say what was NOT scanned,
    # and an empty section with no durable record of why is indistinguishable
    # from an estate that genuinely has no data services — the exact confusion
    # this scan exists to prevent. The tool response is transient; the ledger
    # is what the assessment and every later reader see.
    scan_notes = inventory.setdefault("data_dependency_scan_notes", [])
    for note in notes:
        if note not in scan_notes:
            scan_notes.append(note)
    return HarvestResult(notes, workloads, sorted(truncated),
                         sorted(excluded), scanned, hints)
