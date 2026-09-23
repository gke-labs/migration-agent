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

"""Which procedure a data service moves by, and the template for it.

Pure apart from reading this package's own `runbooks/` directory, which ships
with the server and is therefore not I/O the caller has to be able to fail.

WHY A TEMPLATE AND NOT A GENERATED SCRIPT. The image replication runbook emits
runnable `skopeo copy` lines because the server knows both ends of that copy. A
database move has three things the server does not hold — the source host, the
credentials, and the maintenance window — so a generated
`gcloud database-migration` line would carry an invented hostname, which is
worse than no line at all (`datamigration.runbook`'s docstring argues the same
point for the worklist). What the server CAN do is hand over the right
procedure with the estate's own facts beside it and let the agent fill the rest
in with the operator. That division is the whole design: the file is fixed and
reviewed, the substitution is per estate, and the placeholders are the seam.

WHY A DIRECTORY AND NOT MORE SECTIONS IN THE KNOWLEDGE DOCUMENT. `data-
migration.md` is read start-to-finish by an agent deciding what to say; a
runbook is read by an operator executing one move, days later, with a terminal
open. Merging them means every reader pays for every service, and the document
that gets rendered per estate would be the one nobody reviewed. The knowledge
document keeps the arguments — what gates, why an empty cache is a working
cache, what keeping a service in AWS costs — and sends the reader here for the
steps.
"""

import os
import re

# One procedure per file, shipped beside this module.
RUNBOOK_DIR = os.path.join(os.path.dirname(__file__), "runbooks")

# Where a rendered runbook lands in the ledger. One per data service, beside
# the worklist the step already writes.
RENDERED_PREFIX = "platform/deployment/runbooks/"

# What the agent has to replace before a rendered runbook is worth keeping.
# `<AR_DESTINATION>` in the replication runbook set this convention and
# `mark_replication_complete` refuses over it for the same reason: a stand-in
# left in a document an operator runs from is a command that fails at best,
# and at worst one that succeeds against the wrong thing.
PLACEHOLDER_RE = re.compile(r"<[A-Z][A-Z0-9_]{2,}>")

# What to print when asking for an engine: the canonical tokens, not the
# prose names. `_normalised_engine` accepts "SQL Server" too, but a printed
# call is copied verbatim and the tokens are what the table is keyed on.
ENGINE_CHOICES = ("postgres", "mysql", "mariadb", "sqlserver")

# The RDS engines Cloud SQL runs natively, normalised from what a Terraform
# declaration states. `engine` is free text on the AWS side: "postgres",
# "sqlserver-se", "sqlserver-ex", "mysql", "mariadb". Aurora engines
# ("aurora-postgresql", "aurora-mysql") do not appear here — an entry carrying
# one is graded `aurora`, which is an escalation, EXCEPT for the Multi-AZ DB
# cluster the harvester refines back to `rds` (datastores._refine_service),
# whose engine is a plain one and lands on these prefixes.
_RDS_ENGINES = (
    ("postgres", "rds-postgres"),
    ("mysql", "rds-mysql"),
    ("mariadb", "rds-mariadb"),
    ("sqlserver", "rds-sqlserver"),
)

# Services with one procedure regardless of what they carry.
_BY_SERVICE = {
    "memorydb": "memorydb",
    "elasticache": "elasticache-persistent",
    "s3": "s3",
    "efs": "efs",
    "fsx": "fsx",
    "secretsmanager": "secretsmanager",
    "ssm": "ssm",
}

# Why there is no runbook, per service, when the answer is a decision rather
# than a missing file. Said out loud at the point of asking: an operator who
# asks for help with a DynamoDB table has to hear "this is a re-platform"
# rather than "not found", which reads as a gap in the tool.
NO_RUNBOOK_REASON = {
    "aurora": ("Aurora has no engine-compatible target. Aurora PostgreSQL to "
               "AlloyDB and Aurora MySQL to Cloud SQL are engine changes, "
               "which this repository escalates by policy rather than "
               "planning."),
    "docdb": ("DocumentDB moves to Firestore or to MongoDB on GCP, and which "
              "one depends on whether the application needs Mongo wire "
              "compatibility. That is a design decision, not a procedure."),
    "neptune": ("Neptune has no GCP equivalent. The answer is a decision "
                "about the graph workload, not a copy."),
    "dynamodb": ("DynamoDB to Bigtable, Spanner or Firestore is a re-platform "
                 "driven by the access pattern. Keeping it in AWS and "
                 "reaching it across the boundary is frequently the right "
                 "answer."),
    "redshift": ("Redshift to BigQuery changes the model rather than moving "
                 "rows. It is a project of its own."),
}

# The queue-shaped `replatform` grades share one answer, so they share one
# sentence rather than several copies of it. NOT only reachable via a human
# upgrade: `get_data_migration_runbook` resolves any entry in the section and
# answers for non-`migrate` grades on purpose (DESIGN §7's tool row), so this
# text is operator-facing whatever the grade — which is why the OpenSearch
# case above had to come out of it.
_REPLATFORM_REASON = (
    "the shape changes rather than the bytes moving — producers and consumers "
    "are cut over to the GCP service (Pub/Sub, Managed Service for Kafka, "
    "Cloud Logging) and in-flight messages are the cutover's cost. There is "
    "no copy to run, so there is no runbook to render.")
# OpenSearch is NOT one of the queue-shaped replatforms, and sharing their
# sentence told an operator there was no copy to run over a document store
# holding an indexed corpus. It has no GCP-native managed equivalent, so it is
# still a replatform — but the data is real and moving it is a project.
NO_RUNBOOK_REASON["opensearch"] = (
    "`opensearch` has no GCP-native managed equivalent: the realistic targets "
    "are Elastic Cloud on GCP or a self-hosted cluster on GKE, and which one "
    "is a decision before it is a procedure. Unlike the queue-shaped "
    "replatforms, there IS data to move — a snapshot-and-restore or a "
    "re-index from the source of truth — so do not cut over on the assumption "
    "that an empty target is a working one.")

for _service in ("kinesis", "firehose", "msk", "mq", "sqs", "sns",
                 "eventbridge"):
    NO_RUNBOOK_REASON[_service] = (f"`{_service}` is a replatform: "
                                   + _REPLATFORM_REASON)
del _service


def _normalised_engine(engine) -> str:
    """The engine as a comparison key: lowercased, punctuation and spaces gone.

    `engine` arrives from two places that spell it differently. A Terraform
    declaration says `sqlserver-se`; an operator answering the question the
    tool asks says "SQL Server", because that is what the listing offered them.
    Matching on the raw string sent the fourth of the four offered spellings
    down the engine-change escalation — with `rds-sqlserver.md` sitting in the
    directory — so the key drops everything that is not alphanumeric.

    Safe against the Aurora engines it must NOT absorb: `aurora-postgresql`
    normalises to `aurorapostgresql`, which starts with neither `postgres` nor
    `mysql`, so an Aurora entry still reaches its escalation.
    """
    return re.sub(r"[^a-z0-9]", "", (engine or "").strip().lower())


def name_for(entry: dict):
    """The runbook file for this entry, or None.

    None is an answer, not a failure: `escalate` services have no procedure by
    design, and a service nobody has written one for yet must refuse rather
    than be handed the nearest-looking file. Nothing here falls back.
    """
    service = entry.get("service")
    if service == "rds":
        engine = _normalised_engine(entry.get("engine"))
        for prefix, name in _RDS_ENGINES:
            if engine.startswith(prefix):
                return name
        # An engine the declaration did not state is the ordinary case for a
        # module-sourced RDS, and PostgreSQL is the majority — but guessing
        # would hand an operator a `pglogical` procedure for an Oracle
        # instance. The tool asks instead.
        return None
    return _BY_SERVICE.get(service)


# How the move actually goes, where the service-level answer is wrong. Keyed
# on the runbook, because the runbook is what knows: `datamigration.TARGETS`
# is keyed on the service, and "Database Migration Service, continuous" is
# false for two of the four RDS engines. The label is not decoration — the
# knowledge document defines "continuous" as "stop writes, wait for lag to
# reach zero, minutes", and an operator sizing a MariaDB window from that
# plans for minutes when they need hours with the writers stopped.
_HOW_BY_RUNBOOK = {
    "rds-mariadb": ("Cloud SQL for MySQL",
                    "logical dump and import — NO continuous path, so the "
                    "window covers dump, transfer and import with writers "
                    "stopped throughout"),
    "rds-sqlserver": ("Cloud SQL for SQL Server",
                      "Database Migration Service, continuous, seeded from "
                      "backups in Cloud Storage"),
}


def target_for(entry: dict, service_default: tuple) -> tuple:
    """(target, how) for this entry, refined past the service-level default.

    `service_default` is what `datamigration.TARGETS` says. Everything except
    RDS is answered there; RDS is four procedures with two cutover classes
    between them, and the service-level row can only state one.
    """
    if needs_engine(entry):
        # Not the service default. `TARGETS["rds"]` says "Database Migration
        # Service, continuous", and for the MariaDB half of the unknown that
        # is the exact claim `_HOW_BY_RUNBOOK` exists to stop — the operator
        # sizes minutes and needs hours with the writers stopped. Until the
        # engine is known, the honest announcement is that it is not known.
        return ("Cloud SQL",
                "depends on the engine, which the declaration does not state: "
                "continuous through Database Migration Service for "
                "PostgreSQL, MySQL and SQL Server, and a dump-and-import "
                "window with no continuous path for MariaDB")
    return _HOW_BY_RUNBOOK.get(name_for(entry) or "", service_default)


def needs_engine(entry: dict) -> bool:
    """True for an RDS entry whose declaration states no engine — the
    `engine = var.db_engine` case, which `name_for`'s comment calls the
    ordinary one for a module-sourced database.

    It exists so the two surfaces can tell this apart from a service that has
    no procedure at all. Both print "no runbook" without it, and the operator
    reading the worklist three weeks later is told their PostgreSQL database is
    a dead end when one question would produce the procedure.
    """
    return (entry.get("service") == "rds"
            and not _normalised_engine(entry.get("engine")))


def unsupported_engine(entry: dict):
    """The declared RDS engine Cloud SQL has no counterpart for, or None.

    The third case, and it must not be folded into either neighbour. Asking
    "which engine does this run?" about an instance whose declaration says
    `oracle-se2` wastes the operator's time and reads as the tool not having
    read its own input; calling it "no procedure for this service" hides that
    RDS in general is well covered. What is true is narrower: this is an engine
    change, which the repository escalates by policy.
    """
    declared = (entry.get("engine") or "").strip()
    if (entry.get("service") == "rds" and _normalised_engine(declared)
            and name_for(entry) is None):
        # The DECLARED spelling, not the match key: an operator reading
        # "Cloud SQL has no `oraclese2` engine" has to translate it back to
        # what their Terraform says before the sentence means anything.
        return declared
    return None


def load(name: str) -> str:
    """The template text. Raises if the file is missing — that is a packaging
    error rather than an estate condition, and swallowing it would present an
    empty procedure as a complete one."""
    path = os.path.join(RUNBOOK_DIR, f"{name}.md")
    with open(path, encoding="utf-8") as handle:
        return handle.read()


def unresolved(text: str) -> list:
    """Placeholders still in a rendered runbook, deduplicated and sorted."""
    return sorted(set(PLACEHOLDER_RE.findall(text or "")))


def slug(value: str) -> str:
    """A blob-name-safe form of a name the customer chose.

    Identifiers come from Terraform and are already tame, but `bucket` and
    `name` arguments accept characters a path should not carry, and an entry
    whose identifier fell back to the block address carries a dot. Anything
    outside the safe set collapses to a single hyphen.
    """
    cleaned = re.sub(r"[^a-z0-9]+", "-", (value or "").strip().lower())
    return cleaned.strip("-") or "unnamed"


def rendered_name(entry: dict) -> str:
    """The ledger object a rendered runbook for this entry lands on.

    Carries all three axes `datamigration.key_of` identifies an entry by —
    directory, identifier and address — because anything coarser puts two
    entries on one file. The directory separates the dev/prod pair `_target`
    refuses to guess between; the address separates two blocks one root module
    declares under the same name (`aws_db_instance.orders` and
    `module.db.aws_db_instance.this`, both identified as `orders-db`), which
    the identifier alone does not. The result is long, and that is the right
    trade for an artifact whose whole job is to be the runbook for exactly one
    database.

    Not absolute: `slug` is lossy, so two entries differing only in
    punctuation still land on one name. The overwrite refusal in
    `save_data_migration_runbook` is what catches that, and it is one reason
    that refusal exists rather than a last-write-wins.
    """
    # `directory_of`, not `entry_directory`: an entry known only from its ARN
    # has no declaring root module, and its `evidence[0]` is whichever file
    # the walk met first — a name built from it would change between scans,
    # and the overwrite refusal below would stop finding the file it guards.
    # Imported here for the same reason `datamigration` imports this module
    # lazily: each reads the other.
    from .datamigration import directory_of
    directory = directory_of(entry)
    stem = (f"{entry.get('service') or 'data'}-"
            + slug(entry.get("identifier") or entry.get("address"))
            + "--" + slug(entry.get("address") or entry.get("identifier")))
    return f"{slug(directory)}--{stem}.md" if directory else f"{stem}.md"


def rendered_blob(entry: dict) -> str:
    return RENDERED_PREFIX + rendered_name(entry)


def rendered_blobs(entry: dict, records=()) -> list:
    """Every ledger object an adapted runbook for this entry may sit on: the
    one its current handle names first, then every name an earlier handle
    gave it.

    An entry's `address` moves across a fold — a scope amendment brings the
    declaration in and `arn:aws:s3:::acme-logs` becomes `aws_s3_bucket.logs`
    in `envs/prod` — and a name keyed on it moves with it, so the runbook an
    operator adapted was suddenly "not yet adapted", the listing stopped
    naming it, and the overwrite refusal stopped guarding it. The same for a
    respelling (a wildcard ARN joined by a fully qualified sighting), a
    renamed declaring directory, and a secret whose console form was the
    handle before it folded onto the bare name. Readers look under every
    handle, as `datamigration.record_matches` does for outcomes — and the
    outcome `records` matched to this entry are where the earlier spellings
    and directories survive, so each one contributes a candidate; writers
    write under the first and refuse while any other exists.
    """
    # Deferred, as `directory_of` is above: the discovery package imports the
    # deployment one for the runbook catalogue.
    from servers.phases.discovery.discovery_init_1.datastores import is_literal_handle

    def named(address, identifier, directory):
        if not address:
            return None
        pseudo = dict(entry, address=address, identifier=identifier or entry.get("identifier"),
                      detection="referenced" if is_literal_handle(address) else "declared",
                      evidence=[f"{directory}/x.tf" if directory else "x.tf"])
        return rendered_blob(pseudo)

    paths = [rendered_blob(entry)]
    arn = entry.get("arn")
    if arn and arn != entry.get("address"):
        paths.append(named(arn, entry.get("identifier"), ""))
    # The endpoint-era name too: a queue adapted while its URL was the handle
    # takes the ARN as handle once a policy names it.
    endpoint = entry.get("endpoint")
    if endpoint and endpoint != entry.get("address"):
        paths.append(named(endpoint, entry.get("identifier"), ""))
    console = entry.get("console_form")
    if arn and console and console != entry.get("identifier"):
        head, separator, _rest = str(arn).rpartition(":secret:")
        if separator:
            paths.append(named(f"{head}{separator}{console}", console, ""))
    for record in records or ():
        paths.append(named(record.get("address"), record.get("identifier"),
                           record.get("directory") or ""))
        for alias in record.get("aliases") or ():
            if is_literal_handle(alias):
                paths.append(named(alias, record.get("identifier"), ""))
    seen, ordered = set(), []
    for path in paths:
        if path and path not in seen:
            seen.add(path)
            ordered.append(path)
    return ordered


def earlier_copies(entry: dict, existing, records=()) -> list:
    """The adapted runbooks that exist under an EARLIER handle of this entry
    — the ones a save under the current name would write around."""
    return [p for p in rendered_blobs(entry, records)[1:] if p in existing]


def supersede_earlier(earlier: list, delete, not_found=()) -> tuple:
    """Removes the earlier copies a replace supersedes, one `delete(path)`
    each. Returns (removed, left_behind): a path already gone counts as
    removed, any other failure leaves it behind — and the caller says so,
    because two files is a state the operator has to know about. Pure apart
    from `delete`, so the tool's behaviour under a failing bucket is testable
    without one."""
    removed, left_behind = [], []
    for path in earlier:
        try:
            delete(path)
            removed.append(path)
        except not_found:
            removed.append(path)
        except Exception:
            left_behind.append(path)
    return removed, left_behind


# Entry fields worth putting in front of the agent before it adapts a
# template, in the order a reader wants them. Only what the scan recorded
# LITERALLY — a null here means the declaration used a variable, and the
# difference between "not stated" and "not applicable" is the operator's to
# resolve. `datastores._ENGINE_ARGS` and its siblings are where these get onto
# the entry.
#
# `region` and `arn` are populated by the literal-ARN harvest for an entry a
# policy names by ARN (and for a declaration such an ARN folded onto); they
# stay null for a declaration nothing names that way, since its ARN is only
# known after apply. `facts()` skips nulls either way.
FACT_FIELDS = (
    ("engine", "engine"),
    ("engine_version", "engine version"),
    ("allocated_storage", "declared size (GiB)"),
    ("storage_type", "storage type"),
    ("multi_az", "multi-AZ"),
    ("region", "region"),
    ("arn", "ARN"),
)


def facts(entry: dict) -> list:
    """(label, value) for everything the ledger knows about this service.

    What separates a rendered runbook from the template is exactly this list,
    so it is handed over whole rather than summarised. Fields the declaration
    did not state are omitted rather than printed as null: an agent that sees
    `engine version: None` writes it into the document, and a runbook that
    states a version nobody declared is a runbook that lies quietly.
    """
    found = []
    for field, label in FACT_FIELDS:
        value = entry.get(field)
        if value is None or value == "":
            continue
        found.append((label, str(value).strip()))
    return found


def offer(entry: dict, target: str) -> str:
    """The one-line offer, in the operator's terms rather than Terraform's.

    Deliberately names the source service and the target product: "the orders
    database" is what the operator calls it, and "from RDS to Cloud SQL" is
    what tells them whether the offer is worth taking. The engine is included
    when the declaration states one, because it is what decides the procedure
    and an operator who knows the instance is MariaDB should see that the
    agent knows it too.
    """
    # The DECLARED spelling, for `unsupported_engine`'s reason: since the key
    # drops punctuation, printing it asks the operator about `sqlserverse`.
    engine = (entry.get("engine") or "").strip()
    described = entry.get("identifier") or entry.get("address") or "(unnamed)"
    source = (entry.get("service") or "data service").upper()
    return (f"{described} ({source}"
            + (f", {engine}" if engine else "")
            + f") → {target}")
