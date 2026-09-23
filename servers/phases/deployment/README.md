# Deployment Phase

Prepares what the applied estate needs before workloads run on it. The landing zone and
translation phases end at a Pull Request — the user applies the Terraform. This phase is
the server-side follow-through: resources the migration itself needs that are cheap,
idempotent, and safe to create directly, starting with the Artifact Registry repository
the migrated container images land in.

The phase entry is an agent task: after the translation PR opens, the graph parks at
`STATE_DEPLOYMENT_INIT` — translation's story ends at the PR, and the deployment phase
starts (and reports) its own work. Image replication follows the provisioning state in
the same drain.

## Layout

```
servers/phases/deployment/
├── __init__.py                 # register(mcp) — wires every step's tools into the server
├── actions.py                  # provision_artifact_registry + the ACTIONS table
├── replication.py              # replicate_images (skopeo copies / self-service runbook)
│                               #   + mark_self_service_complete (the user-asserted flip)
│                               #   + abandon_copies (the image will not be copied)
├── replication_test.py         # marking semantics + the exports image_map flip
├── datamigration.py            # what the migration still owes: outstanding services,
│                               #   the outcome store, the per-estate runbook
├── datamigration_test.py       # keying, outstanding/settled, the runbook's claims
├── runbooks.py                 # which procedure a service moves by; placeholder checks
├── runbooks_test.py            # selection, refusal reasons, rendered blob naming
├── runbooks/                   # one migration procedure per file — see its README
│   ├── rds-postgres.md  rds-mysql.md  rds-sqlserver.md  rds-mariadb.md
│   ├── memorydb.md  elasticache-persistent.md  s3.md  efs.md  fsx.md
│   └── secretsmanager.md  ssm.md
├── knowledge/
│   └── data-migration.md       # what each move costs and why; the steps live in runbooks/
├── deployment_provision_1/     # STATE_DEPLOYMENT_INIT: provision the image destination
│   ├── instructions.md         # agent instructions returned by get_next_stage
│   └── tools.py                # prepare_image_deployment, mark_replication_complete,
│                               #   abandon_image_replication
└── deployment_datamigration_2/ # STATE_DEPLOYMENT_DATA_MIGRATION: track the data moves
    ├── instructions.md
    └── tools.py                # list_data_migrations, get_data_migration_runbook,
                                #   save_data_migration_runbook,
                                #   mark_data_service_migrated,
                                #   mark_data_service_migrating, complete_data_migration
```

## The provisioning chain

```
STATE_TRANSLATION_SUBMIT_PR
  on_success → STATE_DEPLOYMENT_INIT            AGENT_TASK (the walk parks; the agent
                 │                              calls get_next_stage, then the tool)
                 └─ prepare_image_deployment
                      → STATE_DEPLOYMENT_PROVISION_AR   action: provision_artifact_registry
                           on_success | on_failure
                      → STATE_DEPLOYMENT_IMAGE_REPLICATION   HITL: replicate how?
                           on_reject → STATE_DEPLOYMENT_DATA_MIGRATION   (skip)
                      → STATE_DEPLOYMENT_REPLICATE_IMAGES   action: replicate_images
                           on_success → STATE_DEPLOYMENT_DATA_MIGRATION
                           on_failure → STATE_DEPLOYMENT_INIT        (user fixes, re-runs)

STATE_DEPLOYMENT_DATA_MIGRATION                 AGENT_TASK (the walk parks again)
  complete_data_migration → STATE_DEPLOYMENT_COMPLETED   (terminal)
```

Both image outcomes converge into the data migration step, so declining
replication does not skip it.

## The data migration step

The data services graded `migrate` at the review have to actually move, and nothing
here can move them — the server holds no AWS credentials (§10) and never sees a copy
happen. So the step says what is owed, writes the worklist to
`platform/deployment/data-migration-runbook.md`, and records what the operator reports.
Outcomes live in `platform/deployment/data_migrations.json`.

It also **offers to help**. Each outstanding service that has a procedure behind it gets
an `offer help:` line in the listing, carrying the sentence to say and the call to make;
`get_data_migration_runbook` returns that procedure as a template with the estate's own
facts above it, the agent adapts it with the operator, and `save_data_migration_runbook`
writes the adapted copy to
`platform/deployment/runbooks/[<dir>--]<service>-<name>--<address>.md`. A service
with no procedure gets no offer — `runbooks.NO_RUNBOOK_REASON` says why, and the refusal
quotes it rather than handing over the nearest-looking file.

Decisions worth knowing:

- **The outcome is not written onto the inventory entry**, the way `images[].replication`
  is. The data scan clears and rebuilds `data_dependencies` on every run and
  `amend_discovery_scope` makes a re-scan routine, so a completion fact stored there
  would be destroyed by a scan of a checkout that had not even changed.
- **The graph parks here until nothing is owed**, and that is the point rather than a
  cost. A TERMINAL state declares no step, no instructions and no knowledge, so an
  operator arriving six weeks later to report that a database finally landed would be
  told the migration is complete and offered nothing — the tool would exist, and the
  only thing that could mention it would be behind them. A state is the one mechanism
  here that survives elapsed time and a change of operator. Parking costs nothing: the
  graphs are independent, exports publish from this step, and `mark_replication_complete`
  accepts it too. `complete_data_migration` refuses while a service is unreported, and
  `keep-in-aws` is the exit for one that genuinely cannot move.
- **Nothing is callable at the terminal.** A terminal means the workflow is finished, so
  `mark_replication_complete` moved here too and the step waits for image copies as well as
  databases — otherwise moving the tool would just have moved where a self-service copy gets
  stranded. Both kinds have an exit: `keep-in-aws` for a data service, `abandon_image_replication`
  for an image nobody is going to copy.
- **It is a holding state, not a task.** An `AGENT_TASK` parks the walk and returns
  control, so the operator is free to do anything while a migration runs — including
  asking for help running it, which is what the phase knowledge document is for. The
  instructions tell the agent to report where things stand, act on a completion when one
  is reported, and otherwise get out of the way.
- **`annotate_data_dependency` is reachable from this step**, and only that one. A
  service that turns out not to be movable can be recorded `keep-in-aws` here, which is
  where operators actually discover it. Rejecting and attaching consumers stays sealed
  to the review: that is the mapping, which was signed off and which extraction froze.

The exports publish is keyed on reaching this step, not on the terminal. Those fields
are what an application team needs to plan and translate, they are ready when
provisioning finishes, and waiting for the terminal would hold every developer behind a
database migration.

The `data_gate` slice is the other direction: this step's reports are what RELEASE an
application team. Every call that changes what is owed — each `mark_data_service_*`, the
`keep-in-aws` annotation, and the close — republishes it, because the developer half of
the gate (`servers/phases/workload/datagate`, DESIGN.md §6.4) runs in sessions that hold
no read on `platform/*` and can learn none of this any other way. The close republishes
too, with no liveness guard: unlike the runbook, whose value is that the close freezes
it, this slice is a live fact, and the close is the moment the most ship gates open.

## Image replication

`replicate_images` (replication.py) consumes the inventory's `images[]` entries with
`registry == "ecr"`, in the mode the user chose at the elicitation:

- **`self_service`** (default): writes a runbook of `skopeo copy --preserve-digests`
  commands to `platform/deployment/replication-runbook.sh` in the ledger and marks the
  entries `self_service`. The server never reads from AWS in this mode.
- **`agent_mediated`**: the server runs the copies itself, using only credentials already
  present in the local environment. The preflight verifies the skopeo binary, ECR read
  access (one `skopeo inspect` per distinct registry host) and the destination
  repository's existence — anything missing is reported with the exact command the user
  runs to fix it. The server never runs `aws`, never runs `skopeo login`, never writes an
  auth file; pushes use the server's own ADC access token.

Per-image outcomes are written back to `inventory.images[].replication`. Failures park
the graph back at `STATE_DEPLOYMENT_INIT`: the user fixes their environment (or applies
the PR so the destination exists) and calls `prepare_image_deployment` again.

A `self_service` outcome is a plan, not a copy — nothing downstream treats the image as
moved until the user says so. `mark_replication_complete` (registered by
`deployment_provision_1/tools.py`; no DAG transition, callable at
`STATE_DEPLOYMENT_DATA_MIGRATION` — not the terminal, which accepts nothing)
is the completion path: after the user runs the
runbook (or hand-fixes a failed agent copy), it flips the named entries — or all
remaining markable ones — to `status: "replicated"` with `verified_by: "user_asserted"`,
recording a user-supplied content digest or destination verbatim and never inventing
either, then republishes the deployment exports slice so `image_map` reflects the flip.
Unknown refs are named, never created; an entry whose destination cannot be resolved (an
`<AR_DESTINATION>` runbook) is refused unless the pushed reference is supplied, and one
that does not parse as a registry reference is refused too. A repaired `replication_failed`
entry keeps what broke under `previous`. Idempotent but not inert: a digest or destination
supplied for an already-replicated entry upgrades the record (and is picked up by a bulk
call, since the tool's own tip asks for exactly that re-call), while a repeat that adds
nothing republishes nothing. Where the target clone is absent — the runbook can be run
weeks later on another machine — only the exports `image_map` is republished, so the
fields derived from the clone keep their last published values.

Server-side copies record their own provenance the same way:
`verified_by: "skopeo_preserve_digests"`, plus the `content_digest` skopeo reports through
`--digestfile` — the digest actually written, since `--multi-arch=system` copies one
instance out of an index and the source's index digest names nothing at the destination.

`provision_artifact_registry` resolves the destination registry in precedence order —
already recorded in the ledger, declared in the landing-zone design (the clone is scanned
for `google_artifact_registry_repository` resources), else a deterministic default
(`{GKE_AGENTIC_MIGRATION_AR_LOCATION|us-central1}-docker.pkg.dev/<gcp_project>/<workspace-slug>`) — then
checks each fully-resolved destination via the Artifact Registry API and creates it when
missing, the way onboarding creates the SSM repository. The resolved list is recorded to
`variables.artifact_registry_destinations`: the contract image replication reads.

Destinations the design declares with computed values (Terraform variables or
interpolation) are the user's `terraform apply`'s to create; they are reported, never
created, and never guessed at.

Deliberately, there is no elicitation and no gate:

- An empty docker repository is near-free, idempotent to create, and required before any
  image can be replicated. The audit trail is the ledger history entry, not a prompt.
- Failures never block. The migration PR is already shipped by the time this state runs;
  a registry problem (missing `artifactregistry.repositories.create` on the server's ADC
  identity, an API error) is fixable later and is carried in the drain message instead.

Users who want the registry in their GitOps repository instead can adapt the starter
module at
[reference/terraform/modules/artifact-registry/main.tf](../../../reference/terraform/modules/artifact-registry/main.tf)
in their landing-zone design — the scan honors it, and an existing repository is left
untouched (`exists`, not re-created).

## Adding an action

1. Implement `action_name(variables, config) -> (transition_key, message)` in
   `actions.py` and add it to `ACTIONS`.
2. Add the `INTERNAL_TASK_SERVER_MUTATION` state to `servers/dag/platform_dag.json`
   with `"action": "action_name"` and its transitions.
3. `main.run_internal_mutation` reaches it through the `ACTIONS` table — no `main.py`
   change beyond the existing dispatch line.
