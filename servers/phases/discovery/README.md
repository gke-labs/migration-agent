# Discovery Phase

Everything related to the discovery phase of the EKS → GKE migration lives in this
folder: the per-step agent instructions and the MCP tools each step exposes. The
goal is that a contributor can work on the discovery phase without touching the
shared MCP server entrypoint (`servers/dag/main.py`).

Discovery opens with one read-only scan of the live AWS/EKS estate — what is
actually running, projected to key names and structural fields and written to the
ledger as the Live IR, or a recorded
skip when there is no live estate — and is then a map-reduce pipeline over the
sources: index the IaC sources (no contents in context), fan the files out to
small-context LLM extraction workers, merge their fragments deterministically,
generate a readiness report, and put the result in front of a human for sign-off.

## Layout

```
servers/phases/discovery/
├── __init__.py               # register(mcp) — wires every step's tools into the server
├── discovery_livescan_0/     # STATE_DISCOVERY_LIVE: scan the live AWS/EKS estate
│   ├── instructions.md
│   ├── tools.py              # discover_and_dump_all_clusters (ledger + state wrapper)
│   ├── live_discovery.py     # single round-trip orchestrator (pure, injected seams)
│   ├── aws_live.py           # cloud-plane walk: EKS, nodegroups/ASGs, VPC, LBs, IRSA (pure)
│   ├── k8s_live.py           # in-cluster walk over an injected get_json (pure)
│   ├── projection.py         # keys-only projection: structure and structural strings kept, every other value <omitted> (pure)
│   ├── live_csv.py           # live IR → one CSV per table for the agent (pure)
│   ├── live_schema.py        # the live IR's JSON-schema check, non-blocking (pure)
│   └── eks_auth.py           # the only boto3/kubernetes imports (lazy); STS bearer tokens
├── discovery_init_1/         # STATE_DISCOVERY: index local IaC sources + image inventory
│   ├── instructions.md
│   ├── tools.py              # discover_configuration_files (manifest only)
│   ├── files.py              # index_configuration_files + legacy scanner
│   ├── images.py             # image ref extraction + render target detection (pure logic)
│   ├── datastores.py         # managed data service harvest from Terraform (pure logic)
│   ├── consumers.py          # which workload needs which of them (pure logic)
│   ├── clusterdns.py         # the CoreDNS configuration, copied verbatim (pure logic)
│   ├── addressspace.py       # the source address space: VPC, subnet, cluster and routed ranges (pure logic)
│   ├── overrides.py          # the review's corrections, replayed over each scan (pure logic)
│   └── render.py             # scan/render/decline engine actions (helm, kubectl kustomize)
├── discovery_scope_2/        # STATE_DISCOVERY_SCOPING: client scope sign-off
│   ├── instructions.md
│   ├── tools.py              # update_discovery_scope, confirm_discovery_scope
│   └── scope.py              # pure pattern-matching / scope algebra
├── discovery_datascan_3/     # STATE_DISCOVERY_DATA_SCAN: record managed data services + cluster DNS
│   ├── instructions.md
│   └── tools.py              # scan_data_dependencies (wraps the two pure harvesters)
├── discovery_datareview_3/   # STATE_DISCOVERY_DATA_REVIEW: human sign-off on the mapping
│   ├── instructions.md
│   ├── tools.py              # list/reject/attach/annotate, then confirm_data_dependencies
│   └── candidates.py         # name-similarity ranking, offered only as a guess (pure logic)
├── discovery_extract_3/      # STATE_DISCOVERY_RUNNING: map-reduce extraction
│   ├── instructions.md
│   ├── tools.py              # run_discovery_extraction, write_discovery_inventory
│   ├── chunker.py            # deterministic chunking (content-addressed chunk IDs)
│   ├── extractor.py          # small-context Agent SDK workers + auth preflight
│   ├── merger.py             # deterministic fragment merge
│   └── reporter.py           # readiness-report generation (one big-model pass)
└── ...
```

The human review of what extraction produced is not a discovery step: it lives
with the assessment phase (`../assessment/assessment_review_1/`), one review
covering the inventory, the readiness report, and the blocker list. The review
in `discovery_datareview_3/` is a different one, earlier and narrower: the
workload→data-service mapping, before extraction spends anything.

```
```

The inventory contract shared by workers and the merger lives at
`servers/dag/server/schema/inventory.json`. Extraction workers authenticate from
the environment: `GEMINI_API_KEY` or `GOOGLE_GENAI_USE_VERTEXAI=True` + gcloud ADC
for the default (Antigravity) backend, `ANTHROPIC_API_KEY` or
`CLAUDE_CODE_USE_VERTEX=1` + gcloud ADC for the Claude backup (see
`servers/phases/agent_workers.py`); Claude worker models are configurable via
`GKE_AGENTIC_MIGRATION_EXTRACT_MODEL` / `GKE_AGENTIC_MIGRATION_REPORT_MODEL`.

The image pipeline states between `STATE_DISCOVERY` and `STATE_DISCOVERY_SCOPING`
(`STATE_DISCOVERY_IMAGE_SCAN`, `STATE_DISCOVERY_RENDER_APPROVAL`,
`STATE_DISCOVERY_RENDER`, `STATE_DISCOVERY_RENDER_DECLINED`) carry no step folder:
they are internal/HITL states the dispatch loop drives through inside the
`discover_configuration_files` tool call, so the agent never acts on them. Their
actions live in `discovery_init_1/render.py` and are dispatched from
`main.run_internal_mutation`; the render approval schema is registered in
`servers/dag/dispatch.py` (`ELICITATIONS`).

`STATE_DISCOVERY_DATA_SCAN` sits on the other side of the scope gate, between
`STATE_DISCOVERY_SCOPING` and `STATE_DISCOVERY_RUNNING`, and unlike the image
pipeline it *is* an agent task with its own step folder
(`discovery_datascan_3/`). Two reasons it is not folded into the image scan as
another internal action. The harvest honors the operator's exclusions, and the
image scan runs before a confirmed scope exists. And it reads a local checkout
the image scan gets to assume, because that one runs inside the call that made
the clone — a session resuming on another workstation has nothing to read and
has to be able to say so and ask (DESIGN.md issue 26). An agent task also keeps
a failure recoverable: the state stays put, so the next session is still told
there is work to do. `STATE_ASSESSMENT`'s `on_amend_scope` edge routes back
through it, so an amended scope re-harvests rather than reusing a result
derived from the old one.

The harvest records two kinds of entry. A *declared* one comes from a
Terraform `resource` or module block in the checkout. A *referenced* one comes
from a literal ARN — in an IAM policy, an IRSA module's arguments, a Helm
value — that nothing in the checkout declares: a bucket another team's
Terraform provisions, a console-created database. Its `address` is the ARN,
which is how the review tools name it; both consumer chains match literal
ARNs, so the hand-written IRSA role granting a service account access to such
a bucket still attributes it. Endpoints — an RDS hostname in a ConfigMap, a
queue URL in a Helm value — are read the same way. Wildcard ARNs and ARNs
built from variables are scan notes, not entries.

A third kind is a *guess* (`discovery_init_1/inferred.py`): a bare name a
configuration key types — `INVOICE_BUCKET=acme-invoice-archive` — recorded
with `detection: inferred`, disposition `undecided`, and the workload holding
the key as its consumer. It gates nothing until the review answers it. The
scan reads Terraform and the chart values and manifests in scope for these,
which is also where it finds most endpoints. A bare name that matches an entry
already recorded is not a guess: it is offered at the review as the first
candidate consumer, never attached.

The review answers guesses with `confirm_data_dependency` /
`dismiss_data_dependency`, records services nothing proposed with
`add_data_dependency`, and its sign-off refuses while a guess is open. All
three are overrides like the link-level ones, replayed over every scan.

`STATE_DISCOVERY_DATA_REVIEW` follows it, and is the only human look the
workload→data-service mapping gets: extraction carries `data_dependencies`
over untouched, so what is attached when it starts is what the assessment
grades and what the workload data gate holds a team on at its ship gate. Because
the scan rebuilds the section from the checkout on every run — and the
amend-scope edge makes that routine — the reviewer's corrections cannot live
in the section they correct. They go to
`platform/discovery/data_consumer_overrides.json` and are replayed by
`harvest_datastores` after the merge and before the unattributed notes, so an
attached consumer clears "nothing references this" and a rejection that empties
an entry earns it. The corrections are durable across re-scans; the approval is
per-scan, so a re-scan asks again.

It is also where the customer's decision to *not* move a data service is
recorded (`disposition: keep-in-aws`). The scan cannot propose that — it grades
what the Terraform declares, and marks an AWS-only service `escalate`, meaning
"a human decides" — so the step offers the option rather than waiting to be
asked. What that costs the customer (cross-cloud connectivity, credentials, and
egress on every call) is stated with it; what it does not cost them is an
application team's release, since only `migrate` gates. The deployment data
migration step reads the grade — a service carrying it stops being owed a move
— and that step is also where the decision can be recorded once the migration
is under way and a service turns out not to be movable. Nothing else consumes
it; DESIGN.md issue 30 has the rest.

## The live scan (STATE_DISCOVERY_LIVE)

Discovery opens on the live estate, before the static IaC index. Everything
that follows in this phase reads the Terraform and GitOps sources a team
*declares*; `discovery_livescan_0` reads what AWS is actually *running*, so a
later reconciliation ([PLANNED], DESIGN.md) has an operational reality to diff
the declared sources against. `discover_and_dump_all_clusters` does it in one
call: the AWS cloud plane (EKS clusters, node groups and ASGs,
VPC/subnets/security groups, load balancers, IRSA roles) and, for every
reachable cluster, the in-cluster estate (Deployments and the other workload
kinds, owner-less pods, HPAs, Karpenter node pools, nodes, PVCs, PVs and
StorageClasses, Services/Ingress plus Gateway API / Istio / Traefik routing,
ConfigMaps, Secrets and external secret references).

Two properties are structural, not optional. It is **read-only** against the
customer estate — every AWS and cluster operation is a List/Describe/Get; the
action list is in `discovery_livescan_0/instructions.md` — and **no
free-form value a customer authored reaches the ledger**: every object
leaves through `projection.project`, which keeps the object's structure and
the strings a migration reproduces literally — identifiers and references,
images, ports, classes and drivers; the scheduling and routing contracts a
node pool or load balancer must match (selectors, taints and tolerations,
requirements, requests and limits, hostnames, route paths); and, inside the
user-keyed maps that are otherwise key names only, an exact-key list of
platform labels, class/mode/role annotations, the external-dns hostname,
EBS/EFS StorageClass parameters, an S3 volume's bucket and a redirect's
target hostname — and replaces every other string value with the
`<omitted>` marker. A Secret or ConfigMap becomes its key names, an env literal or
command token or custom annotation value becomes the marker, a
`secretKeyRef` or `imagePullSecrets` entry survives whole; runtime
metadata, `status` and the last-applied annotation are dropped. Nothing is
parsed or classified, so there is no credential shape to miss — what is not
on the allowlist is not written — and the AWS side follows the same rule
(cluster tags, nodegroup labels and subnet tags are key names only; a
security group's free-text description is not recorded). The migration
learns that a `DB_PASSWORD` exists and where it comes from, which is what
it needs; the value stays in the customer's estate.

The engine is pure and injectable — `aws_live` walks an injected
`client_factory(service, region)`, `k8s_live` walks an injected
`get_json(path)` — so the whole walk is unit-tested with no boto3, no
kubernetes client, no credentials and no network. `eks_auth` is the one module
that imports those packages, lazily, and mints the EKS bearer token (a
presigned STS `GetCallerIdentity` URL); the rest of the server runs whether or
not they are installed, and the tool reports an actionable `pip install` line
when they are absent. Credentials come only from the caller's local boto3
chain; the server never asks for or stores one, and holds the boto3, botocore,
urllib3 and kubernetes loggers at INFO while it authenticates and walks,
releasing them after, so a DEBUG root logger (the server's own default) cannot
receive a signing trace.

It is an agent task, not a server action, for two reasons. Live AWS access is
not guaranteed at discovery time (a repos-only engagement, a green-field
target), so the step offers a **recorded skip** — `skip=True` with a
`skip_reason` — that advances the graph rather than failing, and an empty Live
IR then reads as a decision, not a gap. And an error otherwise — authentication,
an exception in the walk, no requested region that would list its clusters —
leaves the state in place, so an expired SSO session or a mistyped region can be
fixed and the call retried. Partial results are the contract: a region denied
among others that listed is a coverage note on a completed scan (named in the
summary's `regions_unreachable`, left out of `regions`), not an error, one
unreachable cluster
keeps its AWS-side record with `kubernetes_error` set and the walk goes on, and
every deliberate omission (a skipped secret type, a page cap) is noted, because
a section that is empty because the walk did not look must never read as an
estate that has none.

The Live IR (`live_discovery.json`) and one CSV per table are written under
`platform/discovery/live/`, its own ledger prefix — not folded into
`inventory.json`, which the static pipeline owns; joining the two is the
[PLANNED] reconciliation step. The IR's contract is
`servers/dag/server/schema/live_discovery.json` (`live_schema.py`): closed on
the structure the walk owns — the cluster record, the in-cluster sections,
the reduced records — and open inside the projected objects, which are
Kubernetes' contract, not ours. Every object in it went through
`projection.project`: its structure and the strings a migration reproduces
(identifiers and references, images, ports, classes, drivers, scheduling
and routing contracts, the exact-key platform labels and annotations) are
kept and every other string value is the `<omitted>` marker, so no
free-form value a customer authored — a Secret or ConfigMap value, an env
literal, a command token, a custom annotation, an AWS tag — is in the
ledger; a migration needs to know a `DB_PASSWORD` exists and where it comes
from, not what it holds. The schema states the Secret invariant outright: a Secret record
admits the marker under `data`/`stringData` and nothing else. The tool holds
every IR to it before persisting; because the IR is code-authored, a
violation is an agent bug, so it becomes a coverage note inside the IR
rather than a refusal that would drop the operator's only copy of the scan.
The tool response is summary-only (counts, unreachable clusters, coverage
notes to relay verbatim); the IR and its tables stay in the ledger and are
served to the Review UI.

## How it connects to the DAG

States in `servers/dag/platform_dag.json` reference steps by folder path:

```json
"STATE_DISCOVERY": {
  "type": "AGENT_TASK",
  "phase": "discovery",
  "step": "servers/phases/discovery/discovery_init_1",
  "instructions": "servers/phases/discovery/discovery_init_1/instructions.md",
  ...
}
```

When the agent calls `get_next_stage`, the server reads the `instructions` file for
the current state and returns its contents, so the agent always receives the playbook
for the step it is on. The discovery flow is
`STATE_DISCOVERY_LIVE → STATE_DISCOVERY → STATE_DISCOVERY_SCOPING → STATE_DISCOVERY_DATA_SCAN →
STATE_DISCOVERY_DATA_REVIEW → STATE_DISCOVERY_RUNNING → STATE_ASSESSMENT` (the assessment phase's combined review). From that review,
`amend_discovery_scope` loops back through the data scan, its review and then extraction with an updated scope (cheap —
cached fragments cover the unchanged chunks), and declining the approval
elicitation loops all the way back to `STATE_DISCOVERY`.

## Adding a step

1. Create `servers/phases/discovery/discovery_<purpose>_N/` (e.g. `discovery_init_1`,
   `discovery_extract_3` — a short purpose label plus the step's position in the
   phase) with `instructions.md` and (if the step needs
   server-side tools) a `tools.py` exposing a `register(mcp)` function.
2. Call the new step's `register` from `servers/phases/discovery/__init__.py`.
3. Add or update the state in `servers/dag/platform_dag.json` with matching
   `phase`, `step`, and `instructions` fields.
