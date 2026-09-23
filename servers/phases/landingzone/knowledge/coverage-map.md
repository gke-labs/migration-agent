# Coverage map — who generates every migration artifact

One migration produces artifacts from three generators: the **landing zone** designs the
foundation, **platform translation** generates everything platform-owned that layers on top of
it, and the per-workload **workload** phase generates what each application team
ships. The boundary between them has so far lived in prose — planner docstrings, knowledge
documents, worker prompts. This document makes it a single normative table: for every kind of
artifact this migration produces or is designed to produce, exactly one owner. Rows whose
generation is not built yet carry **[PLANNED]** in their notes — on the platform side as much
as the workload side; the marker discipline is the same one DESIGN.md uses.

The server parses this table at start-up (`servers/dag/server/coverage_map.py`, the same
knowledge-as-machine-read-authority arrangement as the assessment blocker taxonomy): a table
that fails to parse, names an unknown owner or column, or lists a kind twice is a refusal to
start. Adding or moving an artifact kind is therefore a documentation edit (renaming or moving
a row a translation unit cites also moves the citation — see "How to change this table"), and
the table the humans read is the table the machine enforces, by construction.

Two further machine uses are built, at v1 **section granularity** — the granularity today's
inventory has (section summaries, not typed per-fact rows). At translation-planning time
(`plan_translation`) the map is instantiated against the approved inventory: each row's
backticked `Discovered from` sections are resolved and the row gets a verdict — `facts-present`,
`no-facts`, `not-scanned` (a "—" cell), or `unknown-section` (a cell naming a section the
inventory schema does not declare — a map defect reported as a verdict, not a crash). Translation
units cite the rows they cover (the planner's `covers` field; a no-facts placeholder carries its
family's citation), so three checks run mechanically at plan review: nothing omitted (a
facts-present platform-translation row no unit covers), nothing overlapping (a
platform-translation row covered by more than one active unit family), everything traceable
(units citing zero or unknown rows) — and any `unknown-section` row is itself reported as a
finding, so a defective map cannot read as clean.

At plan review the findings are WARNING-grade — the humans weigh them. The OMISSION half is
then ENFORCED at the validate step (`translation_validate_3/coverage_gate.py`, added after a
pre-submit audit found an end-to-end run had shipped a landing zone with no
`google_container_cluster` and `terraform validate` passed): a facts-present row owned by
`landing-zone` or `platform-translation` with no artifact behind it — no done citing unit, no
explicitly skipped citation, no clone-scanned resource (the GKE-cluster row is proven by
scanning the materialized clone for `google_container_cluster`), and no documented
`UNENFORCED_ROWS` pin — fails validation naming the row. Overlap and traceability stay
observe-only in v1. **[PLANNED]** v2 instantiates at fact granularity (each discovered fact
assigned to its owning row) once the inventory schema carries typed per-fact rows; until then a
row's verdict is only as fine as the sections it names.

## Vocabulary

The parser enforces these values; anything else in the table is a start-up failure.

- **Column** — which API the artifact targets, fixed at the resource level:
  `terraform` (provisioned through GCP APIs) or `k8s` (an object inside the cluster).
- **Owner** — who generates it: `landing-zone`, `platform-translation`, or `workload`
  (the per-component developer phase; every row is listed whether or not its generation is
  built, so omission checks see the whole boundary).

## Coverage map

| Artifact kind | Column | Owner | Discovered from | Notes |
|---|---|---|---|---|
| GKE cluster (control plane, target shape) | terraform | landing-zone | `clusters`, `triggers`, landing-zone decisions | The four target-shape decisions land here; designed by the landing-zone phase, never re-emitted by a unit. Enforced at the validate gate by clone scan (`coverage_gate.RESOURCE_SCANS`): a facts-present run whose clone declares no `google_container_cluster` fails validation — the case that audit found. The same gate reads one field off every declared cluster: `dns_config { cluster_dns = "CLOUD_DNS" }` (`coverage_gate.scan_cluster_dns_config`; Autopilot exempt, an expression accepted with a note). `clusters` leads the cell on purpose: it is populated for every estate discovery scanned a cluster in, while all four `triggers` booleans default false, so sourcing the row from `triggers` alone made the flagship enforcement conditional on Karpenter/privileged/GPU/peering having fired. |
| Base VPC, subnets, secondary ranges | terraform | landing-zone | `network` (base), `address_space` (the source ranges the target must not overlap) | Units layer on top of this network; they never re-emit it. Pinned unenforced at the validate gate (`coverage_gate.UNENFORCED_ROWS`): adopting a pre-existing VPC (data-sourced, not declared) is a legitimate shape a resource scan cannot tell from an omission. |
| Organization hierarchy and org policies | terraform | landing-zone | — | Standing landing-zone modules (`hierarchy`, `orgpolicy`). |
| Monitoring baseline | terraform | landing-zone | — | Standing landing-zone module. |
| GKE node pools | terraform | platform-translation | `nodegroups` | Skipped when the derived cluster mode is Autopilot (`decisions.cluster_mode`); the requirements become workload constraints instead. |
| Node auto-provisioning (Karpenter replacement) | terraform | platform-translation | `autoscaling`, `triggers.karpenter` | Shape follows the `karpenter` decision: the NAP guardrail under `GKE_STANDARD_NAP`, or the ComputeClass row below under `GKE_STANDARD_COMPUTECLASS` (the `autoscaling` unit is then skipped with the reason). |
| ComputeClass (Karpenter NodePool replacement) | k8s | platform-translation | `autoscaling.karpenter_nodepools` | Generation built (the planner's `compute-class` family). One ComputeClass per typed NodePool, name preserved, planned only when the derived Karpenter replacement is ComputeClass (the recorded `GKE_STANDARD_COMPUTECLASS` choice under Standard) and skipped otherwise; the NAP row above carries the other Standard arm. Shape, spot flags and the family mapping are checked by `translation_validate_3/computeclass_contract.py` against `gke-compute-classes.md`. |
| Cluster addon disposition (drop / built-in / reconfigure) | terraform | platform-translation | `addons` | Includes enabling GKE-managed equivalents (e.g. Filestore CSI). |
| Workload Identity: GSA and IAM bindings | terraform | platform-translation | `workloads.irsa_bindings` | The Terraform half of a two-party pair; the KSA half is a `workload` row below. |
| Cross-VPC connectivity and load balancers | terraform | platform-translation | `network` | Layers on the landing-zone VPC per the `vpc_peering` decision. The terraform half only: the same `network` facts also gate the k8s `Gateway (shared entry point)` row, so this unit emits no Gateway API objects. |
| Artifact Registry repository | terraform | landing-zone | `images` | Declared in the landing-zone design when present; when absent the deployment phase provisions a default via the API — an action, not a repository artifact. Optional-by-design, so the validate gate pins it unenforced (`coverage_gate.UNENFORCED_ROWS`). |
| Filestore instances | terraform | platform-translation | `storage` | Backing for Filestore-class volumes when the estate needs them. Emitted today at the storage unit's discretion — no unit family is charged with this row yet, so the validate gate pins it unenforced (`coverage_gate.UNENFORCED_ROWS`); charging a family means dropping the pin in the same change. |
| Privileged workload policy | terraform | platform-translation | `workloads.privileged_daemonsets` | Strategy follows the `privileged_daemonsets` decision. |
| hostNetwork workload compatibility | terraform | platform-translation | `workloads.host_network` | Split from the privileged-policy row: the planner briefs them as separate families, and one row per family is what lets the overlap check treat a second family on a row as a defect. Column inherited from the parent row at the split, not re-adjudicated; if this work lands mostly in-cluster (pod specs, NetworkPolicy), moving the row to `k8s` is a one-cell edit to argue here. |
| StorageClass tier menu | k8s | platform-translation | `storage` | Generation built (the planner's `storage` unit family). Contract menu; source class names preserved so PVC references keep resolving. |
| Gateway (shared entry point) | k8s | platform-translation | `network` (load balancers, ingress hosts) | Generation built (the planner's `gateway` unit family). The manifest must carry the `gkma.dev/shared-gateway: "true"` marker and set `metadata.namespace` — exports publishes only a marked Gateway, and the validate step's gateway output contract fails the unit that omits either. The attach contract is product-owned, never worker-designed: every listener sets `allowedRoutes.namespaces` to `from: All` or a `from: Selector` on exactly the `gkma.dev/gateway-access: "shared"` label (never `Same` — accepted routes that never attach), the unit itself emits the platform Namespace its Gateway lives in (labeled, name byte-identical, colliding with no recorded workload namespace), and the gate cross-checks the shipped policy against every Namespace manifest the run ships. Per-workload HTTPRoutes attach to it from the `workload` side; the terraform `Cross-VPC connectivity and load balancers` row owns the infrastructure half and emits no Gateway API objects. |
| Cluster DNS resolver configuration (stub domains, upstream nameservers) | k8s | platform-translation | `cluster_dns` | Generation built (the planner's `cluster-dns` family). The CoreDNS Corefile discovery copied verbatim, carried onto Cloud DNS for GKE: on Standard clusters the `kube-dns` ConfigMap in `kube-system` (the only object the unit may place there); on Autopilot, or when the mode is null (none recorded, or the recorded decisions disagree), nothing in this column — the terraform row below carries it. The mapping is `cluster-dns-translation.md`, attached to the worker prompt by unit kind; shape, owner boundary and literal fidelity are checked by `translation_validate_3/clusterdns_contract.py`. |
| Cluster DNS zones and records (hosts entries, forwarding zones) | terraform | platform-translation | `cluster_dns` | Generation built (the same `cluster-dns` family — two rows, one family, both facts-present together). Private managed zones with record sets for Corefile `hosts` entries; private forwarding zones for stub domains under Autopilot or a null mode (none recorded, or the recorded decisions disagree). Layers on the landing-zone VPC by self-link; never emits `dns_config`, `addons_config` or a `google_dns_policy` (VPC-wide, one per network — landing-zone territory). |
| Priority class scheme | k8s | platform-translation | — | **[PLANNED]**. Contract menu. |
| Namespaces | k8s | platform-translation | `clusters[].workloads.namespaces` | Generation built (the planner's `tenancy` unit family). Per-namespace issuance, names verbatim from the inventory (surrounding whitespace stripped — a padded recording is the same namespace); a multi-team shared namespace stays one namespace — the estate reality this design accepts (acme runs 3 teams in `acme-shop`) — never split or renamed per team. Must exist before workloads deploy into them. Every issued Namespace carries the product attach-permission label `gkma.dev/gateway-access: "shared"` — not a discovered fact but the permission the shared Gateway's Selector listeners select on. The platform-owned namespace the Gateway itself lives in is NOT a recorded name and not this family's to issue: the `gateway` unit emits it (labeled, name byte-identical to its Gateway's `metadata.namespace`), and the validate step's gateway contract fails a run where no shipped manifest creates it or where one name is issued by two units. |
| ResourceQuota and LimitRange | k8s | platform-translation | `clusters[].workloads.namespaces` | ResourceQuota generation built (the `tenancy` unit family): per-namespace issuance, one quota per namespace even when teams share it; quota values are stated assumptions or open questions, never discovered facts — and with zero recorded quota facts the ordered starter quotas are object-count-only (pods, services, persistentvolumeclaims), never cpu/memory: a compute quota makes requests/limits mandatory while the LimitRange that could default them stays unemitted. LimitRange: conditional generation is in the brief — ordered only where an entry records limit facts (honored literally); no recorded limit facts, no LimitRange, and a recorded compute-constraining quota without limit facts routes its pod-rejection edge to open_questions. |
| Namespace RBAC (roles, bindings) | k8s | platform-translation | — | **[PLANNED]**. Team facts live outside the IaC fossil record; gathered by elicitation, never invented from the scan. |
| NetworkPolicy (namespace isolation) | k8s | platform-translation | — | **[PLANNED]**. No unit family emits one and the tenancy brief forbids it explicitly: isolation posture is not a recorded tenancy fact, and a default-deny invented from a scan would cut east-west traffic for the migrated workloads. Needs elicited or workload-phase facts before any family owns this row. The workload-side `NetworkPolicy` row below is the per-workload half of the same territory. |
| Workload manifests (Deployments, Services, config) | k8s | workload | — | Built: the `wkld-manifests` unit of the developer phase. The manifests come from the source repository, not from inventory sections. |
| HTTPRoute (per-workload routing) | k8s | workload | `network` (ingress hosts) | Built: the `wkld-routing` unit emits HTTPRoutes whose `parentRefs` come verbatim from `exports.gateway`. While that field is null the unit parks honestly (facts recorded, attach point missing) and unparks at translate entry once the platform Gateway publishes. Attaches to the platform Gateway — the other half of that pair. |
| KSA and Workload Identity annotation | k8s | workload | `workloads.irsa_bindings` | Built: the `wkld-identity` unit. The Kubernetes half of the GSA pair. The translation workload-identity unit used to emit a default-off KSA convenience resource; it shed that resource once `wkld-identity` existed to own this row, and its brief now says so and the translation validate gate rejects the resource in every form it could come back in (DESIGN §14 issue 18). What still crosses owners is the `ksa_annotations` output contract — the designed handover, not a straddle. The pairing data itself flows only when the plan threads the workspace's recorded target project into the unit (`inputs.target_project`); without a recorded project the unit's empty-map escape publishes `gsa_bindings: null` and the translation validate gate raises it loudly over recorded IRSA bindings. |
| PVC storage class usage | k8s | workload | `storage` | Built: the `wkld-storage` unit. Consumes the StorageClass menu by source name; claims only, no data movement. |
| Container image references | k8s | workload | `images` | Built: in plain manifests the deterministic pass rewrites them from the exports `image_map` (never guessed). Inside chart/kustomize SOURCES the translation worker owns the rewrite — the plan brief lists the replicated `image_map` literals to transcribe, and the workload validate gate re-renders the shipped sources and blocks any left-over rewritable value. Replication itself is the deployment phase's action, not a repository artifact. |
| NetworkPolicy | k8s | workload | `network` | **[PLANNED]**. Out of the workload pipeline's scope for now (HTTPRoute translation shipped first) — the wkld-routing brief says so. The platform-side `NetworkPolicy (namespace isolation)` row above is the namespace-isolation half of the same territory. |

## Structural properties

Three properties of this table are load-bearing; a change that breaks one deserves suspicion,
not a quiet edit.

- **The workload × terraform cell is empty on purpose.** The developer phase writes Kubernetes
  objects; infrastructure stays with the platform. A row that wants to land there is probably
  misclassified — the same instinct the phase-split analysis applies to the thin
  landing-zone × k8s cell.
- **Two-party artifacts always cross owners — and may cross columns.** Workload Identity is a
  GSA (terraform, platform-translation) paired with a KSA (k8s, workload): it crosses both, as
  the matrix analysis notes. The Gateway (platform-translation) pairs with HTTPRoutes
  (workload) within one column. Each half is its own row so an omission check can see half a
  pair missing — the "half-unit" failure the matrix analysis calls out.
- **`workload` rows name the developer phase's units.** They document the boundary the platform
  rows were drawn against, and the built ones now say which unit family owns them
  (`wkld-manifests`, `wkld-identity`, `wkld-storage`, `wkld-routing`). A unit that
  needs a row that is not here means the map — not the boundary — was wrong, and the fix starts
  with this table.

Known open edges, left visible on purpose: the inventory sections `data_dependencies`,
`observability`, `workloads.gpu_tpu_workloads`, and `workloads.host_path_volumes` currently have
no owning row. (Pod-level `dnsPolicy` / `dnsConfig`, the workload side of cluster DNS, is not an
inventory section at all: the workload planner reads it from the developer's clone into the
manifests unit's `pod_dns_facts` and `poddns_contract.py` gates it at the workload validate step
— owned by the workload pipeline, outside this table by construction.) The v1 checks
look from map rows to the sections they name, so an unclaimed section is invisible to them;
surfacing ownerless facts is the **[PLANNED]** fact-level instantiation's job. Assigning these
sections owners is a design decision this table records when it is made, not one it should
smuggle in.

## How to change this table

Adding, renaming, or moving an artifact kind is an edit to this file plus the pinned tests
that ratchet it — no server logic changes, but the planner-binding invariant (`planner_test`),
the real-map pins (the uncovered platform set, the "—"-sourced rows), and the validate gate's
enforcement pins (`coverage_gate.RESOURCE_SCANS` / `UNENFORCED_ROWS`, bound by
`coverage_gate_test`) are same-change bookkeeping by design, so a table edit lands in them the
day it happens. A NEW facts-present `landing-zone`/`platform-translation` row that arrives with
no citing family, no resource scan, and no pin fails validation by default — enforcement widens
with the map or the edit does not land. The parser will refuse: an owner or column outside the
vocabulary above, a kind listed twice (comparison ignores backticks, spacing, and case), a row
with an empty kind cell, or an empty table. Renaming a platform-translation row also renames
the citation the planner's unit family carries (`planner.FAMILY_COVERS`), and a test binds the
two — so that half IS a code change, by design.

The `Discovered from` column is machine-read at planning time: the backticked tokens are dotted
section paths resolved against `servers/dag/server/schema/inventory.json` (a `[]` segment, as in
`clusters[].workloads.namespaces`, steps into an array's items; an undeclared path makes the
row's verdict `unknown-section`), `—` means the facts come from elsewhere (standing modules,
elicitation), and everything outside backticks — like "(ingress hosts)" — is annotation for
humans, invisible to the v1 resolver.
