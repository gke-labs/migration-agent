# GKE Landing Zone

The target GCP foundation an EKS estate lands on: project hierarchy, Shared VPC, GKE clusters,
organization policies, baseline IAM, observability and budgets, expressed as modular Terraform in the
target GitOps repository.

This document is the domain reference for the `landingzone` phase. It says *what* a correct landing zone
looks like and *why*. The step instructions delivered with each state say which tool to call and when.

## Scope

- **In:** the design decisions, the standing defaults, the modules to write, the hardening controls every
  generated cluster must satisfy.
- **Out:** applying the Terraform. The phase ends at a Pull Request; a human applies it. Nothing in this
  phase touches a live cluster or a live GCP project.

## Inputs

Everything needed comes from the ledger:

- The approved discovery inventory (`platform/discovery/inventory.json`) — cluster facts, node groups,
  privileged workloads, GPU/TPU requirements, VPC peering, storage classes, IRSA bindings. The four
  `triggers` decide the target-shape questions below. Its `address_space` section is the source
  network as the files declare it — VPC and subnet CIDRs, each cluster's service range and hybrid
  remote ranges, peered and routed ranges — and is what the target ranges are chosen against.
- The source ranges and the proposed target ranges, echoed by every `resolve_lz_decision` call
  (`source_address_space`, `proposed_target_ranges`, `unresolved_source_ranges`). Pure code
  computes them from `address_space`; use them as the network defaults and never choose your own.
- The readiness report and its blocker list — every blocker is owned and dated before this phase is
  reachable, and each resolution path constrains the design.
- `variables.lz_decisions` — the answers already recorded for the four decisions.
- `variables.target_clone_path` — the local clone of the target repository, on branch
  `variables.lz_branch_name`. All generated code goes here.
- `variables.target_path` — the subpath within the target repository the workspace was configured to
  write into.

## The four target-shape decisions

Each decision is derived from a discovery trigger and has a fixed set of choices. The first is the default
recommendation; deviate only for the reason given. These are the exact tokens `resolve_lz_decision` accepts —
it rejects anything else. **The server reads this table** (`servers/dag/server/decisions.py`, the same
arrangement as the assessment blocker table and the coverage map): the choice set, the default, and the
`Implies mode` column are parsed at start-up, so adding or renaming a choice is an edit here plus the module
guidance and validate contract that choice needs. A row with an empty `decision_id` continues the decision
above it. `Implies mode` is `standard` or `autopilot` for a voting decision, `advisory:standard` /
`advisory:autopilot` for one that implies a mode without deciding it, and `—` for none.

| `decision_id` | Trigger in the source estate | Choice | Take it when | Implies mode |
|---|---|---|---|---|
| `karpenter` | `karpenter.sh` NodePool / Provisioner in the source IaC | `GKE_STANDARD_NAP` | Each NodePool provisions one shape: a single capacity type, one instance family or none, taint-driven placement. Node auto-provisioning defaults carry that. When a pool mixes capacity types or families (the row below), prefer ComputeClass. | `standard` |
| | | `GKE_AUTOPILOT` | Karpenter exists but only autoscales a homogeneous pool. Autopilot subsumes it and removes the node layer entirely. | `autopilot` |
| | `any(autoscaling.karpenter_nodepools[].capacity_types, len > 1) or any(autoscaling.karpenter_nodepools[].instance_families, len > 1) or any(autoscaling.karpenter_nodepools[].requirements_unreduced, contains karpenter.sh/capacity-type)` | `GKE_STANDARD_COMPUTECLASS` | A NodePool expresses a priority ladder inside itself: a spot + on-demand mix, or several instance families. A GKE ComputeClass carries the ordered fallback, the spot flag and the family per priority, and node pools are auto-created per class (`gke-compute-classes.md`). Standard only. | `standard` |
| `privileged_daemonsets` | `hostNetwork` / `hostPID` / `privileged: true` DaemonSets | `GKE_STANDARD` | The DaemonSets are load-bearing (security agents, storage plugins, custom CNI helpers) and need node-level access. | `standard` |
| | | `GKE_AUTOPILOT_BYPASS` | The privileged set is small and replaceable by a managed equivalent, or the workloads qualify for Autopilot's allowlisted partner exemptions. | `autopilot` |
| `gpu_tpu` | `nvidia.com/gpu`, `aws.amazon.com/neuron`, p4/g4/g5/trn1/inf instance types | `GKE_STANDARD_SPECIALIZED` | Accelerated workloads need pinned driver versions, specific GPU SKUs, time-sharing or MIG partitioning. | `advisory:standard` |
| | | `GKE_AUTOPILOT_SPECIALIZED` | Accelerator use is bursty and standard: a supported GPU class with default drivers, no partitioning. | `advisory:autopilot` |
| `vpc_peering` | `aws_vpc_peering_connection`, transit gateway attachments | `PUBLIC_AUTHORIZED_NETS` | No VPN or Interconnect to GCP exists yet. The control plane endpoint stays public but reachable only from named CIDRs. | `—` |
| | | `PRIVATE_ONLY_PEERING` | Private connectivity to GCP already exists. The control plane endpoint is private and the peering topology is rebuilt on the GCP side. | `—` |

A trigger that is `false` in the inventory still has a decision to record — the estate simply has no reason
to deviate, so take the default and say so in the design document. When the inventory carries no `triggers`
at all, every recorded choice counts as deliberate: a defaulted decision must then agree in mode with the
ones you chose, or `finalize_landing_zone_design` refuses the pair.

**The cluster mode is derived, never recorded.** `decisions.cluster_mode` reads the voting decisions
(`karpenter`, `privileged_daemonsets`): a decision votes when its trigger fired or its choice is not the
default; agreeing voters give the mode, disagreeing voters give no mode and the design step refuses to
finalize until one is re-resolved. The `gpu_tpu` choice is advisory: when it implies the other mode the
translation plan carries a finding for the plan reviewer instead of changing the mode. Every reader of the
mode (the node-pool units, the cluster-dns unit, `exports.cluster.type`, the validate contracts) goes through
that one projection.

Karpenter deserves a specific warning: its `NodePool` and `EC2NodeClass` resources do not translate as
they are. Capture the taints, labels and instance constraints and rebuild them as GKE node pools, NAP
rules, or — under `GKE_STANDARD_COMPUTECLASS` — one ComputeClass per NodePool, written by the
`compute-class` translation units from the typed `autoscaling.karpenter_nodepools` entries (the cluster
prerequisites for that arm are in `gke-compute-classes.md`, section 1). The community Karpenter provider
for GCP is preview-grade; treating it as parity is a re-platform, not a migration. See [LFF-16](../../../../reference/lessons-from-the-field.md#lff-16--karpenter-on-gcp-exists-but-is-preview-grade-treat-the-parity-as-a-re-platform).

## Standing defaults

Apply these unless the estate gives a documented reason not to. Every deviation is an entry in
`design-decisions.md` with its rationale.

The last two columns are machine-read (`decisions.py`): a row that fills both `Fact path` (a dotted inventory
path, `[]` stepping into an array) and `Expected` makes the translation plan juxtapose the recorded fact with
the default for the plan reviewer, one line per item, judging nothing. A row that fills only one of the two
refuses start-up. Every row is empty today.

| Setting | Default | Deviate when | Fact path | Expected |
|---|---|---|---|---|
| Cluster availability | Regional | Zonal only for ephemeral non-prod test clusters | | |
| Control plane endpoint | Private, with master authorized networks | Public + authorized networks when no VPN/Interconnect exists (`vpc_peering = PUBLIC_AUTHORIZED_NETS`) | | |
| Nodes | Private (`enable_private_nodes = true`), Cloud NAT for egress | Never for prod | | |
| Dataplane | Dataplane V2 (`datapath_provider = "ADVANCED_DATAPATH"`) | Never — it is also how NetworkPolicy is enforced | | |
| Identity | Workload Identity Federation on every cluster (`workload_pool`) | Never | | |
| Cluster DNS | Cloud DNS for GKE, cluster scope (`dns_config { cluster_dns = "CLOUD_DNS" }`), NodeLocal DNSCache on (`addons_config { dns_cache_config { enabled = true } }`); Autopilot has both by default | VPC scope (`cluster_dns_scope = "VPC_SCOPE"` + `cluster_dns_domain`) only when another cluster or on-prem must resolve this cluster's Services by name — Standard-only, immutable once set, and it stops reading the kube-dns ConfigMap; record it in `design-decisions.md` | | |
| Cluster mode | Derived from the `karpenter` and `privileged_daemonsets` decisions above (the four target-shape decisions are the only place a mode is chosen) | Never here: change the decision, not the default | | |
| Fleet | One fleet per environment | Single fleet only for very small estates | | |
| Release channel | Pinned channel (Stable for prod, Regular for non-prod) | Static version only under a compliance mandate | | |
| Cluster database encryption | CMEK | No compliance requirement *and* the key cost is unjustified | | |
| Binary Authorization | Enabled with an allow-all policy initially | Enforce attestors only after registry migration completes | | |
| Pod CIDR | `/16` or larger per cluster | Never smaller — see pitfalls | | |
| Node SA | Dedicated least-privileged service account | Never the default Compute Engine SA | | |
| Node image | Container-Optimized OS, Shielded Nodes, secure boot | Never | | |
| Prod cluster resources | `deletion_protection = true` | Never in prod | | |
| Private Google Access | On, plus Private Service Connect endpoints for `googleapis.com` | Never for private clusters | | |

## What to produce

Write into the clone at `target_clone_path`, on the branch already checked out for you:

```
<target_clone_path>/
├── 03-landing-zone/
│   ├── plan.md                 # the architecture document: topology, IAM groups, budgets
│   └── design-decisions.md     # every decision above with the rationale and who chose it
└── terraform/
    └── modules/
        ├── hierarchy/          # folders, projects, API enablement
        ├── shared-vpc/         # host VPC, subnets, NAT, PSC, firewall
        ├── gke-cluster/        # cluster and node pool specifications
        ├── orgpolicy/          # org policy constraints and baseline IAM bindings
        └── monitoring/         # monitoring workspaces, alert policies, budgets
```

`terraform/modules/gke-cluster/` and `terraform/modules/shared-vpc/` have starter modules in this
repository — [reference/terraform/modules/gke-cluster/main.tf](../../../../reference/terraform/modules/gke-cluster/main.tf)
and [reference/terraform/modules/shared-vpc/main.tf](../../../../reference/terraform/modules/shared-vpc/main.tf).
Adapt them rather than starting from scratch. `hierarchy`, `orgpolicy` and `monitoring` are authored per run.

Render `plan.md` from [templates/landing-zone-design.md](../../../../templates/landing-zone-design.md).

**A module no root module references is never compiled.** The validation gate runs
`terraform init -backend=false && terraform validate` at the root of the clone, not per module directory. If
the clone root has no configuration wiring these modules together, validation passes without having looked
at any of them. Write a root module at the clone root — or extend the target repository's existing one — that
instantiates every module you generated.

### 1. Project hierarchy

Folders and projects, with the APIs each needs enabled:

- Folder `prod`: `net-prod-host` (Shared VPC host), `gke-prod-clusters` (control planes), `data-prod`
  (Cloud SQL, Memorystore), `obs-prod` (monitoring scope).
- Folder `nonprod`: `net-nonprod-host`, `gke-nonprod-clusters`, `data-nonprod`.
- Folder `shared`: `artifact-registry`, `cicd`, `terraform-state`.

APIs: `container`, `compute`, `iam`, `logging`, `monitoring`, `gkehub`, `dns` (Cloud DNS for GKE and the cluster-dns unit's zones need it), plus `artifactregistry`,
`secretmanager` and `cloudkms` where the estate needs them.

### 2. Shared VPC

- One subnet per region, with secondary ranges for pods and services. Baseline sizing: nodes `/22`,
  pods `/16`, services `/20`, at `10.0.0.0/22`, `10.4.0.0/16` and `10.20.0.0/20` when nothing
  in the source uses them.
- The target ranges never overlap the source address space: the two networks are routable to
  each other for as long as the migration runs. `resolve_lz_decision` echoes the source ranges
  (`source_address_space`) and a proposal that keeps each baseline block unless it collides, in
  which case it takes the first clear block of the same size in private space
  (`proposed_target_ranges`, with `moved_off_the_baseline` naming the ones that moved; the last
  candidate space is `100.64.0.0/10`, which GKE accepts for pod and service ranges; `172.17.0.0/16`
  is never proposed, Google Cloud's VPC documentation says not to use it where a product routes
  it inside the guest OS, as the default Docker bridge does; and
  `no_free_block_for` names a range no candidate space can hold, which the user then chooses). Write
  the proposed ranges into the network module and plan.md, and list the source ranges beside
  them so a reviewer sees what they were kept clear of. When the echo reports
  `unresolved_source_ranges`, it lists the ranges the files build from something discovery could
  not follow (address, file, argument, expression); ask the user for each listed entry before the
  design is final, do not re-derive the list from `address_space.unresolved`, and say in
  `design-decisions.md` which ranges the user supplied. `unresolved_but_covered`
  counts entries that cannot change the proposal (a subnet inside a stated VPC range or of a VPC
  whose own range is already among the questions, a public-endpoint allow list): no question of
  their own, they are in the inventory for the record. `vpcs_without_a_stated_range`
  names a VPC the files declare without its CIDR and without a question for it under `unresolved`
  (an IPAM pool, an existing VPC by id), and
  `clusters_without_a_recorded_vpc` names a cluster whose VPC has no entry at all (an existing VPC
  reached through a data source): ask for those ranges too. `clusters_with_a_defaulted_service_range`
  names a cluster that set no service range and has no question for it, so EKS chose one at creation
  (usually 10.100.0.0/16 or 172.20.0.0/16; a cluster with remote networks may have been given another
  block): it is not avoided, so ask the user for the live value (`aws eks describe-cluster`) and keep
  the proposal clear of it. When the echo says no source address space was recorded, or that no range was stated,
  the proposal is the unchecked baseline: ask the user for the source ranges first.
- Cloud Router and Cloud NAT per region.
- Private Google Access on every subnet; Private Service Connect endpoints for `googleapis.com`.
- VPC firewall rules, plus organization-level hierarchical firewall policies: deny ingress from
  `0.0.0.0/0` except IAP and Google front-end health-check ranges; allow internal egress within the VPC.

### 3. GKE clusters

Both modes: pinned release channel, Workload Identity, Gateway API, Binary Authorization, master authorized
networks, CMEK on the cluster database, Dataplane V2, private nodes and private endpoint per the decisions.

Cluster DNS is Cloud DNS for GKE in cluster scope, with NodeLocal DNSCache — declared on Standard
clusters, implicit on Autopilot. Cloud DNS is a managed data plane with no resolver pods: what the
source estate customized in its CoreDNS Corefile (stub domains, pinned upstream nameservers, static
host records) is carried by the `cluster-dns` translation unit into the `kube-dns` ConfigMap on
Standard or into Cloud DNS private and forwarding zones on the VPC (`cluster-dns-translation.md`),
never by editing the cluster resource. Two consequences to write into `design-decisions.md` when they
apply: VPC scope ignores the `kube-dns` ConfigMap and is immutable once chosen, and enabling or
disabling NodeLocal DNSCache on an existing cluster recreates every node.

Standard clusters additionally declare node pools: Container-Optimized OS, Shielded GKE Nodes, secure boot,
surge upgrade settings, and the taints and labels carried over from the source node groups. Karpenter
NodePools become NAP defaults under `GKE_STANDARD_NAP` or one ComputeClass each under
`GKE_STANDARD_COMPUTECLASS` (the `compute-class` units emit the classes; the cluster module only meets the
prerequisites in `gke-compute-classes.md`, section 1).

### 4. Baseline IAM and organization policies

Federated group bindings — `gcp-platform-admins`, `gcp-sre-prod`, `gcp-developers-<env>` — never individual
user bindings. Organization policy constraints, at minimum:

- `compute.vmExternalIpAccess` — deny all
- `compute.requireShieldedVm` — enforce
- `compute.trustedImageProjects` — restrict to `cos-cloud` and `gke-node-images`

Workload Identity Federation for GKE (a standing default above) is what keeps pods away from node
credentials: once it is enabled on a node pool, the GKE metadata server intercepts `169.254.169.254` and
pods can no longer reach the Compute Engine metadata server. Do **not** add a NetworkPolicy blocking that
address — on Dataplane V2 the GKE metadata server itself answers on `169.254.169.254:80` (and on
`169.254.169.252:988`), so such a policy breaks token issuance for every pod it covers. Design for the two
documented gaps instead: every Standard node pool must have Workload Identity enabled (Autopilot always
does), and pods with `hostNetwork: true` bypass it and reach node metadata, so restrict `hostNetwork` by
policy. Strict egress policies must still allow the two addresses above. See
[Workload Identity Federation for GKE](https://cloud.google.com/kubernetes-engine/docs/concepts/workload-identity)
and [LFF-08](../../../../reference/lessons-from-the-field.md#lff-08--on-gke-node-credential-exposure-is-closed-by-workload-identity-on-every-node-pool-not-by-blocking-169254169254).

### 5. Budgets and monitoring

Cloud Monitoring workspaces aggregating cluster metrics; default alert policies for apiserver SLO burn rate
and node saturation; billing budgets with alerts at 50% / 80% / 100% of the agreed cost ceiling.

## Hardening controls

Every generated cluster design is checked against these. Autopilot enforces some of them for you; the rest
you must write, in both modes.

| Control | GKE default | Required | Autopilot enforces |
|---|---|---|---|
| Custom IAM node service account | Default Compute Engine SA | Dedicated least-privileged SA | No |
| Container-Optimized OS | COS | COS on all pools | Yes |
| Shielded GKE Nodes | On in Autopilot | Enabled on all pools | Yes |
| Kubelet read-only port disabled | Disabled | Keep disabled | Yes |
| Workload Identity Federation | On in Autopilot | Enabled for all workloads | Yes |
| Master authorized networks | Public endpoint | Restricted to named CIDRs | No |
| Private nodes | Nodes get external IPs | Private nodes + Cloud NAT | Yes |
| Pod-to-pod restriction | All traffic allowed | NetworkPolicy / mesh (Dataplane V2) | Yes |

The two rows Autopilot does **not** cover — the node service account and master authorized networks — are the
two most often missed. Check them explicitly before finalizing.

## Common pitfalls

- **Pod CIDRs sized too small.** A `/20` pod range looks generous until the cluster autoscales; IP exhaustion
  then presents as unschedulable pods with no obvious cause. Use `/16` or larger, and size the secondary
  ranges for the peak node count, not the current one.
- **Private clusters without Private Service Connect.** Calls to `googleapis.com` time out rather than fail
  fast, and the symptom surfaces in workloads, not in the network config.
- **The default Compute Engine service account.** It carries Editor. Every node pool that keeps it grants
  project-wide write access to anything that reaches the node.
- **Missing deletion protection.** `deletion_protection = true` on every prod cluster resource. A cluster
  deleted by a Terraform refactor is recoverable only from backups that mostly do not exist —
  see [LFF-31](../../../../reference/lessons-from-the-field.md#lff-31--spotify-accidentally-deleted-all-their-kubernetes-clusters-with-no-user-impact--the-canonical-dr-case-study).
- **Cloud SQL behind a second peering hop.** Cloud SQL private IP lives on a Google-managed peering network,
  and peering routes do not propagate transitively. Keep data and nodes in the same VPC, or use Private
  Service Connect — see [LFF-12](../../../../reference/lessons-from-the-field.md#lff-12--cloud-sql-hides-behind-google-controlled-vpc-peering-blocking-second-hop-route-propagation).
- **Regional quota surprises.** Regional node pools multiply disk by the number of zones and routinely trip
  a new project's initial SSD quota, and Terraform's parallelism trips per-minute API quotas on large
  applies. List the quotas the design consumes in `plan.md` and request increases before the apply window —
  see [LFF-37](../../../../reference/lessons-from-the-field.md#lff-37--regional-gke-node-pools-need-300-gb-ssd-by-default-and-trip-initial-regional-quotas)
  and [LFF-38](../../../../reference/lessons-from-the-field.md#lff-38--default-per-minute-api-quotas-throttle-terraform-on-multi-resource-gke-upgrades).
- **Assuming Autopilot is cheaper.** At high pod counts with bursty, over-requested workloads, per-pod
  billing can exceed the equivalent Standard fleet. Model it against the inventory's actual requests before
  defaulting — see [LFF-34](../../../../reference/lessons-from-the-field.md#lff-34--autopilot-pay-per-request-billing-exploded-at-scale-for-one-team-they-reversed-to-ekskarpenter).
- **Carrying EKS request padding into node pool sizing.** Requests tuned for the EKS scheduler routinely
  leave GKE nodes under 40% utilized. Size pools from observed usage, and flag the gap rather than
  provisioning around it — see [LFF-19](../../../../reference/lessons-from-the-field.md#lff-19--gke-bin-packing-post-migration-commonly-drops-to-40-node-utilization).

## Validation

Before finalizing the design:

- `terraform validate` reports 0 errors, from a root module that actually references every generated
  module. *(gate-enforced: the server runs it and sends failures back)*
- The design MUST contain exactly **1** `google_container_cluster`, under any profile — a shrunk
  sandbox included. Zero clusters is a landing zone that lands nothing; two is a second control
  plane nobody decided. *(gate-enforced: the validate step counts landing-zone-owned declarations
  in the materialized clone and fails on zero or more than one; a cluster declared inside a
  translation unit is an owner-boundary finding of its own)*
- Every cluster sets `workload_pool`.
- Every cluster sets `datapath_provider = "ADVANCED_DATAPATH"`.
- Every Standard cluster sets `dns_config { cluster_dns = "CLOUD_DNS" }`. *(gate-enforced: the
  validate gate reads the block off every declared cluster; a cluster with the literal
  `enable_autopilot = true` is exempt, a value through a variable is accepted with a note)*
- Every prod cluster resource sets `deletion_protection = true`.
- No node pool uses the default Compute Engine service account.
- Master authorized networks are set on every cluster with a public endpoint.
- Pod secondary ranges are `/16` or larger.
- No target range overlaps a range in the inventory's `address_space` (VPC, subnet, service,
  remote or routed), and plan.md lists the source ranges the target was kept clear of.
- Each of the four decisions appears in `design-decisions.md` with its rationale.
- Every deviation from the standing defaults is written down. An undocumented deviation is the one a
  reviewer cannot approve.

## Escalation triggers

Escalate rather than guess when the estate shows:

- An organization policy already in force that conflicts with a required constraint.
- A compliance regime (FedRAMP, PCI, data residency) that constrains region or CMEK choice beyond what the
  inventory records.
- An existing GCP landing zone the target must merge into rather than create.
- A source topology that cannot be expressed as Shared VPC — overlapping RFC1918 space across environments
  that must remain routable, for instance.

## Sources

- **Canonical sources**: [reference/sources.md](../../../../reference/sources.md).
- [Hardening your cluster's security](https://docs.cloud.google.com/kubernetes-engine/docs/how-to/hardening-your-cluster)
- [Landing zone design in Google Cloud](https://cloud.google.com/architecture/landing-zones)
- [Setting up clusters with Shared VPC](https://cloud.google.com/kubernetes-engine/docs/how-to/cluster-shared-vpc)
- [Autopilot vs Standard](https://cloud.google.com/kubernetes-engine/docs/concepts/choose-cluster-mode) — the decision points above.
- [reference/service-mapping.md](../../../../reference/service-mapping.md) — AWS→GCP service equivalents.
- [templates/landing-zone-design.md](../../../../templates/landing-zone-design.md) — the `plan.md` template.
