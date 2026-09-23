# Migration Assessment

You are a migration architect. The discovery extraction has already generated the readiness report and the inventory is in the ledger; your job at the assessment review is to judge them against this rubric, agree the blocker list with the user, and submit it. This document is the grading standard a PSO consultant would apply after week 1.

## Purpose

Turn the extraction-generated report and inventory into an agreed decision: what is a real blocker (Step 4's taxonomy — the server enforces its categories verbatim), what the risks and guardrails are, and whether the user signs off.

## When to use this guide

- Immediately after `eks-discovery`.
- When the user asks "are we ready to migrate?" or "what's blocking the migration?".
- When the user wants an effort estimate before committing budget.
- To produce a board-ready summary slide of migration risk.

## Prerequisites

- `01-discovery/inventory.json` exists.
- The orchestrator's `00-orchestrator-state.json` contains the user's constraints (compliance, SLOs, window, cost ceiling).

## Procedure

### Step 1 — Load inputs

Read `inventory.json` and `00-orchestrator-state.json`. Build an in-memory model: clusters → namespaces → workloads → dependencies.

### Step 2 — Score each workload

For each workload, score on four axes (0–3 where 3 = lowest risk):

| Axis                    | 3 (low risk)                                | 2                                          | 1                                          | 0 (blocker)                                  |
|-------------------------|---------------------------------------------|--------------------------------------------|--------------------------------------------|----------------------------------------------|
| **Stateless-ness**      | Pure stateless                              | Stateless + non-PVC config                 | Has PVC, not RWX                           | Has RWX PVC or local hostPath                |
| **Identity portability**| No IRSA / generic SA                        | IRSA with policy fully covered by GCP IAM  | IRSA with one IAM gap                      | IRSA touches AWS-only services (KMS, STS heavily) |
| **Network portability** | ClusterIP only                              | Service of type LoadBalancer (NLB)         | Ingress via ALB                            | Uses VPC CNI features (ENI per pod), hostNetwork, custom CNI |
| **Data dependencies**   | None                                        | Same-region GCP data sink possible (Cloud SQL homogeneous) | Heterogeneous data sink (DynamoDB, Aurora) | On-prem-pinned dep, or cross-account RDS without replica path |

Composite score = sum / 12. Buckets:
- **Ready** (>= 0.75): low risk, fits standard pattern.
- **Tractable** (0.50–0.74): translatable with explicit plan, no rewrite.
- **High-risk** (0.25–0.49): plan + dry-run + extra validation; consider re-platforming sub-components.
- **Blocked** (< 0.25): cannot be migrated as-is; needs design work or stays on EKS / re-platforms.

### Step 3 — Cluster-level scoring

Aggregate workload scores per cluster, plus add cluster-level signals:

- Control plane version drift. Compare each source cluster's Kubernetes minor version (from the inventory) with the minor versions GKE offers today: read the **Current versions** table in the [GKE release notes](https://cloud.google.com/kubernetes-engine/docs/release-notes) for the target's release channel (Regular unless the landing-zone design says otherwise), and the [GKE release schedule](https://cloud.google.com/kubernetes-engine/docs/release-schedule) for end-of-support dates. No drift when the source minor is available in that channel; drift when it is older than the oldest minor available there, in which case plan a Kubernetes version bump pre- or post-migration. Never hardcode version numbers: quote the table you read and the date you read it, and if the page is unreachable ask the operator for the current values rather than guessing.
- Number of custom CRDs / operators in use (more = more translation work).
- Number of platform-level DaemonSets (more = more parallel translation).
- Whether the cluster authentication mode is `CONFIG_MAP` (legacy) — needs `aws-auth` translation to GKE IAM.
- Whether secrets-at-rest encryption is enabled and which KMS key — required for GKE parity.

### Step 4 — Identify blockers

Walk the inventory's `escalations.md` and the workload scoring. Categorize blockers:

| Blocker                                               | Resolution path                                         |
|-------------------------------------------------------|---------------------------------------------------------|
| Custom CNI (non-VPC-CNI) in production use            | Decide GKE CNI: Dataplane V2 default; Cilium-on-GKE if you need eBPF API parity. Plan migration test. |
| `hostNetwork: true` workloads outside known catalog   | Reproduce on GKE with same node-host tolerations; many will work, validate via canary. |
| RWX PVC not on EFS                                    | Identify backing CSI; map to Filestore Enterprise or NetApp Volumes. |
| Karpenter custom NodePools with provisioner-specific taints | Rebuild as GKE node pools, NAP rules or one ComputeClass per NodePool, per the `karpenter` landing-zone decision; capture taints and labels. |
| App Mesh in use                                       | Re-platform to Anthos Service Mesh (managed Istio). Non-trivial. |
| SDK calls to AWS-only services (DynamoDB, Kinesis, …) | Either keep service in AWS during co-existence (with cross-cloud connectivity) or re-platform to GCP equivalent. |
| Cross-account ECR pulls                               | Plan dual-tag strategy or pull-through cache during co-existence. |
| Federated SSO via AWS IAM Identity Center             | Target Workforce Identity Federation; produce identity-provider plan. |
| Compliance scope (FedRAMP, HIPAA, PCI)                | Verify GCP service mappings are in scope; flag any outliers. |
| CoreDNS customization with no Cloud DNS equivalent (`rewrite`, `template`, a custom plugin, a DNS-over-TLS `forward`) | Cloud DNS for GKE carries stub domains, upstream nameservers and static host records; it has no rewrite or template layer. Rename on the application side, or the customer operates a self-managed resolver on GKE — not a GKE-managed feature. The scan records the Corefile verbatim in `cluster_dns`; cite the construct. |
| Local `hostPath` volumes in workloads                 | Identify what the host path provides (metrics, sockets, node config). Reproduce on GKE Standard where the path is allowed, or replace with an API-based source; Autopilot permits only specific paths. |
| VPC CNI-specific features in use (ENI per pod, security groups for pods) | No feature-for-feature GKE equivalent. Map pod-level security groups to Dataplane V2 NetworkPolicy or FQDN policies; plan a connectivity test matrix before cutover. |
| On-prem-pinned dependency or cross-account RDS without a replica path | Treat as a data decision, not a lift: keep the dependency in place over hybrid connectivity (Interconnect or VPN) during co-existence, or build a replication path first. |
| Cluster authentication via legacy `aws-auth` ConfigMap (CONFIG_MAP mode) | Translate the `aws-auth` role and user mappings to GKE IAM plus RBAC bindings. When the inventory records the mode as null, confirm the real mode with the user before scoring. |
| Secrets-at-rest encryption parity (EKS KMS `encryption_config`) | Record the source KMS key; the landing zone's CMEK application-layer secrets encryption is the default parity answer — verify it is in the design before cutover. |

### Step 5 — Effort estimate

For each cluster, compute effort using:

| Driver                                | Cost (engineer-days) |
|---------------------------------------|----------------------|
| Per workload, **Ready** bucket         | 0.5                  |
| Per workload, **Tractable** bucket     | 1.5                  |
| Per workload, **High-risk** bucket     | 4                    |
| Per workload, **Blocked** bucket       | quote separately     |
| Per ALB / NLB                          | 0.5                  |
| Per IRSA role                          | 0.25                 |
| Per RWX PV                             | 1                    |
| Per RDS to Cloud SQL homogeneous move  | 2                    |
| Per Aurora → AlloyDB engine-change     | 8 (and quote)        |
| Cluster baseline (landing zone, fleet) | 5                    |

Multiply by an organizational coefficient (0.8 if the team has prior GKE experience; 1.5 if no GCP experience). Express as a range (P50 / P90).

### Step 6 — Phased plan

Produce a recommended phasing:

1. **Foundation** (week 1–2): landing zone, identity, network plumbing, observability stack, registry mirroring.
2. **Tier-2 first wave** (week 3–4): stateless tractable workloads. Build the muscle.
3. **Stateful + tier-1** (week 5–8): one workload at a time, with full validation.
4. **Tier-0** (week 9–10): final, slowest cutovers, extended soak.
5. **Decommission** (week 11–12): EKS teardown, post-mortem, FinOps tune.

Adjust to the user's stated window. If the user has 30 days, phases collapse and risks tighten — surface that.

### Step 7 — Risks & guardrails

For each named risk, propose a guardrail:

- "RDS → Cloud SQL replication lag during cutover" → guardrail: replication-lag SLO, pause if > 30s for 5 minutes.
- "Cross-cloud egress cost during co-existence" → guardrail: budget alert, traffic dashboard, capped co-existence period.
- "Workload Identity binding drift" → guardrail: CI check that compares IRSA → WI map to live state daily.
- "GKE cluster cost overrun vs EKS baseline" → guardrail: monthly FinOps review; auto-scaler tuning playbook.

### Step 8 — Judge the generated report

The readiness report was generated at extraction time and lives in the ledger
(`platform/discovery/readiness-report.md`). Judge it against the
[readiness-report template](../../../../templates/readiness-report.md)'s sections —
executive summary, scope, scorecard, blockers, phased plan, effort estimate,
risks & guardrails, open questions — and surface anything the generated report
missed as discussion points with the user. Do not author a second report: the
gaps you find either become blockers (Step 4), guardrails (Step 7), or open
questions raised in conversation. The blocker list you agree with the user is
submitted through `submit_assessment` — each entry needs an id, title, a Step 4
category verbatim, a rationale, and a real resolution path (never "TBD");
owners and close dates are assigned in the next step.

## Decision points

- **What counts as tier-0?** Default to whatever the user said in `00-orchestrator-state.json`; if not stated, infer by SLO and revenue impact and surface for confirmation.
- **Engine change vs lift-and-shift for data.** Default to homogeneous moves (RDS→Cloud SQL same engine). Engine change is escalation-only.
- **GKE Autopilot vs Standard at the cluster level.** Not decided here. The mode is derived from the `karpenter` and `privileged_daemonsets` target-shape decisions recorded in the landing-zone design step (`landingzone/knowledge/gke-landing-zone.md`, "The four target-shape decisions"); the report states the triggers that will drive them, not a recommendation of its own.

## Outputs / Deliverables

The ledger already holds the headline artifacts — the inventory
(`platform/discovery/inventory.json`) and the generated readiness report
(`platform/discovery/readiness-report.md`). This review's deliverable is the
agreed blocker list recorded by `submit_assessment` (stored in the workspace
state), which the next step turns into an owned, dated checklist.

## Validation

- Every workload in `inventory.json` appears in `scorecard.json` exactly once.
- Every blocker has a resolution path. None is left as "TBD".
- Effort estimate range is sane (no $0 estimates, no >2-year estimates without explicit reasoning).
- Phased plan fits within the user's stated window; if it doesn't, the report says so on page 1.

## Escalation triggers

- The user's stated window is impossible at the P90 estimate. Escalate before continuing.
- Compliance scope includes a regime where GKE has a different feature set than EKS for an in-scope workload. Surface explicitly.
- Effort estimate shows >50% of workloads in the "High-risk" or "Blocked" buckets — recommend the user revisit scope.

## Common pitfalls

- **Hand-waving the effort estimate.** Don't say "a few weeks". Use the per-driver cost table. Defend your number.
- **Treating IRSA as a 1:1 to WI.** It usually is; the few that aren't are exactly the ones that bite. Score each role.
- **Underestimating data moves.** Heterogeneous data store changes are projects of their own. If the user wants to migrate the K8s fleet but keep AWS RDS, that's a valid choice; surface it.
- **Listing risks without guardrails.** A risk without a guardrail is a guess. Every risk gets a measurable guardrail.

## References

- **Canonical sources**: [reference/sources.md](../../../../reference/sources.md).
- [Migrate from Amazon EKS to GKE — Plan and build your foundation](https://docs.cloud.google.com/architecture/migrate-amazon-eks-to-gke) — Google's matching planning phase.
- [Landing zone design in Google Cloud](https://cloud.google.com/architecture/landing-zones) — informs phasing.
- The discovery phase's extraction pipeline — produces the input.
- [templates/readiness-report.md](../../../../templates/readiness-report.md) — the output template.
- [docs/core/glossary.md](../../../../docs/core/glossary.md) — service map for translation feasibility.
