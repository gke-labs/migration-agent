# Migration Readiness Report — {{customer_name}}

**Run ID:** {{run_id}}
**Prepared:** {{date}}
**Migration window:** {{window_start}} → {{window_end}} ({{window_days}} days)
**Compliance scope:** {{compliance_list}}

---

## 1. Executive summary

- **Overall grade:** {{overall_grade}} (Ready / Tractable / High-risk / Blocked).
- **Workloads in scope:** {{total_workloads}} ({{ready_count}} Ready · {{tractable_count}} Tractable · {{high_risk_count}} High-risk · {{blocked_count}} Blocked).
- **Effort estimate:** P50 {{p50_days}} engineer-days · P90 {{p90_days}} engineer-days, based on org coefficient {{org_coefficient}}.
- **Top three blockers:** {{top_blockers}}.
- **Recommendation:** {{recommendation_one_line}}

## 2. Scope

Discovered {{total_clusters}} EKS clusters across {{total_regions}} regions, {{total_namespaces}} namespaces, {{total_workloads}} workloads, {{total_pvcs}} PVCs, {{total_data_systems}} external data systems.

| Cluster      | Region       | K8s version | Nodes | Workloads | PVCs | Data deps |
|--------------|--------------|-------------|-------|-----------|------|-----------|
| {{cluster_a}} | {{region_a}} | {{ver_a}}   | {{n_a}} | {{w_a}}   | {{p_a}} | {{d_a}}    |

## 3. Scorecard

### Per-cluster summary

| Cluster      | Stateless-ness | Identity | Network | Data | Composite |
|--------------|----------------|----------|---------|------|-----------|
| {{cluster_a}} | {{...}}         | {{...}}  | {{...}} | {{...}} | {{...}}    |

### Workloads in High-risk or Blocked buckets

| Cluster | Namespace / Workload | Reason | Resolution path |
|---------|----------------------|--------|------------------|
| {{...}} | {{...}}              | {{...}} | {{...}}          |

## 4. Blockers

For each blocker, named, categorized, with a resolution path and owner.

### B-001 — {{blocker_title}}

- **Category:** {{category}}
- **Affected workloads:** {{workloads}}
- **Why it's a blocker:** {{rationale}}
- **Resolution path:** {{path}}
- **Owner:** {{owner}}
- **Target close date:** {{date}}

(Repeat per blocker.)

## 5. Phased plan

| Phase                | Duration | Deliverable                                                   | Gate to next phase                              |
|----------------------|----------|---------------------------------------------------------------|-------------------------------------------------|
| 1. Foundation        | 2 wk     | Landing zone applied; identity, network, observability stack | Terraform applied, smoke tests pass             |
| 2. First-wave (Tier-2 stateless) | 2 wk | First cohort cut over                                  | All P1 SLOs green for 7 days                    |
| 3. Stateful / Tier-1 | 4 wk     | Per-workload runbooks executed                                | Each cutover: all gates green for 24 h          |
| 4. Tier-0            | 2 wk     | Final cutovers; extended soak                                  | SLO-tied burn-rate alerts clean for 7 days      |
| 5. Decommission      | 2 wk     | EKS scaled down; AWS resources retired                        | Source decommission complete; final summary     |

## 6. Effort estimate

Driver counts and unit costs:

| Driver                                    | Count       | Unit cost (eng-days) | Subtotal     |
|-------------------------------------------|-------------|----------------------|--------------|
| Workloads — Ready                         | {{...}}     | 0.5                  | {{...}}      |
| Workloads — Tractable                     | {{...}}     | 1.5                  | {{...}}      |
| Workloads — High-risk                     | {{...}}     | 4                    | {{...}}      |
| ALBs / NLBs                               | {{...}}     | 0.5                  | {{...}}      |
| IRSA roles                                | {{...}}     | 0.25                 | {{...}}      |
| RWX PVCs                                  | {{...}}     | 1                    | {{...}}      |
| RDS → Cloud SQL homogeneous moves         | {{...}}     | 2                    | {{...}}      |
| Aurora → AlloyDB engine-change            | {{...}}     | 8                    | {{...}}      |
| Cluster baselines (landing zones)         | {{...}}     | 5                    | {{...}}      |
| Subtotal                                  |             |                      | {{subtotal}} |
| Org coefficient                           |             |                      | × {{coef}}   |
| **P50**                                   |             |                      | **{{p50}}**  |
| **P90 (× 1.5)**                           |             |                      | **{{p90}}**  |

## 7. Risks and guardrails

| Risk                                        | Likelihood | Impact | Guardrail                                          |
|---------------------------------------------|------------|--------|----------------------------------------------------|
| RDS → Cloud SQL replication lag at cutover  | Medium     | High   | Replication-lag SLO; auto-pause cutover if > 30 s for 5 min |
| Cross-cloud egress cost during co-existence | Medium     | Medium | Budget alert at 80% / 100% of co-existence budget   |
| Workload Identity binding drift             | Low        | Medium | CI check comparing IRSA → WI map to live state daily |
| GKE cluster cost overrun vs EKS baseline    | Medium     | Medium | Monthly FinOps review; right-size after 30-day soak |
| Custom CNI runtime parity                   | Low        | High   | Stage cluster with Dataplane V2; canary tier-2 first |

## 8. Open questions for the customer

- Compliance-scope confirmation: in-scope for {{regimes}}? Any regime not present in `obs-prod` audit logs?
- Cost ceiling per environment: confirm $/mo for prod, non-prod.
- Cutover strategy: weighted DNS, mesh routing, or edge-proxy split?
- Co-existence period acceptable: 30 days default, or shorter / longer?
- Source decommission: 14-day cool-down default, or modified per regime?
- Identity provider: Workforce Identity Federation source (Okta / Azure AD / Google IdP)?
- Tier-0 list confirmation: workloads {{tier0_list}} are tier-0; any additions?

## 9. References

- `01-discovery/inventory.json` — full inventory
- `01-discovery/inventory-summary.md` — narrative summary
- `02-assessment/scorecard.json` — per-workload scoring
- `02-assessment/blockers.md` — blocker checklist
- `00-orchestrator-state.json` — run state
