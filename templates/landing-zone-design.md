# GKE Landing Zone Design — {{customer_name}}

**Run ID:** {{run_id}}
**Date:** {{date}}

---

## 1. Topology

```
Org: {{org_name}}
├── Folder: prod
│   ├── Project: net-prod-host        (Shared VPC)
│   ├── Project: gke-prod-clusters    (GKE)
│   ├── Project: data-prod
│   └── Project: obs-prod
├── Folder: nonprod
│   └── ... (mirror of prod)
└── Folder: shared
    ├── Project: artifact-registry
    ├── Project: cicd
    └── Project: terraform-state
```

## 2. Clusters

| Cluster name | Project              | Region        | Type      | Channel  | Private nodes | Workload Identity | Dataplane V2 | BinAuthz   |
|--------------|----------------------|---------------|-----------|----------|----------------|--------------------|---------------|------------|
| prod-primary | gke-prod-clusters    | {{region}}    | Autopilot | Stable   | Yes            | Yes                | Yes           | Evaluation |
| nonprod-primary | gke-nonprod-clusters | {{region}}  | Standard  | Regular  | Yes            | Yes                | Yes           | Disabled   |

### Node pools (Standard clusters only)

| Cluster        | Pool         | Machine family | Min / Max | Spot? | Taints                                |
|----------------|--------------|----------------|-----------|-------|---------------------------------------|
| nonprod-primary | default      | e2-standard-4  | 3 / 12    | No    | (none)                                |
| nonprod-primary | spot         | e2-standard-4  | 0 / 30    | Yes   | `cloud.google.com/gke-spot=true:NoSchedule` |

## 3. Network

### Per-environment Shared VPC

| Region        | Nodes CIDR         | Pods CIDR          | Services CIDR     |
|---------------|--------------------|--------------------|-------------------|
| {{region}}    | {{nodes_cidr}}     | {{pods_cidr}}      | {{services_cidr}} |

The three ranges are the `proposed_target_ranges` echoed by `resolve_lz_decision`, chosen
clear of the source address space discovery recorded. Source ranges kept clear of:
{{source_ranges}} (from the inventory's `address_space`; "supplied by the user" when the
echo reported no recorded range and the user gave them).

Cloud NAT and Cloud Router per region.
Private Google Access on every subnet.
Private Service Connect endpoints for `googleapis.com`.

### Hierarchical firewall (Org-level)

- Deny all `INGRESS` from `0.0.0.0/0` except IAP ranges and GFE health-check ranges.
- Allow internal egress within the VPC.

## 4. IAM groups

| Group                                    | Role(s)                                            | Scope           |
|------------------------------------------|----------------------------------------------------|-----------------|
| gcp-org-admins@{{domain}}                 | roles/resourcemanager.organizationAdmin            | Org             |
| gcp-platform-admins@{{domain}}            | roles/resourcemanager.folderAdmin, roles/compute.networkAdmin | Folders + host |
| gcp-platform-readers@{{domain}}           | roles/viewer                                       | Org             |
| gcp-sre-prod@{{domain}}                   | roles/container.clusterAdmin, roles/monitoring.viewer, roles/logging.viewer | Prod folder |
| gcp-developers-prod@{{domain}}            | roles/container.developer + namespace RBAC         | Prod cluster    |

## 5. Org policies

| Policy                                       | State                            |
|----------------------------------------------|----------------------------------|
| compute.vmExternalIpAccess                   | Deny all                         |
| compute.requireShieldedVm                    | Enforce                          |
| compute.trustedImageProjects                 | Allow `cos-cloud`, `gke-node-images` |
| iam.disableServiceAccountKeyCreation         | Enforce                          |
| iam.allowedPolicyMemberDomains               | `{{customer_org_id}}` only       |
| storage.uniformBucketLevelAccess             | Enforce                          |
| compute.disableSerialPortAccess              | Enforce                          |

## 6. Observability

- Metrics scope project per environment (`obs-prod`, `obs-nonprod`).
- Default cluster dashboards in Cloud Monitoring (auto-provisioned).
- Managed Service for Prometheus enabled on every cluster.
- Cloud Logging buckets at project default; long-term retention via log sinks if compliance requires.

## 7. Budgets

| Folder    | 50% alert | 80% alert | 100% alert | Notification channel        |
|-----------|-----------|-----------|------------|------------------------------|
| prod      | $X        | $Y        | $Z         | platform-finops@{{domain}}   |
| nonprod   | $X        | $Y        | $Z         | platform-finops@{{domain}}   |

## 8. Apply plan

```
terraform plan: 4 projects, 2 networks, 4 clusters, 12 IAM bindings, 3 org policies
Estimated apply time: 25 minutes
```

Apply order:

1. Org-level org policies.
2. Folders + projects.
3. Shared VPC + subnets + Cloud NAT.
4. KMS keys.
5. GKE clusters (regional).
6. Fleet membership + features.
7. IAM bindings (project + cluster).
8. Budgets + notification channels.
9. Verify.

## 9. Verification checklist

- [ ] `gcloud projects list --filter='parent.id={{folder_id}}'` returns expected projects.
- [ ] `gcloud container clusters list` returns expected clusters.
- [ ] `gcloud container clusters describe …` shows Workload Identity, Dataplane V2, Gateway API enabled.
- [ ] `kubectl get ns` succeeds via private endpoint or authorized-network.
- [ ] `kubectl get crd gateways.gateway.networking.k8s.io` succeeds (Gateway API installed).
- [ ] Test deploy of a no-op pod confirms Cloud Logging and GMP scrape work.
- [ ] Org policy inheritance: `gcloud org-policies list --project gke-prod-clusters` shows expected policies.
- [ ] Billing budget alerts have working notification channels.

## 10. Open decisions

- {{decision_1}}
- {{decision_2}}
