# Canonical Sources

The authoritative external references the agent leans on. Each phase's knowledge documents link here. When a phase cites a specific page, that page appears in this file too — the phase links to the canonical doc and to the sub-section here that summarizes our use of it. The owners named in the **Where it is used** column are phase directories under `servers/phases/` (or the DAG server in `servers/dag/`), not skills.

If a recommendation cannot be cited from one of these sources (or from a phase's own first-hand exercise of an environment), it is *tool-opinionated* and labeled as such. We never present the agent's heuristics as Google guidance.

> **Pinning note.** GCP doc URLs occasionally change as products are renamed or pages are restructured (e.g., "Workload Identity" → "Workload Identity Federation for GKE"). When a link 404s, search the page title — the canonical content usually still exists at a new URL. PR welcome to update.

---

## A — Headline migration sources

| Source | URL | Where it is used |
|---|---|---|
| Migrate from AWS to Google Cloud: **Migrate from Amazon EKS to GKE** (Architecture Center series) | https://docs.cloud.google.com/architecture/migrate-amazon-eks-to-gke | Whole run (DAG server), plus discovery / assessment / deployment |
| Migrate containers to Google Cloud: Migrate from Kubernetes to GKE | https://docs.cloud.google.com/architecture/migrating-containers-kubernetes-gke | workload, translation |
| Migrate from Amazon RDS / Aurora MySQL to Cloud SQL for MySQL | https://docs.cloud.google.com/architecture/migrate-aws-rds-to-sql-mysql | deployment |
| Migrate your EKS attached cluster (alternative path: attach EKS as a fleet member, migrate workloads incrementally) | https://cloud.google.com/kubernetes-engine/multi-cloud/docs/attached/eks/how-to/migrate-cluster | DAG server (alternative discovery path) |
| What's new in the Architecture Center | https://cloud.google.com/architecture/release-notes | (general — watch for updates) |
| Brain Corp migrates from AWS EKS to GKE Autopilot (Google Cloud Blog) | https://cloud.google.com/blog/products/containers-kubernetes/brain-corp-migrates-from-aws-eks-to-gke-autopilot | real-world precedent |

## B — Foundations and landing zone

| Source | URL | Where it is used |
|---|---|---|
| Hardening your cluster's security | https://docs.cloud.google.com/kubernetes-engine/docs/how-to/hardening-your-cluster | landing zone (the 34 controls fold in directly) |
| Best practices for enterprise organizations | https://cloud.google.com/architecture/best-practices-for-enterprise-organizations | landing zone (project hierarchy, IAM patterns) |
| Landing zone design in Google Cloud | https://cloud.google.com/architecture/landing-zones | landing zone |
| Cloud Foundation Fabric (Google reference Terraform, GitHub) | https://github.com/GoogleCloudPlatform/cloud-foundation-fabric | reference/terraform/ — patterns inspired by Fabric |
| GKE security posture dashboard | https://cloud.google.com/kubernetes-engine/docs/concepts/about-security-posture-dashboard | landing zone |
| Setting up clusters with Shared VPC | https://cloud.google.com/kubernetes-engine/docs/how-to/cluster-shared-vpc | landing zone |
| GKE Autopilot vs Standard | https://cloud.google.com/kubernetes-engine/docs/concepts/choose-cluster-mode | landing zone (decision points) |

## C — Networking, ingress, edge

| Source | URL | Where it is used |
|---|---|---|
| GKE Gateway controller | https://cloud.google.com/kubernetes-engine/docs/concepts/gateway-api | translation |
| Choosing a load balancer | https://cloud.google.com/load-balancing/docs/choosing-load-balancer | translation |
| Cloud Armor preconfigured WAF rules | https://docs.cloud.google.com/armor/docs/waf-rules | translation (verbatim rule names + tuning) |
| Cloud Armor rule tuning (sensitivity / paranoia levels) | https://docs.cloud.google.com/armor/docs/rule-tuning | translation |
| Cloud Armor adaptive protection | https://docs.cloud.google.com/armor/docs/adaptive-protection-overview | translation |
| Certificate Manager | https://cloud.google.com/certificate-manager/docs | translation |
| Cloud DNS routing policies | https://cloud.google.com/dns/docs/zones/manage-routing-policies | translation |
| Private Service Connect | https://cloud.google.com/vpc/docs/private-service-connect | landing zone, translation |

## D — Identity

| Source | URL | Where it is used |
|---|---|---|
| Workload Identity Federation for GKE | https://cloud.google.com/kubernetes-engine/docs/concepts/workload-identity | translation, workload |
| Workload Identity Federation with AWS or Azure (cross-cloud) | https://cloud.google.com/iam/docs/workload-identity-federation-with-other-clouds | translation (cross-cloud federation) |
| Best practices for using service accounts | https://cloud.google.com/iam/docs/best-practices-for-using-and-managing-service-accounts | translation, landing zone |

## E — Workloads and admission

| Source | URL | Where it is used |
|---|---|---|
| Autopilot resource requests/limits | https://cloud.google.com/kubernetes-engine/docs/concepts/autopilot-resource-requests | workload (the deny list) |
| Pod Security Standards on GKE | https://cloud.google.com/kubernetes-engine/docs/how-to/podsecurityadmission | workload |
| Removed Kubernetes APIs by version | https://kubernetes.io/docs/reference/using-api/deprecation-guide/ | workload (manifest validity) |
| Policy Controller | https://cloud.google.com/anthos-config-management/docs/concepts/policy-controller | unowned — no phase covers this yet |
| Binary Authorization | https://cloud.google.com/binary-authorization/docs | deployment |

## F — Storage

| Source | URL | Where it is used |
|---|---|---|
| Persistent Disk types | https://cloud.google.com/compute/docs/disks/persistent-disks | translation |
| Hyperdisk overview | https://cloud.google.com/compute/docs/disks/hyperdisks | translation |
| Filestore tiers | https://cloud.google.com/filestore/docs/service-tiers | translation |
| Parallelstore | https://cloud.google.com/parallelstore/docs | translation |
| Backup for GKE | https://cloud.google.com/kubernetes-engine/docs/add-on/backup-for-gke | translation, deployment |
| Migrate stateful workloads to GKE | https://cloud.google.com/architecture/best-practices-stateful-applications-gke | translation, deployment |

## G — Registry and supply chain

| Source | URL | Where it is used |
|---|---|---|
| Artifact Registry overview | https://cloud.google.com/artifact-registry/docs | deployment |
| Artifact Analysis (vulnerability scanning) | https://cloud.google.com/artifact-analysis/docs | deployment |
| Sigstore + Binary Authorization on GKE | https://cloud.google.com/binary-authorization/docs/setting-up-cosign | deployment |
| SLSA framework | https://slsa.dev | deployment (supply-chain references) |

## H — Data

| Source | URL | Where it is used |
|---|---|---|
| Database Migration Service overview | https://cloud.google.com/database-migration/docs | deployment |
| DMS — configure Postgres source (RDS) | https://docs.cloud.google.com/database-migration/docs/postgres/configure-source-database | deployment (verbatim pre-flight) |
| DMS — configure MySQL source (RDS) | https://docs.cloud.google.com/database-migration/docs/mysql/configure-source-database | deployment (verbatim pre-flight) |
| DMS — Postgres known limitations | https://docs.cloud.google.com/database-migration/docs/postgres/known-limitations | deployment |
| DMS — MySQL known limitations | https://docs.cloud.google.com/database-migration/docs/mysql/known-limitations | deployment |
| Storage Transfer Service | https://cloud.google.com/storage-transfer/docs | deployment |
| Cloud SQL HA / DR | https://cloud.google.com/sql/docs/postgres/high-availability | deployment |
| AlloyDB migration guide | https://cloud.google.com/alloydb/docs/migration | deployment (engine change scoping) |

## I — Observability

| Source | URL | Where it is used |
|---|---|---|
| Managed Service for Prometheus | https://cloud.google.com/stackdriver/docs/managed-prometheus | landing zone |
| Migrating from CloudWatch to Cloud Operations | https://cloud.google.com/architecture/migration-to-google-cloud-monitoring-from-cloudwatch | unowned — no phase covers this yet (metric-name table) |
| SLO monitoring | https://cloud.google.com/stackdriver/docs/solutions/slo-monitoring | landing zone |
| Cloud Trace + OpenTelemetry on GKE | https://cloud.google.com/trace/docs/setup/opentelemetry | unowned — no phase covers this yet |
| OpenTelemetry on GCP | https://cloud.google.com/stackdriver/docs/instrumentation/setup | unowned — no phase covers this yet |

## J — Cutover, reliability, SRE

| Source | URL | Where it is used |
|---|---|---|
| The SRE Workbook (free online) | https://sre.google/workbook/table-of-contents/ | unowned — no phase covers this yet (canary, error budgets, postmortems) |
| Site Reliability Engineering (book, free online) | https://sre.google/sre-book/table-of-contents/ | (cross-cutting reliability principles) |
| Anthos Service Mesh multi-cluster | https://cloud.google.com/service-mesh/docs/managed/multi-cluster | unowned — no phase covers this yet (mesh routing path) |
| GKE Gateway traffic splitting | https://cloud.google.com/kubernetes-engine/docs/how-to/gateway-traffic-splitting | unowned — no phase covers this yet |

## K — FinOps / cost optimization

| Source | URL | Where it is used |
|---|---|---|
| **Best practices for running cost-optimized Kubernetes applications on GKE** (canonical) | https://docs.cloud.google.com/architecture/best-practices-for-running-cost-effective-kubernetes-applications-on-gke | unowned — no phase covers this yet (verbatim methodology) |
| About Vertical Pod Autoscaling | https://docs.cloud.google.com/kubernetes-engine/docs/concepts/verticalpodautoscaler | unowned — no phase covers this yet (OOM safety buffer) |
| Configuring HPA | https://cloud.google.com/kubernetes-engine/docs/how-to/horizontal-pod-autoscaling | unowned — no phase covers this yet |
| Cluster autoscaler | https://cloud.google.com/kubernetes-engine/docs/how-to/cluster-autoscaler | unowned — no phase covers this yet |
| Node auto-provisioning | https://cloud.google.com/kubernetes-engine/docs/how-to/node-auto-provisioning | translation |
| Spot VMs on GKE | https://cloud.google.com/kubernetes-engine/docs/how-to/spot-vms | unowned — no phase covers this yet |
| Compute Engine committed-use discounts | https://cloud.google.com/compute/docs/instances/signing-up-committed-use-discounts | unowned — no phase covers this yet |
| CUD analysis | https://cloud.google.com/billing/docs/how-to/cud-analysis | unowned — no phase covers this yet |

## L — Outside cloud.google.com

| Source | URL | Where it is used |
|---|---|---|
| Kubernetes Gateway API | https://gateway-api.sigs.k8s.io/ | translation |
| Kubernetes NetworkPolicy | https://kubernetes.io/docs/concepts/services-networking/network-policies/ | translation, workload |
| Pod Security Admission | https://kubernetes.io/docs/concepts/security/pod-security-admission/ | workload |
| Cilium / Dataplane V2 | https://cilium.io/ | landing zone, translation |
| Strimzi (Kafka on K8s) | https://strimzi.io/ | deployment (self-managed Kafka path) |
| Velero | https://velero.io/ | deployment |
| AWS EKS user guide | https://docs.aws.amazon.com/eks/latest/userguide/ | discovery (source-side accuracy) |
| Karpenter docs | https://karpenter.sh/docs/ | discovery, translation |

---

## What this file is *not*

- Not a substitute for reading the linked docs. Each phase cites the *page* that grounds its recommendations; the page is the source of truth.
- Not a list of every page on cloud.google.com. Only pages the agent actually uses.
- Not curated for completeness over time. PRs to add or correct entries are welcome; see CONTRIBUTING.md.

## See also

- [lessons-from-the-field.md](lessons-from-the-field.md) — companion knowledge base of practitioner war stories and incident reports. Where this file documents canonical *guidance*, that file documents what has *actually broken* in real migrations. Both inform the phases.
