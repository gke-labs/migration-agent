# EKS ↔ GKE Glossary

This is the canonical AWS-to-GCP translation map the agent consults. It covers the services, concepts, and terms you will see most often in a real migration. Skills cite this document when they translate manifests; SREs use it to read each other's diagrams.

## Compute

| EKS / AWS                      | GKE / GCP                                               | Notes |
|--------------------------------|---------------------------------------------------------|-------|
| EKS cluster                    | GKE cluster (Autopilot or Standard)                     | Autopilot is the closer match to "managed K8s + node management" if you do not customize node OS. |
| Managed Node Group             | Node pool                                               | One-to-one. |
| Karpenter                      | Node Auto-Provisioning (NAP) + Cluster Autoscaler       | NAP creates new node pools on demand; Karpenter creates nodes directly. The lifecycle of a "Karpenter NodeClaim" maps to a NAP-created node in a NAP-managed pool. |
| EC2 instance type (m5.large)   | GCE machine type (e2-standard-2, n2-standard-2, c3-…)   | See `reference/service-mapping.md` for the price/perf-equivalent table. |
| Spot instance                  | Spot VM (formerly preemptible)                          | GCP Spot has no max-24h lifetime; preemption notice is 30s. |
| Bottlerocket / AL2 nodes       | Container-Optimized OS (COS) / Ubuntu                   | COS is the default and recommended for almost all workloads. |
| Fargate (EKS Fargate profile)  | Autopilot                                               | Different model, similar value prop: no node ops. |

## Identity & access

| EKS / AWS                              | GKE / GCP                                              | Notes |
|----------------------------------------|--------------------------------------------------------|-------|
| IAM user                               | (Use Workforce Identity Federation; do not create new GCP users) | Map identity through your IdP, not by recreating users in GCP. |
| IAM role                               | IAM role + service account                             | "Role" semantics differ: AWS roles are assumed; GCP roles are bound to principals. |
| IRSA (IAM Roles for Service Accounts)  | Workload Identity Federation for GKE                   | Both use OIDC. WI binds a Kubernetes ServiceAccount to a Google Service Account. |
| Trust policy on IAM role               | IAM policy with `roles/iam.workloadIdentityUser`       | The KSA principal looks like `serviceAccount:PROJECT.svc.id.goog[NAMESPACE/KSA]`. |
| AWS STS AssumeRole                     | gcloud iam service-accounts impersonate                | Used for cross-account / cross-project automation. |
| EKS access entries / aws-auth ConfigMap | GKE IAM + RBAC                                         | EKS access entries are GKE's IAM bindings on the cluster resource plus K8s RBAC. |

## Networking

| EKS / AWS                              | GKE / GCP                                              | Notes |
|----------------------------------------|--------------------------------------------------------|-------|
| VPC                                    | VPC (typically Shared VPC for multi-team)              | GCP Shared VPC has no AWS-direct equivalent; it is the recommended pattern for production. |
| Subnet                                 | Subnet                                                 | GCP subnets are *regional*, not zonal. |
| Internet Gateway                       | Default route + Cloud NAT for egress                   | GCP has no IGW concept; nodes get egress via Cloud NAT. |
| NAT Gateway                            | Cloud NAT                                              | Cloud NAT is regional and managed. |
| Transit Gateway                        | Network Connectivity Center hub                        | Multi-VPC, multi-region routing. |
| Direct Connect                         | Cloud Interconnect (Dedicated or Partner)              | For private connectivity to on-prem. |
| VPC Peering                            | VPC Peering or Network Connectivity Center             | NCC is preferred for hub-and-spoke. |
| Security group                         | VPC firewall rule (network tag-targeted)               | GCP firewall is at the VPC level, not per-resource. |
| Network ACL                            | (No direct equivalent — use VPC firewall rules and hierarchical firewall policies) | |
| AWS PrivateLink                        | Private Service Connect (PSC)                          | Same idea: private endpoints to producer services. |
| Route 53                               | Cloud DNS                                              | |
| CoreDNS (EKS cluster DNS, Corefile)    | Cloud DNS for GKE (`dns_config`), NodeLocal DNSCache   | Managed data plane, no resolver pods. Stub domains and upstreams → `kube-dns` ConfigMap (Standard) or Cloud DNS forwarding zones; `hosts` → private zone; `rewrite`/`template` → no equivalent. |
| Route 53 health checks                 | Cloud DNS routing policies + health checks (or external) | |
| Application Load Balancer (ALB)        | Application Load Balancer (Global external HTTPS LB)   | Reach via GKE Gateway API. |
| Network Load Balancer (NLB)            | Network Load Balancer (passthrough or proxy)           | TCP/UDP, region-scoped. |
| AWS Load Balancer Controller           | GKE Gateway controller (gke-l7-global-external-managed) + GKE Service controller | Native managed controller on GKE. |
| Ingress (annotated for ALB)            | Gateway API (`Gateway` + `HTTPRoute`)                  | Ingress resources still work but are legacy on GKE; new work should use Gateway API. |
| AWS WAF                                | Cloud Armor                                            | WAF + DDoS protection. Attached to the load balancer's backend. |
| AWS Certificate Manager                | Google-managed SSL certificate / Certificate Manager   | Use Certificate Manager for wildcard and SAN certs. |
| AWS Shield                             | Cloud Armor managed protection                         | DDoS. |

## Storage

| EKS / AWS                              | GKE / GCP                                              | Notes |
|----------------------------------------|--------------------------------------------------------|-------|
| EBS gp3                                | Persistent Disk Balanced (`pd-balanced`)               | Closest perf/cost match. |
| EBS io1 / io2                          | Persistent Disk SSD (`pd-ssd`), or Hyperdisk Extreme where the node pools' machine series supports it | `pd-ssd` is the machine-series-safe default; state the choice in tradeoffs. |
| EBS st1                                | Persistent Disk Standard (`pd-standard`)               | HDD-class. |
| EFS                                    | Filestore (Basic, Enterprise, Zonal) or GCS Fuse       | Filestore for POSIX NFS; GCSFuse for object-mounted-as-fs. |
| FSx Lustre                             | Parallelstore                                          | High-performance parallel FS. |
| FSx for NetApp ONTAP                   | NetApp Volumes                                         | First-party NetApp on GCP. |
| EBS CSI driver (`ebs.csi.aws.com`)     | PD CSI driver (`pd.csi.storage.gke.io`)                | StorageClass parameters differ; see `storage-translation`. |
| EFS CSI driver                         | Filestore CSI driver                                   | |
| Snapshot                               | Snapshot                                               | Both incremental, both regional/multi-regional. |
| S3                                     | Cloud Storage (GCS)                                    | API surfaces differ; Storage Transfer Service handles the cross-cloud move. |
| ECR                                    | Artifact Registry                                      | AR supports container, language packages, and OCI artifacts in one service. |

## Data

| EKS / AWS                              | GKE / GCP                                              | Notes |
|----------------------------------------|--------------------------------------------------------|-------|
| RDS for PostgreSQL                     | Cloud SQL for PostgreSQL or AlloyDB for PostgreSQL     | AlloyDB is the higher-performance option; Cloud SQL is the closer 1:1. |
| RDS for MySQL                          | Cloud SQL for MySQL                                    | |
| Aurora PostgreSQL                      | AlloyDB                                                | Engine compatibility, but feature surface differs. |
| Aurora MySQL                           | Cloud SQL for MySQL (large tier)                       | No exact analogue; performance tuning required. |
| ElastiCache Redis                      | Memorystore for Redis or Memorystore for Redis Cluster | Cluster mode for sharded. |
| ElastiCache Memcached                  | Memorystore for Memcached                              | |
| DynamoDB                               | Bigtable, Firestore, or Spanner                        | Heterogeneous; pick by access pattern. The agent scopes this and hands back. |
| Kinesis Data Streams                   | Pub/Sub or Managed Service for Apache Kafka            | Not Pub/Sub Lite: deprecated, closed to new customers since 2024-09-24, turndown 2027-01-31. |
| MSK (Kafka)                            | Managed Service for Apache Kafka                       | GA since 2024-11. |
| Neptune                                | (No direct equivalent)                                 | Re-platform onto AlloyDB AGE or self-managed graph. |
| Redshift                               | BigQuery                                               | Different model; not a 1:1 port. |
| Glue                                   | Dataproc / Dataform / Cloud Data Fusion                | Multiple analogues depending on what Glue is doing. |

## Observability

| EKS / AWS                              | GKE / GCP                                              | Notes |
|----------------------------------------|--------------------------------------------------------|-------|
| CloudWatch Metrics                     | Cloud Monitoring                                       | |
| CloudWatch Logs                        | Cloud Logging                                          | |
| CloudWatch Container Insights          | GKE-native dashboards in Cloud Monitoring              | Auto-enabled on most GKE clusters. |
| AWS X-Ray                              | Cloud Trace                                            | Both OpenTelemetry-friendly. |
| Managed Prometheus (AMP)               | Managed Service for Prometheus (GMP)                   | Drop-in PromQL. |
| Managed Grafana (AMG)                  | (Self-host Grafana or use Cloud Monitoring dashboards) | No GCP-managed Grafana GA; verify current state. |
| Fluent Bit / OTel collector            | Same — works unchanged                                 | Reconfigure exporters to point to Cloud Logging / GMP. |

## Secrets, certs, supply chain

| EKS / AWS                              | GKE / GCP                                              | Notes |
|----------------------------------------|--------------------------------------------------------|-------|
| AWS Secrets Manager                    | Secret Manager                                         | |
| AWS Systems Manager Parameter Store    | Secret Manager (or Cloud KMS for keys only)            | |
| AWS KMS                                | Cloud KMS                                              | |
| AWS ACM                                | Certificate Manager                                    | |
| Image scanning in ECR                  | Artifact Analysis (formerly Container Analysis)        | |
| Signing / Sigstore via Notary          | Binary Authorization with Sigstore                     | |
| EKS Pod Identity                       | Workload Identity                                      | |

## Cluster lifecycle

| EKS / AWS                              | GKE / GCP                                              | Notes |
|----------------------------------------|--------------------------------------------------------|-------|
| `eksctl create cluster`                | `gcloud container clusters create-auto …` (Autopilot) or `... clusters create …` (Standard) | |
| EKS add-on (managed)                   | GKE add-on or installed manifest                       | GKE installs many capabilities natively (Dataplane V2, GMP, GKE Gateway). |
| EKS API endpoint (public/private)      | Control plane endpoint (private cluster, authorized networks) | |
| `aws-auth` ConfigMap                   | (Removed in favor of GKE IAM + RBAC)                   | |
| Cluster Autoscaler                     | Cluster Autoscaler (built-in) + NAP                    | |
| Vertical Pod Autoscaler (manual)       | VPA (built-in)                                         | |
| HPA                                    | HPA                                                    | Same. |
| KEDA                                   | KEDA                                                   | Same. Works on both. |
| Fluxcd / ArgoCD                        | Same — works unchanged                                 | Or use Config Sync (GKE-native GitOps). |
| Service Mesh (App Mesh, Istio)         | Anthos Service Mesh (managed Istio) or self-managed Istio | App Mesh has no GCP equivalent; re-platform onto ASM. |

## Org-level / billing

| EKS / AWS                              | GKE / GCP                                              | Notes |
|----------------------------------------|--------------------------------------------------------|-------|
| AWS Organizations                      | GCP Organization + Folders                             | |
| OU                                     | Folder                                                 | |
| Account                                | Project                                                | |
| Cost Allocation Tags                   | Labels (resource) and Billing labels                   | |
| Service Quotas                         | Quotas (per project, per region)                       | |
| Reserved Instances / Savings Plans     | Committed Use Discounts (CUDs)                         | Resource-based or spend-based. |

## Terms that don't translate cleanly

| Term                                   | Why it's tricky                                        |
|----------------------------------------|--------------------------------------------------------|
| "AWS account boundary"                 | GCP's equivalent boundary is the project, but org policy and folder hierarchy do work AWS Orgs cannot. Designs need re-thinking, not 1:1 mapping. |
| "Availability Zone"                    | GCP zones exist but most managed services are regional by default; you typically design at the *region* layer. |
| "VPC endpoint (interface)"             | Use Private Service Connect; the configuration model is different. |
| "Reserved Instance"                    | CUDs apply differently (per-resource vs spend) and overlap with Sustained Use Discounts. |
| "Spot fleet"                           | Spot VMs do not have a fleet abstraction; node pools with mixed-instance and Spot fill the role. |

When in doubt, search this document first. If the term isn't here, it probably needs a discussion before it gets translated.
