# AWS → GCP Service Mapping (GKE Agentic Migration Reference)

A working reference of how AWS services translate to GCP services for the workloads GKE Agentic Migration is most likely to encounter. Each row records the *closest* GCP service plus a "comparable on" axis — what the analogue is good at — and a "differs on" axis — where you should expect re-architecture, not re-configuration.

Use this when planning, not when coding. The skill files cite this document so you don't have to re-derive every mapping.

## Compute / scheduling

| AWS                          | GCP                                  | Comparable on                                           | Differs on                                                                 |
|------------------------------|--------------------------------------|---------------------------------------------------------|----------------------------------------------------------------------------|
| EKS                          | GKE Standard                         | Managed K8s, full node control                          | Default networking (Dataplane V2 / Cilium), built-in features (GMP, Gateway API), regional default |
| EKS Fargate                  | GKE Autopilot                        | No node ops, per-pod billing                            | Resource constraints, admission policy, no privileged                      |
| EC2                          | Compute Engine                       | VMs                                                     | Live migration default-on (no surprise reboots from host maintenance)      |
| ECS                          | Cloud Run + Cloud Run Jobs           | Container-as-a-service                                  | No 1:1; ECS Fargate is closer to Cloud Run                                  |
| Lambda                       | Cloud Run / Cloud Functions          | Event-driven compute                                    | Cold-start, runtime, packaging differ                                       |
| Batch                        | Batch                                | Managed batch                                           | Different scheduling primitives                                            |
| Auto Scaling Group           | Managed Instance Group               | Auto-scaling VMs                                        | Health checks and instance templates differ                                |
| Karpenter                    | NAP + CA                             | Just-in-time node creation                              | Karpenter creates Nodes; NAP creates NodePools                              |

## Networking

| AWS                          | GCP                                  | Comparable on                                           | Differs on                                                                 |
|------------------------------|--------------------------------------|---------------------------------------------------------|----------------------------------------------------------------------------|
| VPC                          | VPC                                  | Software-defined networking                             | GCP subnets are *regional*; firewall rules at VPC level                    |
| Subnet                       | Subnet                               | CIDR ranges                                             | Regional, secondary IP ranges for pods/services                             |
| IGW                          | Default route + Cloud NAT            | Egress to internet                                      | No IGW concept; egress through Cloud NAT                                    |
| NAT Gateway                  | Cloud NAT                            | Outbound NAT                                            | Cloud NAT is regional, integrated with subnets                              |
| Transit Gateway              | Network Connectivity Center          | Multi-VPC / multi-network routing                       | Hub-and-spoke model differs                                                |
| VPC Peering                  | VPC Peering / NCC                    | Network-to-network                                      | NCC is preferred for hub-and-spoke                                          |
| PrivateLink                  | Private Service Connect              | Private endpoints                                       | Producer/consumer model; PSC has NEGs                                       |
| Direct Connect               | Cloud Interconnect (Dedicated/Partner) | Private circuit to on-prem                            | Capacity tiers differ; pricing per port differs                            |
| Route 53                     | Cloud DNS                            | Authoritative DNS                                       | Routing policies similar (WRR, geo, latency)                                |
| ACM                          | Certificate Manager                  | TLS cert management                                     | DNS-validated certs auto-renew via Cloud DNS                                |
| ALB                          | Global / Regional External HTTPS LB  | L7 LB                                                   | Implemented via GKE Gateway, Backend Service, NEG model                    |
| NLB                          | Network LB (passthrough/proxy)       | L4 LB                                                   | Pass-through is regional only                                              |
| AWS WAF                      | Cloud Armor                          | WAF + DDoS                                              | CEL expressions, preconfigured rules differ in mapping                     |
| AWS Shield Advanced          | Cloud Armor managed protection       | Advanced DDoS                                           | Pricing and packaging differ                                               |
| Security Group               | VPC firewall rule + tags             | Stateful network ACL                                    | GCP firewall is at VPC level, network-tag targeted                         |
| Network ACL                  | (No direct; use VPC firewall + hierarchical policy) | Stateless ACL                                | Use VPC firewall + hierarchical org policies                                |
| Global Accelerator           | Global External LB anycast IP        | Static anycast frontend                                 | Implementation pattern differs                                              |
| AWS Verified Access          | Identity-Aware Proxy (IAP)           | App-level zero-trust access                             | Identity provider integrations differ                                      |

## Identity & Access

| AWS                          | GCP                                  | Comparable on                                           | Differs on                                                                 |
|------------------------------|--------------------------------------|---------------------------------------------------------|----------------------------------------------------------------------------|
| IAM user                     | (Use Workforce Identity Federation; do not create users) | Identity                                  | GCP best practice is federated, not native users                            |
| IAM role                     | IAM role + Service Account           | Permission bundle                                       | Roles bind to principals; principals can be federated                       |
| IAM Identity Center (SSO)    | Workforce Identity Federation        | Org-wide SSO                                            | WIF maps directly to existing IdP (Okta, Azure AD, etc.)                    |
| IRSA                         | Workload Identity                    | Pod-level identity via OIDC                             | KSA → GSA binding via `roles/iam.workloadIdentityUser`                      |
| EKS Pod Identity             | Workload Identity                    | Same                                                    | Same                                                                       |
| STS AssumeRole               | iam impersonateServiceAccount        | Short-term credentials                                  | Different syntax                                                           |
| Service control policies     | Org policies + IAM Conditions        | Org-wide guardrails                                     | Different policy language                                                  |
| KMS                          | Cloud KMS                            | Managed key service                                     | Crypto API and key versioning differ                                        |
| Secrets Manager              | Secret Manager                       | Secret storage                                          | Versioning model differs slightly                                           |
| ACM Private CA               | Certificate Authority Service        | Private CA                                              | Different API surface                                                       |

## Storage

| AWS                          | GCP                                  | Comparable on                                           | Differs on                                                                 |
|------------------------------|--------------------------------------|---------------------------------------------------------|----------------------------------------------------------------------------|
| EBS gp3                      | PD Balanced                          | SSD block storage                                       | IOPS scale differently with size                                            |
| EBS io1 / io2                | PD SSD (`pd-ssd`), or Hyperdisk Extreme where the machine series supports it | High-IOPS block | `pd-ssd` is the machine-series-safe default; Hyperdisk sizing model and limits differ |
| EBS st1                      | PD Standard                          | HDD block                                               | Performance lower than EBS st1 in some shapes                              |
| EFS                          | Filestore (Basic, Enterprise, Zonal) | NFS shared FS                                           | Tiers differ; Basic ≪ EFS Bursting; Enterprise ≈ EFS Provisioned            |
| FSx Lustre                   | Parallelstore                        | Parallel high-throughput FS                             | API/config differs                                                         |
| FSx for ONTAP                | NetApp Volumes                       | NetApp ONTAP                                            | Same vendor, different cloud SLA                                            |
| FSx for OpenZFS              | (No direct equivalent)               | -                                                       | Re-platform                                                                |
| S3                           | Cloud Storage (GCS)                  | Object storage                                          | Lifecycle, ACL semantics, signed URLs differ                                |
| S3 Glacier                   | Archive storage class                | Cold object                                             | Retrieval times differ                                                     |
| Storage Gateway              | (No direct; use STS, gcsfuse)        | On-prem gateway                                         | Re-platform                                                                |

## Data services

| AWS                          | GCP                                  | Comparable on                                           | Differs on                                                                 |
|------------------------------|--------------------------------------|---------------------------------------------------------|----------------------------------------------------------------------------|
| RDS Postgres                 | Cloud SQL Postgres / AlloyDB         | Managed Postgres                                        | AlloyDB has columnar engine + AI features; Cloud SQL is direct equivalent  |
| RDS MySQL                    | Cloud SQL MySQL                      | Managed MySQL                                           | Connector/auth proxy differs                                                |
| RDS MariaDB                  | Cloud SQL MySQL (compatible)         | Managed                                                 | MariaDB-specific features may not be supported                              |
| Aurora                       | AlloyDB / Cloud SQL                  | Managed RDBMS                                           | Aurora's storage engine has no GCP analogue; expect perf differences        |
| DocumentDB                   | Firestore (Native mode) / MongoDB Atlas on GCP | Document DB                                  | Firestore is not Mongo wire-compat                                          |
| DynamoDB                     | Bigtable / Spanner / Firestore       | KV / wide-column / document                             | Heterogeneous; access pattern dictates choice                              |
| ElastiCache Redis            | Memorystore for Redis Cluster        | Managed Redis                                           | Cluster mode flag differs                                                  |
| ElastiCache Memcached        | Memorystore for Memcached            | Managed Memcached                                       | Direct                                                                     |
| OpenSearch                   | (Self-host on GKE or Elastic Cloud on GCP) | Managed search                                    | No GCP-native managed Elasticsearch GA                                      |
| MSK                          | Managed Service for Apache Kafka     | Managed Kafka                                           | GA since 2024-11; open-source Kafka in KRaft mode, Kafka Connect included    |
| Kinesis Data Streams         | Pub/Sub or Managed Service for Apache Kafka | Streaming ingest                                 | Different ordering / partitioning model. Not Pub/Sub Lite: deprecated, closed to new customers since 2024-09-24, turndown 2027-01-31 |
| SQS                          | Pub/Sub (with subscription)          | Queue / pub-sub                                         | Pub/Sub is more pub-sub than queue; semantics differ                       |
| SNS                          | Pub/Sub                              | Pub/sub                                                 | Direct, with tweaks                                                         |
| Step Functions               | Workflows                            | Orchestration                                           | Definition language differs (ASL → YAML)                                   |
| EMR                          | Dataproc                             | Managed Hadoop/Spark                                    | Direct                                                                     |
| Glue                         | Dataproc / Data Fusion / Dataform    | ETL                                                     | Multi-product mapping                                                       |
| Athena                       | BigQuery (or BigQuery Omni for cross-cloud) | Serverless SQL on object storage                | BigQuery is far more performant; Athena→BQ is a re-platform               |
| Redshift                     | BigQuery                             | DW                                                      | Different model (no clusters; serverless)                                   |
| QuickSight                   | Looker / Looker Studio               | BI                                                      | Different IDE, modeling layer                                               |

## Observability

| AWS                          | GCP                                  | Comparable on                                           | Differs on                                                                 |
|------------------------------|--------------------------------------|---------------------------------------------------------|----------------------------------------------------------------------------|
| CloudWatch Metrics           | Cloud Monitoring                     | Metrics                                                 | Filter language is MQL/PromQL                                              |
| CloudWatch Logs              | Cloud Logging                        | Logs                                                    | Filter language differs; sinks model                                        |
| CloudWatch Alarms            | Alerting Policies                    | Alerts                                                  | Multi-condition combiner explicit                                          |
| CloudWatch Dashboards        | Cloud Monitoring Dashboards          | Dashboards                                              | Widget DSL differs                                                          |
| AWS X-Ray                    | Cloud Trace                          | Distributed tracing                                     | OTel-native easier on GCP                                                   |
| Amazon Managed Prometheus    | Managed Service for Prometheus       | PromQL                                                  | GMP integrates with GKE natively                                            |
| Amazon Managed Grafana       | (Self-host Grafana)                  | -                                                       | No GA managed Grafana                                                       |
| AWS Health Dashboard         | Service Health / Personalized Service Health | Service status                                | Per-product feed                                                            |

## CI/CD, Build, Supply Chain

| AWS                          | GCP                                  | Comparable on                                           | Differs on                                                                 |
|------------------------------|--------------------------------------|---------------------------------------------------------|----------------------------------------------------------------------------|
| CodeCommit                   | Cloud Source Repositories            | Git host                                                | Most teams use GitHub/GitLab anyway                                         |
| CodeBuild                    | Cloud Build                          | Managed build                                           | YAML differs                                                                |
| CodePipeline                 | Cloud Deploy + Cloud Build           | Pipelines                                               | Cloud Deploy is canary/release-focused                                      |
| CodeDeploy                   | Cloud Deploy                         | Deployment orchestration                                | Direct                                                                     |
| ECR                          | Artifact Registry                    | Container registry                                      | AR also handles language packages                                           |
| Container Registry signing   | Sigstore + Binary Authorization      | Image attestation                                       | BinAuthz is policy-driven                                                   |

## Compliance & Governance

| AWS                          | GCP                                  | Comparable on                                           | Differs on                                                                 |
|------------------------------|--------------------------------------|---------------------------------------------------------|----------------------------------------------------------------------------|
| AWS Config                   | Cloud Asset Inventory + Org Policy   | Config compliance                                       | Policy language differs                                                     |
| AWS Security Hub             | Security Command Center              | Centralized findings                                    | Different finding sources / catalog                                         |
| GuardDuty                    | SCC (Threat Detection)               | Threat detection                                        | Different categories                                                        |
| AWS Inspector                | SCC (Vulnerability)                  | Vuln scanning                                           | Container-aware via Artifact Analysis                                       |
| AWS Audit Manager            | SCC + Cloud Audit Logs               | Audit prep                                              | Pre-built frameworks differ                                                 |
| AWS Trust Lens / Trust Boundary | Org Policies + folders             | Guardrails                                              | Hierarchical model differs                                                  |

## Migration tools

| AWS                          | GCP                                  | Comparable on                                           | Differs on                                                                 |
|------------------------------|--------------------------------------|---------------------------------------------------------|----------------------------------------------------------------------------|
| AWS DataSync                 | Storage Transfer Service             | Object/file transfer                                    | STS supports S3 → GCS natively                                              |
| AWS DMS                      | Database Migration Service           | DB migration with CDC                                   | Source and target support overlap heavily                                   |
| AWS Application Migration Service | Migrate to Virtual Machines        | Server migration                                        | For VMs, not containers                                                     |
| AWS Snowball                 | Transfer Appliance                   | Offline bulk transfer                                   | Capacity tiers differ                                                       |
