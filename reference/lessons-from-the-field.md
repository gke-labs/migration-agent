# Lessons from the Field

A curated knowledge base of real-world failure modes practitioners have reported when migrating Kubernetes workloads to GKE — primarily from EKS, with related multi-cloud lessons where the pattern transfers. Each entry cites a verifiable source, paraphrases the lesson (we never reproduce >15 words verbatim), and names the phase that owns the prevention. Owners are phase directories under `servers/phases/` (or the DAG server in `servers/dag/`), not skills.

This is the document the official cloud.google.com series can't write — vendor docs describe happy paths, while this records what actually breaks. It is intentionally small: each lesson must come from a citable practitioner source, and each must be actionable. We grow this file by adding war stories from real migration runs.

## How to use this document

- **Read it before kicking off a migration.** Look at every entry whose `severity = 3` — those are the items that have caused real outages or unbudgeted financial damage. They are the ones a PSO consultant would tell you about over coffee on day one.
- **Read it after a phase produces a plan.** When you are about to execute a phase, scan that phase's lessons. The orchestrator references the lessons it deems relevant, but human judgment is the final filter.
- **Contribute back.** When the agent encounters a new failure mode in the wild, add an entry. See [§ Contributing](#contributing).

## Severity scale

- **1 — Advisory.** A pattern worth knowing. May save time. Not load-bearing.
- **2 — Workaround required.** Without addressing this, the team will hit a non-obvious problem they will then have to work around mid-migration.
- **3 — Outage-class.** Without addressing this, a real production outage, data loss, or unbudgeted financial overrun is likely or has been documented in the cited source.

## Index by failure category

- [§1. Cross-cloud egress costs](#1-cross-cloud-egress-costs)
- [§2. DNS, TTL, and client-side caching during cutover](#2-dns-ttl-and-client-side-caching-during-cutover)
- [§3. Identity translation (IRSA ↔ Workload Identity)](#3-identity-translation-irsa-↔-workload-identity)
- [§4. Stateful data migration](#4-stateful-data-migration)
- [§5. Autoscaling parity (Karpenter ↔ NAP, HPA + VPA)](#5-autoscaling-parity-karpenter-↔-nap-hpa--vpa)
- [§6. Networking, ingress, Gateway API](#6-networking-ingress-gateway-api)
- [§7. Cert management](#7-cert-management)
- [§8. Service mesh re-platforms](#8-service-mesh-re-platforms)
- [§9. Cluster lifecycle and DR](#9-cluster-lifecycle-and-dr)
- [§10. Cost-model surprises post-migration](#10-cost-model-surprises-post-migration)
- [§11. PVC and storage finalizers](#11-pvc-and-storage-finalizers)
- [§12. Long-lived connections during cutover](#12-long-lived-connections-during-cutover)
- [§13. Cluster bring-up — quotas and apply-time blocks](#13-cluster-bring-up--quotas-and-apply-time-blocks)

## Index by owning phase

| Phase | Lessons that own prevention |
|---|---|
| discovery | LFF-15 |
| assessment | LFF-01, LFF-02, LFF-29, LFF-34, LFF-35 |
| landing zone | LFF-08, LFF-10, LFF-12, LFF-30, LFF-31, LFF-34, LFF-35, LFF-37, LFF-38 |
| translation | LFF-08, LFF-09, LFF-16, LFF-20, LFF-21, LFF-22, LFF-26, LFF-27, LFF-28, LFF-33, LFF-36 |
| workload | LFF-06, LFF-07, LFF-14, LFF-16, LFF-17, LFF-20, LFF-22, LFF-33 |
| deployment | LFF-01, LFF-02, LFF-11, LFF-13, LFF-14, LFF-15 |
| DAG server | LFF-24, LFF-25 |
| unowned — no phase covers this yet | LFF-03, LFF-04, LFF-05, LFF-18, LFF-19, LFF-23, LFF-32 |

An entry with more than one owning phase is listed once under each of them, so the
same `LFF-NN` appears in as many rows as its **Owning phases** line names.

---

## 1. Cross-cloud egress costs

### LFF-01 — Egress during co-existence routinely runs 5–10× the unbudgeted estimate

**Severity:** 3
**Owning phases:** assessment, deployment
**Sources:**
- CloudOptimo, *The True Cost of Cloud Data Egress And How to Manage It*, 2024 — <https://www.cloudoptimo.com/blog/the-true-cost-of-cloud-data-egress-and-how-to-manage-it/>
- NTC Tech, *Cloud Egress Costs Explained: Why Your Architecture Is Paying a Tax You Never Modeled*, dev.to, 2024 — <https://dev.to/ntctech/cloud-egress-costs-explained-why-your-architecture-is-paying-a-tax-you-never-modeled-554c>

**What practitioners report.** A 10 TiB database replicated continuously between clouds for two weeks costs four-figure sums in egress alone. The dev.to post cites a workload that jumped from ~$4.1k/mo to ~$25k/mo after a migration whose architecture pushed traffic across clouds. Migration teams rarely model this until the first invoice lands.

**What to do.**
- Estimate egress in the assessment phase using a per-workload data-rate × co-existence-window calculation. Surface this in the readiness report's Section 7.
- For data migration cohorts, prefer pre-cutover seeding via Cloud Interconnect or Storage Transfer Service (when applicable) over open-internet replication.
- During Phase 4, monitor egress in near-real-time; cap the co-existence window when costs threaten the ceiling.
- After the migration, treat residual cross-cloud egress as a decommission blocker — every byte still crossing is a bill.

---

### LFF-02 — Sift's petabyte AWS→GCP move dual-wrote and shadow-read for weeks

**Severity:** 2
**Owning phases:** assessment, deployment
**Sources:**
- Sift Engineering, *Migrating our cloud infrastructure to Google Cloud (Part 1/4)*, ~2019 — <https://engineering.sift.com/gcp-data-mig-1/>
- HN discussion: *Sift migrated petabyte-scale HBase from AWS to Bigtable with zero downtime*, 2020 — <https://news.ycombinator.com/item?id=22329216>
- Sift Engineering, on dedicated interconnect cost — same series

**What practitioners report.** At PB scale, the only safe pattern Sift could find was dual-write + shadow-read for weeks before switching reads to the target. They also reported that a vendor-implemented dedicated cloud interconnect was roughly 5× cheaper than open-internet egress for the same volume.

**What to do.** For data systems above ~1 TiB with strict consistency requirements, plan a multi-week shadow-read window into the migration plan. Cost-model dedicated interconnect against egress; the break-even is much lower than people assume.

---

## 2. DNS, TTL, and client-side caching during cutover

### LFF-03 — Default JVM caches DNS forever; AWS SDK for Java recommends a 5-second TTL

**Severity:** 3
**Owning phase:** unowned — no phase covers this yet
**Sources:**
- AWS, *Set the JVM TTL for DNS name lookups* (SDK for Java docs) — <https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/jvm-ttl-dns.html>
- Tyler Russell, *Save yourself time: Set the JVM TTL for DNS name lookups in AWS*, 2025 — <https://tylerrussell.dev/2025/06/02/save-yourself-time-set-the-jvm-ttl-for-dns-name-lookups-in-aws/>
- GoogleContainerTools, *java should set networkaddress.cache.ttl* — issue, 2019 — <https://github.com/GoogleContainerTools/distroless/issues/327>

**What practitioners report.** A default JVM caches the first DNS answer it receives for the lifetime of the process. AWS SDK for Java explicitly recommends setting `networkaddress.cache.ttl=5`. Distroless Java images inherit a 30-second cache. Teams who ramp DNS-weighted traffic without addressing client TTLs see the EKS-side stay live for *hours* on long-running JVMs.

**What to do.**
- Before any DNS-based ramp, mandate a JVM-side `networkaddress.cache.ttl` set to a small value (5–60s) on every Java workload that resolves migrated hostnames.
- At traffic cutover, *verify* the JVM TTL setting per workload, not just the DNS TTL.
- For non-JVM languages, audit the AWS SDK's regional-endpoint cache and any in-process resolver caches the same way.

### LFF-04 — Long-lived WebSocket clients on heterogeneous devices ignore DNS TTLs

**Severity:** 2
**Owning phase:** unowned — no phase covers this yet
**Source:** Edward Beech (initialed85), *Migrating From AWS EKS to GCP GKE*, 2024 — <https://initialed85.cc/posts/migrating-from-aws-eks-to-gcp-gke/>

**What practitioners report.** When the client fleet is "the open internet" — IoT devices, mobile apps, embedded — connections persist across DNS changes for arbitrarily long. The post describes needing to keep an EKS-side forwarding presence after the official cutover so that legacy WebSocket clients did not time out.

**What to do.** For workloads with persistent client connections from heterogeneous fleets, plan a *forwarding tail* in the cutover runbook: keep a small EKS footprint that proxies to GKE for 30+ days post-cutover. Track residual connection counts and decommission only when the count drops below a threshold.

### LFF-05 — gRPC and round-robin client policy can SYN-flood targets during a rollover

**Severity:** 3
**Owning phase:** unowned — no phase covers this yet
**Sources:**
- Datadog Engineering, *It's always DNS . . . except when it's not*, 2022 — <https://www.datadoghq.com/blog/engineering/grpc-dns-and-load-balancing-incident/>
- Jiri Luska (Jamf Engineering), *How three lines of configuration solved our gRPC scaling issues in Kubernetes*, 2022 — <https://medium.com/jamf-engineering/how-three-lines-of-configuration-solved-our-grpc-scaling-issues-in-kubernetes-ca1ff13f7f06>

**What practitioners report.** Datadog documents a real incident where a gRPC client policy switch caused a SYN flood pattern across a deploy. Jamf reports gRPC clients pinning to original pods unless `MaxConnectionAge` is set to force periodic re-resolution.

**What to do.** For any gRPC workload in scope, set `MaxConnectionAge` and `MaxConnectionAgeGrace` on the server side. At traffic cutover, treat gRPC and websocket workloads as a separate cohort with their own validation gate (connection-redistribution metric, not just request-rate).

---

## 3. Identity translation (IRSA ↔ Workload Identity)

### LFF-06 — Workload Identity metadata server isn't ready at pod start; ~50% of cold-start auths fail

**Severity:** 2 (outage-class on tier-0)
**Owning phase:** workload
**Sources:**
- google-auth-library-python, *DefaultCredentialsError in a GKE Workload Identity setup due to MDS slow start up*, GitHub issue, 2021 — <https://github.com/googleapis/google-auth-library-python/issues/604>
- Google Cloud, *About Workload Identity Federation for GKE* (acknowledges this in the docs) — <https://docs.cloud.google.com/kubernetes-engine/docs/concepts/workload-identity>
- morzzz007, *wait-for-workload-identity* (init container workaround) — <https://github.com/morzzz007/wait-for-workload-identity>

**What practitioners report.** The `gke-metadata-server` daemon is not always ready at the moment a pod's first request fires. Practitioners report ~50% failure rates on cold starts during HPA scale-ups. The official GCP docs acknowledge "authentication may fail in the first seconds." A community init-container workaround exists.

**What to do.** Every workload that uses Workload Identity for outbound API calls must either retry with backoff at startup, *or* gate startup with the `wait-for-workload-identity` (or equivalent) init container. The orchestrator should fail any cohort whose smoke tests don't include a cold-start identity check.

### LFF-07 — Argo CD pods lose IAM access for ~40 minutes after first install

**Severity:** 3
**Owning phase:** workload
**Source:** balusarakesh, *argocd pods lose IAM access for the first few minutes*, GitHub issue, argo-cd #7483, 2021 — <https://github.com/argoproj/argo-cd/issues/7483>

**What practitioners report.** The vanilla Argo CD install manifests overwrite the ServiceAccount mount, so IRSA-style auth silently breaks for ~40 minutes after every fresh install. The same class of issue can hit any workload whose Helm chart uses `automountServiceAccountToken: false` or replaces the projected token mount.

**What to do.** In the workload phase, validate per-workload that `automountServiceAccountToken` is `true` and that no chart template silently disables the projected token volume. Add a check to the validation pod (Step 3) that asserts the projected token is mounted at the canonical path.

### LFF-08 — On GKE, node-credential exposure is closed by Workload Identity on every node pool, not by blocking 169.254.169.254

**Severity:** 3
**Owning phases:** landing zone, translation
**Source:** Sandlayth, *GKE — How to Reliably Block Egress to Metadata IP at Network Level*, r/kubernetes, 2025 — <https://www.reddit.com/r/kubernetes/comments/1kz8auo/gke_how_to_reliably_block_egress_to_metadata_ip/>; Google Cloud, *About Workload Identity Federation for GKE* — <https://cloud.google.com/kubernetes-engine/docs/concepts/workload-identity>

**What practitioners report.** On EKS with IRSA, the projected-token model means pods don't reach for the EC2 metadata service. On GKE, if Workload Identity is not enabled on a node pool, or a pod runs with `hostNetwork: true`, the pod can still hit the node's Compute Engine metadata at `169.254.169.254` and inherit the node's service account. Practitioners report this as a security regression that one-to-one IRSA→WI translation does not catch, and some reach for a NetworkPolicy that blocks the metadata address.

**What to do.** The NetworkPolicy is the wrong tool. On a Workload Identity node pool the GKE metadata server itself intercepts `169.254.169.254` (answering on port 80 under Dataplane V2, and on `169.254.169.252:988`), and pods can no longer reach the Compute Engine metadata server at all; blocking the address breaks token issuance for every pod the policy covers. In the landing zone phase, enable Workload Identity Federation for GKE on every Standard node pool (Autopilot always has it) and restrict `hostNetwork: true`, the one documented bypass. The translation phase adds a verification step that confirms a pod without a WI binding cannot reach Google APIs, and that strict egress policies still allow the two metadata-server addresses.

### LFF-09 — AWS STS trusts Google identities natively (no per-cluster OIDC provider needed)

**Severity:** 1 (saves work)
**Owning phase:** translation
**Sources:**
- Jason Umiker, *Cross-cloud identities between GCP and AWS from GKE and/or EKS*, 2025 — <https://jason-umiker.medium.com/cross-cloud-identities-between-gcp-and-aws-from-gke-and-or-eks-182652bddadb>
- Steven Aldinger (TeamSnap Engineering), *Accessing AWS Resources from Google Kubernetes Engine*, 2024 — <https://medium.com/teamsnap-engineering/accessing-aws-resources-from-google-kubernetes-engine-3df21a8ca297>

**What practitioners report.** AWS already trusts Google's identity provider for `accounts.google.com` audience tokens. Cross-cloud federation from GKE → AWS does not require setting up a per-cluster OIDC provider on AWS — just a trust policy on the AWS IAM role accepting the federated principal. This simplifies the cross-cloud federation pattern in the translation phase.

**What to do.** Use the natively-trusted Google IdP path rather than provisioning a per-cluster OIDC provider on AWS. Test against a representative GSA before migrating identity for any tier-0 workload.

### LFF-10 — Stale `gke-metadata-server` skipped during auto-upgrade can break kube-dns and CI/CD

**Severity:** 3
**Owning phase:** landing zone
**Source:** Able Lv (Airwallex Engineering), *How We Deal with a Google Kubernetes Engine (GKE) Metadata Server Outage*, 2022 — <https://medium.com/airwallex-engineering/how-we-deal-with-a-google-kubernetes-engine-gke-metadata-server-outage-b627958ee7ad>

**What practitioners report.** Airwallex's post describes a real incident where a stale `gke-metadata-server` daemon was skipped during a GKE auto-upgrade, breaking kube-dns lookups and the CI/CD pipeline that relied on Workload Identity. This is one of the strongest publicly available production-incident write-ups for WI.

**What to do.** In the landing zone phase, configure release-channel auto-upgrades but add an alerting policy on `gke-metadata-server` pod readiness. Day-2, add a recurring check that asserts the metadata server matches the cluster's running version.

---

## 4. Stateful data migration

### LFF-11 — ZFS-send over WAN bottlenecks at ~30 MB/s on default Linux TCP buffers

**Severity:** 3 (perf, not correctness)
**Owning phase:** deployment
**Source:** levkk (PostgresML), *We migrated from AWS to GCP with minimal downtime*, HN, 2024 — <https://news.ycombinator.com/item?id=40609908>

**What practitioners report.** PostgresML reported that ZFS-send between clouds bottlenecked at ~30 MB/s due to default Linux TCP buffer sizes plus ~40 ms RTT — a classic bandwidth-delay-product trap. The team initially started rewriting in Rust before realizing the fix was kernel parameter tuning.

**What to do.** Any cross-cloud bulk transfer over open internet should explicitly tune `net.core.{r,w}mem_max`, `net.ipv4.tcp_{r,w}mem`, and TCP congestion control before measuring throughput. Document this as part of the FinOps + perf playbook.

### LFF-12 — Cloud SQL hides behind Google-controlled VPC peering, blocking second-hop route propagation

**Severity:** 3
**Owning phase:** landing zone
**Source:** Edward Beech (initialed85), *Migrating From AWS EKS to GCP GKE*, 2024 — <https://initialed85.cc/posts/migrating-from-aws-eks-to-gcp-gke/>

**What practitioners report.** Cloud SQL with private IP creates a Google-controlled VPC peering. That peering does *not* propagate routes through a second peering or VPN. So if your GKE pods need to reach Cloud SQL via a transit VPC, the routes simply don't work — and the failure mode is silent.

**What to do.** In the landing zone phase, never plan a topology where Cloud SQL access traverses a second peering hop. Use Private Service Connect endpoints for Cloud SQL, or co-locate the database project's VPC with the cluster project's VPC.

### LFF-13 — Cloud SQL DMS new-instance flow only supports VPC peering for private IP

**Severity:** 2
**Owning phase:** deployment
**Source:** Edward Beech (initialed85), *Migrating From AWS EKS to GCP GKE*, 2024 — <https://initialed85.cc/posts/migrating-from-aws-eks-to-gcp-gke/>

**What practitioners report.** DMS's new-instance migration flow can only attach the new Cloud SQL instance via VPC peering. Private Service Connect targets are only supported when migrating to a *pre-existing* instance. Practitioners report 30-minute reset cycles after every failed configuration attempt.

**What to do.** Already in the deployment phase (the Postgres runbook). Reinforce: if PSC is required, pre-create the instance and target it from DMS rather than letting DMS create it.

### LFF-14 — Cloud SQL Proxy randomly drops with `NOT_AUTHORIZED` due to token-refresh races

**Severity:** 3
**Owning phases:** workload, deployment
**Sources:**
- GoogleCloudPlatform/cloud-sql-proxy issue #1078, 2021 — <https://github.com/GoogleCloudPlatform/cloud-sql-proxy/issues/1078>
- Google developer forum, *Cloud SQL Proxy randomly losing connection due to being NOT_AUTHORIZED*, 2024 — <https://discuss.google.dev/t/cloud-sql-proxy-randomly-loosing-connection-due-to-being-not-authorized/129191>

**What practitioners report.** Cloud SQL Auth Proxy under Workload Identity intermittently loses connections with `NOT_AUTHORIZED` due to token-refresh races. Practitioners describe this as flapping that takes down connection pools at random intervals.

**What to do.** In the workload phase, add a per-workload Cloud-SQL-using validation that runs sustained connection traffic (≥30 min) before declaring identity complete. In the deployment phase, ensure connection-pool layers (PgBouncer, application-side pools) handle abrupt re-auth with reconnection logic.

### LFF-15 — Postgres replication slots can be dropped silently on managed-DB failover

**Severity:** 3
**Owning phases:** discovery, deployment
**Source:** airbytehq/airbyte, GitHub issue #29333 — *Missing replication slot caused by Azure PG Flex Server failover leads to data loss*, 2023 — <https://github.com/airbytehq/airbyte/issues/29333>

**What practitioners report.** A managed-DB failover on Azure dropped a logical replication slot silently; downstream consumers (CDC tooling) lost rows. The same class of failure can happen on RDS during a Postgres source's pre-cutover prep if `pglogical` slots are not pinned across failover.

**What to do.** During discovery, capture every replication slot in use on the source DB. During the data migration cutover, monitor slot existence and lag continuously; treat any slot vanishing during the cutover window as a hard stop.

---

## 5. Autoscaling parity (Karpenter ↔ NAP, HPA + VPA)

### LFF-16 — Karpenter on GCP exists but is preview-grade; treat the parity as a re-platform

**Severity:** 2
**Owning phases:** translation, workload
**Sources:**
- Better-Concept-1682, *Karpenter on GKE*, r/kubernetes, 2025 — <https://www.reddit.com/r/kubernetes/comments/1mp9wo9/karpenter_on_gke/>
- jwcesign, *Karpenter GCP Provider is available now!*, r/kubernetes, 2025 — <https://www.reddit.com/r/kubernetes/comments/1m7caam/karpenter_gcp_provider_is_available_now/>
- Cast AI, *Is There a Karpenter Equivalent on GKE?*, 2024 — <https://cast.ai/blog/gke-vs-karpenter/>

**What practitioners report.** A community Karpenter provider for GCP exists but is explicitly not production-ready. NAP behaves like a node-pool-creator, not a node-creator like Karpenter. Teams arriving from EKS-with-Karpenter expect single-node creation with seconds-class latency; NAP provisions whole pools and runs on the order of minutes.

**What to do.** In the workload phase, do not translate Karpenter `NodePool` and `EC2NodeClass` resources verbatim. Capture taints/labels and rebuild as GKE Standard node pools (or NAP rules) in the translation phase. Surface the autoscaling-latency delta as a known regression in the readiness report.

### LFF-17 — Salesforce Karpenter migration brought scaling latency from minutes to seconds

**Severity:** 1 (context for the inverse direction)
**Owning phase:** workload
**Sources:**
- AWS Architecture Blog, *How Salesforce migrated from Cluster Autoscaler to Karpenter*, 2024 — <https://aws.amazon.com/blogs/architecture/how-salesforce-migrated-from-cluster-autoscaler-to-karpenter-across-their-fleet-of-1000-eks-clusters/>
- Tasrie IT Services, *Karpenter vs Cluster Autoscaler: We Migrated 50+ Clusters*, 2026 — <https://tasrieit.com/blog/karpenter-vs-cluster-autoscaler-eks-comparison-2026>

**What practitioners report.** Karpenter starts pods in roughly 55 seconds vs Cluster Autoscaler's 3–4 minutes per Tasrie's 50+-cluster benchmark. Salesforce reports the same seconds-vs-minutes shift. When migrating *off* Karpenter onto NAP, plan for the inverse: workloads tuned to fast scale-up will see queueing.

**What to do.** Increase HPA `*minReplicas*` and lower target utilization on workloads previously tuned for Karpenter's fast cycle. Use pause Pods to absorb bursts that NAP can't satisfy in time.

### LFF-18 — HPA + VPA on the same metric thrashes; upstream warns explicitly

**Severity:** 2
**Owning phase:** unowned — no phase covers this yet
**Sources:**
- kubernetes/autoscaler, GitHub issue #6060, 2023 — <https://github.com/kubernetes/autoscaler/issues/6060>
- ScaleOps, *HPA's Three Architectural Flaws and Why Your Autoscaling Keeps Failing*, 2024 — <https://scaleops.com/blog/hpas-three-architectural-flaws-and-why-your-autoscaling-keeps-failing/>

**What practitioners report.** Running HPA on CPU while VPA mutates CPU requests on the same workload produces unstable replica counts: VPA changes the denominator while HPA scales the numerator. Upstream Kubernetes documents this as an unsupported configuration.

**What to do.** A day-2 concern. Reinforce in the validation gate: any workload with both HPA and VPA on CPU is a hard stop until VPA is moved to recommendation mode or HPA is moved to a custom metric.

### LFF-19 — GKE bin-packing post-migration commonly drops to <40% node utilization

**Severity:** 2
**Owning phase:** unowned — no phase covers this yet
**Source:** sanpoke18, *Struggling with High Unused Resources in GKE (Bin Packing Problem)*, r/kubernetes, 2025 — <https://www.reddit.com/r/kubernetes/comments/1pe41bl/struggling_with_high_unused_resources_in_gke_bin/>

**What practitioners report.** Teams arriving from Karpenter-on-EKS report sub-40% utilization on GKE because NAP creates many small node pools and the scheduler does not consolidate aggressively. Costs balloon until the team retunes.

**What to do.** Day-2, set the cluster autoscaler profile to `optimize-utilization` for non-prod, monitor consolidation events, and consolidate node pools where workload affinities allow.

---

## 6. Networking, ingress, Gateway API

### LFF-20 — ALB `group.name` annotation creates a new ALB instead of mutating the existing one

**Severity:** 2
**Owning phases:** translation, workload
**Source:** kubernetes-sigs/aws-load-balancer-controller issue #2271, 2021 — <https://github.com/kubernetes-sigs/aws-load-balancer-controller/issues/2271>

**What practitioners report.** Adding `alb.ingress.kubernetes.io/group.name` to an existing Ingress on EKS creates a *new* ALB rather than joining the existing group. This EKS-only behavior has no clean Gateway API equivalent on GKE.

**What to do.** During the translation phase, do not attempt to translate `group.name` semantics. Rebuild as a single GKE Gateway with multiple HTTPRoutes attached, or as multiple Gateways with a shared static IP (see LFF-21).

### LFF-21 — Each Istio Gateway resource creates its own LB Service; shared static IPs collide on port 15021

**Severity:** 2
**Owning phase:** translation
**Source:** istio/istio issue #54453, 2024 — <https://github.com/istio/istio/issues/54453>

**What practitioners report.** When using Istio + Gateway API on GKE, each Istio `Gateway` resource creates its own LoadBalancer Service exposing port 15021 (Istio's default health-check port). Two Gateways trying to share a static IP collide.

**What to do.** Use a single Istio Gateway with multiple listeners rather than multiple Gateways. If multi-Gateway is unavoidable, use distinct static IPs.

### LFF-22 — AWS LBC moved off annotations to CRDs; many EKS-only fields don't fit Gateway API

**Severity:** 2
**Owning phases:** translation, workload
**Sources:**
- kubernetes-sigs/aws-load-balancer-controller issue #2949, 2023 — <https://github.com/kubernetes-sigs/aws-load-balancer-controller/issues/2949>
- AWS Networking & Content Delivery Blog, *AWS LBC adds GA support for Kubernetes Gateway API*, 2026-03 — <https://aws.amazon.com/blogs/networking-and-content-delivery/aws-load-balancer-controller-adds-general-availability-support-for-kubernetes-gateway-api/>

**What practitioners report.** AWS itself moved its LB controller from annotations to CRDs because annotation strings could not represent the configuration. Some `alb.ingress.kubernetes.io/*` annotations encode shapes (auth flows, conditional actions, group ordering) that don't fit Gateway API field semantics directly.

**What to do.** Already covered in the translation phase and `reference/api-translation.md`. Reinforce: any annotation in the source that doesn't appear in the api-translation table is an escalation, not a heuristic.

### LFF-23 — Service-by-service migration via Endpoints requires whitelisting node public IPs across both clusters

**Severity:** 2
**Owning phase:** unowned — no phase covers this yet
**Source:** Ganesh Kaila (Searce), *Migrating Kubernetes Workloads from GKE to EKS*, ~2020 — <https://blog.searce.com/migrating-kubernetes-workloads-from-gke-to-eks-f541acb8f269>

**What practitioners report.** When using a Service+Endpoints pattern to gradually move backends across clusters, the source cluster's Services must accept the target cluster's node public IPs. Practitioners report forgetting this and getting silent connection refusals during ramp.

**What to do.** At traffic cutover, document the source-side allow-list update as an explicit prerequisite for each workload using the Endpoints pattern.

### LFF-24 — Tamr literally spanned a single cluster across AWS and GCP for the cutover

**Severity:** 1 (context — pattern that worked)
**Owning phase:** DAG server
**Source:** Anthony Cozzie (Tamr) via Google Cloud Blog, *8 DevOps tools that smoothed our migration from AWS to GCP*, ~2017 — <https://cloud.google.com/blog/products/containers-kubernetes/8-devops-tools-that-smoothed-our-migration-from-aws-to-gcp-tamr>

**What practitioners report.** Tamr ran a single Mesos+Marathon cluster spanning both AWS and GCP nodes during their cutover, then drained services using placement constraints. This is the canonical "co-existence at the cluster layer" pattern.

**What to do.** For organizations with mesh tooling already (ASM, Istio multi-cluster), this pattern is achievable on GKE today. Surface it as an option at the start of the run, alongside per-cluster co-existence.

### LFF-25 — Form3 runs three independent clusters connected by NATS JetStream and CockroachDB

**Severity:** 1 (advanced pattern)
**Owning phase:** DAG server
**Source:** Kevin Holditch & Ross McFarlane (Form3), *How To Run on Three Clouds at Once, and When Not To*, InfoQ, 2026 — <https://www.infoq.com/news/2026/03/form3-triple-active-multicloud/>

**What practitioners report.** Form3 weathered a full GCP outage with low-severity alerts only because their architecture was triple-active across clouds with independent K8s clusters joined at the data plane (NATS JetStream + CockroachDB).

**What to do.** Most runs do not need this — but for tier-0 workloads where the migration window also implies a multi-cloud target steady-state, this is the reference architecture to study. Cite from the orchestrator design doc.

---

## 7. Cert management

### LFF-26 — GKE ManagedCertificate validates only after the LB IP attaches and DNS A record points at it

**Severity:** 2
**Owning phase:** translation
**Sources:**
- gke-managed-certs issue #13, 2019 — <https://github.com/GoogleCloudPlatform/gke-managed-certs/issues/13>
- gke-managed-certs issue #14, 2019 — <https://github.com/GoogleCloudPlatform/gke-managed-certs/issues/14>

**What practitioners report.** A ManagedCertificate stays in `FAILED_NOT_VISIBLE` state until the LB has an external IP *and* the DNS A record for the cert's hostname resolves to that IP. Default node SA scopes can also block cert creation.

**What to do.** In the translation phase, sequence: provision Gateway → wait for external IP → update DNS to point at that IP → only then expect ManagedCertificate to validate. For Gateway API on GKE, prefer Certificate Manager certs attached via certmap (pre-issuance) over ManagedCertificate.

### LFF-27 — ManagedCertificate doesn't work with nginx-ingress; cert-manager is the alternative

**Severity:** 2
**Owning phase:** translation
**Source:** gke-managed-certs issue #42, 2019 — <https://github.com/GoogleCloudPlatform/gke-managed-certs/issues/42>

**What practitioners report.** ManagedCertificate is tightly coupled to the GCE Ingress class. Teams running nginx-ingress on GKE must use cert-manager (with Cloud DNS DNS01 or HTTP01 against an LB the controller manages) instead.

**What to do.** Decision point in the translation phase: GKE Gateway + ManagedCertificate/CertificateMap, or nginx-ingress + cert-manager. Both are valid. Document the choice up front rather than mid-cutover.

### LFF-28 — Gateway API on GKE requires pre-shared Compute SslCertificate, not a ManagedCertificate CR

**Severity:** 2
**Owning phase:** translation
**Source:** gateway-api issue #1275, 2022 — <https://github.com/kubernetes-sigs/gateway-api/issues/1275>

**What practitioners report.** Gateway API on GKE expects a pre-shared SslCertificate Compute resource (via `networking.gke.io/pre-shared-certs` annotation), not a ManagedCertificate CRD. Teams used to ManagedCertificate get bitten when they switch to Gateway API.

**What to do.** Already in the translation phase. Reinforce: when targeting Gateway API, plan TLS via Certificate Manager (creates SslCertificate Compute resources directly) from the start.

---

## 8. Service mesh re-platforms

### LFF-29 — App Mesh is retiring 2026-09-30; the migration path is Istio (or VPC Lattice / Service Connect on AWS)

**Severity:** 2
**Owning phase:** assessment
**Sources:**
- Steef-Jan Wiggers (InfoQ), *AWS Sunsets More Services, Including AWS App Mesh*, 2024 — <https://www.infoq.com/news/2024/10/aws-retires-services/>
- Tetrate (Jimmy Song), *Migrating from AWS App Mesh to Istio: A Comprehensive Guide*, 2024 — <https://tetrate.io/blog/migrating-from-aws-app-mesh-to-istio-a-comprehensive-guide>

**What practitioners report.** AWS announced App Mesh retirement on 2026-09-30 (verify against current AWS notices). Tetrate's guide notes that Virtual Nodes and Virtual Routers do not translate one-to-one to Istio — automated translation tooling helps, but the rewrite is real work.

**What to do.** In the assessment phase, App Mesh in scope is a hard escalation: re-platform to Anthos Service Mesh (managed Istio) is its own mini-project. Surface the effort estimate as a separate line item with explicit human sign-off.

### LFF-30 — Confluent migrated CNI (to Cilium) live across AWS/Azure/GCP; needed cluster-by-cluster sequencing

**Severity:** 3
**Owning phase:** landing zone
**Source:** Nimisha Mehta & Alvaro Aleman (Confluent), *Confluent's Multi-Cloud Journey to Cilium: Pitfalls and Lessons Learned*, 2024 — <https://www.youtube.com/watch?v=vOSiVeBXYpM>

**What practitioners report.** Replacing the CNI live across AWS/Azure/GCP required cluster-by-cluster planning to keep stateful traffic flowing. Doing this without state-aware sequencing can sever inflight connections.

**What to do.** Most runs use GKE's Dataplane V2 from cluster creation, so no live CNI swap. But for organizations choosing to keep stateful workloads on a parallel CNI (e.g., Cilium-on-GKE for eBPF API parity with EKS), the cluster-by-cluster pattern from Confluent is the reference.

---

## 9. Cluster lifecycle and DR

### LFF-31 — Spotify accidentally deleted all their Kubernetes clusters with no user impact — the canonical DR case study

**Severity:** 1 (context — preparedness pattern)
**Owning phase:** landing zone
**Source:** David Xia (Spotify), *Keynote: How Spotify Accidentally Deleted All its Kube Clusters with No User Impact*, KubeCon EU 2019 — <https://www.youtube.com/watch?v=ix0Tw8uinWs>

**What practitioners report.** Spotify recovered from accidentally deleting every Kube cluster because (a) clusters were defined declaratively in Terraform, (b) workloads were backed up with Velero/Ark, (c) they ran multiple clusters so the blast radius was bounded. The talk is widely cited as the gold standard for DR preparedness.

**What to do.** Treat this as the prep checklist for the landing zone phase: declarative IaC for every cluster, backup-for-GKE on every namespace with state, and at least two clusters per environment so a single-cluster disaster doesn't stop traffic.

### LFF-32 — Stuck PVs / finalizer races at decommissioning time

**Severity:** 2
**Owning phase:** unowned — no phase covers this yet
**Sources:**
- kubernetes/kubernetes issue #69697, 2018 — <https://github.com/kubernetes/kubernetes/issues/69697>
- kubernetes-csi/external-provisioner issue #1217, 2022 — <https://github.com/kubernetes-csi/external-provisioner/issues/1217>

**What practitioners report.** PVs can stick in `Terminating` because the external-attacher finalizer persists. Two CSI controllers racing on finalizer removal can leave PVs stuck forever. This bites at decommission time.

**What to do.** In the decommission plan, before destroying any source cluster, manually verify PV finalizers have been cleaned up. Add the finalizer-cleanup step explicitly to the T+14 checklist.

### LFF-33 — Strimzi Kafka StatefulSets race with multi-zone gce-pd; mount lost+found via subPath

**Severity:** 2
**Owning phases:** translation, workload
**Sources:**
- strimzi-kafka-operator issue #477, 2018 — <https://github.com/strimzi/strimzi-kafka-operator/issues/477>
- strimzi-kafka-operator issue #1307, 2019 — <https://github.com/strimzi/strimzi-kafka-operator/issues/1307>

**What practitioners report.** Multi-zone gce-pd StorageClasses race against StatefulSet ordering on GKE; default class avoids the deadlock. Freshly provisioned PDs ship with `lost+found` directories that crash Kafka log scanning.

**What to do.** In the translation phase, default to `volumeBindingMode: WaitForFirstConsumer` (already done) and add a `subPath` mount for any Kafka-class workload to skip `lost+found`.

---

## 10. Cost-model surprises post-migration

### LFF-34 — Autopilot pay-per-request billing exploded at scale for one team; they reversed to EKS+Karpenter

**Severity:** 3 (financial)
**Owning phases:** assessment, landing zone
**Source:** El Mehdi Arezki, *Why Moving from GKE Autopilot to EKS with Karpenter Slashes Costs*, 2026 — <https://earezki.com/ai-news/2026-04-25-why-we-moved-from-gke-to-eks/>

**What practitioners report.** A team migrated to GKE Autopilot, found that the per-pod request-based billing scaled badly at their workload shape, and reversed to EKS + Karpenter. Autopilot saved K8s ops headcount but cost more in compute spend at scale.

**What to do.** Already in the landing zone phase's decision points (Autopilot vs Standard). Reinforce the warning in the assessment phase: at high pod count with bursty workloads, model Autopilot vs Standard cost on actual workload shape before defaulting.

### LFF-35 — Autopilot POC cost ~$1k/mo for trivial CPU workloads in one team's measurement

**Severity:** 2 (specific to small workloads)
**Owning phases:** assessment, landing zone
**Source:** Fernando Duran (SadServers), *Migrating Kubernetes out of the Big Cloud Providers*, 2024–2025 — <https://docs.sadservers.com/blog/migrating-k8s-out-of-cloud-providers/>

**What practitioners report.** A small Autopilot POC ran ~$1k/mo, where equivalent on Hetzner came to ~$30/mo. The author argues that Autopilot's overhead is real for non-trivial-but-not-large workloads.

**What to do.** Confirm Autopilot fits the workload shape before recommending it for non-prod. Standard is often cheaper at small scale.

---

## 11. PVC and storage finalizers

### LFF-36 — External Secrets fails when GSA and Secret Manager live in different projects

**Severity:** 2
**Owning phase:** translation
**Sources:**
- external-secrets/external-secrets issue #772, 2022 — <https://github.com/external-secrets/external-secrets/issues/772>
- external-secrets/external-secrets issue #2017, 2023 — <https://github.com/external-secrets/external-secrets/issues/2017>
- external-secrets/kubernetes-external-secrets issue #591, 2020 — <https://github.com/external-secrets/kubernetes-external-secrets/issues/591>

**What practitioners report.** External Secrets initially assumed the cluster project and the Secret Manager project were the same. Org-level landing zones (the agent's default) put them in *different* projects, breaking the assumption. Common symptom: 404 on `idbindtoken` calls.

**What to do.** In the translation phase, every GSA that needs to read secrets in `data-prod` must have `roles/secretmanager.secretAccessor` granted *in the data-prod project*, not the cluster project. Validate this with a test pod *before* deploying any workload that uses external-secrets.

---

## 12. Long-lived connections during cutover

(Combined with §2 — see LFF-04 and LFF-05.)

---

## 13. Cluster bring-up — quotas and apply-time blocks

### LFF-37 — Regional GKE node pools need ~300 GB SSD by default and trip initial regional quotas

**Severity:** 2
**Owning phase:** landing zone
**Source:** Asif Shaikh, *Fixing Insufficient Regional Quota for SSD_TOTAL_GB*, Medium, 2023 — <https://medium.com/@asifsource/fixing-error-403-insufficient-regional-quota-to-satisfy-request-resource-ssd-total-gb-when-6390c7f40770>

**What practitioners report.** A regional GKE node pool needs three nodes' worth of SSD (typically ~300 GB) to provision. New projects hit `SSD_TOTAL_GB` quota at apply time.

**What to do.** In the landing zone phase's apply-plan output, surface a quota pre-check: list every quota the plan will consume, compare to current limits, and request increases *before* the apply window — not during.

### LFF-38 — Default per-minute API quotas throttle Terraform on multi-resource GKE upgrades

**Severity:** 2
**Owning phase:** landing zone
**Source:** hashicorp/terraform-provider-google issue #3782, 2019 — <https://github.com/hashicorp/terraform-provider-google/issues/3782>

**What practitioners report.** Default per-minute API rate limits cause `Error 429: quota exceeded` during large `terraform apply` runs that touch many GKE resources at once.

**What to do.** Use `terraform apply -parallelism=4` (or lower) for landing-zone bring-up; request an API quota increase on the project ahead of time; split the apply into staged workspaces.

---

## Source index — alphabetical

| Author / Org | Title | Year | URL |
|---|---|---|---|
| Aleman, Alvaro & Mehta, Nimisha (Confluent) | Confluent's Multi-Cloud Journey to Cilium | 2024 | <https://www.youtube.com/watch?v=vOSiVeBXYpM> |
| AWS Architecture Blog | How Salesforce migrated from Cluster Autoscaler to Karpenter | 2024 | <https://aws.amazon.com/blogs/architecture/how-salesforce-migrated-from-cluster-autoscaler-to-karpenter-across-their-fleet-of-1000-eks-clusters/> |
| AWS SDK for Java docs | Set the JVM TTL for DNS name lookups | ongoing | <https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/jvm-ttl-dns.html> |
| AWS Networking & Content Delivery Blog | AWS LBC adds GA support for Kubernetes Gateway API | 2026 | <https://aws.amazon.com/blogs/networking-and-content-delivery/aws-load-balancer-controller-adds-general-availability-support-for-kubernetes-gateway-api/> |
| airbytehq | Issue #29333: Missing replication slot caused by Azure PG Flex Server failover | 2023 | <https://github.com/airbytehq/airbyte/issues/29333> |
| Arezki, El Mehdi | Why Moving from GKE Autopilot to EKS with Karpenter Slashes Costs | 2026 | <https://earezki.com/ai-news/2026-04-25-why-we-moved-from-gke-to-eks/> |
| Beech, Edward (initialed85) | Migrating From AWS EKS to GCP GKE | 2024 | <https://initialed85.cc/posts/migrating-from-aws-eks-to-gcp-gke/> |
| Better-Concept-1682 (Reddit) | Karpenter on GKE | 2025 | <https://www.reddit.com/r/kubernetes/comments/1mp9wo9/karpenter_on_gke/> |
| Cast AI | Is There a Karpenter Equivalent on GKE? | 2024 | <https://cast.ai/blog/gke-vs-karpenter/> |
| CloudOptimo | The True Cost of Cloud Data Egress | 2024 | <https://www.cloudoptimo.com/blog/the-true-cost-of-cloud-data-egress-and-how-to-manage-it/> |
| Cozzie, Anthony (Tamr) | 8 DevOps tools that smoothed our migration from AWS to GCP | 2017 | <https://cloud.google.com/blog/products/containers-kubernetes/8-devops-tools-that-smoothed-our-migration-from-aws-to-gcp-tamr> |
| Datadog Engineering | It's always DNS . . . except when it's not | 2022 | <https://www.datadoghq.com/blog/engineering/grpc-dns-and-load-balancing-incident/> |
| Duran, Fernando (SadServers) | Migrating Kubernetes out of the Big Cloud Providers | 2024–2025 | <https://docs.sadservers.com/blog/migrating-k8s-out-of-cloud-providers/> |
| external-secrets/external-secrets | Issue #772 (cross-project Secret Manager) | 2022 | <https://github.com/external-secrets/external-secrets/issues/772> |
| external-secrets/external-secrets | Issue #2017 (idbindtoken 404) | 2023 | <https://github.com/external-secrets/external-secrets/issues/2017> |
| external-secrets/kubernetes-external-secrets | Issue #591 (legacy chart provider mismatch) | 2020 | <https://github.com/external-secrets/kubernetes-external-secrets/issues/591> |
| GoogleCloudPlatform/cloud-sql-proxy | Issue #1078 (Workload Identity setup) | 2021 | <https://github.com/GoogleCloudPlatform/cloud-sql-proxy/issues/1078> |
| GoogleContainerTools/distroless | Issue #327 (java should set networkaddress.cache.ttl) | 2019 | <https://github.com/GoogleContainerTools/distroless/issues/327> |
| GoogleCloudPlatform/gke-managed-certs | Issue #13 (FAILED_NOT_VISIBLE) | 2019 | <https://github.com/GoogleCloudPlatform/gke-managed-certs/issues/13> |
| GoogleCloudPlatform/gke-managed-certs | Issue #14 (Insufficient Permission) | 2019 | <https://github.com/GoogleCloudPlatform/gke-managed-certs/issues/14> |
| GoogleCloudPlatform/gke-managed-certs | Issue #42 (different ingress class) | 2019 | <https://github.com/GoogleCloudPlatform/gke-managed-certs/issues/42> |
| googleapis/google-auth-library-python | Issue #604 (DefaultCredentialsError on MDS slow start) | 2021 | <https://github.com/googleapis/google-auth-library-python/issues/604> |
| googleapis/google-cloud-go | Issue #6315 (pubsub random unauthenticated errors) | 2022 | <https://github.com/googleapis/google-cloud-go/issues/6315> |
| googleapis/google-cloud-go | Issue #6574 (compute IAP key 500) | 2022 | <https://github.com/googleapis/google-cloud-go/issues/6574> |
| Google Cloud (Brain Corp customer story) | Brain Corp migrates from AWS EKS to GKE Autopilot | 2022 | <https://cloud.google.com/blog/products/containers-kubernetes/brain-corp-migrates-from-aws-eks-to-gke-autopilot> |
| Google developer forum | Cloud SQL Proxy randomly losing connection | 2024 | <https://discuss.google.dev/t/cloud-sql-proxy-randomly-loosing-connection-due-to-being-not-authorized/129191> |
| hashicorp/terraform-provider-google | Issue #3782 (429: quota exceeded) | 2019 | <https://github.com/hashicorp/terraform-provider-google/issues/3782> |
| Holditch, Kevin & McFarlane, Ross (Form3) | How To Run on Three Clouds at Once, and When Not To | 2026 | <https://www.infoq.com/news/2026/03/form3-triple-active-multicloud/> |
| istio/istio | Issue #43185 (Workload Identity metadata fields) | 2023 | <https://github.com/istio/istio/issues/43185> |
| istio/istio | Issue #54453 (multi-Gateway shared LB IP collision) | 2024 | <https://github.com/istio/istio/issues/54453> |
| Isenberg, Karl (Cruise) | Kubernetes at Cruise: Two Years of Multitenancy | 2019 | <https://www.youtube.com/watch?v=m19D9vZ1QFQ> |
| jwcesign (Reddit) | Karpenter GCP Provider is available now! | 2025 | <https://www.reddit.com/r/kubernetes/comments/1m7caam/karpenter_gcp_provider_is_available_now/> |
| Kaila, Ganesh (Searce) | Migrating Kubernetes Workloads from GKE to EKS | ~2020 | <https://blog.searce.com/migrating-kubernetes-workloads-from-gke-to-eks-f541acb8f269> |
| karpenter-provider-aws | Issue #5676 (instance type rejected) | 2024 | <https://github.com/aws/karpenter-provider-aws/issues/5676> |
| kubernetes/autoscaler | Issue #6060 (HPA + VPA limitation) | 2023 | <https://github.com/kubernetes/autoscaler/issues/6060> |
| kubernetes-csi/external-provisioner | Issue #1217 (PV stuck Terminating) | 2022 | <https://github.com/kubernetes-csi/external-provisioner/issues/1217> |
| kubernetes/kubernetes | Issue #69697 (PV stuck terminating) | 2018 | <https://github.com/kubernetes/kubernetes/issues/69697> |
| kubernetes-sigs/aws-load-balancer-controller | Issue #2271 (group.name creates new ALB) | 2021 | <https://github.com/kubernetes-sigs/aws-load-balancer-controller/issues/2271> |
| kubernetes-sigs/aws-load-balancer-controller | Issue #2949 (annotations invalid) | 2023 | <https://github.com/kubernetes-sigs/aws-load-balancer-controller/issues/2949> |
| kubernetes-sigs/gateway-api | Issue #1275 (ManagedCertificate not found) | 2022 | <https://github.com/kubernetes-sigs/gateway-api/issues/1275> |
| Kurtti, Niko (Shopify) | Keep Building Fresh: Shopify's Journey to Kubernetes (SREcon18 EU) | 2018 | <https://www.usenix.org/conference/srecon18europe/presentation/kurtti> |
| levkk (PostgresML) | We migrated from AWS to GCP with minimal downtime | 2024 | <https://news.ycombinator.com/item?id=40609908> |
| Little, Tim (Kudos) | Our migration journey from AWS to Google Cloud — Part 1 | 2018–2019 | <https://medium.com/kudos-engineering/our-migration-journey-from-aws-to-google-cloud-part-1-542b6e40b730> |
| Little, Tim (Kudos) | Our migration journey from AWS to Google Cloud — Part 2 | 2019 | <https://medium.com/kudos-engineering/our-migration-journey-from-aws-to-google-cloud-part-2-e66bd53b5d9a> |
| Liu, Yunpeng & Zhang, Andy (Niantic) | Scaling Geo-Temporal ML for Pokémon GO | 2025 | <https://kccncna2025.sched.com/event/27FUm/keynote-scaling-geo-temporal-ml-how-pokemon-go-optimizes-global-gameplay-with-kubernetes-and-kubeflow> |
| Lv, Able (Airwallex) | How We Deal with a GKE Metadata Server Outage | 2022 | <https://medium.com/airwallex-engineering/how-we-deal-with-a-google-kubernetes-engine-gke-metadata-server-outage-b627958ee7ad> |
| Luska, Jiri (Jamf) | How three lines of configuration solved our gRPC scaling issues | 2022 | <https://medium.com/jamf-engineering/how-three-lines-of-configuration-solved-our-grpc-scaling-issues-in-kubernetes-ca1ff13f7f06> |
| MeilleursAgents (HN discussion) | Story of a Successful Migration to Google Cloud Platform | 2017 | <https://news.ycombinator.com/item?id=14607300> |
| Mirensky, Olga (ANZ Bank) | Lessons Learned Running GKE Clusters on Spot Instances (SREcon23 APAC) | 2023 | <https://www.usenix.org/conference/srecon23apac/presentation/mirensky> |
| morzzz007 | wait-for-workload-identity (init container workaround) | 2020 | <https://github.com/morzzz007/wait-for-workload-identity> |
| NTC Tech | Cloud Egress Costs Explained | 2024 | <https://dev.to/ntctech/cloud-egress-costs-explained-why-your-architecture-is-paying-a-tax-you-never-modeled-554c> |
| OkEngineering8530 (Reddit) | Traffic Cutover Strategy for Ingress Nginx Migration | 2025 | <https://www.reddit.com/r/kubernetes/comments/1qu3dxm/traffic_cutover_strategy_for_ingress_nginx/> |
| Passing, Franka (Duolingo) | Duolingo's Kubernetes Leap (InfoQ) | 2025 | <https://www.infoq.com/presentations/duolingo-eks-kubernetes/> |
| Redis docs | FAILOVER command | ongoing | <https://redis.io/docs/latest/commands/failover/> |
| RevenueCat (HN discussion) | Postmortem for RevenueCat's Aurora Postgres Migration Turned Downtime | 2022 | <https://news.ycombinator.com/item?id=33877950> |
| rudderstackdev (Reddit) | My experience with Vertical Pod Autoscaler | 2025 | <https://www.reddit.com/r/kubernetes/comments/1nhczxz/my_experience_with_vertical_pod_autoscaler_vpa/> |
| Russell, Tyler | Save yourself time: Set the JVM TTL for DNS name lookups in AWS | 2025 | <https://tylerrussell.dev/2025/06/02/save-yourself-time-set-the-jvm-ttl-for-dns-name-lookups-in-aws/> |
| Sandlayth (Reddit) | GKE: How to Reliably Block Egress to Metadata IP | 2025 | <https://www.reddit.com/r/kubernetes/comments/1kz8auo/gke_how_to_reliably_block_egress_to_metadata_ip/> |
| sanpoke18 (Reddit) | Struggling with High Unused Resources in GKE | 2025 | <https://www.reddit.com/r/kubernetes/comments/1pe41bl/struggling_with_high_unused_resources_in_gke_bin/> |
| ScaleOps | HPA's Three Architectural Flaws | 2024 | <https://scaleops.com/blog/hpas-three-architectural-flaws-and-why-your-autoscaling-keeps-failing/> |
| Shaikh, Asif | Fixing Insufficient Regional Quota for SSD_TOTAL_GB | 2023 | <https://medium.com/@asifsource/fixing-error-403-insufficient-regional-quota-to-satisfy-request-resource-ssd-total-gb-when-6390c7f40770> |
| Shoddy_5385 (Reddit) | What Kubernetes feature looked great on paper but hurt you in prod? | 2025 | <https://www.reddit.com/r/kubernetes/comments/1r9q60h/what_kubernetes_feature_looked_great_on_paper_but/> |
| Shopify Engineering | Shopify's Infrastructure Collaboration with Google | 2018 | <https://shopify.engineering/shopify-infrastructure-collaboration-with-google> |
| Sift Engineering | Migrating our cloud infrastructure to Google Cloud (Part 1/4) | ~2019 | <https://engineering.sift.com/gcp-data-mig-1/> |
| Song, Jimmy (Tetrate) | Migrating from AWS App Mesh to Istio | 2024 | <https://tetrate.io/blog/migrating-from-aws-app-mesh-to-istio-a-comprehensive-guide> |
| strimzi-kafka-operator | Issue #477 (multi-zone gce-pd race) | 2018 | <https://github.com/strimzi/strimzi-kafka-operator/issues/477> |
| strimzi-kafka-operator | Issue #1307 (lost+found crash) | 2019 | <https://github.com/strimzi/strimzi-kafka-operator/issues/1307> |
| Tasrie IT Services | Karpenter vs Cluster Autoscaler: We Migrated 50+ Clusters | 2026 | <https://tasrieit.com/blog/karpenter-vs-cluster-autoscaler-eks-comparison-2026> |
| Aldinger, Steven (TeamSnap) | Accessing AWS Resources from GKE | 2024 | <https://medium.com/teamsnap-engineering/accessing-aws-resources-from-google-kubernetes-engine-3df21a8ca297> |
| Thukral, Karan (Shopify) | Building Shopify's PaaS on Kubernetes (SREcon18 Americas) | 2018 | <https://www.usenix.org/conference/srecon18americas/presentation/thukral> |
| Uber Engineering | Migrating Uber's Compute Platform to Kubernetes | 2025 | <https://www.uber.com/blog/migrating-ubers-compute-platform-to-kubernetes-a-technical-journey/> |
| Umiker, Jason | Cross-cloud identities between GCP and AWS from GKE and/or EKS | 2025 | <https://jason-umiker.medium.com/cross-cloud-identities-between-gcp-and-aws-from-gke-and-or-eks-182652bddadb> |
| Vayghan, Leila (Shopify) | Enhancing Elasticsearch Performance with KEDA (SREcon24 EMEA) | 2024 | <https://www.usenix.org/conference/srecon24emea/presentation/vayghan> |
| Wiggers, Steef-Jan (InfoQ) | AWS Sunsets More Services, Including AWS App Mesh | 2024 | <https://www.infoq.com/news/2024/10/aws-retires-services/> |
| Xia, David (Spotify) | How Spotify Accidentally Deleted All its Kube Clusters | 2019 | <https://www.youtube.com/watch?v=ix0Tw8uinWs> |

---

## Contributing

Add a new entry when:
- A real migration run uncovers a failure mode the existing lessons did not anticipate.
- A new public source meets the bar for an existing lesson and corroborates / extends it.

Each entry must:
- Have an `LFF-NN` ID (next available integer).
- Cite at least one verifiable source URL.
- Paraphrase the lesson — no >15-word verbatim quotes from any source.
- Attribute the source's author from the source page itself.
- Name the owning phase.
- Specify a severity (1, 2, or 3) with a reasoned justification.

Re-link in the *Index by failure category*, *Index by owning phase*, and *Source index* tables when adding.

## Notes on coverage

Some pain points lack a strong single citation but are widely understood failure patterns. We do not invent citations; if no public source meets the bar, the lesson is not in this file. Notable gaps as of this version:

- A canonical first-person practitioner narrative of an EKS→GKE migration at >1k-node scale. Brain Corp is the largest publicly-cited customer, but the post is a vendor-blog story. Until a non-vendor narrative at that scale is published, treat the Brain Corp post as a partial source.
- Public dollar figures for cross-cloud egress overruns are rare; the LFF-01 sources are the strongest available.
- App Mesh → ASM migration field reports are mostly vendor-authored. No first-person customer narratives surfaced.
- HPA + VPA flapping incident postmortems with specific company attribution are not public; we cite the upstream warning instead.

A pull request that closes one of these gaps with a verifiable, attributable source is a high-impact contribution.
