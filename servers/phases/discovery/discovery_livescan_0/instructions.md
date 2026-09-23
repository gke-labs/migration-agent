# Discovery Step 0 — Scan the Live AWS Estate

**DAG state:** `STATE_DISCOVERY_LIVE` · **Expected tool call:** `discover_and_dump_all_clusters`

This is the first step of discovery, before the static IaC is indexed. It captures
the **operational reality**: what is actually running in the customer's EKS clusters
and the AWS resources around them, projected to key names and structural fields and
written to the ledger as the Live IR.
A later reconciliation diffs the static Terraform/GitOps sources against it, because
production always drifts from source — manual `kubectl` edits, emergency hotfixes,
Karpenter-provisioned nodes and dynamic PVCs that no Terraform state reflects.

The scan is **read-only** against AWS and the clusters, and uses only the credentials
already in the platform engineer's local environment. Nothing here mutates the estate,
and no free-form value a customer authored is written to the ledger: every object is projected to its key names and structural fields (names, references, images, ports, classes, selectors, taints and tolerations, resource requests, hostnames and route paths, a short exact-key list of platform labels, class/mode/role annotations, the external-dns hostname annotation, StorageClass parameters, an S3 volume's bucket and a redirect's target hostname) and every other string value — a Secret or ConfigMap value, an env literal, a command token, a custom annotation, an AWS tag or label — is replaced by the `<omitted>` marker. Values never leave the process; the record says *what exists and where it comes from*, which is what a migration needs.

## What to do

1. Ask the platform engineer which **AWS region(s)** the EKS estate runs in. Do not
   guess, and do not scan every region — that is slow and usually wrong. Then call
   `discover_and_dump_all_clusters(regions=[...])`.
2. If they already know the specific clusters, pass `cluster_names=[...]` to narrow the
   walk. Omit it to take every cluster in the named regions.
3. If they use a named AWS profile rather than the default credential chain, pass
   `aws_profile=<name>`.
4. To narrow the in-cluster walk to specific namespaces, pass `namespaces=[...]`.
   Omit it to walk every namespace except the AWS system ones (`kube-system`,
   `kube-public`, `kube-node-lease`, `amazon-cloudwatch`, `aws-observability`,
   `amazon-guardduty`) — their workloads are EKS plumbing that GKE replaces
   wholesale. Naming a system namespace explicitly does include it.
5. The scan lands under `platform/discovery/live/`: `live_discovery.json` is
   the Live IR (every object projected — its structure, names and references,
   with every free-text value shown as `<omitted>`) and one CSV per table sits
   beside it. A `<omitted>` value is not missing data: it is a value the scan
   deliberately did not record. Ask the platform engineer for it if a
   translation needs it.
6. Present the result as a short summary: how many clusters were found and fully
   walked, any that could not be reached, and whether Karpenter is in use. Lead
   with anything unreachable — that is what a platform engineer acts on first.
7. **Relay every coverage note verbatim.** They record what was *not* covered: a region
   that was denied, a cluster whose control plane did not answer, a listing that hit its
   page cap. An unreachable cluster keeps its AWS-side record but has no in-cluster
   workloads — say so, because a half-walked estate must never read as a smaller one.
8. The graph advances to the static index (`STATE_DISCOVERY`). Call
   `discover_configuration_files()` next.

The Live IR lands at `platform/discovery/live/live_discovery.json`, one CSV
per table beside it. Its schema is
`servers/dag/server/schema/live_discovery.json`; if a coverage note says the IR
does not match its schema, relay it like any other note — the scan is recorded in
full and the graph has advanced; the note is a bug report for the agent's authors,
not a reason to re-run.

## If there is no live AWS access

Some engagements have no live estate to scan at discovery time — a repos-only
engagement, or a green-field target with no source cluster. In that case call
`discover_and_dump_all_clusters(skip=True, skip_reason="…")` with a short reason. This
advances to the static index and records that the live estate was deliberately not
scanned, so the empty Live IR reads as a decision rather than a gap. A skip also clears
whatever an earlier scan left under the live prefix, so a record from a scan that has since
been superseded never stands beside the skip. Do not skip merely because the first call
errored — fix the error and retry.

## What the scan needs

Read-only AWS access for the caller's credentials: `sts:GetCallerIdentity`;
`eks:ListClusters`, `eks:DescribeCluster`, `eks:ListAddons`, `eks:DescribeAddon`,
`eks:ListNodegroups`, `eks:DescribeNodegroup`; `ec2:DescribeVpcs`, `ec2:DescribeSubnets`,
`ec2:DescribeSecurityGroups`, `ec2:DescribeRouteTables`;
`autoscaling:DescribeAutoScalingGroups`; `elasticloadbalancing:DescribeLoadBalancers`,
`elasticloadbalancing:DescribeTags`; and `iam:GetRole`, `iam:ListAttachedRolePolicies`,
`iam:ListRolePolicies` (these three are optional — a `GetRole` denial records the role with
`exists: null` and the error, a policy-listing denial keeps `exists: true` and notes what was
not read on the entry). Nothing that writes. On each cluster the same principal
needs a read-only Kubernetes identity (an EKS access entry, or an `aws-auth` mapping):
`get`/`list` on the core objects the `view` ClusterRole grants **plus secrets** (read only for
their key names and type — the value never leaves the process), nodes,
persistentvolumes, storageclasses, ingressclasses, and the
optional CRD groups (`karpenter.sh`, `karpenter.k8s.aws`, `gateway.networking.k8s.io`,
`networking.istio.io`, `traefik.io`/`traefik.containo.us`, `external-secrets.io`,
`secrets-store.csi.x-k8s.io`). A cluster that refuses the identity is a per-cluster error
on a completed scan, not a failure of the step.

## What gets captured

| Plane | Captured |
|---|---|
| Compute | EKS cluster metadata and versions, endpoint access, add-ons; managed node groups + ASGs (instance types, ON_DEMAND/SPOT, AMI type); Karpenter NodePools/NodeClasses; live nodes; pod node-selectors, taints, tolerations, affinity, topology spread, requests/limits |
| Networking | VPC IDs and CIDRs, subnets per AZ, security groups; ALB/NLB from the AWS LB Controller; IngressClasses and OSS ingress controllers; Gateway API, Istio and Traefik routing objects (IngressRoutes and Middlewares) |
| Identity | IRSA bindings joined to their AWS trust policies and attached policies (whether the role still exists — `null` when it could not be checked: IAM not readable, the role in another account, or the annotation not a role ARN at all) |
| Storage | PVCs joined to their PVs and StorageClasses/provisioners (EBS/EFS/S3 CSI), with the GKE target for each |
| Config | ConfigMaps and Secrets as key names and type — every value is `<omitted>` (Helm release records and service-account tokens skipped and counted); external secret references (External Secrets Operator, Secrets Store CSI) as store names and the remote object names spelled as fields (`remoteRef.key`, a `secretObjects` entry's `objectName`; the CSI `parameters.objects` YAML is the marker) |
| Images | ECR (and other) image references, classified ECR vs other, declared vs actually running |

## If the scan fails

Report the error and stop. A failure — a missing or expired credential (`aws sts
get-caller-identity` to check, refresh SSO if needed), a wrong profile, an
`InvalidClientTokenId` from every requested region (a key that does not exist, or regions
the account has not enabled — one such region among others that authenticate is a
coverage note instead, and a region that could not be reached at all is named beside the
refusal), an exception in
the walk, or no requested region that would list its clusters (every name mistyped,
`eks:ListClusters` denied everywhere) — leaves the state here, so the step is retryable
once the problem is fixed. Do not skip ahead to the static index: skipping is only for
when there is genuinely no live access, not for working around an error.

A region denied among others that listed (named in the summary's `regions_unreachable`)
or a cluster that refuses the token is **not** a failure: the scan
completes with a coverage note or a per-cluster error (step 7), and the graph advances,
so a second call is refused as out of state. The per-cluster error says why the cluster
refused: the AWS credentials themselves expired when a fresh token could not be minted;
otherwise a missing EKS access entry, or session credentials that expired during the
walk — `aws sts get-caller-identity` failing now says which. There is no per-step rewind
(the Migration Admin's `reset_dag_state("platform")` starts the whole platform journey
over), so relay the note and go on: the static index covers what the walk could not
reach, and the note keeps the gap visible downstream.
