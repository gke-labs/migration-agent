# Pod DNS translation — pod-level DNS settings from EKS to GKE with Cloud DNS

You are translating workload manifests whose pod specs carry their own DNS settings. The brief
lists them per document (`Pod DNS facts of <Kind> <name>: ...`) and the unit carries them
structured as `pod_dns_facts`: `dnsPolicy`, `dnsConfig.nameservers`, `dnsConfig.searches`,
`dnsConfig.options`, `hostAliases` and `hostNetwork`, exactly as the source declares them. Nothing
has interpreted them before you. The platform side has already moved the **cluster** resolver
(CoreDNS on EKS becomes Cloud DNS for GKE, through the landing zone's `cluster-dns` unit); this
document is about what a **pod** says on its own, and what each statement becomes on GKE.

## The target

On GKE with Cloud DNS, a pod resolves through its **node**: the node hands every pod the address of
the local Cloud DNS data plane (`169.254.169.254`), or of NodeLocal DNSCache (`169.254.20.10`) when
the cache is on — it is on by default in the landing zone and always on in Autopilot. The `kube-dns`
Service still exists but pods do not use its address. Two consequences drive the whole table:

- **Nothing a pod pins by address survives.** The EKS cluster DNS ClusterIP (`10.100.0.10` or
  `172.20.0.10` by default — the `.10` of the cluster's service range) and the AWS VPC resolver
  (`169.254.169.253`, or the `.2` address of the VPC's CIDR) do not exist on GCP. Nor should a pod pin
  the GKE addresses above: they are what the node hands out anyway, and pinning them breaks on the
  next platform change.
- **Cluster names, stub domains and upstreams come from the platform.** The `cluster-dns` unit
  carries the source Corefile's stub domains and upstream nameservers — through the `kube-dns`
  ConfigMap on **Standard** (cluster scope), through Cloud DNS forwarding zones otherwise. The
  workload side cannot see which it did; a pod that needs a domain the cluster resolver serves asks
  the platform team to confirm it, it does not re-create the resolver in its own `dnsConfig`.

The cluster domain stays `cluster.local` under the landing-zone default (Cloud DNS cluster scope), so
search domains under it are portable. NodeLocal DNSCache caps cached TTLs at 30 s (5 s for negative
answers); a pod whose `dnsConfig.nameservers` names a resolver bypasses that cache.

## Mapping

`exports.cluster.type` (restated in the brief's cluster type line) is `standard`, `autopilot` or
null. Where the columns differ, null means the Autopilot column; say so in `assumptions`.

| Pod setting as written | Standard | Autopilot or null | Notes |
|---|---|---|---|
| `dnsPolicy` absent or `ClusterFirst`, no `dnsConfig` | keep | keep | Nothing to do. This is the case for most pods. |
| `dnsPolicy: ClusterFirstWithHostNet` (a `hostNetwork` pod) | keep | keep | Add a `tradeoffs` line: on GKE Dataplane V2 with NodeLocal DNSCache, a hostNetwork pod under this policy may not reach the cluster DNS backends (a documented GKE limitation); the platform team confirms the data plane. Never change the policy. |
| `hostNetwork: true` with `dnsPolicy` absent or `ClusterFirst` | keep | keep | Kubernetes silently applies `Default` to such a pod. Say so in `assumptions`; if the pod needs cluster names, raise an `open_questions` entry naming `ClusterFirstWithHostNet` — do not change the policy yourself. |
| `dnsPolicy: Default` | keep | keep | The pod inherits the node's resolver. On EKS that was the VPC resolver with the EC2 search suffixes (`ec2.internal`, `<region>.compute.internal`); on GKE it is the Cloud DNS data plane without them. One `assumptions` line: the pod does not rely on an EC2 suffix. |
| `dnsConfig.nameservers` entry that is the EKS cluster DNS address (`10.100.0.10`, `172.20.0.10`, or any address ending in `.0.10` inside an RFC 1918 `/16`) | drop, one `tradeoffs` line naming the address | the same | On GKE the node provides cluster resolution; the address does not exist. An address off the two defaults is a guess by its shape — name it in `open_questions` as the likely cluster DNS address rather than keeping it silently. |
| `dnsConfig.nameservers` entry that is the AWS VPC resolver (`169.254.169.253`) | drop, one `tradeoffs` line | the same | Does not exist on GCP; the node's resolver takes its place. |
| `dnsConfig.nameservers` entry that is an RFC 1918 address ending in `.2` (`10.0.0.2`, `10.42.0.2`) | `open_questions` naming both readings; keep it in the file | the same | It is either the source VPC's resolver (which maps to nothing) or a corporate resolver that happens to end in `.2`; a pod spec does not show the VPC range, so do not decide. The operator answers; the contract accepts the kept value while the question is open. |
| `dnsConfig.nameservers` entry that is any other address (a corporate or on-prem resolver) | keep, `open_questions` on reachability from the GKE VPC, `tradeoffs` line that the pod bypasses NodeLocal DNSCache for it | the same | The address may be reachable over the landing zone's peering or Interconnect, or not; only the platform team knows. |
| `dnsPolicy: None` whose nameservers are ALL cluster DNS or VPC resolver addresses | `dnsPolicy: ClusterFirst`, remove `dnsConfig.nameservers`, `tradeoffs` line naming the removed addresses | the same | A `None` pod with nothing left to name has no resolver; `ClusterFirst` gives it the node's, which is what the source addresses provided. Keep `searches` and `options`. On a `hostNetwork` pod, `ClusterFirstWithHostNet` is required if a cluster DNS address was among them (row below); if only the VPC resolver was, either policy is accepted — say in `assumptions` which you chose and why. |
| `dnsPolicy: None` on a `hostNetwork: true` pod whose nameservers include a cluster DNS address | `dnsPolicy: ClusterFirstWithHostNet`, then the row below for the other resolvers | the same | This is how an EKS host-network pod got cluster names. `ClusterFirst` would silently mean `Default` on host networking; `ClusterFirstWithHostNet` is the policy that keeps cluster names (with the Dataplane V2 tradeoff line above). |
| `dnsPolicy: None` whose nameservers MIX a cluster DNS address with other resolvers | `dnsPolicy: ClusterFirst`, drop the cluster DNS address, `open_questions` addressed to the platform team: the remaining resolvers belong in the `cluster-dns` unit (`upstreamNameservers` or a stub domain on Standard) | the same, the remaining resolvers belong in a Cloud DNS forwarding zone | On EKS the first entry gave the pod cluster names and the others gave it corporate names. On GKE a pod cannot have both by address: keeping `None` with only the corporate resolvers ships a pod that resolves no cluster name. Do not keep the other resolvers under `ClusterFirst` either — they are then a cluster-wide concern. Drop them from the file and name each in `open_questions`. |
| `dnsConfig.searches` entry ending in `ec2.internal` or `compute.internal` | drop, `tradeoffs` line naming it | the same | EC2 suffixes; nothing on GCP answers them. |
| `dnsConfig.searches` entry under `cluster.local` (`orders.svc.cluster.local`) | keep | keep | The cluster domain is unchanged. |
| `dnsConfig.searches` entry that is a corporate domain (`corp.example.com`) | keep, `open_questions`: confirm the `cluster-dns` unit carries the domain | keep, the same question | The search suffix only helps if the cluster resolver serves the domain; on Standard that is a stub domain, on Autopilot a forwarding zone, and the workload side cannot see either. |
| `dnsConfig.options` (`ndots`, `timeout`, `attempts`, `single-request-reopen`, `use-vc`, ...) | keep verbatim | keep verbatim | Resolver options are the pod's; none depends on the cloud. Do not add any. |
| `hostAliases` | keep verbatim; one `open_questions` entry per alias address | the same | An alias to an AWS-private address (an RDS endpoint at `10.0.x.x`) will not exist on GCP; the workload side cannot know which addresses do. Keep every entry and every hostname; ask. |
| A pod template inside a kind outside the closed portability table (an Argo Rollout, an operator CR) that carries any of the above | apply the same rows to that template | the same | The residual-bucket rule ("emit unchanged") does not cover a cluster DNS address left in a pod template: the address is dead on GKE whatever kind carries it. |

## Output contract (machine-checked at validation)

1. The shipped pod specs carry no nameserver from the closed list — `169.254.169.253`,
   `10.100.0.10`, `172.20.0.10`, `169.254.169.254`, `169.254.20.10` — and no search domain ending in
   `ec2.internal` or `compute.internal`.
2. `dnsPolicy` is one of `Default`, `ClusterFirst`, `ClusterFirstWithHostNet`, `None`; a `None` pod
   has at least one nameserver; at most 3 nameservers and 32 search domains.
3. `dnsPolicy` is unchanged from the source (absent counts as `ClusterFirst`), with one permitted
   change: `None` to `ClusterFirst`; a `hostNetwork` pod may become `ClusterFirstWithHostNet`
   instead. The change is **required** when a `None` pod's source nameservers include a cluster
   DNS address, and on a `hostNetwork` pod only `ClusterFirstWithHostNet` satisfies it then.
4. Every source nameserver, search domain, option and host alias (address and hostnames) is either
   kept in the shipped pod specs or named in `tradeoffs`, `open_questions` or `assumptions`. Name
   each address and domain exactly as the source writes it; an option is named by its name
   (`ndots`), and a kept option keeps its value.
5. Nothing appears that the source did not carry: no new nameserver, search domain, option or host
   alias. The GKE side needs none. (When a render source did not render at plan time, the facts
   are incomplete and only this item is suspended; every other item still runs over the facts
   that were read.)
6. A source document that carries pod DNS settings is present in the output under its kind and
   name. Removing a pod's last DNS field is fine (the pod is still there, now on the defaults);
   removing or renaming the document is not. A document with no `metadata.name` (a `kind: List`
   bundle) is not held to this — split it into named documents; its addresses and domains are
   still held to items 4 and 5.

## Sources

- Cloud DNS for GKE (2026-09-14): pods resolve through the node's Cloud DNS data plane
  (`169.254.169.254`), or through NodeLocal DNSCache (`169.254.20.10`); `kube-dns` keeps running
  but is not used; stub domains and upstream nameservers apply on Standard cluster scope only.
- NodeLocal DNSCache (2026-09-14): on by default in Autopilot; cached TTLs capped at 30 s, 5 s for
  NXDOMAIN; the Dataplane V2 + hostNetwork limitation.
- Kubernetes, DNS for Services and Pods (2026-09-14): the four policies, `None` requires
  `dnsConfig`, at most 3 nameservers, at most 32 search domains / 2048 characters, hostNetwork pods
  fall back to `Default` under `ClusterFirst`.
- Amazon EKS networking: the service range defaults (`10.100.0.0/16` or `172.20.0.0/16`, cluster
  DNS at `.10`), the VPC resolver (`.2` of the VPC CIDR, `169.254.169.253`), the EC2 search
  suffixes.
