# Cluster DNS translation — CoreDNS on EKS to Cloud DNS for GKE

You are translating the source cluster's DNS configuration. The inputs carry it **verbatim**:
`inputs.cluster_dns.sources[].text` is the CoreDNS Corefile, or the managed add-on's
`configuration_values` that wraps one, exactly as the source repository declares it — a heredoc, a
JSON string, or a raw `jsonencode({...})` object. Nothing has interpreted it before you. Read it as
CoreDNS reads it: server blocks (`<zone>[:port] { plugins }`), one plugin per line, and translate
each construct with the table below. `inputs.cluster_mode` is the cluster mode the planner derived
from the landing-zone decisions through the decision registry's one projection: `standard`,
`autopilot`, or **null** — either because no decision recorded a mode (`inputs.decision_reason` =
`none recorded`) or because the recorded decisions imply different modes (`inputs.decision_reason`
starts with `disagree:` and names them). It decides which column applies — and null means the
Autopilot column: the `kube-dns` ConfigMap is read on Standard only, so it is emitted only when
`inputs.cluster_mode` is `standard`, and the Cloud DNS zones work in either mode. Use
`inputs.cluster_mode`, not the raw `landing_zone_decisions` in your prompt and not `inputs.decision`
(the recorded `karpenter` token, kept for reference), to pick the column. When it is null because none
was recorded, say so in `assumptions`; when it is null because the decisions disagree, that is a
landing-zone design conflict — name it in `open_questions` and do not pick a mode yourself.

## The target

The GKE landing zone runs **Cloud DNS for GKE** as the cluster DNS provider (a standing default of
`gke-landing-zone.md`), with NodeLocal DNSCache. Cloud DNS is a managed data plane: there are no
resolver pods, no Corefile, and nothing to scale. Two mechanisms carry a customization:

- **The `kube-dns` ConfigMap in `kube-system`** — on **Standard** clusters in cluster scope, Cloud DNS
  applies `stubDomains` and `upstreamNameservers` from it. It is not read on Autopilot, and not under
  VPC scope.
- **Cloud DNS private zones on the VPC** — a private managed zone with a `forwarding_config` sends
  one domain to named resolvers; a private managed zone with record sets answers static names. Every
  cluster on the VPC that uses Cloud DNS resolves them, in either mode, so their reach is the whole VPC.

Never emit a `google_dns_policy`. A DNS policy is VPC-wide and a network can carry only one; its
`alternative_name_server_config` replaces the whole resolution order — private zones, forwarding
zones, Compute Engine internal names and the metadata server are then not consulted — so it would
break the very zones this unit emits. Query logging is a policy too; if the operator wants it, it is
a landing-zone matter: name it in `open_questions`.

Prefer the ConfigMap on Standard: it is cluster-scoped, which is what a Corefile was. Use the private
zones on Autopilot or when the mode is null, and on Standard only for what the ConfigMap cannot carry
(static names), saying in `tradeoffs` that the reach widens from the cluster to the VPC.

## Mapping

| Corefile construct | Standard (`inputs.cluster_mode = standard`) | Autopilot (`inputs.cluster_mode = autopilot`) or mode null | Notes |
|---|---|---|---|
| A non-root server block with `forward . <ip> [<ip>...]` (a stub domain: `corp.example.com:53 { forward . 10.1.2.3 10.1.2.4 }`) | `stubDomains` entry in the `kube-dns` ConfigMap: `{"corp.example.com": ["10.1.2.3", "10.1.2.4"]}` | `google_dns_managed_zone` with `visibility = "private"`, `dns_name = "corp.example.com."`, a `forwarding_config` naming the same IPs, and the landing-zone VPC's self-link in `private_visibility_config` (output contract item 3) | IPs and domain verbatim. Several stub domains are several map entries or several zones. |
| Root block `forward . <ip> [<ip>...]` where the IP is the **source VPC's resolver** — `169.254.169.253`, or the `.2` address of the VPC's CIDR (`10.0.0.2` in a `10.0.0.0/16` VPC; the cluster's own addresses and the stub-domain targets tell you the range) | nothing, with a tradeoff naming the address | nothing, with a tradeoff naming the address | The same thing as `forward . /etc/resolv.conf`: the AWS resolver does not exist on GCP, and the GKE default already forwards to the VPC's resolution order. Carrying it into `upstreamNameservers` would send every off-cluster lookup to an address that answers nothing. |
| Root block `forward . <ip> [<ip>...]` with any other IP (a corporate or on-prem resolver) | open question naming the IPs — and only if the operator confirms it is reachable from the landing-zone VPC, `upstreamNameservers` in the `kube-dns` ConfigMap (at most three) | open question naming the IPs | Outside the cluster domain, resolution follows the VPC's order — private and forwarding zones first, then Google Public DNS; there is no per-cluster upstream on Autopilot, and the VPC-wide alternative (a DNS policy) would bypass every private zone. Ask what the resolver answered that the VPC order will not, whether it is reachable from GCP (Interconnect, VPN), and whether a forwarding zone per such domain covers it. The same reachability question applies to every stub-domain target: put it in `assumptions`. |
| Root block `forward . /etc/resolv.conf` | nothing | nothing | The GKE default already forwards to the VPC resolver. |
| `hosts { <ip> <name> ... fallthrough }` with inline entries | `google_dns_managed_zone` (private) + one `google_dns_record_set` per entry (type `A` for an IPv4 address, `AAAA` for IPv6, `rrdatas` = the IP, name verbatim with a trailing dot) | the same | The zone's `dns_name` is the common parent of the names, or one zone per name. One `resource` per zone with a literal `dns_name` — no `for_each`, no variable: the gate reads the domain off the resource. |
| `forward` options `tls://`, `tls_servername`, `except` | open question | open question | No equivalent on either mechanism. Name the option and the domain; cite the blocker category *CoreDNS customization with no Cloud DNS equivalent*. |
| `forward` options `max_concurrent`, `prefer_udp`, `force_tcp`, `policy`, `health_check`, `expire` | nothing, one line in `tradeoffs` | the same | Resolver-pod tuning; the EKS add-on's own default carries `max_concurrent 1000`. There are no resolver pods to tune, and this is not a blocker. |
| `rewrite`, `template`, `autopath`, any plugin not in this table (`file`, `etcd`, `k8s_external`, `acl`, `dnstap`, ...) | open question | open question | No equivalent. Same blocker category. For `autopath`, add the tradeoff that pods rely on the search list and `ndots`; the workload pipeline owns pod `dnsConfig`. |
| `cache <ttl>` other than the default | tradeoff | tradeoff | NodeLocal DNSCache caps cached TTLs (30 s, 5 s for NXDOMAIN) and the caps are not configurable; a longer cache does not carry over. |
| `log` | open question | open question | Query logging on Cloud DNS is a VPC-wide DNS policy — one per network, the landing zone's to own — not something this unit emits. |
| `errors`, `health`, `ready`, `kubernetes ...`, `prometheus`, `loop`, `reload`, `loadbalance` | nothing | nothing | The stock plugins; the managed data plane covers them. Do not mention them individually. |
| Add-on settings `replicaCount`, `resources`, `autoScaling`, `podDisruptionBudget`, `tolerations`, `affinity`, `nodeSelector`, `topologySpreadConstraints`, `computeType` | dropped, with a tradeoff | dropped, with a tradeoff | There are no resolver pods under Cloud DNS. One sentence in `tradeoffs` naming what was set. |
| A `node-local-dns` Corefile | fold its stub domains into the table above; the cache itself is a landing-zone default | the same | The node-local cache exists on GKE by default; only its forwarding rules matter. Its `bind` addresses (`169.254.20.10` plus the source cluster's kube-dns ClusterIP, `172.20.0.10`-style) and the AWS VPC resolver `169.254.169.253` have no GKE meaning: name the ClusterIP in `tradeoffs` as dropped, so the gate can see it was not lost by accident (the two link-local addresses are ignored by the gate). |
| A source the scan could not read (an `inputs` note, or a `cluster_dns_scan_notes` line, says the Corefile was behind a variable or a file) | open question naming the file | the same | Never guess what an unread Corefile said. |

Read the add-on's `configuration_values` for its `corefile` key first; that string is the Corefile.
Apply the table to it, then to the remaining keys as add-on settings.

## Output contract

The validate gate checks these mechanically. A unit that breaks one goes back to review.

1. **Standard (`inputs.cluster_mode = standard`):** at most one `ConfigMap`, named exactly `kube-dns`,
   in namespace `kube-system`. Its `data` carries only `stubDomains` and/or `upstreamNameservers`,
   each a JSON string: `stubDomains` an object of domain → list of IPs, `upstreamNameservers` a list
   of at most three IPs. No `Corefile` key — Cloud DNS does not read one, and a stray Corefile
   misleads the next reader.
2. **Autopilot, or mode null:** no `kube-dns` ConfigMap at all. Stub domains are Cloud DNS private
   forwarding zones; pinned upstreams are an open question.
3. Every `google_dns_managed_zone` sets `visibility = "private"` (a forwarding zone is a private zone
   with a `forwarding_config`), its `dns_name` is a literal string ending with a dot (one resource per
   zone, never `for_each` or a variable), and every `google_dns_record_set` sets
   `managed_zone = google_dns_managed_zone.<name>.name` for a zone this unit declares — never a
   variable, a data source or a literal naming a pre-existing zone. No `google_dns_policy`.
   `private_visibility_config.networks.network_url` needs the VPC's self-link, and the landing zone's
   `network` variable is a bare name: compose
   `"projects/${var.network_project}/global/networks/${var.network}"` from the two variables the
   landing zone already declares, never a literal project or network.
4. **Literal fidelity.** Every IP address in the source text appears in your files or, when you drop
   it, in `tradeoffs` / `open_questions` — nothing is silently lost. Every IP address and every domain
   name in your files comes from the source text or from the unit inputs — nothing is invented. Copy
   them character for character: a `stubDomains` key and a forwarding zone's `dns_name` are exactly
   the domain the Corefile forwards, never a parent of it, and a record set's `name` is exactly the
   host the Corefile gives; only a zone that holds `hosts` records may be named for the common parent
   of those records. The only object this unit may place in `kube-system` is the `kube-dns` ConfigMap.
5. **Owner boundary.** Never emit `google_container_cluster`, a `dns_config` block, `addons_config`,
   a `coredns` or `node-local-dns` Deployment, DaemonSet or ConfigMap, or a `kube-system` Namespace.
   The cluster and its DNS provider are the landing zone's; the resolver pods are GKE's.
6. If the inputs carry text and every construct in it maps to "nothing" or to an open question, still
   ship one file: a `README.md` is not allowed by the worker contract, so emit the `kube-dns`
   ConfigMap with an empty `data` map on Standard (it is inert) or, on Autopilot, a `variables.tf`
   declaring `var.network` — and put the whole analysis in `tradeoffs`. Say explicitly which
   constructs were stock and which have no equivalent.

## What to write in `tradeoffs`

One paragraph per construct you translated or dropped: what the Corefile did, what carries it on
GKE, and what changes (reach, TTL behaviour, ordering). Name the scan notes you relied on. A reviewer
who has never seen the Corefile must be able to check your ConfigMap against it line by line.

## Sources

The behaviour this document relies on, as read from the GKE documentation when it was written
(2026-09): *Using Cloud DNS for GKE* — stub domains and upstream nameservers from the `kube-dns`
ConfigMap are applied to Cloud DNS in cluster scope on Standard clusters only, and VPC scope ignores
the ConfigMap; Autopilot clusters run Cloud DNS cluster scope and NodeLocal DNSCache by default;
*Setting up NodeLocal DNSCache* — the cache picks up the same stub domains and caps TTLs at 30 s /
5 s; *Setting up a custom kube-dns Deployment / custom resolvers* — `stubDomains` and
`upstreamNameservers` keys, at most three upstream nameservers. If one of these moves, this file
is the place to say so.
