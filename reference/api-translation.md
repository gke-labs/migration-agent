# Annotation & API Translation Reference

The annotation-by-annotation map skills cite when translating manifests. If something isn't here, treat it as an escalation rather than guessing.

## Service / Ingress

### Service annotations

| EKS / AWS                                                                          | GKE / GCP                                                                                | Notes |
|------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------|-------|
| `service.beta.kubernetes.io/aws-load-balancer-type: external`                      | drop                                                                                      | Type is implied by `Service.spec.type` and Gateway selection |
| `service.beta.kubernetes.io/aws-load-balancer-nlb-target-type: ip`                 | drop                                                                                      | Use Container-native LB via NEG annotations |
| `service.beta.kubernetes.io/aws-load-balancer-scheme: internet-facing`             | drop (for Service); for L4 use Service `type: LoadBalancer` without internal annotation   |  |
| `service.beta.kubernetes.io/aws-load-balancer-scheme: internal`                    | `networking.gke.io/load-balancer-type: "Internal"`                                       |  |
| `service.beta.kubernetes.io/aws-load-balancer-cross-zone-load-balancing-enabled`   | (default behavior on GCP)                                                                | GCP regional LBs are cross-zone by default within the region |
| `service.beta.kubernetes.io/aws-load-balancer-ssl-cert: <ACM ARN>`                 | TLS via Gateway listener `tls.certificateRefs` or Certificate Manager attachment         |  |
| `service.beta.kubernetes.io/aws-load-balancer-ssl-ports: "443"`                    | Gateway listener port + protocol                                                         |  |
| `service.beta.kubernetes.io/aws-load-balancer-attributes: …`                       | `GCPBackendPolicy` (timeout, drainingTimeout, sessionAffinity, etc.)                     |  |
| `external-dns.alpha.kubernetes.io/hostname`                                        | Same — external-dns supports Cloud DNS provider; reconfigure provider                    |  |
| `external-dns.alpha.kubernetes.io/aws-…`                                           | drop; use Cloud DNS-equivalent annotations or Cloud DNS-native                            |  |

### Ingress / Gateway annotations

The closed disposition table of the workload routing unit (`wkld-routing`, which turns an Ingress into an HTTPRoute attached to the shared platform Gateway). The server parses it at start-up (`servers/dag/server/api_translation.py`) and briefs the worker with one row per annotation key actually present on a scoped Ingress, so this table and the brief cannot drift. Three dispositions, and only three:

- **mapped**: the HTTPRoute carries the behaviour; the rationale names the field.
- **dropped with tradeoff**: either the concern is the shared platform Gateway's to own (its scheme, listeners, certificates, address, placement), or the annotation has no GCP equivalent and nothing is lost by dropping it (tags, capacity pre-warming, target type); the worker drops the annotation and records the source value in the unit's tradeoffs so the platform side can verify parity. Owning is not the same as having: the platform Gateway ships HTTP-only today and records TLS as its own open question, so a dropped TLS annotation never lets the worker promise HTTPS.
- **open question**: the annotation adds a protection or a behaviour that nobody carries unless someone acts (a WAF, an allowlist, authentication, a custom health check, a timeout); the worker raises it, never approximates it, and never invents a policy object the unit's coverage-map row does not own. On GKE those objects (`HealthCheckPolicy`, `GCPBackendPolicy`) must live beside the Service in the workload namespace, so they are not the platform side's either: no unit owns them today, and the open question says so.

A key ending in `*` is a prefix row. Lookup is exact key first, then the longest matching prefix. A key no row covers is an open question by rule, never silently dropped. The unit sees the Ingress's annotation keys; a value-dependent split (HTTP versus HTTPS backend protocol) is stated in the rationale, not keyed. Service annotations (`service.beta.kubernetes.io/aws-load-balancer-*`) live on Services, which this unit does not translate; they stay in the Service table above.

**How to change this table.** Adding an annotation is an edit to this file: a new row with one key, a disposition from the vocabulary above and a rationale. The parser refuses a disposition outside the vocabulary, a key cell that is not one annotation key, a key listed twice, an empty rationale, a row with more than three cells (a `|` inside a rationale), or an empty table, and the server does not start until the row is fixed. The `does not infer health checks` phrasing of the health-check rows is pinned by `workload_plan_2/planner_test.py` (the `never silently dropped` phrasing belongs to the unknown-key rule, which is code). An exact row placed under a prefix row is allowed but deliberate: `api_translation_test.py` lists the pairs.

| Annotation | Disposition | Target / rationale |
|---|---|---|
| `kubernetes.io/ingress.class` | dropped with tradeoff | class selection is replaced by `parentRefs` attachment to the shared Gateway |
| `alb.ingress.kubernetes.io/scheme` | dropped with tradeoff | internet-facing vs internal is a property of the shared platform Gateway (`gke-l7-global-external-managed` or `gke-l7-regional-external-managed` vs `gke-l7-rilb`); record the source value in the tradeoff so the platform side can verify parity; an Ingress with no `scheme` annotation was internal (the controller default) |
| `alb.ingress.kubernetes.io/certificate-arn` | dropped with tradeoff | certificates are platform-side (Gateway `tls.certificateRefs` to a Secret on any class, or a Certificate Manager map on the global class only); an ACM ARN has no GCP meaning; record the ARN in the tradeoff; the platform Gateway ships HTTP-only today, so do not promise HTTPS |
| `alb.ingress.kubernetes.io/create-acm-cert` | dropped with tradeoff | the controller issued the certificate itself; the same platform-side concern as `certificate-arn`; record the hosts |
| `alb.ingress.kubernetes.io/acm-pca-arn` | dropped with tradeoff | a certificate from a Private CA; the same platform-side concern as `certificate-arn` (a private CA has no Certificate Manager map); record the ARN |
| `alb.ingress.kubernetes.io/ssl-policy` | dropped with tradeoff | the TLS policy belongs to the Gateway (a GCP SSL policy set through `GCPGatewayPolicy.spec.default.sslPolicy`); record the policy name for parity; no HTTPS listener exists yet to carry it |
| `alb.ingress.kubernetes.io/listen-ports` | dropped with tradeoff | listeners belong to the Gateway |
| `alb.ingress.kubernetes.io/listener-attributes.*` | dropped with tradeoff | listener attributes belong to the Gateway; record them |
| `alb.ingress.kubernetes.io/ssl-redirect` | open question | HTTP->HTTPS redirect is a Gateway/listener concern (on GKE it needs an HTTPS listener plus a `sectionName: http` route, which this unit may not emit); the platform Gateway ships HTTP-only for now, so until it carries TLS this application is served in cleartext, which the source never allowed: raise it as a security question, not only a listener question |
| `alb.ingress.kubernetes.io/actions.ssl-redirect` | open question | the older spelling of `ssl-redirect` (a redirect action to HTTPS); the same concern and the same security question: the platform Gateway ships HTTP-only for now, so a `RequestRedirect` to https would point at a listener that does not exist and the application is served in cleartext until it does |
| `alb.ingress.kubernetes.io/mutual-authentication` | open question | client-certificate mTLS: the source refused clients without a trusted certificate, and nothing on the target does until the platform side configures frontend mTLS on the Gateway listener (`tls.frontendValidation`, or the `networking.gke.io/frontend-trust-config` annotation); until then the application accepts any client; record the trust store and mode and raise it as a security question |
| `alb.ingress.kubernetes.io/load-balancer-name` | dropped with tradeoff | the Gateway's name and address are platform-owned; record the source name |
| `alb.ingress.kubernetes.io/ip-address-type` | dropped with tradeoff | the address family is the Gateway's; record it |
| `alb.ingress.kubernetes.io/subnets` | dropped with tradeoff | placement is the platform VPC's; record the subnet ids |
| `alb.ingress.kubernetes.io/customer-owned-ipv4-pool` | dropped with tradeoff | address pools are the platform side's; record the pool |
| `alb.ingress.kubernetes.io/ipam-ipv4-pool-id` | dropped with tradeoff | address pools are the platform side's; record the pool |
| `alb.ingress.kubernetes.io/enable-frontend-nlb` | dropped with tradeoff | a frontend NLB in front of the ALB exists for a static address; the Gateway's address is the platform side's; record that it was on |
| `alb.ingress.kubernetes.io/frontend-nlb-*` | dropped with tradeoff | frontend NLB settings (scheme, subnets, security groups, health checks, EIPs); all belong to the platform side's Gateway address; record them |
| `alb.ingress.kubernetes.io/multi-cluster-target-group` | open question | sharing one target group across clusters; the GKE equivalent is a multi-cluster GatewayClass (`-mc`) with `ServiceImport` backendRefs, and the shared platform Gateway is single-cluster today by the platform side's choice; a lost capability, record it and raise it |
| `alb.ingress.kubernetes.io/tags` | dropped with tradeoff | AWS resource tags have no HTTPRoute equivalent (labels on the HTTPRoute are not load-balancer tags); record them for the platform side's labelling |
| `alb.ingress.kubernetes.io/load-balancer-attributes` | open question | the idle timeout (`idle_timeout.timeout_seconds`, ALB default 60, often raised for uploads, SSE and websockets) maps to `GCPBackendPolicy.timeoutSec` (default 30 s) and access logs to `GCPBackendPolicy.logging`, both beside the Service and outside this unit; `deletion_protection`, `drop_invalid_header_fields` and `client_keep_alive` have no equivalent; raise the idle timeout with the source value, record the rest |
| `alb.ingress.kubernetes.io/minimum-load-balancer-capacity` | dropped with tradeoff | capacity pre-warming is not a GCP concept |
| `alb.ingress.kubernetes.io/group.name` | mapped | ALB sharing becomes multiple HTTPRoutes attaching to the one shared Gateway — the Gateway API default |
| `alb.ingress.kubernetes.io/group.order` | open question | cross-Ingress rule precedence has no direct HTTPRoute equivalent within one component's view |
| `alb.ingress.kubernetes.io/actions.*` | mapped | a redirect action becomes an HTTPRoute `RequestRedirect` filter (`hostname`, `path.replaceFullPath`, `scheme`, `statusCode`; a redirect `port` is unsupported on every GKE GatewayClass and `query` has no field, both stay open questions) and a weighted forward action to Services becomes weighted `backendRefs`; a forward to a `targetGroupARN` or `targetGroupName` targets a group created outside Kubernetes, so it has no `backendRef` and stays an open question, as does `targetGroupStickinessConfig` (affinity is a per-Service `GCPBackendPolicy`, and weighted splitting overrides it); a fixed-response action has no HTTPRoute filter and stays an open question; a redirect to HTTPS is the `actions.ssl-redirect` row. The path that invokes the action names the action as its backend service with port name `use-annotation`: that is a sentinel, not a Service, so the named-port rule does not apply to it and no `backendRef` is emitted for it |
| `alb.ingress.kubernetes.io/transforms.*` | mapped | a `url-rewrite` whose regex is exactly 'strip or replace the matched prefix' of a PathPrefix rule becomes a `URLRewrite` filter with `path.replacePrefixMatch` (GKE supports no `replaceFullPath` for rewrites); a `host-header-rewrite` to a literal host becomes `URLRewrite.hostname`; any regex with a capture group in the replacement, or any other transform, stays an open question. No ALB transform adds or removes headers, so `RequestHeaderModifier` and `ResponseHeaderModifier` are not this row's output |
| `alb.ingress.kubernetes.io/use-regex-path-match` | open question | regular-expression matching for `ImplementationSpecific` paths only, in force when the value is `true` (the unit sees the key, not the value: a `false` value is the controller default and changes nothing, say so and move on) (Exact and Prefix paths are unaffected; the leading `/` is stripped from the regex); `RegularExpression` path matches exist on the Envoy-based classes (regional external, cross-regional internal, `gke-l7-rilb`) and not on `gke-l7-global-external-managed`, and the class is the platform side's; each such path needs a human decision, never a guessed prefix |
| `alb.ingress.kubernetes.io/conditions.*` | mapped | http-header and query-string conditions whose `values` contain no `*` or `?` become HTTPRoute `matches` (`headers`, `queryParams`, type Exact) on the rule of the path whose backend (a Service or an action) they name, one `matches` entry per OR'd value; a `path-pattern` condition adds a path match; a `host-header` condition cannot be a match (HTTPRoute hostnames are route-wide, an ALB condition is per path), so that path is split into its own HTTPRoute with the condition's host as its `spec.hostnames`, recorded in tradeoffs. `http-request-method` has no GKE match (no GKE GatewayClass supports `method`) and stays an open question; so do `source-ip`, any `regexValues`, and any `values` with `*` or `?` (only the Envoy-based classes accept `RegularExpression`, and the class is the platform side's) |
| `alb.ingress.kubernetes.io/target-type` | dropped with tradeoff | backend attachment is expressed by `backendRefs` + Service (NEG-based) |
| `alb.ingress.kubernetes.io/target-node-labels` | dropped with tradeoff | instance targets do not exist under container-native load balancing; record the labels |
| `alb.ingress.kubernetes.io/manage-backend-security-group-rules` | dropped with tradeoff | node-to-load-balancer rules are managed by GKE for NEG backends (except in a Shared VPC, where the platform side deploys the Gateway firewall rules by hand) |
| `alb.ingress.kubernetes.io/backend-protocol` | open question | HTTP is the default and needs nothing; HTTPS to the pod is `spec.ports[].appProtocol: HTTPS` on the Service (the Gateway controller does not read the Ingress-era `cloud.google.com/app-protocols` annotation) plus a matching HTTPS `HealthCheckPolicy`; both outside this unit |
| `alb.ingress.kubernetes.io/backend-protocol-version` | open question | HTTP2 is `appProtocol: HTTP2` (TLS to the pod) and GRPC is `appProtocol: HTTP2` or `kubernetes.io/h2c` (cleartext) on the Service; GKE Gateway routes gRPC through the same HTTPRoute (GRPCRoute is not supported on GKE); a gRPC backend also needs a `GRPC`-type `HealthCheckPolicy` because `GET /` never returns 200; all outside this unit |
| `alb.ingress.kubernetes.io/target-group-attributes` | open question | stickiness maps to `GCPBackendPolicy.sessionAffinity` (CLIENT_IP or GENERATED_COOKIE; weighted traffic splitting overrides it), deregistration delay to `connectionDraining.drainingTimeoutSec` (default 0), slow start to nothing; the policy must live beside the Service in the workload namespace; no unit owns that object today; raise them with the source values, do not invent one |
| `alb.ingress.kubernetes.io/healthcheck-*` | open question | the GKE Gateway controller does not infer health checks from readiness probes (unlike GKE Ingress): the default check is HTTP `GET /` against the serving port and requires exactly 200 (any 3xx or 4xx is unhealthy; 15 s interval), so a source health-check customisation may mean the default check fails; the fix is a `HealthCheckPolicy` beside the Service (same namespace), which no unit owns today. Raise it naming the source path, port, protocol, interval, thresholds and codes; do not invent the policy |
| `alb.ingress.kubernetes.io/healthy-threshold-count` | open question | a health-check knob: the GKE Gateway controller does not infer health checks from readiness probes (unlike GKE Ingress): the default check is HTTP `GET /` against the serving port and requires exactly 200 (any 3xx or 4xx is unhealthy; 15 s interval), so a source health-check customisation may mean the default check fails; the fix is a `HealthCheckPolicy` beside the Service (same namespace), which no unit owns today. Raise it naming the source path, port, protocol, interval, thresholds and codes; do not invent the policy |
| `alb.ingress.kubernetes.io/unhealthy-threshold-count` | open question | a health-check knob: the GKE Gateway controller does not infer health checks from readiness probes (unlike GKE Ingress): the default check is HTTP `GET /` against the serving port and requires exactly 200 (any 3xx or 4xx is unhealthy; 15 s interval), so a source health-check customisation may mean the default check fails; the fix is a `HealthCheckPolicy` beside the Service (same namespace), which no unit owns today. Raise it naming the source path, port, protocol, interval, thresholds and codes; do not invent the policy |
| `alb.ingress.kubernetes.io/success-codes` | open question | a health-check knob: the GKE Gateway controller does not infer health checks from readiness probes (unlike GKE Ingress): the default check is HTTP `GET /` against the serving port and requires exactly 200 (any 3xx or 4xx is unhealthy; 15 s interval), so a source health-check customisation may mean the default check fails; the fix is a `HealthCheckPolicy` beside the Service (same namespace), which no unit owns today. Raise it naming the source path, port, protocol, interval, thresholds and codes; do not invent the policy; Google Cloud health checks accept 200 only (the source allowed lists and ranges such as `200-300`), so a source that returned another code needs an application change or a dedicated health path |
| `alb.ingress.kubernetes.io/security-groups` | open question | a custom security group is an allowlist; nothing on the target re-creates it unless the platform side does (VPC firewall rules, Cloud Armor); record the ids |
| `alb.ingress.kubernetes.io/security-group-prefix-lists` | open question | an allowlist, as `security-groups` (the controller ignores it when `security-groups` is set, so one question for both); record the prefix lists |
| `alb.ingress.kubernetes.io/inbound-cidrs` | open question | a source-IP allowlist is a protection nobody carries unless someone creates a Cloud Armor policy (the platform side's) and attaches it with a `GCPBackendPolicy` that must live beside the Service in the workload namespace; no unit owns that object today; record the CIDRs; a value equal to the controller default `0.0.0.0/0, ::/0` is not an allowlist and needs no question |
| `alb.ingress.kubernetes.io/wafv2-acl-arn` | open question | the WAF rules must be re-created as a Cloud Armor policy (a regional policy for a regional Gateway) and attached with a `GCPBackendPolicy.spec.default.securityPolicy` that must live beside the Service in the workload namespace; no unit owns that object today; losing them silently is a security regression; record the ARN and the gap |
| `alb.ingress.kubernetes.io/wafv2-acl-name` | open question | the WAFv2 web ACL named instead of by ARN; as `wafv2-acl-arn`; record the name |
| `alb.ingress.kubernetes.io/waf-acl-id` | open question | a classic regional WAF ACL, as `wafv2-acl-arn`; record the id |
| `alb.ingress.kubernetes.io/shield-advanced-protection` | open question | DDoS protection: the source had AWS Shield Advanced, and nothing on the target carries it unless the platform side enables Cloud Armor Advanced network DDoS protection or an edge security policy; a lost protection, raise it |
| `alb.ingress.kubernetes.io/jwt-validation` | open question | JWT validation at the load balancer has no IAP equivalent (IAP does not validate third-party JWTs); the GKE options are a `GCPAuthzPolicy` on single-cluster managed classes, or in-app validation; any attaching object must live beside the Service in the workload namespace; no unit owns that object today; record the issuer and audiences |
| `alb.ingress.kubernetes.io/auth-*` | open question | ALB-native Cognito/OIDC authentication is Identity-Aware Proxy on GCP: a different identity model, attached with `GCPBackendPolicy.spec.default.iap` that must live beside the Service in the workload namespace; no unit owns that object today (the OAuth client is the platform side's); real carry-over work, record the provider and scopes |
| `external-dns.alpha.kubernetes.io/hostname` | mapped | keep verbatim on the HTTPRoute; the external-dns `gateway-httproute` source reads `spec.hostnames` and this annotation on the Route, but only under the annotation prefix external-dns is configured for (from v0.22.0 the default is `external-dns.kubernetes.io/` with no fallback; older versions or `--annotation-prefix=external-dns.alpha.kubernetes.io/` keep this key). Record as tradeoffs: the prefix; that records appear only after the Gateway accepts the route; that a listener hostname on the Gateway filters the names; and whether external-dns runs on the target with the Cloud DNS provider (the platform side's). Prefer duplicating the host into `spec.hostnames` so the record does not depend on the annotation |
| `external-dns.alpha.kubernetes.io/ttl` | mapped | keep verbatim on the HTTPRoute, as `hostname`; honoured on Routes by the `gateway-httproute` source under the configured prefix |
| `external-dns.alpha.kubernetes.io/target` | dropped with tradeoff | the record target is the Gateway's address, not the ALB's; record the source target |
| `external-dns.alpha.kubernetes.io/alias` | dropped with tradeoff | Route 53 alias records have no Cloud DNS meaning |
| `external-dns.alpha.kubernetes.io/set-identifier` | open question | a Route 53 routing-policy record set; a Route 53 routing policy (weighted, failover, geolocation, latency, health-checked) is a behaviour nobody carries on the target: external-dns has no Cloud DNS routing-policy support, so the record becomes a plain one; raise it with the source values |
| `external-dns.alpha.kubernetes.io/aws-*` | open question | a Route 53 routing policy (weighted, failover, geolocation, latency, health-checked) is a behaviour nobody carries on the target: external-dns has no Cloud DNS routing-policy support, so the record becomes a plain one; raise it with the source values |
| `external-dns.kubernetes.io/hostname` | mapped | the v0.22.0+ default-prefix spelling of `external-dns.alpha.kubernetes.io/hostname`; same treatment |
| `external-dns.kubernetes.io/ttl` | mapped | the v0.22.0+ spelling of `external-dns.alpha.kubernetes.io/ttl`; same treatment |
| `external-dns.kubernetes.io/target` | dropped with tradeoff | the v0.22.0+ spelling of `external-dns.alpha.kubernetes.io/target`; same treatment |
| `external-dns.kubernetes.io/alias` | dropped with tradeoff | the v0.22.0+ spelling of `external-dns.alpha.kubernetes.io/alias`; same treatment |
| `external-dns.kubernetes.io/set-identifier` | open question | the v0.22.0+ spelling of `external-dns.alpha.kubernetes.io/set-identifier`; same treatment |
| `external-dns.kubernetes.io/aws-*` | open question | the v0.22.0+ spelling of `external-dns.alpha.kubernetes.io/aws-*`; same treatment |
| `cert-manager.io/*` | dropped with tradeoff | the ingress-shim has no HTTPRoute equivalent; TLS terminates at the platform Gateway, whose certificate is the platform side's (cert-manager can annotate the Gateway instead); record the issuer name; do not promise HTTPS |
| `acme.cert-manager.io/*` | dropped with tradeoff | ingress-shim ACME solver knobs (`http01-edit-in-place`, `http01-ingress-class`); the same concern as `cert-manager.io/*`; record them |
| `kubernetes.io/tls-acme` | dropped with tradeoff | the legacy ingress-shim trigger; the same concern as `cert-manager.io/*`; record it |

## ServiceAccount / Identity

| EKS / AWS                                                                | GKE / GCP                                                          | Notes |
|--------------------------------------------------------------------------|--------------------------------------------------------------------|-------|
| `eks.amazonaws.com/role-arn: arn:aws:iam::…:role/foo`                    | `iam.gke.io/gcp-service-account: foo@PROJECT.iam.gserviceaccount.com` | Identity translation |
| `eks.amazonaws.com/sts-regional-endpoints: "true"`                       | drop                                                                |  |
| `eks.amazonaws.com/audience: sts.amazonaws.com`                          | drop                                                                |  |
| `eks.amazonaws.com/token-expiration: "86400"`                            | drop                                                                |  |
| `eks.amazonaws.com/skip-containers: "init-container"`                    | drop                                                                |  |

## Pod / scheduling

### Node selectors

| EKS                                                                       | GKE                                                                                |
|---------------------------------------------------------------------------|------------------------------------------------------------------------------------|
| `eks.amazonaws.com/nodegroup: <ng>`                                       | `cloud.google.com/gke-nodepool: <pool>`                                            |
| `karpenter.sh/capacity-type: spot`                                        | `cloud.google.com/gke-spot: "true"`                                                |
| `karpenter.sh/capacity-type: on-demand`                                   | a required `nodeAffinity` `cloud.google.com/gke-spot DoesNotExist` (the label exists on spot nodes only; omitting the constraint would let a ComputeClass with spot priorities place the pod on spot) |
| `karpenter.sh/nodepool: <name>` (also `karpenter.sh/provisioner-name`)   | `cloud.google.com/compute-class: <name>` when exports `compute_classes` lists the name (the `compute-class` unit keeps the NodePool name); under a NAP or Autopilot landing zone, the target node pool label or drop — an open question |
| `node.kubernetes.io/instance-type: m5.large`                              | translate to GCE machine type (e.g., `e2-standard-2`); use `cloud.google.com/machine-family` if family-only |
| `topology.kubernetes.io/region: us-east-1`                                | `topology.kubernetes.io/region: us-central1`                                       |
| `topology.kubernetes.io/zone: us-east-1a`                                 | `topology.kubernetes.io/zone: us-central1-a`                                       |
| `eks.amazonaws.com/compute-type: fargate`                                 | drop; ensure cluster is Autopilot if Fargate-equivalent is desired                 |

### Tolerations

| EKS taint                                                                | GKE taint                                                                          |
|--------------------------------------------------------------------------|------------------------------------------------------------------------------------|
| `karpenter.sh/disruption=NoSchedule`                                     | (no equivalent; GKE Standard uses `cloud.google.com/gke-preemptible` or `gke-spot` taints when applicable) |
| Custom Karpenter NodePool taint                                          | Apply same taint name to the target GKE node pool                                  |

### Container security

| EKS / pod spec                                                            | GKE / pod spec                                                                    | Notes |
|---------------------------------------------------------------------------|-----------------------------------------------------------------------------------|-------|
| `securityContext.runAsNonRoot: true`                                      | Same; Autopilot enforces                                                          |  |
| `securityContext.privileged: true`                                        | Same on Standard; **denied on Autopilot**                                         |  |
| `hostNetwork: true`                                                       | Same on Standard; **denied on Autopilot**                                         |  |
| `hostPath` volumes                                                        | Same on Standard; **restricted on Autopilot** (only specific paths allowed)       |  |
| `runtimeClassName: crun`                                                  | Same                                                                              |  |

## Storage / PVC

| EKS                                                                       | GKE                                                                               | Notes |
|---------------------------------------------------------------------------|-----------------------------------------------------------------------------------|-------|
| StorageClass `provisioner: ebs.csi.aws.com`                               | StorageClass `provisioner: pd.csi.storage.gke.io`                                  |  |
| StorageClass parameter `type: gp2` / `gp3`                                | StorageClass parameter `type: pd-balanced`                                        |  |
| StorageClass parameter `type: io1` / `io2`                                | StorageClass parameter `type: pd-ssd`, or `hyperdisk-extreme` where the node pools' machine series supports Hyperdisk | `pd-ssd` is the machine-series-safe default; state the choice in tradeoffs |
| StorageClass parameter `encrypted: "true" / kmsKeyId: ...`                | StorageClass parameter `disk-encryption-kms-key: projects/…/cryptoKeys/…`         |  |
| StorageClass parameter `iops: "...", throughput: "..."`                   | `provisioned-iops-on-create`, `provisioned-throughput-on-create` (Hyperdisk)      |  |
| StorageClass `provisioner: efs.csi.aws.com`                               | StorageClass `provisioner: filestore.csi.storage.gke.io`                           |  |
| StorageClass `provisioner: fsx.csi.aws.com` (Lustre)                      | StorageClass `provisioner: parallelstore.csi.storage.gke.io`                      |  |
| `volumeBindingMode: WaitForFirstConsumer`                                  | Same — required for zonal correctness                                              |  |

## Image references

| EKS                                                                       | GKE                                                                               |
|---------------------------------------------------------------------------|-----------------------------------------------------------------------------------|
| `123456789012.dkr.ecr.us-east-1.amazonaws.com/<repo>:<tag>`               | `<region>-docker.pkg.dev/<ar-project>/<repo>:<tag>`                              |
| `public.ecr.aws/<image>:<tag>`                                            | mirror to AR or use AR pull-through cache                                          |

## Logging / sidecars

| EKS                                                                       | GKE                                                                               | Notes |
|---------------------------------------------------------------------------|-----------------------------------------------------------------------------------|-------|
| Fluent Bit DaemonSet to CloudWatch                                        | drop; GKE-native logging covers it                                                 |  |
| ADOT Collector                                                            | Use the OTel Collector pointing at Cloud Trace / Cloud Monitoring                  |  |
| `eks.amazonaws.com/cloudwatch-observability` addon                        | drop; built-in                                                                     |  |

## Webhooks / cluster add-ons

| EKS                                                                       | GKE                                                                               |
|---------------------------------------------------------------------------|-----------------------------------------------------------------------------------|
| `aws-pod-identity-webhook` MutatingWebhookConfiguration                   | drop; native Workload Identity                                                    |
| `aws-load-balancer-controller` Deployment + CRDs                          | drop; native GKE Gateway controller                                               |
| `aws-ebs-csi-driver` addon                                                | drop; GKE installs PD CSI by default                                              |
| `aws-efs-csi-driver` addon                                                | replace with Filestore CSI                                                         |
| `kube-proxy` (managed addon)                                              | replaced by Dataplane V2 eBPF                                                      |
| `coredns` (managed addon)                                                 | Cloud DNS for GKE (`dns_config { cluster_dns = "CLOUD_DNS" }`, a landing-zone default). Corefile customizations: stub domains → the `kube-dns` ConfigMap (Standard) or Cloud DNS private forwarding zones (Autopilot, or mode unrecorded); pinned upstream nameservers → the ConfigMap on Standard, an open question otherwise (never a `google_dns_policy`: VPC-wide, one per network, and alternative name servers bypass every private zone); `hosts` entries → a private zone; `rewrite` / `template` → no equivalent (blocker). See `servers/phases/landingzone/knowledge/cluster-dns-translation.md`. |

## CRDs of note

| Source CRD (group)                                                                        | Action |
|-------------------------------------------------------------------------------------------|--------|
| `nodepools.karpenter.sh`, `ec2nodeclasses.karpenter.k8s.aws`                              | drop; rebuild as GKE node pools, NAP rules or one ComputeClass per NodePool in landing-zone, per the `karpenter` decision |
| `targetgroupbindings.elbv2.k8s.aws`                                                       | drop; replace with NEG-based service backends                  |
| `appmesh.k8s.aws/Mesh, VirtualNode, VirtualService`                                       | drop; re-platform on Anthos Service Mesh                       |
| `acmpca.aws.com/AWSPCAClusterIssuer` (cert-manager external)                              | replace with `clusterissuer` for ACME via Cloud DNS or Certificate Manager attestor |
| `secrets-store.csi.x-k8s.io` SecretProviderClass with AWS provider                        | swap provider to GCP Secret Manager provider                    |
