# Karpenter NodePools on GKE ComputeClass

This document serves two readers. The landing-zone design step reads section 1 when the
recorded `karpenter` decision is `GKE_STANDARD_COMPUTECLASS`: it says what the cluster module
must and must not do for ComputeClass node auto-creation. The `compute-class` translation
unit's worker reads all of it: sections 2 to 5 are the mapping it applies to one Karpenter
NodePool and the output contract the validate step checks (`translation_validate_3/computeclass_contract.py`).

A GKE **ComputeClass** (`cloud.google.com/v1`, cluster-scoped) is an ordered list of node
shapes GKE tries top to bottom when a Pod that selects the class needs a node. With
`nodePoolAutoCreation.enabled: true` GKE creates the node pool for the shape it picks. That is
the closest equivalent of a Karpenter NodePool: requirements become priorities, the capacity
type becomes the `spot` flag per priority, and the workload selects the class by name with
`nodeSelector: {cloud.google.com/compute-class: <name>}`. Source of this mapping: the official
`google/skills` repository, `skills/cloud/gke-compute-classes`
(`references/compute-class-karpenter-migration.md`, `references/compute-class-crd-fields.md`).
Nothing from that repository's example assets is copied into a customer's output: every asset
there is labelled `EXAMPLE TEMPLATE - DO NOT DEPLOY` and carries zone placeholders.

## 1. Cluster prerequisites (the ComputeClass arm of the cluster module)

Applies only when `variables.lz_decisions.karpenter` is `GKE_STANDARD_COMPUTECLASS`.

- The cluster is **Standard**. ComputeClass on Autopilot uses a different field set and is not
  covered by this document; a `GKE_AUTOPILOT` choice skips the `compute-class` units.
- Node pool auto-creation from a ComputeClass needs **GKE 1.33.3 or later** (control plane).
  A repository cannot tell you which version the cluster will run: write the requirement into
  `design-decisions.md` as an open decision naming the release channel you chose, and do not
  pin a version literal you did not get from the user.
- Cluster-level node auto-provisioning (`cluster_autoscaling { enabled = true }`) is **not
  required** for this arm; ComputeClass creates pools per class. Leave it off unless the user
  wants it as a fallback for Pods that select no class, and say which in `design-decisions.md`.
  The `nap_enabled` value the translation plan derives (`plan.derived.nap_enabled`) is `false`
  under this choice.
- Node pools the design declares for the source node groups stay as they are: a ComputeClass
  replaces a Karpenter NodePool, not a managed node group.
- Nothing else in the cluster module changes for this arm. The ComputeClass objects themselves
  are Kubernetes YAML written by the `compute-class` translation units, never Terraform in the
  cluster module.

## 2. Mapping: one Karpenter NodePool to one ComputeClass

| Karpenter (source, `inputs.nodepool`) | ComputeClass (target) | Rule |
|---|---|---|
| `name` | `metadata.name` | Byte-identical. The workload rewrite swaps `karpenter.sh/nodepool: <name>` for `cloud.google.com/compute-class: <name>`, so the name must not change. |
| `weight` across several pools | position in `priorities[]` | Not applicable inside one class: one NodePool becomes one class. Record the source weight in `tradeoffs`. |
| `instance_families` (from `karpenter.k8s.aws/instance-family In [...]`) | `priorities[].machineFamily` | One priority per source family, mapped with the table in section 3, in the source list's order. No family constraint: use the "no constraint" row. |
| `architectures` (`kubernetes.io/arch In [...]`) | the families you may pick | Every `machineFamily` must match a listed architecture (section 3 column `arch`). |
| `capacity_types` (`karpenter.sh/capacity-type In [...]`) | `priorities[].spot` | `spot` in the list: at least one priority with `spot: true`. `on-demand` in the list: at least one with `spot` absent or `false`. Both: spot priorities first, then the on-demand floor. Spot only: an on-demand floor the source did not have is allowed only as a recorded tradeoff. `reserved`: no ComputeClass field, `open_question`. Never introduce `spot: true` when the source did not allow spot. |
| `requirements` on other keys (`instance-cpu`, `instance-memory`, `instance-category`, `instance-generation`) | `priorities[].minCores`, `minMemoryGb` | Translate a lower bound where the operator is `Gt`; everything else is an `open_question`. |
| `taints` | `nodePoolConfig.taints` | Same key, value and effect. Keys containing `kubernetes.io` are refused by GKE; route such a taint to `open_questions`. |
| `labels` (template labels) | `nodePoolConfig.nodeLabels` | Same key and value. |
| `limits` (`cpu`, `memory`) | no ComputeClass field | State the source limit and its disposition in `tradeoffs`: a `CapacityQuota` (GKE 1.36.2+) or cluster NAP `resource_limits` can cap it; neither is this unit's object. The number must appear in your files or your prose. |
| `disruption.consolidation_policy: WhenUnderutilized` / `consolidate_after` | `autoscalingPolicy.consolidationDelayMinutes` (floor 1) | `WhenEmpty` only: `consolidationThreshold` is not applicable; say so. |
| `disruption.expire_after` | no equivalent | `open_question`. |
| `disruption.budgets` | PodDisruptionBudget on the workloads | Not this unit's object; name it in `tradeoffs`. |
| drift | `activeMigration.optimizeRulePriority: true` | Serving tiers only; record the choice in `tradeoffs`. |
| `node_class_ref` (EC2NodeClass: AMI, subnets, security groups) | none | GKE owns the node image and network placement; `open_question` only if the node class carried something a workload depends on (a custom AMI, user data). |

## 3. Family table (machine-read)

Parsed at server start-up (`servers/dag/server/computeclass_families.py`); the validate
contract accepts a `machineFamily` only from the row that matches the source family. The
first candidate is the default recommendation. Source: the official reference's family list,
extended with the same-generation neighbours it implies.

| AWS family | GCP `machineFamily` candidates | arch |
|---|---|---|
| m5, m6i, m7i | n4, n2 | amd64 |
| m5a, m6a, m7a | n4d, n2d | amd64 |
| c5, c6i, c7i | c4, c2 | amd64 |
| c5a, c6a, c7a | c4d, c2d | amd64 |
| r5, r6i, r7i | n4, n2 | amd64 |
| r5a, r6a | n4d, n2d | amd64 |
| m6g, m7g, c6g, c7g, r6g, r7g, t4g | n4a, c4a | arm64 |
| t3, t3a, t2 | e2 | amd64 |
| (no constraint) | n4, n2, e2, c4, n4d | amd64 |
| (no constraint) | n4a, c4a | arm64 |

`machineFamily` takes a bare Compute Engine series (`n4`, `c4`, `n2d`); there is no `-highmem`
family value. A memory-optimized source family maps to the general series plus `minMemoryGb` on
the priority. A source family this table does not list has no candidates: the worker names it in
`open_questions` and picks nothing for it.

## 4. Rules the validate contract enforces

1. Exactly one YAML document, `apiVersion: cloud.google.com/v1`, `kind: ComputeClass`, no
   `metadata.namespace`, `metadata.name` equal to `inputs.nodepool.name`. No Terraform files.
2. `spec.nodePoolAutoCreation.enabled: true` (literally). Without it the class only orders
   existing pools and nothing replaces Karpenter's provisioning.
3. `spec.whenUnsatisfiable: DoNotScaleUp`. `ScaleUpAnyway` falls back to a family nobody chose
   (E2); a Karpenter migration accepts `Pending` and surfaces it instead.
4. `spec.priorities` is a non-empty list. Each entry names `machineFamily` or `machineType`,
   never both and never neither. When `priorityScore` is used it is set on every entry and no
   score is shared by more than three entries.
5. Spot as in section 2: `spot: true` only if the source allowed spot; an on-demand priority
   whenever the source allowed on-demand.
6. Every `machineFamily`, and the series prefix of every `machineType` (the text before the
   first `-`), is in the section 3 row for one of the source families (or the no-constraint row
   when the source names none) and matches a source architecture. A source family the table does
   not list replaces this check for that family with "the family appears in `open_questions`"
   (the known families are still checked); an unreduced `karpenter.k8s.aws/instance-family`
   requirement replaces it for the whole unit with "that key appears in `open_questions`"; an
   unreduced `kubernetes.io/arch` only lifts the architecture filter. A `nodePoolConfig` taint
   whose key contains `kubernetes.io` is a finding.
7. Forbidden: the key `bootDiskSizeGb` (the field is `bootDiskSize`), `spec.autopilot`, the
   strings `EXAMPLE TEMPLATE`, `<zone>`, `REPLACE_ME`, and quoted integers on `bootDiskSize`,
   `minCores`, `minMemoryGb`.
8. Every value under `inputs.nodepool.limits` and every source taint key appears in your files
   or in `tradeoffs` / `assumptions` / `open_questions`. Nothing is dropped silently.
9. The unit is honoured only when the derived Karpenter replacement the plan recorded for it
   is ComputeClass (`inputs.derived_decisions.karpenter_replacement.choice = computeclass`, which
   the planner sets only under Standard and the `GKE_STANDARD_COMPUTECLASS` choice); a done unit
   under any other recorded value is a finding.

## 5. Output contract for the worker

- Files: one `<name>.yaml` carrying the one ComputeClass. No other kind, no PodDisruptionBudget,
  no `google_container_cluster` or `google_container_node_pool`, no CapacityQuota.
- `tradeoffs`: the family pick per priority and why, the spot ordering, the disposition of
  `limits`, the disruption mapping, and the GKE version assumption (1.33.3+).
- `open_questions`: the GKE minimum version and release channel, the zones the class should
  prefer (never invent a region or zone), CUD or reservation alignment for the chosen families,
  anything section 2 routes there.
- `assumptions`: every default you took where the source recorded nothing (for example
  `minCores` when the source had no cpu requirement).
