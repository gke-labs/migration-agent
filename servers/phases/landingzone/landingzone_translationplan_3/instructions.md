# Landing Zone Step 3 — Plan the Translation

**DAG state:** `STATE_LZ_TRANSLATION_PLAN` · **Expected tool call:** `plan_translation`

The landing zone is designed. Translation begins by breaking every problem the
discovery inventory surfaced into bounded, independently-translatable units —
before any code is generated.

## What to do

1. Tell the user the landing zone design is recorded — it is not validated or
   PR'd yet; that happens at the end of translation, where it ships together with
   the generated units. The phase concludes by agreeing the translation plan —
   what the translation phase will generate.
2. Call `plan_translation()`. The server deterministically decomposes the approved
   inventory into units — node pools, autoscaling strategy, workload policies for
   privileged DaemonSets, network topology, the shared entry-point Gateway,
   namespace tenancy (Namespace + ResourceQuota per discovered namespace),
   storage, workload identity, cluster DNS (the CoreDNS customizations discovery
   copied verbatim, carried onto Cloud DNS for GKE) — each
   carrying its inventory slice and the relevant landing-zone decisions
   (Autopilot vs Standard+NAP, peering strategy, ...). The base VPC and GKE
   cluster are NOT units: the landing zone phase already owns their Terraform.
3. Present the returned plan to the user: one line per unit with its kind, title,
   and any notes (e.g. node pools auto-skipped when the derived cluster mode is Autopilot),
   then the plan's `derived` values and `findings` lines (decisions that disagree, facts that
   argue against a recorded choice) — the sign-off prompt repeats the findings.
   Unit families whose inventory section holds no facts appear as `skipped`
   placeholders naming the empty section — surface those too, they are the
   plan's claim about what discovery did NOT find. A placeholder carrying a
   WARNING note means other inventory evidence contradicts the absence
   (a likely discovery gap): raise it with the user before the plan review.
4. When present, the plan's `coverage` section is the artifact coverage map
   instantiated against the inventory (v1, section granularity); a failed
   instantiation is logged and leaves the plan without it. If its checks list
   any findings — omitted rows, overlapping rows, uncited units, citations of
   unknown rows, map rows naming unknown inventory sections — present them as
   warnings for the review. Overlap and traceability inform the sign-off
   without blocking it; an OMITTED row is more than a warning in effect,
   because the validate step enforces the same condition and fails the run
   naming the row (unless its `UNENFORCED_ROWS` pin excuses that row) — so
   an omission surfaced here should be resolved (re-plan, or an explicit
   skip decision) before generation spend.

## What happens next

The DAG advances to `STATE_LZ_TRANSLATION_PLAN_REVIEW`
(see [landing zone step 4](../landingzone_planreview_4/instructions.md)), where the
client adjusts and signs off on the plan before translation spend.

## Rules

- Planning is deterministic — if a unit looks wrong, the fix is in the inventory
  (rediscover) or the landing-zone decisions, not in hand-editing the plan.
- If the tool returns an `ERROR`, report it to the user verbatim and stop.
