# Landing Zone Step 1 — Open the Workspace, Resolve the Decisions, Generate the Design

**DAG state:** `STATE_LZ_DESIGN` · **Expected tool call:** `finalize_landing_zone_design`

Discovery is approved and every assessment blocker has an owner and a target close date, so the target
design is now unlocked. This is the entry step of the landing zone phase: settle the four target-shape
decisions, get a clone of the target repository ready to write into, write the Terraform and the design
documents, then hand them to the plan and approval gates.

The `gke-landing-zone` phase knowledge document delivered with this state is the design authority — the four
decisions, the standing defaults, the five modules, the hardening controls and the validation list. Read it
before you write any HCL. Do not substitute a different set of defaults.

## What to do

1. **Resolve the four decisions.** For each one, present the trigger the discovery inventory recorded, the
   recommended choice and what it costs to deviate, then call `resolve_lz_decision` with the user's answer.
   The decision ids, the choice tokens (the first listed is the default) and the "take it when" for each are
   the table in the `gke-landing-zone` knowledge document, "The four target-shape decisions"; the server
   reads that same table, so the tokens are exact and it rejects anything else. Resolve all four even when a
   trigger is `false` in the inventory — recording the default is how the design says the case was
   considered. `finalize_landing_zone_design` refuses to complete while any of the four is unrecorded, naming
   the ones still missing, and it refuses a pair of choices that imply different cluster modes (a
   `karpenter` choice for Autopilot beside a `privileged_daemonsets` choice for Standard), naming the ids to
   re-resolve. When the inventory carries no `triggers`, a defaulted decision must agree in mode with the
   ones you chose. Where the recorded facts argue against a choice, the translation plan shows that beside
   the choice at the plan review; the answer stays the user's.

2. **Prepare the clone.** The first `resolve_lz_decision` call allocates the workspace variables — read them
   back: `lz_branch_uuid`, `lz_branch_name` (`migration/gke-landing-zone-<uuid>`) and `target_clone_path`
   (`servers/dag/scratch/target-repo-<uuid>`). They persist for the rest of the phase. The same reply
   carries the network input: `source_address_space` (the ranges discovery recorded for the source VPC,
   subnets, clusters and routes) and `proposed_target_ranges` (nodes, pods and services blocks computed
   clear of them). Those proposed ranges are the defaults for the network module and plan.md's network
   table; put the source ranges beside them. If the reply lists `unresolved_source_ranges`,
   ask the user for each entry it lists there (address, file, argument and expression) before
   generating the design; the reply has already set aside the entries that need no answer of their
   own (a subnet inside a stated VPC, a subnet of a VPC whose own range is among the questions, a
   public-endpoint allow list) and counts them as `unresolved_but_covered`, so do not re-derive that
   split from `address_space.unresolved`; `no_free_block_for` means the user has to
   choose that range; `vpcs_without_a_stated_range` names a VPC whose CIDR the files do not state and
   that is not already among the questions, and
   `clusters_without_a_recorded_vpc` a cluster whose VPC has no entry at all; ask for those too.
   `moved_off_the_baseline` names the ranges that left the knowledge document's baseline because it
   overlapped a source range, and `clusters_with_a_defaulted_service_range` a cluster that set no
   service range and has no question for it, so EKS chose one at creation (usually 10.100.0.0/16 or
   172.20.0.0/16; a cluster with remote networks may have been given another block): not avoided,
   so ask the user for the live value (`aws eks describe-cluster`) and keep the proposal clear of
   it. If it says no source address space was recorded, or no
   range was stated, the proposal is the unchecked baseline: ask the user for the source ranges first.
   Then, unless the clone is already there from an earlier pass:
   - Clone `target_repo_url` at `target_branch` into `target_clone_path`.
   - Create and check out `lz_branch_name`.
   - If the directory already exists, verify it is on `lz_branch_name` rather than re-cloning over it.

   Nothing else creates this directory. The validation gate later in the phase fails with *"Target clone
   directory does not exist"* if it is missing, and the design work is lost. Write only inside
   `target_clone_path`; the source checkout is read-only from here on.

3. **Generate the design** into `target_clone_path`: the five Terraform modules, a root module that
   instantiates them, `03-landing-zone/plan.md` and `03-landing-zone/design-decisions.md`. The knowledge
   document specifies the contents of each.

   Write the root module even if the target repository has none. `terraform validate` runs at the root of
   the clone, so modules nothing references are never compiled and the gate passes without checking them.

4. **Call `finalize_landing_zone_design`.** It takes no arguments.

**Do not ask the user to approve the design first.** The approval question is raised later as a server-side
elicitation the user answers directly. Asking beforehand makes them answer twice, and your version is not the
one the state machine reads.

## Coming back here after a failure

Two paths return to this state, both with everything you already recorded intact — `lz_decisions`, the
branch variables and the files in the clone all survive:

- **The user rejected the plan at the plan-review gate.** Ask what they want changed before you re-enter the
  loop; re-submitting the same design will be reviewed the same way. Re-running `resolve_lz_decision` does not
  re-allocate the branch or the clone path, so the re-entry is cheap.
- **Terraform validation failed at the end of the phase.** That gate returns to the unit review, not here —
  the HCL fix happens against the generated units in the clone.

## What happens next

`finalize_landing_zone_design` records the design and moves straight to the translation plan — nothing is
validated or pushed here. Terraform validation (with an auto-fix pass) and the Pull Request happen at the end
of the phase, over the landing-zone draft plus the generated units together. Read the state the call reports
back:

- **Design recorded** → `STATE_LZ_TRANSLATION_PLAN`: the phase continues by decomposing the inventory into
  translation units and getting the plan approved. Call `get_next_stage` to continue.

## Rules

- Never apply Terraform, and never touch a live GCP project or cluster. This phase ends at a Pull Request; a
  human applies it. The gate is `terraform init -backend=false && terraform validate` — offline and
  credential-free by design.
- Never resolve a decision the user has not made. The recorded answer is what the design document will cite
  as their choice.
- Never invent an IP range. The network ranges come from `proposed_target_ranges` or from the user; a range
  you chose yourself is the one that overlaps the source VPC and breaks routing during the migration.
- Every deviation from the standing defaults goes in `design-decisions.md` with its rationale. An
  undocumented deviation is the one the reviewer cannot approve.
- If the tool returns an `ERROR`, report it to the user verbatim and stop.
