# Assessment Step 1 — Review the Discovery, Agree the Blockers

**DAG state:** `STATE_ASSESSMENT` · **Expected tool call:** `submit_assessment`

Extraction is complete: the inventory and the readiness report are already in
the ledger. This is the one human review between discovery and landing zone
design — present what was found, agree the blocker list with the user, and
submit. The user's answer to the server's approval prompt (not your call) is
the decision. You never approve on their behalf.

## What to do

1. Call `get_discovery_inventory` to retrieve the persisted inventory and the
   readiness report.
2. Present the readiness report to the user (or the inventory summary if no
   report was generated). Highlight the decision triggers — Karpenter usage,
   privileged DaemonSets, GPU/TPU workloads, VPC peering — since these drive
   the landing-zone decisions in the next phase. Surface any extraction errors
   or merge_notes, and offer to list the discovered resources in full.
3. Using the Step 4 blocker table of the assessment knowledge document, agree
   with the user which findings are migration blockers. Each blocker needs an
   `id`, `title`, `category` (verbatim from the Step 4 table), `rationale`,
   and a real `resolution_path` (never "TBD"). Leave owner and close date
   out — a human assigns those in the next step.
4. If — and only if — the user spots a *scope* problem (missing files or
   directories, or content that should not have been analyzed), correct it
   with `amend_discovery_scope` and re-run extraction — do NOT call
   `submit_assessment` for a scope problem. Once the elicitation has been
   declined the DAG is already back at `STATE_DISCOVERY` and the scope must
   be re-curated from the start; amending here first is the cheap path.
5. Otherwise call `submit_assessment(blockers=[...])` (an empty list when
   nothing blocks). The server then asks the user directly to approve — that
   elicitation is the sign-off. Do not call the tool again to act on their
   answer; read the state it reports back.

## What happens next

- **Approved with blockers** → `STATE_BLOCKER_RESOLUTION`
  ([assessment step 2](../assessment_blockers_2/instructions.md)): every
  blocker must get an owner and a target close date before landing zone
  design unlocks.
- **Approved with no blockers** → `STATE_LZ_DESIGN`: landing zone design starts.
- **Declined** → `STATE_DISCOVERY`: the user wants a fresh scan.
- `amend_discovery_scope` → `STATE_DISCOVERY_DATA_SCAN` (the data scan re-runs
  ([discovery step 3a](../../discovery/discovery_datascan_3/instructions.md)), the user
  reviews the data mapping again
  ([step 3b](../../discovery/discovery_datareview_3/instructions.md)), and extraction
  follows that): the corrections recorded at an earlier review are replayed over the new
  scan, but the sign-off is not, and cached fragments mean only new or changed chunks are
  re-extracted.

## Rules

- Never approve on the user's behalf — the elicitation raised by
  `submit_assessment` is the only approval.
- Blocker categories come verbatim from the knowledge document's Step 4
  table; the server rejects anything else.
- If a tool returns an `ERROR`, report it to the user verbatim and stop.
