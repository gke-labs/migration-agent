# Discovery Step 2 — Confirm the Scope

**DAG state:** `STATE_DISCOVERY_SCOPING` · **Expected tool call:** `confirm_discovery_scope`

The IaC sources are indexed. Before any extraction (and LLM spend) happens, the
client decides what is actually in scope: remove files or directories that don't
belong to this migration, and add anything the scan missed.

## What to do

1. Present the manifest from the previous step to the user, grouped by directory or
   kind, and ask what should be excluded — vendored modules, test fixtures,
   unrelated environments, generated files are common candidates.
2. Apply their decisions with `update_discovery_scope`:
   - `exclude`: paths, directory prefixes (`"vendor/"`), or globs
     (`"**/test_*.yaml"`), relative to the scanned root.
   - `include`: entries to bring (back) into scope — undo an exclusion, override a
     broader exclude pattern for specific files, or add extra files/directories the
     scan did not cover (absolute paths, or relative to the root).
   The tool returns the effective scope (in-scope count, exclusions, samples) and
   can be called as many times as needed.
3. Show the user the effective scope after each change. When they are satisfied,
   call `confirm_discovery_scope()` — this records their sign-off and advances the
   DAG to the data-dependency scan.

## What happens next

`confirm_discovery_scope` advances the DAG to `STATE_DISCOVERY_DATA_SCAN`
(see [discovery step 3a](../discovery_datascan_3/instructions.md)), where
`scan_data_dependencies()` records the managed data services the workloads
depend on, the cluster DNS configuration and the source address space, reading
only the in-scope files. The user then reviews who uses each
of them ([step 3b](../discovery_datareview_3/instructions.md)), and extraction
follows that. During the later assessment review the user can still amend the
scope (`amend_discovery_scope`), which loops back through the data scan and its
review, then re-extracts just the delta.

## Rules

- Never confirm the scope on the user's behalf; this step exists to capture their
  sign-off before extraction spend.
- Do not read the configuration files into this conversation.
- If a tool returns an `ERROR`, report it to the user verbatim and stop.
