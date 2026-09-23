# Discovery Step 1 — Index Local IaC Sources

**DAG state:** `STATE_DISCOVERY` · **Expected tool call:** `discover_configuration_files`

You are at the collect step of discovery. Your goal is to build a manifest of the
source EKS estate's IaC files — not to analyze them yet. Analysis happens in the
next step with small-context extraction workers, so file contents must not be
loaded into this conversation.

## What to do

1. Call `discover_configuration_files` with **no arguments**. The server clones the
   source repository that was configured during repository onboarding (at its
   configured branch and `source_path`) and indexes that clone itself. You do not
   need — and should not guess — any local path.
2. Only pass `root_dir` if the user explicitly asks you to index a local checkout
   they already have, and then only as the **absolute path** of that checkout.
   Never pass `.`, your working directory, a home directory, or any broad path —
   the server refuses them (its working directory is not yours).
3. The tool returns a manifest: file paths, sizes, and kinds (terraform / helm /
   k8s-manifest), plus anything skipped — no file contents. Present the summary to
   the user: how many files, of what kinds, which source was indexed, and anything
   skipped. If the scope looks wrong (wrong repository or path, suspiciously few
   files), fix the repository configuration with the user before continuing —
   extraction costs LLM calls.
4. Be aware of what the tool does beyond the manifest: it builds the container
   image inventory in the ledger (`platform/discovery/inventory.json`) — literal
   image references plus any Helm charts / Kustomize roots that need rendering.
   If render targets are found, the server asks the user directly (via an
   elicitation prompt, not through you) whether to render them locally; rendering
   requires the `helm` and `kubectl` binaries and can take a few minutes on large
   estates. A decline or a failed render never blocks the migration — unrendered
   targets are recorded in the inventory with reasons. Include the tool's
   `--- Image inventory ---` summary (image counts and any unrendered targets)
   when you present the manifest.

## What happens next

A successful call drives the DAG through the image scan (and optional render
approval) to `STATE_DISCOVERY_SCOPING`
(see [discovery step 2](../discovery_scope_2/instructions.md)), where the client
removes out-of-scope files (or adds missed ones) and signs off before extraction
spends any LLM calls.

Later, when the inventory reaches the approval gate, declining there returns the
DAG here (`STATE_DISCOVERY`) for a fresh scan.

## Rules

- Only scan the configured source directory; never scan unrelated paths.
- Do not read the configuration files into this conversation — the extraction
  step handles contents.
- If the tool returns an `ERROR`, report it to the user verbatim and stop — do not
  attempt to work around the state machine.
