# Deployment Step 1 — Provision the Image Destination, Replicate Images

**DAG state:** `STATE_DEPLOYMENT_INIT` · **Expected tool call:** `prepare_image_deployment`

The translation Pull Request is open in the target repository. This phase prepares what the
applied estate needs before workloads can run on it: the Artifact Registry repository the
migrated container images will land in, then the images themselves.

## What to do

1. Tell the user the migration PR is open (the link is in the ledger as `pull_request_url`)
   and that the next step prepares the container image destination.
2. Call `prepare_image_deployment`. It takes no arguments. The server resolves the
   destination registry — the one the landing-zone design declared if there is one,
   otherwise a deterministic default — then checks it exists and creates it via the
   Artifact Registry API when it does not. Nothing is asked of you or the user for this:
   creating an empty docker repository is cheap and idempotent, and the outcome is
   recorded in the ledger history.
3. The server then raises the image replication question directly with the user
   (an elicitation — you do not answer it). The choices:
   - **`self_service`** (default): the server writes a runbook of `skopeo copy` commands
     to the ledger (`platform/deployment/replication-runbook.sh`); the user runs it with
     their own credentials. The server never reads from AWS in this mode. When the user
     later reports the runbook done, record it with `mark_replication_complete` (below).
   - **`agent_mediated`**: the server runs the `skopeo` copies itself, using only
     credentials already present in the local environment. It first checks that skopeo,
     ECR read access and the destination repository are in place — it never installs
     anything and never sets up credentials. Anything missing is reported with the exact
     command **the user** runs to fix it.
   - Declining skips replication and leaves the images in ECR. It does NOT end the
     migration: the graph parks at `STATE_DEPLOYMENT_DATA_MIGRATION` either way.
4. Present the outcome: the destination URL(s) and their created/existed status, then the
   replication result — images replicated, failures, the runbook path in self-service
   mode, and the reported remainder (non-ECR images and never-rendered targets).
5. Say what happens next, and do not call the migration finished. Both replication
   outcomes drain into `STATE_DEPLOYMENT_DATA_MIGRATION`, where the deployment segment
   waits for the work this server cannot do: the data services graded `migrate` have to
   actually move, and self-service image copies have to be run and reported. Call
   `get_next_stage` and follow that step's instructions.

## When the tool parks back at this state

A replication failure (missing skopeo, missing ECR login, destination repository not yet
applied, or nothing copied) returns the graph here rather than completing. Report the
reasons verbatim — they include the fix commands. Once the user says they have fixed the
environment (logged in, installed skopeo, applied the PR), call `prepare_image_deployment`
again; re-provisioning is idempotent and the replication question is asked afresh.

## After the user runs the self-service runbook

The self-service copies happen outside the server, so nothing flips on its own: the
inventory keeps `self_service` on every entry and the exports `image_map` still presents
the images as unmoved. When the user tells you they have run the runbook (or copied
images by hand), call `mark_replication_complete` — it records their assertion
(`verified_by: user_asserted`) into the inventory and refreshes the exports so
downstream consumers see the images as replicated.

- No arguments marks every remaining self-service (or failed) entry — the common case.
- `refs=[...]` marks a subset, for a partially completed runbook.
- `digests={<source ref>: <digest>}` records the destination content digest verbatim.
  Offer the user the verification command first —
  `skopeo inspect --format '{{.Digest}}' docker://<destination ref>` — and pass on
  exactly what they report; never compute, complete, or guess a digest yourself.
- `destinations={<source ref>: <dest ref>}` is needed only when the runbook was written
  with an `<AR_DESTINATION>` placeholder; pass the references the user actually pushed to.

Do not call it preemptively: it records a user assertion, so it requires the user's
explicit statement that the copies are done. Relay any `Not marked —` lines verbatim.

Calling it a second time with a digest the user has since verified is expected, not a
mistake: the entry is already `replicated`, and the call upgrades that record and
republishes. The response says `Already replicated, record improved with ...` when it
did, and `Already replicated (left as recorded)` when the call added nothing.

The response may also carry a `NOTE:` that only the exports `image_map` was republished.
That is the correct outcome when this machine is not the one that provisioned the landing
zone: the fields derived from the target clone keep their last published values. Relay
the note; the remedy, if the user wants those fields recomputed, is `refresh_exports` run
where the clone lives.

## Rules

- Never run `gcloud`, `aws`, `skopeo`, or any cloud CLI yourself — the server performs
  the copies, and credential setup belongs to the user alone. Relay fix commands to the
  user; do not execute them.
- Do not answer the replication elicitation on the user's behalf, and do not pick a mode
  for them.
- If the tool returns an `ERROR`, report it to the user verbatim and stop.
