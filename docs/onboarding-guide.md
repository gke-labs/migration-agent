# GKE Agentic Migration: Onboarding Guide

**Applies to:** `main` @ `c012c0f` (plugin 1.0.0). Guide updated 2026-09-22.
**Audience:** first-time users. Written to be followable with zero knowledge of the product's
internals.

The agent runs inside an agent harness. This guide supports two: **Antigravity** and
**Claude Code**. Everything except setup is the same in both. Sections 1–3 apply to everyone;
section 4 has one setup subsection per harness; sections 5–10 are common again.

---

## 1. What is this?

GKE Agentic Migration is an **EKS → GKE migration assistant you drive by chatting with your
agent harness**.

- **What you do:** open your harness, answer the agent's questions, and respond to the approval
  forms that appear along the way.
- **What the agent does:** reads your AWS-side code (the EKS IaC repo), optionally scans the live
  EKS estate read-only, catalogs what is there, generates GKE Terraform and Kubernetes manifests,
  and submits them **as pull requests**.
- **What it never does:** create or change real clusters (`terraform apply`), or modify your
  existing AWS environment. The end product is a PR; merging and applying is up to humans.

All progress is stored in a single GCS bucket, called the **ledger**. That is why you can close a
session and pick up where you left off, or hand the migration to someone else mid-flight.

## 2. The three roles

A migration is split across three roles. For a trial, one person can play all three (§4.3).

| Role | What they do | Rough time |
|---|---|---|
| **Admin** | Once, at the start: creates the workspace (ledger bucket + permissions). Stays available to add or remove members and to reset progress. | minutes |
| **Platform Engineer** | The main journey: connect repos → scan and analyze the AWS estate → review data dependencies and blockers → decide the GKE design → generate the landing-zone Terraform → PR → prepare images → track data migration | hours of session time, mostly watching and approving; the data-migration wait at the end can last days or weeks |
| **App Developer** | Picks one application (a "component") and translates its manifests for GKE → PR | under an hour |

Permissions are separated so each role can only see and touch its own part. For example, a
developer cannot read the platform's materials. "I'm a developer and I can't read this" is
usually working as intended, not a bug.

## 3. What you need

**Everyone:**

- One of the two supported harnesses: Antigravity, or the Claude Code CLI.
- gcloud login:

  ```bash
  gcloud auth application-default login
  ```

  The agent decides **who you are from the account you log in with here**. It must be the same
  email that gets registered during bootstrap.
- Python 3.11+, git.
- A checkout of the `migration-agent` repository (latest `main`). In Antigravity the setup
  prompt clones it for you (§4.1).
- A Gemini endpoint for the agent's background workers (the analysis and translation steps run
  as separate model calls inside the MCP server). Simplest: a Google Cloud project with the
  **Vertex AI API** (`aiplatform.googleapis.com`) enabled; your metadata project below is fine.
  Have its project ID and a region (for example `us-central1`) ready. §4 shows where to put them
  for each harness.

**Per role:**

- Platform Engineer: `terraform`, `helm`, and `kubectl` installed. The analysis step renders Helm
  charts and kustomize overlays on your machine, and the validation step runs
  `terraform validate`.
- Platform Engineer, optional: AWS credentials in your normal local credential chain (an AWS
  profile). Two steps use them: the read-only live scan at the start of discovery, which you can
  skip, and agent-mediated image copying from ECR, which you can decline.
- Platform Engineer, optional: `skopeo`, if you want to copy container images from ECR to
  Artifact Registry. The self-service runbook is a list of `skopeo copy` commands; agent-mediated
  copying also needs an existing ECR login (`aws ecr get-login-password | skopeo login …`), which
  the agent never creates for you.
- App Developer: `helm` and `kubectl` installed, plus your own local clone of the source repo.

**GCP side:**

- One GCP project where the admin can create a bucket. This is the "metadata project": it stores
  progress; the migrated workloads do NOT get deployed here. The ledger bucket is created in the
  GCS default location (US multi-region); there is no region option today.
- (If you want to test image replication) enable the **Artifact Registry API** in the destination
  project. If it is off, you will see one 403 error late in the journey, but overall progress is
  not blocked (§8).

**Repositories:**

- Source (AWS side): the Git repository holding the EKS estate (Terraform, charts, manifests).
  The team tests with a fixture estate of a fictional company, "Acme Cafe".
- Target (GKE side): the agent can create one for you (in Secure Source Manager), or bring any
  existing Git repo you can push to.

---

## 4. Getting started

### 4.0 The MCP server: you don't need to think about it

Behind the agent there is a local program doing the real work (saving progress, calling GCP,
generating code). **Your harness starts it for you.** The **very first launch** takes a few
minutes while it creates a Python virtual environment and installs its dependencies. Both
harnesses register it under the name `migration-dag`.

The server's background workers run on Gemini when they can reach it. Give them a Gemini
endpoint through these environment variables (the per-harness subsections show where they go):

```text
GOOGLE_GENAI_USE_VERTEXAI=True
GOOGLE_CLOUD_PROJECT=<project ID>
GOOGLE_CLOUD_LOCATION=<region>
```

A `GEMINI_API_KEY` works instead of the three. Without any of them, the workers fall back to
the Claude Agent SDK, which needs Claude credentials in the environment (an `ANTHROPIC_*` or
`CLAUDE_CODE_*` variable, or a credentials file). In practice that means Claude Code, and not on
every setup, so set the Gemini variables.

### 4.1 Setup with Antigravity

You do not install anything by hand. **Antigravity installs the plugin for itself**: you paste one
setup prompt into a fresh Antigravity conversation, the agent performs the steps (clone the plugin
repo, register it, register its MCP server), and then you restart Antigravity once.

Prerequisites: access to the GitHub repository `gke-labs/migration-agent`, plus everything in §3.

**Step 1: the setup prompt.** Open a fresh Antigravity conversation and paste this in:

````text
You are setting up the GKE Agentic Migration plugin on this machine. Work through the
steps below in order, and show me what you changed after each step.

1. Ask me two things and wait for my answers: (a) where to clone the plugin repo;
   offer the default ~/gkma/migration-agent-plugin; (b) the GCP project ID and
   region for Vertex AI workers; suggest region us-central1. Then clone
   https://github.com/gke-labs/migration-agent into that path (I already have access
   to this repo). If the path already holds a clone of this repo, run git pull in it
   instead of cloning.

2. Make sure these files exist at the clone root, creating any that are missing:

   plugin.json, exactly:
   {
     "$schema": "https://antigravity.google/schemas/v1/plugin.json",
     "name": "gke-agentic-migration",
     "description": "Agent-driven EKS to GKE migration: DAG-orchestrated MCP server plus migration skills."
   }

   mcp_config.json, as follows, with the two placeholders replaced by my step-1
   answers:
   {
     "mcpServers": {
       "migration-dag": {
         "command": "servers/dag/mcp-server",
         "env": {
           "GOOGLE_GENAI_USE_VERTEXAI": "True",
           "GOOGLE_CLOUD_PROJECT": "<project ID>",
           "GOOGLE_CLOUD_LOCATION": "<region>"
         }
       }
     }
   }

   rules/rendering.md, built from the clone's rules files, in this order: first this
   frontmatter:
   ---
   trigger: always_on
   ---
   then the full content of the clone's AGENTS.md, then the full content of the clone's
   GEMINI.md with its first line (the "@./AGENTS.md" import) removed. That file holds
   the Antigravity-specific rendering rules and must not be dropped.

3. Register the plugin: in ~/.gemini/config/plugins.json, append
   {"path": "<absolute clone path>"} to the "entries" array. Create the file with that
   single entry if it does not exist. Preserve every existing entry and do not add a
   duplicate.

4. Register the MCP server globally as well: in ~/.gemini/config/mcp_config.json, add
   under "mcpServers":
   "migration-dag": {
     "command": "<absolute clone path>/servers/dag/mcp-server",
     "env": {
       "GOOGLE_GENAI_USE_VERTEXAI": "True",
       "GOOGLE_CLOUD_PROJECT": "<project ID>",
       "GOOGLE_CLOUD_LOCATION": "<region>"
     }
   }
   Preserve every existing server entry. Back up both JSON files you edit in this step
   and step 3 (as <name>.json.bak) before changing them. Do NOT add any ANTHROPIC_* or
   CLAUDE_* variables anywhere in this setup.

5. Do NOT start or test the server yourself, and do not run any gcloud commands. Tell
   me setup is complete and that I must fully quit and reopen Antigravity.
````

**Step 2: restart and verify.**

1. Fully quit Antigravity and reopen it.
2. Check that `migration-dag` appears in the MCP panel with its tools (`get_next_stage`,
   `join_ledger`, `initialize_ledger`, …). The first launch installs Python dependencies and can
   take a few minutes; if the server shows as failed, give it a minute and re-check.
3. It should appear **once**. If you see two `migration-dag` entries (the plugin's own
   registration plus the global one), delete the `migration-dag` entry from
   `~/.gemini/config/mcp_config.json` and restart; two live copies of the server can confuse the
   bootstrap flow.

**Using it in Antigravity:**

- **Kick off in plain language.** *"Bootstrap a new migration workspace"* (admin) or *"Join the
  ledger at gs://…"* (platform engineer / developer). Approval forms appear as native dialogs in
  the chat.
- **Switching roles is just joining again** as the other role: *"Join gs://… as a developer, my
  component is orders-component."* If you want several roles running **at the same time**, open
  each in its own Antigravity workspace (a separate folder). Two conversations in one workspace
  share the same joined-role cache and overwrite each other. Every workspace shares the same
  ledger.
- **If the agent suddenly acts as if you never joined** (for example after switching Antigravity
  workspaces), nothing is lost. Join again with the same `gs://` address (and component, if you
  are a developer) and you continue where you left off.
- **Everything runs on Gemini.** The agent you chat with is Antigravity's Gemini, and the server's
  workers use Gemini on Vertex AI with the project and region you gave at setup.

**Uninstall:** remove the plugin's entry from `~/.gemini/config/plugins.json`, remove the
`migration-dag` entry from `~/.gemini/config/mcp_config.json`, delete the clone directory, and
restart Antigravity.

### 4.2 Setup with Claude Code

Clone the repository, then point Claude Code at the checkout as a plugin directory. No other
installation step: the plugin's MCP registration ships in the tree.

**One folder per role.** Create an empty folder per role and start Claude Code inside it. Some
progress data is keyed to "which folder you launched from", so running two roles in one folder
gets tangled.

```bash
git clone https://github.com/gke-labs/migration-agent ~/gkma/migration-agent-plugin
mkdir -p ~/gkma/admin ~/gkma/platform ~/gkma/dev

# Gemini endpoint for the background workers (see §4.0)
export GOOGLE_GENAI_USE_VERTEXAI=True
export GOOGLE_CLOUD_PROJECT=<project ID>
export GOOGLE_CLOUD_LOCATION=<region>

cd ~/gkma/admin        # go to the folder for your role, then
claude --permission-mode default \
       --plugin-dir ~/gkma/migration-agent-plugin \
       --allowedTools 'mcp__plugin_gke-agentic-migration_migration-dag__*'
```

The `--allowedTools` line means "don't ask me every time a migration tool is used". Drop it if you
want to inspect each tool call.

**Check that it is up.** Type `/mcp` in Claude Code. If `migration-dag` shows as **connected**,
you are good. If it shows failed, the first-time install may still be running; check again in a
minute or two.

**Using it in Claude Code:**

- **Slash commands or plain language.** `/gke-migration-bootstrap` (admin) and
  `/gke-migration-join gs://<bucket>` (platform engineer) work, and so do the plain sentences
  used in §5. For a developer say *"Join gs://… as a developer, my component is
  orders-component."*
- **Switching roles means switching folders.** Go to the folder for the other role and start
  Claude Code there.
- **Which model does the work.** The agent you chat with is Claude. The server's workers use
  Gemini when the variables above are set. When they are not, the workers fall back to the Claude
  Agent SDK, which works only if Claude credentials are visible in the environment (§4.0).

### 4.3 Playing all three roles yourself

Register your own email as admin at bootstrap and you can run the whole platform journey
directly. For the developer role, join again as a developer (Antigravity) or start Claude Code in
the dev folder (Claude Code) and say: *"Join as a developer, my component is orders-component."*

### 4.4 Two rules while using it

1. **Don't touch the bucket or GCP resources yourself with gcloud/gsutil.** The agent won't know
   about your change, and the migration state becomes unreliable from that point on. The one
   exception is the access-grant commands the agent prints for the admin (§5.3 step 2).
2. **Read the approval forms carefully and answer them.** They are the product's core safety
   mechanism: nothing lands in any repo until you approve it.

---

## 5. Walkthrough: the happy path

The prompts below are plain sentences and work in both harnesses.

### 5.1 Admin: create the workspace

1. Say: *"Bootstrap a new migration workspace."*
2. The agent asks, one at a time:
   - Workspace name, e.g. `<your-org>-migration`
   - GCP project: the metadata project from §3
   - Ledger bucket name, e.g. `gs://<your-org>-gkma-ledger-1` (a name that doesn't exist yet)
   - Emails per role: admin (required), platform engineer (required), developer (optional, but
     adding it now is easiest)
3. One approval form appears: "OK to create the bucket and permissions?" → approve.
4. The rest is automatic. The agent then shows the workspace summary and stays available: if you
   typed an email wrong or want to change who is a platform engineer or a developer, just ask
   ("add x@… as a developer", "remove y@…"). The same session can also reset the whole
   migration, update the ledger to the newest graph version, or rewind the platform team's (or
   one component's) progress to the start. Admins cannot be changed here.
5. **Give the bucket address (`gs://…`) to the platform engineer and developer.** That is their
   invitation.

### 5.2 Platform Engineer: from analysis to the landing-zone PR

1. **Join:** *"Join the ledger at gs://<bucket>."* From here on, the agent reports progress and
   artifacts step by step in the chat. A local, read-only **review page** also opens in your
   browser; it shows the same pipeline and is where the final before/after comparison is
   presented (§7 explains how to turn it off).
2. **Connect repos:** the agent asks one value at a time.
   - Source repo URL / branch / path.
   - Target repo: say "create a new one" and a repo is created in Secure Source Manager (one
     approval form + a few minutes' wait); give an existing repo URL and you move straight on.
3. **Live scan (optional):** the agent asks which AWS region(s) to scan, and for an AWS profile
   if you use one. The scan is read-only and records only structure and identifiers, never
   free-form values. If you have no live access, say so; the agent records a skip reason and
   continues from the code alone. If the scan fails on credentials, fix them and retry; the
   agent will not skip around an error on its own.
4. **Analysis (discovery):** the agent reads the source repo and shows the file list by kind. If
   the repo contains Helm charts or kustomize overlays, an approval form asks whether to **render
   them locally**. Approve; without it, container images that are only referenced inside those
   charts/overlays stay out of the image inventory. Then confirm the scope: exclude anything
   that is not part of this migration.
5. **Data dependencies review:** the agent lists the managed AWS data services it found (RDS,
   ElastiCache, S3, EFS, Secrets Manager, …), which workloads use each, and a proposed
   disposition. It also shows the cluster DNS configuration and the source IP address space,
   and asks you to fill in any range it could not resolve. Correct the mapping (attach or reject
   consumers, answer its guesses yes/no, mark a service `keep-in-aws` if it stays), then sign
   off in an approval form. This mapping later holds each developer's PR until their data has
   moved, so it is worth getting right.
6. **Extraction:** automatic. Model workers turn the files into an inventory. A few minutes.
7. **Review the results + blockers (the assessment phase):** one combined review, with an
   approval form: the inventory, a readiness report, and a list of **blockers**, each tagged with
   a category from a fixed taxonomy.
   - **What a blocker is:** something in your *current AWS environment* that cannot be carried
     over to GKE as-is: hostNetwork workloads, a custom CNI, custom Karpenter NodePools, IRSA
     roles that reach AWS-only services, and the like.
   - **The agent only finds blockers, records them, and has you assign an owner and a target
     close date to each. It does not resolve them.** The owner handles it outside the agent.
   - The condition to move on is "every blocker has an owner and a date". Naming an
     unregistered email pops a "register this person?" form; this is also a fine moment to
     register your developer.
8. **GKE design decisions (the landing-zone phase):** once every blocker is owned, you move to
   a different kind of question: **the architecture of the new GKE environment you are about to
   build**. There are always four decisions: how nodes are provisioned (the Karpenter question),
   whether privileged DaemonSets need Standard mode, GPU/TPU handling, and how the VPC connects.
   The cluster mode (Autopilot or Standard) follows from your answers. If a source IP range is
   still unresolved, the agent asks for it here. What you choose flows directly into the
   Terraform that gets generated.

   Easy to confuse with blockers, but they are different phases asking different questions:

   | | Blockers (step 7, assessment) | Design decisions (step 8, landing zone) |
   |---|---|---|
   | About | Problems in your **current AWS environment** | The shape of the **GKE environment to be built** |
   | The question | "Who will deal with it, and by when?" | "How should it be built?" (a choice) |
   | Handled by | The owner, manually, outside the agent | The agent: your answers go into the generated Terraform |

9. **Approve the translation plan:** a list of *what will be generated* (node pools, networking,
   the shared Gateway, namespaces, storage classes, workload identity, cluster DNS, …), with an
   approval form. Code generation only starts after this approval.
10. **Generation + review:** Terraform/manifests are generated per unit (several minutes). Look
    through the per-unit results the agent presents (changes, tradeoffs, open questions); for any
    unit you don't like, say "redo this one" and only that unit is regenerated.
11. **Validation + ship approval:** automatic checks run (`terraform validate` and friends), then
    the final approval form appears with a before/after comparison. Approve, and a PR branch
    (`migration/gke-landing-zone-…`) is pushed to the target repo. **This is the only moment in
    the platform journey where generated code lands in a repo.**
12. **Image preparation:** Artifact Registry creation, then a "copy the ECR images?" form.
    Copy-it-yourself is the default: the agent writes a runbook script of `skopeo copy` commands
    for you. When you have run it, tell the agent so it records the copy as complete.
    Agent-mediated copying runs `skopeo` on your machine and needs an existing ECR login.
13. **Data migration (a waiting state):** the journey now parks until every data service marked
    for migration has been reported moved. For each service the agent offers a runbook (RDS to
    Cloud SQL, and so on) that an operator runs outside the agent, over days or weeks. Come back
    and say *"the orders database has landed"* and the agent records it; the platform journey
    completes when nothing is owed: every service marked for migration reported moved (or marked
    `keep-in-aws`), and every image copy resolved: self-service copies reported done, failed
    agent-mediated copies redone by hand and reported, or abandoned with a reason. Developers
    whose components depend on a service are held at their ship gate until you report it.

> Tip: as the platform journey progresses, the information developers need (target repo address,
> file listings, Gateway details, service-account bindings, the image map) is shared
> automatically. If a developer says "I can't see the info", just say *"refresh exports"* in the
> platform session.

### 5.3 App Developer: translate your component and PR it

Prep: clone the source repo yourself, e.g. `git clone <source repo> ~/src/estate`.

1. **Join:** *"Join gs://<bucket> as a developer. My component is orders-component."*
   - The component name is a lowercase id you pick yourself. Team convention is
     `<name>-component`.
2. **Wait for the admin (this is the normal procedure):** after joining, the agent prints a few
   `gcloud` commands with the note "an admin must run these"; they grant you access to your
   component's folder. **Send them to the admin and have them run them**, then join again.
   Until then, reads and writes fail with that instruction, not a raw error.
3. **Agree the scope:** the agent shows candidate files along with a note about which signals
   (namespace, team labels, paths) actually distinguish teams in this estate. Narrow the list
   down to "these are my component's files" with filters, then confirm (approval form). If it
   advises excluding cluster-wide files like StorageClass, Namespace, or CRDs, exclude them;
   those belong to the platform. The form may carry an advisory listing AWS data services your
   files use that have not moved yet; approving over it is normal.
4. **Plan:** the agent asks for your local clone path (e.g. `~/src/estate`). Your in-scope files
   are automatically sorted into four groups (general manifests / service accounts / storage /
   routing) and the plan comes back with an approval form.
   - Seeing **"parked"** or **"placeholder"** entries in the plan is not an error. E.g. if the
     platform's shared Gateway is not ready yet, the routing group is parked automatically.
5. **Translate:** on approval, GKE-ready YAML is generated per group. Image address rewrites,
   IRSA → Workload Identity conversion, and storage-class checks are applied automatically too.
   Anything the platform side has not published yet is skipped with a recorded reason, never
   guessed.
6. **Review → validate → PR:** review the results, automatic validation runs, and after the final
   approval form a PR branch (`migration/workload-orders-component-…`) is pushed to the target
   repo, with a link for opening the PR. If your component uses a data service that the platform
   engineer has not reported as moved, validation passes but the PR is **held** until they do.
7. **If routing was parked:** once the platform's Gateway is ready, **join again with the same
   component**. The parked routing group unlocks automatically, translation re-runs, and a new PR
   comes out.

---

## 6. The moments where a human decides (approval points)

| When | What you decide |
|---|---|
| Bootstrap | OK to create the bucket and permissions? |
| Target repo | OK to create a new Secure Source Manager repo? (only if you asked for one) |
| Chart rendering | OK to render Helm charts / kustomize overlays locally for analysis? |
| Data dependencies | Is the workload → data-service mapping right, and which services stay in AWS? |
| Analysis review | Are the inventory, report, and blocker list right? |
| Blockers | Does every blocker have an owner and a target date? (checked automatically; resolving them stays with the owner, outside the agent) |
| Translation plan | What should be generated? |
| Generated code | Per unit: OK, or redo? |
| Ship | OK to open the PR? |
| Image copy | Copy the ECR images yourself, let the agent do it, or leave them in ECR? |

The point: **nothing lands in any repo until you approve it.**
The developer journey has the same pattern of approvals: scope, plan, code, ship. Four steps
are chat decisions rather than forms: the live scan, the platform's discovery scope
confirmation, the design decisions, and the per-unit "OK or redo" review of generated code. (The
developer's scope confirmation is a form.)

## 7. These are not bugs: check before filing

- **"A browser tab with a review page opened"** → that is the built-in review UI. It is
  read-only, shows the platform pipeline, and is where the ship approval's before/after
  comparison lives. To keep it from opening the browser set `GKE_AGENTIC_MIGRATION_FRONTEND_OPEN=0`
  before starting the harness; to disable it entirely set `GKE_AGENTIC_MIGRATION_FRONTEND=0`. It is
  not shown to developer sessions.
- **"I assigned an owner to a blocker but the agent doesn't fix it"** → working as intended. The
  agent's job ends at finding blockers, recording them, and assigning owners and dates.
  Resolution is done by the owner, outside the agent.
- **"The platform journey never says 'done' after the image copy"** → it is parked at data
  migration (§5.2 step 13). It completes once every service marked for migration has been
  reported moved or marked `keep-in-aws`, and every image copy is resolved. If you ran the copy
  script yourself, say so. If an agent-mediated copy failed for some images, copy them by hand
  and report it, or ask the agent to abandon them with a reason.
- **"My developer PR is held after validation passed"** → a data service your component uses
  has not been reported as moved by the platform engineer. Ask them.
- **"I got a 403 creating the Artifact Registry"** → the Artifact Registry API is disabled in the
  destination project. Enable it. This failure doesn't block the rest.
- **"I'm a developer and I can't read anything after joining"** → the admin hasn't run the
  access-grant commands yet (§5.3 step 2).
- **"My routing group stays 'parked'"** → the platform's shared Gateway isn't ready yet. Join
  again after it's ready and it unlocks (§5.3 step 7).
- **"Two people worked the same role at once and things got tangled"** → there is no lock yet
  that prevents simultaneous work. Use one person per role and one session per role.
- **"I want to switch to a different component"** → just join again with the other component
  name (in Claude Code, from the same folder).

## 8. When something goes wrong

- **`migration-dag` doesn't show up as connected** → the first-time install is still running, or
  Python is missing. Check again in a few minutes.
- **"Access Denied. User '…' is not registered"** → the account you logged into gcloud with isn't
  the email registered at bootstrap. Check which account you used for
  `gcloud auth application-default login`.
- **The extraction or translation step reports that no worker backend is usable** → the server
  has no Gemini endpoint (§4.0). Set the three `GOOGLE_*` variables where your harness reads
  them and restart it.
- **The agent just says "a tool failed" and stops** → working as intended. The agent is built not
  to try to fix internal problems itself. Capture the error message verbatim and file it.
- **"I want to start over from scratch"** → from the admin session, ask the agent to reset the
  migration. It asks for approval, wipes the migration state and keeps the bucket, the
  registered emails and their permissions, so nobody has to be re-invited. Only re-run bootstrap
  with a new bucket name if you want a different bucket or project.
- **"A platform engineer / developer email is wrong"** → from the admin session, ask the agent
  to remove the wrong address and add the right one.

## 9. Harness differences at a glance

| | Antigravity | Claude Code |
|---|---|---|
| Install | Paste the setup prompt (§4.1); restart once | `git clone` + `--plugin-dir` (§4.2) |
| Start a journey | Plain language | Slash commands or plain language |
| Several roles | Join again as the other role, or one workspace (folder) per role | One folder per role |
| Worker model | Gemini on Vertex AI (from the setup prompt) | Gemini when the `GOOGLE_*` variables are exported; otherwise the Claude Agent SDK, if Claude credentials are visible |
| Approval forms | Native dialogs in the chat | Prompts in the terminal |

The migration itself (roles, steps, approvals, what lands where) is identical. If something
misbehaves in one harness only, report which one.

## 10. What to include in a report

- Harness (Antigravity or Claude Code), workspace name and ledger bucket address
- Which role, which step, and what happened
- The approval forms you answered and the values you chose
- The **verbatim** error message from the screen
- The PR or branch link, if you got one
