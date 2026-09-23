# GKE Agentic Migration

**Copyright:** 2026 Google LLC

---

## 🚀 What is GKE Agentic Migration?

GKE Agentic Migration is a **client-side, serverless, spec-driven migration framework** that
discovers an Amazon Elastic Kubernetes Service (EKS) estate from its checked-in
infrastructure-as-code and translates it into a hardened, opinionated Google Kubernetes Engine
(GKE) landing zone plus GKE-native workloads.

GKE Agentic Migration is designed to automate and de-risk the migration of containerized
workloads from EKS to GKE.

It is **not** a CLI and **not** a hosted control plane. Built on the Model Context Protocol (MCP),
the agent couples conversational LLM reasoning with deterministic, compiled policy validators and
an asynchronous state ledger. It allows platform teams and application owners to discover, design,
translate, and verify complex multi-cloud migrations with strict Human-in-the-Loop (HITL) control.
It is loaded by a standard agent harness (Antigravity and Claude Code today), which knows how to
drive a state machine.

Durable coordination happens in a **GCS state ledger**. There is no server to run, no database to
operate, and no service-account key file on disk: the agent acts with your own Application
Default Credentials.

**New here? Start with the [Onboarding Guide](docs/onboarding-guide.md).** It covers setup for
each harness, the three roles, and a step-by-step walkthrough of the migration journey.

## ⚡ Features

- **Zero-host footprint:** No centralized backend. Orchestration is agent skills + a local MCP
  server.
- **Division of labour:** The LLM does judgement work (reading IaC, choosing target shapes,
  authoring HCL and Markdown). Compiled MCP tools do everything that must be deterministic and
  auditable (state transitions, cloud mutations, git writes, validation).
- **Persona segregation:** Prefix-gated ledger boundaries, enforced by GCS managed folders and
  IAM, so each role only sees what it needs.
- **Pre-flight guarantees:** Block HCL and manifest promotion until validation passes
  (`terraform validate`, manifest structure checks, output contracts).
- **Deterministic resumability:** State lives in the GCS ledger, not in the conversation; any
  session can rejoin.
- **Nothing lands without approval:** The agent never runs `terraform apply` and never modifies
  the source EKS estate (the optional live scan is read-only). Its output is a pull request in a
  Git repository you own.

## 🎯 Scope Boundaries

To prevent unbounded scope creep, GKE Agentic Migration functions as the **Kubernetes Domain
Expert**:

| Domain | In Scope (Automated) | Out of Scope (Handed off / Runbooks) |
| :--- | :--- | :--- |
| **Kubernetes Objects** | Deployments, StatefulSets, DaemonSets, Jobs, CronJobs, Services, ConfigMaps, HPAs, PDBs, ServiceAccounts, PVCs, Ingress. | Non-containerized VMs, bare-metal workloads. |
| **Templating** | Helm charts, Kustomize overlays, plain manifests, Terraform. | Application source code refactoring (`.java`, `.cs`, `.py`). |
| **Networking & Ingress** | AWS ALB Ingress annotations → **GKE Gateway API** (`HTTPRoute` attached to a shared platform Gateway). | Cross-cloud SD-WAN / Direct Connect mesh. |
| **Identity & Access** | AWS IAM Roles for Service Accounts (IRSA) → **Workload Identity Federation for GKE**. | Enterprise IdP / Active Directory federation. |
| **Storage & DBs** | EBS CSI → **GKE Persistent Disk CSI** (`gp2`/`gp3` → `pd-balanced`, `io1`/`io2` → `pd-ssd` or Hyperdisk). In-cluster DBs migrated as generic StatefulSets. | Cloud-managed data services (RDS, ElastiCache, S3, EFS, Secrets Manager, …) → **emits a per-service manual runbook** and gates the workload until the data has moved. |

## 📦 Prerequisites

Ensure your local workstation has the following installed and available on your `$PATH`:

* **Agent Harness:** [Antigravity](https://antigravity.google/) or
  [Claude Code](https://code.claude.com/docs/en/overview)
* **Google Cloud SDK (`gcloud`):** to create Application Default Credentials
  (`gcloud auth application-default login`), which is how the agent identifies you, and for the
  admin to run the access-grant commands the agent prints when a developer joins.
* **Python:** 3.11 or later (the MCP server builds its own virtual environment on first launch)
* **Git**
* **Terraform, Helm, kubectl:** for the platform engineer role (discovery renders charts and
  overlays locally; validation runs `terraform validate`). Developers need Helm and kubectl only.
* **AWS credentials (optional):** used by the read-only live EKS scan at the start of discovery
  and by agent-mediated image copying from ECR, both through your local AWS credential chain.
  Both can be skipped; discovery then works from the checked-in IaC alone.
* **skopeo (optional):** for copying container images from ECR to Artifact Registry. The
  self-service runbook is a list of `skopeo copy` commands; agent-mediated copying also needs an
  existing ECR login made with `skopeo login`.
* **A Gemini endpoint for the agent's background workers:** a Google Cloud project with the
  Vertex AI API enabled (recommended), or a `GEMINI_API_KEY`. The onboarding guide shows how to
  configure this per harness.

## 👥 Multi-Persona Operating Model

To satisfy enterprise compliance, migrations are partitioned across three distinct roles:

```
                  ┌──────────────────────────────────────────────┐
                  │ 1. Migration Admin                           │
                  │    • Initializes GCS Ledger Bucket           │
                  │    • Registers Platform & App IAM identities │
                  └──────────────────────┬───────────────────────┘
                                         │
                 ┌───────────────────────┴───────────────────────┐
                 │                                               │
┌────────────────▼────────────────────────┐    ┌─────────────────▼────────────────────────┐
│ 2. Platform Engineer                    │    │ 3. App Developer / DevOps                │
│    • Reviews discovery and blockers     │    │    • Scopes one component's files        │
│    • Approves GKE Landing Zone design   │    │    • Translates app manifests & storage  │
│    • Ships Platform IaC Pull Request    │    │    • Ships Workload GitOps Pull Request  │
└─────────────────────────────────────────┘    └──────────────────────────────────────────┘
```

1. **Migration Admin:** Bootstraps the workspace by saying:
   ```text
   "Bootstrap a new migration workspace."
   ```
2. **Platform Engineer:** From their own working folder, says:
   ```text
   "Join the ledger at gs://[LEDGER_BUCKET]."
   ```
3. **App Developer:** From their own working folder, says:
   ```text
   "Join gs://[LEDGER_BUCKET] as a developer. My component is [COMPONENT_ID]."
   ```

Each role's step-by-step journey, and what each approval form means, is in the
[Onboarding Guide](docs/onboarding-guide.md).

## 🛠️ Troubleshooting & Common Pitfalls

### 1. Identity Resolution
The agent identifies you from Application Default Credentials, not from `gcloud auth login`.
Run `gcloud auth application-default login` with the same account that was registered at
bootstrap. On a GCE VM, Cloud Workstations, or a CI runner, the metadata-server credential is
used; if it cannot be resolved to an email, set `GKE_MIGRATION_USER_EMAIL` to the registered
address.

### 2. Ledger Bucket Location
The ledger bucket is created in the GCS default location (US multi-region). There is no
region option at bootstrap today. Choose the metadata project with that in mind; the bucket
holds migration state only, never workload data.

### 3. Missing Local Binaries During Discovery
If discovery fails with unrendered Helm charts or kustomize overlays, ensure `helm` and `kubectl`
are located in a directory present on your system's global `$PATH` (e.g., `/usr/local/bin` or
explicitly exported in your shell profile).

### 4. Cross-Role Dependencies
Application manifest translations consume what the platform journey publishes to the ledger:
the target repository, the shared Gateway, Google service account bindings for IRSA, the storage
class menu and the Artifact Registry image map. When an item is not published yet, the affected
work is **parked or skipped with a recorded reason**, never guessed: routing waits for the
Gateway, image rewrites wait for replication. Re-join the component after the platform side has
advanced and the parked work resumes.

### 5. Developer Cannot Read the Ledger After Joining
A developer's access to their component folder is granted by the admin. On join, the agent
prints the exact commands the admin must run; until then reads and writes fail with that
instruction, not with a raw error.

## 🤝 Contributing

We welcome contributions! Please see our [CONTRIBUTING.md](CONTRIBUTING.md) for details on how
to get started, set up your development environment, and submit pull requests.

## 📄 License

This project is licensed under the Apache 2.0 License.

- Modifications and additions are **Copyright 2026 Google LLC**.

See the [LICENSE](LICENSE) file for more information.
