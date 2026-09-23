# Contributing to GKE Agentic Migration

We'd love to accept your patches and contributions to this project. GKE Agentic Migration is intentionally small and intentionally opinionated. Please read this whole document before opening a PR — it will save us both time.

## Before you begin

### Sign our Contributor License Agreement

Contributions to this project must be accompanied by a
[Contributor License Agreement](https://cla.developers.google.com/about) (CLA).
You (or your employer) retain the copyright to your contribution; this simply
gives us permission to use and redistribute your contributions as part of the
project.

If you or your current employer have already signed the Google CLA (even if it
was for a different project), you probably don't need to do it again.

Visit <https://cla.developers.google.com/> to see your current agreements or to
sign a new one.

### Review our community guidelines

This project follows
[Google's Open Source Community Guidelines](https://opensource.google/conduct/).

## What we want

- **Bug fixes and corrections** to existing phases, knowledge documents and skills (a translation that's wrong, a command that no longer works, a recommendation that contradicts current GKE guidance).
- **Tightening** of existing step instructions and worker briefs: clearer triggers, better escalation language, more precise output schemas.
- **New reference content**: Terraform modules, service mappings, runbook templates that one of the existing phases can call into.
- **Friction logs**: write up what broke when you ran GKE Agentic Migration on a real migration. These are gold.
- **New entries in [reference/lessons-from-the-field.md](reference/lessons-from-the-field.md)** — citable practitioner war stories that the existing skills should anticipate. The bar: a verifiable URL, an attributable author, paraphrased lesson (≤15 words verbatim), an owning phase, and a justified severity rating.

## What we don't want (yet)

- **New top-level skills.** The three shipped skills (`bootstrapping`, `join`, `dag_executor`) are entry points; the migration logic lives in the phases under `servers/phases/` and their step instructions. A new skill is justified only for a new way *into* the product, not for a new migration capability — those are phase steps.
- **Speculative features.** No "future-looking" skills for GCP services that have not GA'd. We will add support after launch, not before.
- **Tooling lock-in.** Skills and rules files must work with any agent runtime that loads `SKILL.md`-format files and speaks MCP. Do not introduce dependencies on a specific orchestrator, plugin format, or model provider.

## The bar

A change ships when **two SREs at two different companies can execute it end-to-end on their own infrastructure without our help**. This is not a slogan. It is the merge criterion.

Concretely:

- A **skill** (`skills/<name>/SKILL.md`) has valid frontmatter (`name`, `description`, and a `commands` list with each argument described), then the sections the shipped skills use: `Intent`, `General Rules`, `Workflows` (one per command, numbered steps naming the exact MCP tool and arguments), and, where the skill drives the DAG, `Role-Specific Stage Instructions` keyed by state name. The `description` must trigger on the right requests and not on the wrong ones; test it with five real-sounding prompts.
- A **phase step** (`servers/phases/<phase>/<step>/`) has `instructions.md` (what the agent tells the user and which tool it calls), `tools.py` (the MCP tools, thin over pure helpers), pure helper modules with a `_test.py` beside each, and a `README.md` row in the phase's step table. Knowledge documents live in `servers/phases/<phase>/knowledge/` and are cited from the step instructions.
- Real commands where a human runs one. Not pseudocode. Not "run the appropriate `gcloud` command". The actual command, with the actual flags. The agent itself never runs cloud CLIs; it calls MCP tools.
- Every new behaviour has a unit test, and any change to a knowledge document that code parses (the assessment blocker table, the coverage map) keeps the parser's tests green.

## Style

- **Imperative, terse, factual.** "Create the cluster with these flags." Not "you might want to consider creating a cluster, perhaps with the following flags".
- **Show diffs.** When a skill rewrites a manifest, it shows the before, the after, and the rationale.
- **Cite when you assert.** If you say "GKE does X" in a customer-facing skill, link to the public doc that says so. If the public doc disagrees with the internal truth, the skill says so explicitly and flags the discrepancy.
- **No emojis** in skill files unless the user asked for them.

## Contribution process

### Code reviews

All submissions, including submissions by project members, require review. We use GitHub pull requests for this purpose. Consult [GitHub Help](https://help.github.com/articles/about-pull-requests/) for more information on using pull requests.

## How to propose a change

1. Open an issue describing what you're trying to fix or add. For non-trivial changes, *wait for ack* before writing the PR.
2. Branch from `main`, name your branch `<area>/<short-description>` (`workload-translation/helm-values-overrides`).
3. One change per PR. Mixed changes get split or rejected.
4. Run the affected flow end-to-end on a real or representative environment. Paste the trace into the PR description.

## License

By submitting a contribution, you agree that it will be released under [Apache 2.0](LICENSE).
