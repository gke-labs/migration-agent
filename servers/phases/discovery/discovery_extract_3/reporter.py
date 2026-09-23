# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Readiness report generation: one larger-model pass over the merged inventory.

The prompt is built by pure code; the single LLM call is confined to
extractor._run_worker (run through the shared timeout/retry wrapper) so it
can be substituted.
"""

import json
import logging

from servers.phases import agent_workers

from ..discovery_init_1.addressspace import triage_unresolved
from . import extractor

logger = logging.getLogger("migration-dag")

REPORT_RULES = """You are writing the discovery readiness report for an EKS-to-GKE
migration. You receive the merged discovery inventory (ground truth — do not
invent facts beyond it). Write a concise Markdown report with these sections:

# Discovery Readiness Report
## 1. Executive summary        (3-5 sentences, overall migration posture)
## 2. Estate overview           (clusters, nodegroups, workload highlights)
## 3. Decision triggers         (karpenter, privileged daemonsets, GPU/TPU,
                                 VPC peering — state each trigger's value and
                                 what it implies for the GKE landing zone)
## 4. Risks and blockers        (anything in findings/merge_notes that needs a
                                 human decision; say "none found" if empty)
## 4a. Cluster DNS              (what `cluster_dns` records verbatim — the
                                 CoreDNS Corefile and add-on settings: summarise
                                 the customizations it carries, name any with
                                 no Cloud DNS for GKE equivalent, relay
                                 `cluster_dns_scan_notes`; an empty section
                                 with its notes means nothing was found or
                                 read, not that there is nothing)
## 4b. Address space            (what `address_space` records — the VPC and
                                 subnet CIDRs, each cluster's service range,
                                 public-endpoint CIDRs and hybrid remote
                                 ranges, and the routed ranges: list them as
                                 recorded, say which VPC the cluster sits in,
                                 list `address_space_questions.blocking` as
                                 ranges the user must supply and
                                 `address_space_questions.covered` as needing
                                 no answer (code made that split; do not
                                 re-derive it from the entries), relay
                                 `address_space_scan_notes`; never fill a
                                 range the section does not state — the
                                 landing zone must not overlap any of these)
## 5. Coverage                  (sources examined; extraction errors, if any)

Cite inventory fields rather than speculating. If evidence is missing, say so."""


def build_report_prompt(inventory: dict, extraction_errors: dict = None) -> str:
    # The split of address_space.unresolved into questions and entries no
    # answer can change is one rule (addressspace.triage_unresolved), read
    # by the scan summary, the design echo and this report alike: handed to
    # the model as data so the report cannot re-derive it differently.
    blocking, covered = triage_unresolved(inventory.get("address_space") or {})
    payload = {
        "inventory": inventory,
        "address_space_questions": {"blocking": blocking, "covered": covered},
        "extraction_errors": extraction_errors or {},
    }
    prompt = f"{REPORT_RULES}\n\nInput data:\n{json.dumps(payload, indent=2, default=str)}"
    return prompt


async def generate_report(inventory: dict, extraction_errors: dict = None, model: str = None) -> str:
    """Generates the readiness report Markdown. Raises on LLM failure."""
    prompt = build_report_prompt(inventory, extraction_errors)
    model = model or extractor.REPORT_MODEL
    # Same hard-timeout + transient-retry wrapper the extraction workers use:
    # the report pass is the longest single generation in the pipeline, and a
    # stalled stream would otherwise hang run_discovery_extraction forever.
    report = await agent_workers.call_worker(
        extractor._run_worker, prompt, model,
        extractor.WORKER_TIMEOUT_SECONDS, "report")
    if not report.strip():
        raise ValueError("Report model returned empty output")
    return report.strip()
