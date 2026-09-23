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

"""Discovery step 0 — live discovery from AWS (STATE_DISCOVERY_LIVE).

Runs before the static IaC index (discovery_init_1). It walks the live EKS
estate — the AWS cloud plane (boto3) and each cluster's control plane (the
kubernetes client) in one call — projects every object to its key names
and structural fields (no free-form value a customer authored reaches
the ledger), and emits a Live Intermediate Representation plus CSV tables to
the ledger. This is the operational reality a later reconciliation step
(**[PLANNED]**) diffs the static Terraform/GitOps sources against.

Layout:
  live_discovery.py  orchestrator: one call → full Live IR + CSV tables
  aws_live.py        AWS cloud-plane walk (injected client factory)
  k8s_live.py        in-cluster walk (injected get_json)
  eks_auth.py        the only boto3 / kubernetes imports, lazily loaded
  projection.py      keys-only projection: structure kept, values omitted (pure)
  live_csv.py        Live IR → CSV tables (pure)
  live_schema.py     the Live IR contract: validation against
                     servers/dag/server/schema/live_discovery.json (pure)
  tools.py           the MCP tool (discover_and_dump_all_clusters),
                     registered by servers/phases/discovery/__init__.py
"""
