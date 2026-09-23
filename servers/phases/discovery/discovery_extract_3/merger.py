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

"""Deterministic merge of extraction fragments into one inventory.

Pure code, no LLM: named collections (clusters, nodegroups,
privileged_daemonsets) merge by name, trigger booleans OR together, string
lists union with order preserved, and scalar conflicts are recorded in
merge_notes rather than silently overwritten.

The merge also repairs what workers filed elsewhere: triggers are re-derived
from structured evidence, the storage summary is derived from CSI-driver
addon evidence, cluster-scoped IRSA bindings are promoted into the
top-level workloads section downstream consumes, and every typed Karpenter
NodePool (autoscaling.karpenter_nodepools, merged by name) gets its
capacity_types / instance_families / architectures lists derived from its
verbatim requirements.
"""

import re

# Matches "StorageClass <name>" in addon evidence strings — workers phrase
# the name bare or quoted: "StorageClass gp3-encrypted with provisioner ..."
# and "StorageClass 'gp3-encrypted' uses provisioner ..." both occur. The
# quote (if any) is captured so the harvester can tell names from prose.
_STORAGE_CLASS_RE = re.compile(r"StorageClass\s+(['\"`]?)([A-Za-z0-9][A-Za-z0-9._-]*)\1")


def _plausible_class_names(evidence: str) -> list:
    """StorageClass names in evidence text, filtered against prose captures.

    Evidence is worker free-text, so "no StorageClass found in this chunk"
    would otherwise harvest "found". A quoted token is always a name; a bare
    token must look like a resource name (contain a digit, dot, dash or
    underscore — gp2, gp3-encrypted, premium-rwo). A bare all-alpha name like
    "standard" is deliberately missed: a junk name poisons the plan, a missed
    name still surfaces through the CSI flags.
    """
    return [
        name for quote, name in _STORAGE_CLASS_RE.findall(evidence)
        if quote or re.search(r"[0-9._-]", name)
    ]


def merge_fragments(fragments: list) -> dict:
    """Merges inventory fragments (in deterministic input order) into one dict."""
    inventory = {
        "clusters": [],
        "addons": [],
        "nodegroups": [],
        "autoscaling": {"karpenter": False, "cluster_autoscaler": False, "evidence": [],
                        "karpenter_nodepools": []},
        "workloads": {
            "privileged_daemonsets": [],
            "host_network": [],
            "host_path_volumes": [],
            "gpu_tpu_workloads": [],
            "irsa_bindings": [],
        },
        "network": {"vpc_peering": False, "private_only_endpoints": False, "load_balancers": [], "ingress_hosts": [], "evidence": []},
        "storage": {"ebs_csi": False, "efs_csi": False, "storage_classes": []},
        "triggers": {
            "karpenter": False,
            "privileged_daemonsets": False,
            "gpu_tpu": False,
            "vpc_peering": False,
        },
        "findings": [],
        "sources": [],
        "merge_notes": [],
    }

    for fragment in fragments:
        if not isinstance(fragment, dict):
            continue
        _merge_named_list(inventory["clusters"], fragment.get("clusters"), inventory["merge_notes"], "clusters")
        _merge_named_list(inventory["addons"], fragment.get("addons"), inventory["merge_notes"], "addons")
        _merge_named_list(inventory["nodegroups"], fragment.get("nodegroups"), inventory["merge_notes"], "nodegroups")
        # The typed NodePool list merges by (kind, name, directory): two chunks
        # describing one pool fold into one entry, while two clusters that each
        # declare a `default` pool stay two entries. Popped first because
        # _merge_bool_section would union the lists by JSON key.
        autoscaling_fragment = fragment.get("autoscaling")
        if isinstance(autoscaling_fragment, dict):
            autoscaling_fragment = dict(autoscaling_fragment)
            _merge_nodepools(inventory["autoscaling"]["karpenter_nodepools"],
                             autoscaling_fragment.pop("karpenter_nodepools", None),
                             inventory["merge_notes"])
        _merge_bool_section(inventory["autoscaling"], autoscaling_fragment)
        _merge_workloads(inventory["workloads"], fragment.get("workloads"), inventory["merge_notes"])
        _merge_bool_section(inventory["network"], fragment.get("network"))
        storage_fragment = fragment.get("storage")
        if isinstance(storage_fragment, list) and storage_fragment:
            # The schema's resource-level list arm is valid in a fragment but
            # the merge only folds the dict summary; say so instead of
            # dropping the facts silently (pre-existing limitation).
            inventory["merge_notes"].append(
                "storage: a fragment supplied the resource-level list form, which the "
                "merge does not fold in — its entries were dropped"
            )
        _merge_bool_section(inventory["storage"], storage_fragment)
        for key, value in (fragment.get("triggers") or {}).items():
            if key in inventory["triggers"] and value is True:
                inventory["triggers"][key] = True
        _union(inventory["findings"], fragment.get("findings"))
        _union(inventory["sources"], fragment.get("sources"))

    # Derive triggers from structured evidence even when a worker forgot to set them.
    _derive_nodepool_summaries(inventory)
    if inventory["autoscaling"]["karpenter_nodepools"]:
        # A typed NodePool is Karpenter present (source presence, not a
        # judgement): the flag and the trigger follow.
        inventory["autoscaling"]["karpenter"] = True
    if inventory["autoscaling"]["karpenter"]:
        inventory["triggers"]["karpenter"] = True
    if inventory["workloads"]["privileged_daemonsets"]:
        inventory["triggers"]["privileged_daemonsets"] = True
    if inventory["workloads"]["gpu_tpu_workloads"] or any(n.get("gpu") for n in inventory["nodegroups"]):
        inventory["triggers"]["gpu_tpu"] = True
    if inventory["network"]["vpc_peering"]:
        inventory["triggers"]["vpc_peering"] = True

    # Same contract for facts workers reliably file in one place but not the
    # other: repair from the evidence already in the inventory. Derivation
    # only ever adds — worker-provided facts win.
    _derive_storage(inventory)
    _promote_cluster_irsa(inventory)

    return inventory


# The three requirement keys the planner and the karpenter decision's
# predicate read as derived lists. Only an `In` requirement reduces to a
# list of values; any other operator is recorded under
# `requirements_unreduced` so an empty list is never read as "no constraint".
_NODEPOOL_SUMMARY_KEYS = (
    ("capacity_types", "karpenter.sh/capacity-type"),
    ("instance_families", "karpenter.k8s.aws/instance-family"),
    ("architectures", "kubernetes.io/arch"),
)


def _nodepool_scope(entry: dict) -> str:
    """The directory a pool was declared in — the scope that tells two
    same-named pools apart. Karpenter's canonical pool name is `default` and
    NodePools are cluster-scoped, so a multi-cluster repository routinely
    carries one per cluster directory; folding them would invent a pool
    that mixes both capacity types and exists in no source object."""
    files = entry.get("source_files")
    files = [f for f in files if isinstance(f, str)] if isinstance(files, list) else []
    if not files:
        return ""
    first = sorted(files)[0]
    return first.rsplit("/", 1)[0] if "/" in first else ""


def _normalize_pool(item: dict) -> dict:
    """Recorded as written, before the merge: bare-number limits become
    strings so two chunks of one pool cannot conflict on type alone."""
    item = dict(item)
    limits = item.get("limits")
    if isinstance(limits, dict):
        item["limits"] = {str(k): (str(v) if isinstance(v, (int, float)) and not isinstance(v, bool) else v)
                          for k, v in limits.items()}
    return item


def _merge_nodepools(target: list, items, notes: list):
    """Merges typed NodePool entries by (kind, name, directory).

    Two entries with the same kind and name fold when their directories
    agree, or when one of them names no directory at all (a worker that left
    source_files empty must not split a pool the other chunk placed); they
    stay apart, with a note, only when both name a directory and the two
    differ (one `default` pool per cluster directory).
    """
    if not isinstance(items, list):
        return
    for item in items:
        if not isinstance(item, dict) or not item.get("name"):
            continue
        item = _normalize_pool(item)
        scope = _nodepool_scope(item)
        candidates = [e for e in target
                      if e.get("name") == item.get("name") and e.get("kind") == item.get("kind")]
        existing = next((e for e in candidates
                         if not scope or not _nodepool_scope(e) or _nodepool_scope(e) == scope), None)
        if existing is None:
            if candidates:
                notes.append(
                    f"autoscaling.karpenter_nodepools/{item['name']}: declared in more than "
                    "one place (" + ", ".join(sorted({_nodepool_scope(e) or "." for e in candidates}
                                                     | {scope or "."}))
                    + "); kept as separate entries")
            target.append(item)
            continue
        _merge_named_list([existing], [item], notes, "autoscaling.karpenter_nodepools")


def derive_nodepool_summaries(inventory: dict) -> dict:
    """Public entry for the manual inventory path (write_discovery_inventory):
    the derived lists are the merger's, whichever way the inventory arrived."""
    autoscaling = inventory.get("autoscaling") if isinstance(inventory, dict) else None
    if isinstance(autoscaling, dict) and isinstance(autoscaling.get("karpenter_nodepools"), list):
        _derive_nodepool_summaries(inventory)
        if autoscaling["karpenter_nodepools"]:
            autoscaling["karpenter"] = True
            triggers = inventory.setdefault("triggers", {})
            if isinstance(triggers, dict):
                triggers["karpenter"] = True
    return inventory


def _derive_nodepool_summaries(inventory: dict):
    """Fills the derived lists on every karpenter_nodepools entry from its
    verbatim requirements. Deterministic and overwriting: a worker-filled
    summary can never disagree with the requirements it summarizes. Also
    stringifies bare-number limits ({"cpu": 64} -> {"cpu": "64"}): recorded
    as written, never parsed here."""
    for entry in inventory["autoscaling"].get("karpenter_nodepools") or []:
        if not isinstance(entry, dict):
            continue
        requirements = entry.get("requirements")
        requirements = requirements if isinstance(requirements, list) else []
        unreduced = []
        for field, key in _NODEPOOL_SUMMARY_KEYS:
            values = []
            for requirement in requirements:
                if not isinstance(requirement, dict) or requirement.get("key") != key:
                    continue
                operator = requirement.get("operator") or "In"
                if operator != "In":
                    if key not in unreduced:
                        unreduced.append(key)
                    continue
                for value in requirement.get("values") or []:
                    text = str(value)
                    if text not in values:
                        values.append(text)
            # A key with any non-In requirement is unreduced as a whole: a
            # list beside a NotIn on the same key would read as a complete
            # constraint it is not.
            entry[field] = [] if key in unreduced else values
        entry["requirements_unreduced"] = unreduced
        limits = entry.get("limits")
        if isinstance(limits, dict):
            entry["limits"] = {str(k): (str(v) if isinstance(v, (int, float)) and not isinstance(v, bool) else v)
                               for k, v in limits.items()}


def _derive_storage(inventory: dict):
    """Fills the storage summary from CSI-driver addon evidence.

    The worker rules call addons out explicitly, so CSI drivers land there
    with evidence — while the storage section routinely arrives empty (the
    Acme e2e shipped StorageClass `gp3-encrypted` as addon evidence three
    runs in a row over an empty storage section, silently dropping the
    storage translation unit until the planner grew placeholders).
    """
    storage = inventory["storage"]
    if not isinstance(storage, dict):
        # Unreachable today — merge_fragments always keeps the dict skeleton
        # (list-form fragments are dropped with a merge note, see the loop) —
        # but guards a future skeleton adopting the schema's list arm.
        return
    notes = inventory["merge_notes"]
    names = []
    for addon in inventory["addons"]:
        name = str(addon.get("name", "")).lower()
        evidence = " ".join(str(e) for e in (addon.get("evidence") or []))
        ebs = ("csi" in name and "ebs" in name) or "ebs.csi.aws.com" in evidence
        efs = ("csi" in name and "efs" in name) or "efs.csi.aws.com" in evidence
        if ebs and not storage.get("ebs_csi"):
            notes.append(f"storage.ebs_csi: derived from addon '{addon.get('name')}'")
        if efs and not storage.get("efs_csi"):
            notes.append(f"storage.efs_csi: derived from addon '{addon.get('name')}'")
        storage["ebs_csi"] = storage.get("ebs_csi") or ebs
        storage["efs_csi"] = storage.get("efs_csi") or efs
        if ebs or efs:
            # Only storage-driver addons get their evidence mined for names:
            # any addon's prose may contain the word StorageClass, and a junk
            # name would flip the planner's skipped-with-WARNING net into a
            # planned unit with garbage inputs.
            names.extend(_plausible_class_names(evidence))
    classes = storage.setdefault("storage_classes", [])
    recovered = [n for n in names if n not in classes]
    _union(classes, names)
    if recovered:
        notes.append(f"storage.storage_classes: recovered {sorted(set(recovered))} from addon evidence")


def _promote_cluster_irsa(inventory: dict):
    """Promotes cluster-scoped IRSA bindings into top-level workloads.

    Workers sometimes file IRSA facts only under their cluster entry (as
    {namespace, sa, role_arn} objects, per the cluster schema) and leave
    top-level workloads.irsa_bindings — the list the workload-identity
    translation unit and the readiness report consume — empty. Promote them
    as the "namespace/sa" strings the top-level schema requires; the role
    detail stays on the cluster entry.
    """
    top = inventory["workloads"]["irsa_bindings"]
    for cluster in inventory["clusters"]:
        promoted = []
        for binding in ((cluster or {}).get("workloads") or {}).get("irsa_bindings") or []:
            if isinstance(binding, dict) and binding.get("namespace") and binding.get("sa"):
                promoted.append(f"{binding['namespace']}/{binding['sa']}")
            elif isinstance(binding, str):
                promoted.append(binding)
        new = [b for b in promoted if b not in top]
        _union(top, promoted)
        if new:
            inventory["merge_notes"].append(
                f"workloads.irsa_bindings: promoted {new} from cluster '{cluster.get('name')}'"
            )


def _union(target: list, items):
    if not items:
        return
    seen = {_key(v) for v in target}
    for item in items:
        key = _key(item)
        if key not in seen:
            target.append(item)
            seen.add(key)


def _key(value):
    import json

    return json.dumps(value, sort_keys=True, default=str)


def _merge_named_list(target: list, items, notes: list, section: str):
    if not items:
        return
    by_name = {entry.get("name"): entry for entry in target}
    for item in items:
        if not isinstance(item, dict) or not item.get("name"):
            continue
        existing = by_name.get(item["name"])
        if existing is None:
            target.append(dict(item))
            by_name[item["name"]] = target[-1]
            continue
        for field, value in item.items():
            if value in (None, [], {}):
                continue
            current = existing.get(field)
            if isinstance(current, list) and isinstance(value, list):
                _union(current, value)
            elif current in (None, [], {}):
                existing[field] = value
            elif current != value:
                notes.append(
                    f"{section}/{item['name']}.{field}: conflicting values {current!r} vs {value!r}; kept {current!r}"
                )


def _merge_bool_section(target: dict, section):
    if not isinstance(section, dict):
        return
    for field, value in section.items():
        current = target.get(field)
        if isinstance(current, bool):
            target[field] = current or bool(value)
        elif isinstance(current, list) and isinstance(value, list):
            _union(current, value)
        elif field not in target and value not in (None, [], {}):
            target[field] = value


def _merge_workloads(target: dict, workloads, notes: list):
    if not isinstance(workloads, dict):
        return
    _merge_named_list(target["privileged_daemonsets"], workloads.get("privileged_daemonsets"), notes, "privileged_daemonsets")
    for field in ("host_network", "host_path_volumes", "gpu_tpu_workloads", "irsa_bindings"):
        _union(target[field], workloads.get(field))
