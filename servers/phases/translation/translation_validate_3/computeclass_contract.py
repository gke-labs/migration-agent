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

"""The compute-class unit's output contract.

The worker was handed one typed Karpenter NodePool (`inputs.nodepool`, copied
by discovery) and the mapping document
(`landingzone/knowledge/gke-compute-classes.md`), and it wrote the GKE side:
one `cloud.google.com/v1 ComputeClass` whose priorities replace the pool's
family, architecture and capacity-type requirements. This gate checks what
it wrote without interpreting Karpenter itself: the family table the
document carries is machine-read (`server/computeclass_families.py`), the
source facts are in the unit's inputs, and everything else is shape. The
seven checks are the ones the document's section 4 lists, in this order:

1. shape: one document, the right kind and apiVersion, cluster-scoped, the
   NodePool's name kept byte-identical (the workload rewrite relies on it),
   no Terraform;
2. spec: non-empty priorities each naming exactly one of machineFamily /
   machineType, nodePoolAutoCreation literally enabled, `DoNotScaleUp`,
   priorityScore all-or-none and never shared by more than three;
3. spot: the source's capacity types decide which priorities may be spot
   and whether an on-demand floor must exist;
4. family: every machineFamily is in the table's candidate set for the
   source families (or the no-constraint row) and a source architecture;
5. forbidden: the fields and placeholder strings the document bans;
6. conservation: the source limits and taint keys are not dropped silently;
7. decision: the derived Karpenter replacement recorded for this plan is
   ComputeClass — a done unit under any other value was unskipped at Gate
   C against the decision, or the plan predates the registry.

Checks 3 and 4 are replaced by an open_questions requirement when the
matching requirement key survived reduction (`requirements_unreduced`): a
constraint discovery could not type is the client's to settle, not the
worker's to guess.

Shape follows gateway_contract.check_units: compute-class units get the
full contract, every other unit gets the foreign-object sweep (a
ComputeClass shipped by another family is a finding on that unit), and a
legacy blob without a kind is swept, never a crash.
"""

from servers.dag.server import computeclass_families
from servers.dag.server import exports as exports_lib

UNIT_KIND = "compute-class"
KIND = "ComputeClass"
API_VERSION = "cloud.google.com/v1"
EXPECTED_CHOICE = "computeclass"

CAPACITY_KEY = "karpenter.sh/capacity-type"
FAMILY_KEY = "karpenter.k8s.aws/instance-family"
ARCH_KEY = "kubernetes.io/arch"

FORBIDDEN_STRINGS = ("EXAMPLE TEMPLATE", "<zone>", "REPLACE_ME")
FORBIDDEN_KEY = "bootDiskSizeGb"
INTEGER_KEYS = ("bootDiskSize", "minCores", "minMemoryGb", "priorityScore")
MAX_SHARED_SCORE = 3


def _documents(entry: dict) -> tuple:
    """([(path, doc)], [parse errors]) over the unit's YAML files."""
    docs, errors = [], []
    for path, content in sorted(exports_lib._unit_files(entry, (".yaml", ".yml")).items()):
        loaded, error = exports_lib._load_yaml_docs(content)
        if error:
            errors.append(f"{path}: {error}; this gate cannot read it")
            continue
        docs.extend((path, doc) for doc in loaded if doc is not None)
    return docs, errors


def _identity(doc) -> tuple:
    if not isinstance(doc, dict):
        return "", "", None
    meta = doc.get("metadata") if isinstance(doc.get("metadata"), dict) else {}
    return str(doc.get("kind") or ""), str(meta.get("name") or ""), meta.get("namespace")


def _walk(node, path=""):
    """Yields (dotted path, key, value) for every mapping entry in a document."""
    if isinstance(node, dict):
        for key, value in node.items():
            here = f"{path}.{key}" if path else str(key)
            yield here, key, value
            for item in _walk(value, here):
                yield item
    elif isinstance(node, list):
        for index, value in enumerate(node):
            for item in _walk(value, f"{path}[{index}]"):
                yield item


def _text_of(values) -> str:
    """One searchable string out of a str, a list, or anything else."""
    if values is None:
        return ""
    if isinstance(values, str):
        return values
    if isinstance(values, (list, tuple)):
        return "\n".join(_text_of(v) for v in values)
    return str(values)


def _nodepool(entry: dict) -> dict:
    inputs = ((entry or {}).get("unit") or {}).get("inputs") or {}
    nodepool = inputs.get("nodepool")
    return nodepool if isinstance(nodepool, dict) else {}


def _strings(values) -> list:
    return [str(v) for v in values] if isinstance(values, (list, tuple)) else []


def _open_questions_text(entry: dict) -> str:
    result = (entry or {}).get("result") or {}
    return _text_of(result.get("open_questions"))


def _shape_errors(entry: dict, nodepool: dict):
    """Check 1. Returns (errors, the one ComputeClass doc or None, its path)."""
    docs, errors = _documents(entry)
    tf_files = sorted(exports_lib._unit_files(entry, (".tf",)))
    if tf_files:
        errors.append(
            f"the unit ships Terraform ({', '.join(tf_files)}); a ComputeClass is "
            "a Kubernetes object and the compute-class unit is YAML only — the "
            "cluster and its node pools are the landing zone's")
    if not docs and not errors:
        return ([f"the unit ships no YAML document; the compute-class unit emits "
                 f"exactly one {API_VERSION} {KIND}"], None, "")
    if len(docs) > 1:
        errors.append(
            f"the unit ships {len(docs)} YAML documents ("
            + ", ".join(f"{path}: {_identity(doc)[0] or type(doc).__name__}"
                        for path, doc in docs)
            + f"); the brief orders exactly one document, the {KIND}")
    chosen, chosen_path = None, ""
    for path, doc in docs:
        kind, name, namespace = _identity(doc)
        if not isinstance(doc, dict):
            errors.append(f"{path}: a top-level {type(doc).__name__} is not a "
                          "Kubernetes object; the one document must be a "
                          f"{KIND} mapping")
            continue
        if kind != KIND:
            errors.append(f"{path}: ships a {kind or '?'} document; the only "
                          f"kind this unit may ship is {KIND}")
            continue
        if chosen is None:
            chosen, chosen_path = doc, path
        api = str(doc.get("apiVersion") or "")
        if api != API_VERSION:
            errors.append(f"{path}: {KIND} {name or '?'} sets apiVersion {api!r}; "
                          f"the GKE ComputeClass API is {API_VERSION}")
        if namespace is not None:
            errors.append(f"{path}: {KIND} {name or '?'} sets metadata.namespace "
                          f"{namespace!r}; ComputeClass is cluster-scoped and "
                          "must carry no namespace")
        expected = str(nodepool.get("name") or "")
        if name != expected:
            errors.append(f"{path}: {KIND} metadata.name is {name!r} where the source "
                          f"NodePool is {expected!r}; the name must be byte-identical "
                          "because the workload rewrite swaps karpenter.sh/nodepool "
                          "for cloud.google.com/compute-class by that name")
    return errors, chosen, chosen_path


def _spec_errors(doc: dict, path: str) -> list:
    """Check 2: the priorities list and the two literal spec fields."""
    errors = []
    spec = doc.get("spec") if isinstance(doc.get("spec"), dict) else {}
    priorities = spec.get("priorities")
    if not isinstance(priorities, list) or not priorities:
        errors.append(f"{path}: spec.priorities is missing or empty; a ComputeClass "
                      "with no priorities orders nothing and replaces no NodePool")
        priorities = []
    scored = 0
    scores = {}
    for index, entry in enumerate(priorities):
        if not isinstance(entry, dict):
            errors.append(f"{path}: spec.priorities[{index}] is not a mapping; each "
                          "priority names machineFamily or machineType")
            continue
        named = [k for k in ("machineFamily", "machineType") if k in entry]
        if len(named) != 1:
            errors.append(f"{path}: spec.priorities[{index}] names "
                          + (" and ".join(named) if named else "neither machineFamily nor machineType")
                          + "; each priority names exactly one of the two")
        if "priorityScore" in entry:
            scored += 1
            score = entry.get("priorityScore")
            # 5, 5.0 and "5" are one score for the sharing count.
            try:
                score = float(score)
            except (TypeError, ValueError):
                score = repr(score)
            scores.setdefault(score, []).append(index)
    if scored and scored != len([p for p in priorities if isinstance(p, dict)]):
        errors.append(f"{path}: priorityScore is set on {scored} of {len(priorities)} "
                      "priorities; when it is used it is set on every entry")
    for score, indexes in sorted(scores.items()):
        if len(indexes) > MAX_SHARED_SCORE:
            errors.append(f"{path}: priorityScore {score} is shared by {len(indexes)} "
                          f"priorities; no score may be shared by more than "
                          f"{MAX_SHARED_SCORE}")
    npac = spec.get("nodePoolAutoCreation")
    enabled = npac.get("enabled") if isinstance(npac, dict) else None
    if enabled is not True:
        errors.append(f"{path}: spec.nodePoolAutoCreation.enabled is {enabled!r}; it must "
                      "be the boolean true, or the class only orders existing pools "
                      "and nothing replaces Karpenter's provisioning")
    when = spec.get("whenUnsatisfiable")
    if when != "DoNotScaleUp":
        errors.append(f"{path}: spec.whenUnsatisfiable is {when!r}; it must be "
                      "DoNotScaleUp — ScaleUpAnyway falls back to a family nobody chose")
    return errors


def _priorities(doc: dict) -> list:
    spec = doc.get("spec") if isinstance(doc.get("spec"), dict) else {}
    priorities = spec.get("priorities")
    return [p for p in priorities if isinstance(p, dict)] if isinstance(priorities, list) else []


def _spot_errors(doc: dict, path: str, entry: dict, nodepool: dict) -> list:
    """Check 3: capacity types decide the spot / on-demand split."""
    if CAPACITY_KEY in _strings(nodepool.get("requirements_unreduced")):
        if CAPACITY_KEY not in _open_questions_text(entry):
            return [f"the source NodePool carries an unreduced {CAPACITY_KEY} "
                    "requirement, so the spot decision is the client's; "
                    f"open_questions must name {CAPACITY_KEY}"]
        return []
    src = _strings(nodepool.get("capacity_types"))
    priorities = _priorities(doc)
    has_spot = any(p.get("spot") is True for p in priorities)
    has_floor = any(p.get("spot") is not True for p in priorities)
    errors = []
    if ("spot" in src) != has_spot:
        if has_spot:
            errors.append(f"{path}: a priority sets spot: true while the source "
                          f"capacity_types {src} never allowed spot; never introduce "
                          "spot the source did not allow")
        else:
            errors.append(f"{path}: the source capacity_types {src} allow spot but no "
                          "priority sets spot: true; the class must keep the spot arm")
    if "on-demand" in src and priorities and not has_floor:
        errors.append(f"{path}: the source capacity_types {src} allow on-demand but "
                      "every priority is spot; at least one priority must leave "
                      "spot absent or false as the on-demand floor")
    return errors


def _family_errors(doc: dict, path: str, entry: dict, nodepool: dict) -> list:
    """Check 4: every machineFamily, and every machineType's series prefix,
    is in the table for the source families. An unreduced instance-family
    requirement or a source family the table does not list replaces the
    check with an open_questions requirement; an unreduced arch requirement
    only lifts the architecture filter."""
    unreduced = _strings(nodepool.get("requirements_unreduced"))
    questions = _open_questions_text(entry)
    families = _strings(nodepool.get("instance_families"))
    archs = [] if ARCH_KEY in unreduced else _strings(nodepool.get("architectures"))
    errors = []
    if ARCH_KEY in unreduced and ARCH_KEY not in questions:
        errors.append(f"the source NodePool carries an unreduced {ARCH_KEY} requirement, so "
                      f"the architecture is the client's; open_questions must name {ARCH_KEY}")
    if FAMILY_KEY in unreduced:
        if FAMILY_KEY not in questions:
            errors.append(f"the source NodePool carries an unreduced {FAMILY_KEY} requirement, "
                          f"so the family pick is the client's; open_questions must name {FAMILY_KEY}")
        return errors
    unknown = computeclass_families.unknown_families(families)
    for family in unknown:
        if family not in questions:
            errors.append(f"the source family {family!r} is not in the family table, so no "
                          f"machineFamily can be derived for it; open_questions must name "
                          f"{family}")
    known = [f for f in families if str(f).strip().casefold() not in unknown]
    if families and not known:
        # Every source family is unknown: nothing to check the priorities against.
        return errors
    families = known
    allowed = computeclass_families.allowed_families(families, archs)
    source = (f"source families {families}" if families else "the no-constraint row") \
        + (f" for architectures {archs}" if archs else "")
    for index, priority in enumerate(_priorities(doc)):
        if "machineFamily" in priority:
            label, family = "machineFamily", str(priority.get("machineFamily") or "")
        elif "machineType" in priority:
            label = "machineType"
            family = str(priority.get("machineType") or "").split("-", 1)[0]
        else:
            continue
        if family.strip().casefold() not in allowed:
            errors.append(f"{path}: spec.priorities[{index}].{label} {priority.get(label)!r} is "
                          f"not in the family table's candidates for {source} "
                          f"({', '.join(sorted(allowed)) or 'none'}); every machine family, "
                          "a machineType's series included, comes from the table row of a "
                          "source family")
    return errors


def _forbidden_errors(doc: dict, path: str, text: str) -> list:
    """Check 5: the fields and placeholder strings the document bans."""
    errors = []
    spec = doc.get("spec") if isinstance(doc.get("spec"), dict) else {}
    if "autopilot" in spec:
        errors.append(f"{path}: spec.autopilot is set; the class targets a Standard "
                      "cluster and the autopilot block is forbidden")
    for dotted, key, value in _walk(doc):
        if key == FORBIDDEN_KEY:
            errors.append(f"{path}: {dotted} uses the key {FORBIDDEN_KEY}; the "
                          "ComputeClass field is bootDiskSize")
        elif key in INTEGER_KEYS and isinstance(value, str):
            errors.append(f"{path}: {dotted} is the quoted string {value!r}; {key} "
                          "is an integer field and a quoted value is rejected")
    for needle in FORBIDDEN_STRINGS:
        if needle in text:
            errors.append(f"{path}: the file contains the placeholder {needle!r}; "
                          "placeholders and template markers must not ship")
    return errors


def _conservation_errors(entry: dict, nodepool: dict) -> list:
    """Check 6: limits and taint keys appear in the files or the prose."""
    result = (entry or {}).get("result") or {}
    haystack = "\n".join(
        [_text_of(list(exports_lib._unit_files(entry, (".yaml", ".yml", ".tf", ".md")).values())),
         _text_of(result.get("tradeoffs")),
         _text_of(result.get("assumptions")),
         _text_of(result.get("open_questions"))])
    errors = []
    limits = nodepool.get("limits")

    def present(needle: str) -> bool:
        # Whole-token match: the limit "1" is not satisfied by "10" or "v1".
        import re
        return re.search(r"(?<![\w.])" + re.escape(needle) + r"(?![\w.])", haystack) is not None

    for key, value in sorted((limits or {}).items()) if isinstance(limits, dict) else []:
        if not present(str(value)):
            errors.append(f"the source limit {key}={value} appears nowhere in the files, "
                          "tradeoffs, assumptions or open_questions; a limit with no "
                          "ComputeClass field is stated in tradeoffs, never dropped")
    for taint in nodepool.get("taints") or []:
        key = taint.get("key") if isinstance(taint, dict) else taint
        key = str(key or "")
        if key and key not in haystack:
            errors.append(f"the source taint key {key!r} appears nowhere in the files, "
                          "tradeoffs, assumptions or open_questions; every taint is "
                          "carried as nodePoolConfig.taints or routed to open_questions")
    return errors


def _taint_key_errors(doc: dict, path: str) -> list:
    """A nodePoolConfig taint whose key contains kubernetes.io is refused by
    GKE at apply; the mapping routes such a taint to open_questions."""
    spec = doc.get("spec") if isinstance(doc.get("spec"), dict) else {}
    config = spec.get("nodePoolConfig") if isinstance(spec.get("nodePoolConfig"), dict) else {}
    errors = []
    for index, taint in enumerate(config.get("taints") or []):
        key = str((taint or {}).get("key") or "") if isinstance(taint, dict) else ""
        if "kubernetes.io" in key:
            errors.append(f"{path}: spec.nodePoolConfig.taints[{index}] key {key!r} contains "
                          "kubernetes.io, which GKE refuses on a ComputeClass; route that taint "
                          "to open_questions instead")
    return errors


def _decision_errors(entry: dict) -> list:
    """Check 7: the derived Karpenter replacement recorded for the plan."""
    inputs = ((entry or {}).get("unit") or {}).get("inputs") or {}
    derived = inputs.get("derived_decisions")
    value = (derived or {}).get("karpenter_replacement") if isinstance(derived, dict) else None
    choice = value.get("choice") if isinstance(value, dict) else value
    if choice == EXPECTED_CHOICE:
        return []
    reason = (value.get("reason") if isinstance(value, dict) else None) or "no reason recorded"
    return [f"inputs.derived_decisions.karpenter_replacement.choice is {choice!r} "
            f"({reason}) where a done compute-class unit requires "
            f"{EXPECTED_CHOICE!r}; the unit was unskipped at Gate C against the "
            "decision, or the plan predates the registry"]


def check_unit(entry: dict) -> list:
    """Contract errors for ONE compute-class unit's outputs (empty when clean).

    Checks 2 to 5 need the one ComputeClass document; when check 1 finds
    none they are not run and the shape findings go back to review first.
    Checks 6 and 7 read only inputs and prose and always run.
    """
    nodepool = _nodepool(entry)
    errors, doc, path = _shape_errors(entry, nodepool)
    if doc is not None:
        text = exports_lib._unit_files(entry, (".yaml", ".yml")).get(path, "")
        errors.extend(_spec_errors(doc, path))
        errors.extend(_spot_errors(doc, path, entry, nodepool))
        errors.extend(_family_errors(doc, path, entry, nodepool))
        errors.extend(_forbidden_errors(doc, path, text))
        errors.extend(_taint_key_errors(doc, path))
    errors.extend(_conservation_errors(entry, nodepool))
    errors.extend(_decision_errors(entry))
    return errors


def foreign_computeclass_errors(entry: dict) -> list:
    """ComputeClass objects shipped by a unit of ANOTHER kind.

    One NodePool is one class and one unit; a ComputeClass from the storage
    or autoscaling family would be a second owner for the same object.
    Parse-broken files are the owning unit's contracts' business.
    """
    errors = []
    docs, _parse_errors = _documents(entry)
    for path, doc in docs:
        kind, name, _namespace = _identity(doc)
        if kind == KIND:
            errors.append(f"{path}: ships {KIND} {name or '?'}; a ComputeClass is the "
                          f"{UNIT_KIND} unit's alone — one NodePool, one class, one unit")
    return errors


def check_units(done_units: list) -> dict:
    """Contract check over the done unit blobs the validate step ships.

    Units of kind `compute-class` get the full contract; every other unit
    gets the foreign-object sweep. Returns {"checked": [unit_id],
    "findings": [{"unit_id", "error"}]} — `checked` names the compute-class
    units only, and a legacy blob without a kind is swept, never a crash.
    """
    report = {"checked": [], "findings": []}
    for entry in done_units or []:
        unit = (entry or {}).get("unit") or {}
        unit_id = str(unit.get("unit_id") or unit.get("kind") or "?")
        if unit.get("kind") != UNIT_KIND:
            for error in foreign_computeclass_errors(entry):
                report["findings"].append({"unit_id": unit_id, "error": error})
            continue
        report["checked"].append(unit_id)
        for error in check_unit(entry):
            report["findings"].append({"unit_id": unit_id, "error": error})
    return report
