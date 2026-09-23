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

"""Publication of exports.json — the single platform→developer channel.

Developers cannot read platform/* (DESIGN §4.5), so every cross-pipeline
value reference flows through one object at the ledger root. The platform
side publishes it — hooks inside existing platform tools at completion
points — and developers only read it.

Two disciplines govern every field. Derivation is deterministic parsing of
persisted artifacts — no LLM anywhere. And every field is explicitly
nullable: a null is never a guess, and derivation_notes records every
explicit-null-instead-of-guess decision.
"""

import fnmatch
import json
import logging
import os
from datetime import datetime, timezone

import yaml
from google.api_core import exceptions

from . import decisions as decisions_lib

logger = logging.getLogger("migration-dag")

# Must match ledger_iam.EXPORTS_OBJECT: the bootstrap-time conditional grants
# name exactly this object, so publishing anywhere else would write an object
# no developer can read.
EXPORTS_BLOB = "exports.json"
MANIFEST_BLOB = "platform/discovery/manifest.json"

SOURCES = ("discovery", "translation", "deployment", "data")
# Which exports fields each pipeline stage derives; publish() refuses a
# slice that strays outside its source's row.
SOURCE_FIELDS = {
    "discovery": ("source_repo", "target_repo", "component_seed_index"),
    "translation": ("storage_class_menu", "gateway", "gsa_bindings", "compute_classes"),
    "deployment": ("artifact_registry", "node_shapes", "project", "cluster",
                   "workload_pool", "staging_bucket"),
    "data": ("data_gate",),
}

# `data` is a source rather than a field of `discovery` or `deployment`
# because BOTH of those write it — the section is discovery's, the outcomes
# are the deployment data step's — and `publish` gives one source's slice to
# one publisher: a second publisher under an existing source would replace
# that source's derivation notes with its own. It is also the only slice that
# republishes on a cadence measured in operator reports rather than in phase
# completions, which is why the workload staleness gate excludes it by name
# (workload_validate_5._staleness_findings). A database landing does not make
# a translated manifest stale, and forcing a replan on every
# mark_data_service_migrated would make the gate cost more than it protects.
DATA_SOURCE = "data"

# Mirrors servers/phases/k8s_manifests.MAX_MANIFEST_BYTES — the dag server
# must not depend on the phase packages, so the cap is restated here.
MAX_SEED_FILE_BYTES = 1_000_000


def empty_exports() -> dict:
    """The all-null document: every field present, nothing guessed."""
    return {
        "source_repo": None,
        "target_repo": None,
        "component_seed_index": None,
        "storage_class_menu": None,
        "gateway": None,
        "gsa_bindings": None,
        "compute_classes": None,
        "artifact_registry": None,
        "node_shapes": None,
        "project": None,
        "cluster": None,
        "workload_pool": None,
        "staging_bucket": None,
        "data_gate": None,
        "generated_at": None,
        "generations": {source: 0 for source in SOURCES},
        "derivation_notes": [],
    }


class _NoAliasSafeLoader(yaml.SafeLoader):
    """SafeLoader that refuses aliases (mirrors servers/phases/k8s_manifests).

    Customer IaC is untrusted input: safe_load still expands anchor/alias
    references, so a small billion-laughs document can balloon in memory.
    The seed index only needs envelope fields, so refusing fails safe.
    """

    def compose_node(self, parent, index):
        if self.check_event(yaml.events.AliasEvent):
            raise yaml.YAMLError("YAML aliases are not supported")
        return super().compose_node(parent, index)


# --- pure derivation core (unit-testable without GCS) -----------------------

def derive_source_repo(variables: dict) -> dict:
    """source_repo from the onboarding variables. Empty strings become null."""
    return {
        "url": variables.get("source_repo_url") or None,
        "branch": variables.get("source_branch") or None,
        "path": variables.get("source_path") or None,
    }


def derive_target_repo(variables: dict) -> tuple:
    """(target_repo, notes) from the onboarding variables — where the
    workload pull requests ship.

    These coordinates live in platform/onboarding/state.json, which the
    ledger IAM (§4.5) forbids developers to read: at the bucket root a
    developer holds exactly two conditional grants, the registry and this
    object, so exports.json is the only channel a developer session can
    legally learn the target repository through. Empty strings become
    null; an SSM triple whose Git URL is still unresolved is a note,
    never a guessed URL.
    """
    value = {
        "url": variables.get("target_repo_url") or None,
        "branch": variables.get("target_branch") or None,
        "path": variables.get("target_path") or None,
    }
    notes = []
    if not value["url"] and all(variables.get(key) for key in
                                ("ssm_instance", "ssm_location", "ssm_repository")):
        notes.append("discovery: target_repo: the SSM repository triple is "
                     "recorded but its Git URL is unresolved; url stays null "
                     "(never guessed) until configure_repositories resolves "
                     "it and the publish reruns")
    return value, notes


def derive_seed_index(files: list, read_text, chart_roots: list) -> tuple:
    """Builds component_seed_index from the manifest's file entries.

    files: manifest entries carrying "path"; read_text: callable(path) -> str
    over the local source checkout, or None when no checkout is available;
    chart_roots: repo-relative Helm chart root directories (the manifest's
    render-target knowledge) whose templated files must not hit the parser.

    Returns (index, notes). Index-level facts only — Kubernetes kinds,
    metadata.namespace values and *team*-suffixed label values per document —
    never file contents, never findings.
    """
    index = {}
    notes = []
    for entry in files:
        path = entry.get("path")
        if not path:
            continue
        index[path], note = _seed_entry(path, read_text, chart_roots)
        if note:
            notes.append(f"discovery: component_seed_index: {note}")
    return index, notes


def _seed_entry(path: str, read_text, chart_roots: list) -> tuple:
    """(entry, note) for one manifest path. note is None unless the entry
    degraded to empty metadata for a reason worth recording."""
    empty = {"kinds": [], "namespaces": [], "team_labels": [], "names": []}
    lower = path.lower()
    if lower.endswith((".tf", ".tfvars")):
        return {**empty, "kinds": ["terraform"]}, None
    if _under_chart_root(path, chart_roots):
        # Chart contents are Go templates, not parseable YAML; membership in
        # the chart root is itself the ownership signal. A consumer inside a
        # chart is attributed by its `source_path` instead, which is the one
        # case that field is populated for.
        return {**empty, "kinds": ["helm-chart"]}, None
    if read_text is None:
        return empty, None  # one checkout-level note covers every entry
    try:
        content = read_text(path)
    except Exception as e:
        return empty, f"{path}: unreadable ({_first_line(e)}); entry recorded with empty metadata"
    if len(content.encode("utf-8", errors="replace")) > MAX_SEED_FILE_BYTES:
        return empty, f"{path}: exceeds {MAX_SEED_FILE_BYTES} bytes; not parsed"
    try:
        docs = list(yaml.load_all(content, Loader=_NoAliasSafeLoader))
    except yaml.YAMLError as e:
        return empty, (f"{path}: failed to parse ({_first_line(e)}); "
                       "entry recorded with empty metadata")
    return _document_metadata(docs), None


def _under_chart_root(path: str, chart_roots: list) -> bool:
    for root in chart_roots or []:
        if root in (".", "") or path == root or path.startswith(root.rstrip("/") + "/"):
            return True
    return False


def _first_line(error) -> str:
    text = str(error) or "unknown error"
    return text.splitlines()[0][:120]


def _document_metadata(docs: list) -> dict:
    """Envelope facts across every mapping document in one file.

    `names` is here for the data gate. A data dependency's consumer is a
    workload NAME — a Helm release, a Kubernetes object, or the service
    account an IRSA chain named — and `source_path` only ever carries a
    local chart directory, so a consumer reached through IRSA (the acme
    estate's defining case: orders -> s3, via an IAM policy ARN) has no path
    at all. Without a name in the index there is no way to decide which
    component is waiting on that database, and a gate that cannot attribute
    can only block everybody or nobody. Recorded as `(kind, name)` pairs
    rather than bare names because "the Deployment orders" and "the
    ServiceAccount orders" are routinely both present and the consumer
    record says which of the two it means.
    """
    kinds, namespaces, team_labels, names = set(), set(), set(), set()
    for doc in docs:
        if not isinstance(doc, dict):
            continue
        kind = doc.get("kind")
        if isinstance(kind, str) and kind.strip():
            kinds.add(kind.strip())
        metadata = doc.get("metadata")
        if not isinstance(metadata, dict):
            continue
        name = metadata.get("name")
        if isinstance(name, str) and name.strip():
            names.add(f"{(kind or '').strip()}/{name.strip()}"
                      if isinstance(kind, str) and kind.strip()
                      else name.strip())
        namespace = metadata.get("namespace")
        if isinstance(namespace, str) and namespace.strip():
            namespaces.add(namespace.strip())
        labels = metadata.get("labels")
        if not isinstance(labels, dict):
            continue
        for key, value in labels.items():
            # Ownership signals: label keys whose bare name is *team*-suffixed
            # (team, owning-team, example.com/team, ...).
            bare = str(key).rsplit("/", 1)[-1].lower()
            if bare.endswith("team") and isinstance(value, str) and value.strip():
                team_labels.add(value.strip())
    return {"kinds": sorted(kinds), "namespaces": sorted(namespaces),
            "team_labels": sorted(team_labels), "names": sorted(names)}


# --- publish wrapper ---------------------------------------------------------

def load_exports(bucket) -> tuple:
    """(document, generation), or (None, None) when nothing is published yet."""
    blob = bucket.blob(EXPORTS_BLOB)
    try:
        blob.reload()
        return json.loads(blob.download_as_text()), blob.generation
    except exceptions.NotFound:
        return None, None


def publish(bucket, source: str, fields: dict, notes: list, now_iso: str = None) -> dict:
    """Read-derive-write of one source's slice of exports.json.

    Reads the current document (all-null skeleton when absent), overlays
    `fields`, bumps the publishing source's generation, replaces that
    source's derivation notes while keeping the other sources', stamps
    generated_at, and writes under the generation that was read
    (if_generation_match=0 on create). A 412 is retried once by re-reading;
    a second conflict propagates to the caller.
    """
    if source not in SOURCE_FIELDS:
        raise ValueError(f"unknown exports source: {source}")
    stray = sorted(set(fields) - set(SOURCE_FIELDS[source]))
    if stray:
        raise ValueError(f"fields {stray} do not belong to exports source '{source}'")
    last_conflict = None
    for attempt in (1, 2):
        current, generation = load_exports(bucket)
        doc = empty_exports()
        if isinstance(current, dict):
            for key in doc:
                if key in current:
                    doc[key] = current[key]
        doc.update(fields)
        generations = {s: int((doc.get("generations") or {}).get(s) or 0) for s in SOURCES}
        generations[source] += 1
        doc["generations"] = generations
        doc["generated_at"] = now_iso or datetime.now(timezone.utc).isoformat()
        kept = [n for n in (doc.get("derivation_notes") or [])
                if not str(n).startswith(f"{source}:")]
        doc["derivation_notes"] = kept + [str(n) for n in notes]
        try:
            bucket.blob(EXPORTS_BLOB).upload_from_string(
                json.dumps(doc, indent=2), content_type="application/json",
                if_generation_match=generation if generation is not None else 0)
            return doc
        except exceptions.PreconditionFailed as e:
            last_conflict = e
            logger.debug(f"exports.json changed under us (attempt {attempt}); re-reading")
    raise last_conflict


# --- discovery-completion hook -----------------------------------------------

def publish_discovery_exports(bucket, variables: dict, inventory: dict,
                              filter_files=None) -> str:
    """Best-effort publish for the two discovery inventory-persist paths.

    filter_files is the discovery scope filter (scope_lib.filter_files,
    injected — the dag server does not import phase code): callable
    (files, scope) -> (kept, removed). Returns "" on success, or a warning
    line for the tool's response. Never raises: a publish failure must not
    fail a discovery step whose inventory is already persisted.
    """
    try:
        fields, notes = derive_discovery_fields(
            bucket, variables, inventory or {}, filter_files)
        publish(bucket, "discovery", fields, notes)
        return ""
    except Exception as e:
        logger.error(f"exports.json publication failed: {e}")
        return (f"WARNING: exports.json publication failed ({e}). Discovery "
                "results are persisted and this step is complete; "
                "refresh_exports retries the publish.")


def derive_discovery_fields(bucket, variables: dict, inventory: dict,
                            filter_files=None) -> tuple:
    notes = []
    fields = {"source_repo": derive_source_repo(variables)}
    fields["target_repo"], target_notes = derive_target_repo(variables)
    notes.extend(target_notes)
    manifest = _load_manifest(bucket)
    if manifest is None:
        fields["component_seed_index"] = None
        notes.append("discovery: component_seed_index: "
                     f"{MANIFEST_BLOB} is absent or unreadable; index not derived")
        return fields, notes
    # The persisted manifest is the UNFILTERED index of the checkout; the
    # operator's confirmed scope exclusions must hold here too — an excluded
    # file's metadata must not cross into a member-readable object.
    files = manifest.get("files") or []
    scope = variables.get("discovery_scope") or {}
    if filter_files and scope:
        files, removed = filter_files(files, scope)
        if removed:
            notes.append("discovery: component_seed_index: "
                         f"{len(removed)} file(s) excluded by the confirmed "
                         "discovery scope are not indexed")
    unindexed = _includes_outside_manifest(scope.get("included") or [],
                                           manifest.get("files") or [])
    if unindexed:
        notes.append("discovery: component_seed_index: "
                     f"{len(unindexed)} scope-included path(s) fall outside "
                     "the persisted manifest and are not indexed")
    chart_roots = [t.get("root") for t in inventory.get("render_targets") or []
                   if isinstance(t, dict) and t.get("type") == "helm" and t.get("root")]
    root_dir = (variables.get("discovery_scope") or {}).get("root_dir")
    read_text = None
    if root_dir and os.path.isdir(root_dir):
        read_text = _checkout_reader(root_dir)
    else:
        notes.append("discovery: component_seed_index: source checkout "
                     "unavailable; entries carry no per-document metadata")
    index, index_notes = derive_seed_index(files, read_text, chart_roots)
    fields["component_seed_index"] = index
    notes.extend(index_notes)
    return fields, notes


def _includes_outside_manifest(included: list, files: list) -> list:
    """Scope includes that match no manifest entry (absolute paths,
    out-of-tree files): extraction reads them, but the seed index carries
    persisted-manifest facts only, so their absence deserves a note. An
    include that merely un-excludes an in-manifest path matches here and is
    fully indexed — noting it would be a false degradation record."""
    outside = []
    paths = [str(f.get("path") or "") for f in files]
    for pattern in included:
        norm = str(pattern).strip().rstrip("/")
        if not norm:
            continue
        if os.path.isabs(norm):
            outside.append(norm)
            continue
        if not any(p == norm or p.startswith(norm + "/") or fnmatch.fnmatch(p, norm)
                   for p in paths):
            outside.append(norm)
    return outside


def _load_manifest(bucket):
    try:
        return json.loads(bucket.blob(MANIFEST_BLOB).download_as_text())
    except Exception as e:
        logger.error(f"Could not read {MANIFEST_BLOB} for exports derivation: {e}")
        return None


def _checkout_reader(root_dir: str):
    """Returns read_text(path) confined to root_dir.

    Manifest paths are ledger content. A path escaping the checkout must not
    leak even index-level metadata of unrelated local files into an object
    every registered member can read.
    """
    real_root = os.path.realpath(root_dir)

    def read_text(path: str) -> str:
        full = os.path.realpath(os.path.join(real_root, path))
        if full != real_root and not full.startswith(real_root + os.sep):
            raise ValueError(f"path escapes the source checkout: {path}")
        # Cap before reading: the checkout is re-read at publish time, which
        # can be much later than indexing — a file that grew since must not
        # be pulled into memory just to be rejected.
        size = os.path.getsize(full)
        if size > MAX_SEED_FILE_BYTES:
            raise ValueError(f"exceeds {MAX_SEED_FILE_BYTES} bytes ({size})")
        with open(full, "r", encoding="utf-8", errors="replace") as f:
            return f.read()

    return read_text


# --- translation-completion derivation ---------------------------------------

def _unit_files(unit_entry: dict, suffixes: tuple) -> dict:
    """{path: content} of one persisted unit blob's files, filtered by suffix.
    Tolerates legacy blob shapes: missing pieces yield an empty dict."""
    files = {}
    for f in ((unit_entry or {}).get("result") or {}).get("files") or []:
        if not isinstance(f, dict):
            continue
        path = str(f.get("path") or "")
        if path.endswith(suffixes):
            files[path] = str(f.get("content") or "")
    return files


def _load_yaml_docs(content: str) -> tuple:
    """(docs, error) under the hardened posture; error is '' when parseable."""
    if len(content.encode("utf-8", errors="replace")) > MAX_SEED_FILE_BYTES:
        return [], f"exceeds {MAX_SEED_FILE_BYTES} bytes"
    try:
        return list(yaml.load_all(content, Loader=_NoAliasSafeLoader)), ""
    except yaml.YAMLError as e:
        return [], f"failed to parse ({_first_line(e)})"


def _compute_class_menu(done_units: list, plan) -> tuple:
    """(compute_classes, notes): the ComputeClass names the done compute-class
    units ship, sorted. `[]` only when the derivation can prove the landing
    zone ships none — every compute-class unit in the plan is done or skipped
    and no done one carries a ComputeClass document; otherwise (a unit still
    planned, revise or error, or an unreadable plan) None with a note, so a
    refresh mid-translation can never publish a guess. Deviates from the
    storage menu, which is None whenever no storage unit is done: the
    workload rewrite needs "no class" as a fact under the NAP and Autopilot
    arms, where no compute-class unit is ever done."""
    notes = []
    # The plan gate comes FIRST: a unit still planned, revise or error means the
    # menu is not yet a fact, whatever the done units already ship — a partial
    # menu would brief the pending pool as "absent" (a guess).
    plan_units = (plan or {}).get("units") if isinstance(plan, dict) else None
    if not isinstance(plan_units, list):
        notes.append("translation: compute_classes: the translation plan is unavailable; "
                     "null, not guessed")
        return None, notes
    pending = [str(u.get("unit_id")) for u in plan_units
               if isinstance(u, dict) and u.get("kind") == "compute-class"
               and u.get("status") not in ("done", "skipped")]
    if pending:
        notes.append("translation: compute_classes: compute-class unit(s) pending ("
                     + ", ".join(pending) + "); menu not derived")
        return None, notes
    units = _units_of_kind(done_units, "compute-class")
    menu, unreadable = set(), []
    for entry in units:
        for path, content in sorted(_unit_files(entry, (".yaml", ".yml")).items()):
            docs, error = _load_yaml_docs(content)
            if error:
                unreadable.append(f"{path}: {error}")
                continue
            for doc in docs:
                if not (isinstance(doc, dict) and doc.get("kind") == "ComputeClass"):
                    continue
                name = (doc.get("metadata") or {}).get("name")
                if isinstance(name, str) and name.strip():
                    menu.add(name.strip())
    if unreadable:
        # A done unit whose file the loader could not read may carry a class
        # the menu cannot see; "ships none" would be a guess.
        notes.append("translation: compute_classes: a done compute-class unit has an "
                     "unreadable file (" + "; ".join(unreadable) + "); null, not guessed")
        return None, notes
    if menu:
        return sorted(menu), notes
    notes.append("translation: compute_classes: no ComputeClass in the validated translation "
                 "(the NAP or Autopilot arm, or no typed NodePool); empty menu")
    return [], notes


def _units_of_kind(done_units: list, kind: str) -> list:
    return [u for u in done_units or []
            if ((u or {}).get("unit") or {}).get("kind") == kind]


def derive_translation_fields(done_units: list, parse_ksa, plan: dict = None) -> tuple:
    """storage_class_menu, gateway, gsa_bindings and compute_classes from the
    done unit blobs.

    parse_ksa is the ksa_annotations contract parser (injected — the dag
    server does not import phase code): callable({path: tf content}) ->
    {"status", "bindings", "error", ...}. `plan` is the persisted translation
    plan (variables.translation_plan); compute_classes needs it to tell "the
    landing zone ships no class" from "compute-class units still pending".
    Returns (fields, notes).
    """
    notes = []
    fields = {}

    storage_units = _units_of_kind(done_units, "storage")
    if not storage_units:
        fields["storage_class_menu"] = None
        notes.append("translation: storage_class_menu: no done storage unit; "
                     "menu not derived")
    else:
        menu = set()
        for entry in storage_units:
            for path, content in sorted(_unit_files(entry, (".yaml", ".yml")).items()):
                docs, error = _load_yaml_docs(content)
                if error:
                    notes.append(f"translation: storage_class_menu: {path}: {error}; "
                                 "file skipped")
                    continue
                for doc in docs:
                    if not (isinstance(doc, dict) and doc.get("kind") == "StorageClass"):
                        continue
                    name = (doc.get("metadata") or {}).get("name")
                    if isinstance(name, str) and name.strip():
                        menu.add(name.strip())
        fields["storage_class_menu"] = sorted(menu)

    fields["gateway"], gateway_notes = _find_gateway(done_units)
    notes.extend(gateway_notes)

    fields["compute_classes"], cc_notes = _compute_class_menu(done_units, plan)
    notes.extend(cc_notes)

    wi_units = _units_of_kind(done_units, "workload-identity")
    if not wi_units:
        fields["gsa_bindings"] = None
        notes.append("translation: gsa_bindings: no done workload-identity unit; "
                     "explicit null, not a guess")
    else:
        result = parse_ksa(_unit_files(wi_units[0], (".tf",)))
        if result.get("status") == "ok":
            bindings = result.get("bindings")
            unit_inputs = ((wi_units[0].get("unit") or {}).get("inputs") or {})
            if bindings == {} and unit_inputs.get("irsa_bindings"):
                # The contract's honest escape: bindings exist, their emails
                # do not. The validate gate now refuses to ship this shape,
                # so this arm is the last line of defense (legacy blobs, a
                # refresh over old artifacts) — and the note names WHICH
                # half failed: the pipeline never supplying a target project
                # is a different defect from a worker declining one it held.
                fields["gsa_bindings"] = None
                if unit_inputs.get("target_project"):
                    notes.append(
                        "translation: gsa_bindings: the contract map is empty "
                        "while the unit's inputs record IRSA bindings and "
                        f"target project '{unit_inputs['target_project']}' — "
                        "the worker declined to bind despite a recorded "
                        "project (see the unit's open_questions); explicit "
                        "null, never a placeholder")
                else:
                    notes.append(
                        "translation: gsa_bindings: the contract map is empty "
                        "while the unit's inputs record IRSA bindings and no "
                        "target project was supplied to the unit; explicit "
                        "null, never a placeholder")
            else:
                fields["gsa_bindings"] = bindings
        else:
            fields["gsa_bindings"] = None
            reason = ("contract absent (a run predating it)"
                      if result.get("status") == "absent"
                      else f"contract malformed: {result.get('error')}")
            notes.append(f"translation: gsa_bindings: ksa_annotations {reason}; "
                         "explicit null, never a guess")
    return fields, notes


GATEWAY_API_PREFIX = "gateway.networking.k8s.io/"
# The platform Gateway must carry this annotation or label; it is
# what distinguishes THE shared entry point from an app-scoped Gateway a unit
# legitimately emits (an early end-to-end run's network unit shipped a real
# Gateway API `frontend/frontend` — publishing it would have pointed every
# developer HTTPRoute at one app's gateway).
SHARED_GATEWAY_MARKER = "gkma.dev/shared-gateway"
# The attach-permission label pair. The gateway brief orders the shared
# Gateway's `from: Selector` listeners to select exactly this label, the
# tenancy brief stamps it on every Namespace it issues, the validate gate
# (translation_validate_3/gateway_contract) judges the shipped policy
# against the shipped Namespace manifests, and the workload routing brief
# states it to the developer worker. One product-owned
# definition in the shared exports base, so the briefs and the gate can
# never drift: an early end-to-end run shipped a worker-invented selector no
# Namespace satisfied — attachedRoutes stayed 0 behind a "structurally
# valid" report.
GATEWAY_ACCESS_LABEL = "gkma.dev/gateway-access"
GATEWAY_ACCESS_VALUE = "shared"
# The landing-zone planner's gateway unit family — the unit charged with
# emitting the marked Gateway. Its presence in the done units is what tells
# a null gateway field apart from a workspace that translated before the
# family existed: one is a unit to revise, the other is a refresh to run.
GATEWAY_UNIT_KIND = "gateway"


def gateway_documents(entry: dict) -> tuple:
    """(marked, unmarked, errors): Gateway API docs in ONE unit's YAML.

    Each found document is {"name", "namespace", "path", "doc"} — "doc" is
    the parsed mapping itself, which the validate gate's attach-policy check
    reads `spec.listeners` off (the derivation ignores it); errors are
    per-file parse messages. Both the exports derivation below and the
    validate step's gateway output contract read this one scanner, so
    "what counts as the shared Gateway" is defined in a single place.
    """
    marked, unmarked, errors = [], [], []
    for path, content in sorted(_unit_files(entry, (".yaml", ".yml")).items()):
        docs, error = _load_yaml_docs(content)
        if error:
            # A parse-broken file must not silently read as "no Gateway
            # manifest" — a broken platform Gateway would misreport as
            # not-yet-shipped.
            errors.append(f"{path}: {error}")
            continue
        for doc in docs:
            if not (isinstance(doc, dict) and doc.get("kind") == "Gateway"):
                continue
            if not str(doc.get("apiVersion") or "").startswith(GATEWAY_API_PREFIX):
                continue
            metadata = doc.get("metadata") or {}
            marks = {}
            for key in ("annotations", "labels"):
                if isinstance(metadata.get(key), dict):
                    marks.update(metadata[key])
            found = {"name": metadata.get("name"),
                     "namespace": metadata.get("namespace"), "path": path,
                     "doc": doc}
            if str(marks.get(SHARED_GATEWAY_MARKER)).lower() == "true":
                marked.append(found)
            else:
                unmarked.append(found)
    return marked, unmarked, errors


def _find_gateway(done_units: list) -> tuple:
    """(gateway, notes): the marked shared Gateway from the unit outputs.

    Only Gateway API documents count, and only one carrying the shared
    marker is published; everything else is named as a candidate in the
    notes rather than guessed at. The degradation notes name the actual
    remedy, which depends on whether the plan carried a gateway unit at
    all: revise that unit, or run the translation that ships one.
    """
    marked, candidates, scan_notes = [], [], []
    planned = False
    for entry in done_units or []:
        if ((entry or {}).get("unit") or {}).get("kind") == GATEWAY_UNIT_KIND:
            planned = True
        found, unmarked, errors = gateway_documents(entry)
        marked.extend({"name": g["name"], "namespace": g["namespace"]}
                      for g in found)
        candidates.extend(f"{g['namespace'] or '?'}/{g['name'] or '?'}"
                          for g in unmarked)
        scan_notes.extend(f"translation: gateway: {e}; file skipped"
                          for e in errors)
    # Two units carrying the SAME marked Gateway (e.g. shipped once and again
    # by a refresh-delivered platform unit) is agreement, not ambiguity.
    distinct = list({(g.get("namespace"), g.get("name")): g for g in marked}.values())
    if len(distinct) == 1:
        return distinct[0], scan_notes
    if len(distinct) > 1:
        return None, scan_notes + [
            f"translation: gateway: {len(distinct)} distinct Gateway manifests "
            f"carry the {SHARED_GATEWAY_MARKER} marker; not guessing which is "
            "the shared entry point"]
    remedy = (
        f"the plan's `{GATEWAY_UNIT_KIND}` unit is what must ship the marked "
        "Gateway — revise that unit and re-run translation"
        if planned else
        "no `gateway` unit ran in this workspace (it translated before the "
        "family existed, or discovery recorded no entry point): re-plan, or "
        "run refresh_exports once a unit ships one")
    if candidates:
        return None, scan_notes + [
            "translation: gateway: Gateway API manifest(s) in the unit "
            f"outputs ({', '.join(sorted(candidates))}) carry no "
            f"{SHARED_GATEWAY_MARKER} marker — an app-scoped gateway is "
            f"not the shared entry point; {remedy}"]
    return None, scan_notes + [
        "translation: gateway: no Gateway manifest in the unit outputs; "
        + remedy]


def publish_translation_exports(bucket, done_units: list, parse_ksa, plan: dict = None) -> str:
    """Best-effort publish for the platform-translation completion hook.
    Returns "" on success or a warning line for the tool's response."""
    try:
        fields, notes = derive_translation_fields(done_units, parse_ksa, plan)
        publish(bucket, "translation", fields, notes)
        return ""
    except Exception as e:
        logger.error(f"exports.json publication failed: {e}")
        return (f"WARNING: exports.json publication failed ({e}). Validation "
                "results are persisted and this step is complete; "
                "refresh_exports retries the publish.")


# --- deployment-completion derivation -----------------------------------------

def _flat_choices(decisions: dict) -> dict:
    """lz_decisions as a flat {id: choice} map (resolved form also accepted)."""
    return decisions_lib.flat_choices(decisions)


def _cluster_type(decisions: dict, triggers=None) -> tuple:
    """(type, note): autopilot vs standard through the one registry projection
    (decisions_lib.cluster_mode) over the recorded choices and the inventory
    triggers; conflicting or absent implications are an explicit null, never a
    guess. Without usable triggers the projection falls back to choice-only
    voting, which is exactly what this function did before the registry."""
    mode, reason = decisions_lib.cluster_mode(decisions, triggers)
    if mode is not None:
        return mode, None
    if reason == "none recorded":
        return None, ("deployment: cluster.type: no recorded landing-zone decision "
                      "implies a cluster mode; null")
    return None, ("deployment: cluster.type: the landing-zone decisions imply "
                  "conflicting cluster modes; null, not guessed")


def _target_project(destinations: list, cluster_scan, ledger_project) -> tuple:
    """(project, note). Only where persisted artifacts record the target
    project: a literal LZ HCL attribute, or a destination project that is not
    the workspace's ledger metadata project — the provisioning default reuses
    that project, so a value equal to it cannot be told apart from a default
    and stays null (do NOT conflate the two)."""
    literal_projects = sorted({c.get("project") for c in cluster_scan or []
                               if c.get("project")})
    if len(literal_projects) == 1:
        return literal_projects[0], None
    if len(literal_projects) > 1:
        return None, ("deployment: project: multiple google_container_cluster "
                      "resources declare different literal projects "
                      f"({', '.join(literal_projects)}); not guessing which "
                      "is the target")
    for dest in destinations:
        project = dest.get("project")
        if project and project != ledger_project:
            return project, None
    if any(dest.get("project") == ledger_project for dest in destinations if dest.get("project")):
        return None, ("deployment: project: the only recorded project equals the "
                      "workspace's ledger metadata project, which the provisioning "
                      "default also uses; null, not guessed")
    return None, ("deployment: project: no persisted artifact records the target "
                  "project as a literal; null")


def derive_deployment_fields(variables: dict, inventory: dict, ledger_project,
                             planned_refs: dict, cluster_scan) -> tuple:
    """artifact_registry, node_shapes, project, cluster, workload_pool and
    staging_bucket from the persisted deployment artifacts.

    planned_refs: {source ref -> planned dest ref} for self_service entries
    (their persisted outcome carries no destination; the plan is re-derived
    deterministically by the caller). cluster_scan: literal
    google_container_cluster attributes scanned from the target clone, or
    None when no clone is available. Returns (fields, notes).
    """
    notes = []
    fields = {}
    inventory = inventory or {}
    destinations = [d for d in (variables.get("artifact_registry_destinations") or [])
                    if isinstance(d, dict)]

    if not destinations:
        fields["artifact_registry"] = None
        notes.append("deployment: artifact_registry: no destinations recorded "
                     "(deployment has not provisioned); explicit null")
    else:
        if any(not d.get("url") for d in destinations):
            notes.append("deployment: artifact_registry: a destination is declared "
                         "with computed HCL values; its URL stays unresolved "
                         "(null, not guessed)")
        image_map = {}
        for image in inventory.get("images") or []:
            replication = (image or {}).get("replication") or {}
            status = replication.get("status")
            ref = (image or {}).get("ref")
            # Failed and never-planned images stay out of the map.
            if not ref or status not in ("replicated", "self_service"):
                continue
            dest_ref = replication.get("destination") or (planned_refs or {}).get(ref)
            if not dest_ref:
                dest_ref = None
                notes.append(f"deployment: artifact_registry.image_map: {ref}: "
                             "planned destination unresolved; dest_ref null")
            entry = {"dest_ref": dest_ref, "status": status}
            # The content digest the ledger recorded, passed through for
            # consumers that pin by digest — never derived here. `verified_by`
            # rides with it because the two provenances are not equally
            # trustworthy: "skopeo_preserve_digests" is a digest the server
            # observed at the destination, "user_asserted" is one a user
            # typed. A consumer that pins by digest must be able to tell them
            # apart, so the channel is self-describing rather than a bare hash.
            if replication.get("content_digest"):
                entry["content_digest"] = replication["content_digest"]
            if replication.get("verified_by"):
                entry["verified_by"] = replication["verified_by"]
            image_map[ref] = entry
        fields["artifact_registry"] = {
            "destinations": [d["url"] for d in destinations if d.get("url")],
            "image_map": image_map,
        }

    if not inventory:
        fields["node_shapes"] = None
        notes.append("deployment: node_shapes: no discovery inventory persisted; null")
    else:
        shapes = []
        for nodegroup in inventory.get("nodegroups") or []:
            if not isinstance(nodegroup, dict):
                continue
            name = str(nodegroup.get("name") or "nodegroup")
            types = [str(t) for t in nodegroup.get("instance_types") or []]
            summary = (f"{name}: {', '.join(types)}" if types
                       else f"{name}: instance types unrecorded")
            if nodegroup.get("gpu"):
                summary += " (gpu)"
            shapes.append(summary)
        fields["node_shapes"] = shapes

    project, project_note = _target_project(destinations, cluster_scan, ledger_project)
    fields["project"] = project
    if project_note:
        notes.append(project_note)

    cluster_type, type_note = _cluster_type(variables.get("lz_decisions"),
                                            inventory.get("triggers"))
    if type_note:
        notes.append(type_note)
    name = location = None
    if cluster_scan is None:
        notes.append("deployment: cluster: target clone unavailable; literal "
                     "name/location not derived")
    elif len(cluster_scan) == 1:
        name = cluster_scan[0].get("name")
        location = cluster_scan[0].get("location")
        if name is None or location is None:
            notes.append("deployment: cluster: google_container_cluster declares "
                         "computed name/location values; null, not guessed")
    elif not cluster_scan:
        notes.append("deployment: cluster: the design declares no "
                     "google_container_cluster; name/location null")
    else:
        notes.append(f"deployment: cluster: {len(cluster_scan)} "
                     "google_container_cluster resources declared; not guessing "
                     "which one is the target")
    if name is None and cluster_type is None and location is None:
        fields["cluster"] = None
    else:
        fields["cluster"] = {"name": name, "type": cluster_type, "location": location}

    fields["workload_pool"] = f"{project}.svc.id.goog" if project else None
    if not project:
        notes.append("deployment: workload_pool: derives only from a non-null "
                     "project; null")

    # Nothing persisted records a staging bucket today; inventing one would
    # break the null-is-never-a-guess rule.
    fields["staging_bucket"] = None
    notes.append("deployment: staging_bucket: nothing in the persisted artifacts "
                 "records a staging bucket; explicit null")
    return fields, notes


def _image_slice_only(bucket, fields: dict, notes: list, reason: str) -> tuple:
    """artifact_registry alone, with the stored notes for the rest kept.

    The degraded leg of the deployment hook. `publish` overlays only the keys
    it is handed, so omitting a field leaves the stored value intact — but it
    replaces the source's notes wholesale, so the notes belonging to the
    fields not recomputed are carried forward explicitly.
    """
    current, _ = load_exports(bucket)
    prefix = "deployment: artifact_registry"
    kept = [n for n in (current or {}).get("derivation_notes") or []
            if str(n).startswith("deployment:") and not str(n).startswith(prefix)]
    mine = [n for n in notes if str(n).startswith(prefix)]
    return ({"artifact_registry": fields.get("artifact_registry")},
            mine + kept + [f"deployment: {reason} — only artifact_registry was "
                           "republished; the other deployment fields keep "
                           "their last published values"])


def publish_deployment_exports(bucket, variables: dict, inventory: dict,
                               ledger_project, planned_refs: dict,
                               cluster_scan, degraded: str = None) -> str:
    """Best-effort publish for the deployment-completion hook.

    `degraded` names an input unavailable in THIS environment (canonically
    the target clone, on a machine that did not run provisioning). The hook
    then publishes the image map alone rather than the whole slice: deriving
    `cluster` without the clone yields null, and a wholesale republish would
    overwrite hook-published data from a healthier machine with nulls — the
    regression `refresh_exports` avoids by omitting the source entirely.
    Returns "" on a clean publish, or a warning line for the tool's response.
    """
    try:
        fields, notes = derive_deployment_fields(
            variables, inventory, ledger_project, planned_refs, cluster_scan)
        if degraded:
            fields, notes = _image_slice_only(bucket, fields, notes, degraded)
        publish(bucket, "deployment", fields, notes)
        if degraded:
            return (f"NOTE: {degraded}, so only the exports image_map was "
                    "republished; cluster, node_shapes, workload_pool and "
                    "staging_bucket keep their last published values "
                    "(re-run refresh_exports where the clone lives).")
        return ""
    except Exception as e:
        logger.error(f"exports.json publication failed: {e}")
        return (f"WARNING: exports.json publication failed ({e}). Deployment "
                "results are persisted and this step is complete; "
                "refresh_exports retries the publish.")


# --- data-gate derivation ------------------------------------------------------

DATA_GATE_SCHEMA_VERSION = 1

# The third statement of one rule, and the reason it is not one constant: the
# deployment phase owns it (`datamigration.GATING_DISPOSITION` / `MIGRATED`),
# the workload phase restates it (`workload/datagate`) because a developer
# module must not import a platform one, and this module restates it again
# because `servers/dag/server` imports no phase code at all. Only the note
# count below uses these — the gate itself is computed developer-side — but a
# count that drifted from the rule would report a worklist nobody is held on.
# `servers/phases/workload/datagate_test.DriftTest` is what keeps the
# three equal — it lives with the developer half because that is the
# only package allowed to import all three.
GATING_DISPOSITION = "migrate"
MIGRATED_STATUS = "migrated"


def derive_data_gate(inventory: dict, migrations: dict,
                     key_of, status_of) -> tuple:
    """(fields, notes) for the `data` source: what each data service owes.

    The two halves of the answer live in two objects a developer may not read
    — the inventory's `data_dependencies` section (what the estate uses, and
    what the migration decided to do with each of them) and
    platform/deployment/data_migrations.json (what has actually moved) — so
    this joins them into the one document that crosses the boundary. Both
    callables are injected: `servers/dag/server` does not import phase code,
    and the identity of a data service is the deployment phase's definition,
    not a second one written here that could drift from it. `status_of`
    rather than a key function and an index of our own, for exactly that
    reason: an entry matches a record on more than tuple equality — an
    ARN-addressed record is matched on the ARN alone, tolerating a fold and a
    change of spelling — and a lookup written here would have to restate that
    rule and would fall behind it. It did: while this built its own index, a
    service whose handle moved between scans read as never migrated HERE
    while the deployment step's own runbook printed it as done, and the ship
    gate held every component consuming data that had already landed.

    `scanned` distinguishes an estate with no data dependencies from a
    discovery that never ran the scan. Both publish an empty `services`, and
    a gate that could not tell them apart would read the second as
    permission to ship.
    """
    section = inventory.get("data_dependencies") if isinstance(inventory, dict) else None
    scanned = isinstance(section, list)
    entries = section if scanned else []
    services, gating = [], 0
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        # With the section: the lookup refuses a spelling that reaches more
        # than one entry in it, which one entry at a time cannot see.
        record = status_of(migrations or {}, entry, entries) or {}
        status = record.get("status")
        disposition = entry.get("disposition")
        if disposition == GATING_DISPOSITION and status != MIGRATED_STATUS:
            gating += 1
        services.append({
            "service": entry.get("service"),
            "identifier": entry.get("identifier"),
            "address": entry.get("address"),
            # Spelled out rather than left to the reader: the developer side
            # never sees `evidence`, which is what entry_directory derives it
            # from, so the key would be unreconstructable there.
            "directory": key_of(entry)[1],
            "disposition": disposition,
            "status": status,
            "consumers": [
                {"workload": c.get("workload"), "kind": c.get("kind"),
                 "namespace": c.get("namespace"),
                 "source_path": c.get("source_path"),
                 "detection": c.get("detection")}
                for c in entry.get("consumers") or [] if isinstance(c, dict)],
        })

    notes = []
    if not scanned:
        notes.append("data: data_gate: the inventory has no data_dependencies "
                     "section — the discovery data scan has not run, so this "
                     "slice records scanned=false rather than 'no dependencies'")
    else:
        notes.append(f"data: data_gate: {len(services)} data service(s), "
                     f"{gating} graded 'migrate' and not reported migrated")
    unattributed = sum(1 for s in services
                       if s["disposition"] == GATING_DISPOSITION
                       and s["status"] != MIGRATED_STATUS and not s["consumers"])
    if unattributed:
        notes.append(f"data: data_gate: {unattributed} outstanding service(s) "
                     "have no attributed consumer, so no component's ship gate "
                     "can hold on them — attach a consumer at the data review "
                     "if one is known")
    return ({"data_gate": {"schema_version": DATA_GATE_SCHEMA_VERSION,
                           "scanned": scanned,
                           "services": services}}, notes)


def publish_data_exports(bucket, inventory: dict, migrations: dict,
                         key_of, status_of) -> str:
    """Best-effort publish of the `data` slice. "" on success, else a warning.

    `migrations` is None when the outcome store exists but could not be read.
    The slice is then NOT published: deriving from an unreadable store would
    republish every gating service as outstanding and hold ship gates on
    completions that are recorded — the same degraded-input discipline
    `refresh_exports` applies per source. An ABSENT store is not degraded, it
    is a migration where nothing has been reported yet, and {} says so.
    """
    if migrations is None:
        return ("WARNING: platform/deployment/data_migrations.json could not "
                "be read, so the exports data_gate slice was left at its last "
                "published value rather than republished as 'nothing "
                "migrated'. Repair the object and run refresh_exports.")
    try:
        fields, notes = derive_data_gate(inventory or {}, migrations,
                                         key_of, status_of)
        if not fields["data_gate"]["scanned"]:
            # The SAME degraded-input discipline as the unreadable outcome
            # store above, applied to the other input — an asymmetry a review
            # caught. A vanished section is not evidence that the estate has
            # no data dependencies, and `carry_scan_sections` reaches this
            # state on any inventory read failure: it logs and returns, so
            # `write_discovery_inventory` hands us an inventory with no
            # section at all. Publishing scanned=false there would lay "nobody
            # looked" over a good slice and refuse EVERY component in the
            # estate at its ship gate, with only a platform engineer able to
            # undo it. An estate that genuinely never scanned publishes
            # nothing instead, and the developer-side refusal for an absent
            # slice covers it — the same answer, without the clobber.
            return ("WARNING: the inventory carries no data_dependencies "
                    "section, so the exports data_gate slice was left at its "
                    "last published value rather than republished as 'never "
                    "scanned'. If the discovery data scan HAS run, this is an "
                    "inventory read failure — repair it and run "
                    "refresh_exports.")
        publish(bucket, DATA_SOURCE, fields, notes)
        return ""
    except Exception as e:
        logger.error(f"exports.json publication failed: {e}")
        return (f"WARNING: exports.json data_gate publication failed ({e}). "
                "This step's own results are persisted; refresh_exports "
                "retries the publish. Until it succeeds, developer ship gates "
                "read the last published slice.")


# --- full recompute (the refresh_exports tool) ---------------------------------

def refresh_document(bucket, per_source: dict, now_iso: str = None) -> tuple:
    """Recomputes the document from per-source derivations.

    per_source: {source: (fields, notes)} for each source that was actually
    recomputed. A source whose derivation inputs are unavailable (a missing
    local checkout, an unreadable unit blob) must be OMITTED, not derived to
    nulls: the stored slice — published from a healthier environment — keeps
    its fields, its notes and its generation, so a degraded recompute never
    regresses known-good data. Bumps the generation of each recomputed
    source whose field slice differs from the current document (a mere
    re-run bumps nothing), replaces the recomputed sources' notes while
    keeping the others', stamps generated_at, and writes under the
    generation that was read with one re-read retry.
    Returns (document, changed_sources).
    """
    last_conflict = None
    for attempt in (1, 2):
        current, generation = load_exports(bucket)
        doc = empty_exports()
        if isinstance(current, dict):
            for key in doc:
                if key in current:
                    doc[key] = current[key]
        generations = {s: int((doc.get("generations") or {}).get(s) or 0) for s in SOURCES}
        changed = []
        notes = [str(n) for n in (doc.get("derivation_notes") or [])
                 if str(n).split(":", 1)[0] not in per_source]
        for source in SOURCES:
            if source not in per_source:
                continue
            fields, source_notes = per_source[source] or ({}, [])
            if any(doc.get(key) != value for key, value in fields.items()):
                changed.append(source)
                generations[source] += 1
            doc.update(fields)
            notes.extend(str(n) for n in source_notes)
        doc["generations"] = generations
        doc["generated_at"] = now_iso or datetime.now(timezone.utc).isoformat()
        doc["derivation_notes"] = notes
        try:
            bucket.blob(EXPORTS_BLOB).upload_from_string(
                json.dumps(doc, indent=2), content_type="application/json",
                if_generation_match=generation if generation is not None else 0)
            return doc, changed
        except exceptions.PreconditionFailed as e:
            last_conflict = e
            logger.debug(f"exports.json changed under us (attempt {attempt}); re-reading")
    raise last_conflict
