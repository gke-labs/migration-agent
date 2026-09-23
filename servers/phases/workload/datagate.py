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

"""Whether a component may ship yet, given the data it depends on.

Pure — no I/O, no MCP, no ledger. The two step packages that use it own
those: `workload_scopeconfirm_2` warns with it, `workload_validate_5` blocks
with it.

WARN EARLY, BLOCK LATE. Most workloads in a real estate have a data
dependency, and a database migration takes days. Blocking at scope confirm
would stall every team behind the slowest database and buy nothing:
translating, reviewing and validating a component against a database that has
not moved yet is work that stays correct when it does. Only the last step —
opening the pull request that puts the workload in front of traffic — is the
one that must not happen first. So scope confirm says what is outstanding and
lets the developer through; the ship gate refuses.

THE INPUT IS `exports.json` AND NOTHING ELSE — two of its fields. A developer
session holds two conditional grants at the ledger root (§4.5) and `platform/*`
is not one of them: GCS itself returns 403, not the server's own check, so the
inventory section and the deployment outcome store are both unreadable here.
The platform side joins those two into one published slice
(`exports.derive_data_gate`), and this module reads it for WHAT is owed. The
other field is `exports.component_seed_index`, which answers WHO owes it: the
whole name-matching half of the attribution (`_seed_names`, `blind_spots`,
`_namespaces_at`) runs off it, so a null index does not merely degrade the
gate — it makes every scope path blind and sends every consumer-bearing
service to `untestable`. A null data_gate slice is "the platform has not
published yet", never "nothing is owed", and `verdict` says so rather than
passing the component.

ONLY `migrate` GATES, and the argument is the deployment module's
(`servers/phases/deployment/datamigration`): `keep-in-aws` is a decision the
customer is entitled to make, `rebuild` means an empty target is a working
target, `replatform` means the shape changes rather than the data moving, and
`escalate`/`undecided` mean nobody has yet said the thing must move. The
constant is restated here rather than imported because a developer-phase
module must not depend on a platform-phase one — they run in different
sessions against different credentials.

WHAT THE GATE WILL NOT DO: BLOCK ON AN UNATTRIBUTED SERVICE. A service graded
`migrate` whose entry lists no consumer is real work that is really owed, and
this module reports it at both sites — but it does not hold any component's
ship gate, because it cannot say WHICH component. The alternatives are to
block everybody, which stalls an estate on one unattributed database, or to
block nobody and say nothing. `consumers.py` refuses name similarity for the
same reason in the other direction ("a wrong consumer stops the wrong team"),
and the estate-wide backstop already exists: the platform walk parks at
STATE_DEPLOYMENT_DATA_MIGRATION until every gating service is settled, so the
migration cannot COMPLETE over one. What this gate adds is per-component, and
it is only as good as the attribution it is given.
"""

import posixpath

# The disposition that owes a move. See the module docstring.
GATING_DISPOSITION = "migrate"
MIGRATED = "migrated"

# Consumer kinds whose `workload` is a Kubernetes object name of a KNOWN
# kind, so the seed index lookup can be kind-qualified. Everything else in
# the schema's `kind` field is either a Terraform resource type
# (helm_release, kubernetes_secret) whose name need not match any object's,
# or the reviewer's unclassified 'workload' — those match on the bare name
# against any kind, which over-matches rather than under-matches on purpose:
# an over-match holds a component whose files merely CONTAIN that name, and
# a held component is recoverable in a way a shipped one is not.
KIND_ALIASES = {"service_account": "ServiceAccount"}


def slice_of(exports: dict):
    """The published data_gate slice, or None when there is not one."""
    if not isinstance(exports, dict):
        return None
    value = exports.get("data_gate")
    return value if isinstance(value, dict) else None


def describe(service: dict) -> str:
    """How a data service is named back to the developer.

    Deliberately the same grammar as `datamigration.describe`, which is what
    the platform operator sees: the two sides of a handoff that consists of
    one person telling another a service has landed must call it the same
    thing.
    """
    name = service.get("service") or "data service"
    identifier = service.get("identifier") or service.get("address") or "(unnamed)"
    return f"{name} {identifier}"


def outstanding(slice_doc: dict) -> list:
    """Published services graded `migrate` and not reported migrated.

    `in_progress` is still outstanding — a status report is not a completion,
    and shipping against a database that is still copying is the failure this
    gate exists to prevent.
    """
    return [s for s in (slice_doc or {}).get("services") or []
            if isinstance(s, dict)
            and s.get("disposition") == GATING_DISPOSITION
            and s.get("status") != MIGRATED]


def _under(path: str, directory: str) -> bool:
    """True when `path` is `directory` or sits beneath it.

    A prefix test on whole path SEGMENTS: `charts/web-legacy/values.yaml` is
    NOT under `charts/web`, which a bare `startswith` would say it is.

    Both sides are normalized first. `consumers.py` normpaths what IT
    produces, but `attach_data_consumer(source_path=...)` is free text, so a
    reviewer who types "./charts/web" would otherwise fail to match
    "charts/web/values.yaml" and quietly not hold a component that should be.

    A directory that names the whole repository — "", ".", "/", "./" — matches
    NOTHING here rather than everything, and neither does one that climbs out
    of the checkout ("../x"), which nothing in a component scope can sit
    under. That record is durable and replayed over every later scan, so a
    reviewer who typed "." would otherwise attach that consumer to every file
    in every component: the whole estate held on one service, with no way out
    but hand-editing the corrections object. A path that wide is not evidence,
    so the consumer falls through to its workload name like any other.

    This is the ONE place those decisions live. An earlier draft screened the
    repo-wide case at the call site as well, and the duplicate turned out to
    be a branch no test could reach.
    """
    directory = posixpath.normpath((directory or "").strip()).strip("/")
    path = posixpath.normpath((path or "").strip()).strip("/")
    if not directory or directory == "." or directory.startswith(".."):
        return False
    return path == directory or path.startswith(directory + "/")


def _seed_names(seed_index: dict, paths) -> dict:
    """{name: [path]} over the given paths, for both spellings the index
    records — bare `name` and the kind-qualified `Kind/name`."""
    found = {}
    for path in paths:
        entry = (seed_index or {}).get(path)
        if not isinstance(entry, dict):
            continue
        for recorded in entry.get("names") or []:
            found.setdefault(recorded, []).append(path)
            if "/" in recorded:
                found.setdefault(recorded.split("/", 1)[1], []).append(path)
    return found


def _namespaces_at(seed_index: dict, paths) -> set:
    return {ns for path in paths
            for ns in ((seed_index or {}).get(path) or {}).get("namespaces") or []}


def blind_spots(scope_paths, seed_index: dict) -> list:
    """Scope files the seed index records no workload NAME for.

    This is what stops a name-miss being read as "belongs to another
    component". Two ways a file in scope carries no name:

    - It is Helm chart content. `exports._seed_entry` marks everything under
      a chart root `kinds: ["helm-chart"]` and parses nothing, because chart
      templates are Go templates rather than YAML. A component deployed from
      a chart therefore has NO indexed names at all — and an IRSA consumer,
      which `consumers.py` always builds with `source_path: None`, is nothing
      BUT a name. Neither signal is available, so the honest answer for that
      pair is "cannot tell", not "somebody else's".
    - It is Terraform. `_seed_entry` kinds it `terraform` and never parses it,
      and a Kubernetes workload very much CAN be declared in one: a
      `helm_release` or a `kubernetes_secret` is exactly what
      `consumers.py` builds a `terraform_wiring` consumer from, and that
      consumer carries a `source_path` only when its chart is a local
      relative path — a registry chart (`bitnami/postgresql`) yields None and
      leaves nothing but the name. An earlier draft exempted Terraform on the
      grounds that no workload could be declared there. That was simply
      wrong, and it left the same false "belongs to another component" claim
      this function exists to prevent.
    - Its entry is absent, or degraded to empty metadata by a parse failure
      or a size cap. Absent also covers the drift case: the scope was
      confirmed against one published index and a later extraction
      republished a narrower one, so a path the component still ships has
      quietly stopped being indexed.

    What is left, and the only thing that makes a component fully indexed, is
    a parsed Kubernetes manifest that declared at least one name.
    """
    return [path for path in scope_paths or []
            if not ((seed_index or {}).get(path) or {}).get("names")]


def _match_consumer(consumer: dict, scope_paths, seed_index: dict) -> tuple:
    """(matched, how, mismatch) for one consumer against one component.

    `how` is the evidence line for the developer; `mismatch` is a disagreement
    between what discovery recorded and what the component's own files say,
    reported WITHOUT being resolved. A mismatch never turns a match off: the
    two signals disagree about a detail, not about whether the workload is
    here, and the conservative reading of a disagreement at a ship gate is
    the one that holds.
    """
    workload = consumer.get("workload")
    source_path = consumer.get("source_path")
    if source_path:
        inside = [p for p in scope_paths if _under(p, source_path)]
        if inside:
            return True, f"chart {source_path} is in this component's scope", None

    if not workload:
        return False, "", None
    kind = KIND_ALIASES.get(consumer.get("kind"))
    names = _seed_names(seed_index, scope_paths)
    hit = names.get(f"{kind}/{workload}") if kind else None
    if hit is None:
        hit = names.get(workload)
    if not hit:
        return False, "", None

    where = sorted(set(hit))
    how = (f"{kind or 'workload'} '{workload}' is declared in "
           + ", ".join(where[:3])
           + (f" (+{len(where) - 3} more)" if len(where) > 3 else ""))
    mismatch = None
    namespace = consumer.get("namespace")
    recorded = _namespaces_at(seed_index, where)
    if namespace and recorded and namespace not in recorded:
        mismatch = (
            f"discovery recorded '{workload}' in namespace '{namespace}', but "
            f"the file(s) in this scope declare it in "
            f"{', '.join(sorted(recorded))}. The gate holds on the name match "
            "and reports the disagreement rather than picking a side")
    elif source_path:
        mismatch = (
            f"discovery recorded '{workload}' under chart {source_path}, which "
            "is not in this component's scope, but the name is declared in "
            "files that are. The gate holds on the name match and reports the "
            "disagreement rather than picking a side")
    return True, how, mismatch


def verdict(exports: dict, scope_paths, seed_index: dict, *,
            scope_unreadable: bool = False,
            scope_reresolved: bool = False) -> dict:
    """What this component's data dependencies mean for it right now.

    scope_paths: the component's repo-relative files — the confirmed
    scope.json `resolved_paths` at the ship gate, the proposal's resolution
    at scope confirm. None (a degraded resolution that never enumerated
    files) is not an empty scope: nothing can be attributed, and the verdict
    says that instead of reading it as "no dependencies".

    Returns:
      published   — False when the platform has not published the slice.
      scanned     — False when discovery never ran the data scan.
      attributable— False when scope_paths is None.
      scope_unreadable — the scope OBJECT could not be read. A different
                    fact from "resolved to nothing", and the only one of the
                    two a re-run can clear, so it holds rather than passing.
      scope_reresolved — the paths came from re-resolving a degraded scope's
                    globs, so nobody has ever checked that they enumerate the
                    component. Suppresses `elsewhere`.
      blind       — scope files the index records no workload name for.
      blocking    — outstanding services attributed to THIS component.
      unattributed— outstanding services the MAPPING names no consumer for.
      untestable  — outstanding services this component could not be tested
                    against, because of a gap on THIS side.
      elsewhere   — outstanding services whose consumers are declared in no
                    file this component ships.
      mismatches  — disagreements surfaced, never resolved.

    `elsewhere` is a claim about THIS component and no other. What it rests
    on is exactly "the index recorded no such name for any file this
    component ships" — not "the component does not declare it", which is
    stronger than the index can support: a `kind: List` bundle contributes no
    names, and neither does an object using `generateName`. The sentence
    built from it is worded to that limit. It is also suppressed entirely for
    a re-resolved scope, whose file list nobody verified.
    Where the component is not fully indexed a name-miss means "cannot tell"
    and the service goes to `untestable` instead. Saying "somebody else's"
    about a service this component may well use is worse than saying nothing:
    it is the one sentence that would stop a developer looking further.

    `unattributed` and `untestable` are separated because they are different
    facts with different remedies, and one message for both was wrong in both
    directions — it sent a reader to the data review to repair a mapping that
    was fine, and it described a blind scope as a mapping gap. The mapping
    names no consumer: attach one at the review. This component cannot be
    tested: nothing about the mapping will fix that.
    """
    slice_doc = slice_of(exports)
    result = {"published": slice_doc is not None,
              "scanned": bool((slice_doc or {}).get("scanned")),
              "attributable": scope_paths is not None,
              "scope_unreadable": bool(scope_unreadable),
              "scope_reresolved": bool(scope_reresolved),
              "blind": [], "blocking": [], "unattributed": [],
              "untestable": [], "elsewhere": [], "mismatches": []}
    if slice_doc is None:
        return result

    paths = list(scope_paths or [])
    result["blind"] = blind_spots(paths, seed_index)
    for service in outstanding(slice_doc):
        consumers = service.get("consumers") or []
        if not consumers:
            result["unattributed"].append(service)
            continue
        if not result["attributable"]:
            # The scope never resolved to files, so no consumer can be tested
            # against it — a gap on this side, not in the mapping.
            result["untestable"].append(service)
            continue
        hits = []
        for consumer in consumers:
            if not isinstance(consumer, dict):
                continue
            matched, how, mismatch = _match_consumer(consumer, paths, seed_index)
            if matched:
                hits.append({"workload": consumer.get("workload"),
                             "detection": consumer.get("detection"),
                             "how": how})
            if mismatch:
                result["mismatches"].append(
                    {"service": describe(service),
                     "workload": consumer.get("workload"),
                     "detail": mismatch})
        if hits:
            result["blocking"].append({**service, "matched": hits})
        elif result["blind"] or result["scope_reresolved"]:
            # A re-resolved scope is drawn from the index BY CONSTRUCTION, so
            # `blind` can never flag a file missing from it — and nobody ever
            # checked that those globs enumerate the component. A developer
            # who wrote `k8s/**` from their own clone, while the component
            # also ships `charts/orders/`, gets a fully-indexed-looking scope
            # that is simply incomplete. Claiming the consumer is recorded
            # nowhere here would rest on a list no one verified.
            result["untestable"].append(service)
        else:
            result["elsewhere"].append(service)
    return result


def _cannot_tell(result: dict) -> str:
    """Why this component could not be tested, or "" when it could.

    ONE builder, because the advisory and the ship gate both need the
    sentence and the two drifted the moment they were written separately.

    None of these is the developer's to fix, which is why the wording never
    asks them to: the graph has no edge back to STATE_WKLD_SCOPE once a scope
    is confirmed, so a scope that resolved to nothing stays that way. An
    earlier draft ordered these "by how much the reader can do about it" and
    called an unresolved scope theirs to re-run, which contradicts the very
    finding this gate is built on.
    """
    if result["scope_unreadable"]:
        return ("this component's scope object could not be read on this "
                "run, so nothing could be tested against it — this is a "
                "transient read failure, not a statement about the scope")
    if not result["attributable"]:
        return ("this component's scope never resolved to a file list, so no "
                "consumer could be tested against it")
    blind = result["blind"]
    if not blind:
        # A re-resolved scope reaches `untestable` with NOTHING blind — its
        # paths come from the index by construction — so a builder keyed on
        # `blind` alone returned "" and `residue` rendered "hold nothing as a
        # result — .", with the reason blanked out of the one paragraph the
        # developer reads before approving. Every route into `untestable`
        # needs a sentence here, which is the point of there being one
        # builder.
        if result["scope_reresolved"]:
            return ("this component's scope was confirmed before the file "
                    "index existed, so the gate matched against its "
                    "include/exclude patterns re-resolved just now, and "
                    "nobody has ever checked that those patterns list "
                    "everything the component ships")
        return ""
    shown = ", ".join(blind[:3]) + (f" (+{len(blind) - 3} more)"
                                    if len(blind) > 3 else "")
    # Say the RULE, not a list of causes. Two earlier drafts enumerated, and
    # both enumerations were wrong for the commonest file in a real scope: a
    # `kustomization.yaml`, a plain `.json` config or any YAML that is not a
    # named Kubernetes object parses perfectly and records no name, so
    # "chart content", "Terraform", "no checkout" and "did not parse" are all
    # false of it — and the remedy those imply (refresh_exports) does
    # nothing. The index records a name only for a document carrying
    # `metadata.name`; everything else contributes none, whether by design or
    # by degradation. The rule covers every branch of `_seed_entry` including
    # the ones no enumeration remembered.
    return (f"{len(blind)} file(s) this component ships carry no indexed "
            f"workload name — {shown}. The index records a name only where a "
            "document declares `metadata.name`: a kustomization, a plain "
            "config file, Helm chart content and Terraform all record none by "
            "design, and so does any file the index could not parse or was "
            "built without the checkout to read. A consumer reached through "
            "an IRSA chain or a registry chart is a NAME with no path, so "
            "where there are no names there is nothing to match it against")


def _service_lines(services: list, matched: bool = False) -> list:
    lines = []
    for service in services:
        status = service.get("status") or "not started"
        line = f"  - {describe(service)} — reported {status}"
        if matched:
            who = ", ".join(
                f"{h['workload']} ({h['how']})" for h in service["matched"])
            line += f"; this component's {who}"
        elif service.get("consumers"):
            who = ", ".join(sorted({c.get("workload") for c in service["consumers"]
                                    if c.get("workload")}))
            line += f"; consumed by {who}"
        lines.append(line)
    return lines


def advisory(result: dict) -> str:
    """The scope-confirm notice, or "" when there is nothing to say.

    Never blocks and never asks for anything: it exists so that the first
    time a developer hears about a database that has not moved is at the
    start of the work rather than at the end of it.
    """
    if not result["published"]:
        return ("Data dependencies: the platform pipeline has not published "
                "the data gate yet, so this component's dependencies are not "
                "known here. The ship gate will hold until it does.")
    if not result["scanned"]:
        return ("Data dependencies: the discovery data scan has not run for "
                "this estate, so nothing is recorded about the AWS data "
                "services this component uses. The ship gate cannot check "
                "them.")
    lines = []
    if result["blocking"]:
        lines.append(
            f"Data dependencies: {len(result['blocking'])} AWS data service(s) "
            "this component uses have not moved to GCP yet:")
        lines.extend(_service_lines(result["blocking"], matched=True))
    if result["unattributed"]:
        lines.append(
            f"{len(result['unattributed'])} outstanding data service(s) have "
            "no attributed consumer, so they may or may not be yours:")
        lines.extend(_service_lines(result["unattributed"]))
    if result["untestable"]:
        lines.append(
            f"{len(result['untestable'])} outstanding data service(s) could "
            f"not be matched to this component — {_cannot_tell(result)}. They "
            "may or may not be yours:")
        lines.extend(_service_lines(result["untestable"]))
    if not lines:
        return ""
    lines.append(
        "This does not block scoping, planning, translation or review — only "
        "the pull request at the end. The platform team reports each service "
        "as it lands; nothing is needed from you here.")
    return "\n".join(lines)


def _anything_outstanding(result: dict) -> bool:
    """Did the slice list any gating service at all, however it was placed?

    The four buckets partition `outstanding`, so their union answers "is
    there anything the scope could be relevant to" without re-reading the
    slice.
    """
    return bool(result["blocking"] or result["unattributed"]
                or result["untestable"] or result["elsewhere"])


def refusal(result: dict, component: str) -> str:
    """The ship-gate refusal, or "" when the component may ship.

    Names the service, who owns the decision and both exits, because neither
    exit is a developer's to take: the two tools live in the platform
    session at STATE_DEPLOYMENT_DATA_MIGRATION.
    """
    if not result["published"]:
        return (f"Component '{component}' cannot ship yet: exports.json "
                "carries no data_gate slice, so whether this component's AWS "
                "data services have moved is unknown. This is a platform-side "
                "publication gap, not a finding against your manifests — ask "
                "the platform team to run refresh_exports. Owner: Platform "
                "Engineer.")
    if result["scope_unreadable"] and _anything_outstanding(result):
        # The sibling read on this path — exports.json — fails CLOSED, and
        # this one used to fail open: a transient 503 on scope.json
        # attributed nothing, cleared the component and shipped it, while
        # telling the developer their scope "never resolved to a file list",
        # which was false and named no remedy. A read that FAILED is the one
        # case here a re-run genuinely fixes, so it is the one case that says
        # so.
        #
        # Guarded on something actually being owed, because the scope only
        # ever decides WHOSE an outstanding service is. With none outstanding
        # — a scan that found no data services, or an estate where every one
        # has landed — the file list cannot change the answer, and holding on
        # it would claim the dependencies are "unknown" when they are known
        # and empty. Unguarded, one flaky read of an object the gate does not
        # need parked every component in a data-free estate.
        return (f"Component '{component}' cannot ship yet: its scope object "
                "could not be read on this run, so which files it ships — and "
                "therefore which data services it depends on — is unknown. "
                "Nothing is wrong with your manifests and nothing has to be "
                "re-planned: re-run run_workload_validation. If it persists, "
                f"workloads/{component}/scope.json is unreadable and an admin "
                "should look at it.")
    if not result["scanned"]:
        # The same epistemic state as an unpublished slice, and it refuses for
        # the same reason. `scanned` exists precisely because an estate with
        # no data dependencies and an estate nobody looked at both publish an
        # empty `services`; distinguishing them and then shipping over the
        # second anyway would make the distinction pointless. The current
        # graph cannot reach here — the data scan is on the path to
        # extraction, which is what publishes the seed index the scope step
        # refuses without — so this costs nothing and is the right default if
        # that ever changes.
        return (f"Component '{component}' cannot ship yet: the discovery data "
                "scan has not run for this estate, so nothing is recorded "
                "about the AWS data services this component uses and the "
                "check cannot be made. Not a finding against your manifests — "
                "ask the platform team to run the discovery data scan. Owner: "
                "Platform Engineer.")
    if not result["blocking"]:
        return ""
    lines = [f"Component '{component}' cannot ship yet: "
             f"{len(result['blocking'])} AWS data service(s) it depends on are "
             "graded 'migrate' and have not been reported as landed."]
    lines.extend(_service_lines(result["blocking"], matched=True))
    lines.append(
        "Owner: Platform Engineer. Two exits, both from the platform session "
        "parked at STATE_DEPLOYMENT_DATA_MIGRATION: report the move with "
        "mark_data_service_migrated once the data is in GCP, or record "
        "annotate_data_dependency(disposition='keep-in-aws') for a service "
        "that turns out not to be movable — a kept service stops being owed "
        "and stops gating.")
    lines.append(
        "Everything else about this component is validated and persisted. "
        "Re-run run_workload_validation once the service is settled; nothing "
        "has to be re-planned or re-translated.")
    return "\n".join(lines)


def residue(result: dict) -> str:
    """What the gate saw but did not act on, for the tool's response.

    Separate from `refusal` because it prints on the way through as well as
    on the way back: a component that ships while three unattributed services
    are outstanding should be told so, and the shape of this gate is that
    nobody is held on them.
    """
    lines = []
    for mismatch in result["mismatches"]:
        lines.append(f"Mapping disagreement on {mismatch['service']}: "
                     f"{mismatch['detail']}.")
    if result["unattributed"]:
        lines.append(
            f"{len(result['unattributed'])} outstanding data service(s) have "
            "no attributed consumer and hold no component: "
            + "; ".join(describe(s) for s in result["unattributed"])
            + ". If one of them is yours, attach the consumer at the data "
              "review so the gate can see it.")
    if result["untestable"]:
        # A DIFFERENT fact from the one above, with a different remedy: these
        # services DO have consumers, and the reason none could be tested is
        # on this side. Reported as "no attributed consumer" it sent the
        # reader to the data review to repair a mapping that was fine.
        lines.append(
            f"{len(result['untestable'])} outstanding data service(s) could "
            f"not be tested against this component, and hold nothing as a "
            f"result — {_cannot_tell(result)}. The services: "
            + "; ".join(describe(s) for s in result["untestable"])
            + ". Check by hand whether any of them is yours before relying on "
              "this gate.")
    if result["elsewhere"]:
        # What the check ESTABLISHED, not what one would like it to mean.
        # Being fully name-indexed proves the consumer is not declared here;
        # it proves nothing about the consumer being declared anywhere. A
        # workload that lives only in some other component's Helm chart is
        # declared in no index entry at all, so "they hold their own
        # components" would be the same false reassurance this module removed
        # from the blind case, relocated to the indexed one.
        lines.append(
            f"{len(result['elsewhere'])} further outstanding data service(s) "
            "name consumers the index records in no file this component "
            "ships. That is what the check establishes and no more — a "
            "`kind: List` bundle or a `generateName` object contributes no "
            "name to the index — and whether another component holds them "
            "depends on that component's own scope.")
    return "\n".join(lines)
