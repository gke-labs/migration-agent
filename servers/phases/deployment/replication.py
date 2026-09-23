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

"""Deployment-stage engine action: replicate the discovered ECR images.

`replicate_images` backs STATE_DEPLOYMENT_REPLICATE_IMAGES. The mode the user
chose at the STATE_DEPLOYMENT_IMAGE_REPLICATION elicitation decides what it
does:

- `agent_mediated`: the server runs `skopeo copy --preserve-digests` itself,
  one copy per `images[]` entry with `registry == "ecr"`, source pinned by
  digest when discovery recorded one. Credentials are strictly the ones
  already present in the local environment: the preflight verifies the skopeo
  binary, ECR read access (one `skopeo inspect` per distinct registry host)
  and the destination repository's existence, and reports exactly what is
  missing — it never runs `aws`, never runs `skopeo login`, never writes an
  auth file. The Artifact Registry push authenticates with the ADC access
  token the server already holds for provisioning.
- `self_service` (the default, and what a bare accept means): the server
  writes a runbook of `skopeo copy` commands to the ledger and touches
  nothing — the no-AWS contract of DESIGN.md §10 stays fully intact.

A `self_service` outcome is a plan, not a copy: nothing downstream treats the
image as moved until the user says so. `mark_self_service_complete` (backing
the `mark_replication_complete` tool) is that path — it flips entries to
`replicated` with `verified_by: "user_asserted"`, recording a content digest
only when the user supplies one, never inventing digests or destinations.

Per-image outcomes land in the inventory (`images[].replication`), so the
ledger is the audit trail. A preflight or total failure returns `on_failure`,
which parks the graph back at STATE_DEPLOYMENT_INIT: the user fixes
credentials or applies the PR, then calls `prepare_image_deployment` again.
Partial failures are reported but do not block (`on_success`) — an incomplete
replication reduces coverage, never progress.
"""

import logging
import os
import re
import shutil
import subprocess
import tempfile
import time

import google.auth
from google.auth.transport.requests import AuthorizedSession, Request

from servers.dag.state_management import (
    get_bucket_name,
    load_inventory,
    save_inventory,
)
import servers.dag.state_management as state_mgr

logger = logging.getLogger("migration-dag")

AR_API = "https://artifactregistry.googleapis.com/v1"
SCOPE = "https://www.googleapis.com/auth/cloud-platform"

RUNBOOK_BLOB_PATH = "platform/deployment/replication-runbook.sh"

ELICITATION_STATE = "STATE_DEPLOYMENT_IMAGE_REPLICATION"

# An inspect is a metadata round-trip; a copy moves layers. The copy bound is
# deliberately generous — killing a large image mid-transfer helps nobody —
# but it exists so a wedged network cannot hang the mutation forever.
INSPECT_TIMEOUT_S = 60
COPY_TIMEOUT_S = 1800

# The loose "algo:hex" shape of an OCI content digest. mark-complete refuses
# anything else: a digest is recorded verbatim or not at all, and a value that
# cannot be a digest is a paste error, not a fact to persist.
DIGEST_RE = re.compile(r"^[a-z0-9+._-]+:[0-9a-fA-F]{32,}$")

# The replication statuses a user assertion may flip to `replicated`: the
# self-service plan, and an agent-mediated failure the user then fixed by
# hand. `replicated` itself is idempotent-skip, anything else is refused.
USER_MARKABLE_STATUSES = ("self_service", "replication_failed")

# The image stays in ECR by decision. Reached from a failed copy nobody is
# going to chase or a runbook nobody is going to run — the counterpart of a
# data service recorded `keep-in-aws`, and there for the same reason: without
# it, the step that waits for these confirmations would be a trap rather than
# a decision point.
#
# Deliberately NOT in USER_MARKABLE_STATUSES: a bulk `mark_replication_complete()`
# with no refs must not resurrect a decision somebody made on purpose. Naming
# the ref explicitly does re-open it, because changing your mind is allowed —
# it just has to be deliberate.
ABANDONED = "abandoned"

# provision_artifact_registry deliberately does not poll the repository-create
# LRO (repositories settle in seconds). When the user opts into agent_mediated
# within the same drain, the preflight's existence check can land before the
# repository settles; a few short retries absorb that race instead of parking
# the graph with advice about a terraform apply that is not the problem.
DEST_CHECK_ATTEMPTS = 3
DEST_CHECK_RETRY_S = 3


def _bucket(config):
    return state_mgr.gcs_client.bucket(get_bucket_name(config["ledger_uri"]))


def _run(cmd: list[str], timeout: int) -> tuple[bool, str]:
    """Runs a command; returns (ok, trimmed combined output)."""
    try:
        res = subprocess.run(
            cmd, capture_output=True, text=True, timeout=timeout)
    except subprocess.TimeoutExpired:
        return False, f"timed out after {timeout}s"
    output = ((res.stdout or "") + (res.stderr or "")).strip()
    return res.returncode == 0, output[-500:]


def _image_path(image: dict) -> str:
    """The repository path without its registry host (payments/api)."""
    _, sep, path = (image.get("repository") or "").partition("/")
    return path if sep else (image.get("repository") or "")


def _src_ref(image: dict) -> str:
    """The reference to pull: the digest pins content, so it wins over the ref."""
    if image.get("digest"):
        return f"{image['repository']}@{image['digest']}"
    return image["ref"]


def _dest_tag(image: dict) -> str:
    """Registries refuse pushes to `@digest` references, so the destination is
    always a tag: the source tag when there is one, a digest-derived tag for
    digest-only pins (content equality is guaranteed by --preserve-digests,
    the tag is just an addressable name), and `latest` when the source ref
    carried neither (which is what that ref meant at pull time)."""
    if image.get("tag"):
        return image["tag"]
    digest = image.get("digest")
    if digest:
        return "sha-" + digest.partition(":")[2][:12]
    return "latest"


def _dest_ref(dest_url: str, image: dict) -> str:
    return f"{dest_url}/{_image_path(image)}:{_dest_tag(image)}"


def _plan_destinations(dest_base: str, ecr_images: list) -> tuple[dict, str]:
    """The full destination reference per source ref, collision-free.

    Refs can span ECR accounts and regions, so two sources can carry the same
    path and tag (111….us-east-1…/payments/api:1.4.2 and
    222….eu-west-1…/payments/api:1.4.2) and would map to one destination ref —
    both copies "succeed" and the tag points at whichever landed last. Such a
    group keeps a host-derived path prefix instead, and the note reports the
    rewrite; the common case stays a plain mirror of the source path.
    """
    by_dest = {}
    for image in ecr_images:
        by_dest.setdefault(_dest_ref(dest_base, image), []).append(image)
    refs, rewritten = {}, []
    for dest, group in by_dest.items():
        if len(group) == 1:
            refs[group[0]["ref"]] = dest
            continue
        for image in group:
            host_slug = re.sub(
                r"[^a-z0-9]+", "-", image["ref"].partition("/")[0].lower()).strip("-")
            refs[image["ref"]] = (
                f"{dest_base}/{host_slug}/{_image_path(image)}:{_dest_tag(image)}")
            rewritten.append(image["ref"])
    note = ""
    if rewritten:
        note = (" Destination collision: " + ", ".join(sorted(rewritten)) +
                " share a path and tag across registry hosts; each is kept under "
                "a host-prefixed path so neither silently overwrites the other.")
    return refs, note


def planned_destination_refs(dest_url: str, ecr_images: list) -> dict:
    """The deterministic {source ref -> destination ref} plan for a destination.

    Public wrapper for the exports derivation: a self_service outcome
    persists no destination, but the plan is a pure function of the
    destination URL and the image list, so a consumer re-derives exactly the
    references the runbook printed."""
    refs, _ = _plan_destinations(dest_url, ecr_images)
    return refs


def destination_is_resolvable(dest: str) -> bool:
    """Does this look like a registry reference a consumer could pull?

    Hygiene only, never resolution: the server cannot reach the user's
    destination. A digest is validated against DIGEST_RE, and a destination
    deserves the same asymmetry — 'yes', a bare 'acme/frontend:1.5.0' or a
    half-pasted host would otherwise be published as the image_map dest_ref
    and become what the workload transforms rewrite every container image to.
    """
    text = str(dest or "").strip()
    host, sep, path = text.partition("/")
    if not sep or not path.strip("/") or " " in text:
        return False
    return "." in host or ":" in host or host == "localhost"


def mark_self_service_complete(inventory: dict, refs: list = None,
                               digests: dict = None, destinations: dict = None,
                               dest_url: str = None) -> tuple[list, list, list, list]:
    """Records the user's assertion that self-service/manual copies are done.

    Flips `images[].replication` to `status: "replicated"` with
    `verified_by: "user_asserted"`. Nothing is invented: the destination is
    the caller's explicit value, the entry's recorded one, or the
    deterministic runbook plan for `dest_url` — an image with none of the
    three is refused, because a replicated map entry with no destination
    would poison every consumer, and one that does not parse as a registry
    reference is refused too (a malformed destination looks resolvable and is
    not). A content digest is recorded only when supplied (and shaped like
    one); absent means unknown, never guessed. Flipping an entry the server's
    own copy FAILED keeps that failure under `previous`, so the ledger still
    shows what broke and that a human repaired it.

    refs: source refs to mark; None/empty marks every markable entry plus
    every ref a digest or destination was supplied for (bulk).
    digests / destinations: optional {source ref -> value} maps.
    Mutates the inventory in place; the caller persists it.
    Returns (marked_refs, updated_refs, already_replicated_refs, problems).
    `updated_refs` are entries already replicated whose record this call
    improved with a digest or destination the first call did not carry.
    """
    digests, destinations = digests or {}, destinations or {}
    images = [i for i in (inventory or {}).get("images") or []
              if isinstance(i, dict) and i.get("ref")]
    by_ref = {i["ref"]: i for i in images}
    ecr_images = [i for i in images if i.get("registry") == "ecr"]
    planned = (planned_destination_refs(dest_url, ecr_images)
               if dest_url and ecr_images else {})
    if refs:
        targets = list(dict.fromkeys(refs))
    else:
        # Bulk also covers any ref the caller supplied a value FOR, even one
        # already replicated: the tool's own response tells the user to go
        # verify with `skopeo inspect` and re-call with the digest, and that
        # second call carries digests but no refs.
        targets = [i["ref"] for i in images
                   if (i.get("replication") or {}).get("status")
                   in USER_MARKABLE_STATUSES]
        targets += [r for r in dict.fromkeys(list(digests) + list(destinations))
                    if r not in targets]
    marked, updated, already, problems = [], [], [], []
    for ref in targets:
        image = by_ref.get(ref)
        if image is None:
            problems.append(
                f"{ref}: not in the discovery inventory — nothing recorded "
                "(compare the ref against the runbook's source references)")
            continue
        outcome = image.get("replication") or {}
        status = outcome.get("status")
        digest = digests.get(ref)
        if digest is not None and not DIGEST_RE.match(str(digest)):
            problems.append(
                f"{ref}: '{digest}' does not look like a content digest "
                "(expected algo:hex, e.g. sha256:...) — not marked; digests "
                "are recorded verbatim or not at all")
            continue
        explicit = destinations.get(ref)
        if explicit is not None and not destination_is_resolvable(explicit):
            problems.append(
                f"{ref}: '{explicit}' does not look like a registry reference "
                "(expected host/path[:tag], the host carrying a dot or a "
                "port) — not marked. A malformed destination is worse than a "
                "missing one: it is what every consumer rewrites the image to")
            continue
        if status == "replicated":
            # Idempotent, but not inert. The tool's own response tells the
            # user to verify with `skopeo inspect` and re-call with the
            # digest, so a well-shaped digest or destination supplied for an
            # entry already marked is an upgrade of the record, not a skip.
            upgrade = {k: v for k, v in
                       (("content_digest", str(digest) if digest else None),
                        ("destination", explicit))
                       if v and outcome.get(k) != v}
            if upgrade:
                outcome.update(upgrade)
                image["replication"] = outcome
                updated.append(ref)
            else:
                already.append(ref)
            continue
        # An abandoned entry is markable only when the caller named it: see
        # the ABANDONED comment. `refs` being set is what makes it deliberate.
        if status not in USER_MARKABLE_STATUSES and not (
                status == ABANDONED and refs):
            problems.append(
                f"{ref}: no self-service or failed replication outcome is "
                f"recorded (status: {status or 'none'}) — only entries the "
                "replication step planned or failed can be marked, and this "
                "one was not. The replication step is behind this one and "
                "nothing transitions back to it"
                + (". This one was abandoned; name it in refs= to record a "
                   "copy after all" if status == ABANDONED else ""))
            continue
        dest = explicit or outcome.get("destination") or planned.get(ref)
        if not dest:
            problems.append(
                f"{ref}: no destination is resolvable (the runbook was "
                "written with an <AR_DESTINATION> placeholder) — pass the "
                "full reference you pushed to via destinations={...}")
            continue
        record = {"status": "replicated", "destination": dest,
                  "verified_by": "user_asserted"}
        if digest:
            record["content_digest"] = str(digest)
        if status in ("replication_failed", ABANDONED):
            # The one ledger record that the server's own copy failed, or that
            # somebody decided against it, and a human then made it. Overwriting
            # it wholesale would erase the audit history this tool exists to
            # keep.
            #
            # `error` is carried forward from the outcome BEING replaced or
            # from the one IT replaced. failed -> abandoned -> replicated is a
            # reachable sequence, and rebuilding `previous` from the current
            # layer alone dropped the server's failure evidence at the last
            # step — keeping the human decision and losing the machine's
            # reason for it, which is backwards. `previous` is flat by schema,
            # so the chain collapses into one record rather than nesting.
            previous = {
                "status": status,
                "error": (outcome.get("error")
                          or (outcome.get("previous") or {}).get("error")),
                "reason": outcome.get("reason"),
                # Who decided, and when. `abandon_copies` records them because
                # "the next reader has to be able to tell that from an
                # oversight" — which is just as true after somebody reverses
                # the decision as before.
                "abandoned_by": outcome.get("abandoned_by"),
                "abandoned_at": outcome.get("abandoned_at"),
            }
            record["previous"] = {k: v for k, v in previous.items()
                                  if v is not None}
        image["replication"] = record
        marked.append(ref)
    return marked, updated, already, problems


def abandon_copies(inventory: dict, refs: list, reason: str, author: str,
                   abandoned_at: str) -> tuple[list, list]:
    """Records that these images will not be copied. Returns (abandoned, problems).

    Only an entry the replication step planned or failed can be abandoned:
    there is nothing to decide about an image already replicated, and one with
    no outcome at all was never in scope for a copy (replication was declined,
    and the images stay in ECR without anybody having to say so per image).

    `reason` is required, unlike most notes here. Abandoning is the one action
    in this step that leaves a workload pointing at ECR forever, and the next
    reader needs to know it was a decision rather than an oversight.

    Mutates the inventory in place; the caller persists it.
    """
    by_ref = {i["ref"]: i for i in (inventory or {}).get("images") or []
              if isinstance(i, dict) and i.get("ref")}
    abandoned, problems = [], []
    for ref in dict.fromkeys(refs or []):
        image = by_ref.get(ref)
        if image is None:
            problems.append(
                f"{ref}: not in the discovery inventory — nothing recorded")
            continue
        outcome = image.get("replication") or {}
        status = outcome.get("status")
        if status == ABANDONED:
            problems.append(f"{ref}: already abandoned")
            continue
        if status not in USER_MARKABLE_STATUSES:
            problems.append(
                f"{ref}: status is {status or 'none'} — only a planned or "
                "failed copy can be abandoned"
                + (" (this one is already replicated)"
                   if status == "replicated" else ""))
            continue
        record = {"status": ABANDONED, "reason": reason,
                  "abandoned_by": author, "abandoned_at": abandoned_at}
        if outcome.get("destination"):
            # What it would have been copied to. Kept so a later reader can
            # tell an abandoned plan from one that never had a destination.
            record["destination"] = outcome["destination"]
        if outcome.get("error"):
            record["previous"] = {"status": status, "error": outcome["error"]}
        image["replication"] = record
        abandoned.append(ref)
    return abandoned, problems


def _resolved_destinations(variables: dict) -> list:
    """Every fully-resolved destination provisioning recorded. Empty when
    all declared registries are computed by the user's terraform apply.
    Replication targets the first; callers must report the rest, not drop
    them silently."""
    return [d for d in variables.get("artifact_registry_destinations") or []
            if d.get("url")]


def _login_command(host: str) -> str:
    """The command the USER runs to grant ECR read access — printed, never run."""
    if host == "public.ecr.aws":
        return "skopeo login public.ecr.aws  # anonymous pulls usually work; login only if inspect failed"
    region = host.split(".")[3] if len(host.split(".")) > 4 else "<region>"
    return (f"aws ecr get-login-password --region {region} | "
            f"skopeo login --username AWS --password-stdin {host}")


def _preflight(skopeo: str, ecr_images: list, dest: dict,
               session: AuthorizedSession) -> list[str]:
    """Returns the list of problems standing between us and copying. Checks
    only — fixing any of these is the user's job, by design."""
    problems = []

    resource = (f"projects/{dest['project']}/locations/{dest['location']}"
                f"/repositories/{dest['repository']}")
    for attempt in range(DEST_CHECK_ATTEMPTS):
        res = session.get(f"{AR_API}/{resource}")
        if res.status_code != 404 or attempt == DEST_CHECK_ATTEMPTS - 1:
            break
        time.sleep(DEST_CHECK_RETRY_S)
    if res.status_code == 404:
        problems.append(
            f"destination repository {dest['url']} does not exist yet — if it is "
            "created by your terraform, apply the migration PR first, then re-run "
            "prepare_image_deployment")
    elif res.status_code != 200:
        problems.append(
            f"destination repository {dest['url']} not verifiable "
            f"(HTTP {res.status_code})")

    # One inspect per distinct registry host proves read credentials are
    # already present. Refs can span accounts and regions; each host
    # authenticates separately.
    hosts = {}
    for image in ecr_images:
        hosts.setdefault(image["ref"].partition("/")[0], image)
    for host, image in sorted(hosts.items()):
        ok, output = _run(
            [skopeo, "inspect", "--no-tags", f"docker://{_src_ref(image)}"],
            INSPECT_TIMEOUT_S)
        if not ok:
            # A failed inspect is usually a missing login, but not always:
            # the recorded image may have been removed from ECR, or egress
            # may be blocked — so the diagnosis is offered, not asserted.
            problems.append(
                f"cannot read from {host} "
                f"({output.splitlines()[-1] if output else 'no output'}) — "
                f"missing login, an image since removed, or blocked egress. "
                f"If you have not logged in, run: {_login_command(host)}")
    return problems


def _adc_credentials():
    credentials, _ = google.auth.default(scopes=[SCOPE])
    return credentials


def _copy_images(skopeo: str, ecr_images: list, dest_refs: dict, credentials) -> tuple[int, list]:
    """Copies every image, recording the outcome on the entry in place.

    Returns (replicated_count, failures as (ref, error) pairs). The ADC token
    rides on the command line (--dest-creds): it is short-lived, scoped to the
    user's own identity, and visible only in the local process list — the
    alternative, writing an auth file, is credential setup this action must
    not do. It is re-derived per image when no longer valid: a copy run can
    outlive a single access token, and an expired token would misreport as a
    permissions failure on every later push.
    """
    replicated, failures = 0, []
    for image in ecr_images:
        if not credentials.valid:
            credentials.refresh(Request())
        dst = dest_refs[image["ref"]]
        # --digestfile makes skopeo report the digest it actually WROTE.
        # Deriving it from the source ref instead would be wrong for an
        # index-pinned image: the default --multi-arch=system copies one
        # platform instance out of a manifest list, so the destination
        # resolves to the instance digest and the source's index digest
        # names a manifest that does not exist there. This is the commit
        # whose thesis is that digests are observed, never invented.
        with tempfile.TemporaryDirectory() as tmp:
            digestfile = os.path.join(tmp, "digest")
            ok, output = _run(
                [skopeo, "copy", "--preserve-digests",
                 "--digestfile", digestfile,
                 "--dest-creds", f"oauth2accesstoken:{credentials.token}",
                 f"docker://{_src_ref(image)}", f"docker://{dst}"],
                COPY_TIMEOUT_S)
            written = ""
            if ok:
                try:
                    with open(digestfile, "r", encoding="utf-8") as f:
                        written = f.read().strip()
                except OSError:
                    written = ""  # an old skopeo without --digestfile support
        if ok:
            replicated += 1
            image["replication"] = {
                "status": "replicated", "destination": dst,
                "verified_by": "skopeo_preserve_digests"}
            if DIGEST_RE.match(written):
                image["replication"]["content_digest"] = written
        else:
            error = output.splitlines()[-1] if output else "no output"
            failures.append((image["ref"], error))
            image["replication"] = {
                "status": "replication_failed", "destination": dst, "error": error}
    return replicated, failures


def _write_runbook(bucket, ecr_images: list, dest_url: str) -> str:
    """One skopeo copy per image, credential steps left to the reader.
    Returns the collision note (empty when no destinations collided)."""
    dest_base = dest_url or "<AR_DESTINATION>"
    dest_refs, collision_note = _plan_destinations(dest_base, ecr_images)
    hosts = sorted({image["ref"].partition("/")[0] for image in ecr_images})
    lines = [
        "#!/bin/sh -e",
        "# Replication runbook — generated by GKE Agentic Migration (self-service mode).",
        "# One `skopeo copy` per ECR image in the discovery inventory, digest-preserving.",
        "#",
    ]
    if not dest_url:
        lines += [
            "# <AR_DESTINATION>: your landing-zone design declares the registry with computed",
            "# Terraform values, so the server cannot resolve its URL. After `terraform apply`",
            "# creates it, replace every <AR_DESTINATION> below with the real registry, e.g.",
            "# us-central1-docker.pkg.dev/<project>/<repository>.",
            "#",
        ]
    lines += [
        "# Before running, authenticate with YOUR credentials:",
        "#   Artifact Registry (push):",
        '#     gcloud auth print-access-token | skopeo login --username oauth2accesstoken --password-stdin \\',
        f"#       {dest_base.partition('/')[0]}",
        "#   ECR (pull), per registry host:",
    ]
    lines += [f"#     {_login_command(host)}" for host in hosts]
    if collision_note:
        lines += ["#" + collision_note, "#"]
    lines.append("")
    for image in ecr_images:
        lines.append("skopeo copy --preserve-digests \\")
        lines.append(f"  docker://{_src_ref(image)} \\")
        lines.append(f"  docker://{dest_refs[image['ref']]}")
        lines.append("")
    bucket.blob(RUNBOOK_BLOB_PATH).upload_from_string(
        "\n".join(lines), content_type="text/plain")
    return collision_note


def replicate_images(variables: dict, config: dict,
                     session: AuthorizedSession = None) -> tuple[str, str]:
    """STATE_DEPLOYMENT_REPLICATE_IMAGES: move the ECR images, or hand over the
    commands to. See the module docstring for the two modes and the
    credential rules."""
    # Consumed once, then removed: the variables persist to the ledger when a
    # failure parks the graph, and a stored agent_mediated opt-in must not
    # survive to be replayed by a later run's bare-accept default (same
    # hygiene as the blocker-owner elicitation in assessment_blockers_2).
    responses = variables.get("elicitation_responses") or {}
    mode = (responses.pop(ELICITATION_STATE, {}) or {}).get("mode", "self_service")

    try:
        bucket = _bucket(config)
        inventory, generation = load_inventory(bucket)
    except Exception as e:
        logger.exception("Image replication could not read the inventory")
        return "on_failure", f"Image replication could not read the inventory: {e}"

    images = (inventory or {}).get("images") or []
    ecr_images = [i for i in images if i.get("registry") == "ecr"]
    non_ecr = len(images) - len(ecr_images)
    unrendered = [t.get("id") for t in (inventory or {}).get("render_targets") or []
                  if t.get("status") != "rendered"]
    remainder_note = (
        f" Remainder: {non_ecr} non-ECR image(s) left where they are; "
        f"{len(unrendered)} render target(s) were never rendered, so their images "
        "are not in the inventory — unverified coverage, not an error.")

    if not ecr_images:
        return "on_success", "No ECR images in the inventory; nothing to replicate." + remainder_note

    resolved = _resolved_destinations(variables)
    dest = resolved[0] if resolved else None
    surplus_note = ""
    if len(resolved) > 1:
        surplus_note = (
            " Note: every image targets the first resolved destination; also "
            "resolved but not targeted: "
            + ", ".join(d["url"] for d in resolved[1:]) + ".")

    if mode == "self_service":
        try:
            collision_note = _write_runbook(
                bucket, ecr_images, dest["url"] if dest else None)
            for image in ecr_images:
                image["replication"] = {"status": "self_service"}
            save_inventory(bucket, inventory, generation)
        except Exception as e:
            logger.exception("Writing the replication runbook failed")
            return "on_failure", f"Writing the replication runbook failed: {e}"
        return ("on_success",
                f"Self-service replication: runbook for {len(ecr_images)} image(s) "
                f"written to {RUNBOOK_BLOB_PATH}; run it with your own credentials."
                + collision_note + surplus_note + remainder_note)

    # agent_mediated from here on. Checks first, and checks only: anything
    # missing is reported with the command the user runs themselves.
    skopeo = shutil.which("skopeo")
    if not skopeo:
        return ("on_failure",
                "skopeo is not installed (or not on PATH). Install it, then re-run "
                "prepare_image_deployment.")
    if dest is None:
        return ("on_failure",
                "Every declared registry is created by your terraform apply and is "
                "not resolvable yet. Apply the migration PR, then re-run "
                "prepare_image_deployment.")
    try:
        credentials = _adc_credentials()
        session = session or AuthorizedSession(credentials)
        problems = _preflight(skopeo, ecr_images, dest, session)
    except Exception as e:
        logger.exception("Replication preflight failed")
        return "on_failure", f"Replication preflight failed (GCP credentials?): {e}"
    if problems:
        return ("on_failure",
                "Replication preflight: " + "; ".join(problems) +
                ". Fix the above yourself (the server never sets up credentials), "
                "then re-run prepare_image_deployment.")

    # The guard exists because the inventory blob is user-writable: a
    # hand-edited entry (schema-valid but e.g. digest without repository)
    # must surface as the same calm on_failure as every other problem, not
    # as a raw exception out of the tool.
    try:
        dest_refs, collision_note = _plan_destinations(dest["url"], ecr_images)
        replicated, failures = _copy_images(skopeo, ecr_images, dest_refs, credentials)
    except Exception as e:
        logger.exception("Image replication stopped unexpectedly")
        return ("on_failure",
                f"Image replication stopped unexpectedly ({e}); no statuses were "
                "recorded. Fix the cause (check the inventory entries), then re-run "
                "prepare_image_deployment.")
    try:
        save_inventory(bucket, inventory, generation)
    except Exception as e:
        logger.exception("Saving replication statuses to the inventory failed")
        return ("on_failure",
                f"Copied {replicated}/{len(ecr_images)} image(s) but saving statuses "
                f"failed: {e}")

    failure_note = ""
    if failures:
        shown = "; ".join(f"{ref}: {err}" for ref, err in failures[:3])
        failure_note = (f" Failed: {len(failures)} "
                        f"({shown}{'; …' if len(failures) > 3 else ''}).")
    summary = (f"Replicated {replicated}/{len(ecr_images)} ECR image(s) to "
               f"{dest['url']}.{failure_note}{collision_note}{surplus_note}{remainder_note}")
    if replicated == 0:
        return "on_failure", summary + " Nothing copied — fix the errors, then re-run prepare_image_deployment."
    return "on_success", summary
