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

"""Deployment-stage engine actions: prepare what the applied estate needs
before workloads run.

`provision_artifact_registry` backs STATE_DEPLOYMENT_PROVISION_AR, the first
deployment state. It resolves the Artifact Registry destination the migrated
images will land in — the design's own registry when the landing-zone clone
declares one, a deterministic default otherwise — then checks it exists and
creates it via the Artifact Registry API when it does not, the same way
onboarding creates the SSM repository. Deliberately no elicitation: creating
an empty docker repository is cheap, idempotent, and required before any
image can be replicated; the resolved destination is recorded to the ledger
(`artifact_registry_destinations`) and the outcome to `history`, so the audit
trail is the record. Failures never block: the migration PR is already
shipped and a registry problem is fixable later, so both legs drain to
STATE_DEPLOYMENT_DATA_MIGRATION with the reason in the message.
"""

import logging
import os
import re

import google.auth
from google.auth.transport.requests import AuthorizedSession

from servers.phases.deployment.replication import replicate_images

logger = logging.getLogger("migration-dag")

AR_API = "https://artifactregistry.googleapis.com/v1"
SCOPE = "https://www.googleapis.com/auth/cloud-platform"

# Where the default repository lands when the design declared none. The
# location has no source-side analogue to derive from (an ECR region says
# nothing about the GCP region the user wants), so it is configuration with
# a conventional default rather than a guess.
DEFAULT_LOCATION_ENV = "GKE_AGENTIC_MIGRATION_AR_LOCATION"
DEFAULT_LOCATION = "us-central1"

_AR_RESOURCE_RE = re.compile(
    r'resource\s+"google_artifact_registry_repository"\s+"[^"]+"\s*\{')

# A literal string assignment only: values carrying interpolation or
# references are design-time unknowns and are recorded as null, not guessed.
_LITERAL_ATTR_RE = r'^\s*{attr}\s*=\s*"([^"${{}}]*)"\s*$'


def _slug(text: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", str(text or "").lower()).strip("-")
    return slug or "gke-agentic-migration-images"


def _resource_body(text: str, open_brace: int) -> str:
    """Returns the brace-matched body starting at text[open_brace] == '{'."""
    depth = 0
    for i in range(open_brace, len(text)):
        if text[i] == "{":
            depth += 1
        elif text[i] == "}":
            depth -= 1
            if depth == 0:
                return text[open_brace + 1:i]
    return text[open_brace + 1:]


def _literal_attr(body: str, attr: str):
    m = re.search(_LITERAL_ATTR_RE.format(attr=attr), body, re.MULTILINE)
    return m.group(1) if m else None


def scan_artifact_registry_destinations(clone_dir: str) -> list:
    """Finds every google_artifact_registry_repository declared in the clone.

    Returns [{project, location, repository, url}] in file-walk order. Fields
    are the literal strings written in the HCL; anything computed (variables,
    interpolation) is None, and url is only composed when all three parts are
    literal — a partial record means "created by the user's terraform apply",
    not an error.
    """
    destinations = []
    for dirpath, dirnames, filenames in os.walk(clone_dir):
        dirnames[:] = sorted(d for d in dirnames if d not in (".git", ".terraform"))
        for name in sorted(filenames):
            if not name.endswith(".tf"):
                continue
            try:
                with open(os.path.join(dirpath, name), "r") as f:
                    text = f.read()
            except OSError:
                continue
            for match in _AR_RESOURCE_RE.finditer(text):
                body = _resource_body(text, match.end() - 1)
                project = _literal_attr(body, "project")
                location = _literal_attr(body, "location")
                repository = _literal_attr(body, "repository_id")
                url = None
                if project and location and repository:
                    url = f"{location}-docker.pkg.dev/{project}/{repository}"
                destinations.append({
                    "project": project,
                    "location": location,
                    "repository": repository,
                    "url": url,
                })
    return destinations


_CLUSTER_RESOURCE_RE = re.compile(
    r'resource\s+"google_container_cluster"\s+"[^"]+"\s*\{')


def scan_container_clusters(clone_dir: str) -> list:
    """Every google_container_cluster declared in the clone, literals only.

    Same discipline as the registry scan above: returns [{name, location,
    project}] in file-walk order, with computed values recorded as None —
    the exports derivation never guesses what terraform apply will evaluate.
    """
    clusters = []
    for dirpath, dirnames, filenames in os.walk(clone_dir):
        dirnames[:] = sorted(d for d in dirnames if d not in (".git", ".terraform"))
        for name in sorted(filenames):
            if not name.endswith(".tf"):
                continue
            try:
                with open(os.path.join(dirpath, name), "r") as f:
                    text = f.read()
            except OSError:
                continue
            for match in _CLUSTER_RESOURCE_RE.finditer(text):
                body = _resource_body(text, match.end() - 1)
                clusters.append({
                    "name": _literal_attr(body, "name"),
                    "location": _literal_attr(body, "location"),
                    "project": _literal_attr(body, "project"),
                })
    return clusters


def _default_destination(config: dict) -> dict:
    project = config.get("gcp_project")
    location = os.environ.get(DEFAULT_LOCATION_ENV) or DEFAULT_LOCATION
    repository = _slug(config.get("workspace_name"))
    return {
        "project": project,
        "location": location,
        "repository": repository,
        "url": f"{location}-docker.pkg.dev/{project}/{repository}",
    }


def resolve_destinations(variables: dict, config: dict) -> tuple[list, str]:
    """The destination contract, in precedence order: what the ledger already
    records, what the landing-zone design wrote, then the deterministic
    default. Returns (destinations, source_note)."""
    recorded = variables.get("artifact_registry_destinations")
    if recorded:
        return recorded, "recorded in the ledger"

    clone_dir = variables.get("target_clone_path")
    if clone_dir and os.path.isdir(clone_dir):
        scanned = scan_artifact_registry_destinations(clone_dir)
        if scanned:
            return scanned, "declared in the landing-zone design"

    return [_default_destination(config)], "default (the design declares no registry)"


def _default_session() -> AuthorizedSession:
    credentials, _ = google.auth.default(scopes=[SCOPE])
    return AuthorizedSession(credentials)


def _repository_resource(dest: dict) -> str:
    return (f"projects/{dest['project']}/locations/{dest['location']}"
            f"/repositories/{dest['repository']}")


def _ensure_repository(session: AuthorizedSession, dest: dict) -> str:
    """Returns 'exists' or 'created'; raises on anything else."""
    url = f"{AR_API}/{_repository_resource(dest)}"
    res = session.get(url)
    if res.status_code == 200:
        return "exists"
    if res.status_code != 404:
        raise RuntimeError(
            f"checking failed: HTTP {res.status_code}: {res.text}")

    parent = f"projects/{dest['project']}/locations/{dest['location']}"
    res = session.post(
        f"{AR_API}/{parent}/repositories",
        params={"repositoryId": dest["repository"]},
        json={
            "format": "DOCKER",
            "description": ("Container images migrated from the source EKS "
                            "estate (GKE Agentic Migration)."),
        },
    )
    if res.status_code not in (200, 201):
        raise RuntimeError(
            f"creation failed: HTTP {res.status_code}: {res.text}")
    # Creation returns a long-running operation; docker repositories settle in
    # seconds and the replication step re-checks existence anyway, so the LRO
    # is not polled here.
    return "created"


def provision_artifact_registry(variables: dict, config: dict,
                                session: AuthorizedSession = None) -> tuple[str, str]:
    """STATE_DEPLOYMENT_PROVISION_AR: make sure the image destination exists.

    Resolves the destination, ensures every fully-resolved entry exists
    (creating it when missing), and records the resolved list to the ledger.
    Entries with computed fields belong to the user's terraform apply and are
    reported, not created. on_failure is reserved for nothing usable at all —
    and drains to the same terminal, carrying the reason.
    """
    destinations, source_note = resolve_destinations(variables, config)
    variables["artifact_registry_destinations"] = destinations

    try:
        session = session or _default_session()
    except Exception as e:
        logger.exception("No GCP credentials for Artifact Registry provisioning")
        return ("on_failure",
                f"Artifact Registry not verified (no GCP credentials: {e}). "
                f"Destination {source_note}: "
                f"{', '.join(d['url'] or d['repository'] or 'unresolved' for d in destinations)}. "
                "Create or verify it before image replication.")

    outcomes = []
    failures = []
    for dest in destinations:
        if not dest.get("url"):
            outcomes.append(
                f"{dest.get('repository') or 'unresolved registry'}: declared with "
                "computed values — created by your terraform apply, not the server")
            continue
        try:
            outcomes.append(f"{dest['url']}: {_ensure_repository(session, dest)}")
        except Exception as e:
            logger.exception(f"Artifact Registry provisioning failed for {dest['url']}")
            failures.append(f"{dest['url']}: {e}")

    summary = (f"Artifact Registry destination {source_note}. "
               + "; ".join(outcomes + failures))
    if failures and len(failures) == len([d for d in destinations if d.get("url")]):
        return ("on_failure",
                summary + " — fix access, then re-verify before image replication.")
    return "on_success", summary


# Reached by main.run_internal_mutation, keyed by the `action` field of the
# deployment INTERNAL_TASK_SERVER_MUTATION states.
ACTIONS = {
    "provision_artifact_registry": provision_artifact_registry,
    "replicate_images": replicate_images,
}
