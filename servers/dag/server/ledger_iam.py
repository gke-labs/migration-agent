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

"""Cloud-side access control for the ledger bucket.

The server checks the workspace registry before it reads or writes a prefix, but
that is a client-side check: it binds only callers who go through the server. The
managed folders and IAM bindings established here are what make a 403 come from
GCS itself.

Two things this deliberately does not do. Per-developer grants on
workloads/<component-id>/ are not possible at bootstrap, because no component ID
exists yet. And managed-folder IAM only grants — a principal holding
roles/storage.admin or Owner at the *project* level reaches every prefix
regardless, so prefix isolation is only a real boundary in a project dedicated
to the ledger.

Managed folders are reached over the storage/v1 JSON API rather than the
google-cloud-storage-control client library, which is not installable from every
environment this server is built in. The endpoint and the credentials are the
same ones google-cloud-storage already uses.
"""

import json
import logging
from urllib.parse import quote

import google.auth
from google.auth.transport.requests import AuthorizedSession

logger = logging.getLogger("migration-dag")

STORAGE_API = "https://storage.googleapis.com/storage/v1"
SCOPE = "https://www.googleapis.com/auth/devstorage.full_control"

PLATFORM_FOLDER = "platform/"
WORKLOADS_FOLDER = "workloads/"
LEDGER_FOLDERS = (PLATFORM_FOLDER, WORKLOADS_FOLDER)
REGISTRY_OBJECT = "workspace_registry.yaml"
EXPORTS_OBJECT = "exports.json"

ROLE_BUCKET_ADMIN = "roles/storage.admin"
ROLE_OBJECT_ADMIN = "roles/storage.objectAdmin"
ROLE_OBJECT_VIEWER = "roles/storage.objectViewer"

# Conditions require an IAM policy at version 3. Reading a policy at version 1
# silently drops conditional bindings, so a write-back would delete them.
POLICY_VERSION = 3

SERVICE_ACCOUNT_SUFFIX = ".gserviceaccount.com"


class LedgerIamError(RuntimeError):
    """A managed folder or IAM policy could not be established."""


def provision_ledger_iam(bucket_name: str, roles: dict, session=None) -> None:
    """Creates the ledger's managed folders and grants each role its prefix.

    Idempotent: folders that exist are left alone, and bindings already present
    are not rewritten.
    """
    session = session or _default_session()
    admins = _principals(roles.get("admins"))
    platform = _principals(roles.get("platform_engineers"))
    developers = _principals(roles.get("developers"))

    if not admins:
        raise LedgerIamError("cannot provision ledger IAM without at least one admin")

    for folder in LEDGER_FOLDERS:
        _create_managed_folder(session, bucket_name, folder)

    # Admins hold the bucket, so they need no per-folder binding. Everyone named
    # in the registry may read the registry itself — that is how a client
    # discovers its own role — but nothing else at the bucket root.
    everyone = sorted(set(admins) | set(platform) | set(developers))
    # exports.json is the single platform→developer channel: every member may
    # read it, and platform engineers may create and overwrite it (publication
    # is a whole-object rewrite). Both grants ride in bootstrap because the
    # acting platform engineer at publish time holds neither root write nor
    # storage.setIamPolicy — and a condition may name an object that does not
    # exist yet.
    _merge_policy(session, f"{STORAGE_API}/b/{bucket_name}/iam", [
        {"role": ROLE_BUCKET_ADMIN, "members": admins},
        {
            "role": ROLE_OBJECT_VIEWER,
            "members": everyone,
            "condition": _registry_condition(bucket_name),
        },
        {
            "role": ROLE_OBJECT_VIEWER,
            "members": everyone,
            "condition": _exports_condition(
                bucket_name, "ledger-exports-read",
                "Read limited to the exports object."),
        },
        {
            "role": ROLE_OBJECT_ADMIN,
            "members": platform,
            "condition": _exports_condition(
                bucket_name, "ledger-exports-write",
                "Publish rights limited to the exports object."),
        },
    ])

    if platform:
        _merge_policy(session, _folder_iam_url(bucket_name, PLATFORM_FOLDER),
                      [{"role": ROLE_OBJECT_ADMIN, "members": platform}])

    # workloads/ is created but left unbound: its subfolders are granted per
    # component when a developer joins, which is not something bootstrap knows.
    logger.info(f"Ledger IAM provisioned on gs://{bucket_name}: "
                f"{len(admins)} admin(s), {len(platform)} platform engineer(s), "
                f"{len(developers)} developer(s)")


def revoke_ledger_member(bucket_name: str, email: str, session=None) -> int:
    """Removes one registry email from every grant bootstrap made for it.

    The bucket policy and the platform/ managed folder are the two places
    provision_ledger_iam binds registry members; both are rewritten without
    the principal. Per-component managed folders under workloads/ are bound
    by an admin out of band and are left alone — the server never granted
    them, so it does not know which exist. Returns how many bindings lost the
    member. A principal that was never bound (a grant that failed before it
    landed) counts as zero and is not an error: the registry entry is what
    the caller is removing, and the cloud side already agrees.
    """
    session = session or _default_session()
    principals = set(_principals([email]))
    removed = 0
    # ROLE_BUCKET_ADMIN is never revoked: it is the admins' hold on the whole
    # bucket, granted from the registry's admin list and fixed at bootstrap
    # (an admin removed by mistake could not undo it). A team member being
    # revoked never legitimately holds it; an admin dual-listed under a team,
    # or an out-of-band bucket owner not in the registry, must not lose it
    # here just because their email flows through a revoke.
    for iam_url in (f"{STORAGE_API}/b/{bucket_name}/iam",
                    _folder_iam_url(bucket_name, PLATFORM_FOLDER)):
        removed += _drop_members(session, iam_url, principals,
                                 protected_roles=frozenset({ROLE_BUCKET_ADMIN}))
    logger.info(f"Revoked ledger access for {email} on gs://{bucket_name}: "
                f"{removed} binding(s) changed")
    return removed


def _drop_members(session, iam_url: str, principals: set,
                  protected_roles: frozenset = frozenset()) -> int:
    """Removes principals from every binding at iam_url. Returns bindings changed.

    Bindings whose role is in protected_roles are left exactly as they are —
    the revoke does not touch them. A binding left empty is deleted rather
    than written back with no members, which the API rejects. Same 412 retry
    as the merge: someone else's concurrent write is re-read once and the
    drop re-applied to it.
    """
    for attempt in (1, 2):
        try:
            policy = _get_policy(session, iam_url)
        except LedgerIamError as e:
            if "404" in str(e):
                return 0  # the managed folder was never created; nothing bound
            raise
        changed = 0
        kept = []
        for binding in policy["bindings"]:
            if binding.get("role") in protected_roles:
                kept.append(binding)
                continue
            members = [m for m in binding.get("members", []) if m not in principals]
            if len(members) != len(binding.get("members", [])):
                changed += 1
            if members:
                kept.append({**binding, "members": members})
        if not changed:
            return 0
        policy["bindings"] = kept
        policy["version"] = POLICY_VERSION
        response = session.request("PUT", iam_url, json=policy)
        if response.status_code == 412:
            if attempt == 1:
                logger.debug(f"Policy at {iam_url} changed under us; re-reading and retrying")
                continue
            raise LedgerIamError(
                f"failed to set IAM policy at {iam_url}: still conflicting after a retry. "
                f"Someone else is editing this policy; retry once they are done.")
        _raise_unless_ok(response, f"set IAM policy at {iam_url}", "storage.buckets.setIamPolicy")
        return changed
    return 0


def _default_session() -> AuthorizedSession:
    credentials, _ = google.auth.default(scopes=[SCOPE])
    return AuthorizedSession(credentials)


def _principals(emails) -> list:
    """Turns registry emails into IAM principals, preserving explicit prefixes."""
    principals = []
    for email in emails or []:
        email = (email or "").strip()
        if not email:
            continue
        if ":" in email:
            # Already qualified (group:, domain:, serviceAccount:, user:).
            principals.append(email)
        elif email.endswith(SERVICE_ACCOUNT_SUFFIX):
            principals.append(f"serviceAccount:{email}")
        else:
            principals.append(f"user:{email}")
    return sorted(set(principals))


def _folder_iam_url(bucket_name: str, folder_id: str) -> str:
    return f"{STORAGE_API}/b/{bucket_name}/managedFolders/{quote(folder_id, safe='')}/iam"


def _registry_condition(bucket_name: str) -> dict:
    return {
        "title": "ledger-registry-only",
        "description": "Read limited to the workspace registry object.",
        "expression": (
            f'resource.name == "projects/_/buckets/{bucket_name}'
            f'/objects/{REGISTRY_OBJECT}"'
        ),
    }


def _exports_condition(bucket_name: str, title: str, description: str) -> dict:
    return {
        "title": title,
        "description": description,
        "expression": (
            f'resource.name == "projects/_/buckets/{bucket_name}'
            f'/objects/{EXPORTS_OBJECT}"'
        ),
    }


def _create_managed_folder(session, bucket_name: str, folder_id: str) -> bool:
    """Returns True if the folder was created, False if it already existed."""
    response = session.request(
        "POST", f"{STORAGE_API}/b/{bucket_name}/managedFolders",
        json={"name": folder_id},
    )
    if response.status_code == 409:
        logger.debug(f"Managed folder {folder_id} already exists on gs://{bucket_name}")
        return False
    _raise_unless_ok(response, f"create managed folder {folder_id} on gs://{bucket_name}",
                     "storage.managedFolders.create")
    logger.debug(f"Created managed folder {folder_id} on gs://{bucket_name}")
    return True


def _merge_policy(session, iam_url: str, wanted: list) -> bool:
    """Adds members to the policy at iam_url, leaving existing bindings intact.

    Returns True if a write happened. A 412 means someone else wrote the policy
    between the read and the write, so the merge is retried once against the
    policy they left behind.
    """
    for attempt in (1, 2):
        policy = _get_policy(session, iam_url)
        if not _add_members(policy, wanted):
            logger.debug(f"Policy at {iam_url} already grants everything required")
            return False

        policy["version"] = POLICY_VERSION
        response = session.request("PUT", iam_url, json=policy)
        if response.status_code == 412:
            if attempt == 1:
                logger.debug(f"Policy at {iam_url} changed under us; re-reading and retrying")
                continue
            raise LedgerIamError(
                f"failed to set IAM policy at {iam_url}: still conflicting after a retry. "
                f"Someone else is editing this policy; re-run bootstrap once they are done.")
        _raise_unless_ok(response, f"set IAM policy at {iam_url}", "storage.buckets.setIamPolicy")
        return True


def _get_policy(session, iam_url: str) -> dict:
    response = session.request(
        "GET", iam_url, params={"optionsRequestedPolicyVersion": POLICY_VERSION})
    _raise_unless_ok(response, f"read IAM policy at {iam_url}", "storage.buckets.getIamPolicy")
    policy = response.json()
    policy.setdefault("bindings", [])
    return policy


def _add_members(policy: dict, wanted: list) -> bool:
    """Merges wanted bindings into policy in place. Returns True if anything changed."""
    changed = False
    for binding in wanted:
        members = binding.get("members") or []
        if not members:
            continue
        existing = _find_binding(policy["bindings"], binding)
        if existing is None:
            policy["bindings"].append({
                "role": binding["role"],
                "members": sorted(members),
                **({"condition": binding["condition"]} if binding.get("condition") else {}),
            })
            changed = True
            continue
        missing = [m for m in members if m not in existing.get("members", [])]
        if missing:
            existing["members"] = sorted(set(existing.get("members", [])) | set(missing))
            changed = True
    return changed


def _find_binding(bindings: list, wanted: dict):
    """A binding is identified by its role *and* its condition, not the role alone."""
    wanted_expression = (wanted.get("condition") or {}).get("expression")
    for binding in bindings:
        if binding.get("role") != wanted["role"]:
            continue
        if (binding.get("condition") or {}).get("expression") == wanted_expression:
            return binding
    return None


def _raise_unless_ok(response, action: str, permission: str) -> None:
    if 200 <= response.status_code < 300:
        return
    detail = _error_detail(response)
    if response.status_code in (401, 403):
        raise LedgerIamError(
            f"failed to {action}: {response.status_code} {detail}. "
            f"The account running the migration agent needs '{permission}' "
            f"(granted by roles/storage.admin) on the ledger bucket.")
    raise LedgerIamError(f"failed to {action}: {response.status_code} {detail}")


def _error_detail(response) -> str:
    try:
        body = response.json()
    except (ValueError, json.JSONDecodeError):
        return (getattr(response, "text", "") or "").strip()[:200]
    error = body.get("error")
    if isinstance(error, dict):
        return error.get("message", "")
    return str(error or body)[:200]
