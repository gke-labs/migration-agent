import copy
import unittest

from server import ledger_iam
from server.ledger_iam import LedgerIamError

BUCKET = "test-ledger"
BUCKET_IAM = f"{ledger_iam.STORAGE_API}/b/{BUCKET}/iam"
PLATFORM_IAM = f"{ledger_iam.STORAGE_API}/b/{BUCKET}/managedFolders/platform%2F/iam"
WORKLOADS_IAM = f"{ledger_iam.STORAGE_API}/b/{BUCKET}/managedFolders/workloads%2F/iam"
FOLDERS_URL = f"{ledger_iam.STORAGE_API}/b/{BUCKET}/managedFolders"

ROLES = {
    "admins": ["ada@example.com"],
    "platform_engineers": ["pat@example.com"],
    "developers": ["dev@example.com"],
}


class FakeResponse:
    def __init__(self, status_code, body=None, text=""):
        self.status_code = status_code
        self._body = body
        self.text = text

    def json(self):
        if self._body is None:
            raise ValueError("no json body")
        return self._body


class FakeStorage:
    """Enough of the storage/v1 IAM surface to hold a policy and answer a read.

    Records every call so a test can assert on what was sent, not only on where
    the fake ended up.
    """

    def __init__(self):
        self.folders = []
        self.policies = {}
        self.calls = []
        self.scripted = {}  # (method, url) -> list of FakeResponse, consumed in order

    def script(self, method, url, *responses):
        self.scripted.setdefault((method, url), []).extend(responses)

    def request(self, method, url, json=None, params=None):
        self.calls.append({"method": method, "url": url, "json": json, "params": params})

        queued = self.scripted.get((method, url))
        if queued:
            return queued.pop(0)

        if method == "POST" and url == FOLDERS_URL:
            name = json["name"]
            if name in self.folders:
                return FakeResponse(409, {"error": {"message": "already exists"}})
            self.folders.append(name)
            return FakeResponse(200, {"name": name})

        if method == "GET" and url.endswith("/iam"):
            # A fresh parse per read, as the real client gives: a caller that
            # edits the policy it read must not edit the fake's stored copy.
            return FakeResponse(200, copy.deepcopy(self.policies.get(url, self._empty_policy(url))))

        if method == "PUT" and url.endswith("/iam"):
            stored = self.policies.get(url, self._empty_policy(url))
            if json.get("etag") != stored["etag"]:
                return FakeResponse(412, {"error": {"message": "etag mismatch"}})
            written = dict(json)
            written["etag"] = stored["etag"] + "+"
            self.policies[url] = written
            return FakeResponse(200, written)

        raise AssertionError(f"unexpected request: {method} {url}")

    @staticmethod
    def _empty_policy(url):
        return {"kind": "storage#policy", "resourceId": url, "version": 1,
                "etag": "ETAG0", "bindings": []}

    # -- assertions helpers -------------------------------------------------

    def binding(self, url, role, expression=None):
        for b in self.policies.get(url, {}).get("bindings", []):
            if b["role"] == role and (b.get("condition") or {}).get("expression") == expression:
                return b
        return None

    def written(self, method, url):
        return [c for c in self.calls if c["method"] == method and c["url"] == url]


class ManagedFolderTest(unittest.TestCase):

    def test_creates_the_platform_and_workloads_folders(self):
        fake = FakeStorage()
        ledger_iam.provision_ledger_iam(BUCKET, ROLES, session=fake)
        self.assertEqual(fake.folders, ["platform/", "workloads/"])

    def test_an_existing_folder_is_not_an_error(self):
        fake = FakeStorage()
        fake.folders.append("platform/")
        ledger_iam.provision_ledger_iam(BUCKET, ROLES, session=fake)
        self.assertEqual(fake.folders, ["platform/", "workloads/"])

    def test_folder_id_is_url_encoded_in_the_iam_path(self):
        fake = FakeStorage()
        ledger_iam.provision_ledger_iam(BUCKET, ROLES, session=fake)
        self.assertIn(PLATFORM_IAM, [c["url"] for c in fake.calls],
                      "the trailing slash in 'platform/' must be percent-encoded")

    def test_a_folder_that_cannot_be_created_aborts(self):
        fake = FakeStorage()
        fake.script("POST", FOLDERS_URL, FakeResponse(500, {"error": {"message": "backend error"}}))
        with self.assertRaises(LedgerIamError) as caught:
            ledger_iam.provision_ledger_iam(BUCKET, ROLES, session=fake)
        self.assertIn("create managed folder platform/", str(caught.exception))


class BucketPolicyTest(unittest.TestCase):

    def setUp(self):
        self.fake = FakeStorage()

    def test_admins_get_storage_admin_on_the_bucket(self):
        ledger_iam.provision_ledger_iam(BUCKET, ROLES, session=self.fake)
        binding = self.fake.binding(BUCKET_IAM, "roles/storage.admin")
        self.assertEqual(binding["members"], ["user:ada@example.com"])

    def test_policy_is_written_at_version_3(self):
        ledger_iam.provision_ledger_iam(BUCKET, ROLES, session=self.fake)
        self.assertEqual(self.fake.policies[BUCKET_IAM]["version"], 3,
                         "conditional bindings are rejected below version 3")

    def test_policy_is_read_at_version_3(self):
        ledger_iam.provision_ledger_iam(BUCKET, ROLES, session=self.fake)
        read = self.fake.written("GET", BUCKET_IAM)[0]
        self.assertEqual(read["params"], {"optionsRequestedPolicyVersion": 3},
                         "a version-1 read drops conditional bindings, and the "
                         "write-back would then delete them")

    def test_registry_read_is_conditional_on_exactly_the_registry_object(self):
        ledger_iam.provision_ledger_iam(BUCKET, ROLES, session=self.fake)
        expression = (f'resource.name == "projects/_/buckets/{BUCKET}'
                      f'/objects/workspace_registry.yaml"')
        binding = self.fake.binding(BUCKET_IAM, "roles/storage.objectViewer", expression)
        self.assertIsNotNone(binding, "the registry grant must be scoped by a condition")
        self.assertEqual(binding["members"],
                         ["user:ada@example.com", "user:dev@example.com", "user:pat@example.com"])

    def test_no_unconditional_read_is_granted_at_the_bucket_root(self):
        ledger_iam.provision_ledger_iam(BUCKET, ROLES, session=self.fake)
        self.assertIsNone(self.fake.binding(BUCKET_IAM, "roles/storage.objectViewer"),
                          "an unconditional viewer binding would expose every prefix")

    def test_existing_unrelated_bindings_are_preserved(self):
        self.fake.policies[BUCKET_IAM] = {
            "etag": "ETAG0", "version": 3,
            "bindings": [{"role": "roles/storage.legacyBucketReader",
                          "members": ["user:audit@example.com"]}],
        }
        ledger_iam.provision_ledger_iam(BUCKET, ROLES, session=self.fake)
        self.assertIsNotNone(self.fake.binding(BUCKET_IAM, "roles/storage.legacyBucketReader"))
        self.assertIsNotNone(
            self.fake.binding(BUCKET_IAM, "roles/storage.objectViewer", EXPORTS_EXPRESSION),
            "the exports read grant must survive a merge with pre-existing bindings")
        self.assertIsNotNone(
            self.fake.binding(BUCKET_IAM, "roles/storage.objectAdmin", EXPORTS_EXPRESSION),
            "the exports write grant must survive a merge with pre-existing bindings")

    def test_members_are_added_to_an_existing_binding_rather_than_replacing_it(self):
        self.fake.policies[BUCKET_IAM] = {
            "etag": "ETAG0", "version": 3,
            "bindings": [{"role": "roles/storage.admin", "members": ["user:sre@example.com"]}],
        }
        ledger_iam.provision_ledger_iam(BUCKET, ROLES, session=self.fake)
        self.assertEqual(self.fake.binding(BUCKET_IAM, "roles/storage.admin")["members"],
                         ["user:ada@example.com", "user:sre@example.com"])

    def test_the_etag_that_was_read_is_sent_back(self):
        ledger_iam.provision_ledger_iam(BUCKET, ROLES, session=self.fake)
        self.assertEqual(self.fake.written("PUT", BUCKET_IAM)[0]["json"]["etag"], "ETAG0")


EXPORTS_EXPRESSION = (f'resource.name == "projects/_/buckets/{BUCKET}'
                      f'/objects/exports.json"')


class ExportsPolicyTest(unittest.TestCase):

    def setUp(self):
        self.fake = FakeStorage()

    def test_every_member_may_read_exactly_the_exports_object(self):
        ledger_iam.provision_ledger_iam(BUCKET, ROLES, session=self.fake)
        binding = self.fake.binding(BUCKET_IAM, "roles/storage.objectViewer",
                                    EXPORTS_EXPRESSION)
        self.assertIsNotNone(binding, "the exports grant must be scoped by a condition")
        self.assertEqual(binding["members"],
                         ["user:ada@example.com", "user:dev@example.com", "user:pat@example.com"])

    def test_platform_engineers_may_write_exactly_the_exports_object(self):
        ledger_iam.provision_ledger_iam(BUCKET, ROLES, session=self.fake)
        binding = self.fake.binding(BUCKET_IAM, "roles/storage.objectAdmin",
                                    EXPORTS_EXPRESSION)
        self.assertIsNotNone(binding, "publish is a whole-object rewrite; platform "
                                      "engineers need create + overwrite on it")
        self.assertEqual(binding["members"], ["user:pat@example.com"])

    def test_no_unconditional_object_admin_is_granted_at_the_bucket_root(self):
        ledger_iam.provision_ledger_iam(BUCKET, ROLES, session=self.fake)
        self.assertIsNone(self.fake.binding(BUCKET_IAM, "roles/storage.objectAdmin"),
                          "an unconditional objectAdmin binding would expose every prefix")

    def test_no_exports_write_binding_without_platform_engineers(self):
        roles = dict(ROLES, platform_engineers=[])
        ledger_iam.provision_ledger_iam(BUCKET, roles, session=self.fake)
        self.assertIsNone(self.fake.binding(BUCKET_IAM, "roles/storage.objectAdmin",
                                            EXPORTS_EXPRESSION))


class ManagedFolderPolicyTest(unittest.TestCase):

    def setUp(self):
        self.fake = FakeStorage()

    def test_platform_engineers_get_object_admin_on_the_platform_folder(self):
        ledger_iam.provision_ledger_iam(BUCKET, ROLES, session=self.fake)
        binding = self.fake.binding(PLATFORM_IAM, "roles/storage.objectAdmin")
        self.assertEqual(binding["members"], ["user:pat@example.com"])

    def test_the_workloads_folder_is_left_unbound(self):
        ledger_iam.provision_ledger_iam(BUCKET, ROLES, session=self.fake)
        self.assertNotIn(WORKLOADS_IAM, self.fake.policies,
                         "per-component grants belong to the developer join, not bootstrap")

    def test_developers_get_nothing_on_the_platform_folder(self):
        ledger_iam.provision_ledger_iam(BUCKET, ROLES, session=self.fake)
        members = self.fake.binding(PLATFORM_IAM, "roles/storage.objectAdmin")["members"]
        self.assertNotIn("user:dev@example.com", members)

    def test_admins_get_no_folder_binding(self):
        ledger_iam.provision_ledger_iam(BUCKET, ROLES, session=self.fake)
        members = self.fake.binding(PLATFORM_IAM, "roles/storage.objectAdmin")["members"]
        self.assertNotIn("user:ada@example.com", members,
                         "roles/storage.admin on the bucket already covers every prefix")

    def test_no_folder_policy_is_written_without_platform_engineers(self):
        roles = dict(ROLES, platform_engineers=[])
        ledger_iam.provision_ledger_iam(BUCKET, roles, session=self.fake)
        self.assertEqual(self.fake.written("PUT", PLATFORM_IAM), [])


class PrincipalTest(unittest.TestCase):

    def test_a_service_account_email_becomes_a_service_account_principal(self):
        fake = FakeStorage()
        roles = dict(ROLES, admins=["agent@my-proj.iam.gserviceaccount.com"])
        ledger_iam.provision_ledger_iam(BUCKET, roles, session=fake)
        self.assertEqual(fake.binding(BUCKET_IAM, "roles/storage.admin")["members"],
                         ["serviceAccount:agent@my-proj.iam.gserviceaccount.com"])

    def test_an_already_qualified_principal_is_left_alone(self):
        fake = FakeStorage()
        roles = dict(ROLES, platform_engineers=["group:platform@example.com"])
        ledger_iam.provision_ledger_iam(BUCKET, roles, session=fake)
        self.assertEqual(fake.binding(PLATFORM_IAM, "roles/storage.objectAdmin")["members"],
                         ["group:platform@example.com"])

    def test_blank_entries_are_dropped(self):
        fake = FakeStorage()
        roles = dict(ROLES, developers=["", "  ", None])
        ledger_iam.provision_ledger_iam(BUCKET, roles, session=fake)
        expression = (f'resource.name == "projects/_/buckets/{BUCKET}'
                      f'/objects/workspace_registry.yaml"')
        members = fake.binding(BUCKET_IAM, "roles/storage.objectViewer", expression)["members"]
        self.assertEqual(members, ["user:ada@example.com", "user:pat@example.com"])

    def test_provisioning_without_an_admin_is_refused(self):
        fake = FakeStorage()
        with self.assertRaises(LedgerIamError):
            ledger_iam.provision_ledger_iam(BUCKET, dict(ROLES, admins=[]), session=fake)


class ConflictAndFailureTest(unittest.TestCase):

    def test_nothing_is_written_when_every_grant_is_already_present(self):
        fake = FakeStorage()
        ledger_iam.provision_ledger_iam(BUCKET, ROLES, session=fake)
        writes = len(fake.written("PUT", BUCKET_IAM))

        second = FakeStorage()
        second.policies = fake.policies
        second.folders = list(fake.folders)
        ledger_iam.provision_ledger_iam(BUCKET, ROLES, session=second)

        self.assertEqual(writes, 1)
        self.assertEqual(second.written("PUT", BUCKET_IAM), [],
                         "re-running bootstrap should not rewrite an unchanged policy")

    def test_a_concurrent_write_is_retried_once(self):
        fake = FakeStorage()
        fake.script("PUT", BUCKET_IAM, FakeResponse(412, {"error": {"message": "conflict"}}))
        ledger_iam.provision_ledger_iam(BUCKET, ROLES, session=fake)
        self.assertEqual(len(fake.written("PUT", BUCKET_IAM)), 2)
        self.assertIsNotNone(fake.binding(BUCKET_IAM, "roles/storage.admin"))

    def test_a_persistent_conflict_gives_up(self):
        fake = FakeStorage()
        conflict = {"error": {"message": "conflict"}}
        fake.script("PUT", BUCKET_IAM,
                    FakeResponse(412, conflict), FakeResponse(412, conflict))
        with self.assertRaises(LedgerIamError) as caught:
            ledger_iam.provision_ledger_iam(BUCKET, ROLES, session=fake)
        self.assertIn("still conflicting", str(caught.exception))

    def test_permission_denied_names_the_permission_that_is_missing(self):
        fake = FakeStorage()
        fake.script("PUT", BUCKET_IAM,
                    FakeResponse(403, {"error": {"message": "caller lacks permission"}}))
        with self.assertRaises(LedgerIamError) as caught:
            ledger_iam.provision_ledger_iam(BUCKET, ROLES, session=fake)
        message = str(caught.exception)
        self.assertIn("storage.buckets.setIamPolicy", message)
        self.assertIn("roles/storage.admin", message)

    def test_a_non_json_error_body_still_produces_a_message(self):
        fake = FakeStorage()
        fake.script("GET", BUCKET_IAM, FakeResponse(502, None, text="<html>bad gateway</html>"))
        with self.assertRaises(LedgerIamError) as caught:
            ledger_iam.provision_ledger_iam(BUCKET, ROLES, session=fake)
        self.assertIn("502", str(caught.exception))


if __name__ == "__main__":
    unittest.main()


class RevokeTest(unittest.TestCase):
    """revoke_ledger_member: the counterpart to a grant, for a member removed
    from the registry. Bucket policy and platform/ folder, nothing else."""

    def setUp(self):
        self.fake = FakeStorage()
        ledger_iam.provision_ledger_iam(BUCKET, ROLES, session=self.fake)
        self.fake.calls.clear()

    def members(self, url, role, expression=None):
        binding = self.fake.binding(url, role, expression)
        return binding["members"] if binding else None

    def test_a_platform_engineer_loses_every_binding_bootstrap_made(self):
        registry_expr = ledger_iam._registry_condition(BUCKET)["expression"]
        exports_expr = ledger_iam._exports_condition(BUCKET, "t", "d")["expression"]
        self.assertIn("user:pat@example.com", self.members(PLATFORM_IAM, "roles/storage.objectAdmin"))

        changed = ledger_iam.revoke_ledger_member(BUCKET, "pat@example.com", session=self.fake)

        # registry read, exports read, exports write, platform/ objectAdmin
        self.assertEqual(changed, 4)
        self.assertNotIn("user:pat@example.com",
                         self.members(BUCKET_IAM, "roles/storage.objectViewer", registry_expr))
        self.assertNotIn("user:pat@example.com",
                         self.members(BUCKET_IAM, "roles/storage.objectViewer", exports_expr))
        # pat was the only platform engineer: the emptied bindings are gone,
        # not written back with no members (the API rejects that).
        self.assertIsNone(self.members(BUCKET_IAM, "roles/storage.objectAdmin", exports_expr))
        self.assertIsNone(self.members(PLATFORM_IAM, "roles/storage.objectAdmin"))

    def test_other_members_keep_their_grants(self):
        registry_expr = ledger_iam._registry_condition(BUCKET)["expression"]
        ledger_iam.revoke_ledger_member(BUCKET, "dev@example.com", session=self.fake)
        self.assertEqual(self.members(BUCKET_IAM, "roles/storage.admin"), ["user:ada@example.com"])
        self.assertEqual(sorted(self.members(BUCKET_IAM, "roles/storage.objectViewer", registry_expr)),
                         ["user:ada@example.com", "user:pat@example.com"])
        self.assertEqual(self.members(PLATFORM_IAM, "roles/storage.objectAdmin"), ["user:pat@example.com"])

    def test_a_member_that_was_never_bound_is_a_no_op_not_an_error(self):
        # The friction-log case: the registry entry exists but the grant for it
        # failed, so there is nothing cloud-side to undo — and no PUT is sent.
        changed = ledger_iam.revoke_ledger_member(BUCKET, "priya@acme.example", session=self.fake)
        self.assertEqual(changed, 0)
        self.assertEqual(self.fake.written("PUT", BUCKET_IAM), [])
        self.assertEqual(self.fake.written("PUT", PLATFORM_IAM), [])

    def test_a_service_account_is_matched_by_its_qualified_principal(self):
        sa = "robot@proj.iam.gserviceaccount.com"
        ledger_iam.provision_ledger_iam(
            BUCKET, {"admins": ["ada@example.com"], "developers": [sa]}, session=self.fake)
        registry_expr = ledger_iam._registry_condition(BUCKET)["expression"]
        self.assertIn(f"serviceAccount:{sa}",
                      self.members(BUCKET_IAM, "roles/storage.objectViewer", registry_expr))
        ledger_iam.revoke_ledger_member(BUCKET, sa, session=self.fake)
        self.assertNotIn(f"serviceAccount:{sa}",
                         self.members(BUCKET_IAM, "roles/storage.objectViewer", registry_expr))

    def test_a_concurrent_policy_write_is_retried_once(self):
        self.fake.script("PUT", BUCKET_IAM, FakeResponse(412, {"error": {"message": "etag mismatch"}}))
        changed = ledger_iam.revoke_ledger_member(BUCKET, "dev@example.com", session=self.fake)
        self.assertGreater(changed, 0)
        self.assertEqual(len(self.fake.written("PUT", BUCKET_IAM)), 2)

    def test_the_bucket_admin_binding_is_never_revoked(self):
        # An admin dual-listed under a team, or an out-of-band bucket owner
        # whose email is passed to revoke, must keep roles/storage.admin — the
        # revoke only clears the four team bindings bootstrap creates.
        self.assertEqual(self.members(BUCKET_IAM, "roles/storage.admin"), ["user:ada@example.com"])
        registry_expr = ledger_iam._registry_condition(BUCKET)["expression"]

        ledger_iam.revoke_ledger_member(BUCKET, "ada@example.com", session=self.fake)

        self.assertEqual(self.members(BUCKET_IAM, "roles/storage.admin"), ["user:ada@example.com"])
        # The revoke still cleared the team-level grants ada also held.
        self.assertNotIn("user:ada@example.com",
                         self.members(BUCKET_IAM, "roles/storage.objectViewer", registry_expr) or [])

    def test_a_missing_platform_folder_is_not_an_error(self):
        # A ledger whose platform/ folder was never created (pre-managed-folder
        # bucket): the folder policy read 404s, and the bucket revoke still runs.
        self.fake.script("GET", PLATFORM_IAM, FakeResponse(404, {"error": {"message": "not found"}}))
        changed = ledger_iam.revoke_ledger_member(BUCKET, "pat@example.com", session=self.fake)
        self.assertEqual(changed, 3)
