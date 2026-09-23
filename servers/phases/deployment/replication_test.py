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

"""Unit tests for mark_self_service_complete and the image_map flip.

Pure: no GCS, no subprocess, no network. The tool-level round trip
(state gating, inventory persistence, exports hook) lives in
servers/dag/main_test.py next to the other deployment-flow tests.
"""

import unittest
from unittest.mock import MagicMock, patch

from servers.dag.server import exports
from servers.phases.deployment import replication

ECR = "123456789012.dkr.ecr.us-east-1.amazonaws.com"
DEST = "us-central1-docker.pkg.dev/acme-prod/images"
DIGEST = "sha256:" + "b" * 64


def _image(ref, registry="ecr", outcome=None, **extra):
    repository, _, tag = ref.rpartition(":")
    image = {"ref": ref, "registry": registry, "repository": repository,
             "tag": tag, "digest": None, "pinned_by": "tag",
             "provenance": [{"kind": "literal", "file": "k8s/deploy.yaml"}]}
    if outcome is not None:
        image["replication"] = outcome
    image.update(extra)
    return image


def _inventory(*images):
    return {"images": list(images)}


class MarkSelfServiceCompleteTest(unittest.TestCase):

    def test_named_self_service_entry_flips_to_user_asserted_replicated(self):
        inv = _inventory(_image(f"{ECR}/api:1.0",
                                outcome={"status": "self_service"}))
        marked, updated, already, problems = \
            replication.mark_self_service_complete(
                inv, refs=[f"{ECR}/api:1.0"], dest_url=DEST)
        self.assertEqual((marked, updated, already, problems),
                         ([f"{ECR}/api:1.0"], [], [], []))
        self.assertEqual(inv["images"][0]["replication"], {
            "status": "replicated", "destination": f"{DEST}/api:1.0",
            "verified_by": "user_asserted"})

    def test_a_supplied_digest_is_recorded_verbatim(self):
        ref = f"{ECR}/api:1.0"
        inv = _inventory(_image(ref, outcome={"status": "self_service"}))
        marked, _, _, problems = replication.mark_self_service_complete(
            inv, refs=[ref], digests={ref: DIGEST}, dest_url=DEST)
        self.assertEqual(marked, [ref])
        self.assertEqual(problems, [])
        self.assertEqual(inv["images"][0]["replication"]["content_digest"],
                         DIGEST)

    def test_no_digest_supplied_means_no_digest_recorded(self):
        ref = f"{ECR}/api:1.0"
        inv = _inventory(_image(ref, outcome={"status": "self_service"}))
        replication.mark_self_service_complete(inv, refs=[ref], dest_url=DEST)
        self.assertNotIn("content_digest", inv["images"][0]["replication"],
                         "a digest the user did not supply is never invented")

    def test_a_malformed_digest_refuses_the_image(self):
        ref = f"{ECR}/api:1.0"
        inv = _inventory(_image(ref, outcome={"status": "self_service"}))
        marked, _, _, problems = replication.mark_self_service_complete(
            inv, refs=[ref], digests={ref: "not-a-digest"}, dest_url=DEST)
        self.assertEqual(marked, [])
        self.assertEqual(inv["images"][0]["replication"]["status"],
                         "self_service", "the entry stays untouched")
        self.assertIn("does not look like a content digest", problems[0])

    def test_bulk_marks_every_self_service_and_failed_entry(self):
        inv = _inventory(
            _image(f"{ECR}/api:1.0", outcome={"status": "self_service"}),
            _image(f"{ECR}/web:2.0", outcome={
                "status": "replication_failed",
                "destination": f"{DEST}/web:2.0", "error": "denied"}),
            _image(f"{ECR}/old:0.1", outcome={
                "status": "replicated", "destination": f"{DEST}/old:0.1"}),
            _image("busybox:stable", registry="dockerhub"))
        marked, updated, already, problems = \
            replication.mark_self_service_complete(inv, dest_url=DEST)
        self.assertEqual(sorted(marked), [f"{ECR}/api:1.0", f"{ECR}/web:2.0"])
        self.assertEqual((updated, already, problems), ([], [], []),
                         "bulk never touches replicated or outcome-less entries")
        self.assertEqual(inv["images"][3].get("replication"), None)

    def test_a_failed_entry_keeps_its_recorded_destination(self):
        ref = f"{ECR}/web:2.0"
        inv = _inventory(_image(ref, outcome={
            "status": "replication_failed",
            "destination": f"{DEST}/elsewhere/web:2.0", "error": "denied"}))
        replication.mark_self_service_complete(inv, refs=[ref], dest_url=DEST)
        self.assertEqual(inv["images"][0]["replication"], {
            "status": "replicated",
            "destination": f"{DEST}/elsewhere/web:2.0",
            "verified_by": "user_asserted",
            "previous": {"status": "replication_failed", "error": "denied"}},
            "the destination the copy actually targeted wins over the plan, "
            "and the repaired failure stays in the ledger")

    def test_partial_marks_the_known_and_names_the_unknown(self):
        known = f"{ECR}/api:1.0"
        inv = _inventory(_image(known, outcome={"status": "self_service"}))
        marked, _, _, problems = replication.mark_self_service_complete(
            inv, refs=[known, f"{ECR}/ghost:9.9"], dest_url=DEST)
        self.assertEqual(marked, [known])
        self.assertEqual(len(problems), 1)
        self.assertIn(f"{ECR}/ghost:9.9: not in the discovery inventory",
                      problems[0])
        self.assertEqual(len(inv["images"]), 1,
                         "an unknown ref never creates an entry")

    def test_marking_twice_is_idempotent(self):
        ref = f"{ECR}/api:1.0"
        inv = _inventory(_image(ref, outcome={"status": "self_service"}))
        replication.mark_self_service_complete(inv, dest_url=DEST)
        first = dict(inv["images"][0]["replication"])
        marked, updated, already, problems = \
            replication.mark_self_service_complete(inv, dest_url=DEST)
        self.assertEqual((marked, updated, already, problems), ([], [], [], []),
                         "a bulk re-run finds nothing left to mark")
        marked, updated, already, problems = \
            replication.mark_self_service_complete(
                inv, refs=[ref], dest_url=DEST)
        self.assertEqual((marked, updated, already, problems),
                         ([], [], [ref], []),
                         "naming an already-replicated ref is calm, not an error")
        self.assertEqual(inv["images"][0]["replication"], first)

    def test_an_entry_without_an_outcome_is_refused(self):
        ref = f"{ECR}/api:1.0"
        inv = _inventory(_image(ref))
        marked, _, _, problems = replication.mark_self_service_complete(
            inv, refs=[ref], dest_url=DEST)
        self.assertEqual(marked, [])
        self.assertIn("no self-service or failed replication outcome",
                      problems[0])
        self.assertNotIn("replication", inv["images"][0])

    def test_an_unresolvable_destination_refuses_the_image(self):
        ref = f"{ECR}/api:1.0"
        inv = _inventory(_image(ref, outcome={"status": "self_service"}))
        marked, _, _, problems = replication.mark_self_service_complete(
            inv, refs=[ref], dest_url=None)
        self.assertEqual(marked, [])
        self.assertEqual(inv["images"][0]["replication"]["status"],
                         "self_service")
        self.assertIn("destinations={...}", problems[0])

    def test_an_explicit_destination_resolves_the_placeholder_case(self):
        ref = f"{ECR}/api:1.0"
        inv = _inventory(_image(ref, outcome={"status": "self_service"}))
        marked, _, _, problems = replication.mark_self_service_complete(
            inv, refs=[ref], destinations={ref: f"{DEST}/api:1.0"})
        self.assertEqual((marked, problems), ([ref], []))
        self.assertEqual(inv["images"][0]["replication"]["destination"],
                         f"{DEST}/api:1.0")

    def test_a_malformed_explicit_destination_refuses_the_image(self):
        ref = f"{ECR}/api:1.0"
        inv = _inventory(_image(ref, outcome={"status": "self_service"}))
        marked, _, _, problems = replication.mark_self_service_complete(
            inv, refs=[ref], destinations={ref: "<AR_DESTINATION>/api:1.0"})
        self.assertEqual(marked, [])
        self.assertEqual(inv["images"][0]["replication"]["status"],
                         "self_service", "the entry stays untouched")
        self.assertIn("does not look like a registry reference", problems[0])

    def test_a_digest_supplied_later_upgrades_the_recorded_entry(self):
        """The tool's own response asks the user to come back with a
        verified digest, so the second call must not be a no-op."""
        ref = f"{ECR}/api:1.0"
        inv = _inventory(_image(ref, outcome={"status": "self_service"}))
        replication.mark_self_service_complete(inv, refs=[ref], dest_url=DEST)
        marked, updated, already, problems = \
            replication.mark_self_service_complete(
                inv, refs=[ref], digests={ref: DIGEST}, dest_url=DEST)
        self.assertEqual((marked, updated, already, problems),
                         ([], [ref], [], []))
        self.assertEqual(inv["images"][0]["replication"]["content_digest"],
                         DIGEST)

    def test_a_bulk_call_covers_the_refs_it_carries_values_for(self):
        """The tip printed after a digest-less mark says `re-call with
        digests={...}` — with no refs. Bulk must honour that literally."""
        ref = f"{ECR}/api:1.0"
        inv = _inventory(_image(ref, outcome={"status": "self_service"}))
        replication.mark_self_service_complete(inv, dest_url=DEST)
        _, updated, _, problems = replication.mark_self_service_complete(
            inv, digests={ref: DIGEST}, dest_url=DEST)
        self.assertEqual((updated, problems), ([ref], []))
        self.assertEqual(inv["images"][0]["replication"]["content_digest"],
                         DIGEST)

    def test_re_supplying_the_same_digest_is_already_not_updated(self):
        ref = f"{ECR}/api:1.0"
        inv = _inventory(_image(ref, outcome={"status": "self_service"}))
        replication.mark_self_service_complete(
            inv, refs=[ref], digests={ref: DIGEST}, dest_url=DEST)
        _, updated, already, _ = replication.mark_self_service_complete(
            inv, refs=[ref], digests={ref: DIGEST}, dest_url=DEST)
        self.assertEqual((updated, already), ([], [ref]),
                         "an upgrade that changes nothing is not an upgrade")


class ImageMapFlipTest(unittest.TestCase):
    """Marking completion is what flips the exports image_map entry from
    the self_service plan to a replicated fact — the discriminator the
    workload-side rewrites key on."""

    VARIABLES = {"artifact_registry_destinations": [{"url": DEST}]}

    def _derive(self, inv):
        planned = replication.planned_destination_refs(
            DEST, [i for i in inv["images"] if i.get("registry") == "ecr"])
        fields, _ = exports.derive_deployment_fields(
            self.VARIABLES, inv, "meta-project", planned, None)
        return fields["artifact_registry"]["image_map"]

    def test_marking_flips_the_map_entry_to_replicated(self):
        ref = f"{ECR}/api:1.0"
        inv = _inventory(_image(ref, outcome={"status": "self_service"}))
        self.assertEqual(self._derive(inv)[ref],
                         {"dest_ref": f"{DEST}/api:1.0",
                          "status": "self_service"})
        replication.mark_self_service_complete(
            inv, refs=[ref], digests={ref: DIGEST}, dest_url=DEST)
        self.assertEqual(self._derive(inv)[ref],
                         {"dest_ref": f"{DEST}/api:1.0",
                          "status": "replicated", "content_digest": DIGEST,
                          "verified_by": "user_asserted"})


class CopyImagesRecordingTest(unittest.TestCase):
    """The server's own copies record how they were verified."""

    def _copy(self, image, written=None, ok=True):
        """Runs one copy with skopeo faked out; `written` is what the fake
        skopeo puts in --digestfile (None: an old skopeo that writes none)."""
        credentials = MagicMock(valid=True, token="tok")
        self.argv = []

        def fake_run(cmd, timeout):
            self.argv = cmd
            if ok and written is not None:
                with open(cmd[cmd.index("--digestfile") + 1], "w",
                          encoding="utf-8") as f:
                    f.write(written)
            return (ok, "" if ok else "denied")

        with patch.object(replication, "_run", side_effect=fake_run):
            replication._copy_images(
                "/usr/bin/skopeo", [image],
                {image["ref"]: f"{DEST}/api:1.0"}, credentials)
        return image["replication"]

    def test_the_recorded_digest_is_the_one_skopeo_wrote(self):
        """Not the source's: --multi-arch=system copies one instance out of
        an index, so an index-pinned source digest names nothing at the
        destination."""
        image = _image(f"{ECR}/api:1.0", digest="sha256:" + "a" * 64)
        outcome = self._copy(image, written="sha256:" + "d" * 64 + "\n")
        self.assertEqual(outcome["verified_by"], "skopeo_preserve_digests")
        self.assertEqual(outcome["content_digest"], "sha256:" + "d" * 64)
        self.assertIn("--digestfile", self.argv)

    def test_a_skopeo_that_wrote_no_digestfile_records_no_digest(self):
        image = _image(f"{ECR}/api:1.0", digest="sha256:" + "a" * 64)
        outcome = self._copy(image, written=None)
        self.assertEqual(outcome["verified_by"], "skopeo_preserve_digests")
        self.assertNotIn("content_digest", outcome,
                         "an unobserved digest value stays unrecorded")

    def test_a_garbled_digestfile_records_no_digest(self):
        outcome = self._copy(_image(f"{ECR}/api:1.0"), written="Trying to...")
        self.assertNotIn("content_digest", outcome)

    def test_a_failed_copy_records_no_digest_and_the_error(self):
        outcome = self._copy(_image(f"{ECR}/api:1.0"),
                             written="sha256:" + "d" * 64, ok=False)
        self.assertEqual(outcome["status"], "replication_failed")
        self.assertNotIn("content_digest", outcome)


class AbandonTest(unittest.TestCase):
    """`abandon_copies` — the exit for a copy nobody is going to make."""

    def _inventory(self, ref, **replication_fields):
        return {"images": [{"ref": ref, "registry": "ecr",
                            "repository": "api", "tag": "1.0",
                            "replication": dict(replication_fields)}]}

    def test_the_planned_destination_survives_the_abandonment(self):
        """It is what makes a reversal work: the registry is computed by the
        user's own apply, so `planned` cannot resolve the ref weeks later, and
        without the carried value `mark_replication_complete` has no
        destination to record. It also tells an abandoned plan from one that
        never had a destination."""
        inventory = self._inventory(f"{ECR}/api:1.0", status="self_service",
                                    destination=f"{DEST}/api:1.0")

        replication.abandon_copies(inventory, [f"{ECR}/api:1.0"], "no upstream",
                                   "platform-user@google.com", "2026-09-02")

        record = inventory["images"][0]["replication"]
        self.assertEqual(record["status"], "abandoned")
        self.assertEqual(record["destination"], f"{DEST}/api:1.0")

    def test_an_abandonment_with_no_planned_destination_records_none(self):
        inventory = self._inventory(f"{ECR}/api:1.0", status="self_service")

        replication.abandon_copies(inventory, [f"{ECR}/api:1.0"], "no upstream",
                                   "platform-user@google.com", "2026-09-02")

        self.assertNotIn("destination", inventory["images"][0]["replication"])

    def test_a_ref_the_inventory_does_not_carry_is_named(self):
        """Silently recording nothing would let an operator believe a copy
        was cancelled when no entry was touched."""
        inventory = self._inventory(f"{ECR}/api:1.0", status="self_service")

        abandoned, problems = replication.abandon_copies(
            inventory, [f"{ECR}/typo:1.0"], "no upstream",
            "platform-user@google.com", "2026-09-02")

        self.assertEqual(abandoned, [])
        self.assertEqual(len(problems), 1)
        self.assertIn("not in the discovery inventory", problems[0])

    def test_abandoning_an_abandoned_copy_says_so(self):
        inventory = self._inventory(f"{ECR}/api:1.0", status="abandoned",
                                    reason="no upstream")

        abandoned, problems = replication.abandon_copies(
            inventory, [f"{ECR}/api:1.0"], "again",
            "platform-user@google.com", "2026-09-02")

        self.assertEqual(abandoned, [])
        self.assertIn("already abandoned", problems[0])

    def test_the_refusal_signposts_how_to_reverse_an_abandonment(self):
        """A bulk `mark_replication_complete` deliberately will not resurrect
        a decision, and this sentence is the only place an operator learns
        that naming the ref does — the two-place guard's own signpost."""
        inventory = {"images": [{
            "ref": f"{ECR}/api:1.0", "registry": "ecr", "repository": "api",
            "tag": "1.0",
            "replication": {"status": "abandoned", "reason": "no upstream"}}]}

        # Bulk selection alone skips an abandoned entry silently. Supplying a
        # digest pulls the ref into the target set — the call shape
        # `mark_replication_complete`'s own response asks for — so the per-ref
        # guard is what refuses, and this is the sentence it refuses with.
        _, _, _, problems = replication.mark_self_service_complete(
            inventory, None, {f"{ECR}/api:1.0": DIGEST}, None, DEST)

        self.assertEqual(len(problems), 1)
        self.assertIn("name it in refs=", problems[0])

    def test_a_ref_named_twice_is_abandoned_once(self):
        """The response lists what it did, and naming one image twice read as
        two decisions."""
        inventory = self._inventory(f"{ECR}/api:1.0", status="self_service")

        abandoned, _ = replication.abandon_copies(
            inventory, [f"{ECR}/api:1.0", f"{ECR}/api:1.0"], "no upstream",
            "platform-user@google.com", "2026-09-02")

        self.assertEqual(abandoned, [f"{ECR}/api:1.0"])


if __name__ == "__main__":
    unittest.main()
