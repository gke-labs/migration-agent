"""Unit tests for the deployment step's handle resolver.

The step's tools are otherwise exercised end to end from `servers/dag/main_test.py`;
this file holds the cases that are about one refusal's wording, which the MCP-level
harness is too heavy to pin one by one.
"""

import unittest

from servers.phases.deployment import datamigration as dm
from servers.phases.deployment import runbooks as rb
from servers.phases.deployment.deployment_datamigration_2 import tools


def _referenced_queue(account: str) -> dict:
    arn = f"arn:aws:sqs:us-east-1:{account}:orders"
    return {
        "service": "sqs", "identifier": "orders", "address": arn, "arn": arn,
        "region": "us-east-1", "account": account, "detection": "referenced",
        "disposition": "migrate", "evidence": ["iam.tf"], "consumers": [],
        "notes": [],
    }


class ReviewRoundFortyFiveTest(unittest.TestCase):
    """Regressions from the forty-fifth adversarial review round."""

    def test_an_under_specified_arn_over_two_arn_only_entries_asks_for_the_exact_spelling(self):
        # Two queues of one name in two accounts, reached through the spelling
        # fallback: no declaring directory, one identifier. Neither axis
        # separates them and the resolver said so with "should not happen;
        # report it" — while the review's sibling resolver, which DESIGN says
        # this one takes its rules from, advises the exact spelling the
        # listing prints.
        context = {"entries": [_referenced_queue("111111111111"),
                               _referenced_queue("222222222222")]}
        with self.assertRaises(tools._Refused) as raised:
            tools._target(context, "arn:aws:sqs:*:*:orders", None, None)
        message = str(raised.exception)
        self.assertIn("exact address as list_data_migrations() prints it", message)
        self.assertIn("arn:aws:sqs:us-east-1:111111111111:orders", message)
        self.assertIn("arn:aws:sqs:us-east-1:222222222222:orders", message)
        self.assertNotIn("should not happen", message)


def _flagged_secret(spelling: str) -> dict:
    arn = f"arn:aws:secretsmanager:{spelling}:secret:orders"
    return {
        "service": "secretsmanager", "identifier": "orders", "address": arn, "arn": arn,
        "detection": "referenced", "disposition": "migrate", "evidence": ["iam.tf"],
        "consumers": [], "notes": [
            "its ARN spelling also matches another entry in this section: (test)"],
    }


class ReviewRoundFortySevenTest(unittest.TestCase):
    """Regressions from the forty-seventh adversarial review round."""

    def test_a_spelling_reach_onto_a_flagged_entry_is_refused(self):
        # The replay refused this; the live resolver placed it and recorded
        # the any-account entry migrated on a report about a spelling no
        # longer in the section — the entry that may be either foreign one.
        context = {"entries": [_flagged_secret("us-east-1:333333333333"),
                               _flagged_secret("us-east-1:444444444444"),
                               _flagged_secret(":")]}
        with self.assertRaises(tools._Refused) as raised:
            tools._target(context, "arn:aws:secretsmanager:us-east-1:111111111111:secret:orders",
                          None, None)
        message = str(raised.exception)
        self.assertIn("by spelling only", message)
        self.assertIn("arn:aws:secretsmanager:::secret:orders", message)
        # The exact spelling still resolves.
        self.assertEqual(
            tools._target(context, "arn:aws:secretsmanager:::secret:orders", None, None)["arn"],
            "arn:aws:secretsmanager:::secret:orders")


class ReviewRoundFiftyTest(unittest.TestCase):
    """Regressions from the fiftieth adversarial review round."""

    def test_an_adapted_runbook_is_found_under_the_entrys_earlier_handle(self):
        # The rendered name is keyed on `address`, which moves across a fold;
        # the adapted file does not. Readers look under every handle the
        # entry has had, and the worklist says "adapted" for the folded entry.
        arn = "arn:aws:s3:::acme-logs"
        referenced = {
            "service": "s3", "identifier": "acme-logs", "address": arn, "arn": arn,
            "detection": "referenced", "disposition": "migrate", "evidence": ["iam.tf"],
            "consumers": [], "notes": [],
        }
        folded = dict(referenced, address="aws_s3_bucket.logs", detection="declared",
                      evidence=["envs/prod/main.tf"])
        earlier = rb.rendered_blob(referenced)
        self.assertNotEqual(rb.rendered_blob(folded), earlier)
        self.assertEqual(rb.rendered_blobs(folded), [rb.rendered_blob(folded), earlier])
        self.assertEqual(rb.rendered_blobs(referenced), [earlier])
        self.assertIn(f"`{earlier}` — adapted for this estate.",
                      dm._procedure_line(folded, rendered={earlier}))
        self.assertIn("not yet adapted", dm._procedure_line(folded, rendered=set()))


class ReviewRoundFiftyOneTest(unittest.TestCase):
    """Regressions from the fifty-first adversarial review round."""

    def test_rendered_blobs_follows_the_outcome_records_earlier_spellings(self):
        # A respelling (wildcard joined by a qualified sighting), a renamed
        # declaring directory and a secret's console form all moved the name
        # while the adapted file stayed; the outcome record matched to the
        # entry holds the earlier handle, so it names the candidate.
        bare = "arn:aws:sqs:::orders"
        full = "arn:aws:sqs:us-east-1:111111111111:orders"
        respelled = {"service": "sqs", "identifier": "orders", "address": full, "arn": full,
                     "detection": "referenced", "disposition": "migrate",
                     "evidence": ["iam.tf"], "consumers": [], "notes": []}
        written_under_bare = rb.rendered_blob(dict(respelled, address=bare, arn=bare))
        record = {"address": bare, "directory": "", "identifier": "orders", "aliases": []}
        self.assertIn(written_under_bare, rb.rendered_blobs(respelled, [record]))
        self.assertNotIn(written_under_bare, rb.rendered_blobs(respelled))
        moved = dict(respelled, address="aws_sqs_queue.orders", detection="declared",
                     evidence=["environments/prod/main.tf"])
        written_before_rename = rb.rendered_blob(dict(moved, evidence=["envs/prod/main.tf"]))
        record = {"address": "aws_sqs_queue.orders", "directory": "envs/prod",
                  "identifier": "orders", "aliases": [full]}
        self.assertIn(written_before_rename, rb.rendered_blobs(moved, [record]))
        secret_arn = "arn:aws:secretsmanager:us-east-1:111111111111:secret:acme/db"
        folded_secret = {"service": "secretsmanager", "identifier": "acme/db",
                         "address": "aws_secretsmanager_secret.db", "arn": secret_arn,
                         "console_form": "acme/db-Ab1Cd2", "detection": "declared",
                         "disposition": "migrate", "evidence": ["envs/prod/sm.tf"],
                         "consumers": [], "notes": []}
        written_as_console = rb.rendered_blob({
            **folded_secret, "identifier": "acme/db-Ab1Cd2",
            "address": secret_arn + "-Ab1Cd2", "arn": secret_arn + "-Ab1Cd2",
            "detection": "referenced"})
        self.assertIn(written_as_console, rb.rendered_blobs(folded_secret))
        # Order and dedupe: the current name first, no repeats.
        blobs = rb.rendered_blobs(folded_secret, [{"address": secret_arn, "directory": "",
                                                   "identifier": "acme/db", "aliases": []}])
        self.assertEqual(blobs[0], rb.rendered_blob(folded_secret))
        self.assertEqual(len(blobs), len(set(blobs)))

    def test_earlier_copies_names_only_the_files_a_save_would_write_around(self):
        arn = "arn:aws:s3:::acme-logs"
        folded = {"service": "s3", "identifier": "acme-logs", "address": "aws_s3_bucket.logs",
                  "arn": arn, "detection": "declared", "disposition": "migrate",
                  "evidence": ["envs/prod/main.tf"], "consumers": [], "notes": []}
        arn_era = rb.rendered_blob(dict(folded, address=arn, detection="referenced"))
        self.assertEqual(rb.earlier_copies(folded, {arn_era}), [arn_era])
        self.assertEqual(rb.earlier_copies(folded, {rb.rendered_blob(folded)}), [])
        self.assertEqual(rb.earlier_copies(folded, set()), [])


class ReviewRoundFiftyTwoTest(unittest.TestCase):
    """Regressions from the fifty-second adversarial review round."""

    def test_a_failed_listing_is_an_error_to_the_guard_and_empty_to_the_worklist(self):
        # `rendered_runbooks` swallowed every exception into an empty set, so
        # the save tool's earlier-copy guard read "could not list" as
        # "nothing there" and wrote around the adapted copy in silence.
        from types import SimpleNamespace
        from unittest import mock
        bucket = SimpleNamespace(name="b")
        with mock.patch.object(tools.state_mgr, "gcs_client") as client:
            client.list_blobs.side_effect = RuntimeError("503 backend error")
            self.assertIs(tools.rendered_runbooks(bucket), dm.LISTING_UNREAD)
            self.assertNotIn(rb.RENDERED_PREFIX + "x.md", tools.rendered_runbooks(bucket))
            with self.assertRaises(RuntimeError):
                tools.rendered_runbooks(bucket, strict=True)
            client.list_blobs.side_effect = None
            client.list_blobs.return_value = [SimpleNamespace(name=rb.RENDERED_PREFIX + "x.md")]
            self.assertEqual(tools.rendered_runbooks(bucket, strict=True),
                             {rb.RENDERED_PREFIX + "x.md"})

    def test_supersede_earlier_reports_what_it_removed_and_what_it_could_not(self):
        class Gone(Exception):
            pass
        calls = []

        def delete(path):
            calls.append(path)
            if path.endswith("gone.md"):
                raise Gone()
            if path.endswith("locked.md"):
                raise PermissionError("denied")
        removed, left = rb.supersede_earlier(
            ["a/old.md", "a/gone.md", "a/locked.md"], delete, (Gone,))
        self.assertEqual(calls, ["a/old.md", "a/gone.md", "a/locked.md"])
        self.assertEqual(removed, ["a/old.md", "a/gone.md"])
        self.assertEqual(left, ["a/locked.md"])
        self.assertEqual(rb.supersede_earlier([], delete, (Gone,)), ([], []))


if __name__ == "__main__":
    unittest.main()
