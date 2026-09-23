"""Unit tests for the data review's handle resolver.

The review's tools are otherwise exercised end to end from `servers/dag/main_test.py`;
this file holds the cases that are about one refusal's wording.
"""

import unittest
from types import SimpleNamespace

from servers.phases.discovery.discovery_datareview_3 import tools

_S = "arn:aws:secretsmanager:us-east-1:111111111111:secret:"


def _review(*entries):
    """The slice of `_Review` that `_target` reads."""
    return SimpleNamespace(
        entries=list(entries),
        inventory={"data_dependencies": list(entries)},
        state_dict={"current_state": tools.REVIEW_STATE})


class ReviewRoundFortyFiveTest(unittest.TestCase):
    """Regressions from the forty-fifth adversarial review round."""

    def test_a_mixed_over_match_gets_the_arn_spelling_refusal(self):
        # A declared secret that absorbed one console form beside the second
        # console form the merge refused: a bare wildcard spelling matches
        # both. `_record_directory` asked without the address counted the
        # declared entry as having a directory, so the refusal claimed "the
        # same block address is declared in more than one root module" —
        # about two secrets that share no block address at all.
        declared = {
            "service": "secretsmanager", "identifier": "acme/db",
            "address": "aws_secretsmanager_secret.db", "arn": f"{_S}acme/db-Prod01",
            "console_form": "acme/db-Prod01", "detection": "declared",
            "disposition": "migrate", "evidence": ["sm.tf"], "consumers": [], "notes": [],
        }
        referenced = {
            "service": "secretsmanager", "identifier": "acme/db-Dev001",
            "address": f"{_S}acme/db-Dev001", "arn": f"{_S}acme/db-Dev001",
            "detection": "referenced", "disposition": "migrate",
            "evidence": ["envs/dev/iam.tf"], "consumers": [], "notes": [],
        }
        matched, error = tools._target(
            _review(declared, referenced),
            "arn:aws:secretsmanager:*:*:secret:acme/db", None, None)
        self.assertIsNone(matched)
        self.assertIn("this spelling matches more than one of them", error)
        self.assertIn(f"{_S}acme/db-Dev001", error)
        self.assertNotIn("more than one root module", error)


class ReviewRoundFortySevenTest(unittest.TestCase):
    """Regressions from the forty-seventh adversarial review round."""

    def test_a_spelling_reach_onto_a_flagged_entry_is_refused(self):
        def flagged(spelling):
            arn = f"arn:aws:secretsmanager:{spelling}:secret:orders"
            return {"service": "secretsmanager", "identifier": "orders", "address": arn,
                    "arn": arn, "detection": "referenced", "disposition": "migrate",
                    "evidence": ["iam.tf"], "consumers": [], "notes": [
                        "its ARN spelling also matches another entry in this section: (test)"]}
        review = _review(flagged("us-east-1:333333333333"),
                         flagged("us-east-1:444444444444"), flagged(":"))
        matched, error = tools._target(
            review, "arn:aws:secretsmanager:us-east-1:111111111111:secret:orders", None, None)
        self.assertIsNone(matched)
        self.assertIn("by spelling only", error)
        self.assertIn("arn:aws:secretsmanager:::secret:orders", error)
        matched, error = tools._target(review, "arn:aws:secretsmanager:::secret:orders", None, None)
        self.assertIsNone(error)
        self.assertEqual(len(matched), 1)



class Cl6RoundOneTest(unittest.TestCase):
    """Literal handles other than ARNs: a guess carries no directory, and an
    endpoint resolves the entry after the ARN took over as the handle."""

    def test_a_guess_and_a_hand_added_entry_carry_no_directory(self):
        guess = {"service": "s3", "identifier": "acme-invoice-archive",
                 "address": "s3:acme-invoice-archive", "detection": "inferred",
                 "disposition": "undecided", "evidence": ["charts/orders/values.yaml"],
                 "consumers": [], "notes": []}
        added = dict(guess, detection="human_review",
                     evidence=["added at the data review by someone on 2026-09-09"])
        for entry in (guess, added):
            self.assertIsNone(tools._record_directory(entry))
            self.assertNotIn("directory=", tools._describe(entry))

    def test_an_endpoint_handle_resolves_the_entry_after_the_arn_took_over(self):
        url = "https://sqs.us-east-1.amazonaws.com/111111111111/orders"
        arn = "arn:aws:sqs:us-east-1:111111111111:orders"
        entry = {"service": "sqs", "identifier": "orders", "address": arn, "arn": arn,
                 "endpoint": url, "detection": "referenced", "disposition": "replatform",
                 "evidence": ["iam/p.tf"], "consumers": [], "notes": []}
        matched, error = tools._target(_review(entry), url, None, None)
        self.assertIsNone(error)
        self.assertEqual(matched, [entry])
        self.assertIn((url, None), tools._handles(entry))



class Cl6RoundSixTest(unittest.TestCase):
    def test_the_guess_handle_is_a_lookup_not_an_alias(self):
        dev = {"service": "sqs", "identifier": "orders", "address": "aws_sqs_queue.orders",
               "detection": "declared", "evidence": ["envs/dev/main.tf"], "consumers": [], "notes": []}
        self.assertEqual([h for h, _d in tools._handles(dev)], ["aws_sqs_queue.orders"])
        self.assertEqual(tools._alias_records(dev, "aws_sqs_queue.orders"), [])


if __name__ == "__main__":
    unittest.main()
