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

"""Ranking candidate owners for an unattributed data service.

The ranking is a guess by construction. What these tests pin is that it is
always presented as one, and that it never narrows to a single name.
"""

import unittest

from servers.phases.discovery.discovery_datareview_3 import candidates


def _workload(name, kind="helm_release", namespace=None, source_path=None):
    return {"workload": name, "kind": kind, "namespace": namespace,
            "source_path": source_path}


POOL = [
    _workload("orders", namespace="orders", source_path="src/orders/chart"),
    _workload("carts"),
    _workload("checkout-api", "service_account", namespace="checkout"),
]


class RankTest(unittest.TestCase):

    def test_the_matching_name_ranks_and_is_labeled_a_guess(self):
        ranked, note = candidates.rank(
            {"identifier": "orders-db", "address": "module.orders_rds"}, POOL)

        self.assertEqual([c["workload"] for c in ranked], ["orders"])
        self.assertIn("GUESS", note)
        # The instruction that keeps a tired reviewer from rubber-stamping the
        # top row is part of the output, not only of the step's instructions.
        self.assertIn("none of these", note)

    def test_more_shared_tokens_rank_higher(self):
        pool = [_workload("checkout"), _workload("checkout-api")]
        ranked, _ = candidates.rank(
            {"identifier": "checkout-api-sessions",
             "address": "module.checkout_api"}, pool)
        self.assertEqual([c["workload"] for c in ranked],
                         ["checkout-api", "checkout"])

    def test_a_namespace_or_chart_path_matches_as_well_as_the_name(self):
        pool = [_workload("web", namespace="orders"),
                _workload("api", source_path="src/orders/chart")]
        ranked, _ = candidates.rank({"identifier": "orders-db"}, pool)
        self.assertEqual({c["workload"] for c in ranked}, {"web", "api"})

    def test_no_match_is_stated_honestly_and_names_nobody(self):
        ranked, note = candidates.rank(
            {"identifier": "legacy-warehouse", "address": "module.dw"}, POOL)

        self.assertEqual(ranked, [])
        self.assertIn("does NOT mean nothing uses it", note)
        # Above all: no candidate is offered on no evidence.
        for name in ("orders", "carts", "checkout-api"):
            self.assertNotIn(f"'{name}'", note)

    def test_generic_halves_of_a_name_do_not_match_everything(self):
        # "db" and "prod" are in every second resource name in a real estate.
        # Matching on them would rank the whole pool and mean nothing, so
        # nothing is presented as a match — the pool is offered unranked
        # instead, which is a different claim.
        ranked, note = candidates.rank(
            {"identifier": "prod-db", "address": "module.database"}, POOL)
        self.assertTrue(all(not c["matched"] for c in ranked))
        self.assertIn("nothing is ranked", note)

    def test_a_short_token_only_matches_a_whole_token(self):
        # "api" inside "rapid" is an accident, not a signal.
        ranked, _ = candidates.rank({"identifier": "rapid-cache"},
                                    [_workload("api")])
        self.assertEqual(ranked, [])

    def test_an_empty_pool_says_so_rather_than_saying_nothing(self):
        ranked, note = candidates.rank({"identifier": "orders-db"}, [])
        self.assertEqual(ranked, [])
        self.assertIn("no workload at all", note)

    def test_an_entry_with_no_usable_tokens_still_gets_the_pool(self):
        # `module.this` with an identifier of `db` is the case the rest of this
        # step singles out as hardest, and the noise list eats its whole name.
        # Returning nothing here left the agent relaying a sentence that
        # pointed at a list that was not printed — so the pool is handed over
        # unranked, and the note says the order means nothing.
        ranked, note = candidates.rank(
            {"identifier": "db", "address": "module.this"}, POOL)
        self.assertEqual([c["workload"] for c in ranked],
                         [c["workload"] for c in POOL])
        self.assertTrue(all(c["matched"] == [] for c in ranked))
        self.assertIn("no usable tokens", note)
        self.assertIn("that order means nothing", note)

    def test_the_unranked_pool_is_still_labeled_a_guess_with_an_opt_out(self):
        """This module states two rules as load-bearing: a match is always
        labeled a guess, and a list always comes with "none of these". Every
        ranked branch honoured both; this one honoured neither — on exactly
        the entries where the evidence is zero and the anchoring risk is
        highest, in a note the agent relays verbatim."""
        _, note = candidates.rank(
            {"identifier": "db", "address": "module.this"}, POOL)
        self.assertIn("GUESS", note)
        self.assertIn("none of these", note)

    def test_an_unranked_candidate_line_does_not_claim_a_shared_token(self):
        ranked, _ = candidates.rank({"identifier": "db"}, POOL)
        self.assertTrue(all("shares" not in line
                            for line in candidates.lines(ranked)))

    def test_the_list_is_capped_and_the_note_says_so(self):
        # "None of these" is only honest when the reviewer knows what "these"
        # was. Offering it over a silently truncated list invites them to rule
        # out a candidate they were never shown.
        pool = [_workload(f"orders-{i}") for i in range(9)]
        ranked, note = candidates.rank({"identifier": "orders-db"}, pool,
                                       limit=3)
        self.assertEqual(len(ranked), 3)
        self.assertIn("9 workload(s)", note)
        self.assertIn("Only the top 3 are listed", note)

    def test_an_uncapped_list_does_not_claim_to_be_capped(self):
        ranked, note = candidates.rank({"identifier": "orders-db"}, POOL)
        self.assertEqual(len(ranked), 1)
        self.assertNotIn("Only the top", note)


class ListingWeightTest(unittest.TestCase):
    """The listing is relayed to a user. Boilerplate per entry buries the
    entries in it — the estates this step exists for are mostly `escalate` and
    mostly unattributed, so per-entry prose multiplies by the worst case."""

    def test_the_per_entry_keep_in_aws_line_is_a_pointer_not_the_argument(self):
        from servers.phases.discovery.discovery_datareview_3 import tools

        line = tools._keep_in_aws_prompt(
            {"identifier": "checkout-sessions", "disposition": "escalate"})

        self.assertLess(len(line), 200)
        self.assertIn("checkout-sessions", line)
        self.assertIn("see the end of this listing", line)
        # The full argument, with its costs, is printed once for the section.
        self.assertGreater(len(tools.KEEP_IN_AWS_OPTION), 400)
        self.assertNotIn(tools.KEEP_IN_AWS_OPTION, line)


class LinesTest(unittest.TestCase):

    def test_a_line_shows_what_matched_and_where_the_chart_is(self):
        ranked, _ = candidates.rank(
            {"identifier": "orders-db"},
            [_workload("orders", namespace="orders",
                       source_path="src/orders/chart")])
        line = candidates.lines(ranked)[0]
        self.assertIn("orders (helm_release, orders)", line)
        self.assertIn("shares orders", line)
        self.assertIn("src/orders/chart", line)

    def test_a_missing_namespace_is_stated_not_blank(self):
        ranked, _ = candidates.rank({"identifier": "orders-db"},
                                    [_workload("orders")])
        self.assertIn("namespace not stated literally",
                      candidates.lines(ranked)[0])


if __name__ == "__main__":
    unittest.main()
