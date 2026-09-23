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

"""Unit tests for the decision registry and the single cluster-mode projection.

Stdlib only and python 3.9 syntax: this module runs in the Kokoro presubmit
with only `servers/dag` on sys.path, beside dag_validation.
"""

import unittest

from server import decisions

NAP, AP = decisions.KARPENTER_NAP, decisions.KARPENTER_AUTOPILOT
STD, BYP = decisions.PRIVILEGED_STANDARD, decisions.PRIVILEGED_BYPASS
GS, GA = decisions.GPU_STANDARD, decisions.GPU_AUTOPILOT

_HEADER = ("| `decision_id` | Trigger in the source estate | Choice | Take it when | Implies mode |\n"
           "|---|---|---|---|---|\n")
_DEFAULTS = ("\n## Standing defaults\n\n"
             "| Setting | Default | Deviate when | Fact path | Expected |\n"
             "|---|---|---|---|---|\n"
             "| Cluster availability | Regional | Zonal for test | | |\n")


def _doc(rows, defaults=_DEFAULTS):
    return ("# GKE Landing Zone\n\n## The four target-shape decisions\n\nprose\n\n"
            + _HEADER + "\n".join(rows) + "\n" + defaults + "\n## What to produce\n")


SYNTH_ROWS = [
    "| `karpenter` | `karpenter.sh` NodePool | `GKE_STANDARD_NAP` | real work | standard |",
    "| | | `GKE_AUTOPILOT` | homogeneous | autopilot |",
    "| | `count(autoscaling.karpenter_nodepools) > 1 or any(autoscaling.karpenter_nodepools[].capacity_types, len > 1)` | `GKE_STANDARD_COMPUTECLASS` | a ladder | standard |",
    "| `privileged_daemonsets` | hostNetwork DaemonSets | `GKE_STANDARD` | load-bearing | standard |",
    "| | | `GKE_AUTOPILOT_BYPASS` | small set | autopilot |",
    "| `gpu_tpu` | nvidia.com/gpu | `GKE_STANDARD_SPECIALIZED` | pinned drivers | advisory:standard |",
    "| | | `GKE_AUTOPILOT_SPECIALIZED` | bursty | advisory:autopilot |",
    "| `vpc_peering` | peering | `PUBLIC_AUTHORIZED_NETS` | no VPN | — |",
    "| | | `PRIVATE_ONLY_PEERING` | VPN exists | — |",
]
SYNTH = decisions.parse_decision_table(_doc(SYNTH_ROWS), "synthetic")

ALL_T = {"karpenter": True, "privileged_daemonsets": True, "gpu_tpu": True, "vpc_peering": True}


def _t(**fired):
    triggers = {k: False for k in ALL_T}
    triggers.update(fired)
    return triggers


class RealDocumentTest(unittest.TestCase):
    """The bundled gke-landing-zone.md is the authority; pin its shape."""

    @classmethod
    def setUpClass(cls):
        cls.reg = decisions.load_registry(refresh=True)

    def test_exactly_four_decision_ids(self):
        self.assertEqual(self.reg["order"],
                         ["karpenter", "privileged_daemonsets", "gpu_tpu", "vpc_peering"])

    def test_every_choice_row_has_a_known_implies_value(self):
        for did in self.reg["order"]:
            for choice in self.reg["decisions"][did]["choices"]:
                self.assertIn(choice["implies"], decisions.IMPLIES_VALUES, choice["token"])

    def test_every_declared_constant_is_in_the_table(self):
        decisions.validate_decisions()
        tokens = set(decisions.all_tokens(self.reg))
        for token in decisions.REQUIRED_TOKENS:
            self.assertIn(token, tokens)

    def test_choices_matches_the_historical_valid_choices_shape(self):
        ch = decisions.choices(self.reg)
        self.assertEqual(ch["privileged_daemonsets"], (STD, BYP))
        self.assertEqual(ch["gpu_tpu"], (GS, GA))
        self.assertEqual(ch["vpc_peering"], (decisions.PEERING_PUBLIC, decisions.PEERING_PRIVATE))
        self.assertEqual(ch["karpenter"], (NAP, AP, decisions.KARPENTER_COMPUTECLASS))

    def test_the_computeclass_row_carries_the_predicate_and_implies_standard(self):
        choice = self.reg["decisions"]["karpenter"]["choices"][2]
        self.assertEqual(choice["token"], decisions.KARPENTER_COMPUTECLASS)
        self.assertEqual(choice["implies"], "standard")
        self.assertIsNotNone(choice["predicate"])
        self.assertEqual([a["op"] for a in choice["predicate"]["atoms"]], ["len", "len", "contains"])
        # A capacity-type requirement the merger could not reduce (NotIn, Exists) is
        # a ladder the derived list cannot show; the third atom reads the marker.
        unreduced = {"autoscaling": {"karpenter_nodepools": [
            {"name": "p", "capacity_types": [], "instance_families": [],
             "requirements_unreduced": ["karpenter.sh/capacity-type"]}]}}
        self.assertEqual(decisions.recommend("karpenter", unreduced, self.reg)[0],
                         decisions.KARPENTER_COMPUTECLASS)
        self.assertNotIn("count(", choice["predicate"]["text"])  # tied to Q4, design §3.2

    def test_recommend_fires_on_the_acme_shaped_pool_and_not_on_an_empty_list(self):
        acme = {"autoscaling": {"karpenter": True, "karpenter_nodepools": [
            {"name": "shop-burst", "capacity_types": ["spot", "on-demand"],
             "instance_families": [], "architectures": ["amd64"]}]}}
        token, reason = decisions.recommend("karpenter", acme, self.reg)
        self.assertEqual(token, decisions.KARPENTER_COMPUTECLASS)
        self.assertIn("shop-burst.capacity_types has 2 entries", reason)
        self.assertEqual(decisions.recommend(
            "karpenter", {"autoscaling": {"karpenter": True, "karpenter_nodepools": []}}, self.reg)[0], None)
        # Two single-shape pools do not fire: the count atom is deliberately absent.
        two = {"autoscaling": {"karpenter_nodepools": [
            {"name": "a", "capacity_types": ["spot"]}, {"name": "b", "capacity_types": ["spot"]}]}}
        self.assertEqual(decisions.recommend("karpenter", two, self.reg)[0], None)
        self.assertEqual(decisions.recommend("karpenter", {}, self.reg)[0], None)

    def test_voting_and_advisory_ids(self):
        self.assertEqual(decisions.voting_ids(self.reg), ["karpenter", "privileged_daemonsets"])
        self.assertEqual(decisions.advisory_ids(self.reg), ["gpu_tpu"])

    def test_defaults_table_parses_with_the_two_empty_columns(self):
        rows = self.reg["defaults"]
        self.assertTrue(any(r["setting"].startswith("Cluster availability") for r in rows))
        for row in rows:
            self.assertEqual((row["fact_path"] is None), (row["expected"] is None), row["setting"])


class ParseTest(unittest.TestCase):

    def test_carry_forward_and_predicate(self):
        k = SYNTH["decisions"]["karpenter"]
        self.assertEqual([c["token"] for c in k["choices"]],
                         [NAP, AP, decisions.KARPENTER_COMPUTECLASS])
        self.assertIsNone(k["choices"][0]["predicate"])
        self.assertIsNone(k["choices"][1]["predicate"])
        self.assertEqual(len(k["choices"][2]["predicate"]["atoms"]), 2)
        self.assertEqual(k["trigger"], "`karpenter.sh` NodePool")

    def test_continuation_without_a_preceding_id_fails(self):
        with self.assertRaises(decisions.DecisionsError):
            decisions.parse_decision_table(
                _doc(["| | | `GKE_AUTOPILOT` | x | autopilot |"]), "s")

    def test_pipe_inside_prose_fails(self):
        with self.assertRaises(decisions.DecisionsError):
            decisions.parse_decision_table(
                _doc(["| `karpenter` | a | `GKE_STANDARD_NAP` | x | y | standard |"]), "s")

    def test_duplicate_token_fails(self):
        with self.assertRaises(decisions.DecisionsError):
            decisions.parse_decision_table(_doc([
                "| `a` | t | `TOK` | x | standard |",
                "| `b` | t | `TOK` | x | standard |"]), "s")

    def test_unknown_implies_value_fails(self):
        with self.assertRaises(decisions.DecisionsError):
            decisions.parse_decision_table(
                _doc(["| `a` | t | `TOK` | x | maybe |"]), "s")

    def test_a_first_row_trigger_is_prose_never_a_predicate(self):
        # The default (first) choice carries no predicate by construction: a
        # first row's trigger cell is the decision's trigger description even
        # when it is written entirely in backticks.
        reg = decisions.parse_decision_table(_doc([
            "| `a` | t | `TOK` | x | standard |",
            "| `b` | `count(x) > 1` | `TOK2` | x | standard |"]), "s")
        self.assertIsNone(reg["decisions"]["b"]["choices"][0]["predicate"])
        self.assertEqual(reg["decisions"]["b"]["trigger"], "`count(x) > 1`")

    def test_unparseable_predicate_fails(self):
        with self.assertRaises(decisions.DecisionsError) as cm:
            decisions.parse_decision_table(_doc([
                "| `a` | t | `TOK` | x | standard |",
                "| | `something(x) > 1` | `TOK2` | x | standard |"]), "s")
        self.assertIn("grammar", str(cm.exception))

    def test_unbackticked_continuation_trigger_fails(self):
        with self.assertRaises(decisions.DecisionsError):
            decisions.parse_decision_table(_doc([
                "| `a` | t | `TOK` | x | standard |",
                "| | count(x) > 1 | `TOK2` | x | standard |"]), "s")

    def test_half_filled_defaults_row_fails(self):
        bad = _DEFAULTS.replace("| Zonal for test | | |", "| Zonal for test | `clusters[].x` | |")
        with self.assertRaises(decisions.DecisionsError):
            decisions.parse_decision_table(_doc(SYNTH_ROWS, bad), "s")

    def test_missing_implies_column_fails(self):
        text = _doc(SYNTH_ROWS).replace(" | Implies mode |", " |").replace("|---|---|---|---|---|\n|", "|---|---|---|---|\n|", 1)
        with self.assertRaises(decisions.DecisionsError):
            decisions.parse_decision_table(text, "s")


class ModeOfTest(unittest.TestCase):

    def test_every_token(self):
        self.assertEqual(decisions.mode_of(NAP, SYNTH), ("standard", False))
        self.assertEqual(decisions.mode_of(AP, SYNTH), ("autopilot", False))
        self.assertEqual(decisions.mode_of(decisions.KARPENTER_COMPUTECLASS, SYNTH), ("standard", False))
        self.assertEqual(decisions.mode_of(STD, SYNTH), ("standard", False))
        self.assertEqual(decisions.mode_of(BYP, SYNTH), ("autopilot", False))
        self.assertEqual(decisions.mode_of(GS, SYNTH), ("standard", True))
        self.assertEqual(decisions.mode_of(GA, SYNTH), ("autopilot", True))
        self.assertEqual(decisions.mode_of(decisions.PEERING_PUBLIC, SYNTH), (None, False))

    def test_never_raises(self):
        self.assertEqual(decisions.mode_of(None, SYNTH), (None, False))
        self.assertEqual(decisions.mode_of("NOT_A_TOKEN", SYNTH), (None, False))
        self.assertEqual(decisions.mode_of(["x"], SYNTH), (None, False))


class ProjectionTest(unittest.TestCase):
    """The twelve compatibility fixtures of coverage-guards v9 G0, each with
    triggers, without triggers, and with a triggers dict missing one voting id
    (both latter regimes reproduce the pre-registry exports value)."""

    # (choices, triggers, expected with triggers, expected without/partial)
    FIXTURES = [
        ({"karpenter": NAP, "privileged_daemonsets": STD}, _t(karpenter=True), "standard", "standard"),
        ({"karpenter": AP, "privileged_daemonsets": BYP, "gpu_tpu": GS},
         _t(karpenter=True, privileged_daemonsets=True), "autopilot", "autopilot"),
        ({"karpenter": AP, "privileged_daemonsets": STD},
         _t(karpenter=True, privileged_daemonsets=True), None, None),
        ({"karpenter": AP, "privileged_daemonsets": STD}, _t(karpenter=True), "autopilot", None),
        ({"karpenter": NAP, "privileged_daemonsets": BYP}, _t(privileged_daemonsets=True), "autopilot", None),
        ({"karpenter": AP, "privileged_daemonsets": STD}, _t(privileged_daemonsets=True), None, None),
        ({"karpenter": AP, "privileged_daemonsets": BYP, "gpu_tpu": GS},
         _t(karpenter=True, privileged_daemonsets=True, gpu_tpu=True), "autopilot", "autopilot"),
        ({"privileged_daemonsets": BYP}, _t(privileged_daemonsets=True), "autopilot", "autopilot"),
        ({}, _t(), None, None),
        ({"karpenter": NAP, "privileged_daemonsets": BYP}, _t(), "autopilot", None),
        ({"karpenter": AP, "privileged_daemonsets": STD}, _t(), "autopilot", None),
        ({"karpenter": NAP, "privileged_daemonsets": STD}, _t(), "standard", "standard"),
    ]

    def test_with_triggers(self):
        for i, (ch, tr, with_t, _without) in enumerate(self.FIXTURES, 1):
            with self.subTest(row=i):
                self.assertEqual(decisions.cluster_mode(ch, tr, SYNTH)[0], with_t)

    def test_without_triggers_and_with_a_partial_dict(self):
        for i, (ch, _tr, _with, without) in enumerate(self.FIXTURES, 1):
            with self.subTest(row=i, regime="none"):
                self.assertEqual(decisions.cluster_mode(ch, None, SYNTH)[0], without)
            with self.subTest(row=i, regime="no-triggers-key"):
                self.assertEqual(decisions.cluster_mode(ch, {}, SYNTH)[0], without)
            partial = {"karpenter": True, "gpu_tpu": True, "vpc_peering": False}  # no privileged
            with self.subTest(row=i, regime="partial"):
                self.assertEqual(decisions.cluster_mode(ch, partial, SYNTH)[0], without)

    def test_a_dict_missing_only_gpu_tpu_stays_trigger_aware(self):
        triggers = {"karpenter": True, "privileged_daemonsets": False, "vpc_peering": False}
        mode, reason = decisions.cluster_mode(
            {"karpenter": AP, "privileged_daemonsets": STD, "gpu_tpu": GS}, triggers, SYNTH)
        self.assertEqual(mode, "autopilot")   # row 4: the default STD is mute
        self.assertEqual(decisions.advisory_mismatches(
            {"karpenter": AP, "privileged_daemonsets": STD, "gpu_tpu": GS}, triggers, SYNTH), [])

    def test_reasons(self):
        self.assertEqual(decisions.cluster_mode({}, _t(), SYNTH), (None, "none recorded"))
        self.assertEqual(decisions.cluster_mode({"vpc_peering": decisions.PEERING_PRIVATE}, _t(), SYNTH),
                         (None, "none recorded"))
        mode, reason = decisions.cluster_mode(
            {"karpenter": AP, "privileged_daemonsets": STD}, _t(karpenter=True, privileged_daemonsets=True), SYNTH)
        self.assertIsNone(mode)
        self.assertEqual(reason, f"disagree: karpenter={AP}, privileged_daemonsets={STD}")

    def test_disagree_reason_names_a_considered_advisory_too(self):
        _mode, reason = decisions.cluster_mode(
            {"karpenter": AP, "privileged_daemonsets": STD, "gpu_tpu": GA},
            _t(karpenter=True, privileged_daemonsets=True, gpu_tpu=True), SYNTH)
        self.assertIn(f"gpu_tpu={GA}", reason)

    def test_advisory_mismatch_row_7_yes_row_2_no(self):
        row7 = ({"karpenter": AP, "privileged_daemonsets": BYP, "gpu_tpu": GS},
                _t(karpenter=True, privileged_daemonsets=True, gpu_tpu=True))
        self.assertEqual(decisions.advisory_mismatches(*row7, SYNTH), [("gpu_tpu", GS, "standard")])
        row2 = ({"karpenter": AP, "privileged_daemonsets": BYP, "gpu_tpu": GS},
                _t(karpenter=True, privileged_daemonsets=True))
        self.assertEqual(decisions.advisory_mismatches(*row2, SYNTH), [])
        # Under the fallback advisories are never considered.
        self.assertEqual(decisions.advisory_mismatches(row7[0], None, SYNTH), [])

    def test_non_string_values_read_as_unrecorded(self):
        self.assertEqual(decisions.cluster_mode({"karpenter": {"odd": 1}, "gpu_tpu": ["x"]}, _t(), SYNTH),
                         (None, "none recorded"))
        self.assertEqual(decisions.cluster_mode({"karpenter": ["GKE_AUTOPILOT"]}, None, SYNTH),
                         (None, "none recorded"))

    def test_resolved_dict_form_is_unwrapped(self):
        self.assertEqual(decisions.cluster_mode({"karpenter": {"choice": AP}}, _t(karpenter=True), SYNTH)[0],
                         "autopilot")

    def test_computeclass_token_votes_standard(self):
        self.assertEqual(decisions.cluster_mode(
            {"karpenter": decisions.KARPENTER_COMPUTECLASS, "privileged_daemonsets": STD},
            _t(karpenter=True), SYNTH)[0], "standard")
        self.assertEqual(decisions.cluster_mode(
            {"karpenter": decisions.KARPENTER_COMPUTECLASS, "privileged_daemonsets": BYP},
            _t(karpenter=True, privileged_daemonsets=True), SYNTH)[0], None)


class DerivedValuesTest(unittest.TestCase):
    CC = decisions.KARPENTER_COMPUTECLASS

    def test_karpenter_replacement_table(self):
        cases = [
            ({"karpenter": NAP, "privileged_daemonsets": STD}, _t(karpenter=True), "nap"),
            ({"karpenter": self.CC, "privileged_daemonsets": STD}, _t(karpenter=True), "computeclass"),
            ({"privileged_daemonsets": STD}, _t(privileged_daemonsets=True), "none"),
            ({"karpenter": AP, "privileged_daemonsets": BYP}, _t(karpenter=True), "none"),
            # Row 12: NAP recorded as a default on an unfired trigger replaces nothing.
            ({"karpenter": NAP, "privileged_daemonsets": STD}, _t(), "none"),
            # Without triggers every recorded choice votes, so NAP counts.
            ({"karpenter": NAP, "privileged_daemonsets": STD}, None, "nap"),
            ({"karpenter": self.CC, "privileged_daemonsets": STD}, None, "computeclass"),
        ]
        for choices_, triggers, expected in cases:
            with self.subTest(choices=choices_, triggers=triggers):
                self.assertEqual(decisions.karpenter_replacement(choices_, triggers, SYNTH)[0], expected)

    def test_the_reason_never_names_a_karpenter_decision_that_was_not_recorded(self):
        value, reason = decisions.karpenter_replacement({"privileged_daemonsets": STD}, _t(), SYNTH)
        self.assertEqual((value, reason), ("none", "no Karpenter replacement was decided"))
        value, reason = decisions.karpenter_replacement(
            {"karpenter": NAP, "privileged_daemonsets": STD}, _t(), SYNTH)
        self.assertEqual(value, "none")
        self.assertIn("default on an unfired trigger", reason)

    def test_unresolved_mode_is_none_with_the_projection_reason(self):
        value, reason = decisions.karpenter_replacement(
            {"karpenter": AP, "privileged_daemonsets": STD},
            _t(karpenter=True, privileged_daemonsets=True), SYNTH)
        self.assertIsNone(value)
        self.assertTrue(reason.startswith("disagree"))
        self.assertEqual(decisions.nap_enabled({}, _t(), SYNTH), (None, "none recorded"))

    def test_nap_enabled_follows_the_replacement(self):
        self.assertEqual(decisions.nap_enabled({"karpenter": NAP, "privileged_daemonsets": STD}, _t(karpenter=True), SYNTH)[0], True)
        self.assertEqual(decisions.nap_enabled({"karpenter": self.CC, "privileged_daemonsets": STD}, _t(karpenter=True), SYNTH)[0], False)
        self.assertEqual(decisions.nap_enabled({"karpenter": NAP, "privileged_daemonsets": STD}, _t(), SYNTH)[0], False)
        self.assertEqual(decisions.nap_enabled({"karpenter": AP, "privileged_daemonsets": BYP}, _t(karpenter=True), SYNTH)[0], False)


class RecommendTest(unittest.TestCase):
    POOLS = {"autoscaling": {"karpenter": True, "karpenter_nodepools": [
        {"name": "shop-burst", "capacity_types": ["spot", "on-demand"], "instance_families": []}]}}

    def test_fires_on_a_mixed_capacity_pool(self):
        token, reason = decisions.recommend("karpenter", self.POOLS, SYNTH)
        self.assertEqual(token, decisions.KARPENTER_COMPUTECLASS)
        self.assertIn("shop-burst.capacity_types has 2 entries", reason)

    def test_fires_on_two_pools(self):
        inv = {"autoscaling": {"karpenter_nodepools": [
            {"name": "a", "capacity_types": ["spot"]}, {"name": "b", "capacity_types": ["spot"]}]}}
        token, reason = decisions.recommend("karpenter", inv, SYNTH)
        self.assertEqual(token, decisions.KARPENTER_COMPUTECLASS)
        self.assertIn("count(autoscaling.karpenter_nodepools) = 2", reason)

    def test_absent_typed_field_recommends_nothing_and_says_absent(self):
        self.assertEqual(decisions.recommend("karpenter", {"autoscaling": {"karpenter": True}}, SYNTH),
                         (None, "the typed field the predicates read is absent from the inventory"))
        self.assertEqual(decisions.recommend("karpenter", {}, SYNTH)[0], None)
        # A present but empty list is a recorded absence: nothing fires.
        self.assertEqual(decisions.recommend(
            "karpenter", {"autoscaling": {"karpenter_nodepools": []}}, SYNTH),
            (None, "no predicate fired on the recorded facts"))

    def test_len_atom_reads_lists_only(self):
        inv = {"autoscaling": {"karpenter_nodepools": [{"name": "p", "capacity_types": "spot,on-demand"}]}}
        self.assertEqual(decisions.recommend("karpenter", inv, SYNTH)[0], None)

    def test_triggers_available(self):
        self.assertTrue(decisions.triggers_available(_t(), SYNTH))
        self.assertFalse(decisions.triggers_available({"karpenter": True}, SYNTH))
        self.assertFalse(decisions.triggers_available(None, SYNTH))
        self.assertTrue(decisions.triggers_available({"karpenter": False, "privileged_daemonsets": False}, SYNTH))

    def test_a_homogeneous_pool_recommends_nothing(self):
        inv = {"autoscaling": {"karpenter_nodepools": [{"name": "a", "capacity_types": ["spot"]}]}}
        self.assertEqual(decisions.recommend("karpenter", inv, SYNTH)[0], None)

    def test_decisions_without_predicates_recommend_nothing(self):
        self.assertEqual(decisions.recommend("vpc_peering", self.POOLS, SYNTH),
                         (None, "no choice of this decision carries a predicate"))
        self.assertEqual(decisions.recommend("nope", self.POOLS, SYNTH)[0], None)

    def test_contains_atom(self):
        reg = decisions.parse_decision_table(_doc([
            "| `a` | t | `TOK` | x | standard |",
            "| | `any(pools[].types, contains spot)` | `TOK2` | x | standard |"]), "s")
        self.assertEqual(decisions.recommend("a", {"pools": [{"name": "p", "types": ["spot"]}]}, reg)[0], "TOK2")
        self.assertEqual(decisions.recommend("a", {"pools": [{"name": "p", "types": ["od"]}]}, reg)[0], None)


if __name__ == "__main__":
    unittest.main()
