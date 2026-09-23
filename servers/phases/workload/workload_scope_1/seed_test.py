"""Unit tests for the pure seed-index logic of the workload scope step.

Terrain notes per fact regime, the token guess (including zero-match
honesty), the AND-combined filters, and the include-first scope resolution
(the deliberate inversion of the discovery algebra). Pure: no GCS.
"""

import unittest

from servers.phases.workload.workload_scope_1 import seed


def entry(kinds=(), namespaces=(), team_labels=()):
    return {"kinds": list(kinds), "namespaces": list(namespaces),
            "team_labels": list(team_labels)}


MULTI_NS = {
    "apps/orders/dep.yaml": entry(["Deployment"], ["orders"], ["team-orders"]),
    "apps/cart/dep.yaml": entry(["Deployment"], ["cart"], ["team-cart"]),
    "apps/cart/svc.yaml": entry(["Service"], ["cart"]),
    "infra/sc.yaml": entry(["StorageClass"]),
}

SINGLE_NS = {
    "charts/orders/Chart.yaml": entry(["helm-chart"]),
    "apps/a.yaml": entry(["Deployment"], ["acme-shop"]),
    "apps/b.yaml": entry(["Service"], ["acme-shop"]),
}


class TerrainNoteTest(unittest.TestCase):

    def test_empty_index_is_stated_honestly(self):
        note = seed.terrain_note({})
        self.assertIn("empty", note)

    def test_multi_namespace_estate_names_namespace_as_a_signal(self):
        note = seed.terrain_note(MULTI_NS)
        self.assertIn("4 file(s)", note)
        self.assertIn("namespaces recorded: 2", note)
        self.assertIn("usable ownership signal", note)
        # D20: a signal, never a boundary.
        self.assertIn("not a component boundary", note)

    def test_single_namespace_estate_says_namespace_does_not_discriminate(self):
        note = seed.terrain_note(SINGLE_NS)
        self.assertIn("does not discriminate", note)
        self.assertIn("acme-shop", note)
        self.assertIn("path prefixes", note)

    def test_team_label_coverage_is_reported_when_present(self):
        note = seed.terrain_note(MULTI_NS)
        self.assertIn("team labels present on 2 of 4", note)
        self.assertIn("team-cart", note)
        self.assertIn("team-orders", note)

    def test_label_free_estate_says_no_labels(self):
        note = seed.terrain_note(SINGLE_NS)
        self.assertIn("no entry records a *team*-suffixed label", note)

    def test_no_namespace_metadata_count_and_kinds_histogram(self):
        note = seed.terrain_note(SINGLE_NS)
        self.assertIn("1 file(s) carry no namespace metadata", note)
        self.assertIn("kinds:", note)
        self.assertIn("Deployment x1", note)

    def test_cluster_scoped_kinds_are_flagged_as_platform_owned(self):
        note = seed.terrain_note(MULTI_NS)
        self.assertIn("StorageClass", note)
        self.assertIn("platform-owned", note)

    def test_no_namespace_estate_conditions_the_claim(self):
        note = seed.terrain_note({"a.tf": entry(["terraform"])})
        self.assertIn("no namespace metadata is recorded", note)


class TokenGuessTest(unittest.TestCase):

    def test_matches_are_labeled_a_guess(self):
        matched, note = seed.token_guess(MULTI_NS, "orders-component")
        self.assertEqual(matched, ["apps/orders/dep.yaml"])
        self.assertIn("GUESS", note)
        self.assertNotIn("component'", note)  # the suffix token is dropped

    def test_zero_match_is_honest(self):
        matched, note = seed.token_guess(MULTI_NS, "zzz-component")
        self.assertEqual(matched, [])
        self.assertIn("matched nothing", note)
        self.assertIn("does NOT mean no candidates exist", note)

    def test_matches_namespaces_and_team_labels_too(self):
        index = {"x/dep.yaml": entry(["Deployment"], ["orders"], [])}
        matched, _ = seed.token_guess(index, "orders-component")
        self.assertEqual(matched, ["x/dep.yaml"])

    def test_tokenless_id_offers_no_guess(self):
        matched, note = seed.token_guess(MULTI_NS, "component")
        self.assertEqual(matched, [])
        self.assertIn("no usable tokens", note)


class FilterSeedTest(unittest.TestCase):

    def test_no_filters_returns_everything(self):
        self.assertEqual(seed.filter_seed(MULTI_NS), MULTI_NS)

    def test_path_glob_uses_the_shared_matcher(self):
        result = seed.filter_seed(MULTI_NS, path_glob="apps/cart/")
        self.assertEqual(sorted(result), ["apps/cart/dep.yaml", "apps/cart/svc.yaml"])

    def test_filters_and_combine(self):
        result = seed.filter_seed(MULTI_NS, path_glob="apps/",
                                  namespace="cart", team_label="team-cart")
        self.assertEqual(sorted(result), ["apps/cart/dep.yaml"])

    def test_token_filter_is_substring(self):
        result = seed.filter_seed(MULTI_NS, token="orders")
        self.assertEqual(sorted(result), ["apps/orders/dep.yaml"])


class ResolveComponentScopeTest(unittest.TestCase):

    PATHS = list(MULTI_NS)

    def test_nothing_included_resolves_empty(self):
        # The inversion: a component scope selects from nothing.
        self.assertEqual(
            seed.resolve_component_scope(self.PATHS, {"included": [], "excluded": []}),
            [])

    def test_include_selects_by_prefix(self):
        resolved = seed.resolve_component_scope(
            self.PATHS, {"included": ["apps/cart/"], "excluded": []})
        self.assertEqual(resolved, ["apps/cart/dep.yaml", "apps/cart/svc.yaml"])

    def test_exclude_carves_out_of_the_selection(self):
        resolved = seed.resolve_component_scope(
            self.PATHS,
            {"included": ["apps/"], "excluded": ["apps/cart/svc.yaml"]})
        self.assertEqual(resolved, ["apps/cart/dep.yaml", "apps/orders/dep.yaml"])

    def test_exclude_beats_include_here_unlike_discovery(self):
        resolved = seed.resolve_component_scope(
            self.PATHS, {"included": ["apps/cart/"], "excluded": ["apps/cart/"]})
        self.assertEqual(resolved, [])


class ClusterScopedTest(unittest.TestCase):

    def test_cluster_scoped_kinds_detected(self):
        self.assertEqual(seed.cluster_scoped_in(
            entry(["StorageClass", "Deployment", "Provisioner"])),
            ["Provisioner", "StorageClass"])
        self.assertEqual(seed.cluster_scoped_in(entry(["Deployment"])), [])


if __name__ == "__main__":
    unittest.main()
