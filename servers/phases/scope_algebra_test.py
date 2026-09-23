"""Unit tests for the shared scope algebra (servers/phases/scope_algebra.py).

Authored with the lift out of discovery_scope_2/scope.py — the module had no
dedicated tests of its own before the move (it was covered only through the
discovery tool flows). Pure logic: no GCS, no filesystem beyond a tempdir for
index_included_paths.
"""

import unittest

from servers.phases import scope_algebra as sa


class NormalizeAndMatchTest(unittest.TestCase):

    def test_normalize_strips_dot_slash_and_trailing_slash(self):
        self.assertEqual(sa.normalize_pattern("./vendor/"), "vendor")
        self.assertEqual(sa.normalize_pattern("  charts/orders/ "), "charts/orders")
        self.assertEqual(sa.normalize_pattern("/"), "")

    def test_matches_exact_prefix_and_glob(self):
        self.assertTrue(sa.matches("charts/orders/Chart.yaml", "charts/orders"))
        self.assertTrue(sa.matches("charts/orders", "charts/orders"))
        self.assertTrue(sa.matches("a/test_x.yaml", "*/test_*.yaml"))
        self.assertFalse(sa.matches("charts/orders-api/x.yaml", "charts/orders"))

    def test_empty_pattern_matches_nothing(self):
        self.assertFalse(sa.matches("anything", ""))
        self.assertFalse(sa.matches("anything", "/"))


class ExclusionTest(unittest.TestCase):

    SCOPE = {"root_dir": "/r", "excluded": ["vendor/"], "included": ["vendor/keep.yaml"]}

    def test_default_is_in_scope(self):
        self.assertFalse(sa.is_excluded("app/dep.yaml", self.SCOPE))

    def test_excluded_pattern_removes(self):
        self.assertTrue(sa.is_excluded("vendor/lib.yaml", self.SCOPE))

    def test_include_wins_over_exclude(self):
        self.assertFalse(sa.is_excluded("vendor/keep.yaml", self.SCOPE))

    def test_filter_files_splits(self):
        files = [{"path": "app/dep.yaml"}, {"path": "vendor/lib.yaml"}]
        kept, removed = sa.filter_files(files, self.SCOPE)
        self.assertEqual([f["path"] for f in kept], ["app/dep.yaml"])
        self.assertEqual([f["path"] for f in removed], ["vendor/lib.yaml"])


class ApplyScopeUpdateTest(unittest.TestCase):

    def base(self):
        return {"root_dir": "/r", "excluded": [], "included": []}

    def test_input_scope_is_not_mutated(self):
        scope = self.base()
        sa.apply_scope_update(scope, exclude=["vendor/"], include=["extra/"])
        self.assertEqual(scope, self.base())

    def test_exclude_appends_and_dedupes(self):
        scope, _ = sa.apply_scope_update(self.base(), exclude=["vendor/", "vendor"])
        self.assertEqual(scope["excluded"], ["vendor"])

    def test_exclude_drops_identical_include_first(self):
        start = {"root_dir": "/r", "excluded": [], "included": ["vendor"]}
        scope, notes = sa.apply_scope_update(start, exclude=["vendor"])
        self.assertEqual(scope["included"], [])
        self.assertEqual(scope["excluded"], ["vendor"])
        self.assertIn("'vendor' removed from includes", notes)

    def test_include_unexcludes_exact_match(self):
        start = {"root_dir": "/r", "excluded": ["vendor"], "included": []}
        scope, notes = sa.apply_scope_update(start, include=["vendor"])
        self.assertEqual(scope["excluded"], [])
        self.assertEqual(scope["included"], [])
        self.assertIn("'vendor' un-excluded", notes)

    def test_include_appends_new_pattern(self):
        scope, notes = sa.apply_scope_update(self.base(), include=["charts/orders/"])
        self.assertEqual(scope["included"], ["charts/orders"])
        self.assertIn("'charts/orders' included", notes)

    def test_blank_entries_are_ignored(self):
        scope, notes = sa.apply_scope_update(self.base(), exclude=["  ", "/"], include=[""])
        self.assertEqual(scope, self.base())
        self.assertEqual(notes, [])

    def test_include_first_also_lands_the_unexcluded_pattern_in_included(self):
        # Default-OUT algebra (component scopes select from nothing): an
        # un-exclude alone selects nothing, so the pattern must also join the
        # selection basis.
        start = {"root_dir": "/r", "excluded": ["vendor"], "included": []}
        scope, notes = sa.apply_scope_update(
            start, include=["vendor"], include_first=True)
        self.assertEqual(scope["excluded"], [])
        self.assertEqual(scope["included"], ["vendor"])
        self.assertIn("'vendor' un-excluded", notes)
        self.assertIn("'vendor' included", notes)

    def test_include_first_toggle_round_trip(self):
        # include X -> exclude X -> include X must end with X included.
        scope = {"root_dir": "/r", "excluded": [], "included": []}
        scope, _ = sa.apply_scope_update(scope, include=["x"], include_first=True)
        scope, _ = sa.apply_scope_update(scope, exclude=["x"], include_first=True)
        self.assertEqual((scope["included"], scope["excluded"]), ([], ["x"]))
        scope, _ = sa.apply_scope_update(scope, include=["x"], include_first=True)
        self.assertEqual((scope["included"], scope["excluded"]), (["x"], []))


class ReExportTest(unittest.TestCase):

    def test_discovery_scope_module_reexports_this_algebra(self):
        from servers.phases.discovery.discovery_scope_2 import scope as legacy
        self.assertIs(legacy.apply_scope_update, sa.apply_scope_update)
        self.assertIs(legacy.matches, sa.matches)


if __name__ == "__main__":
    unittest.main()
