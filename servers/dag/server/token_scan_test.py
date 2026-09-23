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

"""No decision token literal outside the registry (coverage-guards G0 step 5).

Every reader of a landing-zone choice goes through `decisions.cluster_mode`,
`decisions.mode_of` or an imported constant. A string literal (or docstring)
containing a token anywhere else under servers/ is a second interpreter of the
token waiting to drift, so this test computes the token set FROM THE PARSED
TABLE and fails on any hit. Comments are not hits (`tokenize` tells them
apart); test modules are exempt (fixtures need literals); `tests/**` is out of
scope. Stdlib only, runs in the presubmit.
"""

import io
import os
import tokenize
import unittest

from server import decisions

SERVERS_ROOT = os.path.join(decisions.REPO_ROOT, "servers")
EXEMPT_FILES = {os.path.join(SERVERS_ROOT, "dag", "server", "decisions.py")}
SKIP_DIR_NAMES = {"__pycache__", "scratch"}


def _python_files():
    for dirpath, dirnames, filenames in os.walk(SERVERS_ROOT):
        dirnames[:] = sorted(d for d in dirnames
                             if not d.startswith(".") and d not in SKIP_DIR_NAMES)
        for name in sorted(filenames):
            if not name.endswith(".py") or name.endswith("_test.py"):
                continue
            path = os.path.join(dirpath, name)
            if path in EXEMPT_FILES:
                continue
            yield path


def token_literal_hits(tokens, paths=None):
    """[(path, line, literal)] for every string literal containing a token."""
    hits = []
    for path in (paths if paths is not None else _python_files()):
        with open(path, "r", encoding="utf-8") as f:
            source = f.read()
        # Python 3.12+ tokenizes f-strings as FSTRING_START/MIDDLE/END, so the
        # literal text of an f-string is a MIDDLE token, not a STRING.
        string_types = {tokenize.STRING}
        if hasattr(tokenize, "FSTRING_MIDDLE"):
            string_types.add(tokenize.FSTRING_MIDDLE)
        for tok in tokenize.generate_tokens(io.StringIO(source).readline):
            if tok.type not in string_types:
                continue
            for token in tokens:
                if token in tok.string:
                    hits.append((os.path.relpath(path, decisions.REPO_ROOT), tok.start[0], token))
                    break
    return hits


class TokenScanTest(unittest.TestCase):

    def test_no_token_literal_outside_the_registry(self):
        tokens = decisions.all_tokens(decisions.load_registry(refresh=True))
        # Substring containment on purpose: GKE_STANDARD_COMPUTECLASS is a hit
        # through GKE_STANDARD even before its own row lands.
        hits = token_literal_hits(tokens)
        self.assertEqual(hits, [], "decision token literals outside decisions.py:\n"
                         + "\n".join(f"  {p}:{l}: {t}" for p, l, t in hits))

    def test_the_scanner_sees_strings_and_docstrings_but_not_comments(self):
        import tempfile
        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, "m.py")
            with open(path, "w", encoding="utf-8") as f:
                f.write('"""doc GKE_AUTOPILOT"""\n# GKE_STANDARD_NAP comment\nx = "GKE_STANDARD"\n'
                        'y = f"mode GKE_AUTOPILOT_BYPASS {x}"\n')
            hits = token_literal_hits(["GKE_STANDARD_NAP", "GKE_AUTOPILOT", "GKE_STANDARD",
                                       "GKE_AUTOPILOT_BYPASS"], [path])
        self.assertEqual([(l, t) for _p, l, t in hits],
                         [(1, "GKE_AUTOPILOT"), (3, "GKE_STANDARD"), (4, "GKE_AUTOPILOT")])


if __name__ == "__main__":
    unittest.main()
