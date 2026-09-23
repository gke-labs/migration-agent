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

"""Unit tests for the annotation disposition parser. Pure — reads the real
document."""

import unittest

from server import api_translation


def _table(rows):
    """A minimal document holding the given table rows."""
    body = "\n".join(rows)
    return (
        "# Title\n\n## Service / Ingress\n\n### Service annotations\n\n"
        "| a | b | c |\n|---|---|---|\n| `x/y` | not a disposition | z |\n\n"
        "### Ingress / Gateway annotations\n\nprose\n\n"
        "| Annotation | Disposition | Target / rationale |\n"
        "|---|---|---|\n"
        f"{body}\n\n## Next section\n"
    )


ROWS = [
    "| `alb.ingress.kubernetes.io/scheme` | dropped with tradeoff | Gateway |",
    "| `alb.ingress.kubernetes.io/auth-*` | open question | IAP |",
    "| `alb.ingress.kubernetes.io/auth-type` | mapped | exact wins |",
    "| `alb.ingress.kubernetes.io/healthcheck-*` | open question | probes |",
    "| `alb.ingress.kubernetes.io/healthcheck-path-*` | mapped | longer |",
    "| `cert-manager.io/*` | dropped with tradeoff | Gateway TLS |",
]


class RealDocumentTest(unittest.TestCase):
    """The bundled api-translation.md is the authority; pin its load-bearing
    shape."""

    @classmethod
    def setUpClass(cls):
        cls.table = api_translation.load_annotation_dispositions(refresh=True)

    def test_every_row_uses_the_fixed_vocabulary(self):
        for kind in ("exact", "prefix"):
            for key, (disposition, rationale) in self.table[kind].items():
                self.assertIn(disposition, api_translation.DISPOSITIONS, key)
                self.assertTrue(rationale, key)

    def test_all_three_dispositions_are_represented(self):
        used = {d for kind in ("exact", "prefix")
                for d, _ in self.table[kind].values()}
        self.assertEqual(used, set(api_translation.DISPOSITIONS))

    def test_the_acme_estate_annotations_have_rows(self):
        # The acme e2e-qa estate's Ingress carries these five today (the
        # estate is its own repository, so this is a pin of the rows, not of
        # the estate); each must hit a real row, not the unknown rule.
        for key in ("alb.ingress.kubernetes.io/scheme",
                    "alb.ingress.kubernetes.io/target-type",
                    "alb.ingress.kubernetes.io/ssl-redirect",
                    "alb.ingress.kubernetes.io/certificate-arn",
                    "alb.ingress.kubernetes.io/listen-ports"):
            self.assertIsNot(api_translation.annotation_disposition(key),
                             api_translation.UNKNOWN_ROW, key)

    def test_pinned_phrasings_the_planner_tests_rely_on(self):
        disposition, rationale = api_translation.annotation_disposition(
            "alb.ingress.kubernetes.io/healthcheck-path")
        self.assertEqual(disposition, api_translation.OPEN_QUESTION)
        self.assertIn("does not infer health checks", rationale)
        self.assertIn("never silently dropped", api_translation.UNKNOWN_ROW[1])

    def test_security_knobs_are_open_questions_not_tradeoffs(self):
        # A protection or behaviour nobody re-creates on the target must reach
        # the PR body as an open question; a tradeoff line is too quiet for a
        # lost WAF, a 30 s idle timeout or a lost DNS failover.
        for key in ("alb.ingress.kubernetes.io/wafv2-acl-arn",
                    "alb.ingress.kubernetes.io/inbound-cidrs",
                    "alb.ingress.kubernetes.io/auth-type",
                    "alb.ingress.kubernetes.io/mutual-authentication",
                    "alb.ingress.kubernetes.io/shield-advanced-protection",
                    "alb.ingress.kubernetes.io/load-balancer-attributes",
                    "external-dns.alpha.kubernetes.io/aws-failover",
                    "external-dns.kubernetes.io/set-identifier"):
            self.assertEqual(api_translation.annotation_disposition(key)[0],
                             api_translation.OPEN_QUESTION, key)

    def test_prefix_rows_cover_the_open_ended_knobs(self):
        for key in ("alb.ingress.kubernetes.io/actions.redirect-to-https",
                    "alb.ingress.kubernetes.io/conditions.canary",
                    "alb.ingress.kubernetes.io/auth-idp-cognito",
                    "external-dns.alpha.kubernetes.io/aws-weight",
                    "external-dns.kubernetes.io/aws-weight",
                    "external-dns.kubernetes.io/hostname",
                    "cert-manager.io/cluster-issuer",
                    "acme.cert-manager.io/http01-edit-in-place",
                    "kubernetes.io/tls-acme"):
            self.assertIsNot(api_translation.annotation_disposition(key),
                             api_translation.UNKNOWN_ROW, key)

    # Every Ingress annotation the AWS load balancer controller documents
    # (kubernetes-sigs.github.io/aws-load-balancer-controller, Ingress
    # annotations page), placeholders replaced by an example. The table
    # claims to be closed over this surface; this pins it.
    CONTROLLER_KEYS = [
        "load-balancer-name", "group.name", "group.order", "tags",
        "ip-address-type", "scheme", "subnets", "security-groups",
        "manage-backend-security-group-rules", "customer-owned-ipv4-pool",
        "load-balancer-attributes", "wafv2-acl-arn", "wafv2-acl-name",
        "waf-acl-id", "create-acm-cert", "acm-pca-arn",
        "shield-advanced-protection", "listen-ports", "ssl-redirect",
        "inbound-cidrs", "security-group-prefix-lists", "certificate-arn",
        "ssl-policy", "target-type", "backend-protocol",
        "backend-protocol-version", "target-group-attributes",
        "healthcheck-port", "healthcheck-protocol", "healthcheck-path",
        "healthcheck-interval-seconds", "healthcheck-timeout-seconds",
        "healthy-threshold-count", "unhealthy-threshold-count",
        "success-codes", "auth-type", "auth-idp-cognito", "auth-idp-oidc",
        "auth-on-unauthenticated-request", "auth-scope",
        "auth-session-cookie", "auth-session-timeout", "jwt-validation",
        "actions.forward-canary", "transforms.strip-prefix",
        "conditions.forward-canary", "use-regex-path-match",
        "target-node-labels", "mutual-authentication",
        "multi-cluster-target-group", "listener-attributes.HTTPS-443",
        "minimum-load-balancer-capacity", "ipam-ipv4-pool-id",
        "enable-frontend-nlb", "frontend-nlb-scheme",
        "frontend-nlb-subnets", "frontend-nlb-security-groups",
        "frontend-nlb-listener-port-mapping",
        "frontend-nlb-healthcheck-port", "frontend-nlb-healthcheck-path",
        "frontend-nlb-healthcheck-success-codes", "frontend-nlb-tags",
        "frontend-nlb-eip-allocations", "frontend-nlb-attributes",
    ]
    # Documented keys deliberately left to the unknown rule (open question
    # with the generic text): semantics not understood well enough to
    # write a rationale. Listed so the gap is a choice, not an oversight.
    CONTROLLER_KEYS_LEFT_TO_THE_RULE = [
        "target-control-port.web.80",
    ]

    def test_the_controller_surface_is_covered(self):
        missing = [k for k in self.CONTROLLER_KEYS
                   if api_translation.annotation_disposition(
                       "alb.ingress.kubernetes.io/" + k)
                   is api_translation.UNKNOWN_ROW]
        self.assertEqual(missing, [])
        for k in self.CONTROLLER_KEYS_LEFT_TO_THE_RULE:
            self.assertIs(api_translation.annotation_disposition(
                "alb.ingress.kubernetes.io/" + k), api_translation.UNKNOWN_ROW)

    def test_no_prefix_row_shadows_an_exact_row_silently(self):
        # An exact row under a prefix is legitimate (the exact wins), but
        # every such pair is a deliberate one, listed here: a new pair is a
        # review event, not a table edit.
        shadowed = sorted(key for key in self.table["exact"]
                          if any(key.startswith(p) for p in self.table["prefix"]))
        self.assertEqual(shadowed, [
            # The older spelling of ssl-redirect: the same listener
            # concern as the exact ssl-redirect row, so the same open
            # question, not the actions.* "mapped" row.
            "alb.ingress.kubernetes.io/actions.ssl-redirect",
        ])

    def test_the_https_redirect_idiom_is_one_disposition_under_both_spellings(self):
        for key in ("alb.ingress.kubernetes.io/ssl-redirect",
                    "alb.ingress.kubernetes.io/actions.ssl-redirect"):
            self.assertEqual(api_translation.annotation_disposition(key)[0],
                             api_translation.OPEN_QUESTION, key)
        self.assertEqual(api_translation.annotation_disposition(
            "alb.ingress.kubernetes.io/actions.canary")[0],
            api_translation.MAPPED)

    def test_action_rows_name_the_use_annotation_sentinel(self):
        # The path that invokes an action names it as a backend Service with
        # port name use-annotation; the live named-port note would otherwise
        # send the worker looking for a Service that does not exist.
        _, rationale = api_translation.annotation_disposition(
            "alb.ingress.kubernetes.io/actions.forward-weighted")
        self.assertIn("use-annotation", rationale)


class ParserTest(unittest.TestCase):

    def test_missing_section_is_fatal(self):
        with self.assertRaises(api_translation.ApiTranslationError):
            api_translation.parse_annotation_dispositions(
                "# Title\n\n### Service annotations\n\n| a | b |\n|---|---|\n"
                "| `x/y` | z |\n", "doc")

    def test_empty_table_is_fatal(self):
        with self.assertRaises(api_translation.ApiTranslationError):
            api_translation.parse_annotation_dispositions(_table([]), "doc")

    def test_unknown_disposition_is_fatal_not_skipped(self):
        rows = ["| `alb.ingress.kubernetes.io/scheme` | drop | Gateway |"]
        with self.assertRaisesRegex(api_translation.ApiTranslationError,
                                    "unknown disposition"):
            api_translation.parse_annotation_dispositions(_table(rows), "doc")

    def test_underscore_and_upper_case_names_are_keys(self):
        rows = ["| `example.com/Some_Key` | mapped | x |",
                "| `Example.IO/other-*` | mapped | y |"]
        table = api_translation.parse_annotation_dispositions(_table(rows), "doc")
        self.assertIn("example.com/Some_Key", table["exact"])
        self.assertIn("Example.IO/other-", table["prefix"])

    def test_prose_key_cell_is_fatal(self):
        rows = ["| scheme: internet-facing | mapped | Gateway |"]
        with self.assertRaisesRegex(api_translation.ApiTranslationError,
                                    "not one annotation key"):
            api_translation.parse_annotation_dispositions(_table(rows), "doc")

    def test_duplicate_key_is_fatal_across_formatting(self):
        rows = ["| `alb.ingress.kubernetes.io/scheme` | mapped | a |",
                "| alb.ingress.kubernetes.io/scheme | open question | b |"]
        with self.assertRaisesRegex(api_translation.ApiTranslationError,
                                    "listed twice"):
            api_translation.parse_annotation_dispositions(_table(rows), "doc")

    def test_empty_rationale_is_fatal(self):
        rows = ["| `alb.ingress.kubernetes.io/scheme` | mapped |  |"]
        with self.assertRaisesRegex(api_translation.ApiTranslationError,
                                    "empty rationale"):
            api_translation.parse_annotation_dispositions(_table(rows), "doc")

    def test_extra_cells_are_fatal_not_truncated(self):
        # A literal pipe in a rationale splits the row; truncating it would
        # hand the worker half a sentence with no start-up error.
        rows = ["| `alb.ingress.kubernetes.io/scheme` | mapped | HTTP | HTTPS |"]
        with self.assertRaisesRegex(api_translation.ApiTranslationError,
                                    "more than three cells"):
            api_translation.parse_annotation_dispositions(_table(rows), "doc")

    def test_short_row_is_fatal(self):
        rows = ["| `alb.ingress.kubernetes.io/scheme` | mapped |"]
        with self.assertRaises(api_translation.ApiTranslationError):
            api_translation.parse_annotation_dispositions(_table(rows), "doc")

    def test_disposition_is_case_insensitive_and_unbackticked(self):
        rows = ["| `alb.ingress.kubernetes.io/scheme` | `Dropped With Tradeoff` | x |"]
        table = api_translation.parse_annotation_dispositions(_table(rows), "doc")
        self.assertEqual(table["exact"]["alb.ingress.kubernetes.io/scheme"],
                         (api_translation.DROPPED, "x"))

    def test_the_service_table_above_is_not_read(self):
        # Its rows are not dispositions; reading them would be fatal.
        table = api_translation.parse_annotation_dispositions(_table(ROWS), "doc")
        self.assertNotIn("x/y", table["exact"])

    def test_heading_prefix_match_survives_retitle(self):
        text = _table(ROWS).replace("### Ingress / Gateway annotations",
                                    "## Ingress and Gateway annotations (routing unit)")
        table = api_translation.parse_annotation_dispositions(text, "doc")
        self.assertIn("alb.ingress.kubernetes.io/scheme", table["exact"])


class LookupTest(unittest.TestCase):

    def setUp(self):
        self.table = api_translation.parse_annotation_dispositions(
            _table(ROWS), "doc")

    def lookup(self, key):
        return api_translation.annotation_disposition(key, self.table)

    def test_exact_row(self):
        self.assertEqual(self.lookup("alb.ingress.kubernetes.io/scheme"),
                         (api_translation.DROPPED, "Gateway"))

    def test_prefix_row(self):
        self.assertEqual(self.lookup("alb.ingress.kubernetes.io/auth-scope"),
                         (api_translation.OPEN_QUESTION, "IAP"))

    def test_exact_beats_prefix(self):
        self.assertEqual(self.lookup("alb.ingress.kubernetes.io/auth-type"),
                         (api_translation.MAPPED, "exact wins"))

    def test_longest_prefix_wins(self):
        self.assertEqual(
            self.lookup("alb.ingress.kubernetes.io/healthcheck-path-x"),
            (api_translation.MAPPED, "longer"))
        self.assertEqual(
            self.lookup("alb.ingress.kubernetes.io/healthcheck-port"),
            (api_translation.OPEN_QUESTION, "probes"))

    def test_bare_prefix_row(self):
        self.assertEqual(self.lookup("cert-manager.io/cluster-issuer"),
                         (api_translation.DROPPED, "Gateway TLS"))

    def test_unknown_key_is_the_rule_not_a_row(self):
        self.assertIs(self.lookup("nginx.ingress.kubernetes.io/rewrite-target"),
                      api_translation.UNKNOWN_ROW)
        # A prefix never matches across its own boundary.
        self.assertIs(self.lookup("alb.ingress.kubernetes.io/auth"),
                      api_translation.UNKNOWN_ROW)


if __name__ == "__main__":
    unittest.main()
