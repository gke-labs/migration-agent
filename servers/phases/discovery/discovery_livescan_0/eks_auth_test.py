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

"""Tests for the AWS/EKS auth seam.

This is the one module that runs against production credentials, so the parts
that do not need a live AWS or cluster are pinned here with fakes: the bearer-
token format, the cluster-binding header and expiry on the presigned URL, the
404→None mapping the walk depends on, and the credential preflight. The lazy
imports (boto3/botocore/kubernetes) are exercised — they are installed in the
test environment — but never reach the network.
"""

import base64
import builtins
import logging
import sys
import os
import tempfile
import types
import unittest
from unittest.mock import patch
from urllib.parse import parse_qs, urlsplit

try:
    from kubernetes import client as kubernetes_client
    from kubernetes.client.exceptions import ApiException
except ImportError:     # the seam tests below need the real client
    kubernetes_client = ApiException = None

from servers.phases.discovery.discovery_livescan_0 import eks_auth


class _Resp:
    """The urllib3-shaped response ApiException and make_get_json both read."""

    def __init__(self, status, reason, data):
        self.status, self.reason, self.data = status, reason, data

    def getheaders(self):
        return {"Audit-Id": "0f3c-not-wanted-in-notes"}


def _cluster(**overrides):
    base = {"name": "c", "region": "us-east-1", "endpoint": "https://eks.local",
            "certificate_authority_data": base64.b64encode(b"ca").decode()}
    base.update(overrides)
    return base


class EncodeTokenTest(unittest.TestCase):

    def test_prefix_urlsafe_and_unpadded(self):
        url = "https://sts.amazonaws.com/?Action=GetCallerIdentity&X-Amz=a/b+c"
        token = eks_auth._encode_token(url)
        self.assertTrue(token.startswith(eks_auth._TOKEN_PREFIX))
        body = token[len(eks_auth._TOKEN_PREFIX):]
        self.assertNotIn("=", body)                 # padding stripped
        self.assertNotIn("/", body)                 # url-safe alphabet
        # Re-pad and decode: the exact URL round-trips.
        padded = body + "=" * (-len(body) % 4)
        self.assertEqual(base64.urlsafe_b64decode(padded).decode("utf-8"), url)


class MissingDependenciesTest(unittest.TestCase):

    def test_all_present_returns_empty(self):
        self.assertEqual(eks_auth.missing_dependencies(), [])

    def test_absent_package_is_reported(self):
        real_import = builtins.__import__

        def fake_import(name, *args, **kwargs):
            if name == "kubernetes":
                raise ImportError("simulated: kubernetes not installed")
            return real_import(name, *args, **kwargs)

        with patch("builtins.__import__", side_effect=fake_import):
            missing = eks_auth.missing_dependencies()
        self.assertIn("kubernetes", missing)
        self.assertNotIn("boto3", missing)          # still importable


class CallerIdentityTest(unittest.TestCase):

    def test_asks_the_given_region_so_the_partition_is_right(self):
        seen = []

        class Sts:
            def get_caller_identity(self):
                return {"Account": "1", "Arn": "arn:aws-cn:iam::1:user/e",
                        "UserId": "u"}

        def factory(service, region):
            seen.append((service, region))
            return Sts()

        identity = eks_auth.caller_identity(factory, region="cn-north-1")
        self.assertEqual(seen, [("sts", "cn-north-1")])
        self.assertEqual(identity["arn"], "arn:aws-cn:iam::1:user/e")

    def test_maps_the_sts_fields(self):
        class Sts:
            def get_caller_identity(self):
                return {"Account": "111122223333",
                        "Arn": "arn:aws:iam::111122223333:user/eng",
                        "UserId": "AIDAEXAMPLE"}

        identity = eks_auth.caller_identity(lambda service, region: Sts())
        self.assertEqual(identity, {
            "account": "111122223333",
            "arn": "arn:aws:iam::111122223333:user/eng",
            "user_id": "AIDAEXAMPLE"})


# Amazon's documented example key pair (never valid), so presigning runs
# offline through the real botocore signer with no credential lookup.
_EXAMPLE_KEY_ENV = {
    "AWS_ACCESS_KEY_ID": "AKIAIOSFODNN7EXAMPLE",
    "AWS_SECRET_ACCESS_KEY": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
    # A developer's profile (a FIPS or legacy STS endpoint, a region) must
    # not reach the offline signer these tests read the URL of.
    "AWS_CONFIG_FILE": os.devnull,
    "AWS_SHARED_CREDENTIALS_FILE": os.devnull,
}


def _decode_token(token):
    """The inverse of _encode_token: the presigned URL, split for asserting."""
    assert token.startswith(eks_auth._TOKEN_PREFIX)
    encoded = token[len(eks_auth._TOKEN_PREFIX):]
    url = base64.urlsafe_b64decode(
        encoded + "=" * (-len(encoded) % 4)).decode("utf-8")
    parts = urlsplit(url)
    return parts, parse_qs(parts.query)


class PresignedTokenTest(unittest.TestCase):
    """Runs the real boto3 client and signer offline: presigning is a pure
    computation over the credentials, and the token's binding lives in what
    the signer put on the URL, which a recording fake could not show."""

    def setUp(self):
        env = {k: v for k, v in os.environ.items() if not k.startswith("AWS_")}
        env.update(_EXAMPLE_KEY_ENV)
        patcher = patch.dict(os.environ, env, clear=True)
        patcher.start()
        self.addCleanup(patcher.stop)
        self.factory = eks_auth.make_client_factory()
        self.addCleanup(eks_auth.release_library_logs)

    def test_binds_cluster_header_and_uses_the_authenticator_expiry(self):
        token = eks_auth._presigned_token("my-cluster", "us-west-2",
                                          self.factory)
        parts, query = _decode_token(token)

        self.assertEqual(parts.scheme, "https")
        # The regional STS endpoint, not the global one: the token must be
        # signed for the region the cluster lives in.
        self.assertEqual(parts.netloc, "sts.us-west-2.amazonaws.com")
        self.assertEqual(query["Action"], ["GetCallerIdentity"])
        # The value EKS's authenticator accepts (older verifiers reject a
        # larger one); STS honours the URL for 15 minutes regardless, and
        # make_get_json re-mints past that rather than asking for more here.
        self.assertEqual(query["X-Amz-Expires"], ["60"])
        self.assertEqual(int(query["X-Amz-Expires"][0]),
                         eks_auth._STS_PRESIGN_EXPIRES)
        # The cluster name travels as a SIGNED header — this is what binds the
        # token to one cluster — and not as a query parameter.
        signed_headers = query["X-Amz-SignedHeaders"][0].split(";")
        self.assertIn(eks_auth._CLUSTER_NAME_HEADER, signed_headers)
        self.assertNotIn(eks_auth._CLUSTER_NAME_HEADER, query)

    def test_two_tokens_from_one_client_each_carry_their_own_binding(self):
        # The event hooks are registered on a cached client; minting again must
        # neither stack a second handler nor leak the first cluster's name.
        first = eks_auth._presigned_token("alpha", "us-west-2", self.factory)
        second = eks_auth._presigned_token("beta", "us-west-2", self.factory)
        self.assertNotEqual(first, second)
        for token in (first, second):
            _, query = _decode_token(token)
            self.assertEqual(
                query["X-Amz-SignedHeaders"][0].count(
                    eks_auth._CLUSTER_NAME_HEADER), 1)
        # And the hooks stand once on the client, however many tokens it
        # minted: a stacked duplicate would sign the same header twice.
        handlers = self.factory("sts", "us-west-2").meta.events._emitter \
            ._handlers.prefix_search("before-sign.sts.GetCallerIdentity")
        self.assertEqual(sum(1 for h in handlers
                             if h is eks_auth._sign_cluster_name_header), 1)

    def test_follows_the_partition_of_the_region(self):
        token = eks_auth._presigned_token("cn-cluster", "cn-north-1",
                                          self.factory)
        parts, _ = _decode_token(token)
        self.assertEqual(parts.netloc, "sts.cn-north-1.amazonaws.com.cn")


@unittest.skipIf(kubernetes_client is None, "kubernetes client not installed")
class MakeGetJsonTest(unittest.TestCase):

    def _get_json(self, api_cls, cluster=None):
        # The patches stay up for the test: get_json re-mints on a 401.
        for p in (patch.object(eks_auth, "_presigned_token", return_value="tok"),
                  patch.object(eks_auth, "_write_ca", return_value="/tmp/ca.pem"),
                  patch("kubernetes.client.ApiClient", api_cls)):
            p.start()
            self.addCleanup(p.stop)
        return eks_auth.make_get_json(cluster or _cluster(),
                                      lambda service, region: None)

    def test_missing_endpoint_or_ca_raises_before_any_call(self):
        with self.assertRaises(ValueError):
            eks_auth.make_get_json(_cluster(endpoint=None),
                                   lambda service, region: None)
        with self.assertRaises(ValueError):
            eks_auth.make_get_json(_cluster(certificate_authority_data=None),
                                   lambda service, region: None)

    def test_authorization_header_carries_the_bearer_scheme(self):
        # Against the REAL installed Configuration: the client emits the
        # Authorization header verbatim from the BearerToken auth setting's
        # value. EKS rejects a token without the "Bearer " scheme as
        # anonymous, 401-ing the whole walk. Which api_key_prefix identifier
        # the value is built from changed in client v36
        # (kubernetes-client/python#2595); the seam-mocked tests below cannot
        # see the header at all, so this is the only guard on it.
        captured = {}
        real_api_client = kubernetes_client.ApiClient

        def capture(configuration=None):
            captured["conf"] = configuration
            return real_api_client(configuration)

        with patch.object(eks_auth, "_presigned_token",
                          return_value="k8s-aws-v1.TOKEN"), \
             patch.object(eks_auth, "_write_ca", return_value="/tmp/ca.pem"), \
             patch("kubernetes.client.ApiClient", capture):
            eks_auth.make_get_json(_cluster(), lambda service, region: None)
        setting = captured["conf"].auth_settings()["BearerToken"]
        self.assertEqual(setting["key"], "authorization")
        self.assertEqual(setting["value"], "Bearer k8s-aws-v1.TOKEN")

    def test_404_maps_to_none(self):
        class Api:
            def __init__(self, configuration=None):
                pass

            def call_api(self, *args, **kwargs):
                raise ApiException(status=404)

        self.assertIsNone(self._get_json(Api)("/apis/karpenter.sh/v1/nodepools"))

    def test_other_status_raises_a_one_line_control_plane_error(self):
        class Api:
            def __init__(self, configuration=None):
                pass

            def call_api(self, *args, **kwargs):
                raise ApiException(http_resp=_Resp(
                    403, "Forbidden",
                    b'{"kind":"Status","message":"pods is forbidden: User '
                    b'\\"arn:aws:sts::1:assumed-role/r/u\\" cannot list '
                    b'resource \\"pods\\"","code":403}'))

        with self.assertRaises(eks_auth.ControlPlaneError) as ctx:
            self._get_json(Api)("/api/v1/pods")
        # One line, status first, then the API server's own sentence — the
        # walk records str(error) verbatim in a note, and the raw
        # ApiException text is several lines of HTTP headers.
        self.assertEqual(ctx.exception.status, 403)
        text = str(ctx.exception)
        self.assertNotIn("\n", text)
        self.assertTrue(text.startswith("HTTP 403 Forbidden: pods is forbidden"))
        self.assertIn("cannot list resource", text)
        self.assertNotIn("Audit-Id", text)

    def test_control_plane_error_without_a_status_body_keeps_the_reason(self):
        class Api:
            def __init__(self, configuration=None):
                pass

            def call_api(self, *args, **kwargs):
                raise ApiException(http_resp=_Resp(401, "Unauthorized", b""))

        with self.assertRaises(eks_auth.ControlPlaneError) as ctx:
            self._get_json(Api)("/api/v1/pods")
        self.assertEqual(str(ctx.exception), "HTTP 401 Unauthorized")

    def test_a_transport_failure_is_one_line_and_claims_no_status(self):
        # The client folds a TLS failure into status 0 and a two-line
        # reason (the class name over the detail); the note is one line
        # and does not read as an HTTP answer.
        class Api:
            def __init__(self, configuration=None):
                pass

            def call_api(self, *args, **kwargs):
                raise ApiException(
                    status=0,
                    reason="SSLError\n[SSL: CERTIFICATE_VERIFY_FAILED] "
                           "certificate verify failed: unable to get local issuer")

        with self.assertRaises(eks_auth.ControlPlaneError) as ctx:
            self._get_json(Api)("/version")
        text = str(ctx.exception)
        self.assertNotIn("\n", text)
        self.assertNotIn("HTTP 0", text)
        self.assertTrue(text.startswith(
            "no HTTP answer (SSLError): [SSL: CERTIFICATE_VERIFY_FAILED]"), text)
        self.assertEqual(ctx.exception.status, 0)
        self.assertEqual(ctx.exception.reason, "SSLError")

    def test_a_message_that_merely_repeats_the_reason_is_not_repeated(self):
        # EKS answers an unmapped identity with a Status whose message is
        # the bare word "Unauthorized" — the same as the HTTP reason.
        class Api:
            def __init__(self, configuration=None):
                pass

            def call_api(self, *args, **kwargs):
                raise ApiException(http_resp=_Resp(
                    401, "Unauthorized",
                    b'{"kind":"Status","message":"Unauthorized","code":401}'))

        with self.assertRaises(eks_auth.ControlPlaneError) as ctx:
            self._get_json(Api)("/api/v1/pods")
        self.assertEqual(str(ctx.exception), "HTTP 401 Unauthorized")

    def test_success_decodes_the_raw_response_body(self):
        class Api:
            def __init__(self, configuration=None):
                pass

            def call_api(self, *args, **kwargs):
                return _Resp(200, "OK", b'{"items": [1, 2]}')

        self.assertEqual(self._get_json(Api)("/api/v1/pods"),
                         {"items": [1, 2]})

    def test_the_client_retries_once_so_the_timeouts_bound_the_call(self):
        # Against the REAL installed Configuration: rest.py hands urllib3
        # the pool's retries only when `retries` is set, and urllib3's own
        # default is three — four connects and four read windows per call
        # against a stalled endpoint, not the one pair documented above.
        captured = {}
        real_api_client = kubernetes_client.ApiClient

        def capture(configuration=None):
            captured["conf"] = configuration
            return real_api_client(configuration)

        with patch.object(eks_auth, "_presigned_token", return_value="tok"), \
             patch.object(eks_auth, "_write_ca", return_value="/tmp/ca.pem"), \
             patch("kubernetes.client.ApiClient", capture):
            eks_auth.make_get_json(_cluster(), lambda service, region: None)
        retries = captured["conf"].retries
        self.assertEqual(retries.total, eks_auth._RETRIES)
        self.assertEqual(eks_auth._RETRIES, 1)
        # A `Retry-After: 3600` on a 429/503 would otherwise be slept
        # through, outside both timeouts.
        self.assertFalse(retries.respect_retry_after_header)

    def test_every_call_carries_the_connect_and_read_timeouts(self):
        # The kubernetes client's default is NO timeout; without this pair a
        # hung control plane would stall the whole walk indefinitely.
        captured = {}

        class Api:
            def __init__(self, configuration=None):
                pass

            def call_api(self, *args, **kwargs):
                captured.update(kwargs)
                return _Resp(200, "OK", b'{"items": []}')

        self._get_json(Api)("/api/v1/pods")
        self.assertEqual(captured["_request_timeout"],
                         (eks_auth._CONNECT_TIMEOUT_S,
                          eks_auth._READ_TIMEOUT_S))
        # Raw-response mode: we decode the JSON ourselves rather than going
        # through the client's typed deserializer (see make_get_json).
        self.assertIs(captured["_preload_content"], False)

    def test_a_401_re_mints_the_token_once_and_retries(self):
        # The header is rebuilt from the configuration on every call, so a
        # re-mint needs no new client: the retry must go out with the new
        # token, not the one that just 401ed.
        calls = []
        captured = {}

        class Api:
            def __init__(self, configuration=None):
                captured["conf"] = configuration

            def call_api(self, *args, **kwargs):
                calls.append(
                    captured["conf"].auth_settings()["BearerToken"]["value"])
                if len(calls) == 1:
                    raise ApiException(http_resp=_Resp(401, "Unauthorized",
                                                       b""))
                return _Resp(200, "OK", b'{"items": []}')

        with patch.object(eks_auth, "_presigned_token",
                          side_effect=["tok-1", "tok-2"]) as mint, \
             patch.object(eks_auth, "_write_ca", return_value="/tmp/ca.pem"), \
             patch("kubernetes.client.ApiClient", Api):
            get_json = eks_auth.make_get_json(_cluster(),
                                              lambda service, region: None)
            self.assertEqual(get_json("/api/v1/pods"), {"items": []})
        self.assertEqual(mint.call_count, 2)
        self.assertEqual(calls, ["Bearer tok-1", "Bearer tok-2"])

    def test_a_second_401_is_the_verdict_and_is_raised(self):
        class Api:
            def __init__(self, configuration=None):
                pass

            def call_api(self, *args, **kwargs):
                raise ApiException(http_resp=_Resp(401, "Unauthorized", b""))

        with patch.object(eks_auth, "_presigned_token",
                          return_value="tok") as mint, \
             patch.object(eks_auth, "_write_ca", return_value="/tmp/ca.pem"), \
             patch("kubernetes.client.ApiClient", Api):
            get_json = eks_auth.make_get_json(_cluster(),
                                              lambda service, region: None)
            with self.assertRaises(eks_auth.ControlPlaneError) as ctx:
                get_json("/api/v1/pods")
            # Each call gets one retry of its own, not one per client.
            with self.assertRaises(eks_auth.ControlPlaneError):
                get_json("/api/v1/nodes")
        self.assertEqual(ctx.exception.status, 401)
        self.assertEqual(mint.call_count, 3)   # initial + one per call

    def test_the_token_is_re_minted_past_the_refresh_window(self):
        clock = {"now": 1000.0}

        class Api:
            def __init__(self, configuration=None):
                pass

            def call_api(self, *args, **kwargs):
                return _Resp(200, "OK", b'{"items": []}')

        with patch.object(eks_auth, "_presigned_token",
                          return_value="tok") as mint, \
             patch.object(eks_auth, "_write_ca", return_value="/tmp/ca.pem"), \
             patch("kubernetes.client.ApiClient", Api), \
             patch.object(eks_auth.time, "monotonic",
                          side_effect=lambda: clock["now"]):
            get_json = eks_auth.make_get_json(_cluster(),
                                              lambda service, region: None)
            get_json("/api/v1/pods")
            self.assertEqual(mint.call_count, 1)
            clock["now"] += eks_auth._TOKEN_REFRESH_S + 1
            get_json("/api/v1/nodes")
            self.assertEqual(mint.call_count, 2)
            get_json("/api/v1/services")
            self.assertEqual(mint.call_count, 2)

    def test_call_api_keywords_exist_on_the_installed_client(self):
        # The keyword we pass must be one the installed client's call_api
        # actually accepts. The deserializer keyword was renamed across
        # major versions (response_type → response_types_map); an
        # unknown keyword fails every in-cluster walk with a TypeError,
        # which the seam-mocked tests above can never see.
        import inspect
        from kubernetes import client as k8s_client
        params = inspect.signature(k8s_client.ApiClient.call_api).parameters
        for keyword in ("auth_settings", "_preload_content",
                        "_request_timeout"):
            self.assertIn(keyword, params)


class ApiServerMessageTest(unittest.TestCase):

    def test_reads_the_status_message(self):
        self.assertEqual(
            eks_auth.api_server_message(b'{"message": " Unauthorized "}'),
            "Unauthorized")
        self.assertEqual(
            eks_auth.api_server_message('{"message": "nope"}'), "nope")

    def test_anything_else_is_empty(self):
        for body in (None, b"", b"<html>bad gateway</html>", b"[1]",
                     b'{"reason": "x"}', b'{"message": 3}'):
            self.assertEqual(eks_auth.api_server_message(body), "", body)


@unittest.skipIf(kubernetes_client is None, "kubernetes client not installed")
class CleanupTest(unittest.TestCase):

    def _make(self, api_cls):
        ca = tempfile.NamedTemporaryFile(delete=False)
        ca.close()
        self.addCleanup(lambda: os.path.exists(ca.name) and os.unlink(ca.name))
        with patch.object(eks_auth, "_presigned_token", return_value="tok"), \
             patch.object(eks_auth, "_write_ca", return_value=ca.name), \
             patch("kubernetes.client.ApiClient", api_cls):
            get_json = eks_auth.make_get_json(_cluster(),
                                              lambda service, region: None)
        return get_json, ca.name

    def test_cleanup_closes_the_client_and_removes_the_ca_file(self):
        closed = []

        class Api:
            def __init__(self, configuration=None):
                pass

            def close(self):
                closed.append(True)

        get_json, ca_path = self._make(Api)
        self.assertTrue(os.path.exists(ca_path))
        self.assertEqual(closed, [])
        get_json.cleanup()
        self.assertEqual(closed, [True])
        self.assertFalse(os.path.exists(ca_path))
        get_json.cleanup()      # a second call has nothing left to do
        self.assertEqual(closed, [True])

    def test_cleanup_clears_the_connection_pool_as_well(self):
        released = []

        class Pool:
            def clear(self):
                released.append("pool")

        class Rest:
            pool_manager = Pool()

        class Api:
            rest_client = Rest()

            def __init__(self, configuration=None):
                pass

            def close(self):
                released.append("close")

        get_json, _ = self._make(Api)
        get_json.cleanup()
        # ApiClient.close() releases the thread pool only; the sockets are
        # the PoolManager's, and go with it.
        self.assertEqual(released, ["close", "pool"])

    def test_a_client_that_will_not_close_still_loses_its_ca_file(self):
        class Api:
            def __init__(self, configuration=None):
                pass

            def close(self):
                raise RuntimeError("pool already gone")

        get_json, ca_path = self._make(Api)
        get_json.cleanup()
        self.assertFalse(os.path.exists(ca_path))


class LibraryLoggingTest(unittest.TestCase):
    """The client libraries' DEBUG narration carries the signature that IS
    the bearer token; the MCP server's root logger runs at DEBUG. Building
    a client holds those loggers at INFO, only ever raises them, and the
    release puts back exactly what the hold changed."""

    def setUp(self):
        env = {k: v for k, v in os.environ.items() if not k.startswith("AWS_")}
        env.update(_EXAMPLE_KEY_ENV)
        patcher = patch.dict(os.environ, env, clear=True)
        patcher.start()
        self.addCleanup(patcher.stop)
        self._levels = {name: logging.getLogger(name).level
                        for name in eks_auth._LIBRARY_LOGGERS}
        self.addCleanup(self._restore)
        self.addCleanup(eks_auth.release_library_logs)

    def _debug_root(self):
        root = logging.getLogger()
        saved = root.level
        root.setLevel(logging.DEBUG)
        self.addCleanup(root.setLevel, saved)

    def _restore(self):
        for name, level in self._levels.items():
            logging.getLogger(name).setLevel(level)

    def test_presigning_under_a_debug_root_logs_no_signing_material(self):
        for name in eks_auth._LIBRARY_LOGGERS:
            logging.getLogger(name).setLevel(logging.NOTSET)
        records = []

        class Capture(logging.Handler):
            def emit(self, record):
                records.append(record)

        root = logging.getLogger()
        handler = Capture(level=logging.DEBUG)
        saved = root.level
        root.addHandler(handler)
        root.setLevel(logging.DEBUG)
        try:
            factory = eks_auth.make_client_factory()
            token = eks_auth._presigned_token("my-cluster", "us-west-2", factory)
        finally:
            root.removeHandler(handler)
            root.setLevel(saved)
        _, query = _decode_token(token)
        signature = query["X-Amz-Signature"][0]
        library = [r for r in records
                   if r.name.split(".")[0] in eks_auth._LIBRARY_LOGGERS]
        self.assertEqual([(r.name, r.levelname) for r in library
                          if r.levelno < logging.INFO], [])
        rendered = "\n".join(r.getMessage() for r in records)
        self.assertNotIn(signature, rendered)
        self.assertNotIn("StringToSign", rendered)
        self.assertNotIn("CanonicalRequest", rendered)
        self.assertNotIn(_EXAMPLE_KEY_ENV["AWS_SECRET_ACCESS_KEY"], rendered)
        for name in eks_auth._LIBRARY_LOGGERS:
            self.assertEqual(logging.getLogger(name).level, logging.INFO)

    def test_an_operator_level_above_info_is_not_lowered(self):
        logging.getLogger("botocore").setLevel(logging.WARNING)
        eks_auth.make_client_factory()
        self.assertEqual(logging.getLogger("botocore").level, logging.WARNING)

    def test_the_release_puts_back_the_levels_the_hold_changed(self):
        self._debug_root()
        for name in eks_auth._LIBRARY_LOGGERS:
            logging.getLogger(name).setLevel(logging.NOTSET)
        logging.getLogger("botocore").setLevel(logging.DEBUG)   # an operator's
        logging.getLogger("urllib3").setLevel(logging.WARNING)  # choices
        eks_auth.make_client_factory()
        self.assertEqual(logging.getLogger("botocore").level, logging.INFO)
        self.assertEqual(logging.getLogger("boto3").level, logging.INFO)
        self.assertEqual(logging.getLogger("kubernetes").level, logging.INFO)
        self.assertEqual(logging.getLogger("urllib3").level, logging.WARNING)
        eks_auth.release_library_logs()
        self.assertEqual(logging.getLogger("botocore").level, logging.DEBUG)
        self.assertEqual(logging.getLogger("boto3").level, logging.NOTSET)
        self.assertEqual(logging.getLogger("kubernetes").level, logging.NOTSET)
        self.assertEqual(logging.getLogger("urllib3").level, logging.WARNING)

    def test_the_hold_lasts_until_the_last_holder_releases(self):
        self._debug_root()
        for name in eks_auth._LIBRARY_LOGGERS:
            logging.getLogger(name).setLevel(logging.NOTSET)
        eks_auth.make_client_factory()
        eks_auth.make_client_factory()
        eks_auth.release_library_logs()
        self.assertEqual(logging.getLogger("botocore").level, logging.INFO)
        eks_auth.release_library_logs()
        self.assertEqual(logging.getLogger("botocore").level, logging.NOTSET)
        # A release with nothing held is a no-op, not an error.
        eks_auth.release_library_logs()
        self.assertEqual(logging.getLogger("botocore").level, logging.NOTSET)

    def test_a_child_logger_with_its_own_debug_level_is_held_too(self):
        self._debug_root()
        for name in eks_auth._LIBRARY_LOGGERS:
            logging.getLogger(name).setLevel(logging.NOTSET)
        child = logging.getLogger("botocore.auth")
        child.setLevel(logging.DEBUG)      # bypasses the parent's hold
        self.addCleanup(child.setLevel, logging.NOTSET)
        eks_auth.make_client_factory()
        self.assertEqual(child.getEffectiveLevel(), logging.INFO)
        eks_auth.release_library_logs()
        self.assertEqual(child.level, logging.DEBUG)

    def test_a_failed_boto3_import_still_pairs_with_the_callers_release(self):
        # The hold is taken before the import, so the tool's unconditional
        # release in its finally never drops another scan's hold.
        before = eks_auth._HOLD_DEPTH
        with patch.dict(sys.modules, {"boto3": None}):
            with self.assertRaises(ImportError):
                eks_auth.make_client_factory()
        self.assertEqual(eks_auth._HOLD_DEPTH, before + 1)
        eks_auth.release_library_logs()
        self.assertEqual(eks_auth._HOLD_DEPTH, before)


@unittest.skipIf(kubernetes_client is None, "kubernetes client not installed")
class RemintFailureTest(unittest.TestCase):

    def test_a_401_whose_re_mint_fails_keeps_the_401_verdict(self):
        # The retry never happens; the error must still say 401 (not the
        # mint's NoCredentialsError, which would read as a local setup
        # problem) and name the failed re-mint, which is what tells an
        # expired credential from an unmapped identity.
        class Api:
            def __init__(self, configuration=None):
                pass

            def call_api(self, *args, **kwargs):
                raise ApiException(http_resp=_Resp(401, "Unauthorized", b""))

        with patch.object(eks_auth, "_presigned_token",
                          side_effect=["tok-1", RuntimeError("no credentials")]), \
             patch.object(eks_auth, "_write_ca", return_value="/tmp/ca.pem"), \
             patch("kubernetes.client.ApiClient", Api):
            get_json = eks_auth.make_get_json(_cluster(),
                                              lambda service, region: None)
            with self.assertRaises(eks_auth.ControlPlaneError) as ctx:
                get_json("/api/v1/pods")
        self.assertEqual(ctx.exception.status, 401)
        self.assertIsInstance(ctx.exception.token_refresh_error, RuntimeError)
        text = str(ctx.exception)
        self.assertTrue(text.startswith("HTTP 401 Unauthorized: the token was "
                                        "refused and a fresh one could not be "
                                        "minted"), text)
        self.assertIn("RuntimeError: no credentials", text)
        self.assertNotIn("\n", text)

    def test_a_failed_refresh_past_the_window_is_the_401_it_stands_in_for(self):
        # Past _TOKEN_REFRESH_S the client re-mints before the call. When
        # that mint fails (an SSO session ended mid-walk), the failure must
        # surface as the same 401-with-cause the retry path raises — not as
        # a botocore error the walk would note per collection and move past,
        # recording the cluster as walked with a trail of notes.
        clock = {"now": 1000.0}
        calls = []

        class Api:
            def __init__(self, configuration=None):
                pass

            def call_api(self, path, *args, **kwargs):
                calls.append(path)
                return _Resp(200, "OK", b'{"items": []}')

        with patch.object(eks_auth, "_presigned_token",
                          side_effect=["tok-1", RuntimeError("sso session ended")]), \
             patch.object(eks_auth, "_write_ca", return_value="/tmp/ca.pem"), \
             patch("kubernetes.client.ApiClient", Api), \
             patch.object(eks_auth.time, "monotonic",
                          side_effect=lambda: clock["now"]):
            get_json = eks_auth.make_get_json(_cluster(),
                                              lambda service, region: None)
            get_json("/api/v1/pods")
            clock["now"] += eks_auth._TOKEN_REFRESH_S + 1
            with self.assertRaises(eks_auth.ControlPlaneError) as ctx:
                get_json("/api/v1/nodes")
        self.assertEqual(calls, ["/api/v1/pods"])   # nothing sent unsigned
        self.assertEqual(ctx.exception.status, 401)
        self.assertIsInstance(ctx.exception.token_refresh_error, RuntimeError)
        text = str(ctx.exception)
        self.assertTrue(text.startswith(
            "HTTP 401 Unauthorized: the token needed refreshing and a fresh "
            "one could not be minted"), text)
        self.assertIn("RuntimeError: sso session ended", text)
        self.assertNotIn("\n", text)


class UnreachableEndpointTest(unittest.TestCase):

    def test_a_connection_error_anywhere_in_the_chain_is_unreachable(self):
        class EndpointConnectionError(Exception):
            pass

        class InvalidRegionError(Exception):
            pass

        self.assertTrue(eks_auth.is_unreachable_endpoint(
            EndpointConnectionError("no route")))
        self.assertTrue(eks_auth.is_unreachable_endpoint(
            InvalidRegionError("eu-west1")))
        try:
            try:
                raise EndpointConnectionError("no route")
            except EndpointConnectionError as inner:
                raise RuntimeError("preflight failed") from inner
        except RuntimeError as wrapped:
            self.assertTrue(eks_auth.is_unreachable_endpoint(wrapped))

    def test_a_refused_credential_is_not_unreachable(self):
        class ClientError(Exception):
            pass

        self.assertFalse(eks_auth.is_unreachable_endpoint(
            RuntimeError("ExpiredToken")))
        self.assertFalse(eks_auth.is_unreachable_endpoint(
            ClientError("The security token included in the request is expired")))
        self.assertFalse(eks_auth.is_unreachable_endpoint(None))


class CaFileDisciplineTest(unittest.TestCase):

    def test_a_client_that_will_not_build_takes_the_ca_file_with_it(self):
        ca = tempfile.NamedTemporaryFile(delete=False)
        ca.close()
        self.addCleanup(lambda: os.path.exists(ca.name) and os.unlink(ca.name))

        class Api:
            def __init__(self, configuration=None):
                raise RuntimeError("no client")

        with patch.object(eks_auth, "_presigned_token", return_value="tok"), \
             patch.object(eks_auth, "_write_ca", return_value=ca.name), \
             patch("kubernetes.client.ApiClient", Api):
            with self.assertRaises(RuntimeError):
                eks_auth.make_get_json(_cluster(), lambda service, region: None)
        self.assertFalse(os.path.exists(ca.name))

    def test_a_bad_ca_bundle_leaves_no_file_behind(self):
        with patch("tempfile.NamedTemporaryFile") as opened:
            with self.assertRaises(Exception):
                eks_auth._write_ca("abc")      # not base64: bad padding
        opened.assert_not_called()

    def test_a_good_ca_bundle_is_written_and_closed(self):
        path = eks_auth._write_ca(base64.b64encode(b"-----BEGIN CERT").decode())
        self.addCleanup(lambda: os.path.exists(path) and os.unlink(path))
        with open(path, "rb") as handle:
            self.assertEqual(handle.read(), b"-----BEGIN CERT")



@unittest.skipIf(kubernetes_client is None, "kubernetes client not installed")
class RefusedAfterRefreshTest(unittest.TestCase):

    def test_a_401_after_a_good_re_mint_is_flagged_terminal(self):
        # The re-mint is offline and succeeds even on an expired session
        # key; the fresh token is refused too. That 401 must carry the flag
        # k8s_live ends the walk on, or the rest of the walk is a trail of
        # 401 notes reading as a thin estate.
        class Api:
            def __init__(self, configuration=None):
                pass

            def call_api(self, *args, **kwargs):
                raise ApiException(http_resp=_Resp(401, "Unauthorized", b""))

        with patch.object(eks_auth, "_presigned_token",
                          side_effect=["tok-1", "tok-2"]) as mint, \
             patch.object(eks_auth, "_write_ca", return_value="/tmp/ca.pem"), \
             patch("kubernetes.client.ApiClient", Api):
            get_json = eks_auth.make_get_json(_cluster(),
                                              lambda service, region: None)
            with self.assertRaises(eks_auth.ControlPlaneError) as ctx:
                get_json("/api/v1/pods")
        self.assertEqual(mint.call_count, 2)
        self.assertEqual(ctx.exception.status, 401)
        self.assertIs(ctx.exception.refused_after_refresh, True)
        self.assertIsNone(ctx.exception.token_refresh_error)


class EndpointKindTest(unittest.TestCase):
    """Which endpoint failed decides whose failure it is."""

    def test_a_stalled_or_dropped_connection_is_unreachable(self):
        for name in ("ReadTimeoutError", "ConnectionClosedError", "SSLError"):
            error = type(name, (Exception,), {})("x")
            self.assertTrue(eks_auth.is_unreachable_endpoint(error), name)

    def test_a_credential_providers_endpoint_is_the_credentials_failure(self):
        def error(url):
            e = type("EndpointConnectionError", (Exception,), {})("x")
            e.kwargs = {"endpoint_url": url}
            return e
        for url in ("https://portal.sso.us-east-1.amazonaws.com/federation/"
                    "credentials",
                    "http://169.254.170.2/v2/credentials/abc",
                    "http://169.254.169.254/latest/api/token",
                    "http://localhost:8080/creds",
                    "http://127.0.0.1:8080/creds",
                    "http://[::1]:8080/creds",
                    "http://[fd00:ec2::23]/creds"):
            self.assertFalse(eks_auth.is_unreachable_endpoint(error(url)), url)
        for url in ("https://sts.eu-west1.amazonaws.com/",
                    "https://sts-fips.us-east-1.amazonaws.com/",
                    "https://vpce-0abc.sts.us-east-1.vpce.amazonaws.com/",
                    "https://sts.cn-north-1.amazonaws.com.cn/",
                    "https://sts-proxy.corp.example:8443/"):
            self.assertTrue(eks_auth.is_unreachable_endpoint(error(url)), url)


class AwsErrorCodeTest(unittest.TestCase):
    """The AWS error code is read from the ClientError's response shape."""

    def test_the_code_is_read_on_the_error_or_a_cause(self):
        class ClientError(Exception):
            def __init__(self, code):
                super().__init__(code)
                self.response = {"Error": {"Code": code, "Message": "x"}}

        self.assertEqual(eks_auth.aws_error_code(
            ClientError("InvalidClientTokenId")), "InvalidClientTokenId")
        try:
            try:
                raise ClientError("ExpiredToken")
            except ClientError as inner:
                raise RuntimeError("preflight failed") from inner
        except RuntimeError as wrapped:
            self.assertEqual(eks_auth.aws_error_code(wrapped), "ExpiredToken")
        self.assertEqual(eks_auth.aws_error_code(RuntimeError("x")), "")
        self.assertEqual(eks_auth.aws_error_code(None), "")
        bare = ClientError("x")
        bare.response = {}
        self.assertEqual(eks_auth.aws_error_code(bare), "")
        odd = ClientError("x")
        odd.response = {"Error": "not a block"}
        self.assertEqual(eks_auth.aws_error_code(odd), "")


@unittest.skipIf(kubernetes_client is None, "kubernetes client not installed")
class ConfigurationLoggerTest(unittest.TestCase):

    def test_building_a_configuration_leaves_logger_levels_alone(self):
        for name in eks_auth._CONFIGURATION_LOGGERS:
            logger = logging.getLogger(name)
            self.addCleanup(logger.setLevel, logger.level)
            logger.setLevel(logging.ERROR)
        self.assertIsNotNone(eks_auth._configuration(kubernetes_client))
        for name in eks_auth._CONFIGURATION_LOGGERS:
            self.assertEqual(logging.getLogger(name).level, logging.ERROR,
                             name)


class ClientBoundsTest(unittest.TestCase):
    """Every AWS client carries the control-plane bounds — the connect and
    read timeouts and the single retry, in botocore's standard mode, which
    counts attempts — and is built once per (service, region)."""

    def test_aws_clients_carry_the_control_plane_bounds(self):
        import boto3
        made = []

        class FakeSession:
            def __init__(self, profile_name=None):
                self.profile_name = profile_name

            def client(self, service, region_name=None, config=None):
                made.append((service, region_name, config))
                return object()

        self.addCleanup(eks_auth.release_library_logs)
        with patch.object(boto3.session, "Session", FakeSession):
            factory = eks_auth.make_client_factory(profile_name="p")
            first = factory("ec2", "us-east-1")
            self.assertIs(first, factory("ec2", "us-east-1"))
            factory("sts", "eu-west-1")
        self.assertEqual([m[:2] for m in made],
                         [("ec2", "us-east-1"), ("sts", "eu-west-1")])
        for _, _, config in made:
            self.assertEqual(config.connect_timeout, eks_auth._CONNECT_TIMEOUT_S)
            self.assertEqual(config.read_timeout, eks_auth._READ_TIMEOUT_S)
            self.assertEqual(config.retries, {
                "total_max_attempts": eks_auth._RETRIES + 1,
                "mode": "standard"})

    def test_botocore_resolves_the_bound_to_two_attempts(self):
        """`total_max_attempts` is what botocore keeps as given; a
        `max_attempts` of the same number it would read as retries and
        raise by one. A real client, built by the factory with the AWS
        documentation's example credentials and never used, shows the
        resolved value."""
        import boto3
        self.addCleanup(eks_auth.release_library_logs)
        with patch.dict(os.environ, {
                "AWS_ACCESS_KEY_ID": "AKIAIOSFODNN7EXAMPLE",
                "AWS_SECRET_ACCESS_KEY":
                    "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"}):
            client = eks_auth.make_client_factory()("sts", "us-east-1")
        self.assertEqual(client.meta.config.retries, {
            "total_max_attempts": eks_auth._RETRIES + 1, "mode": "standard"})
        self.assertEqual(client.meta.config.retries["total_max_attempts"], 2)
        del boto3

    def test_a_ca_write_that_fails_leaves_no_file(self):
        real = tempfile.NamedTemporaryFile
        names = []

        def failing(*args, **kwargs):
            handle = real(*args, **kwargs)
            names.append(handle.name)

            class Failing:
                name = handle.name

                def write(self, _payload):
                    raise OSError(28, "No space left on device")

                def close(self):
                    handle.close()

            return Failing()

        with patch.object(tempfile, "NamedTemporaryFile", failing):
            with self.assertRaises(OSError):
                eks_auth._write_ca(base64.b64encode(b"ca").decode())
        self.assertEqual(len(names), 1)
        self.assertFalse(os.path.exists(names[0]))


if __name__ == "__main__":
    unittest.main()
