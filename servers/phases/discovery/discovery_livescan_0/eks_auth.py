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

"""AWS/EKS authentication seam for live discovery.

The one module that imports boto3 and the kubernetes client, and it imports
them lazily — inside the functions, not at module load — so aws_live.py,
k8s_live.py, projection.py and live_csv.py stay importable and unit-testable
on a machine with neither package installed. The walk modules receive the
callables this module builds (`client_factory`, `get_json`) and never see a
boto3 or kubernetes symbol.

Credentials come only from the local environment: the default boto3 chain
(~/.aws/credentials, env vars, assumed SSO roles). This module never asks
for a key, never writes one, and issues only read calls. The EKS bearer
token is a presigned STS GetCallerIdentity URL — the standard aws-iam-
authenticator scheme — minted in-memory per cluster and never persisted.
"""

import base64
import json
import logging
import threading
import time
from urllib.parse import urlsplit

# The token the kubelet/authenticator scheme expects: a presigned STS URL,
# base64url without padding, prefixed. Fixed by the EKS auth contract.
_TOKEN_PREFIX = "k8s-aws-v1."
# The presign's X-Amz-Expires. It does not govern the token's life: STS
# honours a presigned GetCallerIdentity for 15 minutes from its X-Amz-Date
# whatever the parameter says, and EKS's authenticator caps what it accepts
# at 60 (older verifiers reject a larger value outright), so 60 is the one
# interoperable value and the one `aws eks get-token` sends. The lifetime a
# large cluster's walk has to live with is therefore 15 minutes from
# minting; get_json re-mints — an offline signature, milliseconds, no
# network — past _TOKEN_REFRESH_S and once more on a 401, so a long walk
# never degrades into a trail of 401 notes reading as a thin estate. A
# re-mint that fails, on either path, is raised as a 401 carrying the
# cause (ControlPlaneError.token_refresh_error): the credentials behind
# the walk are gone. A fresh token refused as well is raised as a 401
# flagged refused_after_refresh: the identity is unmapped, or a session
# key expired during the walk (presigning is offline, so the mint could
# not tell). Either way the walk ends the cluster on it with the
# credential advice rather than noting collection after collection.
_STS_PRESIGN_EXPIRES = 60
_TOKEN_REFRESH_S = 600
_CLUSTER_NAME_HEADER = "x-k8s-aws-id"
# Bounds on every HTTP call, the control plane's and AWS's alike (the AWS
# clients take them below, in make_client_factory). The kubernetes client's default
# is NO timeout, so one hung endpoint (a private control plane behind a
# dropped route) would stall the whole walk indefinitely; with these it
# degrades into the per-collection note or per-cluster error the walk
# already handles. Connect fails fast; the read window is generous enough
# for a 500-object page from a slow control plane. The client's pool
# retries are pinned too: left unset, the kubernetes client hands urllib3
# its default of three, and a stalled endpoint would cost four connects
# and four read windows per call. One retry absorbs a transient reset and
# keeps the worst case per call at twice the pair above — handed over as a
# Retry object that ignores a `Retry-After` header, whose sleep urllib3
# would otherwise take uncapped, outside both timeouts. The AWS clients
# carry the same pair and the same single retry — `total_max_attempts` two,
# since botocore's `max_attempts` counts retries — in its standard mode:
# its own defaults are a minute per connect and per read and several
# retries in its legacy mode, so a region whose endpoint dropped packets
# cost minutes before the walk read it as unreachable.
_CONNECT_TIMEOUT_S = 10
_READ_TIMEOUT_S = 120
_RETRIES = 1


def missing_dependencies() -> list:
    """Names the optional packages the live scan needs but that are absent.

    Called before any AWS work so the tool can return an actionable install
    line instead of an ImportError traceback. Kept here, beside the only
    imports, so the check and the imports cannot drift apart.
    """
    missing = []
    for module_name, package in (("boto3", "boto3"), ("botocore", "botocore"),
                                 ("kubernetes", "kubernetes")):
        try:
            __import__(module_name)
        except Exception:
            missing.append(package)
    return missing


# The client libraries narrate at DEBUG: botocore logs every request's
# SigV4 CanonicalRequest, StringToSign and Signature — and a presigned STS
# URL's signature IS the bearer token the API server accepts — and its
# parsers log raw AWS response bodies; urllib3 and the kubernetes client log
# request lines. The MCP server's root logger runs at DEBUG with a file
# handler, so all of that would land in its log. These loggers are held at
# INFO from the moment the first client is built until the scan releases
# them (release_library_logs) — a child given its own level (`botocore.auth`
# at DEBUG) held with its family — only ever raised, never lowered: an
# operator who set one to WARNING keeps that, and one who set DEBUG on
# purpose gets it back once the walk is over.
_LIBRARY_LOGGERS = ("boto3", "botocore", "urllib3", "kubernetes")
_HELD_LIBRARY_LEVELS = {}   # logger name -> the level it had before the hold
_HOLD_DEPTH = 0             # holders outstanding; levels return at zero
_HOLD_LOCK = threading.Lock()


def _hold_library_logs_at_info() -> None:
    """Raises the AWS and Kubernetes client loggers to INFO when they would
    otherwise inherit DEBUG, so no signing material or response body is
    written to the server's log (see _LIBRARY_LOGGERS). Remembers each
    level it changed so release_library_logs can put it back; a second
    holder before the first releases keeps the hold in place. A child
    logger with a level of its own (`botocore.auth` at DEBUG) does not
    inherit its parent's hold, so every existing child of a held family
    with an explicit level below INFO is held the same way."""
    global _HOLD_DEPTH
    with _HOLD_LOCK:
        _HOLD_DEPTH += 1
        held = set(_LIBRARY_LOGGERS)
        for name, existing in list(logging.root.manager.loggerDict.items()):
            if (isinstance(existing, logging.Logger) and existing.level
                    and name.partition(".")[0] in _LIBRARY_LOGGERS):
                held.add(name)
        for name in sorted(held):
            logger = logging.getLogger(name)
            if logger.getEffectiveLevel() < logging.INFO:
                _HELD_LIBRARY_LEVELS.setdefault(name, logger.level)
                logger.setLevel(logging.INFO)


def release_library_logs() -> None:
    """Restores every logger level _hold_library_logs_at_info changed, once
    the last holder has released — the scan calls this when its walk is
    over, whatever the outcome, so the hold lasts exactly as long as a
    client that could sign is in use. A level an operator raised above
    INFO was never touched and is not touched now."""
    global _HOLD_DEPTH
    with _HOLD_LOCK:
        _HOLD_DEPTH = max(0, _HOLD_DEPTH - 1)
        if _HOLD_DEPTH:
            return
        while _HELD_LIBRARY_LEVELS:
            name, level = _HELD_LIBRARY_LEVELS.popitem()
            logging.getLogger(name).setLevel(level)


def make_client_factory(profile_name=None):
    """Returns a `client_factory(service, region)` over one boto3 session.

    One session, reused across every service and region, so a single set of
    local credentials is resolved once — from the named profile, or the
    default chain when none is given. Clients are cached per (service,
    region): the cloud walk asks for the same ec2 client many times. This
    is the only place boto3 is constructed, so the tool never imports it —
    and the one place the client libraries' DEBUG narration is switched
    off before it can write a signature (_hold_library_logs_at_info; the
    caller releases it with release_library_logs when the clients are
    done — taken before boto3 is even imported, so that release always
    pairs with a hold of this call's own, an import that fails included).
    Every client carries the control-plane bounds (_CONNECT_TIMEOUT_S,
    _READ_TIMEOUT_S, _RETRIES) through a botocore Config, which counts
    total attempts under `total_max_attempts` (its `max_attempts` means
    retries, one fewer), so one more than the retries.
    """
    _hold_library_logs_at_info()
    import boto3
    from botocore.config import Config

    session = boto3.session.Session(profile_name=profile_name)
    config = Config(connect_timeout=_CONNECT_TIMEOUT_S,
                    read_timeout=_READ_TIMEOUT_S,
                    retries={"total_max_attempts": _RETRIES + 1,
                             "mode": "standard"})
    cache = {}

    def client_factory(service, region):
        key = (service, region)
        if key not in cache:
            cache[key] = session.client(service, region_name=region,
                                        config=config)
        return cache[key]

    return client_factory


def caller_identity(client_factory, region="us-east-1") -> dict:
    """Verifies credentials resolve, returning the STS caller identity.

    A read-only preflight: if this fails, nothing downstream can succeed,
    and the failure (no credentials, expired SSO) is reported once here
    rather than as N per-region errors. `region` picks the STS endpoint —
    the caller passes the first region it is about to scan, so the call
    lands in that region's partition (a China-partition credential cannot
    reach sts.us-east-1.amazonaws.com at all).
    """
    sts = client_factory("sts", region)
    identity = sts.get_caller_identity()
    return {"account": identity.get("Account"), "arn": identity.get("Arn"),
            "user_id": identity.get("UserId")}


# The exception classes botocore raises when a region's endpoint cannot be
# reached at all — no DNS name for it (a mistyped region), no route to it, a
# proxy that will not connect, a connection that times out or drops, a TLS
# handshake a middlebox breaks — as against a credential it refused. One
# that names the endpoint it failed on is the region's only when that
# endpoint is STS: a credential provider's own endpoint (an SSO portal, the
# instance metadata service) failing inside the call is the credential's
# failure (_is_sts_endpoint).
_UNREACHABLE_ENDPOINT_ERRORS = frozenset({
    "EndpointConnectionError", "ConnectTimeoutError", "ReadTimeoutError",
    "ConnectionClosedError", "SSLError", "EndpointResolutionError",
    "InvalidRegionError", "ProxyConnectionError"})
_AWS_HOST_SUFFIXES = (".amazonaws.com", ".amazonaws.com.cn", ".c2s.ic.gov",
                      ".sc2s.sgov.gov")


def is_unreachable_endpoint(error) -> bool:
    """True when `error` — or a cause it wraps — says the AWS endpoint could
    not be reached at all (_UNREACHABLE_ENDPOINT_ERRORS): a mistyped region
    has no STS endpoint to answer, which is not a credential's failure.
    Judged by class name, so botocore need not be imported to ask."""
    depth = 0
    while error is not None and depth < 8:
        if type(error).__name__ in _UNREACHABLE_ENDPOINT_ERRORS:
            return _is_sts_endpoint(getattr(error, "kwargs", None))
        error = error.__cause__ or error.__context__
        depth += 1
    return False


def aws_error_code(error) -> str:
    """The AWS error code a botocore ClientError carries
    (`InvalidClientTokenId`, `ExpiredToken`) — on `error` or a cause it
    wraps — or "" when there is none. Read from the response's shape, so
    botocore need not be imported to ask."""
    depth = 0
    while error is not None and depth < 8:
        response = getattr(error, "response", None)
        if isinstance(response, dict):
            block = response.get("Error")
            code = block.get("Code") if isinstance(block, dict) else None
            if code:
                return str(code)
        error = error.__cause__ or error.__context__
        depth += 1
    return ""


def _is_sts_endpoint(kwargs) -> bool:
    """False when the error's `endpoint_url` names a credential provider's
    endpoint failing inside the call — an SSO portal or any AWS host that is
    not STS, the instance metadata or container credential service
    (link-local, or loopback: botocore admits a container credential URI on
    localhost only) — since that is the credential's failure, not the
    region's. True otherwise: an STS host (regional, FIPS or a VPC
    endpoint), a remote override or proxy, or an error that names no
    endpoint."""
    url = kwargs.get("endpoint_url") if isinstance(kwargs, dict) else None
    if not url:
        return True
    host = (urlsplit(str(url)).hostname or "").lower()
    if (host.startswith(("169.254.", "127.", "fd00:ec2:"))
            or host in ("localhost", "::1")):
        return False
    if host.endswith(_AWS_HOST_SUFFIXES):
        labels = host.split(".")
        return "sts" in labels or "sts-fips" in labels
    return True


# The loggers kubernetes.client.Configuration() sets to WARNING as it is
# built (its `debug` property's default), whatever they were before: put
# back, so the hold's promise — only ever raised, never lowered — holds for
# an operator who set one of them to ERROR.
_CONFIGURATION_LOGGERS = ("urllib3", "client", "kubernetes.client")


def _configuration(k8s_client):
    """A kubernetes client Configuration, built without leaving its mark on
    any logger's level (_CONFIGURATION_LOGGERS)."""
    levels = {name: logging.getLogger(name).level
              for name in _CONFIGURATION_LOGGERS}
    configuration = k8s_client.Configuration()
    for name, level in levels.items():
        logging.getLogger(name).setLevel(level)
    return configuration


def make_get_json(cluster: dict, client_factory):
    """Builds a `get_json(path)` bound to one EKS cluster's control plane.

    Uses the cluster's endpoint and CA data (from the AWS describe_cluster
    already in the IR) plus a freshly presigned STS token. Returns the
    decoded list object, None on 404 (an uninstalled CRD group), and raises
    on any other non-2xx so k8s_live records it as a note against that
    collection.

    The token lives 15 minutes from minting (see _STS_PRESIGN_EXPIRES) and
    a large cluster's walk can outlive it, so the client re-mints: before a
    call once the token is _TOKEN_REFRESH_S old, and once more when a call
    answers 401 — then retries that call. A second 401 is the cluster's
    verdict — on the identity (unmapped in aws-auth, say) or, since
    presigning is offline, on a session credential that expired during the
    walk — and is raised; live_discovery's advice names both. A re-mint
    that itself fails, before a call or after a 401, is raised as a 401
    carrying that failure: the credentials are gone, not the mapping.
    """
    from kubernetes import client as k8s_client
    from urllib3.util.retry import Retry

    endpoint = cluster.get("endpoint")
    ca_data = cluster.get("certificate_authority_data")
    if not endpoint or not ca_data:
        raise ValueError(
            f"cluster {cluster.get('name')} has no endpoint or CA data to "
            "connect with (the AWS describe_cluster call did not return them)")

    ca_path = _write_ca(ca_data)

    configuration = _configuration(k8s_client)
    configuration.host = endpoint
    # The client emits the Authorization header verbatim from the BearerToken
    # auth setting's value — the api_key entry joined to its matching
    # api_key_prefix, read from the configuration on every call, which is
    # what lets a re-mint below take effect without a new client. Which
    # identifier the prefix is read under changed in client v36: it now keys
    # the prefix on 'BearerToken', not the 'authorization' alias it resolves
    # the key through (kubernetes-client/python#2595). Set both identifiers
    # so the header comes out "Bearer <token>" on every client version.
    # Without the scheme prefix EKS treats the request as anonymous and the
    # whole walk 401s — invisible to unit tests that mock call_api, caught
    # only against a real control plane.
    configuration.api_key_prefix = {"authorization": "Bearer",
                                    "BearerToken": "Bearer"}
    configuration.ssl_ca_cert = ca_path
    # A Retry object rather than the bare count: given the count, urllib3
    # honours a `Retry-After` header on a 429/503 with an uncapped sleep,
    # outside both timeouts (kube-apiserver sends `Retry-After: 1`; a front
    # in between may send anything).
    configuration.retries = Retry(total=_RETRIES,
                                  respect_retry_after_header=False)
    minted_at = [0.0]

    def mint():
        token = _presigned_token(cluster["name"], cluster["region"],
                                 client_factory)
        configuration.api_key = {"authorization": token, "BearerToken": token}
        minted_at[0] = time.monotonic()

    holder = {}

    def _cleanup():
        # The CA bundle is public key material, but one temp file per cluster
        # per scan would pile up in a long-lived server — and so would the
        # client's connections. ApiClient.close() releases only its thread
        # pool; the sockets live in the REST client's urllib3 PoolManager,
        # cleared here as well. The caller runs this once the cluster walk
        # is done with the client.
        import os
        api = holder.pop("api", None)
        pool = getattr(getattr(api, "rest_client", None), "pool_manager", None)
        for release in (getattr(api, "close", None),
                        getattr(pool, "clear", None)):
            if release is None:
                continue
            try:
                release()
            except Exception:   # noqa: BLE001 — best effort on teardown
                pass
        try:
            os.unlink(ca_path)
        except OSError:
            pass

    try:
        mint()
        holder["api"] = k8s_client.ApiClient(configuration)
    except Exception:
        _cleanup()   # nothing to hand the file to — do not leave it behind
        raise
    api = holder["api"]

    def get_json(path):
        if time.monotonic() - minted_at[0] > _TOKEN_REFRESH_S:
            try:
                mint()
            except Exception as mint_error:
                # The token is about to go stale and no fresh one can be
                # signed: the credentials behind the walk are gone (an SSO
                # session ended, a provider's refresh failed), not the
                # mapping. Raised as the 401 the stale token would have
                # earned, carrying the cause — the verdict the 401 path
                # below reaches — so the walk ends the cluster with the
                # credential advice instead of noting collection after
                # collection under a botocore error name, which would read
                # as a thin estate.
                raise ControlPlaneError(
                    401, "Unauthorized",
                    "the token needed refreshing and a fresh one could not "
                    f"be minted ({type(mint_error).__name__}: {mint_error})",
                    token_refresh_error=mint_error) from mint_error
        retried = False
        while True:
            try:
                # `_preload_content=False` returns the raw urllib3 response
                # and we decode the JSON ourselves. This sidesteps the
                # client's typed deserializer, whose call_api keyword for it
                # has been renamed across major versions (`response_type` →
                # `response_types_map`), while the raw-response path has been
                # stable since v12. A non-2xx status raises ApiException
                # before we get here.
                response = api.call_api(
                    path, "GET", auth_settings=["BearerToken"],
                    _preload_content=False,
                    _request_timeout=(_CONNECT_TIMEOUT_S, _READ_TIMEOUT_S))
                return json.loads(response.data)
            except k8s_client.exceptions.ApiException as e:
                if e.status == 404:
                    return None
                if e.status == 401 and not retried:
                    # An expired token and an unmapped identity both read
                    # as 401; a fresh token is free, so tell them apart by
                    # trying once with one.
                    retried = True
                    try:
                        mint()
                    except Exception as mint_error:
                        # No retry happened, so the 401 stands as the
                        # verdict — and the cause is the credentials
                        # (expired, revoked), not the mapping.
                        raise ControlPlaneError(
                            e.status, e.reason,
                            "the token was refused and a fresh one could "
                            "not be minted for the retry "
                            f"({type(mint_error).__name__}: {mint_error})",
                            token_refresh_error=mint_error) from mint_error
                    continue
                if e.status == 401 and retried:
                    # The fresh token was refused as well: an unmapped
                    # identity, or a session key that expired during the
                    # walk. Either way no later call fares better, so the
                    # walk ends the cluster on it (k8s_live).
                    raise ControlPlaneError(
                        e.status, e.reason, api_server_message(e.body),
                        refused_after_refresh=True) from e
                raise ControlPlaneError(e.status, e.reason,
                                        api_server_message(e.body)) from e

    get_json.cleanup = _cleanup
    return get_json


class ControlPlaneError(RuntimeError):
    """A non-2xx answer from a control plane, reduced to what a note needs.

    The kubernetes client's ApiException renders as several lines of HTTP
    headers (audit ids, cache directives) around the one fact that matters.
    The walk records `str(error)` verbatim in a per-collection note, so this
    keeps that note to one line: the status, its reason, and the API
    server's own message — which for a 403 names the identity and the
    missing verb, exactly what the operator needs to fix the mapping. A
    failure with no HTTP answer at all (the client's status 0 with the
    error's class name over its detail as the reason: a TLS handshake it
    could not complete) is rendered as no answer, on one line still,
    rather than as an `HTTP 0`.
    `token_refresh_error` is set when a re-mint failed — on the 401 whose
    retry could not mint a fresh token, and on the 401 raised in place of
    the call a pre-call refresh could not sign for: then the AWS
    credentials themselves are gone, not the identity's mapping, and the
    advice differs (live_discovery); k8s_live ends the cluster walk on it.
    `refused_after_refresh` is set on the 401 a freshly minted token earned
    on the retry: the identity is unmapped, or its session key expired
    during the walk, and k8s_live ends the walk on it the same way.
    """

    def __init__(self, status, reason, message="", token_refresh_error=None,
                 refused_after_refresh=False):
        reason_lines = [line.strip() for line in str(reason or "").splitlines()
                        if line.strip()]
        reason = reason_lines[0] if reason_lines else ""
        if not message and len(reason_lines) > 1:
            message = " ".join(reason_lines[1:])
        message = " ".join(line.strip() for line in str(message or "").splitlines()
                           if line.strip())
        self.status = status
        self.reason = reason
        self.message = message
        self.token_refresh_error = token_refresh_error
        self.refused_after_refresh = refused_after_refresh
        text = (f"HTTP {status} {reason}".rstrip() if status
                else f"no HTTP answer ({reason})" if reason else "no HTTP answer")
        # A bare 401 answers with message "Unauthorized" — the reason again.
        if message and message.lower() != (reason or "").lower():
            text = f"{text}: {message}"
        super().__init__(text)


def api_server_message(body) -> str:
    """The `message` of a Kubernetes Status body, or "" when there is none.

    Pure: the API server answers every error with a Status object whose
    `message` is the human-readable line; anything else (an HTML page from
    a proxy, an empty body) yields "" and the caller falls back to the HTTP
    reason.
    """
    if not body:
        return ""
    if isinstance(body, bytes):
        body = body.decode("utf-8", errors="replace")
    try:
        parsed = json.loads(body)
    except (TypeError, ValueError):
        return ""
    if isinstance(parsed, dict) and isinstance(parsed.get("message"), str):
        return parsed["message"].strip()
    return ""


def _presigned_token(cluster_name, region, client_factory) -> str:
    """Presigns an STS GetCallerIdentity URL as an EKS bearer token.

    The cluster name travels in a signed header (x-k8s-aws-id), which is
    what binds the token to this one cluster; a token minted for cluster A
    is rejected by cluster B.

    Built with the client's public `generate_presigned_url`, so the STS
    endpoint (regional, and the right partition — `amazonaws.com.cn` in
    China) and the credentials come from the client itself rather than a
    hand-written host and a private attribute. The header is not a
    GetCallerIdentity parameter, so two event hooks — the same technique the
    AWS CLI's own `eks get-token` uses — carry it through: one accepts it as
    a parameter and parks it in the request context, the other reads it back
    and sets it on the request right before signing. Registered with a fixed
    unique_id, so re-registering on the cached client is a no-op.
    """
    sts = client_factory("sts", region)
    sts.meta.events.register(
        "provide-client-params.sts.GetCallerIdentity",
        _accept_cluster_name_param, unique_id=_CLUSTER_NAME_HEADER + "-param")
    sts.meta.events.register(
        "before-sign.sts.GetCallerIdentity",
        _sign_cluster_name_header, unique_id=_CLUSTER_NAME_HEADER + "-header")
    presigned = sts.generate_presigned_url(
        "get_caller_identity", Params={_CLUSTER_NAME_HEADER: cluster_name},
        ExpiresIn=_STS_PRESIGN_EXPIRES, HttpMethod="GET")
    return _encode_token(presigned)


def _accept_cluster_name_param(params, context, **kwargs):
    """Moves the cluster name out of the call's params (where the STS model
    would reject it) into the request context, for _sign_cluster_name_header."""
    if _CLUSTER_NAME_HEADER in params:
        context[_CLUSTER_NAME_HEADER] = params.pop(_CLUSTER_NAME_HEADER)


def _sign_cluster_name_header(request, **kwargs):
    """Sets the cluster-name header on the request before SigV4 signs it, so
    it lands in X-Amz-SignedHeaders and EKS can verify the binding."""
    cluster_name = request.context.get(_CLUSTER_NAME_HEADER)
    if cluster_name:
        request.headers[_CLUSTER_NAME_HEADER] = cluster_name


def _encode_token(presigned_url: str) -> str:
    """Wraps a presigned STS URL as an EKS bearer token: base64url without
    padding, behind the scheme's fixed prefix.

    Pure and import-free — the token *format* contract, split out from the
    signing so it can be checked without boto3 or a live STS.
    """
    encoded = base64.urlsafe_b64encode(
        presigned_url.encode("utf-8")).decode("utf-8")
    return _TOKEN_PREFIX + encoded.rstrip("=")


def _write_ca(ca_data: str) -> str:
    """Materializes the base64 CA bundle to a temp file for the TLS client.

    The kubernetes client verifies the control plane against a CA file path;
    the data comes base64-encoded in the cluster description. A real CA
    certificate is public key material, not a secret.
    """
    import os
    import tempfile

    payload = base64.b64decode(ca_data)   # first: a bad bundle leaves no file
    handle = tempfile.NamedTemporaryFile(
        mode="wb", suffix=".pem", delete=False)
    try:
        handle.write(payload)
    except BaseException:
        handle.close()
        os.unlink(handle.name)   # and a write that fails leaves none either
        raise
    handle.close()
    return handle.name
