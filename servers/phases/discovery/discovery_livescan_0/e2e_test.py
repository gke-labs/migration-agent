"""End-to-end: STATE_DISCOVERY_LIVE through the real server, ledger and DAG.

Everything the step touches outside the process is faked on the loopback;
everything inside the process is real:

  fake   AWS endpoint (HTTP; EKS rest-json, STS/IAM/ELBv2/Autoscaling query
         XML, EC2 XML) reached through AWS_ENDPOINT_URL, signed with the AWS
         documentation's example key pair (never a real credential);
  fake   EKS control plane (HTTPS with a self-signed CA that the fake EKS
         hands out as certificateAuthority.data) that checks every bearer
         token the way aws-iam-authenticator does — a presigned STS
         GetCallerIdentity URL with x-k8s-aws-id among the signed headers and
         a 60s X-Amz-Expires — answers the first authenticated request with a
         401 to exercise the re-mint, pages pods, serves installed-but-empty
         CRD groups, and 404s everything else;
  real   FastMCP server object from servers/dag/main.py (the tool is called
         through mcp.call_tool, not imported), the real platform_dag.json,
         the real ledger module on a FakeStorageClient, the real schema, the
         downstream discover_configuration_files step, and the frontend app.

Planted in the estate are customer-authored VALUES in every placement a live
object carries them — env literals, command tokens, ConfigMap and Secret data,
probe and route headers, annotations, AWS tags and labels — beside the
references and structural fields a migration reads. The test's central claim
is the package's: no planted value reaches any persisted object, and every
structural field does.
"""
import asyncio
import base64
import csv
import datetime
import hashlib
import hmac
import io
import logging
import ipaddress
import json
import math
import os
import shutil
import socket
import ssl
import sys
import tempfile
import threading
import time
import unittest
import urllib.parse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from unittest import mock

import servers.dag.state_management as state_mgr
from servers.dag import fake_gcs
# Imported at module load, under the bare name main_test uses — `main`, on
# the `servers/dag` path entry the documented runner sets — so the two test
# files share one module object whatever order they are collected in:
# main.py registers the phase tools and the mutation runner at import, and a
# second copy under another name would leave main_test's patches on
# `main.*` pointing at whichever import ran last. (Importing the server
# configures the process's root logger — a DEBUG level and a file handler;
# main.py's own residual.)
import main as dag_main
from servers.phases.discovery.discovery_livescan_0 import live_schema, projection, tools

try:
    from cryptography import x509
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import rsa
    from cryptography.x509.oid import ExtendedKeyUsageOID, NameOID
except ImportError:  # pragma: no cover - transitive dep of google-auth
    x509 = None

_HERE = os.path.dirname(os.path.abspath(__file__))
_REPO = os.path.normpath(os.path.join(_HERE, "..", "..", "..", ".."))
_DAG_PATH = os.path.join(_REPO, "servers", "dag", "platform_dag.json")

ACCOUNT = "123456789012"
ECR = f"{ACCOUNT}.dkr.ecr.us-east-1.amazonaws.com"
OIDC = "oidc.eks.us-east-1.amazonaws.com/id/EXAMPLED539D4633E53DE1B71EXAMPLE"
CALLER_ARN = f"arn:aws:iam::{ACCOUNT}:user/e2e"
WEB_ROLE_ARN = f"arn:aws:iam::{ACCOUNT}:role/shop-web"
LB_NAME = "k8s-shop-web-e2e"
LB_ARN = (f"arn:aws:elasticloadbalancing:us-east-1:{ACCOUNT}:loadbalancer/app/"
          f"{LB_NAME}/0123456789abcdef")
# The AWS documentation's example access key pair. Not a credential.
EXAMPLE_KEY_ID = "AKIAIOSFODNN7EXAMPLE"
EXAMPLE_SECRET = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"

# Every planted credential value, by placement. None may be persisted.
SENTINELS = {
    "env AWS_ACCESS_KEY_ID": EXAMPLE_KEY_ID,
    "env AWS_SECRET_ACCESS_KEY": EXAMPLE_SECRET,
    "env DATABASE_URL password": "e2eUrlPw9",
    "env CLIENT_SECRET": "e2eClientSecretHex0123456789abcdef",
    "env SPRING_APPLICATION_JSON password": "e2eSpringPw",
    "env WEBHOOK_SECRET URL path": "e2eHookSecretPath",
    "configmap values.yaml credentials password": "e2eValuesPw",
    "configmap ordered.json value after a boolean": "e2eOrderedPw",
    "args --token=": "e2eArgTok3n",
    "probe header X-Api-Key": "e2eProbeK3y",
    "readiness exec curl -H X-Api-Key": "e2eReadyK3y",
    "lifecycle curl -H X-Token": "e2eCurlT0k",
    "cronjob args --api-key=": "e2eCronK3y",
    "deployment annotation password": "e2eAnnotationPw",
    "configmap db-password": "e2eCmPassw0rd",
    "configmap app.env API_TOKEN": "e2eEnvFileT0k",
    "configmap app.env DATABASE_URL password": "e2eCmUrlPw",
    "configmap config.yaml block scalar line 1": "e2eBlockLine1",
    "configmap config.yaml block scalar line 2": "e2eBlockLine2",
    "configmap settings.json apiToken": "e2eJsonT0k",
    "configmap tls.key PEM body": "e2ePemBodyMIIEvQIBADANBgkqhkiG9w0BAQEFAASC",
    "configmap clientSecret": "0123456789abcdef0123456789abcdef",
    "secret db-creds password (base64)": base64.b64encode(b"e2eSecretValue").decode(),
    "secret regcred .dockerconfigjson (base64)": base64.b64encode(
        b'{"auths":{"x":{"auth":"e2eDockerAuth"}}}').decode(),
    "httproute header X-Api-Key": "e2eRouteK3y",
    "httproute rewrite target query token": "e2eRewriteT0k",
    "traefik match Headers() literal": "e2eTraefikK3y",
    "EKS cluster tag db_password": "e2eClusterTagSecret",
    "subnet tag db_password": "e2eSubnetTagSecret",
    "nodegroup label password": "e2eNodegroupLabelSecret",
}
# The credential inside a decoded Secret value — a docker config's `auth`
# member. A JSON or CSV ledger body carries a decoded document with its
# quotes escaped or doubled, so the document as a whole can never match
# there; the innermost value is searched on its own.
DECODED_INNER = {"secret regcred docker auth (decoded, inner)": "e2eDockerAuth"}
# Benign customer text planted beside the credentials. The projection keeps
# structure, not values, so these leave the record exactly as the credentials
# do — the test asserts it, and a projection that began keeping free text
# would fail there first.
BENIGN = {
    "env DB_SECRET_NAME": "db-creds",
    "configmap existingSecret": "shop-tls",
    "configmap values.yaml passwordSecret name": "shop-db-pw-ref",
    "ingress auth-secret annotation": "basic-auth",
    "httproute X-Trace header": "e2eTraceIdKept",
    "config.yaml non-secret line": "pool: 5",
    "settings.json non-secret line": '"listen": "0.0.0.0"',
    "configmap tls.crt": "-----BEGIN CERTIFICATE-----",
    "args --port=": "--port=8080",
    "readiness exec Accept header": "Accept: application/json",
    "lifecycle curl URL": "http://warm/up",
}
# The benign values that name nothing else in the estate (`db-creds` and
# `shop-tls` are a Secret's name and a `secretKeyRef`/`secretName` too, and
# survive as those): absent from every persisted object, like the credentials.
FREE_TEXT = {what: value for what, value in BENIGN.items()
             if value not in ("db-creds", "shop-tls")}
# A structural reference, kept in the carrier it was planted in.
STRUCTURAL = {"imagePullSecrets name": "regcred"}

AWS_ENV = {
    "AWS_ACCESS_KEY_ID": EXAMPLE_KEY_ID, "AWS_SECRET_ACCESS_KEY": EXAMPLE_SECRET,
    "AWS_DEFAULT_REGION": "us-east-1", "AWS_EC2_METADATA_DISABLED": "true",
    "AWS_CONFIG_FILE": "/dev/null", "AWS_SHARED_CREDENTIALS_FILE": "/dev/null",
}


# --------------------------------------------------------------------------
# fake EKS control plane
# --------------------------------------------------------------------------
def _self_signed():
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "e2e-fake-eks")])
    now = datetime.datetime.now(datetime.timezone.utc)
    cert = (x509.CertificateBuilder().subject_name(name).issuer_name(name)
            .public_key(key.public_key()).serial_number(x509.random_serial_number())
            .not_valid_before(now - datetime.timedelta(minutes=5))
            .not_valid_after(now + datetime.timedelta(days=1))
            .add_extension(x509.SubjectAlternativeName(
                [x509.DNSName("localhost"),
                 x509.IPAddress(ipaddress.ip_address("127.0.0.1"))]), critical=False)
            .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True)
            .add_extension(x509.ExtendedKeyUsage([ExtendedKeyUsageOID.SERVER_AUTH]),
                           critical=False)
            .sign(key, hashes.SHA256()))
    return (cert.public_bytes(serialization.Encoding.PEM),
            key.private_bytes(serialization.Encoding.PEM,
                              serialization.PrivateFormat.TraditionalOpenSSL,
                              serialization.NoEncryption()))


def _meta(name, namespace=None, **extra):
    meta = {"name": name, "uid": f"uid-{name}", "resourceVersion": "4242",
            "managedFields": [{"manager": "kubectl", "operation": "Apply"}]}
    if namespace:
        meta["namespace"] = namespace
    meta.update(extra)
    return meta


def _template(containers, sa, extra=None):
    spec = {"serviceAccountName": sa, "containers": containers,
            "imagePullSecrets": [{"name": STRUCTURAL["imagePullSecrets name"]}]}
    spec.update(extra or {})
    return {"metadata": {"labels": {"app": containers[0]["name"]}}, "spec": spec}


def _node(name, zone, instance_id):
    return {"metadata": _meta(name, labels={
                "node.kubernetes.io/instance-type": "m6i.large",
                "eks.amazonaws.com/nodegroup": "general",
                "eks.amazonaws.com/capacityType": "ON_DEMAND",
                "topology.kubernetes.io/zone": zone}),
            "spec": {"providerID": f"aws:///{zone}/{instance_id}"},
            "status": {"capacity": {"cpu": "2", "memory": "7950000Ki", "pods": "29"},
                       "allocatable": {"cpu": "1930m", "memory": "7300000Ki", "pods": "29"},
                       "nodeInfo": {"kubeletVersion": "v1.31.4-eks-2d5f260",
                                    "containerRuntimeVersion": "containerd://1.7.23",
                                    "osImage": "Amazon Linux 2023", "architecture": "amd64"},
                       "conditions": [{"type": "Ready", "status": "True"}],
                       "images": [{"names": [f"{ECR}/shop/web:2.3.1"], "sizeBytes": 1}]}}


def _pod(name, namespace, node, container, image):
    return {"metadata": _meta(name, namespace, labels={"app": container}),
            "spec": {"nodeName": node, "containers": [{"name": container, "image": image}]},
            "status": {"phase": "Running", "containerStatuses": [
                {"name": container, "image": image, "ready": True,
                 "imageID": f"{image.split(':')[0]}@sha256:{'a' * 64}"}]}}


def _estate():
    shop = "shop"
    web = {
        "name": "web", "image": f"{ECR}/shop/web:2.3.1",
        "env": [
            {"name": "DB_HOST", "value": "db.shop.svc"},
            {"name": "AWS_ACCESS_KEY_ID", "value": SENTINELS["env AWS_ACCESS_KEY_ID"]},
            {"name": "AWS_SECRET_ACCESS_KEY", "value": SENTINELS["env AWS_SECRET_ACCESS_KEY"]},
            {"name": "DB_PASSWORD", "valueFrom": {"secretKeyRef": {"name": "db-creds", "key": "password"}}},
            {"name": "DATABASE_URL", "value": f"postgres://app:{SENTINELS['env DATABASE_URL password']}@db.shop.svc:5432/app"},
            {"name": "CLIENT_SECRET", "value": SENTINELS["env CLIENT_SECRET"]},
            {"name": "SPRING_APPLICATION_JSON",
             "value": '{"spring":{"datasource":{"username":"app","password":"'
                      + SENTINELS["env SPRING_APPLICATION_JSON password"] + '"}}}'},
            {"name": "WEBHOOK_SECRET",
             "value": f"https://hooks.slack.com/services/T0/B0/{SENTINELS['env WEBHOOK_SECRET URL path']}"},
            {"name": "DB_SECRET_NAME", "value": BENIGN["env DB_SECRET_NAME"]}],
        "args": [f"--token={SENTINELS['args --token=']}", BENIGN["args --port="]],
        "resources": {"requests": {"cpu": "250m", "memory": "256Mi"}},
        "livenessProbe": {"httpGet": {"path": "/healthz", "port": 8080, "httpHeaders": [
            {"name": "X-Api-Key", "value": SENTINELS["probe header X-Api-Key"]}]}},
        "readinessProbe": {"exec": {"command": [
            "curl", "-H", f"X-Api-Key: {SENTINELS['readiness exec curl -H X-Api-Key']}",
            "-H", BENIGN["readiness exec Accept header"], "http://127.0.0.1:8080/ready"]}},
        "lifecycle": {"postStart": {"exec": {"command": [
            "sh", "-c", f"curl -H 'X-Token: {SENTINELS['lifecycle curl -H X-Token']}' "
                        f"{BENIGN['lifecycle curl URL']}"]}}},
    }
    configmap = {
        "LOG_LEVEL": "info",
        "db-password": SENTINELS["configmap db-password"],
        "app.env": ("LOG_LEVEL=info\n"
                    f"API_TOKEN={SENTINELS['configmap app.env API_TOKEN']}\n"
                    f"DATABASE_URL=mysql://root:{SENTINELS['configmap app.env DATABASE_URL password']}@db/app\n"),
        "config.yaml": ("db:\n  host: db.shop.svc\n  password: |\n"
                        f"    {SENTINELS['configmap config.yaml block scalar line 1']}\n"
                        f"    {SENTINELS['configmap config.yaml block scalar line 2']}\n"
                        "  pool: 5\n"),
        "settings.json": ('{\n  "apiToken": "' + SENTINELS["configmap settings.json apiToken"]
                          + '",\n  ' + BENIGN["settings.json non-secret line"] + '\n}\n'),
        "tls.key": ("-----BEGIN PRIVATE KEY-----\n"
                    f"{SENTINELS['configmap tls.key PEM body']}\n-----END PRIVATE KEY-----\n"),
        "tls.crt": "-----BEGIN CERTIFICATE-----\nMIICzTCCAbWgAwIBAgIJe2e\n-----END CERTIFICATE-----\n",
        "clientSecret": SENTINELS["configmap clientSecret"],
        "values.yaml": (f"passwordSecret:\n  name: {BENIGN['configmap values.yaml passwordSecret name']}\n  key: password\n"
                        "credentials:\n  - name: primary\n    host: db.shop.svc\n"
                        f"    password: {SENTINELS['configmap values.yaml credentials password']}\n"),
        "ordered.json": ('{"password": {"encrypted": false, "value": "'
                         + SENTINELS["configmap ordered.json value after a boolean"] + '"}}'),
        "existingSecret": BENIGN["configmap existingSecret"],
    }
    return {
        "/api/v1/namespaces": [{"metadata": _meta("shop")}, {"metadata": _meta("kube-system")}],
        "/apis/apps/v1/deployments": [
            {"metadata": _meta("web", shop, annotations={"password": SENTINELS["deployment annotation password"]}),
             "spec": {"replicas": 2, "selector": {"matchLabels": {"app": "web"}},
                      "template": _template([web], "web-sa")},
             "status": {"readyReplicas": 2}},
            {"metadata": _meta("coredns", "kube-system"),
             "spec": {"replicas": 2, "selector": {"matchLabels": {"k8s-app": "kube-dns"}},
                      "template": _template([{"name": "coredns", "image": f"{ECR}/eks/coredns:v1.11.1"}], "coredns")}}],
        "/apis/apps/v1/statefulsets": [
            {"metadata": _meta("db", shop),
             "spec": {"replicas": 1, "serviceName": "db", "selector": {"matchLabels": {"app": "db"}},
                      "template": _template([{"name": "db", "image": "postgres:16",
                                              "env": [{"name": "POSTGRES_PASSWORD", "valueFrom": {"secretKeyRef": {"name": "db-creds", "key": "password"}}}]}], "default"),
                      "volumeClaimTemplates": [{"metadata": {"name": "data"},
                                                "spec": {"accessModes": ["ReadWriteOnce"], "storageClassName": "gp2",
                                                         "resources": {"requests": {"storage": "20Gi"}}}}]}}],
        "/apis/apps/v1/daemonsets": [],
        "/apis/batch/v1/cronjobs": [
            {"metadata": _meta("report", shop),
             "spec": {"schedule": "0 3 * * *", "jobTemplate": {"spec": {"template": _template(
                 [{"name": "report", "image": f"{ECR}/shop/report:1.0", "command": ["python", "report.py"],
                   "args": [f"--api-key={SENTINELS['cronjob args --api-key=']}"]}], "default",
                 {"restartPolicy": "OnFailure"})}}}}],
        "/apis/autoscaling/v2/horizontalpodautoscalers": [
            {"metadata": _meta("web", shop),
             "spec": {"scaleTargetRef": {"apiVersion": "apps/v1", "kind": "Deployment", "name": "web"},
                      "minReplicas": 2, "maxReplicas": 10,
                      "metrics": [{"type": "Resource", "resource": {"name": "cpu", "target": {"type": "Utilization", "averageUtilization": 70}}}]}}],
        "/api/v1/services": [
            {"metadata": _meta("web", shop, annotations={"service.beta.kubernetes.io/aws-load-balancer-type": "nlb"}),
             "spec": {"type": "LoadBalancer", "selector": {"app": "web"}, "ports": [{"port": 80, "targetPort": 8080}]},
             "status": {"loadBalancer": {"ingress": [{"hostname": f"{LB_NAME}-1.us-east-1.elb.amazonaws.com"}]}}}],
        "/apis/networking.k8s.io/v1/ingresses": [
            {"metadata": _meta("web", shop, annotations={
                "alb.ingress.kubernetes.io/scheme": "internet-facing",
                "nginx.ingress.kubernetes.io/auth-secret": BENIGN["ingress auth-secret annotation"]}),
             "spec": {"ingressClassName": "alb", "tls": [{"hosts": ["shop.example.com"], "secretName": "shop-tls"}],
                      "rules": [{"host": "shop.example.com", "http": {"paths": [{"path": "/", "pathType": "Prefix",
                                 "backend": {"service": {"name": "web", "port": {"number": 80}}}}]}}]}}],
        "/apis/networking.k8s.io/v1/ingressclasses": [
            {"metadata": _meta("alb"), "spec": {"controller": "ingress.k8s.aws/alb"}}],
        "/api/v1/serviceaccounts": [
            {"metadata": _meta("web-sa", shop, annotations={"eks.amazonaws.com/role-arn": WEB_ROLE_ARN})},
            {"metadata": _meta("default", shop)}],
        "/api/v1/persistentvolumeclaims": [
            {"metadata": _meta("data-db-0", shop),
             "spec": {"accessModes": ["ReadWriteOnce"], "storageClassName": "gp2", "volumeName": "pvc-e2e-0001",
                      "resources": {"requests": {"storage": "20Gi"}}},
             "status": {"phase": "Bound", "capacity": {"storage": "20Gi"}}}],
        "/api/v1/persistentvolumes": [
            {"metadata": _meta("pvc-e2e-0001"),
             "spec": {"capacity": {"storage": "20Gi"}, "accessModes": ["ReadWriteOnce"], "storageClassName": "gp2",
                      "persistentVolumeReclaimPolicy": "Delete",
                      "csi": {"driver": "ebs.csi.aws.com", "volumeHandle": "vol-0e2e0001", "fsType": "ext4"},
                      "claimRef": {"namespace": shop, "name": "data-db-0"}},
             "status": {"phase": "Bound"}}],
        "/apis/storage.k8s.io/v1/storageclasses": [
            {"metadata": _meta("gp2", annotations={"storageclass.kubernetes.io/is-default-class": "true"}),
             "provisioner": "ebs.csi.aws.com", "parameters": {"type": "gp3", "encrypted": "true"},
             "reclaimPolicy": "Delete", "volumeBindingMode": "WaitForFirstConsumer"}],
        "/api/v1/configmaps": [{"metadata": _meta("app-config", shop), "data": configmap}],
        "/api/v1/secrets": [
            {"metadata": _meta("db-creds", shop), "type": "Opaque",
             "data": {"password": SENTINELS["secret db-creds password (base64)"],
                      "username": base64.b64encode(b"app").decode()}},
            {"metadata": _meta("regcred", shop), "type": "kubernetes.io/dockerconfigjson",
             "data": {".dockerconfigjson": SENTINELS["secret regcred .dockerconfigjson (base64)"]}}],
        "/api/v1/nodes": [_node("ip-10-0-1-10.ec2.internal", "us-east-1a", "i-0a"),
                          _node("ip-10-0-2-11.ec2.internal", "us-east-1b", "i-0b")],
        "/api/v1/pods": [
            _pod("web-7d9f8-abcde", shop, "ip-10-0-1-10.ec2.internal", "web", f"{ECR}/shop/web:2.3.1"),
            _pod("web-7d9f8-fghij", shop, "ip-10-0-2-11.ec2.internal", "web", f"{ECR}/shop/web:2.3.1"),
            _pod("db-0", shop, "ip-10-0-1-10.ec2.internal", "db", "postgres:16"),
            _pod("coredns-5d78c-xyz12", "kube-system", "ip-10-0-1-10.ec2.internal", "coredns", f"{ECR}/eks/coredns:v1.11.1")],
        # Installed CRD groups with nothing in them: served, empty, not absent.
        "/apis/karpenter.sh/v1/nodepools": [],
        "/apis/karpenter.k8s.aws/v1/ec2nodeclasses": [],
        "/apis/gateway.networking.k8s.io/v1/gatewayclasses": [],
        "/apis/gateway.networking.k8s.io/v1/gateways": [],
        "/apis/gateway.networking.k8s.io/v1/httproutes": [
            {"metadata": _meta("shop-route", shop),
             "spec": {"parentRefs": [{"name": "shop-gw"}],
                      "rules": [{"matches": [{"path": {"type": "PathPrefix", "value": "/api"}, "method": "GET"}],
                                 "timeouts": {"request": "10s"},
                                 "filters": [{"type": "RequestHeaderModifier", "requestHeaderModifier": {"set": [
                                    {"name": "X-Api-Key", "value": SENTINELS["httproute header X-Api-Key"]},
                                    {"name": "X-Trace", "value": BENIGN["httproute X-Trace header"]}]}},
                                    {"type": "URLRewrite", "urlRewrite": {"hostname": "web.internal", "path": {
                                        "type": "ReplacePrefixMatch",
                                        "replacePrefixMatch": "/internal?token=" + SENTINELS["httproute rewrite target query token"]}}}],
                                 "backendRefs": [{"name": "web", "port": 80}]}]}}],
        "/apis/traefik.io/v1alpha1/ingressroutes": [
            {"metadata": _meta("shop-ir", shop),
             "spec": {"entryPoints": ["websecure"],
                      "routes": [{"match": "Host(`shop.example.com`) && Headers(`X-Api-Key`, `"
                                           + SENTINELS["traefik match Headers() literal"] + "`)",
                                  "kind": "Rule", "services": [{"name": "web", "port": 80}]}]}}],
        "/apis/traefik.io/v1alpha1/middlewares": [],
    }


def _k8s_status(code, reason, message):
    return {"kind": "Status", "apiVersion": "v1", "metadata": {}, "status": "Failure",
            "message": message, "reason": reason, "code": code}


def _sigv4_signature(url, secret, cluster_name):
    """The SigV4 signature the authenticator recomputes for a presigned
    GetCallerIdentity URL: over the sorted query (less the signature), the
    `host` and `x-k8s-aws-id` headers it signed, and an empty payload, with
    the signing key derived from the secret through the credential scope."""
    parts = urllib.parse.urlsplit(url)
    pairs = urllib.parse.parse_qsl(parts.query, keep_blank_values=True)
    params = dict(pairs)
    canonical_query = "&".join(
        f"{urllib.parse.quote(k, safe='-_.~')}={urllib.parse.quote(v, safe='-_.~')}"
        for k, v in sorted(pairs) if k != "X-Amz-Signature")
    signed_headers = params["X-Amz-SignedHeaders"]
    headers = {"host": parts.netloc, "x-k8s-aws-id": cluster_name}
    canonical_headers = "".join(f"{h}:{headers[h]}\n" for h in signed_headers.split(";"))
    canonical_request = "\n".join([
        "GET", parts.path or "/", canonical_query, canonical_headers, signed_headers,
        hashlib.sha256(b"").hexdigest()])
    scope = params["X-Amz-Credential"].split("/", 1)[1]
    string_to_sign = "\n".join([
        "AWS4-HMAC-SHA256", params["X-Amz-Date"], scope,
        hashlib.sha256(canonical_request.encode()).hexdigest()])
    key = ("AWS4" + secret).encode()
    for piece in scope.split("/"):
        key = hmac.new(key, piece.encode(), hashlib.sha256).digest()
    return hmac.new(key, string_to_sign.encode(), hashlib.sha256).hexdigest()


class FakeControlPlane:
    """HTTPS API server with authenticator-grade token checks: the token is
    decoded, its shape checked, and its signature recomputed from the
    example secret and THIS cluster's name, exactly as EKS's authenticator
    does through STS — a token minted for another cluster, or with another
    key, does not verify."""

    CLUSTER_NAME = "shop-prod"

    def __init__(self, workdir):
        self.objects = _estate()
        self.log = []                 # (path?query, status)
        self.signatures = []          # every verified token's signature
        self.tokens = []              # every Authorization header served
        self.rejected = []            # the header the one-shot 401 refused
        self.reject_next = True       # one-shot 401 on the first good token
        cert, key = _self_signed()
        self.ca_data = base64.b64encode(cert).decode()
        self._cert = os.path.join(workdir, "cert.pem")
        self._key = os.path.join(workdir, "key.pem")
        with open(self._cert, "wb") as f:
            f.write(cert)
        with open(self._key, "wb") as f:
            f.write(key)
        os.chmod(self._key, 0o600)
        self._server = None

    def start(self):
        plane = self

        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def log_message(self, *args):
                pass

            def do_GET(self):
                status, payload = plane._respond(self.path, self.headers.get("Authorization"))
                data = json.dumps(payload).encode()
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)

        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        ctx.load_cert_chain(self._cert, self._key)
        self._server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self._server.socket = ctx.wrap_socket(self._server.socket, server_side=True)
        threading.Thread(target=self._server.serve_forever, daemon=True).start()
        return f"https://127.0.0.1:{self._server.server_address[1]}"

    def stop(self):
        if self._server:
            self._server.shutdown()
            self._server.server_close()

    def _token_problem(self, header):
        prefix = "Bearer k8s-aws-v1."
        if not header or not header.startswith(prefix):
            return "no EKS bearer token"
        token = header[len(prefix):]
        try:
            url = base64.urlsafe_b64decode(token + "=" * (-len(token) % 4)).decode()
        except Exception:
            return "token is not base64url"
        q = urllib.parse.parse_qs(urllib.parse.urlsplit(url).query)
        if q.get("Action") != ["GetCallerIdentity"]:
            return "not a GetCallerIdentity presign"
        signed = (q.get("X-Amz-SignedHeaders") or [""])[0].lower().split(";")
        if "x-k8s-aws-id" not in signed:
            return "x-k8s-aws-id is not a signed header"
        if q.get("X-Amz-Expires") != ["60"]:
            return f"X-Amz-Expires must be 60, got {q.get('X-Amz-Expires')}"
        if not (q.get("X-Amz-Credential") or [""])[0].startswith(EXAMPLE_KEY_ID + "/"):
            return "credential scope carries the wrong key id"
        signature = (q.get("X-Amz-Signature") or [""])[0]
        if not signature:
            return "unsigned"
        try:
            expected = _sigv4_signature(url, EXAMPLE_SECRET, self.CLUSTER_NAME)
        except KeyError as e:
            return f"signed header {e} is not one the authenticator sends"
        if not hmac.compare_digest(signature, expected):
            return "signature does not verify for this cluster and key"
        self.signatures.append(signature)
        return None

    def _respond(self, raw_path, auth):
        problem = self._token_problem(auth)
        if problem is None and self.reject_next:
            self.reject_next = False
            self.rejected.append(auth)
            # The client answers this with a re-mint: an offline signature
            # over the current second (X-Amz-Date). Let that second pass
            # before answering, so a fresh token is distinguishable from a
            # replay of the refused one.
            now = time.time()
            time.sleep(math.ceil(now) - now + 0.05)
            problem = "simulated expired token"
        if problem:
            self.log.append((raw_path, 401))
            return 401, _k8s_status(401, "Unauthorized", problem)
        self.tokens.append(auth)
        url = urllib.parse.urlsplit(raw_path)
        status, payload = self._serve(url.path, urllib.parse.parse_qs(url.query))
        self.log.append((raw_path, status))
        return status, payload

    def _serve(self, path, q):
        if path == "/version":
            return 200, {"major": "1", "minor": "31+", "gitVersion": "v1.31.4-eks-2d5f260"}
        if path not in self.objects:
            return 404, _k8s_status(404, "NotFound", f"no resource at {path}")
        items, meta = self.objects[path], {"resourceVersion": "4242"}
        if path == "/api/v1/pods":
            token = (q.get("continue") or [None])[0]
            if token is None:
                items, meta = items[:2], {**meta, "continue": "e2e-page-2"}
            elif token == "e2e-page-2":
                items = items[2:]
            else:
                return 410, _k8s_status(410, "Expired", "continue token expired")
        return 200, {"kind": "List", "apiVersion": "v1", "metadata": meta,
                     "items": json.loads(json.dumps(items))}


# --------------------------------------------------------------------------
# fake AWS
# --------------------------------------------------------------------------
_STS_NS = "https://sts.amazonaws.com/doc/2011-06-15/"
_IAM_NS = "https://iam.amazonaws.com/doc/2010-05-08/"
_ASG_NS = "http://autoscaling.amazonaws.com/doc/2011-01-01/"
_ELB_NS = "http://elasticloadbalancing.amazonaws.com/doc/2015-12-01/"
_EC2_NS = "http://ec2.amazonaws.com/doc/2016-11-15/"
_TRUST = {"Version": "2012-10-17", "Statement": [{
    "Effect": "Allow", "Action": "sts:AssumeRoleWithWebIdentity",
    "Principal": {"Federated": f"arn:aws:iam::{ACCOUNT}:oidc-provider/{OIDC}"},
    "Condition": {"StringEquals": {f"{OIDC}:sub": "system:serviceaccount:shop:web-sa",
                                   f"{OIDC}:aud": "sts.amazonaws.com"}}}]}


def _query_xml(root, ns, body):
    return (f'<{root} xmlns="{ns}">{body}<ResponseMetadata><RequestId>e2e</RequestId>'
            f"</ResponseMetadata></{root}>")


def _ec2_xml(root, body):
    return f'<{root} xmlns="{_EC2_NS}"><requestId>e2e</requestId>{body}</{root}>'


class FakeAws:
    """One HTTP endpoint for every service; two clusters, one region."""

    def __init__(self, prod_endpoint, staging_endpoint, ca_data):
        self.prod_endpoint, self.staging_endpoint, self.ca_data = prod_endpoint, staging_endpoint, ca_data
        self.log = []                 # action names in arrival order
        self.unhandled = []           # actions no handler answered (a 400)
        self._server = None

    def start(self):
        aws = self

        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def log_message(self, *args):
                pass

            def do_GET(self):
                aws._handle(self)

            def do_POST(self):
                aws._handle(self)

        self._server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        threading.Thread(target=self._server.serve_forever, daemon=True).start()
        return f"http://127.0.0.1:{self._server.server_address[1]}"

    def stop(self):
        if self._server:
            self._server.shutdown()
            self._server.server_close()

    def _handle(self, h):
        length = int(h.headers.get("Content-Length") or 0)
        body = h.rfile.read(length).decode() if length else ""
        url = urllib.parse.urlsplit(h.path)
        if url.path.startswith("/clusters"):
            status, payload = self._eks(url.path, urllib.parse.parse_qs(url.query))
            data, ctype = json.dumps(payload).encode(), "application/json"
        else:
            form = urllib.parse.parse_qs(body)
            status, text = self._query((form.get("Action") or ["?"])[0], form)
            data, ctype = text.encode(), "text/xml"
        h.send_response(status)
        h.send_header("Content-Type", ctype)
        h.send_header("Content-Length", str(len(data)))
        h.end_headers()
        h.wfile.write(data)

    def _cluster(self, name):
        prod = name == "shop-prod"
        return {"name": name, "arn": f"arn:aws:eks:us-east-1:{ACCOUNT}:cluster/{name}",
                "status": "ACTIVE", "version": "1.31", "platformVersion": "eks.12",
                "endpoint": self.prod_endpoint if prod else self.staging_endpoint,
                "certificateAuthority": {"data": self.ca_data},
                "resourcesVpcConfig": {"vpcId": "vpc-0e2e", "subnetIds": ["subnet-0a", "subnet-0b"],
                                       "securityGroupIds": ["sg-0e2e"], "clusterSecurityGroupId": "sg-0cluster",
                                       "endpointPublicAccess": True, "endpointPrivateAccess": True,
                                       "publicAccessCidrs": ["0.0.0.0/0"]},
                "identity": {"oidc": {"issuer": f"https://{OIDC}"}},
                "logging": {"clusterLogging": [{"types": ["api", "audit"], "enabled": True}]},
                "tags": {"env": "prod" if prod else "staging",
                         "db_password": SENTINELS["EKS cluster tag db_password"]}}

    _ADDONS = {"shop-prod": {"vpc-cni": {"addonVersion": "v1.18.1-eksbuild.1", "status": "ACTIVE",
                                         "serviceAccountRoleArn": f"arn:aws:iam::{ACCOUNT}:role/shop-prod-vpc-cni"},
                             "coredns": {"addonVersion": "v1.11.1-eksbuild.4", "status": "ACTIVE"}},
               "shop-staging": {}}
    _NODEGROUPS = {"shop-prod": {"general": {
        "status": "ACTIVE", "capacityType": "ON_DEMAND", "instanceTypes": ["m6i.large"],
        "amiType": "AL2023_x86_64_STANDARD", "releaseVersion": "1.31.4-20250101", "diskSize": 50,
        "scalingConfig": {"minSize": 2, "maxSize": 5, "desiredSize": 2},
        "labels": {"tier": "general", "password": SENTINELS["nodegroup label password"]},
        "taints": [], "subnets": ["subnet-0a", "subnet-0b"],
        "nodeRole": f"arn:aws:iam::{ACCOUNT}:role/shop-prod-node",
        "launchTemplate": {"id": "lt-0e2e", "version": "3"},
        "resources": {"autoScalingGroups": [{"name": "eks-general-e2e"}]}}},
        "shop-staging": {}}

    def _eks(self, path, query):
        parts = path.strip("/").split("/")
        if parts == ["clusters"]:
            token = (query.get("nextToken") or [None])[0]
            self.log.append(f"ListClusters nextToken={token}")
            if token is None:
                return 200, {"clusters": ["shop-prod"], "nextToken": "page-2"}
            return 200, {"clusters": ["shop-staging"]}
        name = parts[1]
        if name not in self._ADDONS:
            return 404, {"message": f"No cluster found for name: {name}."}
        if len(parts) == 2:
            self.log.append(f"DescribeCluster {name}")
            return 200, {"cluster": self._cluster(name)}
        addons = parts[2] == "addons"
        table = self._ADDONS if addons else self._NODEGROUPS
        if len(parts) == 3:
            self.log.append(f"{'ListAddons' if addons else 'ListNodegroups'} {name}")
            return 200, {"addons" if addons else "nodegroups": sorted(table[name])}
        self.log.append(f"{'DescribeAddon' if addons else 'DescribeNodegroup'} {name}/{parts[3]}")
        record = table[name].get(parts[3])
        return (200, {"addon" if addons else "nodegroup": record}) if record else (404, {"message": "not found"})

    def _query(self, action, form):
        self.log.append(action)
        if action == "GetCallerIdentity":
            return 200, _query_xml("GetCallerIdentityResponse", _STS_NS,
                                   f"<GetCallerIdentityResult><Arn>{CALLER_ARN}</Arn><UserId>AIDAE2E</UserId>"
                                   f"<Account>{ACCOUNT}</Account></GetCallerIdentityResult>")
        if action == "DescribeAutoScalingGroups":
            instances = "".join(
                f"<member><InstanceId>{iid}</InstanceId><AvailabilityZone>{az}</AvailabilityZone>"
                "<LifecycleState>InService</LifecycleState><HealthStatus>Healthy</HealthStatus></member>"
                for iid, az in (("i-0a", "us-east-1a"), ("i-0b", "us-east-1b")))
            return 200, _query_xml("DescribeAutoScalingGroupsResponse", _ASG_NS,
                                   "<DescribeAutoScalingGroupsResult><AutoScalingGroups><member>"
                                   "<AutoScalingGroupName>eks-general-e2e</AutoScalingGroupName>"
                                   "<MinSize>2</MinSize><MaxSize>5</MaxSize><DesiredCapacity>2</DesiredCapacity>"
                                   "<AvailabilityZones><member>us-east-1a</member><member>us-east-1b</member></AvailabilityZones>"
                                   f"<Instances>{instances}</Instances></member></AutoScalingGroups>"
                                   "</DescribeAutoScalingGroupsResult>")
        if action == "DescribeLoadBalancers":
            return 200, _query_xml("DescribeLoadBalancersResponse", _ELB_NS,
                                   "<DescribeLoadBalancersResult><LoadBalancers><member>"
                                   f"<LoadBalancerArn>{LB_ARN}</LoadBalancerArn>"
                                   f"<DNSName>{LB_NAME}-1.us-east-1.elb.amazonaws.com</DNSName>"
                                   f"<LoadBalancerName>{LB_NAME}</LoadBalancerName><Scheme>internet-facing</Scheme>"
                                   "<Type>application</Type><VpcId>vpc-0e2e</VpcId>"
                                   "</member></LoadBalancers></DescribeLoadBalancersResult>")
        if action == "DescribeTags":
            return 200, _query_xml("DescribeTagsResponse", _ELB_NS,
                                   f"<DescribeTagsResult><TagDescriptions><member><ResourceArn>{LB_ARN}</ResourceArn>"
                                   "<Tags><member><Key>elbv2.k8s.aws/cluster</Key><Value>shop-prod</Value></member>"
                                   "<member><Key>ingress.k8s.aws/stack</Key><Value>shop/web</Value></member></Tags>"
                                   "</member></TagDescriptions></DescribeTagsResult>")
        if action == "GetRole":
            role = form["RoleName"][0]
            doc = urllib.parse.quote(json.dumps(_TRUST), safe="")
            return 200, _query_xml("GetRoleResponse", _IAM_NS,
                                   f"<GetRoleResult><Role><Path>/</Path><RoleName>{role}</RoleName><RoleId>AROAE2E</RoleId>"
                                   f"<Arn>arn:aws:iam::{ACCOUNT}:role/{role}</Arn><CreateDate>2025-01-01T00:00:00Z</CreateDate>"
                                   f"<AssumeRolePolicyDocument>{doc}</AssumeRolePolicyDocument></Role></GetRoleResult>")
        if action == "ListAttachedRolePolicies":
            return 200, _query_xml("ListAttachedRolePoliciesResponse", _IAM_NS,
                                   "<ListAttachedRolePoliciesResult><AttachedPolicies><member>"
                                   "<PolicyName>AmazonS3ReadOnlyAccess</PolicyName>"
                                   "<PolicyArn>arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess</PolicyArn></member>"
                                   "</AttachedPolicies><IsTruncated>false</IsTruncated></ListAttachedRolePoliciesResult>")
        if action == "ListRolePolicies":
            return 200, _query_xml("ListRolePoliciesResponse", _IAM_NS,
                                   "<ListRolePoliciesResult><PolicyNames><member>inline-sqs</member></PolicyNames>"
                                   "<IsTruncated>false</IsTruncated></ListRolePoliciesResult>")
        if action == "DescribeVpcs":
            return 200, _ec2_xml("DescribeVpcsResponse",
                                 "<vpcSet><item><vpcId>vpc-0e2e</vpcId><state>available</state><cidrBlock>10.0.0.0/16</cidrBlock>"
                                 "<cidrBlockAssociationSet><item><associationId>a</associationId><cidrBlock>10.0.0.0/16</cidrBlock></item>"
                                 "</cidrBlockAssociationSet></item></vpcSet>")
        if action == "DescribeSubnets":
            subnets = "".join(
                f"<item><subnetId>{sid}</subnetId><vpcId>vpc-0e2e</vpcId><availabilityZone>{az}</availabilityZone>"
                f"<cidrBlock>{cidr}</cidrBlock><availableIpAddressCount>200</availableIpAddressCount>"
                "<mapPublicIpOnLaunch>false</mapPublicIpOnLaunch><tagSet>"
                "<item><key>kubernetes.io/role/internal-elb</key><value>1</value></item>"
                f"<item><key>db_password</key><value>{SENTINELS['subnet tag db_password']}</value></item></tagSet></item>"
                for sid, az, cidr in (("subnet-0a", "us-east-1a", "10.0.1.0/24"), ("subnet-0b", "us-east-1b", "10.0.2.0/24")))
            return 200, _ec2_xml("DescribeSubnetsResponse", f"<subnetSet>{subnets}</subnetSet>")
        if action == "DescribeSecurityGroups":
            groups = "".join(
                f"<item><ownerId>{ACCOUNT}</ownerId><groupId>{gid}</groupId><groupName>{gid}-name</groupName>"
                "<groupDescription>e2e</groupDescription><ipPermissions><item><ipProtocol>tcp</ipProtocol>"
                "<fromPort>443</fromPort><toPort>443</toPort></item></ipPermissions></item>"
                for gid in ("sg-0e2e", "sg-0cluster"))
            return 200, _ec2_xml("DescribeSecurityGroupsResponse", f"<securityGroupInfo>{groups}</securityGroupInfo>")
        if action == "DescribeRouteTables":
            return 200, _ec2_xml("DescribeRouteTablesResponse",
                                 "<routeTableSet><item><routeTableId>rtb-0a</routeTableId><vpcId>vpc-0e2e</vpcId></item></routeTableSet>")
        self.unhandled.append(action)
        return 400, ("<ErrorResponse><Error><Code>InvalidAction</Code>"
                     f"<Message>no e2e handler for {action}</Message></Error></ErrorResponse>")


# --------------------------------------------------------------------------
# the test
# --------------------------------------------------------------------------


@unittest.skipIf(x509 is None, "cryptography is needed to mint the fake control plane's certificate")
class LiveDiscoveryEndToEndTest(unittest.IsolatedAsyncioTestCase):
    """The live step from the MCP boundary to the ledger, and what reads it."""

    @classmethod
    def setUpClass(cls):
        cls.dag_main = dag_main
        cls.workdir = tempfile.mkdtemp(prefix="live-e2e-")
        cls.plane = FakeControlPlane(cls.workdir)
        prod_endpoint = cls.plane.start()
        # shop-staging's endpoint is a loopback port bound and released just
        # now, so nothing listens on it: a reachable AWS record, an
        # unreachable control plane.
        with socket.socket() as probe:
            probe.bind(("127.0.0.1", 0))
            closed_port = probe.getsockname()[1]
        cls.aws = FakeAws(prod_endpoint, f"https://127.0.0.1:{closed_port}", cls.plane.ca_data)
        cls.aws_endpoint = cls.aws.start()

    @classmethod
    def tearDownClass(cls):
        cls.plane.stop()
        cls.aws.stop()
        shutil.rmtree(cls.workdir, ignore_errors=True)

    def setUp(self):
        # A clean AWS environment, not an additive one: a developer's
        # AWS_PROFILE, an AWS_ENDPOINT_URL_STS override or an HTTPS_PROXY
        # would route the seam off the loopback fakes.
        inherited = {k: v for k, v in os.environ.items()
                     if not (k.upper().startswith(("AWS_", "GKMA_"))
                             or k.upper().endswith("_PROXY"))}
        self._env = mock.patch.dict(
            os.environ, {**inherited, **AWS_ENV, "AWS_ENDPOINT_URL": self.aws_endpoint,
                         "NO_PROXY": "127.0.0.1,localhost"}, clear=True)
        self._env.start()
        self.addCleanup(self._env.stop)
        # Registered as a cleanup, not left to tearDown: a setUp that fails
        # after this line still hands the module its globals back.
        self.addCleanup(self._restore_state_module, state_mgr.LEDGER_CONFIG_PATH,
                        state_mgr.LEDGER_CONFIG_DIR, state_mgr.gcs_client)
        email = mock.patch.object(state_mgr, "get_authenticated_user_email", return_value="eng@x.com")
        email.start()
        self.addCleanup(email.stop)
        self._reset()

    def _reset(self):
        """Fresh fakes and a fresh ledger at STATE_DISCOVERY_LIVE — what a
        second scan within one test needs, without patching the environment
        or registering the module's restoration a second time."""
        self.plane.log.clear()
        self.plane.signatures.clear()
        self.plane.tokens.clear()
        self.plane.rejected.clear()
        self.plane.reject_next = True
        self.aws.log.clear()
        self.aws.unhandled.clear()
        config_dir = tempfile.mkdtemp(prefix="live-e2e-config-", dir=self.workdir)
        state_mgr.LEDGER_CONFIG_PATH = os.path.join(config_dir, "ledger_config.yaml")
        state_mgr.LEDGER_CONFIG_DIR = os.path.join(config_dir, "ledger_config.d")
        self.fake = fake_gcs.FakeStorageClient()
        state_mgr.gcs_client = self.fake
        self.bucket = self.fake.bucket("plat-ledger")
        self.bucket.blob("workspace_registry.yaml").upload_from_string(json.dumps(
            {"workspace_name": "ws-plat", "gcp_project": "proj",
             "roles": {"platform_engineers": ["eng@x.com"]}}))
        with open(_DAG_PATH) as f:
            self.bucket.blob("platform_dag.json").upload_from_string(f.read())
        self.bucket.blob("platform/onboarding/state.json").upload_from_string(json.dumps(
            {"current_state": "STATE_DISCOVERY_LIVE", "history": [], "variables": {}}))
        state_mgr.write_local_config("gs://plat-ledger", "platform", "ws-plat", "proj")

    @staticmethod
    def _restore_state_module(config_path, config_dir, client):
        state_mgr.LEDGER_CONFIG_PATH, state_mgr.LEDGER_CONFIG_DIR = config_path, config_dir
        state_mgr.gcs_client = client

    # -- helpers ------------------------------------------------------------
    async def _call(self, name, args):
        result = await self.dag_main.mcp.call_tool(name, args)
        if isinstance(result, tuple):
            result = result[0]
        return "".join(getattr(c, "text", "") for c in result)

    def _state(self):
        return json.loads(self.bucket.blob("platform/onboarding/state.json").download_as_text())

    def _live(self):
        names = sorted(b.name for b in self.bucket.list_blobs(prefix=tools.LIVE_PREFIX))
        return {n: self.bucket.blob(n).download_as_text() for n in names}

    def _generation(self, name):
        blob = self.bucket.blob(name)
        blob.reload()
        return blob.generation

    async def _scan(self):
        text = await self._call("discover_and_dump_all_clusters", {"regions": ["us-east-1"]})
        self.assertTrue(text.startswith("SUCCESS"), text[:400])
        # Every action the walk issued had a handler: an unanswered one
        # would surface only as a coverage note, and the scan would still
        # report success.
        self.assertEqual(self.aws.unhandled, [])
        return text

    # -- tests --------------------------------------------------------------
    async def test_the_server_walks_the_estate_and_advances_the_dag(self):
        listed = {t.name: t for t in await self.dag_main.mcp.list_tools()}
        self.assertIn("discover_and_dump_all_clusters", listed)
        props = set(listed["discover_and_dump_all_clusters"].inputSchema.get("properties", {}))
        self.assertLessEqual({"regions", "cluster_names", "namespaces", "aws_profile", "skip", "skip_reason"}, props)
        self.assertNotIn("include_manifests", props)

        text = await self._scan()
        self.assertIn("shop-prod", text)
        self.assertIn("shop-staging", text)

        state = self._state()
        self.assertEqual(state["current_state"], "STATE_DISCOVERY")
        self.assertTrue(any("STATE_DISCOVERY_LIVE" in h and "STATE_DISCOVERY" in h for h in state["history"]))
        record = state["variables"]["live_discovery"]
        self.assertEqual(record["status"], "completed")
        self.assertEqual(record["summary"]["clusters_found"], 2)
        self.assertEqual(record["summary"]["clusters_walked"], 1)
        self.assertEqual(record["summary"]["clusters_unreachable"], ["shop-staging"])

        live = self._live()
        csvs = [n for n in live if n.endswith(".csv")]
        self.assertEqual(len(live), 10, sorted(live))
        self.assertEqual(len(csvs), 9)
        self.assertIn(tools.LIVE_IR_BLOB, live)
        # CSVs first, the IR last.
        self.assertGreater(self._generation(tools.LIVE_IR_BLOB), max(self._generation(c) for c in csvs))

        ir = json.loads(live[tools.LIVE_IR_BLOB])
        live_schema.validate_live_ir(ir)
        self.assertEqual(ir["scanned_by"], CALLER_ARN)

        clusters = {c["name"]: c for c in ir["clusters"]}
        self.assertEqual(set(clusters), {"shop-prod", "shop-staging"})
        prod, staging = clusters["shop-prod"], clusters["shop-staging"]
        self.assertTrue(staging.get("kubernetes_error"))
        self.assertNotIn("kubernetes", staging)
        prod_json = json.dumps(prod)
        for aws_side in ("eks-general-e2e", "vpc-0e2e", "subnet-0a", "m6i.large", LB_ARN, "vpc-cni"):
            self.assertIn(aws_side, prod_json)
        self.assertEqual(prod["tags"], {"env": projection.OMITTED, "db_password": projection.OMITTED})
        self.assertEqual(prod["nodegroups"][0]["labels"], {"tier": projection.OMITTED, "password": projection.OMITTED})
        self.assertEqual(prod["network"]["subnets"][0]["tags"],
                         {"kubernetes.io/role/internal-elb": projection.OMITTED, "db_password": projection.OMITTED})

        k8s = prod["kubernetes"]
        workloads = json.dumps(k8s["workloads"])
        for name in ('"web"', '"db"', '"report"'):
            self.assertIn(name, workloads)
        self.assertNotIn("coredns", workloads)
        self.assertNotIn("kube-system", workloads)
        self.assertEqual(k8s["pod_count"], 3)
        identity = json.dumps(k8s["identity"])
        self.assertIn("web-sa", identity)
        self.assertIn(WEB_ROLE_ARN, identity)
        self.assertIn("system:serviceaccount:shop:web-sa", identity)
        self.assertIn("AmazonS3ReadOnlyAccess", identity)
        self.assertEqual(sorted(s["metadata"]["name"] for s in k8s["config"]["secrets"]), ["db-creds", "regcred"])
        self.assertEqual(len(k8s["autoscaling"]["hpas"]), 1)

        rows = list(csv.DictReader(io.StringIO(live[tools.LIVE_PREFIX + "clusters.csv"])))
        self.assertEqual(len(rows), 2)
        self.assertIn("shop/web:2.3.1", live[tools.LIVE_PREFIX + "images.csv"])
        self.assertIn("web-sa", live[tools.LIVE_PREFIX + "identity.csv"])

    async def test_no_planted_value_is_persisted_and_every_structural_field_survives(self):
        # Every in-cluster sentinel is really served (the AWS-side three are
        # pinned by their carriers in the previous test), so the sweep cannot
        # pass because a value sat under a path the walk never read.
        served = json.dumps(_estate())
        for what, value in SENTINELS.items():
            if not what.startswith(("EKS ", "subnet ", "nodegroup ")):
                self.assertIn(value, served, what)
        text = await self._scan()
        live = self._live()
        leaks = [f"{what} in {where}" for where, body in {**live, "tool response": text}.items()
                 for what, value in SENTINELS.items() if value in body]
        self.assertEqual(leaks, [])
        # Nor in a base64 spelling of a planted value (a Secret's encoding,
        # a `Basic` header's), for the values that are not base64 already.
        encoded = {what: base64.b64encode(value.encode()).decode()
                   for what, value in SENTINELS.items() if "(base64)" not in what}
        leaks = [f"{what} (base64) in {where}"
                 for where, body in {**live, "tool response": text}.items()
                 for what, value in encoded.items() if value in body]
        self.assertEqual(leaks, [])
        # Nor the plaintext of a Secret's base64 value: nothing decodes
        # one, and nothing may — not into the record, the response or the
        # ledger's state.
        decoded = {what: base64.b64decode(value).decode()
                   for what, value in SENTINELS.items() if "(base64)" in what}
        decoded.update(DECODED_INNER)
        state_text = json.dumps(self._state())
        bodies = {**live, "tool response": text, "state.json": state_text}
        leaks = [f"{what} (decoded) in {where}"
                 for where, body in bodies.items()
                 for what, value in decoded.items() if value in body]
        self.assertEqual(leaks, [])
        for what, value in {**SENTINELS, **encoded}.items():
            self.assertNotIn(value, state_text, what)
        # The benign text goes the same way as the credentials: the
        # projection keeps structure, never a value.
        leaks = [f"{what} in {where}" for where, body in live.items()
                 for what, value in FREE_TEXT.items() if value in body]
        self.assertEqual(leaks, [])
        # And each structural field sits in the carrier it was planted in,
        # with the omitted marker exactly where a value was.
        OMITTED = projection.OMITTED
        walk = json.loads(live[tools.LIVE_IR_BLOB])["clusters"][0]["kubernetes"]
        app_config = next(c for c in walk["config"]["configmaps"]
                          if c["metadata"]["name"] == "app-config")
        self.assertEqual(app_config["data"],
                         {key: OMITTED for key in _estate()["/api/v1/configmaps"][0]["data"]})
        secrets = {s["metadata"]["name"]: s for s in walk["config"]["secrets"]}
        self.assertEqual(secrets["db-creds"]["type"], "Opaque")
        self.assertEqual(secrets["db-creds"]["data"], {"password": OMITTED, "username": OMITTED})
        self.assertEqual(secrets["regcred"]["data"], {".dockerconfigjson": OMITTED})
        web = next(d for d in walk["workloads"]["Deployment"]
                   if d["metadata"]["name"] == "web")
        self.assertEqual(web["metadata"]["annotations"], {"password": OMITTED})
        self.assertEqual(web["spec"]["selector"], {"matchLabels": {"app": "web"}})
        self.assertEqual(web["spec"]["template"]["metadata"]["labels"], {"app": OMITTED})
        pod = web["spec"]["template"]["spec"]
        self.assertEqual(pod["serviceAccountName"], "web-sa")
        self.assertEqual(pod["imagePullSecrets"], [{"name": STRUCTURAL["imagePullSecrets name"]}])
        container = pod["containers"][0]
        self.assertEqual(container["image"], f"{ECR}/shop/web:2.3.1")
        env = {e["name"]: e for e in container["env"]}
        self.assertEqual(env["DB_PASSWORD"],
                         {"name": "DB_PASSWORD",
                          "valueFrom": {"secretKeyRef": {"name": "db-creds", "key": "password"}}})
        self.assertEqual({name: e["value"] for name, e in env.items() if "value" in e},
                         {name: OMITTED for name in env if name != "DB_PASSWORD"})
        self.assertEqual(container["args"], [OMITTED, OMITTED])
        self.assertEqual(container["resources"], {"requests": {"cpu": "250m", "memory": "256Mi"}})
        self.assertEqual(container["livenessProbe"]["httpGet"],
                         {"path": OMITTED, "port": 8080,
                          "httpHeaders": [{"name": "X-Api-Key", "value": OMITTED}]})
        self.assertEqual(container["readinessProbe"]["exec"]["command"], [OMITTED] * 6)
        self.assertEqual(container["lifecycle"]["postStart"]["exec"]["command"], [OMITTED] * 3)
        ingress = next(i for i in walk["networking"]["ingresses"]
                       if i["metadata"]["name"] == "web")
        self.assertEqual(ingress["metadata"]["annotations"], {
            "alb.ingress.kubernetes.io/scheme": "internet-facing",
            "nginx.ingress.kubernetes.io/auth-secret": OMITTED})
        self.assertEqual(ingress["spec"]["ingressClassName"], "alb")
        self.assertEqual(ingress["spec"]["tls"], [{"hosts": ["shop.example.com"], "secretName": "shop-tls"}])
        rule = ingress["spec"]["rules"][0]
        self.assertEqual(rule["host"], "shop.example.com")
        self.assertEqual(rule["http"]["paths"], [{"path": "/", "pathType": "Prefix",
                                                  "backend": {"service": {"name": "web", "port": {"number": 80}}}}])
        service = next(s for s in walk["networking"]["services"] if s["metadata"]["name"] == "web")
        self.assertEqual(service["metadata"]["annotations"],
                         {"service.beta.kubernetes.io/aws-load-balancer-type": "nlb"})
        self.assertEqual(service["spec"], {"type": "LoadBalancer", "selector": {"app": "web"},
                                           "ports": [{"port": 80, "targetPort": 8080}]})
        route = next(r for r in walk["networking"]["gateway_api"]["http_routes"]
                     if r["metadata"]["name"] == "shop-route")
        self.assertEqual(route["spec"]["parentRefs"], [{"name": "shop-gw"}])
        self.assertEqual(route["spec"]["rules"][0]["backendRefs"], [{"name": "web", "port": 80}])
        self.assertEqual(route["spec"]["rules"][0]["matches"],
                         [{"path": {"type": "PathPrefix", "value": "/api"}, "method": "GET"}])
        self.assertEqual(route["spec"]["rules"][0]["timeouts"], {"request": "10s"})
        self.assertEqual(route["spec"]["rules"][0]["filters"][0]["requestHeaderModifier"]["set"],
                         [{"name": "X-Api-Key", "value": OMITTED}, {"name": "X-Trace", "value": OMITTED}])
        self.assertEqual(route["spec"]["rules"][0]["filters"][1]["urlRewrite"],
                         {"hostname": "web.internal", "path": {"type": OMITTED, "replacePrefixMatch": OMITTED}})
        ingress_route = walk["networking"]["traefik"]["ingress_routes"][0]
        self.assertEqual(ingress_route["spec"]["routes"],
                         [{"match": OMITTED, "kind": "Rule", "services": [{"name": "web", "port": 80}]}])
        storage_class = walk["storage"]["storage_classes"][0]
        self.assertEqual(storage_class["metadata"]["annotations"],
                         {"storageclass.kubernetes.io/is-default-class": "true"})
        self.assertEqual(storage_class["provisioner"], "ebs.csi.aws.com")
        self.assertEqual(storage_class["parameters"], {"type": "gp3", "encrypted": "true"})
        sa = next(a for a in walk["identity"]["service_accounts"] if a["metadata"]["name"] == "web-sa")
        self.assertEqual(sa["metadata"]["annotations"], {"eks.amazonaws.com/role-arn": WEB_ROLE_ARN})
        # No runtime metadata and no status left on any projected object (the
        # AWS-side records keep their own `status`, an EKS lifecycle state).
        record = json.dumps(walk)
        for word in ('"managedFields"', '"resourceVersion"', '"uid"', '"status"'):
            self.assertNotIn(word, record, word)

    async def test_the_control_plane_protocol_is_the_authenticators(self):
        # The server's root logger runs at DEBUG (main.py); a capture handler
        # at that level sees everything the scan would have written to the
        # server log, signing material included, were the libraries not held
        # at INFO (eks_auth._hold_library_logs_at_info) — a hold the tool
        # releases once the walk is over.
        library = ("boto3", "botocore", "urllib3", "kubernetes")
        levels_before = {name: logging.getLogger(name).level for name in library}
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
            # The libraries inherit the root's DEBUG until the hold: this
            # capture would see their signing trace, were there no hold.
            self.assertEqual(logging.getLogger("botocore").getEffectiveLevel(),
                             logging.DEBUG)
            await self._scan()
        finally:
            root.removeHandler(handler)
            root.setLevel(saved)
        notes = json.loads(self._live()[tools.LIVE_IR_BLOB])["notes"]
        # Every token the plane accepted verified against the example secret
        # and this cluster's name; none of those signatures, nor the secret,
        # nor botocore's signing narration, reached the log.
        self.assertGreaterEqual(len(self.plane.signatures), 2)
        # Rendered as a handler would, a logged traceback included.
        rendered = "\n".join(logging.Formatter().format(r) for r in records)
        for signature in self.plane.signatures:
            self.assertNotIn(signature, rendered)
        # Nor any bearer token the plane saw — each is a signed URL, and a
        # signed URL is a credential for the minutes it is valid.
        self.assertGreaterEqual(len(self.plane.tokens), 2)
        for token in self.plane.tokens:
            self.assertNotIn(token, rendered)
            self.assertNotIn(token.split(" ")[-1], rendered)
        self.assertNotIn(EXAMPLE_SECRET, rendered)
        self.assertNotIn("StringToSign", rendered)
        self.assertNotIn("CanonicalRequest", rendered)
        # Nor any planted value, nor the plaintext of a Secret's base64
        # one: the log is a second sink beside the ledger.
        for what, value in SENTINELS.items():
            self.assertNotIn(value, rendered, what)
            if "(base64)" in what:
                self.assertNotIn(base64.b64decode(value).decode(), rendered, what)
        for what, value in DECODED_INNER.items():
            self.assertNotIn(value, rendered, what)
        self.assertEqual(
            sorted({r.name.split(".")[0] for r in records if r.levelno < logging.INFO}
                   & set(library)), [])
        self.assertEqual({name: logging.getLogger(name).level for name in library},
                         levels_before)
        # First authenticated request took the one-shot 401; the token was
        # re-minted and the same path retried, silently — with a token that
        # differs from the refused one (the plane let the signing second
        # pass), so a plain retry of the old token would not satisfy this.
        self.assertEqual(self.plane.log[:2], [("/version", 401), ("/version", 200)])
        self.assertEqual(len(self.plane.rejected), 1)
        self.assertNotIn(self.plane.rejected[0], self.plane.tokens)
        self.assertGreaterEqual(len(set(self.plane.signatures)), 2)
        self.assertFalse([n for n in notes if "401" in n])
        self.assertFalse([status for _, status in self.plane.log[2:] if status == 401])
        paths = [p for p, _ in self.plane.log]
        self.assertEqual(sum(1 for p in paths if p.startswith("/api/v1/pods")), 2)
        self.assertTrue(any("continue=e2e-page-2" in p for p in paths))
        self.assertTrue(any("/apis/autoscaling/v2/" in p for p in paths))
        self.assertFalse([p for p in paths if "v2beta2" in p])
        self.assertFalse([p for p in paths if "karpenter.sh/v1beta1" in p])
        self.assertFalse([n for n in notes if "arpenter" in n and "not present" in n])
        # AWS side: paged listing, per-cluster describes, IRSA resolution, LB
        # ownership by tag — and nothing but reads.
        self.assertIn("ListClusters nextToken=page-2", self.aws.log)
        self.assertIn("GetRole", self.aws.log)
        self.assertIn("DescribeTags", self.aws.log)
        self.assertIn("DescribeNodegroup shop-prod/general", self.aws.log)
        self.assertTrue(all(a.startswith(("Describe", "List", "Get")) for a in self.aws.log), self.aws.log)

    async def test_the_control_plane_refuses_a_token_it_cannot_verify(self):
        # The protocol test above proves the plane accepted the tokens the
        # scan minted. This proves the acceptance means something: the same
        # oracle rejects a tampered signature and a token minted for another
        # cluster — and, through the real server, a cluster whose name does
        # not match the token's binding ends unreachable on a 401 with the
        # per-cluster advice, not walked.
        await self._scan()
        header = self.plane.tokens[-1]
        self.assertIsNone(self.plane._token_problem(header))
        token = header[len("Bearer k8s-aws-v1."):]
        url = base64.urlsafe_b64decode(token + "=" * (-len(token) % 4)).decode()
        tampered = url[:-1] + ("0" if url[-1] != "0" else "1")
        tampered_header = "Bearer k8s-aws-v1." + base64.urlsafe_b64encode(
            tampered.encode()).decode().rstrip("=")
        self.assertEqual(self.plane._token_problem(tampered_header),
                         "signature does not verify for this cluster and key")
        self.plane.CLUSTER_NAME = "shop-other"      # shadows the class attribute
        self.addCleanup(lambda: delattr(self.plane, "CLUSTER_NAME"))
        self.assertEqual(self.plane._token_problem(header),
                         "signature does not verify for this cluster and key")

        # A fresh run against the misnamed plane: every token is bound to
        # shop-prod (the name AWS reports), so the plane refuses the first
        # one and the re-minted one alike.
        self._reset()
        self.plane.reject_next = False
        text = await self._scan()
        self.assertEqual(self.plane.tokens, [])
        self.assertEqual({status for _, status in self.plane.log}, {401})
        self.assertEqual([p for p, _ in self.plane.log], ["/version", "/version"])
        ir = json.loads(self._live()[tools.LIVE_IR_BLOB])
        self.assertEqual(ir["summary"]["clusters_walked"], 0)
        prod = {c["name"]: c for c in ir["clusters"]}["shop-prod"]
        self.assertTrue(prod["kubernetes_error"].startswith(
            "HTTP 401 Unauthorized: signature does not verify"), prod["kubernetes_error"])
        self.assertNotIn("kubernetes", prod)
        advice = [n for n in ir["notes"]
                  if n.startswith("cluster shop-prod: in-cluster walk failed")]
        self.assertEqual(len(advice), 1)
        self.assertIn("A freshly minted token was refused too", advice[0])
        self.assertIn("add an EKS access entry", advice[0])
        self.assertIn("expired during the walk", advice[0])
        self.assertIn("shop-prod", text)
        self.assertEqual(self._state()["current_state"], "STATE_DISCOVERY")

    async def test_the_frontend_and_the_next_step_read_what_the_scan_left(self):
        from starlette.testclient import TestClient
        from servers.frontend.server import GcsLedger, make_app

        await self._scan()
        with mock.patch("google.cloud.storage.Client", new=lambda *a, **k: self.fake):
            client = TestClient(make_app(GcsLedger()))
            self.assertIn("STATE_DISCOVERY", client.get("/api/overview").text)
            detail = client.get("/api/state/STATE_DISCOVERY_LIVE")
            self.assertEqual(detail.status_code, 200)
            self.assertIn(tools.LIVE_IR_BLOB, detail.text)
            served = client.get("/api/blob", params={"path": tools.LIVE_IR_BLOB})
            self.assertEqual(served.status_code, 200)
            self.assertIn("live_ir_version", served.text)
            self.assertNotEqual(client.get("/api/blob", params={"path": "workspace_registry.yaml"}).status_code, 200)

        before = {n: self._generation(n) for n in self._live()}
        root = tempfile.mkdtemp(prefix="src-", dir=self.workdir)
        os.makedirs(os.path.join(root, "k8s"))
        with open(os.path.join(root, "k8s", "deploy.yaml"), "w") as f:
            f.write(f"image: {ECR}/shop/web:2.3.1\n")
        text = await self._call("discover_configuration_files", {"root_dir": root})
        self.assertFalse(text.startswith("ERROR"), text[:400])
        state = self._state()
        self.assertEqual(state["current_state"], "STATE_DISCOVERY_SCOPING")
        self.assertEqual(state["variables"]["live_discovery"]["status"], "completed")
        self.assertEqual({n: self._generation(n) for n in self._live()}, before)

    async def test_the_stdio_server_lists_the_tool(self):
        try:
            from mcp import ClientSession, StdioServerParameters
            from mcp.client.stdio import stdio_client
        except ImportError:  # pragma: no cover
            self.skipTest("mcp client not installed")
        env = {**os.environ, "PYTHONPATH": os.pathsep.join(
            [_REPO, os.path.join(_REPO, "servers", "dag"), os.environ.get("PYTHONPATH", "")])}
        params = StdioServerParameters(
            command=sys.executable, args=[os.path.join(_REPO, "servers", "dag", "main.py")],
            env=env, cwd=_REPO)
        stderr_path = os.path.join(self.workdir, "stdio-server.stderr")
        with open(stderr_path, "w") as errlog:
            async with stdio_client(params, errlog=errlog) as (read, write):
                async with ClientSession(read, write) as session:
                    await asyncio.wait_for(session.initialize(), 90)
                    listed = await asyncio.wait_for(session.list_tools(), 30)
        self.assertIn("discover_and_dump_all_clusters", {t.name for t in listed.tools})
        # The real server process started under the example credentials:
        # what it wrote to its own stderr — the server log — is read, not
        # discarded, and carries neither of them. (The scan itself is not
        # run in that process: it reads the developer's own ledger config
        # from HOME, not this test's.)
        with open(stderr_path, encoding="utf-8", errors="replace") as fh:
            server_log = fh.read()
        self.assertNotIn(EXAMPLE_SECRET, server_log)
        self.assertNotIn(EXAMPLE_KEY_ID, server_log)


if __name__ == "__main__":
    unittest.main()
