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

"""Unit tests for bare-name guesses and the YAML half of the data scan."""

import os
import tempfile
import unittest

from servers.phases.discovery.discovery_init_1 import datastores, inferred


def _write(root, rel_path, content):
    full = os.path.join(root, rel_path)
    os.makedirs(os.path.dirname(full), exist_ok=True)
    with open(full, "w") as f:
        f.write(content)


def _harvest(files: dict, scope: dict = None) -> dict:
    with tempfile.TemporaryDirectory() as root:
        for rel_path, content in files.items():
            _write(root, rel_path, content)
        inventory = {"data_dependencies": []}
        harvest = datastores.harvest_datastores(inventory, root, scope)
        return {"entries": inventory["data_dependencies"], "notes": harvest.notes,
                "hints": harvest.hints, "workloads": harvest.workloads}


def _by_address(entries):
    return {e["address"]: e for e in entries}


ACME_IAM = '''
resource "aws_iam_role" "orders_irsa" {
  assume_role_policy = jsonencode({
    Statement = [{ Condition = { StringEquals = {
      "oidc.eks.us-east-1.amazonaws.com:sub" = "system:serviceaccount:acme-shop:orders" } } }]
  })
}
resource "aws_iam_role_policy" "orders_s3" {
  role = aws_iam_role.orders_irsa.id
  policy = jsonencode({ Statement = [{
    Resource = ["arn:aws:s3:::acme-invoice-archive", "arn:aws:s3:::acme-invoice-archive/*"] }] })
}
'''
ORDERS_CHART = {
    "charts/orders/Chart.yaml": "apiVersion: v2\nname: orders\nversion: 0.1.0\n",
    "charts/orders/values.yaml": '''
image:
  repository: acme/orders
serviceAccount:
  name: orders
  roleArn: arn:aws:iam::869935070097:role/acme-prod-orders
invoiceBucket: acme-invoice-archive
''',
    "charts/orders/templates/deployment.yaml": '''
apiVersion: apps/v1
kind: Deployment
metadata:
  name: {{ .Chart.Name }}
spec:
  template:
    spec:
      containers:
        - name: orders
          env:
            - name: INVOICE_BUCKET
              value: {{ .Values.invoiceBucket }}
''',
}


class ClassifyTest(unittest.TestCase):
    """The key types the value; a short list of key words is trusted to."""

    def test_keys_that_type_a_value(self):
        cases = [
            ("INVOICE_BUCKET", "acme-invoice-archive", ("s3", "acme-invoice-archive")),
            ("invoiceBucket", "acme-invoice-archive", ("s3", "acme-invoice-archive")),
            ("bucket", "acme.media.assets", ("s3", "acme.media.assets")),
            ("DYNAMODB_TABLE", "carts", ("dynamodb", "carts")),
            ("ddbTableName", "carts-v2", ("dynamodb", "carts-v2")),
            ("SQS_QUEUE", "orders-events", ("sqs", "orders-events")),
            ("sqsQueueName", "orders.fifo", ("sqs", "orders.fifo")),
            ("SNS_TOPIC", "alerts", ("sns", "alerts")),
            ("KINESIS_STREAM", "clicks", ("kinesis", "clicks")),
            ("SECRET_NAME", "prod/db/password", ("secretsmanager", "prod/db/password")),
            ("SSM_PARAM", "/prod/db/host", ("ssm", "/prod/db/host")),
        ]
        for key, value, expected in cases:
            with self.subTest(key=key):
                self.assertEqual(inferred.classify(key, value), expected)

    def test_keys_and_values_that_do_not(self):
        cases = [
            ("DB_TABLE", "users"),                      # a SQL table, not DynamoDB
            ("TABLE_NAME", "users"),
            ("QUEUE_NAME", "celery-default"),           # no sqs in the key
            ("TOPIC", "orders"),                        # Kafka as likely as SNS
            ("STREAM_NAME", "clicks"),
            ("S3_BUCKET_ARN", "arn:aws:s3:::acme-media"),
            ("QUEUE_URL", "https://sqs.us-east-1.amazonaws.com/1/orders"),
            ("BUCKET_PREFIX", "uploads"),
            ("BUCKET_REGION", "us-east-1"),
            ("BUCKET_ENABLED", "true"),
            ("S3_BUCKET_KEY", "exports/2026.csv"),
            ("invoiceBucket", "{{ .Values.bucket }}"),
            ("invoiceBucket", "${var.bucket}"),
            ("invoiceBucket", ""),
            ("invoiceBucket", "none"),
            ("invoiceBucket", "Acme_Bucket"),           # not a bucket name
            ("invoiceBucket", "acme-media/uploads"),    # a path inside one
            ("invoiceBucket", "10.0.0.1"),
            ("invoiceBucket", "the invoice bucket"),
            ("SQS_QUEUE", "orders events"),
        ]
        for key, value in cases:
            with self.subTest(key=key, value=value):
                self.assertIsNone(inferred.classify(key, value))


class TerraformGuessTest(unittest.TestCase):

    def test_a_helm_set_value_becomes_a_guess_with_the_release_as_holder(self):
        out = _harvest({"apps.tf": '''
resource "helm_release" "orders" {
  name      = "orders"
  namespace = "acme-shop"
  chart     = "./charts/orders"
  set {
    name  = "invoiceBucket"
    value = "acme-invoice-archive"
  }
}
'''})
        self.assertEqual(len(out["entries"]), 1)
        entry = out["entries"][0]
        self.assertEqual((entry["service"], entry["identifier"], entry["detection"],
                          entry["disposition"], entry["address"]),
                         ("s3", "acme-invoice-archive", "inferred", "undecided",
                          "s3:acme-invoice-archive"))
        self.assertEqual([(c["workload"], c["kind"], c["detection"], c["evidence"])
                          for c in entry["consumers"]],
                         [("orders", "helm_release", inferred.CONFIG_DETECTION, "apps.tf")])
        self.assertTrue(entry["notes"][0].startswith(inferred.GUESS_NOTE_PREFIX))
        self.assertIn("invoiceBucket in apps.tf", entry["notes"][0])
        self.assertTrue(any("guesses from a configuration key" in n for n in out["notes"]))

    def test_a_config_map_key_becomes_a_guess(self):
        out = _harvest({"apps.tf": '''
resource "kubernetes_config_map" "orders" {
  metadata { name = "orders-config" }
  data = {
    SQS_QUEUE = "orders-events"
    LOG_LEVEL = "info"
  }
}
'''})
        self.assertEqual([(e["service"], e["identifier"]) for e in out["entries"]],
                         [("sqs", "orders-events")])

    def test_a_name_matching_a_declared_entry_is_a_hint_not_a_guess(self):
        out = _harvest({
            "s3.tf": 'resource "aws_s3_bucket" "media" {\n  bucket = "acme-media"\n}\n',
            "apps.tf": '''
resource "helm_release" "web" {
  name  = "web"
  chart = "./charts/web"
  set {
    name  = "mediaBucket"
    value = "acme-media"
  }
}
''',
        })
        self.assertEqual([e["detection"] for e in out["entries"]], ["declared"])
        self.assertEqual(out["entries"][0]["consumers"], [])
        self.assertEqual([(h["address"], h["workload"], h["kind"]) for h in out["hints"]],
                         [("aws_s3_bucket.media", "web", "helm_release")])
        self.assertIn("mediaBucket in apps.tf", out["hints"][0]["reason"])

    def test_two_holders_of_one_name_share_one_guess(self):
        out = _harvest({"apps.tf": '''
resource "helm_release" "orders" {
  name  = "orders"
  chart = "./charts/orders"
  set {
    name  = "invoiceBucket"
    value = "acme-invoice-archive"
  }
}
resource "kubernetes_config_map" "billing" {
  metadata { name = "billing-config" }
  data = { INVOICE_BUCKET = "acme-invoice-archive" }
}
'''})
        self.assertEqual(len(out["entries"]), 1)
        self.assertEqual(sorted(c["workload"] for c in out["entries"][0]["consumers"]),
                         ["billing-config", "orders"])
        self.assertIn("INVOICE_BUCKET in apps.tf", out["entries"][0]["notes"][0])


class YamlScanTest(unittest.TestCase):

    def test_the_acme_chart_values_corroborate_the_arn_entry(self):
        """The acme shape end to end: the bucket comes from the policy ARN,
        the orders chart's values name it under `invoiceBucket`, and that is
        offered as a candidate for the chart rather than duplicated."""
        out = _harvest(dict(ORDERS_CHART, **{"terraform/iam-irsa.tf": ACME_IAM}))
        self.assertEqual(len(out["entries"]), 1)
        entry = out["entries"][0]
        self.assertEqual(entry["detection"], "referenced")
        self.assertEqual([(c["workload"], c["kind"]) for c in entry["consumers"]],
                         [("orders", "service_account")])
        self.assertEqual([(h["workload"], h["kind"], h["source_path"]) for h in out["hints"]],
                         [("orders", inferred.CHART_KIND, "charts/orders")])
        # The chart is in the candidate pool the review ranks.
        self.assertIn(("orders", inferred.CHART_KIND),
                      {(w["workload"], w["kind"]) for w in out["workloads"]})

    def test_chart_values_alone_produce_a_guess_held_by_the_chart(self):
        out = _harvest(ORDERS_CHART)
        self.assertEqual(len(out["entries"]), 1)
        entry = out["entries"][0]
        self.assertEqual(entry["detection"], "inferred")
        self.assertEqual([(c["workload"], c["kind"], c["source_path"], c["evidence"])
                          for c in entry["consumers"]],
                         [("orders", inferred.CHART_KIND, "charts/orders",
                           "charts/orders/values.yaml")])
        # The template full of `{{ }}` was skipped without a note.
        self.assertFalse(any("templates/deployment.yaml" in n for n in out["notes"]))

    def test_a_manifest_env_var_is_held_by_its_deployment(self):
        out = _harvest({"k8s/orders.yaml": '''
apiVersion: apps/v1
kind: Deployment
metadata:
  name: orders
  namespace: acme-shop
spec:
  template:
    spec:
      containers:
        - name: orders
          image: acme/orders:1.0
          env:
            - name: INVOICE_BUCKET
              value: acme-invoice-archive
            - name: LOG_LEVEL
              value: info
'''})
        entry = out["entries"][0]
        self.assertEqual((entry["service"], entry["identifier"]), ("s3", "acme-invoice-archive"))
        self.assertEqual([(c["workload"], c["kind"], c["namespace"]) for c in entry["consumers"]],
                         [("orders", "Deployment", "acme-shop")])

    def test_a_config_map_manifest_with_an_endpoint_is_a_referenced_entry(self):
        out = _harvest({"k8s/config.yaml": '''
apiVersion: v1
kind: ConfigMap
metadata:
  name: orders-config
  namespace: acme-shop
data:
  DB_HOST: orders.c9akciq32xyz.us-east-1.rds.amazonaws.com
  DB_PORT: "5432"
---
apiVersion: v1
kind: Secret
metadata:
  name: orders-secrets
data:
  password: cGFzc3dvcmQ=
stringData:
  QUEUE_URL: https://sqs.us-east-1.amazonaws.com/123456789012/orders-events
'''})
        found = _by_address(out["entries"])
        rds = found["orders.c9akciq32xyz.us-east-1.rds.amazonaws.com"]
        self.assertEqual(rds["detection"], "referenced")
        self.assertEqual([(c["workload"], c["kind"], c["detection"]) for c in rds["consumers"]],
                         [("orders-config", "ConfigMap", inferred.CONFIG_DETECTION)])
        sqs = found["https://sqs.us-east-1.amazonaws.com/123456789012/orders-events"]
        self.assertEqual([c["workload"] for c in sqs["consumers"]], ["orders-secrets"])

    def test_a_yaml_literal_and_a_terraform_literal_fold(self):
        out = _harvest({
            "iam.tf": 'resource "aws_iam_policy" "p" {\n  policy = jsonencode({ Resource = "arn:aws:sqs:us-east-1:123456789012:orders-events" })\n}\n',
            "k8s/config.yaml": '''
apiVersion: v1
kind: ConfigMap
metadata:
  name: orders-config
data:
  QUEUE_URL: https://sqs.us-east-1.amazonaws.com/123456789012/orders-events
''',
        })
        self.assertEqual(len(out["entries"]), 1)
        self.assertCountEqual(out["entries"][0]["evidence"], ["iam.tf", "k8s/config.yaml"])
        self.assertEqual([c["workload"] for c in out["entries"][0]["consumers"]],
                         ["orders-config"])

    def test_yaml_outside_the_confirmed_scope_is_not_read(self):
        out = _harvest(ORDERS_CHART, scope={"included": ["terraform/**"],
                                             "excluded": ["charts/**"]})
        self.assertEqual(out["entries"], [])

    def test_cluster_machinery_and_kustomizations_are_not_holders(self):
        out = _harvest({"k8s/misc.yaml": '''
apiVersion: v1
kind: Namespace
metadata:
  name: acme-shop
  annotations:
    bucket: acme-invoice-archive
---
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources:
  - deployment.yaml
configMapGenerator:
  - name: cfg
    literals:
      - INVOICE_BUCKET=acme-invoice-archive
'''})
        self.assertEqual(out["entries"], [])

    def test_a_declaration_arriving_later_retires_the_guess(self):
        inventory = {"data_dependencies": []}
        guesses, _hints, _notes = inferred.guess_entries(
            [(inferred._holder("orders", "Deployment", None, None, "k8s/o.yaml"),
              "INVOICE_BUCKET", "acme-invoice-archive", "k8s/o.yaml")], [])
        datastores.merge_datastores(inventory, guesses)
        self.assertEqual([e["detection"] for e in inventory["data_dependencies"]],
                         ["inferred"])
        datastores.merge_datastores(inventory, [datastores._entry(
            "s3", "acme-invoice-archive", "s3.tf", {"bucket": "acme-invoice-archive"},
            None, [], address="aws_s3_bucket.archive")])
        self.assertEqual([e["detection"] for e in inventory["data_dependencies"]],
                         ["declared"])



class Cl6RoundOneTest(unittest.TestCase):
    """Regressions from the first adversarial review round."""

    def test_a_secret_manifests_values_are_never_typed_into_a_guess(self):
        out = _harvest({"k8s/secret.yaml": '''
apiVersion: v1
kind: Secret
metadata:
  name: orders-secrets
stringData:
  SQS_QUEUE: orders-events
  SQS_QUEUE_PASSWORD: hunter2-very-secret
''', "k8s/config.yaml": '''
apiVersion: v1
kind: ConfigMap
metadata:
  name: orders-config
data:
  SQS_QUEUE_PASSWORD: hunter2-in-a-configmap
  DB_SECRET: prod/db
  INVOICE_BUCKET: acme-invoice-archive
'''})
        # A Secret's values are secrets by placement; a credential-named key
        # is not a name anywhere. The one honest guess remains.
        self.assertEqual([e["address"] for e in out["entries"]],
                         ["s3:acme-invoice-archive"])
        import json
        self.assertNotIn("hunter2", json.dumps(out["entries"]))

    def test_a_secret_manifests_endpoint_is_still_read(self):
        out = _harvest({"k8s/secret.yaml": '''
apiVersion: v1
kind: Secret
metadata:
  name: orders-db
stringData:
  DATABASE_URL: postgres://app:hunter2@orders.c9akciq32xyz.us-east-1.rds.amazonaws.com:5432/orders
'''})
        (entry,) = out["entries"]
        self.assertEqual((entry["service"], entry["detection"], entry["address"]),
                         ("rds", "referenced", "orders.c9akciq32xyz.us-east-1.rds.amazonaws.com"))
        self.assertEqual([(c["workload"], c["kind"]) for c in entry["consumers"]],
                         [("orders-db", "Secret")])
        import json
        self.assertNotIn("hunter2", json.dumps(out["entries"]))

    def test_a_name_the_merge_would_match_is_a_hint_not_a_dropped_guess(self):
        # A referenced secret under its console form, and a ConfigMap naming
        # the bare name: the merge's tolerance says they are one, so the
        # holder is offered as a hint under the entry rather than becoming a
        # guess the merge then drops.
        out = _harvest({"iam.tf": (
            'resource "aws_iam_policy" "p" {\n  policy = jsonencode({ Resource = '
            '"arn:aws:secretsmanager:us-east-1:111111111111:secret:prod/db-AbC1dE" })\n}\n'),
            "k8s/config.yaml": '''
apiVersion: v1
kind: ConfigMap
metadata:
  name: orders-config
data:
  SECRET_NAME: prod/db
  SSM_PARAMETER_NAME: /app/db-password
''', "iam2.tf": (
            'resource "aws_iam_policy" "q" {\n  policy = jsonencode({ Resource = '
            '"arn:aws:ssm:us-east-1:111111111111:parameter/app/db-password" })\n}\n')})
        self.assertEqual(sorted(e["detection"] for e in out["entries"]),
                         ["referenced", "referenced"])
        self.assertEqual(sorted((h["identifier"], h["workload"]) for h in out["hints"]),
                         [("/app/db-password", "orders-config"), ("prod/db-AbC1dE", "orders-config")])



class Cl6RoundTwoTest(unittest.TestCase):
    """Regressions from the second adversarial review round."""

    CHART = {"charts/orders/Chart.yaml": "apiVersion: v2\nname: orders\n"}

    def test_a_charts_env_list_is_typed_like_its_map_values(self):
        out = _harvest(dict(self.CHART, **{"charts/orders/values.yaml": '''
env:
  - name: INVOICE_BUCKET
    value: acme-invoice-archive
  - name: SQS_QUEUE
    value: orders-events
ingress:
  tls:
    - secretName: orders-tls
existingSecret: orders-db-creds
'''}))
        self.assertEqual(sorted(e["address"] for e in out["entries"]),
                         ["s3:acme-invoice-archive", "sqs:orders-events"])

    def test_credential_words_anywhere_in_the_key_are_not_names(self):
        for key in ("BUCKET_PASSPHRASE", "SQS_PASSPHRASE", "DB_SECRET", "s3BucketPassword",
                    "secretName", "existingSecretName", "secretKeyRef"):
            with self.subTest(key=key):
                self.assertIsNone(inferred.classify(key, "correct-horse-battery"))
        self.assertEqual(inferred.classify("SECRET_NAME", "prod/db"), ("secretsmanager", "prod/db"))
        self.assertEqual(inferred.classify("secret_id", "prod/db"), ("secretsmanager", "prod/db"))

    def test_a_secret_in_the_name_value_shape_is_still_not_typed(self):
        out = _harvest({"k8s/secret.yaml": '''
apiVersion: v1
kind: Secret
metadata:
  name: orders
stringData:
  name: INVOICE_BUCKET
  value: acme-invoice-archive
'''})
        self.assertEqual(out["entries"], [])



class Cl6RoundThreeTest(unittest.TestCase):
    """Regressions from the third adversarial review round."""

    def test_credential_shaped_keys_and_values_never_type(self):
        for key in ("SQS_ACCESS_KEY_ID", "BUCKET_PASSWORD2", "BUCKETPASSWORD", "SQS_ACCESSKEYID",
                    "BUCKET_ACCESS_KEY_ID", "DB_PW", "S3_CREDS_BUCKET", "bucketPassPhrase",
                    "passwd1_bucket"):
            with self.subTest(key=key):
                self.assertIsNone(inferred.classify(key, "hunter2secret"))
        # An access key id is refused whatever the key is called.
        self.assertIsNone(inferred.classify("SQS_QUEUE", "AKIAIOSFODNN7EXAMPLE"))
        # Names still type.
        self.assertEqual(inferred.classify("INVOICE_BUCKET", "acme-invoice-archive"),
                         ("s3", "acme-invoice-archive"))
        self.assertEqual(inferred.classify("secretsmanager_secret_name", "prod/db"),
                         ("secretsmanager", "prod/db"))

    def test_a_values_file_outside_a_chart_is_named_in_a_note(self):
        out = _harvest({"envs/prod/values/orders-prod.yaml": '''
db:
  host: orders.c9akprod000a.us-east-1.rds.amazonaws.com
invoiceBucket: acme-invoice-archive
''', "charts/orders/Chart.yaml": "apiVersion: v2\nname: orders\n",
            "charts/orders/ci/prod-values.yaml": "invoiceBucket: acme-invoice-archive\n"})
        # Round 11: a values file INSIDE the chart (`ci/`) is the chart's and
        # is read; only the one a release names from outside is noted.
        self.assertEqual([(e["address"], e["consumers"][0]["kind"]) for e in out["entries"]],
                         [("s3:acme-invoice-archive", inferred.CHART_KIND)])
        note = next(n for n in out["notes"] if "neither manifests nor under a chart" in n)
        self.assertIn("envs/prod/values/orders-prod.yaml", note)
        self.assertNotIn("charts/orders/ci/prod-values.yaml", note)
        self.assertIn("add_data_dependency", note)



class Cl6RoundFourTest(unittest.TestCase):
    """Regressions from the fourth adversarial review round."""

    def test_the_yaml_side_notes_what_it_could_not_record(self):
        out = _harvest({"k8s/orders.yaml": '''
apiVersion: apps/v1
kind: Deployment
metadata:
  name: orders
spec:
  template:
    spec:
      containers:
        - name: orders
          env:
            - name: DATABASE_URL
              value: postgres://app:${DB_PASSWORD}@orders.cxyzabcd1234.us-east-1.rds.amazonaws.com/orders
            - name: QUEUE_URL
              value: https://sqs.${REGION}.amazonaws.com/123456789012/orders
'''})
        (entry,) = out["entries"]
        self.assertEqual(entry["address"], "orders.cxyzabcd1234.us-east-1.rds.amazonaws.com")
        self.assertEqual([c["workload"] for c in entry["consumers"]], ["orders"])
        self.assertTrue(any("chart values or manifests build the resource name" in n
                            and "sqs.${REGION}" in n for n in out["notes"]), out["notes"])

    def test_tooling_yaml_is_skipped_and_refused_yaml_is_named(self):
        out = _harvest({".github/workflows/ci.yml": "on: push\njobs: {}\n",
                        "docker-compose.yml": "services: {}\n",
                        ".pre-commit-config.yaml": "repos: []\n",
                        "envs/prod/values.yaml": "base: &b\n  a: 1\nx: *b\n"})
        values_note = next((n for n in out["notes"]
                            if "neither manifests nor a chart's own values" in n), "")
        self.assertNotIn(".github", values_note)
        self.assertNotIn("docker-compose", values_note)
        self.assertNotIn("pre-commit", values_note)
        self.assertTrue(any("could not be parsed" in n and "envs/prod/values.yaml" in n
                            for n in out["notes"]), out["notes"])

    def test_more_credential_shapes_never_type(self):
        for key, value in (("SQS_QUEUE_AUTH_HEADER", "Bearer_xyz"), ("BUCKET_HMAC", "abc"),
                           ("SNS_TOPIC_SIGNATURE", "abc"), ("SECRETS_BUCKET", "acme-secrets"),
                           ("SQS_QUEUE", "aGVsbG8gd29ybGQgdGhpcyBpcyBiYXNlNjQ1Njc4OTA1NjcxMjM0")):
            with self.subTest(key=key):
                self.assertIsNone(inferred.classify(key, value))
        self.assertEqual(inferred.classify("PASSWORD_RESET_QUEUE_NAME", "resets"), None)



class Cl6RoundFiveTest(unittest.TestCase):
    """Regressions from the fifth adversarial review round."""

    def test_names_that_merely_contain_a_credential_word_still_type(self):
        for key, value in (("AUTHOR_BUCKET", "acme-authors"), ("SIGNATURE_BUCKET", "acme-sigs"),
                           ("OAUTH_STATE_BUCKET", "acme-oauth-state"),
                           ("DYNAMO_TABLE", "OrdersEventStore2024ProductionArchiveTable")):
            with self.subTest(key=key):
                self.assertIsNotNone(inferred.classify(key, value))
        self.assertIsNone(inferred.classify("BUCKETING_STRATEGY", "round-robin"))
        self.assertIsNone(inferred.classify("AUTH_BUCKET", "x"))
        self.assertIsNone(inferred.classify("SQS_QUEUE", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"))

    def test_a_dot_directory_that_is_not_ci_is_read(self):
        out = _harvest({".helm/Chart.yaml": "apiVersion: v2\nname: orders\n",
                        ".helm/values.yaml": "invoiceBucket: acme-invoice-archive\n",
                        ".github/workflows/ci.yml": "on: push\nenv:\n  INVOICE_BUCKET: ci-bucket\n"})
        self.assertEqual([e["address"] for e in out["entries"]], ["s3:acme-invoice-archive"])



class Cl6RoundSixTest(unittest.TestCase):
    """Regressions from the sixth adversarial review round."""

    def test_camelcase_names_with_acronyms_type_and_base64_does_not(self):
        for name in ("OrdersAPIEventsProductionArchiveTable", "CustomerPIIRecordsProductionArchive2024",
                     "AWSOrdersEventsProductionArchiveStream",
                     "CustomerOrdersTableV2ProductionUSEast1Archive"):
            with self.subTest(name=name):
                self.assertEqual(inferred.classify("DYNAMO_TABLE", name), ("dynamodb", name))
        for key in ("aGVsbG8gd29ybGQgdGhpcyBpcyBiYXNlNjQ1Njc4OTA1NjcxMjM0",
                    "wJalrXUtnFEMIK7MDENGbPxRfiCYEXAMPLEKEY",
                    "dGhpcyBpcyBhIHNlY3JldCBrZXkgZm9yIHRlc3Rpbmc",
                    "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9abcdefgh"):
            with self.subTest(key=key):
                self.assertIsNone(inferred.classify("DYNAMO_TABLE", key))



class Cl6RoundEightTest(unittest.TestCase):
    """Regressions from the eighth adversarial review round."""

    def test_bucket_is_a_word_in_every_key_spelling(self):
        for key in ("bucketName", "s3BucketName", "BucketName", "bucketNames", "invoiceBucket",
                    "bucket_name", "BUCKET_NAME", "s3.bucket"):
            with self.subTest(key=key):
                self.assertEqual(inferred.classify(key, "acme-archive"), ("s3", "acme-archive"))
        for key in ("BUCKETING_STRATEGY", "bucketing", "Bucketing"):
            with self.subTest(key=key):
                self.assertIsNone(inferred.classify(key, "round-robin"))

    def test_a_dotted_kubernetes_secret_key_is_not_a_secrets_manager_guess(self):
        self.assertIsNone(inferred.classify("ingress.tls.secretName", "orders-tls"))
        self.assertIsNone(inferred.classify("auth.existingSecret", "orders-creds"))

    def test_a_set_sensitive_value_is_never_typed(self):
        out = _harvest({"apps.tf": '''
resource "helm_release" "orders" {
  name  = "orders"
  chart = "./charts/orders"
  set {
    name  = "invoiceBucket"
    value = "acme-invoice-archive"
  }
  set_sensitive {
    name  = "s3.bucket"
    value = "sensitive-bucket-name"
  }
}
'''})
        self.assertEqual([e["address"] for e in out["entries"]], ["s3:acme-invoice-archive"])



class Cl6RoundNineTest(unittest.TestCase):
    """Regressions from the ninth adversarial review round."""

    def test_path_shaped_secret_and_parameter_names_type(self):
        self.assertEqual(inferred.classify("DB_SECRET_NAME", "prod/orders/database/credentials/primary"),
                         ("secretsmanager", "prod/orders/database/credentials/primary"))
        self.assertEqual(inferred.classify("SSM_PARAM_NAME", "/prod/orders/database/connection/host"),
                         ("ssm", "/prod/orders/database/connection/host"))
        for key, value in (("SQS_QUEUE_EXPORT", "orders-export"),
                           ("DATA_BUCKET_ACCOUNT", "acme-accounts"), ("bucket_turkey", "acme-turkey")):
            with self.subTest(key=key):
                self.assertIsNotNone(inferred.classify(key, value))
        self.assertIsNone(inferred.classify("bucketUrl", "x"))
        self.assertIsNone(inferred.classify("BUCKET_ARN", "x"))

    def test_sensitive_sets_in_every_shape_are_never_typed(self):
        out = _harvest({"apps.tf": '''
resource "helm_release" "orders" {
  name  = "orders"
  chart = "./charts/orders"
  set_sensitive = [{
    name  = "kinesis.streamName"
    value = "listformsecret"
  }]
  set_wo {
    name  = "sns.topicName"
    value = "writeonlysecret"
  }
  set_sensitive {
    name  = "sqs.queueName"
    value = "brace{inside}secret"
  }
  set {
    name  = "invoiceBucket"
    value = "acme-invoice-archive"
  }
}
'''})
        self.assertEqual([e["address"] for e in out["entries"]], ["s3:acme-invoice-archive"])

    def test_a_charts_holder_is_unified_onto_the_release_that_deploys_it(self):
        out = _harvest({
            "main.tf": '''
resource "aws_s3_bucket" "invoices" {
  bucket = "acme-invoices"
}
resource "helm_release" "orders" {
  name      = "orders"
  namespace = "shop"
  chart     = "./charts/orders"
}
''',
            "charts/orders/Chart.yaml": "apiVersion: v2\nname: orders\n",
            "charts/orders/values.yaml": "s3:\n  bucketName: acme-invoices\nqueueUrl: https://sqs.us-east-1.amazonaws.com/123456789012/orders\n"})
        pool = [(w["workload"], w["kind"]) for w in out["workloads"] if w["workload"] == "orders"]
        self.assertEqual(pool, [("orders", "helm_release")])
        self.assertEqual([(h["workload"], h["kind"]) for h in out["hints"]],
                         [("orders", "helm_release")])
        queue = next(e for e in out["entries"] if e["service"] == "sqs")
        self.assertEqual([(c["workload"], c["kind"], c["namespace"]) for c in queue["consumers"]],
                         [("orders", "helm_release", "shop")])



class Cl6RoundTenTest(unittest.TestCase):
    """Regressions from the tenth adversarial review round."""

    CHART = {"charts/orders/Chart.yaml": "apiVersion: v2\nname: orders\n",
             "charts/orders/values.yaml": ("invoiceBucket: acme-invoice-archive\n"
                                           "dbHost: orders.c9akprod0001.us-east-1.rds.amazonaws.com\n")}

    def test_two_releases_of_one_chart_leave_the_chart_as_the_holder(self):
        out = _harvest(dict(self.CHART, **{"main.tf": '''
resource "helm_release" "orders_dev" {
  name      = "orders-dev"
  namespace = "dev"
  chart     = "./charts/orders"
}
resource "helm_release" "orders_prod" {
  name      = "orders-prod"
  namespace = "prod"
  chart     = "./charts/orders"
}
'''}))
        for entry in out["entries"]:
            self.assertEqual([(c["workload"], c["kind"]) for c in entry["consumers"]],
                             [("orders", inferred.CHART_KIND)], entry["address"])
        self.assertTrue(any("deployed by more than one helm_release" in n
                            and "orders-dev, orders-prod" in n for n in out["notes"]), out["notes"])
        pool = sorted((w["workload"], w["kind"]) for w in out["workloads"])
        self.assertIn(("orders", inferred.CHART_KIND), pool)
        self.assertIn(("orders-dev", "helm_release"), pool)
        self.assertIn(("orders-prod", "helm_release"), pool)

    def test_a_template_that_parses_is_still_not_a_manifest(self):
        out = _harvest({"charts/orders/Chart.yaml": "apiVersion: v2\nname: orders\n",
                        "charts/orders/templates/configmap.yaml": '''
kind: ConfigMap
apiVersion: v1
metadata:
  name: "{{ .Release.Name }}-config"
  namespace: "{{ .Release.Namespace }}"
data:
  INVOICE_BUCKET: acme-invoice-archive
''', "k8s/rendered.yaml": '''
kind: ConfigMap
apiVersion: v1
metadata:
  name: "{{ .Values.name }}-config"
data:
  INVOICE_BUCKET: acme-invoice-archive
'''})
        self.assertEqual(out["entries"], [])
        self.assertFalse(any("{{" in (w.get("workload") or "") for w in out["workloads"]))



class Cl6RoundElevenTest(unittest.TestCase):
    """Regressions from the eleventh adversarial review round."""

    def test_a_values_directory_inside_the_chart_is_the_charts(self):
        out = _harvest({"charts/orders/Chart.yaml": "apiVersion: v2\nname: orders\n",
                        "charts/orders/values/prod.yaml": "invoiceBucket: acme-invoice-archive\n",
                        "charts/orders/ci/test-values.yaml": "sqsQueue: orders-events\n"})
        by_address = _by_address(out["entries"])
        self.assertEqual(sorted(by_address), ["s3:acme-invoice-archive", "sqs:orders-events"])
        for entry in by_address.values():
            self.assertEqual([(c["workload"], c["kind"], c["source_path"]) for c in entry["consumers"]],
                             [("orders", inferred.CHART_KIND, "charts/orders")])
        self.assertFalse(any("neither manifests nor" in n for n in out["notes"]), out["notes"])


class Cl6RoundTwelveTest(unittest.TestCase):
    """Regressions from the twelfth adversarial review round."""

    def test_a_kind_document_without_a_name_under_a_chart_is_not_the_charts_values(self):
        out = _harvest({"deploy/Chart.yaml": "apiVersion: v2\nname: orders\n",
                        "deploy/k8s/list.yaml": '''
apiVersion: v1
kind: List
items:
  - apiVersion: apps/v1
    kind: Deployment
    metadata:
      name: listed
    spec:
      template:
        spec:
          containers:
            - name: app
              env:
                - name: INVOICE_BUCKET
                  value: acme-listed
'''})
        self.assertEqual(out["entries"], [])

    def test_an_in_module_chart_path_unifies_onto_the_release(self):
        out = _harvest({"main.tf": '''
resource "helm_release" "orders" {
  name  = "orders"
  chart = "${path.module}/charts/orders"
}
''', "charts/orders/Chart.yaml": "apiVersion: v2\nname: orders\n",
            "charts/orders/values.yaml": "invoiceBucket: acme-invoice-archive\n"})
        (guess,) = out["entries"]
        self.assertEqual([(c["workload"], c["kind"]) for c in guess["consumers"]],
                         [("orders", "helm_release")])
        self.assertEqual([(w["workload"], w["kind"]) for w in out["workloads"]
                          if w["workload"] == "orders"], [("orders", "helm_release")])



class Cl6RoundThirteenTest(unittest.TestCase):
    """Regressions from the thirteenth adversarial review round."""

    def test_path_root_is_not_resolved_against_the_repository_root(self):
        out = _harvest({"envs/prod/main.tf": '''
resource "helm_release" "orders" {
  name  = "orders"
  chart = "${path.root}/charts/orders"
}
''', "envs/prod/charts/orders/Chart.yaml": "apiVersion: v2\nname: orders\n",
            "envs/prod/charts/orders/values.yaml": "invoiceBucket: acme-invoice-archive\n",
            "charts/orders/Chart.yaml": "apiVersion: v2\nname: orders-legacy\n"})
        release = next(w for w in out["workloads"] if w["kind"] == "helm_release")
        # Honestly unknown, never the repository root's `charts/orders`.
        self.assertIsNone(release.get("source_path"))

    def test_the_s3_xml_namespace_and_a_flow_mapping_brace(self):
        self.assertEqual(datastores.find_endpoints('xmlns="http://s3.amazonaws.com/doc/2006-03-01/"'), [])
        (found,) = datastores.find_endpoints("{host: orders.c9akciq32xyz.us-east-1.rds.amazonaws.com}")
        self.assertEqual((found.kind, found.handle), ("recorded", "orders.c9akciq32xyz.us-east-1.rds.amazonaws.com"))



class Cl6RoundFourteenTest(unittest.TestCase):
    """Regressions from the fourteenth adversarial review round."""

    def test_knobs_and_numbers_never_become_guesses(self):
        for key, value in (("SQS_MAX_MESSAGES", "10"), ("SQS_WAIT_TIME_SECONDS", "20"),
                           ("S3_BUCKET_ACL", "private"), ("KINESIS_SHARD_ITERATOR_TYPE", "LATEST"),
                           ("DYNAMODB_BILLING_MODE", "PAY_PER_REQUEST"), ("SNS_MAX_RETRIES", "3"),
                           ("sqsMaxMessages", "10"), ("DYNAMO_TABLE", "2024")):
            with self.subTest(key=key):
                self.assertIsNone(inferred.classify(key, value))
        self.assertEqual(inferred.classify("SQS_QUEUE", "orders-2024"), ("sqs", "orders-2024"))

    def test_service_principals_and_wildcard_hosts_are_ignored_without_a_note(self):
        out = _harvest({"iam.tf": '''
resource "aws_iam_role" "monitoring" {
  assume_role_policy = jsonencode({ Statement = [{ Principal = { Service = "monitoring.rds.amazonaws.com" } }] })
}
resource "kubernetes_config_map" "csp" {
  metadata { name = "csp" }
  data = { CSP = "img-src *.s3.amazonaws.com" }
}
'''})
        self.assertEqual(out["entries"], [])
        self.assertFalse(any("could not be read" in n for n in out["notes"]), out["notes"])
        # A real bucket host beside them is still recorded.
        (found,) = datastores.find_endpoints('"acme-media.s3.amazonaws.com"')
        self.assertEqual(found.handle, "s3://acme-media")


if __name__ == "__main__":
    unittest.main()
