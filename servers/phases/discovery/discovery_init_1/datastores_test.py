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

"""Unit tests for the pure data-dependency extraction logic. No GCS, no LLM."""

import copy
import os
import tempfile
import unittest

from servers.phases.discovery.discovery_init_1 import datastores


def _write(root, rel_path, content):
    full = os.path.join(root, rel_path)
    os.makedirs(os.path.dirname(full), exist_ok=True)
    with open(full, "w") as f:
        f.write(content)


def _harvest(files: dict, scope: dict = None, overrides: dict = None) -> dict:
    """Runs a full harvest over an in-memory set of {rel_path: content}."""
    with tempfile.TemporaryDirectory() as root:
        for rel_path, content in files.items():
            _write(root, rel_path, content)
        inventory = {"data_dependencies": []}
        harvest = datastores.harvest_datastores(
            inventory, root, scope, overrides)
        return {"entries": inventory["data_dependencies"],
                "notes": harvest.notes, "workloads": harvest.workloads,
                "truncated": harvest.truncated, "scanned": harvest.scanned}


def _noted_duplicate(out):
    """The referenced entry the scan flagged as a possible duplicate of a
    resource counted through its primary, asserting the note is there."""
    flagged = [e for e in out["entries"]
               if any(n.startswith(datastores.DUPLICATE_NOTE_PREFIX)
                      for n in e.get("notes") or [])]
    assert len(flagged) == 1, [e["identifier"] for e in out["entries"]]
    return flagged[0]


def _by_id(entries):
    return {e["identifier"]: e for e in entries}


class ServiceMappingTest(unittest.TestCase):
    """The two declaration forms resolve to the same normalized services."""

    def test_every_raw_resource_type_maps(self):
        for resource_type, expected in datastores.RESOURCE_SERVICES.items():
            with self.subTest(resource_type=resource_type):
                self.assertEqual(
                    datastores.service_for_resource_type(resource_type), expected)

    def test_unknown_resource_type_is_not_a_datastore(self):
        for resource_type in ("aws_instance", "aws_iam_role", "aws_eks_cluster"):
            self.assertIsNone(datastores.service_for_resource_type(resource_type))

    def test_module_sources_from_the_sample_estates(self):
        cases = [
            # The exact sources found in retail-store-sample-app and
            # eks-workshop-v2. cloudposse is deliberately present: the
            # publisher namespace is open, so matching must not assume
            # terraform-aws-modules.
            ("terraform-aws-modules/rds/aws", "rds"),
            ("terraform-aws-modules/rds-aurora/aws", "aurora"),
            ("terraform-aws-modules/dynamodb-table/aws", "dynamodb"),
            ("cloudposse/elasticache-redis/aws", "elasticache"),
            ("terraform-aws-modules/fsx/aws//modules/openzfs", "fsx"),
        ]
        for source, expected in cases:
            with self.subTest(source=source):
                self.assertEqual(datastores.service_for_module_source(source), expected)

    def test_aurora_wins_over_rds(self):
        """Pattern order is load-bearing — 'rds-aurora' contains 'rds'."""
        self.assertEqual(
            datastores.service_for_module_source("terraform-aws-modules/rds-aurora/aws"),
            "aurora")

    def test_non_registry_module_sources(self):
        for source in ("git::https://example.com/modules//rds?ref=v1",
                       "../../modules/rds", "./modules/dynamodb"):
            with self.subTest(source=source):
                self.assertIsNotNone(datastores.service_for_module_source(source))

    def test_a_service_name_inside_an_ordinary_word_does_not_match(self):
        """`rds` is inside `records`, `dashboards`, `standards`. A route53
        records module was being harvested as an unsized Postgres and priced
        at two days."""
        for source in ("terraform-aws-modules/route53/aws//modules/records",
                       "org/dashboards/aws", "org/standards/aws",
                       "org/leaderboards/aws"):
            with self.subTest(source=source):
                self.assertIsNone(datastores.service_for_module_source(source))

    def test_the_subpath_is_preferred_over_the_repository(self):
        """An SQS module in a repository called `rds-modules` is an SQS
        module."""
        self.assertEqual(
            datastores.service_for_module_source("git::https://x.com/rds-modules.git//sqs"),
            "sqs")

    def test_the_repository_is_still_a_fallback_and_can_mislabel(self):
        """Pinning the trade-off rather than a guarantee the code does not
        give. `terraform-aws-modules/rds/aws//modules/db_instance` has no
        service word in its subpath, so the fallback has to exist — and the
        same fallback reads a vpc-endpoint submodule of an rds-named repo as
        RDS. Under-detection is the worse error, so this is deliberate."""
        self.assertEqual(
            datastores.service_for_module_source(
                "terraform-aws-modules/rds/aws//modules/db_instance"), "rds")
        self.assertEqual(
            datastores.service_for_module_source(
                "git::https://x.com/rds-modules.git//vpc-endpoint"), "rds")

    def test_only_the_module_name_decides_the_service(self):
        """A `<getter>::` says how to fetch it, a host says where it is kept, a
        `?ref=` says which version, and an object-store URL puts the bucket in
        the path. None of them describe what the module is."""
        for source in ("s3::https://acme-tf.s3.amazonaws.com/vpc-v3.zip",
                       "gcs::https://www.googleapis.com/storage/v1/acme/eks.zip",
                       "s3::https://s3-eu-west-1.amazonaws.com/rds-modules/vpc.zip",
                       "git::https://github.com/acme/platform.git//vpc?ref=feature/add-s3-backup",
                       "git::https://github.com/acme/vpc.git?ref=v1.2-rds"):
            with self.subTest(source=source):
                self.assertIsNone(datastores.service_for_module_source(source))
        # ...and a bucket module served the same way is still a bucket module.
        self.assertEqual(datastores.service_for_module_source(
            "s3::https://acme-tf.s3.amazonaws.com/s3-bucket.zip"), "s3")

    def test_a_url_host_does_not_decide_whether_a_bucket_is_platform_machinery(self):
        """Judging the raw string means an org hosting modules on Artifactory
        has every bucket module downgraded — the `artifact` hint firing on the
        hostname — which silently un-gates a bucket the declaration names as
        customer data, the one direction the hint list must never fail in."""
        for source in ("git::https://artifactory.acme.com/tf/s3-bucket.git",
                       "s3::https://acme-tf-artifacts.s3.amazonaws.com/s3-bucket.zip",
                       "git::https://github.com/acme/s3-bucket.git"):
            with self.subTest(source=source):
                out = _harvest({"m.tf": f'''
module "uploads" {{
  source = "{source}"
  bucket = "acme-customer-uploads"
}}
'''})
                self.assertEqual(out["entries"][0]["disposition"], "migrate")

    def test_a_scheme_less_host_still_downgrades_the_bucket(self):
        """Pinning the gap rather than a guarantee the code does not give. The
        host is only stripped when the source carries a `://`, so a private
        registry address and an scp-style git source keep theirs and read as
        platform machinery. The entry is still recorded, carrying the note that
        says why, and the obvious fix — drop the leading dotted segment —
        mis-fires on `./modules/…`. Change this test when that is addressed."""
        for source in ("artifactory.acme.com/acme/s3-bucket/aws",
                       "git::git@artifactory.acme.com:acme/s3-data.git"):
            with self.subTest(source=source):
                out = _harvest({"m.tf": f'''
module "uploads" {{
  source = "{source}"
  bucket = "acme-customer-uploads"
}}
'''})
                entry = out["entries"][0]
                self.assertEqual(entry["disposition"], "undecided")
                self.assertTrue(any("platform machinery" in n
                                    for n in entry["notes"]))

    def test_unrelated_modules_are_not_datastores(self):
        for source in ("terraform-aws-modules/vpc/aws",
                       "terraform-aws-modules/eks/aws",
                       "terraform-aws-modules/iam/aws//modules/iam-assumable-role"):
            with self.subTest(source=source):
                self.assertIsNone(datastores.service_for_module_source(source))

    def test_platform_bucket_modules_are_matched_then_downgraded(self):
        """Matching and judging are separate on purpose. Refusing to match here
        would drop the module with no entry and no note — and the hint is fuzzy
        enough to fire on an org whose shared-module repo is called
        `pipeline-modules`, which would lose every S3 module in the estate."""
        for source in ("org/s3-backend/aws", "org/tfstate-s3/aws",
                       "terraform-aws-modules/s3-bucket/aws"):
            with self.subTest(source=source):
                self.assertEqual(datastores.service_for_module_source(source), "s3")

    def test_a_module_declared_platform_bucket_is_recorded_not_dropped(self):
        out = _harvest({"m.tf": '''
module "state" {
  source = "org/s3-backend/aws"
  bucket = "acme-tfstate"
}
'''})
        self.assertEqual(len(out["entries"]), 1)
        self.assertEqual(out["entries"][0]["disposition"], "undecided")

    def test_a_module_repo_name_does_not_condemn_the_bucket(self):
        """The hint fires on the whole source string, so an org's module repo
        naming must not decide what an application's bucket is."""
        out = _harvest({"m.tf": '''
module "uploads" {
  source = "git::https://github.com/acme/pipeline-modules.git//s3-bucket"
  bucket = "acme-customer-uploads"
}
'''})
        self.assertEqual(len(out["entries"]), 1)
        self.assertEqual(out["entries"][0]["disposition"], "migrate")


class DeclarationParsingTest(unittest.TestCase):

    def test_raw_resource_with_literal_properties(self):
        out = _harvest({"rds.tf": '''
resource "aws_db_instance" "catalog" {
  identifier        = "acme-catalog"
  engine            = "postgres"
  engine_version    = "15.4"
  allocated_storage = 100
  storage_type      = "gp3"
  multi_az          = true
}
'''})
        entry = _by_id(out["entries"])["acme-catalog"]
        self.assertEqual(entry["service"], "rds")
        self.assertEqual(entry["engine"], "postgres")
        self.assertEqual(entry["engine_version"], "15.4")
        self.assertEqual(entry["allocated_storage"], 100)
        self.assertEqual(entry["storage_type"], "gp3")
        self.assertTrue(entry["multi_az"])
        self.assertEqual(entry["detection"], "declared")
        self.assertTrue(entry["declared_in_repo"])
        self.assertEqual(entry["disposition"], "migrate")
        self.assertEqual(entry["evidence"], ["rds.tf"])
        self.assertIsNone(entry["module_source"])

    def test_module_declaration_carries_source_and_args(self):
        """The eks-workshop-v2 shape, verbatim."""
        out = _harvest({"main.tf": '''
module "rds" {
  source  = "terraform-aws-modules/rds/aws"
  version = "6.13.1"

  identifier        = "acme-catalog"
  engine            = "mysql"
  engine_version    = var.rds_engine_version
  instance_class    = "db.t4g.micro"
  allocated_storage = 20
}
'''})
        entry = _by_id(out["entries"])["acme-catalog"]
        self.assertEqual(entry["service"], "rds")
        self.assertEqual(entry["engine"], "mysql")
        self.assertEqual(entry["allocated_storage"], 20)
        self.assertEqual(entry["module_source"], "terraform-aws-modules/rds/aws")
        # engine_version is an expression, so it must be null rather than the
        # literal string "var.rds_engine_version".
        self.assertIsNone(entry["engine_version"])

    def test_expressions_never_become_values(self):
        out = _harvest({"m.tf": '''
resource "aws_db_instance" "d" {
  identifier        = "${var.prefix}-db"
  allocated_storage = var.size
  engine            = local.engine
}
'''})
        entry = out["entries"][0]
        # An interpolated identifier is not a name; fall back to the Terraform
        # address, flag it, and say why.
        self.assertEqual(entry["identifier"], "aws_db_instance.d")
        self.assertTrue(entry["identifier_is_fallback"])
        self.assertIsNone(entry["allocated_storage"])
        self.assertIsNone(entry["engine"])
        self.assertTrue(any("Terraform address" in n for n in entry["notes"]))
        self.assertTrue(any("allocated_storage" in n for n in entry["notes"]))

    def test_module_fallback_uses_the_module_address(self):
        """The common real-world case: modules name resources by expression."""
        out = _harvest({"m.tf": '''
module "catalog_rds" {
  source = "terraform-aws-modules/rds-aurora/aws"
  name   = "${var.environment}-catalog"
}
'''})
        entry = out["entries"][0]
        self.assertEqual(entry["identifier"], "module.catalog_rds")
        self.assertTrue(entry["identifier_is_fallback"])

    def test_a_literal_name_is_not_flagged_as_fallback(self):
        out = _harvest({"m.tf": 'resource "aws_sqs_queue" "q" {\n  name = "orders"\n}\n'})
        self.assertEqual(out["entries"][0]["identifier"], "orders")
        self.assertFalse(out["entries"][0]["identifier_is_fallback"])

    def test_nested_blocks_do_not_leak_into_identity(self):
        out = _harvest({"m.tf": '''
resource "aws_dynamodb_table" "carts" {
  name = "carts"
  tags = {
    name = "not-the-table-name"
  }
  point_in_time_recovery {
    enabled = true
  }
}
'''})
        self.assertEqual(out["entries"][0]["identifier"], "carts")

    def test_braces_inside_a_policy_heredoc_do_not_end_the_block(self):
        out = _harvest({"m.tf": '''
resource "aws_s3_bucket" "assets" {
  bucket = "acme-assets"
  policy = <<EOF
{
  "Version": "2012-10-17",
  "Statement": [{"Effect": "Allow", "Action": "s3:GetObject"}]
}
EOF
}

resource "aws_sqs_queue" "orders" {
  name = "orders"
}
'''})
        found = _by_id(out["entries"])
        self.assertIn("acme-assets", found)
        # The second resource is only reachable if the first block closed in
        # the right place.
        self.assertIn("orders", found)

    def test_comments_are_ignored(self):
        out = _harvest({"m.tf": '''
# resource "aws_db_instance" "commented_out" {
resource "aws_kinesis_stream" "events" {
  name = "events"  # the live one
}
'''})
        found = _by_id(out["entries"])
        self.assertIn("events", found)
        self.assertNotIn("commented_out", found)

    def test_vendored_directories_are_pruned(self):
        out = _harvest({
            "main.tf": 'resource "aws_sns_topic" "mine" {\n  name = "mine"\n}\n',
            ".terraform/modules/dep/main.tf":
                'resource "aws_db_instance" "vendored" {\n  identifier = "vendored"\n}\n',
        })
        found = _by_id(out["entries"])
        self.assertIn("mine", found)
        self.assertNotIn("vendored", found)


class CommentAndSeparatorTest(unittest.TestCase):
    """Everything here goes through `harvest_datastores`, not the matching
    helpers. The earlier module-source tests called `service_for_module_source`
    directly and passed while the real path silently dropped every source
    containing `//`, so these exercise the whole chain on purpose."""

    def test_registry_submodule_source_survives(self):
        """`//` is Terraform's submodule separator, not a comment. Stripping
        there blanks the source and drops the resource with no note."""
        out = _harvest({"m.tf": '''
module "fs" {
  source = "terraform-aws-modules/fsx/aws//modules/openzfs"
  name   = "shared-data"
}
'''})
        self.assertEqual([(e["service"], e["identifier"]) for e in out["entries"]],
                         [("fsx", "shared-data")])

    def test_git_source_with_a_subpath_survives(self):
        out = _harvest({"m.tf": '''
module "db" {
  source     = "git::https://example.com/modules//rds?ref=v1.2.0"
  identifier = "acme-catalog"
}
'''})
        self.assertEqual([e["service"] for e in out["entries"]], ["rds"])

    def test_a_real_trailing_comment_is_still_stripped(self):
        out = _harvest({"m.tf": '''
resource "aws_sqs_queue" "q" {
  name = "orders" // the live one
}
resource "aws_sns_topic" "t" {
  name = "events" # also live
}
'''})
        self.assertCountEqual([e["identifier"] for e in out["entries"]],
                              ["orders", "events"])

    def test_block_commented_resources_are_not_live(self):
        """A database taken out of service during migration prep must not come
        back as a gating dependency on something that no longer exists."""
        out = _harvest({"m.tf": '''
/*
resource "aws_db_instance" "decommissioned" {
  identifier = "old-prod-db"
}
*/
resource "aws_sqs_queue" "q" {
  name = "orders"
}
'''})
        self.assertEqual([e["identifier"] for e in out["entries"]], ["orders"])

    def test_an_s3_arn_and_a_cron_expression_are_not_a_comment(self):
        """`/*` ends every S3 ARN wildcard and `*/` appears in every AWS cron
        expression. A string-blind blanker treats everything between them as a
        comment and deletes live infrastructure — worse than the bug it fixes,
        because a missing database looks exactly like an estate without one."""
        out = _harvest({"m.tf": '''
resource "aws_iam_policy" "p" {
  policy_resource = "arn:aws:s3:::acme-media/*"
}
resource "aws_db_instance" "orders" {
  identifier = "acme-orders-prod"
  engine     = "postgres"
}
resource "aws_cloudwatch_event_rule" "nightly" {
  schedule_expression = "cron(0 */6 * * ? *)"
}
'''})
        declared = [e for e in out["entries"] if e["detection"] == "declared"]
        self.assertEqual([e["identifier"] for e in declared], ["acme-orders-prod"])
        # And the ARN that opened the would-be comment is a bucket the estate
        # reaches without declaring: recorded, with the object wildcard
        # stripped back to the bucket.
        referenced = [e for e in out["entries"] if e["detection"] == "referenced"]
        self.assertEqual([e["identifier"] for e in referenced], ["acme-media"])

    def test_heredoc_text_cannot_open_or_close_a_comment(self):
        """Heredoc bodies are arbitrary text — shell scripts, crontabs, JSON.
        A glob path in one and a crontab line in a later one look exactly like
        a block comment wrapped around every resource in between."""
        out = _harvest({"m.tf": '''
resource "aws_instance" "bastion" {
  user_data = <<-EOT
    rm -rf /var/log/nginx/*
  EOT
}
resource "aws_db_instance" "orders" {
  identifier = "acme-orders-prod"
}
resource "aws_s3_bucket" "media" {
  bucket = "acme-customer-media"
}
resource "aws_instance" "worker" {
  user_data = <<-EOT
    */5 * * * * root /usr/local/bin/backup
  EOT
}
'''})
        self.assertCountEqual([e["identifier"] for e in out["entries"]],
                              ["acme-orders-prod", "acme-customer-media"])

    def test_an_escaped_quote_inside_an_interpolation_does_not_poison_the_file(self):
        r"""HCL strings nest: a quoted template holds ${...}, which holds
        another quoted string, which holds \". Pairing quotes by counting them
        gets the parity wrong, and the trailing quote then opens a string that
        runs to end of file — after which nothing is blanked, so commented-out
        resources come back and live ones lose their names. Every form below is
        accepted by `terraform fmt`."""
        forms = [
            r'"${replace(var.n, "\"", "")}"',
            r'"${format("say \"%s\"", var.n)}"',
            r'"${join(",", [for s in var.l : "\"${s}\""])}"',
            r'"${length(regexall("\"[a-z]+\"", var.n))}"',
        ]
        for form in forms:
            with self.subTest(form=form):
                # The poison line comes FIRST. An earlier version of this test
                # put it last, after the name had already been read, so it
                # passed while the block was still being truncated at the
                # interpolation's closing brace.
                out = _harvest({"m.tf": f'''
resource "aws_sqs_queue" "live" {{
  tags_desc = {form}
  name      = "orders"
}}
/*
resource "aws_db_instance" "ghost" {{
  identifier = "ghost-db"
}}
*/
resource "aws_s3_bucket" "after" {{
  bucket = "acme-after"
}}
'''})
                self.assertCountEqual([e["identifier"] for e in out["entries"]],
                                      ["orders", "acme-after"])

    def test_an_interpolation_does_not_truncate_the_block_it_sits_in(self):
        r"""The interpolation's closing `}` must not be read as the block's.
        Truncation is worse than a lost field for two arguments in particular:
        a module loses its `source` and vanishes entirely, and a replica loses
        `replicate_source_db` and is recorded as a second primary."""
        poison = r'tags_desc = "${replace(var.n, "\"", "")}"'
        module_out = _harvest({"m.tf": f'''
module "db" {{
  {poison}
  source     = "terraform-aws-modules/rds/aws"
  identifier = "acme-orders"
}}
'''})
        self.assertEqual([(e["service"], e["identifier"]) for e in module_out["entries"]],
                         [("rds", "acme-orders")])

        replica_out = _harvest({"m.tf": f'''
resource "aws_db_instance" "primary" {{
  identifier = "acme-orders"
}}
resource "aws_db_instance" "replica" {{
  {poison}
  identifier          = "acme-orders-replica"
  replicate_source_db = aws_db_instance.primary.id
}}
'''})
        self.assertEqual([e["identifier"] for e in replica_out["entries"]],
                         ["acme-orders"])

    def test_a_multi_line_template_does_not_leak_its_contents_as_arguments(self):
        """The TF-0.11 `"${ ... }"` wrapper around a multi-line expression. The
        mask blanks its braces so block depth never rises, while the text still
        shows the inner `key = value` lines — which are then read as the
        enclosing block's own arguments. An inner `replicate_source_db` deletes
        the database as a replica; an inner `source` re-types it."""
        primary = _harvest({"m.tf": '''
resource "aws_db_instance" "primary" {
  tags = "${jsonencode({
    replicate_source_db = "aws_db_instance.other"
  })}"
  identifier = "prod-orders-primary"
  engine     = "postgres"
}
'''})
        self.assertEqual([(e["identifier"], e["engine"]) for e in primary["entries"]],
                         [("prod-orders-primary", "postgres")])

        queue = _harvest({"m.tf": '''
module "orders_queue" {
  extra = "${jsonencode({
    source = "terraform-aws-modules/rds/aws"
  })}"
  source = "terraform-aws-modules/sqs/aws"
  name   = "orders-events"
}
'''})
        self.assertEqual([(e["service"], e["identifier"]) for e in queue["entries"]],
                         [("sqs", "orders-events")])

    def test_a_multi_line_template_does_not_forge_a_disposition(self):
        out = _harvest({"m.tf": '''
resource "aws_elasticache_replication_group" "r" {
  tags = "${jsonencode({
    snapshot_retention_limit = 0
  })}"
  replication_group_id     = "prod-sessions"
  snapshot_retention_limit = 7
}
'''})
        self.assertEqual(out["entries"][0]["disposition"], "migrate")

    def test_a_block_whose_only_argument_is_nested_is_not_truncation(self):
        """A real block with no top-level scalar arguments looks identical to
        an empty one. Using that as the "unterminated" signal stopped the scan
        on well-formed files — three in the two sample estates."""
        out = _harvest({"m.tf": '''
resource "null_resource" "blocker" {
  triggers = {
    blocker = module.eks.cluster_id
  }
}
resource "aws_db_instance" "after" {
  identifier = "acme-orders"
}
'''})
        self.assertEqual([e["identifier"] for e in out["entries"]], ["acme-orders"])
        self.assertFalse([n for n in out["notes"] if "never closed" in n])

    def test_an_unterminated_block_stops_the_scan_and_says_so(self):
        out = _harvest({"m.tf": '''
resource "aws_db_instance" "first" {
  identifier = "acme-orders"
resource "aws_s3_bucket" "unreachable" {
  bucket = "never-seen"
}
'''})
        self.assertNotIn("never-seen", [e["identifier"] for e in out["entries"]])
        self.assertTrue([n for n in out["notes"] if "never closed" in n])

    def test_a_byte_order_mark_does_not_hide_the_first_block(self):
        """A BOM survives a utf-8 read and blocks the block pattern's line
        anchor, so the first declaration in the file disappears."""
        with tempfile.TemporaryDirectory() as root:
            with open(os.path.join(root, "m.tf"), "w", encoding="utf-8-sig") as f:
                f.write('resource "aws_db_instance" "d" {\n  identifier = "acme"\n}\n')
            inventory = {"data_dependencies": []}
            datastores.harvest_datastores(inventory, root)
        self.assertEqual([e["identifier"] for e in inventory["data_dependencies"]],
                         ["acme"])

    def test_a_literal_dollar_brace_is_not_an_interpolation(self):
        r"""`$${` is HCL's escape for a literal `${`. Read as an interpolation
        opener it never closes, so nothing after it is blanked and a
        commented-out resource returns."""
        for escape in ("$${-thing", "%%{-thing"):
            with self.subTest(escape=escape):
                out = _harvest({"m.tf": f'''
resource "aws_sqs_queue" "live" {{
  name = "orders"
  x    = "{escape}"
}}
/*
resource "aws_db_instance" "ghost" {{
  identifier = "ghost-db"
}}
*/
'''})
                self.assertEqual([e["identifier"] for e in out["entries"]], ["orders"])

    def test_an_escaped_quote_in_a_value_is_read_not_discarded(self):
        out = _harvest({"m.tf": r'''
resource "aws_s3_bucket" "b" {
  bucket = "acme-say-\"hi\""
}
'''})
        entry = out["entries"][0]
        self.assertEqual(entry["identifier"], 'acme-say-"hi"')
        self.assertFalse(entry["identifier_is_fallback"])

    def test_a_commented_out_heredoc_opener_does_not_run_away(self):
        """Selecting a user_data block and hitting comment-toggle is the usual
        way a heredoc gets commented out. If the opener is recognised inside
        the comment, the terminator is commented too and never matches, so
        everything to the end of the file is blanked."""
        for marker in ("#", "//"):
            with self.subTest(marker=marker):
                out = _harvest({"m.tf": f'''
resource "aws_instance" "bastion" {{
  ami = "ami-1"
{marker}  user_data = <<EOF
{marker}  echo bootstrap
{marker}  EOF
}}
resource "aws_db_instance" "orders" {{
  identifier = "acme-orders-prod"
}}
resource "aws_s3_bucket" "media" {{
  bucket = "acme-customer-media"
}}
'''})
                self.assertCountEqual([e["identifier"] for e in out["entries"]],
                                      ["acme-orders-prod", "acme-customer-media"])

    def test_a_policy_heredoc_does_not_swallow_what_follows(self):
        out = _harvest({"m.tf": '''
resource "aws_iam_policy" "p" {
  policy = <<EOF
{"Statement": [{"Effect": "Allow", "Resource": "arn:aws:s3:::acme/*"}]}
EOF
}
resource "aws_db_instance" "orders" {
  identifier = "acme-orders-prod"
}
'''})
        declared = [e for e in out["entries"] if e["detection"] == "declared"]
        self.assertEqual([e["identifier"] for e in declared], ["acme-orders-prod"])
        # The heredoc's own ARN is read too — as a referenced bucket, not as
        # a comment swallowing the database after it.
        referenced = [e for e in out["entries"] if e["detection"] == "referenced"]
        self.assertEqual([(e["service"], e["identifier"]) for e in referenced],
                         [("s3", "acme")])

    def test_a_brace_in_a_comment_does_not_hide_the_arguments(self):
        """Counting braces in raw text makes one unbalanced brace in a note look
        like a nested block, so every argument after it is read at the wrong
        depth — the resource loses its name and its engine, silently."""
        out = _harvest({"m.tf": '''
resource "aws_db_instance" "d" {
  # NOTE: see the runbook { section 4
  identifier = "acme-orders-prod"
  engine     = "postgres"
}
'''})
        entry = out["entries"][0]
        self.assertEqual(entry["identifier"], "acme-orders-prod")
        self.assertEqual(entry["engine"], "postgres")
        self.assertFalse(entry["identifier_is_fallback"])

    def test_an_unterminated_block_comment_does_not_blank_the_file(self):
        out = _harvest({"m.tf": '''
resource "aws_sqs_queue" "q" {
  name = "orders"
}
/* trailing, never closed
'''})
        self.assertEqual([e["identifier"] for e in out["entries"]], ["orders"])

    def test_template_directives_are_expressions_not_names(self):
        out = _harvest({"m.tf": '''
resource "aws_sqs_queue" "q" {
  name = "%{ if var.prod }prod%{ else }dev%{ endif }-orders"
}
'''})
        entry = out["entries"][0]
        self.assertEqual(entry["identifier"], "aws_sqs_queue.q")
        self.assertTrue(entry["identifier_is_fallback"])


class ResourceGranularityTest(unittest.TestCase):
    """One database must produce one entry: the assessment prices per entry,
    at 2 days for RDS and 8 days plus a quote for Aurora."""

    def test_an_aurora_cluster_and_its_instances_are_one_database(self):
        out = _harvest({"m.tf": '''
resource "aws_rds_cluster" "c" {
  cluster_identifier = "acme-orders"
  engine             = "aurora-postgresql"
}
resource "aws_rds_cluster_instance" "one" {
  cluster_identifier = "acme-orders"
}
resource "aws_rds_cluster_instance" "two" {
  cluster_identifier = "acme-orders"
}
'''})
        self.assertEqual([(e["service"], e["identifier"]) for e in out["entries"]],
                         [("aurora", "acme-orders")])

    def test_instances_named_by_expression_do_not_become_phantoms(self):
        """The normal case: cluster_identifier references the cluster resource,
        so a mapped instance would fall back to its own address and appear as a
        second, differently-named Aurora database."""
        out = _harvest({"m.tf": '''
resource "aws_rds_cluster" "c" {
  cluster_identifier = "acme-orders"
}
resource "aws_rds_cluster_instance" "i" {
  cluster_identifier = aws_rds_cluster.c.id
}
'''})
        self.assertEqual(len(out["entries"]), 1)

    def test_a_multi_az_db_cluster_is_rds_not_aurora(self):
        """`aws_rds_cluster` is Aurora *and* the Multi-AZ DB cluster, told apart
        only by engine. Graded as Aurora, a plain MySQL cluster is escalated as
        having no GCP equivalent and priced at 8 days rather than 2."""
        out = _harvest({"m.tf": '''
resource "aws_rds_cluster" "c" {
  cluster_identifier = "acme-orders"
  engine             = "mysql"
}
'''})
        entry = out["entries"][0]
        self.assertEqual(entry["service"], "rds")
        self.assertEqual(entry["disposition"], "migrate")
        self.assertTrue(any("Multi-AZ DB cluster" in n for n in entry["notes"]))

    def test_a_real_aurora_cluster_still_escalates(self):
        out = _harvest({"m.tf": '''
resource "aws_rds_cluster" "c" {
  cluster_identifier = "acme-orders"
  engine             = "aurora-postgresql"
}
'''})
        self.assertEqual(out["entries"][0]["service"], "aurora")
        self.assertEqual(out["entries"][0]["disposition"], "escalate")

    def test_a_read_replica_is_not_a_second_database(self):
        """The symmetric case of the cluster-and-instances double count: two
        entries at 2 days each, and a runbook for a database nobody migrates."""
        out = _harvest({"m.tf": '''
resource "aws_db_instance" "primary" {
  identifier = "acme-orders"
  engine     = "postgres"
}
resource "aws_db_instance" "replica" {
  identifier          = "acme-orders-replica"
  replicate_source_db = aws_db_instance.primary.identifier
}
'''})
        self.assertEqual([e["identifier"] for e in out["entries"]], ["acme-orders"])

    def test_a_null_replicate_source_is_a_primary_not_a_replica(self):
        """`replicate_source_db = null` is how a primary opts out. Reading the
        argument's presence rather than its value deletes the database."""
        out = _harvest({"m.tf": '''
resource "aws_db_instance" "primary" {
  identifier          = "acme-orders"
  engine              = "postgres"
  replicate_source_db = null
}
'''})
        self.assertEqual([e["identifier"] for e in out["entries"]], ["acme-orders"])

    def test_an_empty_replicate_source_is_a_primary(self):
        """`""` is the no-replication sentinel wrapper modules pass —
        `terraform-aws-modules/rds/aws` defaulted to it before v5. It arrives
        as two literal characters, so a raw-string check reads it as a name."""
        out = _harvest({"m.tf": '''
resource "aws_db_instance" "orders" {
  identifier          = "acme-orders"
  replicate_source_db = ""
}
'''})
        self.assertEqual([e["identifier"] for e in out["entries"]], ["acme-orders"])

    def test_a_replica_of_an_external_primary_is_kept(self):
        """A cross-account ARN or a data source means the primary is outside
        this estate, so nothing else counts it. The assessment grades exactly
        this as its worst readiness band, so dropping it removes the case."""
        for source in ('"arn:aws:rds:eu-west-1:999999999999:db:legacy"',
                       "data.aws_db_instance.legacy.identifier"):
            with self.subTest(source=source):
                out = _harvest({"m.tf": f'''
resource "aws_db_instance" "r" {{
  identifier          = "acme-replica"
  replicate_source_db = {source}
}}
'''})
                declared = [e for e in out["entries"] if e["detection"] == "declared"]
                self.assertEqual([e["identifier"] for e in declared],
                                 ["acme-replica"])
                # A literal cross-account ARN also names the primary itself:
                # an RDS instance in another account this estate reads from,
                # which is the assessment's cross-account case by definition.
                referenced = [e for e in out["entries"]
                              if e["detection"] == "referenced"]
                if source.startswith('"arn:'):
                    self.assertEqual(
                        [(e["service"], e["identifier"], e["region"])
                         for e in referenced], [("rds", "legacy", "eu-west-1")])
                else:
                    self.assertEqual(referenced, [])

    def test_only_an_in_estate_reference_counts_as_a_replica(self):
        """An allowlist, because every round of extending the deny list missed
        the next expression form and each miss deletes a database."""
        for source in ('coalesce(var.a, "")', 'lookup(var.x, "y", null)',
                       "each.value.source", "try(var.src, null)", "null",
                       "var.replicate_source_db", "local.src"):
            with self.subTest(source=source):
                out = _harvest({"m.tf": f'''
resource "aws_db_instance" "p" {{
  identifier          = "acme-orders"
  replicate_source_db = {source}
}}
'''})
                self.assertEqual([e["identifier"] for e in out["entries"]],
                                 ["acme-orders"])

    def test_an_aurora_replica_uses_a_different_argument(self):
        out = _harvest({"m.tf": '''
resource "aws_rds_cluster" "primary" {
  cluster_identifier = "acme-orders"
  engine             = "aurora-postgresql"
}
resource "aws_rds_cluster" "replica" {
  cluster_identifier            = "acme-orders-dr"
  engine                        = "aurora-postgresql"
  replication_source_identifier = aws_rds_cluster.primary.arn
}
'''})
        self.assertEqual([e["identifier"] for e in out["entries"]], ["acme-orders"])

    def test_a_wrapper_modules_passthrough_is_not_a_replica(self):
        """A generic in-repo RDS wrapper declares the argument for every caller
        — `terraform-aws-modules/rds/aws` does exactly this. Treating that as a
        replica loses every database declared through the wrapper."""
        out = _harvest({"m.tf": '''
resource "aws_db_instance" "this" {
  identifier          = "acme-orders"
  replicate_source_db = var.replicate_source_db
}
'''})
        self.assertEqual([e["identifier"] for e in out["entries"]], ["acme-orders"])

    def test_a_multi_az_cluster_keeps_the_name_its_declaration_states(self):
        """Refining aurora to rds must not cost the cluster its identity:
        `cluster_identifier` is not an RDS identity argument by default, so the
        entry would fall back to a Terraform address and claim, falsely, that
        the real name was not knowable from the files."""
        out = _harvest({"m.tf": '''
resource "aws_rds_cluster" "c" {
  cluster_identifier = "acme-orders-prod"
  engine             = "mysql"
}
'''})
        entry = out["entries"][0]
        self.assertEqual(entry["identifier"], "acme-orders-prod")
        self.assertFalse(entry["identifier_is_fallback"])

    def test_skipped_replicas_are_reported_not_silent(self):
        out = _harvest({"m.tf": '''
resource "aws_db_instance" "primary" {
  identifier = "acme-orders"
}
resource "aws_db_instance" "replica" {
  identifier          = "acme-orders-replica"
  replicate_source_db = aws_db_instance.primary.identifier
}
'''})
        self.assertTrue(any("read replica" in n for n in out["notes"]))

    def test_a_snapshot_is_not_a_database(self):
        """A backup artifact would add a phantom entry, and two days to the
        estimate, for a database that does not exist."""
        out = _harvest({"m.tf": '''
resource "aws_db_cluster_snapshot" "nightly" {
  db_cluster_snapshot_identifier = "acme-nightly"
}
'''})
        self.assertEqual(out["entries"], [])


class DeclarationOverridesTheTableTest(unittest.TestCase):
    """Two services cannot be judged from their type alone, and both mistakes
    are silent, so the declaration is read instead of assumed."""

    def test_a_redis_with_snapshots_is_data_not_a_cache(self):
        out = _harvest({"m.tf": '''
resource "aws_elasticache_replication_group" "sessions" {
  replication_group_id     = "acme-sessions"
  snapshot_retention_limit = 7
}
'''})
        entry = out["entries"][0]
        self.assertEqual(entry["disposition"], "migrate")
        self.assertTrue(any("persists" in n for n in entry["notes"]))

    def test_a_redis_with_snapshots_off_still_rebuilds(self):
        out = _harvest({"m.tf": '''
resource "aws_elasticache_replication_group" "cache" {
  replication_group_id     = "acme-cache"
  snapshot_retention_limit = 0
}
'''})
        self.assertEqual(out["entries"][0]["disposition"], "rebuild")

    def test_an_unstated_retention_says_it_could_not_be_ruled_out(self):
        out = _harvest({"m.tf": '''
resource "aws_elasticache_cluster" "c" {
  cluster_id = "acme-cache"
}
'''})
        entry = out["entries"][0]
        self.assertEqual(entry["disposition"], "rebuild")
        self.assertTrue(any("could not be ruled out" in n for n in entry["notes"]))

    def test_platform_buckets_are_recorded_but_not_planned(self):
        """A raw bucket, which is how state and log buckets are usually
        declared — the earlier guard only looked at module sources."""
        out = _harvest({"m.tf": '''
resource "aws_s3_bucket" "build_artifact_bucket" {
  bucket = "acme-codepipeline-artifacts"
}
resource "aws_s3_bucket" "uploads" {
  bucket = "acme-customer-uploads"
}
'''})
        found = _by_id(out["entries"])
        self.assertEqual(found["acme-codepipeline-artifacts"]["disposition"], "undecided")
        self.assertEqual(found["acme-customer-uploads"]["disposition"], "migrate")

    def test_a_bucket_is_never_dropped_only_downgraded(self):
        """A wrong guess must cost a reviewer a moment, not lose a bucket."""
        out = _harvest({"m.tf": '''
resource "aws_s3_bucket" "tf" {
  bucket = "acme-tfstate"
}
'''})
        self.assertEqual(len(out["entries"]), 1)

    def test_estate_named_modules_are_not_mistaken_for_state(self):
        self.assertEqual(datastores.service_for_module_source("realestate/s3-bucket/aws"), "s3")


class ScanNotesArePersistedTest(unittest.TestCase):

    def test_notes_land_in_the_inventory_not_just_the_response(self):
        """An empty section with no durable record of why is indistinguishable
        from an estate that has no data services."""
        with tempfile.TemporaryDirectory() as root:
            _write(root, "eks.tf", 'resource "aws_eks_cluster" "c" {\n  name = "p"\n}\n')
            inventory = {"data_dependencies": []}
            datastores.harvest_datastores(inventory, root)
        persisted = inventory["data_dependency_scan_notes"]
        self.assertTrue(any("Terraform .tf files only" in n for n in persisted))

    def test_persisted_notes_do_not_duplicate_across_rescans(self):
        with tempfile.TemporaryDirectory() as root:
            _write(root, "eks.tf", 'resource "aws_eks_cluster" "c" {\n  name = "p"\n}\n')
            inventory = {"data_dependencies": []}
            datastores.harvest_datastores(inventory, root)
            first = len(inventory["data_dependency_scan_notes"])
            datastores.harvest_datastores(inventory, root)
        self.assertEqual(len(inventory["data_dependency_scan_notes"]), first)


class MultiplicityTest(unittest.TestCase):
    """One block is not always one resource, and the count is what the
    assessment prices and the gate reads."""

    def test_a_local_module_definition_is_a_template_not_a_deployment(self):
        """Three environments instantiating one local module yield a single
        entry, named after the template, evidenced by a file that declares
        none of the three real databases. Detecting that is out of scope here;
        going unremarked is not."""
        out = _harvest({
            "envs/prod/main.tf": '''
module "db" {
  source     = "../../modules/database"
  identifier = "prod-orders"
}
''',
            "modules/database/main.tf": '''
resource "aws_db_instance" "this" {
  identifier = var.identifier
}
''',
        })
        entry = out["entries"][0]
        self.assertTrue(any("template rather than a deployment" in n
                            for n in entry["notes"]))
        self.assertTrue(any("local module definitions" in n for n in out["notes"]))

    def test_count_and_for_each_are_reported(self):
        for meta in ("count = 4", "for_each = toset(var.tenants)"):
            with self.subTest(meta=meta):
                out = _harvest({"m.tf": f'''
resource "aws_db_instance" "shard" {{
  {meta}
  identifier = "acme-shard"
}}
'''})
                # "or none" matters: `count = var.enabled ? 1 : 0` is the
                # standard optional-resource idiom, so the note has to warn
                # about under-counting as well as over-counting.
                self.assertTrue(any("several of these or none" in n
                                    for n in out["entries"][0]["notes"]))


class DispositionTest(unittest.TestCase):

    def test_every_known_service_has_a_disposition(self):
        # Both mappings: a module-only service with no disposition row would
        # otherwise grade undecided with nothing noticing.
        emitted = set(datastores.RESOURCE_SERVICES.values()) | {
            service for _, service in datastores.MODULE_PATTERNS}
        for service in emitted:
            with self.subTest(service=service):
                self.assertIn(service, datastores.DISPOSITIONS)

    def test_cache_rebuilds_but_durable_cache_migrates(self):
        """MemoryDB keeps its data; treating it like ElastiCache loses it."""
        self.assertEqual(datastores.DISPOSITIONS["elasticache"], "rebuild")
        self.assertEqual(datastores.DISPOSITIONS["memorydb"], "migrate")

    def test_engine_change_targets_escalate(self):
        for service in ("dynamodb", "aurora", "neptune", "redshift", "docdb"):
            with self.subTest(service=service):
                self.assertEqual(datastores.DISPOSITIONS[service], "escalate")

    def test_secrets_and_config_stores_gate(self):
        """Decided 2026-08-21, pinned here because the instinct is to exempt
        them: they look like plumbing for the databases beside them, but an
        app whose password or connection string did not come across fails on
        startup with nothing to rebuild from. They gate for manual resolution."""
        for service in ("secretsmanager", "ssm"):
            with self.subTest(service=service):
                self.assertEqual(datastores.DISPOSITIONS[service], "migrate")

    def test_a_service_with_no_disposition_row_grades_undecided(self):
        """Drives the production path rather than asserting dict.get's default:
        a service added to the mapping without a disposition must land as a
        human decision, not as something with a plan."""
        datastores.RESOURCE_SERVICES["aws_quantumledger_ledger"] = "other"
        try:
            out = _harvest({"m.tf": 'resource "aws_quantumledger_ledger" "l" {\n'
                                    '  name = "acme-ledger"\n}\n'})
            self.assertEqual(out["entries"][0]["disposition"], "undecided")
        finally:
            del datastores.RESOURCE_SERVICES["aws_quantumledger_ledger"]


class MergeTest(unittest.TestCase):

    def test_rescan_is_idempotent(self):
        files = {"m.tf": 'resource "aws_db_instance" "d" {\n  identifier = "acme"\n}\n'}
        with tempfile.TemporaryDirectory() as root:
            for rel, content in files.items():
                _write(root, rel, content)
            inventory = {"data_dependencies": []}
            datastores.harvest_datastores(inventory, root)
            first = len(inventory["data_dependencies"])
            notes_first = list(inventory["data_dependencies"][0]["notes"])
            datastores.harvest_datastores(inventory, root)
            self.assertEqual(len(inventory["data_dependencies"]), first)
            self.assertEqual(inventory["data_dependencies"][0]["notes"], notes_first)

    def test_same_resource_in_two_files_unions_evidence(self):
        """Terraform merges `_override.tf` into the resource it repeats, so the
        same database is genuinely declared across two files."""
        out = _harvest({
            "cluster.tf": '''
resource "aws_rds_cluster" "c" {
  cluster_identifier = "acme-orders"
  engine             = "aurora-postgresql"
}
''',
            "instances.tf": '''
resource "aws_rds_cluster" "c" {
  cluster_identifier = "acme-orders"
}
''',
        })
        entries = [e for e in out["entries"] if e["identifier"] == "acme-orders"]
        self.assertEqual(len(entries), 1)
        self.assertCountEqual(entries[0]["evidence"], ["cluster.tf", "instances.tf"])
        # The file that stated the engine wins over the one that did not.
        self.assertEqual(entries[0]["engine"], "aurora-postgresql")

    def test_the_same_literal_name_in_two_environments_is_two_resources(self):
        """A Terraform root module is a directory, so envs/dev and envs/prod
        each declaring a queue named "orders" are two queues in two accounts."""
        out = _harvest({
            "envs/dev/main.tf": 'resource "aws_sqs_queue" "q" {\n  name = "orders"\n}\n',
            "envs/prod/main.tf": 'resource "aws_sqs_queue" "q" {\n  name = "orders"\n}\n',
        })
        self.assertEqual(len(out["entries"]), 2)
        self.assertCountEqual(
            [e["evidence"][0] for e in out["entries"]],
            [os.path.join("envs", "dev", "main.tf"),
             os.path.join("envs", "prod", "main.tf")])

    def test_generic_labels_in_different_directories_do_not_collide(self):
        """`this` is the single-resource module convention — several estates
        use it, and fusing two of them would lose a real dependency."""
        out = _harvest({
            "storage/a/main.tf":
                'module "this" {\n  source = "terraform-aws-modules/efs/aws"\n}\n',
            "storage/b/main.tf":
                'module "this" {\n  source = "terraform-aws-modules/efs/aws"\n}\n',
        })
        self.assertEqual(len(out["entries"]), 2)
        self.assertTrue(all(e["identifier"] == "module.this" for e in out["entries"]))
        self.assertCountEqual(
            [e["evidence"][0] for e in out["entries"]],
            [os.path.join("storage", "a", "main.tf"), os.path.join("storage", "b", "main.tf")])

    def test_the_same_fallback_in_one_directory_still_folds(self):
        out = _harvest({
            "main.tf": 'module "this" {\n  source = "terraform-aws-modules/efs/aws"\n}\n',
            "extra.tf": 'module "this" {\n  source = "terraform-aws-modules/efs/aws"\n}\n',
        })
        self.assertEqual(len(out["entries"]), 1)

    def test_declared_outranks_referenced(self):
        # Consumers are objects, not strings (consumers.py). A string fixture
        # merges fine — merge_datastores is schema-agnostic — but models data
        # save_inventory would reject, so the union is exercised in the shape
        # that actually reaches the ledger.
        ui_consumer = {
            "workload": "ui", "kind": "helm_release", "namespace": None,
            "source_path": None, "detection": "terraform_wiring",
            "evidence": "policy.json",
        }
        inventory = {"data_dependencies": [{
            "service": "s3", "identifier": "acme-assets", "detection": "referenced",
            "declared_in_repo": False, "evidence": ["policy.json"],
            "consumers": [ui_consumer],
        }]}
        datastores.merge_datastores(inventory, [{
            "service": "s3", "identifier": "acme-assets", "detection": "declared",
            "declared_in_repo": True, "evidence": ["s3.tf"], "consumers": [],
            "region": "eu-west-1",
        }])
        entry = inventory["data_dependencies"][0]
        self.assertEqual(len(inventory["data_dependencies"]), 1)
        self.assertEqual(entry["detection"], "declared")
        self.assertTrue(entry["declared_in_repo"])
        self.assertEqual(entry["region"], "eu-west-1")
        self.assertCountEqual(entry["evidence"], ["policy.json", "s3.tf"])
        # A consumer learned from the reference must survive the declaration.
        self.assertEqual(entry["consumers"], [ui_consumer])

    def test_a_referenced_entry_takes_the_declared_twins_address(self):
        """An entry known only from an ARN in a policy has no block to name,
        so it carries a null address. Folding it with the declaration that
        does have one must keep the address, or the merged entry records the
        database as coming from nowhere while the file that declares it is
        right there in `evidence`."""
        inventory = {"data_dependencies": [{
            "service": "s3", "identifier": "acme-assets", "detection": "referenced",
            "declared_in_repo": False, "evidence": ["policy.json"],
            "consumers": [], "address": None,
        }]}
        datastores.merge_datastores(inventory, [{
            "service": "s3", "identifier": "acme-assets", "detection": "declared",
            "declared_in_repo": True, "evidence": ["s3.tf"], "consumers": [],
            "address": "aws_s3_bucket.assets",
        }])
        self.assertEqual(inventory["data_dependencies"][0]["address"],
                         "aws_s3_bucket.assets")

    def test_merging_two_entries_does_not_duplicate_an_identical_consumer(self):
        """Consumers dedupe by dict equality, and `record` appends the same
        object to several entries, so two entries folding together must not
        stack two copies of one workload."""
        consumer = {
            "workload": "ui", "kind": "helm_release", "namespace": "shop",
            "source_path": "src/ui/chart", "detection": "terraform_wiring",
            "evidence": "main.tf",
        }
        inventory = {"data_dependencies": [{
            "service": "s3", "identifier": "acme-assets", "detection": "declared",
            "declared_in_repo": True, "evidence": ["a.tf"],
            "consumers": [dict(consumer)],
        }]}
        datastores.merge_datastores(inventory, [{
            "service": "s3", "identifier": "acme-assets", "detection": "declared",
            "declared_in_repo": True, "evidence": ["a.tf"],
            "consumers": [dict(consumer)],
        }])
        self.assertEqual(inventory["data_dependencies"][0]["consumers"], [consumer])


class ScanReportingTest(unittest.TestCase):

    def test_estate_with_no_datastores_finds_nothing(self):
        """flux-eks-gitops-config's shape: real estate, zero data services."""
        out = _harvest({
            "eks.tf": 'resource "aws_eks_cluster" "c" {\n  name = "prod"\n}\n',
            "iam.tf": 'resource "aws_iam_role" "r" {\n  name = "node"\n}\n',
        })
        self.assertEqual(out["entries"], [])

    def test_dialect_limitation_is_always_reported(self):
        """An empty result must read as 'did not look there', not 'absent'."""
        out = _harvest({"eks.tf": 'resource "aws_eks_cluster" "c" {\n  name = "p"\n}\n'})
        self.assertTrue(any("Terraform .tf files only" in n for n in out["notes"]))

    def test_yaml_is_not_scanned(self):
        """Crossplane and CloudFormation are unsupported; no partial guessing."""
        out = _harvest({"infra.yaml": '''
apiVersion: dynamodb.aws.crossplane.io/v1alpha1
kind: Table
metadata:
  name: products
'''})
        self.assertEqual(out["entries"], [])

    def test_oversized_file_is_noted_not_silently_skipped(self):
        with tempfile.TemporaryDirectory() as root:
            _write(root, "huge.tf", "# padding\n" * 300_000)
            inventory = {"data_dependencies": []}
            notes = datastores.harvest_datastores(inventory, root).notes
            self.assertTrue(any("huge.tf" in n and "larger than" in n for n in notes))


class ScopeTest(unittest.TestCase):
    """The scan runs after confirm_discovery_scope, so exclusions bind.

    This section is headed for the member-readable exports object, so a file
    the operator removed from discovery must not contribute metadata to it.
    """

    ESTATE = {
        "prod/rds.tf": 'resource "aws_db_instance" "d" {\n  identifier = "prod-db"\n}\n',
        "legacy/rds.tf": 'resource "aws_db_instance" "d" {\n  identifier = "legacy-db"\n}\n',
    }

    def test_no_scope_scans_everything(self):
        found = _by_id(_harvest(self.ESTATE)["entries"])
        self.assertCountEqual(found, ["prod-db", "legacy-db"])

    def test_excluded_directory_is_not_scanned(self):
        out = _harvest(self.ESTATE, {"excluded": ["legacy"], "included": []})
        self.assertCountEqual(_by_id(out["entries"]), ["prod-db"])

    def test_exclusion_is_reported_not_silent(self):
        """A thinner result must say why, or it reads as a clean estate."""
        out = _harvest(self.ESTATE, {"excluded": ["legacy"], "included": []})
        self.assertTrue(any("discovery scope excludes them" in n for n in out["notes"]))

    def test_include_wins_over_exclude(self):
        """Matches the shared scope algebra rather than reimplementing it."""
        out = _harvest(self.ESTATE,
                       {"excluded": ["legacy"], "included": ["legacy/rds.tf"]})
        self.assertCountEqual(_by_id(out["entries"]), ["prod-db", "legacy-db"])

    def test_excluding_everything_yields_nothing_with_a_reason(self):
        out = _harvest(self.ESTATE, {"excluded": ["prod", "legacy"], "included": []})
        self.assertEqual(out["entries"], [])
        self.assertTrue(any("discovery scope excludes them" in n for n in out["notes"]))


class SampleEstateTest(unittest.TestCase):
    """The retail-store-sample-app shape: five services, five dispositions."""

    ESTATE = {"dependencies.tf": '''
module "catalog" {
  source             = "terraform-aws-modules/rds-aurora/aws"
  name               = "retail-catalog"
  engine             = "aurora-mysql"
}

module "orders" {
  source             = "terraform-aws-modules/rds-aurora/aws"
  name               = "retail-orders"
  engine             = "aurora-postgresql"
}

module "carts" {
  source = "terraform-aws-modules/dynamodb-table/aws"
  name   = "retail-carts"
}

module "checkout" {
  source = "cloudposse/elasticache-redis/aws"
  name   = "retail-checkout"
}

resource "aws_mq_broker" "orders" {
  broker_name = "retail-orders-mq"
}
'''}

    def test_all_five_are_found(self):
        entries = _harvest(self.ESTATE)["entries"]
        self.assertEqual(len(entries), 5)

    def test_services_and_dispositions(self):
        found = _by_id(_harvest(self.ESTATE)["entries"])
        expected = {
            "retail-catalog": ("aurora", "escalate"),
            "retail-orders": ("aurora", "escalate"),
            "retail-carts": ("dynamodb", "escalate"),
            "retail-checkout": ("elasticache", "rebuild"),
            "retail-orders-mq": ("mq", "replatform"),
        }
        for identifier, (service, disposition) in expected.items():
            with self.subTest(identifier=identifier):
                self.assertEqual(found[identifier]["service"], service)
                self.assertEqual(found[identifier]["disposition"], disposition)

    def test_nothing_in_this_estate_gates_a_workload_by_accident(self):
        """Only 'migrate' should park a component; none of these do."""
        entries = _harvest(self.ESTATE)["entries"]
        self.assertEqual([e for e in entries if e["disposition"] == "migrate"], [])


class ReferencedDatastoreTest(unittest.TestCase):
    """Data stores the estate reaches by ARN but declares nowhere."""

    # The acme-eks-estate shape: a hand-written IRSA role whose inline policy
    # grants the orders service account access to a bucket no file declares.
    ACME = {"terraform/iam-irsa.tf": '''
# IRSA role for the orders service: S3 access for invoice archives.
resource "aws_iam_role" "orders_irsa" {
  name = "acme-prod-orders"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Federated = aws_iam_openid_connect_provider.eks.arn }
      Action    = "sts:AssumeRoleWithWebIdentity"
      Condition = {
        StringEquals = {
          "oidc.eks.us-east-1.amazonaws.com:sub" =
            "system:serviceaccount:acme-shop:orders"
        }
      }
    }]
  })
}

resource "aws_iam_role_policy" "orders_s3" {
  name = "orders-invoice-archive"
  role = aws_iam_role.orders_irsa.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["s3:GetObject", "s3:PutObject", "s3:ListBucket"]
      Resource = ["arn:aws:s3:::acme-invoice-archive",
                  "arn:aws:s3:::acme-invoice-archive/*"]
    }]
  })
}
'''}

    def test_the_acme_bucket_is_recorded_from_its_policy_arn(self):
        out = _harvest(self.ACME)
        self.assertEqual(len(out["entries"]), 1)
        entry = out["entries"][0]
        self.assertEqual(entry["service"], "s3")
        self.assertEqual(entry["identifier"], "acme-invoice-archive")
        self.assertEqual(entry["detection"], "referenced")
        self.assertFalse(entry["declared_in_repo"])
        self.assertEqual(entry["arn"], "arn:aws:s3:::acme-invoice-archive")
        # The ARN is the entry's handle for the review's correction tools.
        self.assertEqual(entry["address"], entry["arn"])
        self.assertEqual(entry["disposition"], "migrate")
        self.assertFalse(entry["identifier_is_fallback"])
        self.assertEqual(entry["evidence"], ["terraform/iam-irsa.tf"])
        self.assertIn(datastores.REFERENCED_NOTE, entry["notes"])

    def test_the_acme_bucket_is_attributed_to_the_orders_service_account(self):
        entry = _harvest(self.ACME)["entries"][0]
        self.assertEqual(
            [(c["workload"], c["kind"], c["namespace"], c["detection"])
             for c in entry["consumers"]],
            [("orders", "service_account", "acme-shop", "irsa")])

    def test_the_scan_note_names_the_referenced_services(self):
        out = _harvest(self.ACME)
        self.assertTrue(any(
            "known only from a literal ARN" in n and "s3 acme-invoice-archive" in n
            for n in out["notes"]), out["notes"])

    def test_a_rescan_is_idempotent(self):
        with tempfile.TemporaryDirectory() as root:
            for rel_path, content in self.ACME.items():
                _write(root, rel_path, content)
            inventory = {"data_dependencies": []}
            datastores.harvest_datastores(inventory, root)
            first = copy.deepcopy(inventory["data_dependencies"])
            datastores.harvest_datastores(inventory, root)
            self.assertEqual(inventory["data_dependencies"], first)

    def test_two_spellings_in_two_files_are_one_entry(self):
        out = _harvest({
            "a/policy.tf": '''
resource "aws_iam_policy" "read" {
  policy = <<EOF
{"Statement": [{"Resource": "arn:aws:s3:::acme-invoice-archive/*"}]}
EOF
}
''',
            "b/policy.tf": '''
resource "aws_iam_policy" "list" {
  policy = <<EOF
{"Statement": [{"Resource": "arn:aws:s3:::acme-invoice-archive"}]}
EOF
}
''',
        })
        self.assertEqual(len(out["entries"]), 1)
        self.assertCountEqual(out["entries"][0]["evidence"],
                              ["a/policy.tf", "b/policy.tf"])

    def test_a_declared_bucket_named_by_a_policy_is_one_declared_entry(self):
        out = _harvest({
            "s3.tf": 'resource "aws_s3_bucket" "archive" {\n  bucket = "acme-invoice-archive"\n}\n',
            "iam.tf": self.ACME["terraform/iam-irsa.tf"],
        })
        self.assertEqual(len(out["entries"]), 1)
        entry = out["entries"][0]
        self.assertEqual(entry["detection"], "declared")
        self.assertTrue(entry["declared_in_repo"])
        self.assertEqual(entry["address"], "aws_s3_bucket.archive")
        # The ARN is filled in from the policy; the referenced-only note is
        # not carried, because the declaration answers it.
        self.assertEqual(entry["arn"], "arn:aws:s3:::acme-invoice-archive")
        self.assertNotIn(datastores.REFERENCED_NOTE, entry["notes"])
        self.assertCountEqual(entry["evidence"], ["s3.tf", "iam.tf"])
        # And the consumer the ARN chain found reaches the declared entry.
        self.assertEqual([c["workload"] for c in entry["consumers"]], ["orders"])

    def test_a_fallback_identifier_never_matches_an_arn(self):
        """A fallback identifier is a Terraform address, not an AWS name, so
        it cannot be the resource an ARN names — even when the bucket behind
        the expression is in fact the same one."""
        out = _harvest({
            "s3.tf": 'resource "aws_s3_bucket" "archive" {\n  bucket = "${var.env}-invoice-archive"\n}\n',
            "iam.tf": self.ACME["terraform/iam-irsa.tf"],
        })
        self.assertEqual(
            sorted((e["detection"], e["identifier"]) for e in out["entries"]),
            [("declared", "aws_s3_bucket.archive"),
             ("referenced", "acme-invoice-archive")])

    def test_an_arn_with_two_declared_twins_stands_on_its_own(self):
        out = _harvest({
            "envs/dev/main.tf": 'resource "aws_sqs_queue" "q" {\n  name = "orders"\n}\n',
            "envs/prod/main.tf": 'resource "aws_sqs_queue" "q" {\n  name = "orders"\n}\n',
            "iam.tf": '''
resource "aws_iam_policy" "p" {
  policy = <<EOF
{"Statement": [{"Resource": "arn:aws:sqs:us-east-1:123456789012:orders"}]}
EOF
}
''',
        })
        self.assertEqual(len(out["entries"]), 3)
        referenced = [e for e in out["entries"] if e["detection"] == "referenced"]
        self.assertEqual(len(referenced), 1)
        self.assertTrue(any("2 declared entries share this name" in n
                            for n in referenced[0]["notes"]), referenced[0]["notes"])

    def test_a_declared_entry_arriving_later_absorbs_the_referenced_one(self):
        """merge_datastores is called per harvest; a second merge that brings
        the declaration must not leave the resource recorded twice."""
        inventory = {"data_dependencies": []}
        datastores.merge_datastores(inventory, [datastores._referenced_entry(
            datastores.find_arns('"arn:aws:s3:::acme-invoice-archive"')[0],
            "iam.tf")])
        datastores.merge_datastores(inventory, [datastores._entry(
            "s3", "acme-invoice-archive", "s3.tf", {"bucket": "acme-invoice-archive"},
            None, [], address="aws_s3_bucket.archive")])
        self.assertEqual(len(inventory["data_dependencies"]), 1)
        entry = inventory["data_dependencies"][0]
        self.assertEqual(entry["detection"], "declared")
        self.assertEqual(entry["arn"], "arn:aws:s3:::acme-invoice-archive")
        self.assertEqual(entry["address"], "aws_s3_bucket.archive")
        self.assertCountEqual(entry["evidence"], ["s3.tf", "iam.tf"])

    def test_wildcards_and_expressions_are_noted_not_recorded(self):
        out = _harvest({"iam.tf": '''
resource "aws_iam_policy" "p" {
  policy = jsonencode({
    Statement = [{
      Resource = [
        "arn:aws:s3:::acme-*",
        "arn:aws:s3:::${var.bucket}",
        "arn:aws:dynamodb:*:*:table/*",
        "arn:aws:dynamodb:us-east-1:123456789012:table/${local.prefix}-carts",
      ]
    }]
  })
}
'''})
        self.assertEqual(out["entries"], [])
        self.assertTrue(any(n.startswith("2 ARN(s) or endpoint(s) name a family") for n in out["notes"]),
                        out["notes"])
        self.assertTrue(any(n.startswith("2 ARN(s) or endpoint(s) build the resource name")
                            for n in out["notes"]), out["notes"])

    def test_a_commented_out_arn_records_nothing(self):
        out = _harvest({"iam.tf": '''
resource "aws_iam_policy" "p" {
  # Resource = "arn:aws:s3:::old-archive"
  policy = "{}"
}
/* arn:aws:sqs:us-east-1:123456789012:retired */
'''})
        self.assertEqual(out["entries"], [])

    def test_an_infrastructure_bucket_arn_is_undecided(self):
        out = _harvest({"iam.tf": '''
resource "aws_iam_policy" "p" {
  policy = "{\\"Resource\\": \\"arn:aws:s3:::acme-tfstate\\"}"
}
'''})
        self.assertEqual([(e["identifier"], e["disposition"]) for e in out["entries"]],
                         [("acme-tfstate", "undecided")])

    def test_region_is_read_from_the_arn(self):
        out = _harvest({"iam.tf": '''
resource "aws_iam_policy" "p" {
  policy = jsonencode({ Resource = [
    "arn:aws:dynamodb:eu-central-1:123456789012:table/carts",
    "arn:aws:s3:::acme-media",
  ] })
}
'''})
        self.assertEqual({e["identifier"]: e["region"] for e in out["entries"]},
                         {"carts": "eu-central-1", "acme-media": None})


class ArnGrammarTest(unittest.TestCase):
    """One row per ARN namespace the scan reads, plus the ones it must not."""

    def test_data_store_arns_resolve_to_a_service_and_a_name(self):
        cases = [
            ("arn:aws:s3:::acme-media/uploads/*", "s3", "acme-media",
             "arn:aws:s3:::acme-media"),
            ("arn:aws:dynamodb:us-east-1:123456789012:table/carts/index/by-user",
             "dynamodb", "carts",
             "arn:aws:dynamodb:us-east-1:123456789012:table/carts"),
            ("arn:aws:rds:us-east-1:123456789012:db:orders", "rds", "orders",
             "arn:aws:rds:us-east-1:123456789012:db:orders"),
            ("arn:aws:rds:us-east-1:123456789012:cluster:catalog", "aurora",
             "catalog", "arn:aws:rds:us-east-1:123456789012:cluster:catalog"),
            ("arn:aws:elasticache:us-east-1:123456789012:replicationgroup:sessions",
             "elasticache", "sessions",
             "arn:aws:elasticache:us-east-1:123456789012:replicationgroup:sessions"),
            ("arn:aws:memorydb:us-east-1:123456789012:cluster/leaderboard",
             "memorydb", "leaderboard",
             "arn:aws:memorydb:us-east-1:123456789012:cluster/leaderboard"),
            ("arn:aws:sqs:us-east-1:123456789012:orders-queue", "sqs",
             "orders-queue", "arn:aws:sqs:us-east-1:123456789012:orders-queue"),
            ("arn:aws:sns:us-east-1:123456789012:alerts:1b2c3d4e-0000-0000-0000-000000000000",
             "sns", "alerts", "arn:aws:sns:us-east-1:123456789012:alerts"),
            ("arn:aws:kinesis:us-east-1:123456789012:stream/clicks", "kinesis",
             "clicks", "arn:aws:kinesis:us-east-1:123456789012:stream/clicks"),
            ("arn:aws:firehose:us-east-1:123456789012:deliverystream/clicks",
             "firehose", "clicks",
             "arn:aws:firehose:us-east-1:123456789012:deliverystream/clicks"),
            ("arn:aws:kafka:us-east-1:123456789012:topic/events/abcd-1234/orders",
             "msk", "events",
             "arn:aws:kafka:us-east-1:123456789012:cluster/events"),
            ("arn:aws:mq:us-east-1:123456789012:broker:orders-mq:b-1234", "mq",
             "orders-mq",
             "arn:aws:mq:us-east-1:123456789012:broker:orders-mq"),
            ("arn:aws:events:us-east-1:123456789012:event-bus/orders",
             "eventbridge", "orders",
             "arn:aws:events:us-east-1:123456789012:event-bus/orders"),
            ("arn:aws:es:us-east-1:123456789012:domain/search/*", "opensearch",
             "search", "arn:aws:es:us-east-1:123456789012:domain/search"),
            ("arn:aws:aoss:us-east-1:123456789012:collection/abc123", "opensearch",
             "abc123", "arn:aws:aoss:us-east-1:123456789012:collection/abc123"),
            ("arn:aws:redshift:us-east-1:123456789012:cluster:warehouse",
             "redshift", "warehouse",
             "arn:aws:redshift:us-east-1:123456789012:cluster:warehouse"),
            ("arn:aws:secretsmanager:us-east-1:123456789012:secret:prod/db/password-??????",
             "secretsmanager", "prod/db/password",
             "arn:aws:secretsmanager:us-east-1:123456789012:secret:prod/db/password"),
            ("arn:aws:ssm:us-east-1:123456789012:parameter/prod/db/host", "ssm",
             "/prod/db/host",
             "arn:aws:ssm:us-east-1:123456789012:parameter/prod/db/host"),
            ("arn:aws:ssm:us-east-1:123456789012:parameter/flag", "ssm", "flag",
             "arn:aws:ssm:us-east-1:123456789012:parameter/flag"),
            ("arn:aws:elasticfilesystem:us-east-1:123456789012:file-system/fs-0123",
             "efs", "fs-0123",
             "arn:aws:elasticfilesystem:us-east-1:123456789012:file-system/fs-0123"),
            ("arn:aws:fsx:us-east-1:123456789012:file-system/fs-0123", "fsx",
             "fs-0123", "arn:aws:fsx:us-east-1:123456789012:file-system/fs-0123"),
            ("arn:aws-us-gov:s3:::gov-media", "s3", "gov-media",
             "arn:aws-us-gov:s3:::gov-media"),
        ]
        for token, service, identifier, arn in cases:
            with self.subTest(token=token):
                found = datastores.find_arns(f'"{token}"')
                self.assertEqual(len(found), 1, found)
                self.assertEqual(found[0].kind, "recorded")
                self.assertEqual((found[0].service, found[0].identifier, found[0].arn),
                                 (service, identifier, arn))

    def test_an_rds_cluster_arn_says_the_engine_is_unknown(self):
        found = datastores.find_arns('"arn:aws:rds:us-east-1:123456789012:cluster:catalog"')
        self.assertIn(datastores._CLUSTER_ARN_NOTE, found[0].notes)

    def test_arns_of_things_that_are_not_data_stores_are_ignored(self):
        for token in (
            "arn:aws:iam::123456789012:role/acme-prod-orders",
            "arn:aws:kms:us-east-1:123456789012:key/abcd",
            "arn:aws:rds:us-east-1:123456789012:subgrp:private",
            "arn:aws:rds:us-east-1:123456789012:pg:custom-pg",
            "arn:aws:events:us-east-1:123456789012:rule/nightly",
            "arn:aws:s3:us-east-1:123456789012:accesspoint/shared",
            "arn:aws:elasticache:us-east-1:123456789012:subnetgroup:private",
            "arn:aws:logs:us-east-1:123456789012:log-group:/aws/eks",
            "arn:aws:redshift:us-east-1:123456789012:dbuser:warehouse/admin",
        ):
            with self.subTest(token=token):
                self.assertEqual(datastores.find_arns(f'"{token}"'), [])

    def test_wildcards_and_expressions_are_classified_not_parsed(self):
        cases = [
            ("arn:aws:s3:::acme-*", "wildcard"),
            ("arn:aws:s3:::*", "wildcard"),
            ("arn:aws:dynamodb:*:*:table/carts-?", "wildcard"),
            ("arn:aws:secretsmanager:us-east-1:123456789012:secret:prod/*", "wildcard"),
            ("arn:aws:secretsmanager:us-east-1:123456789012:secret:app-*", "wildcard"),
            ("arn:aws:s3:::${var.bucket}", "expression"),
            ("arn:aws:s3:::acme-${var.env}-media", "expression"),
            ("arn:aws:dynamodb:${var.region}:${var.account}:table/carts", "expression"),
        ]
        for token, kind in cases:
            with self.subTest(token=token):
                found = datastores.find_arns(f'"{token}"')
                self.assertEqual([f.kind for f in found], [kind])

    def test_prose_punctuation_after_an_arn_is_not_part_of_the_name(self):
        found = datastores.find_arns("see arn:aws:s3:::acme-media. Then stop.")
        self.assertEqual([f.identifier for f in found], ["acme-media"])

    def test_an_arn_inside_a_heredoc_is_read_and_a_commented_one_is_not(self):
        content = '''
resource "aws_iam_policy" "p" {
  # arn:aws:s3:::commented
  policy = <<EOF
{"Resource": "arn:aws:s3:::in-heredoc"}
EOF
}
'''
        text, _mask, _notes, heredocs = datastores.scan_source(content)
        literal = datastores.literal_text(content, text, heredocs)
        self.assertEqual([f.identifier for f in datastores.find_arns(literal)],
                         ["in-heredoc"])


class CrossAccountTest(unittest.TestCase):
    """The account in a referenced ARN, against the estate's own."""

    POLICY = '''
resource "aws_iam_policy" "p" {
  policy = jsonencode({ Resource = [
    "arn:aws:dynamodb:us-east-1:999999999999:table/shared-catalog",
    "arn:aws:sqs:us-east-1:123456789012:orders",
    "arn:aws:s3:::acme-invoice-archive",
  ] })
}
'''

    def test_the_account_is_read_from_the_arn(self):
        found = _by_id(_harvest({"iam.tf": self.POLICY})["entries"])
        self.assertEqual(found["shared-catalog"]["account"], "999999999999")
        self.assertEqual(found["orders"]["account"], "123456789012")
        # An S3 ARN never states one.
        self.assertIsNone(found["acme-invoice-archive"]["account"])

    def test_a_declared_entry_carries_the_field_as_null(self):
        out = _harvest({"m.tf": 'resource "aws_sqs_queue" "q" {\n  name = "orders"\n}\n'})
        self.assertIn("account", out["entries"][0])
        self.assertIsNone(out["entries"][0]["account"])

    def test_an_account_the_provider_does_not_name_is_cross_account(self):
        out = _harvest({
            "providers.tf": '''
provider "aws" {
  region              = "us-east-1"
  allowed_account_ids = ["123456789012"]
}
''',
            "iam.tf": self.POLICY,
        })
        found = _by_id(out["entries"])
        foreign = [n for n in found["shared-catalog"]["notes"]
                   if n.startswith(datastores.CROSS_ACCOUNT_NOTE_PREFIX)]
        self.assertEqual(len(foreign), 1, found["shared-catalog"]["notes"])
        self.assertIn("999999999999", foreign[0])
        self.assertIn("123456789012", foreign[0])
        # Same account, and no account at all, are not cross-account.
        for identifier in ("orders", "acme-invoice-archive"):
            self.assertFalse(any(n.startswith(datastores.CROSS_ACCOUNT_NOTE_PREFIX)
                                 for n in found[identifier]["notes"]), identifier)
        self.assertFalse(any("states no account of its own" in n
                             for n in out["notes"]))

    def test_an_assume_role_arn_names_the_estate_account_too(self):
        out = _harvest({
            "providers.tf": '''
provider "aws" {
  assume_role {
    role_arn = "arn:aws:iam::999999999999:role/terraform"
  }
}
''',
            "iam.tf": self.POLICY,
        })
        found = _by_id(out["entries"])
        self.assertFalse(any(n.startswith(datastores.CROSS_ACCOUNT_NOTE_PREFIX)
                             for n in found["shared-catalog"]["notes"]))
        self.assertTrue(any(n.startswith(datastores.CROSS_ACCOUNT_NOTE_PREFIX)
                            for n in found["orders"]["notes"]))

    def test_without_a_stated_account_the_scan_says_it_cannot_tell(self):
        out = _harvest({"iam.tf": self.POLICY})
        self.assertFalse(any(n.startswith(datastores.CROSS_ACCOUNT_NOTE_PREFIX)
                             for e in out["entries"] for n in e["notes"]))
        unknown = [n for n in out["notes"] if "whether they are cross-account is not knowable" in n]
        self.assertEqual(len(unknown), 1, out["notes"])
        self.assertIn("2 data service(s)", unknown[0])
        self.assertIn("shared-catalog (999999999999)", unknown[0])
        # And each such entry says so itself.
        found = _by_id(out["entries"])
        for identifier in ("shared-catalog", "orders"):
            self.assertTrue(any(n.startswith(datastores.UNKNOWN_ACCOUNT_NOTE_PREFIX)
                                for n in found[identifier]["notes"]), identifier)
        self.assertFalse(any(n.startswith(datastores.UNKNOWN_ACCOUNT_NOTE_PREFIX)
                             for n in found["acme-invoice-archive"]["notes"]))

    def test_a_foreign_account_arn_never_folds_onto_a_declared_twin(self):
        """A same-named queue in the finance account is a different queue.
        Folding it onto the local declaration would erase the one
        cross-account dependency this section exists to surface, so the
        referenced entry stands, note and account intact, beside the
        declared one."""
        out = _harvest({
            "providers.tf": 'provider "aws" {\n  allowed_account_ids = ["123456789012"]\n}\n',
            "sqs.tf": 'resource "aws_sqs_queue" "q" {\n  name = "orders"\n}\n',
            "iam.tf": '''
resource "aws_iam_policy" "p" {
  policy = jsonencode({ Resource = "arn:aws:sqs:us-east-1:999999999999:orders" })
}
''',
        })
        self.assertEqual(sorted(e["detection"] for e in out["entries"]),
                         ["declared", "referenced"])
        declared = next(e for e in out["entries"] if e["detection"] == "declared")
        foreign = next(e for e in out["entries"] if e["detection"] == "referenced")
        self.assertIsNone(declared["account"])
        self.assertIsNone(declared["arn"])
        self.assertEqual(foreign["account"], "999999999999")
        self.assertTrue(any(n.startswith(datastores.CROSS_ACCOUNT_NOTE_PREFIX)
                            for n in foreign["notes"]), foreign["notes"])
        # And it is counted as what it is in the scan notes.
        self.assertTrue(any("1 data service(s) are known only from a literal ARN" in n
                            for n in out["notes"]), out["notes"])

    def test_a_same_account_arn_folds_and_fills_the_account(self):
        out = _harvest({
            "providers.tf": 'provider "aws" {\n  allowed_account_ids = ["123456789012"]\n}\n',
            "sqs.tf": 'resource "aws_sqs_queue" "q" {\n  name = "orders"\n}\n',
            "iam.tf": '''
resource "aws_iam_policy" "p" {
  policy = jsonencode({ Resource = "arn:aws:sqs:us-east-1:123456789012:orders" })
}
''',
        })
        self.assertEqual(len(out["entries"]), 1)
        entry = out["entries"][0]
        self.assertEqual((entry["detection"], entry["account"], entry["region"]),
                         ("declared", "123456789012", "us-east-1"))
        self.assertEqual(entry["arn"], "arn:aws:sqs:us-east-1:123456789012:orders")


class ReviewRoundOneTest(unittest.TestCase):
    """Regressions from the first adversarial review of this change."""

    def test_a_folded_arn_is_not_called_undeclared_in_the_scan_notes(self):
        """The note used to be built before the merge, so a bucket that
        folded onto its own declaration was still named as provisioned
        outside the repository — beside a summary saying zero were."""
        out = _harvest({
            "s3.tf": 'resource "aws_s3_bucket" "archive" {\n  bucket = "acme-invoice-archive"\n}\n',
            "iam.tf": '''
resource "aws_iam_policy" "p" {
  policy = jsonencode({ Resource = "arn:aws:s3:::acme-invoice-archive/*" })
}
''',
        })
        self.assertEqual([e["detection"] for e in out["entries"]], ["declared"])
        self.assertFalse(any("known only from a literal ARN" in n for n in out["notes"]),
                         out["notes"])
        self.assertFalse(any("states no account of its own" in n for n in out["notes"]))

    def test_a_fold_with_no_stated_account_keeps_the_uncertainty_on_the_entry(self):
        """The files state no account of their own, so an ARN naming `carts`
        in account 123456789012 may be this estate's table or a same-named
        one elsewhere. It folds — the common case is that it is the same
        table — but the declared entry keeps the note saying it might not
        be, and the scan counts it among the ones it cannot tell."""
        out = _harvest({
            "ddb.tf": 'resource "aws_dynamodb_table" "carts" {\n  name = "carts"\n}\n',
            "iam.tf": '''
resource "aws_iam_policy" "p" {
  policy = jsonencode({ Resource = [
    "arn:aws:dynamodb:us-east-1:123456789012:table/carts",
    "arn:aws:dynamodb:us-east-1:123456789012:table/orders",
  ] })
}
''',
        })
        found = _by_id(out["entries"])
        self.assertEqual(found["carts"]["detection"], "declared")
        self.assertEqual(found["carts"]["account"], "123456789012")
        self.assertTrue(any(n.startswith(datastores.UNKNOWN_ACCOUNT_NOTE_PREFIX)
                            for n in found["carts"]["notes"]), found["carts"]["notes"])
        self.assertNotIn(datastores.REFERENCED_NOTE, found["carts"]["notes"])
        note = next(n for n in out["notes"] if "whether they are cross-account is not knowable" in n)
        self.assertIn("2 data service(s)", note)
        self.assertIn("dynamodb carts", note)
        self.assertIn("dynamodb orders", note)
        # With the account stated, the same estate folds cleanly and says nothing.
        out = _harvest({
            "providers.tf": 'provider "aws" {\n  allowed_account_ids = ["123456789012"]\n}\n',
            "ddb.tf": 'resource "aws_dynamodb_table" "carts" {\n  name = "carts"\n}\n',
            "iam.tf": '''
resource "aws_iam_policy" "p" {
  policy = jsonencode({ Resource = "arn:aws:dynamodb:us-east-1:123456789012:table/carts" })
}
''',
        })
        self.assertFalse(any(n.startswith((datastores.UNKNOWN_ACCOUNT_NOTE_PREFIX,
                                           datastores.CROSS_ACCOUNT_NOTE_PREFIX))
                             for e in out["entries"] for n in e["notes"]))
        self.assertFalse(any("states no account of its own" in n for n in out["notes"]))

    def test_a_commented_arn_inside_a_heredoc_records_nothing(self):
        out = _harvest({"apps.tf": '''
resource "helm_release" "orders" {
  name   = "orders"
  chart  = "./charts/orders"
  values = [<<EOT
    invoiceBucket: acme-invoice-archive
    # legacy, decommissioned 2024: arn:aws:s3:::acme-old-archive
    queueArn: arn:aws:sqs:us-east-1:123456789012:orders-events
  EOT
  ]
}
'''})
        self.assertEqual([e["identifier"] for e in out["entries"]], ["orders-events"])
        self.assertEqual([c["workload"] for c in out["entries"][0]["consumers"]],
                         ["orders"])

    def test_a_wildcard_region_and_a_literal_one_are_one_table(self):
        out = _harvest({"iam.tf": '''
resource "aws_iam_policy" "p" {
  policy = jsonencode({ Resource = [
    "arn:aws:dynamodb:*:*:table/carts",
    "arn:aws:dynamodb:us-east-1:123456789012:table/carts",
  ] })
}
'''})
        self.assertEqual(len(out["entries"]), 1)
        entry = out["entries"][0]
        self.assertEqual((entry["region"], entry["account"]), ("us-east-1", "123456789012"))

    def test_two_accounts_with_one_name_stay_two_tables(self):
        out = _harvest({"iam.tf": '''
resource "aws_iam_policy" "p" {
  policy = jsonencode({ Resource = [
    "arn:aws:dynamodb:us-east-1:111111111111:table/carts",
    "arn:aws:dynamodb:us-east-1:222222222222:table/carts",
  ] })
}
'''})
        self.assertEqual(sorted(e["account"] for e in out["entries"]),
                         ["111111111111", "222222222222"])

    def test_a_malformed_arn_is_noted_not_dropped(self):
        out = _harvest({"iam.tf": '''
resource "aws_iam_policy" "p" {
  policy = jsonencode({ Resource = "arn:aws:sqs:us-east-1:ACCOUNT:orders" })
}
'''})
        self.assertEqual(out["entries"], [])
        self.assertTrue(any(n.startswith("1 ARN- or endpoint-shaped string(s)") for n in out["notes"]),
                        out["notes"])

    def test_a_fold_drops_the_referenced_sides_dispositional_notes(self):
        """A declared Redis with snapshots persists; the ARN-only view of the
        same cache had no arguments to read and graded it rebuildable. After
        the fold the entry must not say both."""
        out = _harvest({
            "cache.tf": '''
resource "aws_elasticache_replication_group" "sessions" {
  replication_group_id     = "sessions"
  snapshot_retention_limit = 5
}
''',
            "iam.tf": '''
resource "aws_iam_policy" "p" {
  policy = jsonencode({ Resource = "arn:aws:elasticache:us-east-1:123456789012:replicationgroup:sessions" })
}
''',
        })
        self.assertEqual(len(out["entries"]), 1)
        entry = out["entries"][0]
        self.assertEqual(entry["disposition"], "migrate")
        self.assertFalse(any("graded as a rebuildable cache" in n for n in entry["notes"]), entry["notes"])
        self.assertNotIn(datastores.REFERENCED_NOTE, entry["notes"])


class ReviewRoundTwoTest(unittest.TestCase):
    """Regressions from the second adversarial review round."""

    def test_an_example_arn_in_a_variable_description_records_nothing(self):
        out = _harvest({"variables.tf": '''
variable "table_arn" {
  description = "ARN of the orders table, e.g. arn:aws:dynamodb:us-east-1:123456789012:table/my-table"
  type        = string
}
variable "bucket" {
  description = <<-EOT
    The archive bucket, for example arn:aws:s3:::example-archive.
  EOT
}
output "queue" {
  description = "arn:aws:sqs:us-east-1:123456789012:example"
  value       = "arn:aws:sqs:us-east-1:123456789012:example-out"
}
variable "invoice_bucket_arn" {
  description = "see arn:aws:s3:::not-this-one"
  default     = "arn:aws:s3:::acme-invoice-archive"
}
resource "aws_iam_policy" "p" {
  description = "grants arn:aws:s3:::described-only"
  policy      = jsonencode({ Resource = "arn:aws:s3:::acme-invoice-archive" })
}
'''})
        # Descriptions are prose; a variable's default and an output's value
        # are configuration, and a literal ARN in them is a real dependency.
        self.assertEqual(sorted(e["identifier"] for e in out["entries"]),
                         ["acme-invoice-archive", "example-out"])
        archive = _by_id(out["entries"])["acme-invoice-archive"]
        self.assertCountEqual(archive["evidence"], ["variables.tf"])

    def test_an_unclosed_provider_block_is_noted(self):
        out = _harvest({
            "providers.tf": 'provider "aws" {\n  allowed_account_ids = ["123456789012"]\n',
            "iam.tf": '''
resource "aws_iam_policy" "p" {
  policy = jsonencode({ Resource = "arn:aws:sqs:us-east-1:999999999999:orders" })
}
''',
        })
        self.assertTrue(any("a provider block is never closed" in n for n in out["notes"]),
                        out["notes"])

    def test_a_db_instance_arn_is_not_a_clusters_twin(self):
        out = _harvest({
            "rds.tf": '''
resource "aws_rds_cluster" "orders" {
  cluster_identifier = "orders"
  engine             = "aurora-postgresql"
}
''',
            "iam.tf": '''
resource "aws_iam_policy" "p" {
  policy = jsonencode({ Resource = "arn:aws:rds:us-east-1:123456789012:db:orders" })
}
''',
        })
        self.assertEqual(sorted((e["service"], e["detection"]) for e in out["entries"]),
                         [("aurora", "declared"), ("rds", "referenced")])

    def test_a_cluster_arn_is_a_refined_clusters_twin(self):
        out = _harvest({
            "rds.tf": '''
resource "aws_rds_cluster" "orders" {
  cluster_identifier = "orders"
  engine             = "aurora-postgresql"
}
''',
            "iam.tf": '''
resource "aws_iam_policy" "p" {
  policy = jsonencode({ Resource = "arn:aws:rds:us-east-1:123456789012:cluster:orders" })
}
''',
        })
        self.assertEqual([(e["service"], e["detection"]) for e in out["entries"]],
                         [("aurora", "declared")])
        self.assertEqual(out["entries"][0]["arn"],
                         "arn:aws:rds:us-east-1:123456789012:cluster:orders")

    def test_two_spellings_stay_one_entry_when_the_name_has_two_declared_twins(self):
        out = _harvest({
            "envs/dev/main.tf": 'resource "aws_dynamodb_table" "t" {\n  name = "orders"\n}\n',
            "envs/prod/main.tf": 'resource "aws_dynamodb_table" "t" {\n  name = "orders"\n}\n',
            "iam.tf": '''
resource "aws_iam_policy" "p" {
  policy = jsonencode({ Resource = [
    "arn:aws:dynamodb:*:*:table/orders",
    "arn:aws:dynamodb:us-east-1:123456789012:table/orders",
  ] })
}
''',
        })
        referenced = [e for e in out["entries"] if e["detection"] == "referenced"]
        self.assertEqual(len(referenced), 1)
        self.assertEqual(referenced[0]["region"], "us-east-1")
        self.assertTrue(any("2 declared entries share this name" in n
                            for n in referenced[0]["notes"]))


class ReviewRoundThreeTest(unittest.TestCase):
    """Regressions from the third adversarial review round."""

    def test_a_console_secret_arn_folds_onto_its_declaration(self):
        out = _harvest({
            "sm.tf": 'resource "aws_secretsmanager_secret" "db" {\n  name = "prod/db"\n}\n',
            "apps.tf": '''
resource "helm_release" "orders" {
  name  = "orders"
  chart = "./charts/orders"
  set {
    name  = "secretArn"
    value = "arn:aws:secretsmanager:us-west-2:123456789012:secret:prod/db-AbC1dE"
  }
}
''',
        })
        self.assertEqual(len(out["entries"]), 1)
        entry = out["entries"][0]
        self.assertEqual((entry["detection"], entry["identifier"]), ("declared", "prod/db"))
        self.assertEqual([c["workload"] for c in entry["consumers"]], ["orders"])

    def test_a_single_segment_ssm_name_folds_whether_or_not_it_starts_with_a_slash(self):
        out = _harvest({
            "ssm.tf": 'resource "aws_ssm_parameter" "pw" {\n  name = "/db-password"\n}\n',
            "iam.tf": '''
resource "aws_iam_policy" "p" {
  policy = jsonencode({ Resource = "arn:aws:ssm:us-east-1:123456789012:parameter/db-password" })
}
''',
        })
        self.assertEqual([e["detection"] for e in out["entries"]], ["declared"])

    def test_two_declared_twins_do_not_leave_the_undeclared_note_standing(self):
        out = _harvest({
            "envs/dev/main.tf": 'resource "aws_sqs_queue" "q" {\n  name = "orders"\n}\n',
            "envs/prod/main.tf": 'resource "aws_sqs_queue" "q" {\n  name = "orders"\n}\n',
            "iam.tf": '''
resource "aws_iam_policy" "p" {
  policy = jsonencode({ Resource = "arn:aws:sqs:us-east-1:123456789012:orders" })
}
''',
        })
        referenced = next(e for e in out["entries"] if e["detection"] == "referenced")
        self.assertNotIn(datastores.REFERENCED_NOTE, referenced["notes"])
        self.assertTrue(any("2 declared entries share this name" in n
                            for n in referenced["notes"]))

    def test_the_referenced_note_allows_for_a_variable_built_declaration(self):
        out = _harvest({
            "s3.tf": 'resource "aws_s3_bucket" "archive" {\n  bucket = "${var.env}-invoice-archive"\n}\n',
            "iam.tf": '''
resource "aws_iam_policy" "p" {
  policy = jsonencode({ Resource = "arn:aws:s3:::acme-invoice-archive" })
}
''',
        })
        referenced = next(e for e in out["entries"] if e["detection"] == "referenced")
        self.assertIn(datastores.REFERENCED_NOTE, referenced["notes"])
        self.assertIn("declared here under a name built from variables",
                      datastores.REFERENCED_NOTE)
        self.assertTrue(any("under a variable-built name" in n for n in out["notes"]))

    def test_a_provider_account_stated_by_expression_earns_a_caveat(self):
        out = _harvest({
            "envs/dev/providers.tf": '''
provider "aws" {
  assume_role {
    role_arn = "arn:aws:iam::111111111111:role/deploy"
  }
}
''',
            "envs/shared/providers.tf": '''
provider "aws" {
  alias = "shared"
  assume_role {
    role_arn = var.shared_role_arn
  }
}
''',
            "iam.tf": '''
resource "aws_iam_policy" "p" {
  policy = jsonencode({ Resource = "arn:aws:sqs:us-east-1:222222222222:orders" })
}
''',
        })
        entry = out["entries"][0]
        # Not a cross-account verdict: one provider is silent about its
        # account, so 222222222222 may be the estate's own.
        self.assertFalse(any(n.startswith(datastores.CROSS_ACCOUNT_NOTE_PREFIX)
                             for n in entry["notes"]))
        note = next(n for n in entry["notes"]
                    if n.startswith(datastores.UNKNOWN_ACCOUNT_NOTE_PREFIX))
        self.assertIn("states only 111111111111 as its own", note)
        self.assertTrue(any("does not state its account literally" in n
                            for n in out["notes"]), out["notes"])


class ReviewRoundFourTest(unittest.TestCase):
    """Regressions from the fourth adversarial review round."""

    def test_a_default_provider_beside_an_aliased_one_does_not_make_the_estate_foreign(self):
        """`provider "aws" { region = ... }` names no account. With one
        aliased provider that does, the account set is non-empty but
        incomplete, and an ARN in the estate's own (unstated) account must
        fold onto its declaration with the doubt noted — not be refused as
        cross-account."""
        out = _harvest({
            "providers.tf": '''
provider "aws" {
  region = "us-east-1"
}
provider "aws" {
  alias = "audit"
  assume_role {
    role_arn = "arn:aws:iam::111111111111:role/audit"
  }
}
''',
            "sqs.tf": 'resource "aws_sqs_queue" "q" {\n  name = "orders"\n}\n',
            "iam.tf": '''
resource "aws_iam_policy" "p" {
  policy = jsonencode({ Resource = "arn:aws:sqs:us-east-1:222222222222:orders" })
}
''',
        })
        self.assertEqual(len(out["entries"]), 1)
        entry = out["entries"][0]
        self.assertEqual((entry["detection"], entry["account"]), ("declared", "222222222222"))
        self.assertFalse(any(n.startswith(datastores.CROSS_ACCOUNT_NOTE_PREFIX)
                             for n in entry["notes"]))
        self.assertTrue(any(n.startswith(datastores.UNKNOWN_ACCOUNT_NOTE_PREFIX)
                            for n in entry["notes"]), entry["notes"])

    def test_a_complete_account_set_still_gives_a_cross_account_verdict(self):
        out = _harvest({
            "providers.tf": '''
provider "aws" {
  allowed_account_ids = ["111111111111"]
}
provider "aws" {
  alias               = "audit"
  allowed_account_ids = ["333333333333"]
}
''',
            "sqs.tf": 'resource "aws_sqs_queue" "q" {\n  name = "orders"\n}\n',
            "iam.tf": '''
resource "aws_iam_policy" "p" {
  policy = jsonencode({ Resource = "arn:aws:sqs:us-east-1:222222222222:orders" })
}
''',
        })
        self.assertEqual(sorted(e["detection"] for e in out["entries"]),
                         ["declared", "referenced"])
        foreign = next(e for e in out["entries"] if e["detection"] == "referenced")
        self.assertTrue(any(n.startswith(datastores.CROSS_ACCOUNT_NOTE_PREFIX)
                            for n in foreign["notes"]))

    def test_a_declared_secret_ending_in_six_alphanumerics_is_not_stripped(self):
        out = _harvest({
            "sm.tf": 'resource "aws_secretsmanager_secret" "m" {\n  name = "acme/orders-master"\n}\n',
            "iam.tf": '''
resource "aws_iam_policy" "p" {
  policy = jsonencode({ Resource = "arn:aws:secretsmanager:us-east-1:123456789012:secret:acme/orders-??????" })
}
''',
        })
        self.assertEqual(sorted((e["detection"], e["identifier"]) for e in out["entries"]),
                         [("declared", "acme/orders-master"), ("referenced", "acme/orders")])

    def test_re_merging_a_two_twins_arn_does_not_restore_the_undeclared_note(self):
        inventory = {"data_dependencies": []}
        declared = [datastores._entry("sqs", "orders", f"envs/{env}/main.tf",
                                      {"name": "orders"}, None, [],
                                      address="aws_sqs_queue.q") for env in ("dev", "prod")]
        referenced = datastores._referenced_entry(
            datastores.find_arns('"arn:aws:sqs:us-east-1:123456789012:orders"')[0], "iam.tf")
        datastores.merge_datastores(inventory, declared + [copy.deepcopy(referenced)])
        datastores.merge_datastores(inventory, [copy.deepcopy(referenced)])
        standing = next(e for e in inventory["data_dependencies"]
                        if e["detection"] == "referenced")
        self.assertNotIn(datastores.REFERENCED_NOTE, standing["notes"])
        self.assertEqual(len(inventory["data_dependencies"]), 3)


class ReviewRoundFiveTest(unittest.TestCase):
    """Regressions from the fifth adversarial review round."""

    SAME_NAME = {
        "rds.tf": 'resource "aws_db_instance" "orders" {\n  identifier = "orders"\n}\n',
        "iam.tf": '''
resource "aws_iam_policy" "p" {
  policy = jsonencode({ Resource = "arn:aws:rds:us-east-1:222222222222:db:orders" })
}
''',
    }

    def test_an_unclosed_provider_block_withholds_the_cross_account_verdict(self):
        out = _harvest(dict(self.SAME_NAME, **{"providers.tf": '''
provider "aws" {
  allowed_account_ids = ["111111111111"]
}
provider "aws" {
  alias               = "finance"
  allowed_account_ids = ["222222222222"]
'''}))
        # The unread block held the account; no verdict, and the fold happens
        # with the doubt noted.
        self.assertEqual([e["detection"] for e in out["entries"]], ["declared"])
        self.assertTrue(any(n.startswith(datastores.UNKNOWN_ACCOUNT_NOTE_PREFIX)
                            for n in out["entries"][0]["notes"]))

    def test_an_excluded_or_unreadable_file_withholds_the_cross_account_verdict(self):
        files = dict(self.SAME_NAME, **{
            "providers.tf": 'provider "aws" {\n  allowed_account_ids = ["111111111111"]\n}\n',
            "finance/providers.tf": 'provider "aws" {\n  alias = "finance"\n  allowed_account_ids = ["222222222222"]\n}\n',
        })
        out = _harvest(files, scope={"excluded": ["finance/**"]})
        self.assertEqual([e["detection"] for e in out["entries"]], ["declared"])
        self.assertFalse(any(n.startswith(datastores.CROSS_ACCOUNT_NOTE_PREFIX)
                             for n in out["entries"][0]["notes"]))
        self.assertTrue(any("were not read to the end" in n for n in out["notes"]),
                        out["notes"])

    def test_two_same_named_arns_in_different_accounts_fold_onto_nothing(self):
        out = _harvest({
            "sqs.tf": 'resource "aws_sqs_queue" "orders" {\n  name = "orders"\n}\n',
            "a.tf": 'resource "aws_iam_policy" "a" {\n  policy = jsonencode({ Resource = "arn:aws:sqs:us-east-1:111111111111:orders" })\n}\n',
            "b.tf": 'resource "aws_iam_policy" "b" {\n  policy = jsonencode({ Resource = "arn:aws:sqs:us-east-1:222222222222:orders" })\n}\n',
        })
        self.assertEqual(sorted(e["detection"] for e in out["entries"]),
                         ["declared", "referenced", "referenced"])
        declared = next(e for e in out["entries"] if e["detection"] == "declared")
        self.assertIsNone(declared["account"])
        for entry in out["entries"]:
            if entry["detection"] == "referenced":
                self.assertTrue(any(n.startswith(datastores.AMBIGUOUS_ACCOUNT_NOTE_PREFIX)
                                    for n in entry["notes"]), entry["notes"])


class ReviewRoundSixTest(unittest.TestCase):
    """Regressions from the sixth adversarial review round."""

    def test_a_cluster_arn_does_not_fold_onto_a_declared_instance(self):
        out = _harvest({
            "rds.tf": 'resource "aws_db_instance" "orders" {\n  identifier = "orders"\n}\n',
            "iam.tf": 'resource "aws_iam_policy" "p" {\n  policy = jsonencode({ Resource = "arn:aws:rds:us-east-1:123456789012:cluster:orders" })\n}\n',
        })
        self.assertEqual(sorted((e["service"], e["detection"]) for e in out["entries"]),
                         [("aurora", "referenced"), ("rds", "declared")])

    def test_a_cluster_arn_folds_onto_a_multi_az_db_cluster(self):
        """`aws_rds_cluster` with a non-Aurora engine refines to `rds`, but
        it is still a cluster, and the block type says so."""
        out = _harvest({
            "rds.tf": '''
resource "aws_rds_cluster" "orders" {
  cluster_identifier = "orders"
  engine             = "postgres"
}
''',
            "iam.tf": 'resource "aws_iam_policy" "p" {\n  policy = jsonencode({ Resource = "arn:aws:rds:us-east-1:123456789012:cluster:orders" })\n}\n',
        })
        self.assertEqual([(e["service"], e["detection"]) for e in out["entries"]],
                         [("rds", "declared")])

    def test_an_unclosed_block_comment_leaves_the_account_set_unknown(self):
        out = _harvest({
            "providers.tf": '''
provider "aws" { allowed_account_ids = ["111111111111"] }
/* retired 2024 — the finance account
provider "aws" { alias = "finance"
  allowed_account_ids = ["222222222222"] }
''',
            "sqs.tf": 'resource "aws_sqs_queue" "orders" {\n  name = "orders"\n}\n',
            "iam.tf": 'resource "aws_iam_policy" "p" {\n  policy = jsonencode({ Resource = "arn:aws:sqs:us-east-1:222222222222:orders" })\n}\n',
        })
        # No verdict either way: the retired account must not read as the
        # estate's own, and 222… must not be called foreign on that basis.
        entry = out["entries"][0]
        self.assertTrue(any(n.startswith(datastores.UNKNOWN_ACCOUNT_NOTE_PREFIX)
                            for n in entry["notes"]), entry["notes"])
        self.assertFalse(any(n.startswith(datastores.CROSS_ACCOUNT_NOTE_PREFIX)
                             for n in entry["notes"]))

    def test_the_address_of_two_arn_spellings_does_not_depend_on_file_order(self):
        wildcard = 'resource "aws_iam_policy" "w" {\n  policy = jsonencode({ Resource = "arn:aws:dynamodb:*:*:table/orders" })\n}\n'
        literal = 'resource "aws_iam_policy" "l" {\n  policy = jsonencode({ Resource = "arn:aws:dynamodb:us-east-1:123456789012:table/orders" })\n}\n'
        first = _harvest({"a.tf": wildcard, "b.tf": literal})["entries"]
        second = _harvest({"a.tf": literal, "b.tf": wildcard})["entries"]
        self.assertEqual(len(first), 1)
        self.assertEqual(first[0]["address"], second[0]["address"])
        self.assertEqual(first[0]["address"],
                         "arn:aws:dynamodb:us-east-1:123456789012:table/orders")

    def test_a_pathological_file_is_scanned_in_linear_time(self):
        import time
        text = "arn:aws:s3:" * 100000
        started = time.monotonic()
        found = datastores.find_arns(text)
        self.assertLess(time.monotonic() - started, 2.0)
        self.assertTrue(found)
        self.assertEqual({f.kind for f in found}, {"malformed"})


class ReviewRoundSevenTest(unittest.TestCase):
    """Regressions from the seventh adversarial review round."""

    def test_a_partly_literal_account_list_is_not_a_complete_set(self):
        out = _harvest({
            "providers.tf": 'provider "aws" {\n  allowed_account_ids = ["111111111111", var.secondary_account_id]\n}\n',
            "sqs.tf": 'resource "aws_sqs_queue" "orders" {\n  name = "orders"\n}\n',
            "iam.tf": 'resource "aws_iam_policy" "p" {\n  policy = jsonencode({ Resource = "arn:aws:sqs:us-east-1:222222222222:orders" })\n}\n',
        })
        self.assertEqual([e["detection"] for e in out["entries"]], ["declared"])
        self.assertTrue(any(n.startswith(datastores.UNKNOWN_ACCOUNT_NOTE_PREFIX)
                            for n in out["entries"][0]["notes"]))

    def test_an_expression_role_arn_is_not_a_stated_account(self):
        accounts, unstated = datastores.provider_accounts(
            'provider "aws" {\n  assume_role {\n    role_arn = var.deploy_role_arn\n  }\n}\n',
            0, 80)
        self.assertEqual(accounts, set())
        self.assertTrue(unstated)

    def test_no_account_note_when_no_arn_carries_an_account(self):
        out = _harvest({
            "envs/prod/providers.tf": 'provider "aws" {\n  allowed_account_ids = ["111111111111"]\n}\n',
            "modules/eks/providers.tf": 'provider "aws" {\n  region = var.region\n}\n',
            "s3.tf": 'resource "aws_s3_bucket" "media" {\n  bucket = "acme-media"\n}\n',
            "iam.tf": 'resource "aws_iam_policy" "p" {\n  policy = jsonencode({ Resource = "arn:aws:s3:::acme-invoice-archive" })\n}\n',
        })
        self.assertFalse(any("may be incomplete" in n for n in out["notes"]), out["notes"])

    def test_two_partitions_are_two_buckets(self):
        out = _harvest({"iam.tf": '''
resource "aws_iam_policy" "p" {
  policy = jsonencode({ Resource = ["arn:aws:s3:::shared-logs", "arn:aws-cn:s3:::shared-logs"] })
}
'''})
        self.assertEqual(len(out["entries"]), 2)
        note = next(n for n in out["entries"][0]["notes"]
                    if n.startswith(datastores.AMBIGUOUS_ACCOUNT_NOTE_PREFIX))
        self.assertIn("aws-cn/", note)
        self.assertIn("aws/", note)

    def test_an_over_long_token_leaves_a_short_note(self):
        found = datastores.find_arns("arn:aws:s3:::" + "x" * 5000)
        self.assertEqual([f.kind for f in found], ["malformed"])
        self.assertLess(len(found[0].token), 80)


class ReviewRoundEightTest(unittest.TestCase):
    """Regressions from the eighth adversarial review round."""

    def test_two_regions_with_one_name_and_a_declaration_fold_onto_nothing(self):
        out = _harvest({
            "main.tf": 'resource "aws_dynamodb_table" "orders" {\n  name = "orders"\n}\n',
            "policy-a.tf": 'resource "aws_iam_policy" "a" {\n  policy = jsonencode({ Resource = "arn:aws:dynamodb:us-east-1:111111111111:table/orders" })\n}\n',
            "policy-b.tf": 'resource "aws_iam_policy" "b" {\n  policy = jsonencode({ Resource = "arn:aws:dynamodb:us-west-2:111111111111:table/orders" })\n}\n',
        })
        self.assertEqual(sorted(e["detection"] for e in out["entries"]),
                         ["declared", "referenced", "referenced"])
        declared = next(e for e in out["entries"] if e["detection"] == "declared")
        self.assertIsNone(declared["region"])
        # And the same with the files the other way round.
        swapped = _harvest({
            "main.tf": 'resource "aws_dynamodb_table" "orders" {\n  name = "orders"\n}\n',
            "policy-0.tf": 'resource "aws_iam_policy" "b" {\n  policy = jsonencode({ Resource = "arn:aws:dynamodb:us-west-2:111111111111:table/orders" })\n}\n',
            "policy-a.tf": 'resource "aws_iam_policy" "a" {\n  policy = jsonencode({ Resource = "arn:aws:dynamodb:us-east-1:111111111111:table/orders" })\n}\n',
        })
        self.assertEqual(sorted(e["detection"] for e in swapped["entries"]),
                         ["declared", "referenced", "referenced"])

    def test_two_half_wildcard_spellings_compose_one_full_handle(self):
        out = _harvest({"iam.tf": '''
resource "aws_iam_policy" "p" {
  policy = jsonencode({ Resource = [
    "arn:aws:dynamodb:us-east-1:*:table/carts",
    "arn:aws:dynamodb:*:123456789012:table/carts",
  ] })
}
'''})
        self.assertEqual(len(out["entries"]), 1)
        entry = out["entries"][0]
        self.assertEqual(entry["address"], "arn:aws:dynamodb:us-east-1:123456789012:table/carts")
        self.assertEqual(entry["arn"], entry["address"])
        self.assertEqual((entry["region"], entry["account"]), ("us-east-1", "123456789012"))

    def test_an_aws_assigned_id_says_it_will_not_merge_on_its_own(self):
        out = _harvest({
            "efs.tf": 'resource "aws_efs_file_system" "data" {\n  creation_token = "acme-data"\n}\n',
            "iam.tf": 'resource "aws_iam_policy" "p" {\n  policy = jsonencode({ Resource = "arn:aws:elasticfilesystem:us-east-1:111111111111:file-system/fs-0abc123" })\n}\n',
        })
        referenced = next(e for e in out["entries"] if e["detection"] == "referenced")
        self.assertIn(datastores._ASSIGNED_ID_NOTE, referenced["notes"])


class ReviewRoundTenTest(unittest.TestCase):
    """Regressions from the tenth adversarial review round."""

    QUEUE = 'resource "aws_sqs_queue" "orders" {\n  name = "orders"\n}\n'
    FRANKFURT = 'resource "aws_iam_policy" "p" {\n  policy = jsonencode({ Resource = "arn:aws:sqs:eu-west-1:111111111111:orders" })\n}\n'

    def test_an_arn_in_another_region_does_not_fold_onto_a_declaration(self):
        out = _harvest({
            "providers.tf": 'provider "aws" {\n  region = "us-east-1"\n}\n',
            "sqs.tf": self.QUEUE, "iam.tf": self.FRANKFURT,
        })
        self.assertEqual(sorted(e["detection"] for e in out["entries"]),
                         ["declared", "referenced"])
        declared = next(e for e in out["entries"] if e["detection"] == "declared")
        self.assertIsNone(declared["region"])
        foreign = next(e for e in out["entries"] if e["detection"] == "referenced")
        self.assertTrue(any(n.startswith(datastores.CROSS_REGION_NOTE_PREFIX)
                            for n in foreign["notes"]), foreign["notes"])

    def test_a_same_region_arn_still_folds(self):
        out = _harvest({
            "providers.tf": 'provider "aws" {\n  region = "eu-west-1"\n}\n',
            "sqs.tf": self.QUEUE, "iam.tf": self.FRANKFURT,
        })
        self.assertEqual([e["detection"] for e in out["entries"]], ["declared"])

    def test_a_region_stated_by_expression_gives_no_region_verdict(self):
        out = _harvest({
            "providers.tf": 'provider "aws" {\n  region = var.region\n}\n',
            "sqs.tf": self.QUEUE, "iam.tf": self.FRANKFURT,
        })
        self.assertEqual([e["detection"] for e in out["entries"]], ["declared"])
        self.assertFalse(any(n.startswith(datastores.CROSS_REGION_NOTE_PREFIX)
                             for n in out["entries"][0]["notes"]))

    def test_an_arn_in_another_partition_does_not_fold_onto_a_declaration(self):
        out = _harvest({
            "providers.tf": 'provider "aws" {\n  region = "us-east-1"\n}\n',
            "s3.tf": 'resource "aws_s3_bucket" "logs" {\n  bucket = "acme-logs"\n}\n',
            "iam.tf": 'resource "aws_iam_policy" "p" {\n  policy = jsonencode({ Resource = "arn:aws-cn:s3:::acme-logs" })\n}\n',
        })
        self.assertEqual(sorted(e["detection"] for e in out["entries"]),
                         ["declared", "referenced"])

    def test_the_ambiguity_note_reads_as_three_axes(self):
        out = _harvest({
            "a.tf": 'resource "aws_iam_policy" "a" {\n  policy = jsonencode({ Resource = "arn:aws:sqs:us-east-1:111111111111:orders" })\n}\n',
            "b.tf": 'resource "aws_iam_policy" "b" {\n  policy = jsonencode({ Resource = "arn:aws:sqs:us-east-1:222222222222:orders" })\n}\n',
        })
        note = next(n for e in out["entries"] for n in e["notes"]
                    if n.startswith(datastores.AMBIGUOUS_ACCOUNT_NOTE_PREFIX))
        self.assertIn("more than one AWS account, region or partition (aws/us-east-1/111111111111",
                      note)

    def test_an_s3_arn_that_is_not_a_bucket_is_ignored(self):
        for token in ("arn:aws:s3:us-east-1:123456789012:storage-lens/default",
                      "arn:aws:s3:us-east-1:123456789012:job/abcd-1234",
                      "arn:aws:s3:us-east-1:123456789012:access-grants/default",
                      "arn:aws:s3:us-east-1:123456789012:accesspoint/shared"):
            with self.subTest(token=token):
                self.assertEqual(datastores.find_arns(f'"{token}"'), [])
        self.assertEqual([f.identifier for f in datastores.find_arns('"arn:aws:s3:::acme-media/x"')],
                         ["acme-media"])


class ReviewRoundElevenTest(unittest.TestCase):
    """Regressions from the eleventh adversarial review round."""

    QUEUE = 'resource "aws_sqs_queue" "orders" {\n  name = "orders"\n}\n'
    FRANKFURT = 'resource "aws_iam_policy" "p" {\n  policy = jsonencode({ Resource = "arn:aws:sqs:eu-west-1:111111111111:orders" })\n}\n'

    def test_a_truncated_provider_file_that_is_also_account_silent_withholds_the_region_verdict(self):
        out = _harvest({
            "providers.tf": 'provider "aws" {\n  region = "us-east-1"\n}\n\nlocals {\n  bootstrap = <<SCRIPT\necho hi\n',
            "sqs.tf": self.QUEUE, "iam.tf": self.FRANKFURT,
        })
        # The tail of providers.tf was never read; no verdict, so the fold
        # happens and the doubt is on the entry.
        self.assertEqual([e["detection"] for e in out["entries"]], ["declared"])
        self.assertFalse(any(n.startswith(datastores.CROSS_REGION_NOTE_PREFIX)
                             for n in out["entries"][0]["notes"]))
        self.assertTrue(any(n.startswith(datastores.UNKNOWN_REGION_NOTE_PREFIX)
                            for n in out["entries"][0]["notes"]), out["entries"][0]["notes"])

    def test_a_fold_with_an_unverifiable_region_says_so_on_the_entry(self):
        out = _harvest({
            "providers.tf": 'provider "aws" {\n  region              = var.region\n  allowed_account_ids = ["111111111111"]\n}\n',
            "sqs.tf": self.QUEUE, "iam.tf": self.FRANKFURT,
        })
        self.assertEqual([e["detection"] for e in out["entries"]], ["declared"])
        entry = out["entries"][0]
        self.assertEqual(entry["region"], "eu-west-1")
        self.assertTrue(any(n.startswith(datastores.UNKNOWN_REGION_NOTE_PREFIX)
                            for n in entry["notes"]), entry["notes"])
        # With the region verified, the same fold says nothing.
        out = _harvest({
            "providers.tf": 'provider "aws" {\n  region              = "eu-west-1"\n  allowed_account_ids = ["111111111111"]\n}\n',
            "sqs.tf": self.QUEUE, "iam.tf": self.FRANKFURT,
        })
        self.assertFalse(any(n.startswith(datastores.UNKNOWN_REGION_NOTE_PREFIX)
                             for n in out["entries"][0]["notes"]))

    def test_two_spellings_of_one_undeclared_secret_are_one_entry(self):
        out = _harvest({
            "iam.tf": 'resource "aws_iam_policy" "p" {\n  policy = jsonencode({ Resource = "arn:aws:secretsmanager:us-east-1:111111111111:secret:prod/db-??????" })\n}\n',
            "apps.tf": '''
resource "helm_release" "orders" {
  name  = "orders"
  chart = "./charts/orders"
  set {
    name  = "secretArn"
    value = "arn:aws:secretsmanager:us-east-1:111111111111:secret:prod/db-AbC1dE"
  }
}
''',
        })
        self.assertEqual(len(out["entries"]), 1)
        entry = out["entries"][0]
        self.assertEqual(entry["identifier"], "prod/db")
        self.assertEqual(entry["address"], "arn:aws:secretsmanager:us-east-1:111111111111:secret:prod/db")
        self.assertEqual([c["workload"] for c in entry["consumers"]], ["orders"])
        # Two secrets that merely share a prefix stay two.
        out = _harvest({"iam.tf": 'resource "aws_iam_policy" "p" {\n  policy = jsonencode({ Resource = ["arn:aws:secretsmanager:us-east-1:111111111111:secret:app-master", "arn:aws:secretsmanager:us-east-1:111111111111:secret:app-config"] })\n}\n'})
        self.assertEqual(len(out["entries"]), 2)

    def test_the_partition_note_reads_as_a_partition(self):
        out = _harvest({
            "providers.tf": 'provider "aws" {\n  region = "us-east-1"\n}\n',
            "s3.tf": 'resource "aws_s3_bucket" "logs" {\n  bucket = "acme-logs"\n}\n',
            "iam.tf": 'resource "aws_iam_policy" "p" {\n  policy = jsonencode({ Resource = "arn:aws-cn:s3:::acme-logs" })\n}\n',
        })
        foreign = next(e for e in out["entries"] if e["detection"] == "referenced")
        note = next(n for n in foreign["notes"]
                    if n.startswith(datastores.CROSS_PARTITION_NOTE_PREFIX))
        self.assertTrue(note.startswith("in AWS partition aws-cn, while"), note)


class ReviewRoundTwelveTest(unittest.TestCase):
    """Regressions from the twelfth adversarial review round."""

    def test_the_region_description_is_on_the_data_dependency_field(self):
        import json
        path = os.path.join(os.path.dirname(datastores.__file__), "..", "..", "..",
                            "dag", "server", "schema", "inventory.json")
        with open(path) as handle:
            schema = json.load(handle)
        dd = schema["properties"]["data_dependencies"]["items"]["properties"]
        self.assertIn("folds onto it", dd["region"]["description"])
        clusters = schema["properties"]["clusters"]["items"]["properties"]
        self.assertNotIn("description", clusters["region"])

    def test_a_word_suffix_does_not_merge_two_secrets(self):
        declared = 'resource "aws_secretsmanager_secret" "db" {\n  name = "acme/db"\n}\n'
        out = _harvest({
            "sm.tf": declared,
            "iam.tf": 'resource "aws_iam_policy" "p" {\n  policy = jsonencode({ Resource = "arn:aws:secretsmanager:us-east-1:111111111111:secret:acme/db-reader-??????" })\n}\n',
        })
        self.assertEqual(sorted((e["detection"], e["identifier"]) for e in out["entries"]),
                         [("declared", "acme/db"), ("referenced", "acme/db-reader")])
        reader = next(e for e in out["entries"] if e["detection"] == "referenced")
        self.assertTrue(any(n.startswith(datastores.SECRET_SUFFIX_NOTE_PREFIX)
                            for n in reader["notes"]), reader["notes"])
        # The console form, with a random-looking suffix, still folds.
        out = _harvest({
            "sm.tf": declared,
            "iam.tf": 'resource "aws_iam_policy" "p" {\n  policy = jsonencode({ Resource = "arn:aws:secretsmanager:us-east-1:111111111111:secret:acme/db-AbC1dE" })\n}\n',
        })
        self.assertEqual([e["detection"] for e in out["entries"]], ["declared"])
        # Two undeclared secrets differing by a word suffix stay two.
        out = _harvest({"iam.tf": 'resource "aws_iam_policy" "p" {\n  policy = jsonencode({ Resource = ["arn:aws:secretsmanager:us-east-1:111111111111:secret:acme/db-??????", "arn:aws:secretsmanager:us-east-1:111111111111:secret:acme/db-reader-??????"] })\n}\n'})
        self.assertEqual(sorted(e["identifier"] for e in out["entries"]),
                         ["acme/db", "acme/db-reader"])

    def test_a_region_tag_inside_default_tags_is_not_the_providers_region(self):
        out = _harvest({
            "providers.tf": '''
provider "aws" {
  region = var.aws_region
  default_tags {
    tags = {
      region = "us-east-1"
    }
  }
}
''',
            "sqs.tf": 'resource "aws_sqs_queue" "orders" {\n  name = "orders"\n}\n',
            "iam.tf": 'resource "aws_iam_policy" "p" {\n  policy = jsonencode({ Resource = "arn:aws:sqs:eu-west-1:111111111111:orders" })\n}\n',
        })
        # No verdict: the provider's own region is an expression.
        self.assertEqual([e["detection"] for e in out["entries"]], ["declared"])
        self.assertFalse(any(n.startswith(datastores.CROSS_REGION_NOTE_PREFIX)
                             for n in out["entries"][0]["notes"]))
        self.assertTrue(any(n.startswith(datastores.UNKNOWN_REGION_NOTE_PREFIX)
                            for n in out["entries"][0]["notes"]))

    def test_a_partition_fold_under_unverifiable_regions_says_so(self):
        out = _harvest({
            "providers.tf": 'provider "aws" {}\n',
            "s3.tf": 'resource "aws_s3_bucket" "logs" {\n  bucket = "acme-logs"\n}\n',
            "iam.tf": 'resource "aws_iam_policy" "p" {\n  policy = jsonencode({ Resource = "arn:aws-cn:s3:::acme-logs" })\n}\n',
        })
        self.assertEqual([e["detection"] for e in out["entries"]], ["declared"])
        note = next(n for n in out["entries"][0]["notes"]
                    if n.startswith(datastores.UNKNOWN_REGION_NOTE_PREFIX))
        self.assertIn("partition aws-cn", note)

    def test_the_declared_arriving_later_path_also_notes_an_unverified_fold(self):
        inventory = {"data_dependencies": []}
        referenced = datastores._referenced_entry(
            datastores.find_arns('"arn:aws:sqs:eu-west-1:111111111111:orders"')[0], "iam.tf")
        datastores.merge_datastores(inventory, [referenced], regions_verified=False)
        datastores.merge_datastores(inventory, [datastores._entry(
            "sqs", "orders", "sqs.tf", {"name": "orders"}, None, [],
            address="aws_sqs_queue.orders")], regions_verified=False)
        self.assertEqual(len(inventory["data_dependencies"]), 1)
        self.assertTrue(any(n.startswith(datastores.UNKNOWN_REGION_NOTE_PREFIX)
                            for n in inventory["data_dependencies"][0]["notes"]))


class ReviewRoundThirteenTest(unittest.TestCase):
    """Regressions from the thirteenth adversarial review round."""

    CONSOLE = ('resource "aws_iam_policy" "a" {\n  policy = jsonencode({ Resource = '
               '"arn:aws:secretsmanager:us-east-1:111111111111:secret:prod/db-AbC1dE" })\n}\n')
    WILDCARD = ('resource "aws_iam_policy" "b" {\n  policy = jsonencode({ Resource = '
                '"arn:aws:secretsmanager:*:*:secret:prod/db" })\n}\n')
    # An unterminated heredoc: everything below it is unread, so a `//`
    # comment there is restored from raw content unless the span is dropped.
    TRUNCATED = ('resource "aws_iam_policy" "p" {\n'
                 '  policy = <<EOF\n'
                 '{"Resource": "arn:aws:s3:::live-archive"}\n'
                 '}\n'
                 '// retired 2024: '
                 'arn:aws:secretsmanager:us-east-1:111111111111:secret:prod/legacy-db\n')

    def test_the_secret_handle_does_not_depend_on_walk_order(self):
        first = _harvest({"a_policy.tf": self.CONSOLE,
                          "b_policy.tf": self.WILDCARD})["entries"]
        second = _harvest({"a_policy.tf": self.WILDCARD,
                           "b_policy.tf": self.CONSOLE})["entries"]
        self.assertEqual(len(first), 1)
        self.assertEqual(len(second), 1)
        self.assertEqual(first[0]["address"], second[0]["address"])
        self.assertEqual(first[0]["identifier"], "prod/db")
        # The handle states the region and account the entry knows,
        # rather than the wildcard spelling's stars.
        self.assertEqual(
            first[0]["address"],
            "arn:aws:secretsmanager:us-east-1:111111111111:secret:prod/db")
        self.assertEqual((first[0]["region"], first[0]["account"]),
                         ("us-east-1", "111111111111"))
        self.assertNotIn("*", first[0]["arn"])

    def test_an_arn_below_an_unterminated_heredoc_is_not_read(self):
        out = _harvest({"m.tf": self.TRUNCATED})
        self.assertEqual([e["identifier"] for e in out["entries"]], [])
        self.assertTrue(any("never closed" in n for n in out["notes"]), out["notes"])


class ReviewRoundFifteenTest(unittest.TestCase):
    """Regressions from the fifteenth adversarial review round."""

    DECLARED = ('resource "aws_dynamodb_table" "carts" {\n'
                '  name = "carts"\n}\n')
    WILDCARD = ('resource "aws_iam_policy" "w" {\n  policy = jsonencode({ Resource = '
                '"arn:aws:dynamodb:*:*:table/carts" })\n}\n')
    LITERAL = ('resource "aws_iam_policy" "l" {\n  policy = jsonencode({ Resource = '
               '"arn:aws:dynamodb:us-east-1:123456789012:table/carts" })\n}\n')

    def test_a_declarations_arn_composes_across_both_spellings(self):
        first = _harvest({"a.tf": self.WILDCARD, "b.tf": self.LITERAL,
                          "ddb.tf": self.DECLARED})["entries"]
        second = _harvest({"a.tf": self.LITERAL, "b.tf": self.WILDCARD,
                           "ddb.tf": self.DECLARED})["entries"]
        for entries in (first, second):
            self.assertEqual(len(entries), 1)
            entry = entries[0]
            self.assertEqual(entry["detection"], "declared")
            self.assertEqual(entry["address"], "aws_dynamodb_table.carts")
            # The handle the review keys on states what the entry knows.
            self.assertEqual(
                entry["arn"],
                "arn:aws:dynamodb:us-east-1:123456789012:table/carts")
            self.assertEqual((entry["region"], entry["account"]),
                             ("us-east-1", "123456789012"))

    def test_a_db_instance_arn_does_not_fold_onto_a_multi_az_db_cluster(self):
        out = _harvest({
            "rds.tf": ('resource "aws_rds_cluster" "orders" {\n'
                       '  cluster_identifier = "orders"\n'
                       '  engine             = "postgres"\n}\n'),
            "iam.tf": ('resource "aws_iam_policy" "p" {\n  policy = jsonencode({ '
                       'Resource = "arn:aws:rds:us-east-1:123456789012:db:orders" })\n}\n'),
        })
        # Both refine to `rds`, but one is a cluster and one an instance.
        self.assertEqual(sorted(e["detection"] for e in out["entries"]),
                         ["declared", "referenced"])

    def test_a_db_instance_arn_still_folds_onto_a_declared_instance(self):
        out = _harvest({
            "rds.tf": ('resource "aws_db_instance" "orders" {\n'
                       '  identifier = "orders"\n}\n'),
            "iam.tf": ('resource "aws_iam_policy" "p" {\n  policy = jsonencode({ '
                       'Resource = "arn:aws:rds:us-east-1:123456789012:db:orders" })\n}\n'),
        })
        self.assertEqual([e["detection"] for e in out["entries"]], ["declared"])


class ReviewRoundSixteenTest(unittest.TestCase):
    """Regressions from the sixteenth adversarial review round."""

    def test_a_multi_line_description_does_not_leak_its_example_arn(self):
        out = _harvest({"variables.tf": (
            'variable "archive_bucket_arn" {\n'
            '  description = format(\n'
            '    "%s (e.g. arn:aws:s3:::acme-invoice-archive)", "the archive bucket")\n'
            '}\n'
            'variable "queue" {\n'
            '  description = [\n'
            '    "arn:aws:sqs:us-east-1:123456789012:example",\n'
            '  ]\n'
            '  default     = "arn:aws:sqs:us-east-1:123456789012:orders-real"\n'
            '}\n')})
        # The two descriptions are prose; the default is configuration.
        self.assertEqual([e["identifier"] for e in out["entries"]], ["orders-real"])

    def test_a_declared_instance_and_cluster_of_one_name_are_two_databases(self):
        out = _harvest({"rds.tf": (
            'resource "aws_db_instance" "orders" {\n'
            '  identifier = "orders"\n}\n'
            'resource "aws_rds_cluster" "orders" {\n'
            '  cluster_identifier = "orders"\n'
            '  engine             = "postgres"\n}\n')})
        self.assertEqual(sorted(e["address"] for e in out["entries"]),
                         ["aws_db_instance.orders", "aws_rds_cluster.orders"])

    def test_one_cluster_declared_across_two_files_is_still_one(self):
        """The cluster/instance discriminator must not break the documented
        split-declaration merge: both blocks are the same cluster."""
        out = _harvest({
            "cluster.tf": ('resource "aws_rds_cluster" "c" {\n'
                           '  cluster_identifier = "acme-orders"\n'
                           '  engine             = "postgres"\n}\n'),
            "more.tf": ('resource "aws_rds_cluster" "c" {\n'
                        '  cluster_identifier = "acme-orders"\n'
                        '  engine             = "postgres"\n}\n'),
        })
        self.assertEqual(len(out["entries"]), 1)
        self.assertCountEqual(out["entries"][0]["evidence"],
                              ["cluster.tf", "more.tf"])


class ReviewRoundSeventeenTest(unittest.TestCase):
    """Regressions from the seventeenth adversarial review round."""

    def test_a_heredoc_inside_a_description_value_does_not_leak(self):
        out = _harvest({"variables.tf": (
            'variable "archive_bucket_arn" {\n'
            '  description = join("\\n", [\n'
            '    <<-EOT\n'
            '    Example: arn:aws:s3:::acme-invoice-archive\n'
            '    EOT\n'
            '  ])\n'
            '}\n'
            'resource "aws_iam_policy" "p" {\n'
            '  policy = jsonencode({ Resource = "arn:aws:s3:::acme-real" })\n'
            '}\n')})
        self.assertEqual([e["identifier"] for e in out["entries"]], ["acme-real"])

    def test_a_heredoc_description_on_its_own_line_still_does_not_leak(self):
        out = _harvest({"variables.tf": (
            'variable "bucket" {\n'
            '  description = <<-EOT\n'
            '    Example: arn:aws:s3:::acme-example\n'
            '  EOT\n'
            '}\n')})
        self.assertEqual(out["entries"], [])

    def test_a_policy_heredoc_beside_a_description_is_still_read(self):
        out = _harvest({"iam.tf": (
            'resource "aws_iam_policy" "p" {\n'
            '  description = "grants the archive"\n'
            '  policy = <<EOF\n'
            '{"Resource": "arn:aws:s3:::acme-invoice-archive"}\n'
            'EOF\n'
            '}\n')})
        self.assertEqual([e["identifier"] for e in out["entries"]],
                         ["acme-invoice-archive"])


class ReviewRoundEighteenTest(unittest.TestCase):
    """Regressions from the eighteenth adversarial review round."""

    BARE = ('resource "aws_iam_policy" "a" {\n  policy = jsonencode({ Resource = '
            '"arn:aws:s3:::acme-media" })\n}\n')
    STARRED = ('resource "aws_iam_policy" "b" {\n  policy = jsonencode({ Resource = '
               '"arn:aws:s3:*:*:acme-media" })\n}\n')

    def test_a_starred_and_a_bare_s3_arn_give_one_stable_handle(self):
        first = _harvest({"a.tf": self.BARE, "b.tf": self.STARRED})["entries"]
        second = _harvest({"a.tf": self.STARRED, "b.tf": self.BARE})["entries"]
        self.assertEqual(len(first), 1)
        self.assertEqual(len(second), 1)
        self.assertEqual(first[0]["address"], second[0]["address"])
        # And it is the form AWS emits for a bucket.
        self.assertEqual(first[0]["address"], "arn:aws:s3:::acme-media")

    def test_a_stored_handle_never_carries_a_wildcard_field(self):
        found = datastores.find_arns('"arn:aws:dynamodb:*:*:table/carts"')
        self.assertEqual(found[0].arn, "arn:aws:dynamodb:::table/carts")
        self.assertEqual((found[0].region, found[0].account), (None, None))

    def test_field_composition_is_a_total_order(self):
        for one, other in (("", "*"), ("us-east-1", "*"), ("us-east-1", "")):
            with self.subTest(one=one, other=other):
                self.assertEqual(datastores._better_field(one, other),
                                 datastores._better_field(other, one))


class ReviewRoundNineteenTest(unittest.TestCase):
    """Regressions from the nineteenth adversarial review round."""

    def test_the_msk_policy_wildcard_and_the_concrete_arn_are_one_entry(self):
        policy = ('resource "aws_iam_policy" "a" {\n  policy = jsonencode({ Resource = '
                  '"arn:aws:kafka:us-east-1:111111111111:cluster/orders/*" })\n}\n')
        config = ('resource "kubernetes_config_map" "c" {\n'
                  '  metadata { name = "orders-config" }\n'
                  '  data = { BROKERS = "arn:aws:kafka:us-east-1:111111111111:'
                  'cluster/orders/abcd-1234-5678-9012-ef00" }\n}\n')
        for files in ({"a.tf": policy, "b.tf": config},
                      {"a.tf": config, "b.tf": policy}):
            with self.subTest(order=sorted(files)):
                out = _harvest(files)
                self.assertEqual(len(out["entries"]), 1)
                entry = out["entries"][0]
                self.assertEqual(entry["identifier"], "orders")
                self.assertEqual(entry["address"],
                                 "arn:aws:kafka:us-east-1:111111111111:cluster/orders")
                self.assertNotIn("*", entry["arn"])

    def test_a_wildcard_in_a_sub_resource_never_reaches_a_handle(self):
        for token in ("arn:aws:kafka:us-east-1:111111111111:cluster/orders/*",
                      "arn:aws:mq:us-east-1:111111111111:broker:orders-mq:*"):
            with self.subTest(token=token):
                found = datastores.find_arns(f'"{token}"')
                self.assertTrue(found)
                for match in found:
                    self.assertNotIn("*", match.arn or "")

    def test_an_arn_naming_an_aurora_member_instance_is_not_a_second_database(self):
        out = _harvest({
            "rds.tf": ('resource "aws_rds_cluster" "orders" {\n'
                       '  cluster_identifier = "orders"\n'
                       '  engine             = "aurora-postgresql"\n}\n'
                       'resource "aws_rds_cluster_instance" "one" {\n'
                       '  identifier         = "orders-instance-1"\n'
                       '  cluster_identifier = "orders"\n}\n'),
            "iam.tf": ('resource "aws_iam_policy" "p" {\n  policy = jsonencode({ Resource = '
                       '"arn:aws:rds:us-east-1:111111111111:db:orders-instance-1" })\n}\n'),
        })
        self.assertEqual(sorted((e["service"], e["identifier"]) for e in out["entries"]),
                         [("aurora", "orders"), ("rds", "orders-instance-1")])
        self.assertEqual(_noted_duplicate(out)["identifier"], "orders-instance-1")
        self.assertTrue(any("may name a resource this scan already counts" in n
                            for n in out["notes"]), out["notes"])

    def test_an_arn_naming_a_read_replica_is_not_a_second_database(self):
        out = _harvest({
            "rds.tf": ('resource "aws_db_instance" "primary" {\n'
                       '  identifier = "orders"\n}\n'
                       'resource "aws_db_instance" "replica" {\n'
                       '  identifier          = "orders-replica"\n'
                       '  replicate_source_db = aws_db_instance.primary.identifier\n}\n'),
            "iam.tf": ('resource "aws_iam_policy" "p" {\n  policy = jsonencode({ Resource = '
                       '"arn:aws:rds:us-east-1:111111111111:db:orders-replica" })\n}\n'),
        })
        self.assertEqual(sorted(e["identifier"] for e in out["entries"]),
                         ["orders", "orders-replica"])
        self.assertEqual(_noted_duplicate(out)["identifier"], "orders-replica")

    def test_an_unrelated_instance_arn_is_still_recorded(self):
        out = _harvest({
            "rds.tf": ('resource "aws_rds_cluster" "orders" {\n'
                       '  cluster_identifier = "orders"\n'
                       '  engine             = "aurora-postgresql"\n}\n'
                       'resource "aws_rds_cluster_instance" "one" {\n'
                       '  identifier         = "orders-instance-1"\n'
                       '  cluster_identifier = "orders"\n}\n'),
            "iam.tf": ('resource "aws_iam_policy" "p" {\n  policy = jsonencode({ Resource = '
                       '"arn:aws:rds:us-east-1:111111111111:db:legacy-billing" })\n}\n'),
        })
        self.assertEqual(sorted(e["identifier"] for e in out["entries"]),
                         ["legacy-billing", "orders"])


class ReviewRoundTwentyTest(unittest.TestCase):
    """Regressions from the twentieth adversarial review round: the
    uncounted-primary suppression must key on the child's own name."""

    def test_a_cluster_arn_survives_an_instance_whose_identifier_is_an_expression(self):
        out = _harvest({
            "rds.tf": ('resource "aws_rds_cluster" "orders" {\n'
                       '  cluster_identifier = "orders"\n'
                       '  engine             = "aurora-postgresql"\n}\n'
                       'resource "aws_rds_cluster_instance" "members" {\n'
                       '  count              = 2\n'
                       '  identifier         = "orders-${count.index}"\n'
                       '  cluster_identifier = "orders"\n}\n'),
            "iam.tf": ('resource "aws_iam_policy" "p" {\n  policy = jsonencode({ Resource = '
                       '"arn:aws:rds:us-east-1:999999999999:cluster:orders" })\n}\n'),
        })
        # The ARN names the CLUSTER. It must not be deleted as if it named
        # the instance, and its account has to reach the section.
        entry = next(e for e in out["entries"] if e["identifier"] == "orders")
        self.assertEqual(entry["account"], "999999999999")
        self.assertFalse(any("may name a resource this scan already counts" in n for n in out["notes"]),
                         out["notes"])

    def test_an_elasticache_group_arn_survives_a_member_with_an_expression_id(self):
        out = _harvest({
            "cache.tf": ('resource "aws_elasticache_cluster" "member" {\n'
                         '  count                = 2\n'
                         '  cluster_id           = "orders-cache-${count.index}"\n'
                         '  replication_group_id = "orders-cache"\n}\n'),
            "iam.tf": ('resource "aws_iam_policy" "p" {\n  policy = jsonencode({ Resource = '
                       '"arn:aws:elasticache:us-east-1:999999999999:'
                       'replicationgroup:orders-cache" })\n}\n'),
        })
        # The group is declared nowhere; the ARN is the only record of it.
        self.assertEqual([(e["service"], e["identifier"]) for e in out["entries"]],
                         [("elasticache", "orders-cache")])

    def test_a_replicas_own_name_still_suppresses_its_arn(self):
        out = _harvest({
            "rds.tf": ('resource "aws_db_instance" "primary" {\n'
                       '  identifier = "orders"\n}\n'
                       'resource "aws_db_instance" "replica" {\n'
                       '  identifier          = "orders-replica"\n'
                       '  db_name             = "orders"\n'
                       '  replicate_source_db = aws_db_instance.primary.identifier\n}\n'),
            "iam.tf": ('resource "aws_iam_policy" "p" {\n  policy = jsonencode({ Resource = '
                       '"arn:aws:rds:us-east-1:111111111111:db:orders-replica" })\n}\n'),
        })
        self.assertEqual(sorted(e["identifier"] for e in out["entries"]),
                         ["orders", "orders-replica"])
        self.assertEqual(_noted_duplicate(out)["identifier"], "orders-replica")

    def test_a_db_name_is_never_taken_for_an_instance_identifier(self):
        """The replica states `db_name = \"billing\"`; an unrelated instance
        called `billing` elsewhere must still be recorded."""
        out = _harvest({
            "rds.tf": ('resource "aws_db_instance" "primary" {\n'
                       '  identifier = "orders"\n}\n'
                       'resource "aws_db_instance" "replica" {\n'
                       '  identifier          = "orders-replica"\n'
                       '  db_name             = "billing"\n'
                       '  replicate_source_db = aws_db_instance.primary.identifier\n}\n'),
            "iam.tf": ('resource "aws_iam_policy" "p" {\n  policy = jsonencode({ Resource = '
                       '"arn:aws:rds:us-east-1:111111111111:db:billing" })\n}\n'),
        })
        self.assertEqual(sorted(e["identifier"] for e in out["entries"]),
                         ["billing", "orders"])


class ReviewRoundTwentyOneTest(unittest.TestCase):
    """Regressions from the twenty-first adversarial review round: the
    suppression key must carry the ARN's resource kind, so a same-named
    child cannot delete its parent."""

    def test_a_cluster_arn_survives_an_instance_of_the_same_name(self):
        out = _harvest({
            "providers.tf": ('provider "aws" {\n  region              = "us-east-1"\n'
                             '  allowed_account_ids = ["111122223333"]\n}\n'),
            "rds.tf": ('resource "aws_rds_cluster" "orders" {\n'
                       '  cluster_identifier = "orders"\n'
                       '  engine             = "aurora-postgresql"\n}\n'
                       'resource "aws_rds_cluster_instance" "writer" {\n'
                       '  identifier         = "orders"\n'
                       '  cluster_identifier = "orders"\n}\n'),
            "iam.tf": ('resource "aws_iam_policy" "p" {\n  policy = jsonencode({ Resource = '
                       '"arn:aws:rds:us-east-1:999988887777:cluster:orders" })\n}\n'),
        })
        # The ARN names the cluster in another account; the instance shares
        # its name but not its kind, so the cross-account entry must survive
        # (and, being foreign, must not fold onto the declaration).
        foreign = next(e for e in out["entries"] if e["detection"] == "referenced")
        self.assertEqual((foreign["identifier"], foreign["account"]),
                         ("orders", "999988887777"))
        self.assertTrue(any(n.startswith(datastores.CROSS_ACCOUNT_NOTE_PREFIX)
                            for n in foreign["notes"]), foreign["notes"])
        self.assertFalse(any("may name a resource this scan already counts" in n for n in out["notes"]),
                         out["notes"])

    def test_an_instance_arn_of_the_same_name_is_still_suppressed(self):
        out = _harvest({
            "rds.tf": ('resource "aws_rds_cluster" "orders" {\n'
                       '  cluster_identifier = "orders"\n'
                       '  engine             = "aurora-postgresql"\n}\n'
                       'resource "aws_rds_cluster_instance" "writer" {\n'
                       '  identifier         = "orders"\n'
                       '  cluster_identifier = "orders"\n}\n'),
            "iam.tf": ('resource "aws_iam_policy" "p" {\n  policy = jsonencode({ Resource = '
                       '"arn:aws:rds:us-east-1:111122223333:db:orders" })\n}\n'),
        })
        self.assertEqual(sorted((e["service"], e["identifier"]) for e in out["entries"]),
                         [("aurora", "orders"), ("rds", "orders")])
        self.assertEqual(_noted_duplicate(out)["service"], "rds")

    def test_a_replication_group_arn_survives_a_member_of_the_same_name(self):
        out = _harvest({
            "cache.tf": ('resource "aws_elasticache_cluster" "member" {\n'
                         '  cluster_id           = "orders-cache"\n'
                         '  replication_group_id = "orders-cache-rg"\n}\n'),
            "iam.tf": ('resource "aws_iam_policy" "p" {\n  policy = jsonencode({ Resource = '
                       '"arn:aws:elasticache:us-east-1:999988887777:'
                       'replicationgroup:orders-cache" })\n}\n'),
        })
        self.assertEqual([(e["service"], e["identifier"]) for e in out["entries"]],
                         [("elasticache", "orders-cache")])

    def test_a_members_own_cluster_arn_is_still_suppressed(self):
        out = _harvest({
            "cache.tf": ('resource "aws_elasticache_replication_group" "g" {\n'
                         '  replication_group_id = "orders-cache-rg"\n}\n'
                         'resource "aws_elasticache_cluster" "member" {\n'
                         '  cluster_id           = "orders-cache-001"\n'
                         '  replication_group_id = "orders-cache-rg"\n}\n'),
            "iam.tf": ('resource "aws_iam_policy" "p" {\n  policy = jsonencode({ Resource = '
                       '"arn:aws:elasticache:us-east-1:111122223333:'
                       'cluster:orders-cache-001" })\n}\n'),
        })
        self.assertEqual(sorted(e["identifier"] for e in out["entries"]),
                         ["orders-cache-001", "orders-cache-rg"])
        self.assertEqual(_noted_duplicate(out)["identifier"], "orders-cache-001")


class ReviewRoundTwentyTwoTest(unittest.TestCase):
    """Regressions from the twenty-second adversarial review round: a
    module block has no resource type, so the replica's kind must come
    from its service."""

    MODULES = ('module "primary" {\n'
               '  source     = "terraform-aws-modules/rds/aws"\n'
               '  identifier = "orders"\n}\n'
               'module "replica" {\n'
               '  source              = "terraform-aws-modules/rds/aws"\n'
               '  identifier          = "orders-replica"\n'
               '  replicate_source_db = module.primary.db_instance_identifier\n}\n')

    def test_a_module_replicas_own_instance_arn_is_suppressed(self):
        out = _harvest({
            "rds.tf": self.MODULES,
            "iam.tf": ('resource "aws_iam_policy" "p" {\n  policy = jsonencode({ Resource = '
                       '"arn:aws:rds:us-east-1:111111111111:db:orders-replica" })\n}\n'),
        })
        self.assertEqual(sorted(e["identifier"] for e in out["entries"]),
                         ["orders", "orders-replica"])
        self.assertEqual(_noted_duplicate(out)["identifier"], "orders-replica")

    def test_a_same_named_cluster_arn_survives_a_module_replica(self):
        out = _harvest({
            "rds.tf": self.MODULES,
            "iam.tf": ('resource "aws_iam_policy" "p" {\n  policy = jsonencode({ Resource = '
                       '"arn:aws:rds:us-east-1:999999999999:cluster:orders-replica" })\n}\n'),
        })
        foreign = next(e for e in out["entries"] if e["detection"] == "referenced")
        self.assertEqual((foreign["service"], foreign["identifier"], foreign["account"]),
                         ("aurora", "orders-replica", "999999999999"))
        self.assertFalse(any("may name a resource this scan already counts" in n for n in out["notes"]),
                         out["notes"])

    def test_a_module_aurora_replicas_cluster_arn_is_suppressed(self):
        out = _harvest({
            "rds.tf": ('module "primary" {\n'
                       '  source = "terraform-aws-modules/rds-aurora/aws"\n'
                       '  name   = "orders"\n}\n'
                       'module "replica" {\n'
                       '  source                        = "terraform-aws-modules/rds-aurora/aws"\n'
                       '  name                          = "orders-dr"\n'
                       '  replication_source_identifier = module.primary.cluster_arn\n}\n'),
            "iam.tf": ('resource "aws_iam_policy" "p" {\n  policy = jsonencode({ Resource = '
                       '"arn:aws:rds:us-east-1:111111111111:cluster:orders-dr" })\n}\n'),
        })
        self.assertEqual(sorted(e["identifier"] for e in out["entries"]),
                         ["orders", "orders-dr"])
        self.assertEqual(_noted_duplicate(out)["identifier"], "orders-dr")


class ReviewRoundTwentyThreeTest(unittest.TestCase):
    """Regressions from the twenty-third adversarial review round: a
    resource the verdicts place in another account, region or partition
    is never suppressed by a same-named child declared here."""

    LOCAL_REPLICA = ('resource "aws_db_instance" "primary" {\n'
                     '  identifier = "orders"\n}\n'
                     'resource "aws_db_instance" "replica" {\n'
                     '  identifier          = "orders-ro"\n'
                     '  replicate_source_db = aws_db_instance.primary.identifier\n}\n')

    def test_a_cross_account_arn_is_not_suppressed_by_a_local_replica(self):
        out = _harvest({
            "providers.tf": ('provider "aws" {\n  region              = "us-east-1"\n'
                             '  allowed_account_ids = ["111111111111"]\n}\n'),
            "rds.tf": self.LOCAL_REPLICA,
            "iam.tf": ('resource "aws_iam_policy" "p" {\n  policy = jsonencode({ Resource = '
                       '"arn:aws:rds:us-east-1:999999999999:db:orders-ro" })\n}\n'),
        })
        foreign = next(e for e in out["entries"] if e["detection"] == "referenced")
        self.assertEqual((foreign["identifier"], foreign["account"]),
                         ("orders-ro", "999999999999"))
        self.assertTrue(any(n.startswith(datastores.CROSS_ACCOUNT_NOTE_PREFIX)
                            for n in foreign["notes"]), foreign["notes"])
        self.assertFalse(any("may name a resource this scan already counts" in n for n in out["notes"]),
                         out["notes"])

    def test_a_cross_region_arn_is_not_suppressed_by_a_local_member(self):
        out = _harvest({
            "providers.tf": ('provider "aws" {\n  region              = "us-east-1"\n'
                             '  allowed_account_ids = ["111111111111"]\n}\n'),
            "cache.tf": ('resource "aws_elasticache_replication_group" "g" {\n'
                         '  replication_group_id = "carts"\n}\n'
                         'resource "aws_elasticache_cluster" "member" {\n'
                         '  cluster_id           = "carts-001"\n'
                         '  replication_group_id = "carts"\n}\n'),
            "iam.tf": ('resource "aws_iam_policy" "p" {\n  policy = jsonencode({ Resource = '
                       '"arn:aws:elasticache:eu-west-1:111111111111:cluster:carts-001" })\n}\n'),
        })
        foreign = next(e for e in out["entries"] if e["detection"] == "referenced")
        self.assertEqual(foreign["identifier"], "carts-001")
        self.assertTrue(any(n.startswith(datastores.CROSS_REGION_NOTE_PREFIX)
                            for n in foreign["notes"]), foreign["notes"])

    def test_a_same_account_arn_is_still_suppressed(self):
        out = _harvest({
            "providers.tf": ('provider "aws" {\n  region              = "us-east-1"\n'
                             '  allowed_account_ids = ["111111111111"]\n}\n'),
            "rds.tf": self.LOCAL_REPLICA,
            "iam.tf": ('resource "aws_iam_policy" "p" {\n  policy = jsonencode({ Resource = '
                       '"arn:aws:rds:us-east-1:111111111111:db:orders-ro" })\n}\n'),
        })
        self.assertEqual(sorted(e["identifier"] for e in out["entries"]),
                         ["orders", "orders-ro"])
        self.assertEqual(_noted_duplicate(out)["identifier"], "orders-ro")


class ReviewRoundTwentyFourTest(unittest.TestCase):
    """Regressions from the twenty-fourth adversarial review round."""

    LOCAL_REPLICA = ('resource "aws_db_instance" "primary" {\n'
                     '  identifier = "orders"\n}\n'
                     'resource "aws_db_instance" "replica" {\n'
                     '  identifier          = "orders-ro"\n'
                     '  replicate_source_db = aws_db_instance.primary.identifier\n}\n')

    def test_a_foreign_account_arn_survives_the_default_provider_shape(self):
        """No `allowed_account_ids` — the ordinary estate — so no verdict can
        be given. The ARN is kept with the duplicate note rather than
        deleted, because the files cannot say whether it is the local
        replica or a same-named database somewhere else."""
        out = _harvest({
            "providers.tf": 'provider "aws" {\n  region = "us-east-1"\n}\n',
            "rds.tf": self.LOCAL_REPLICA,
            "iam.tf": ('resource "aws_iam_policy" "p" {\n  policy = jsonencode({ Resource = '
                       '"arn:aws:rds:us-east-1:999999999999:db:orders-ro" })\n}\n'),
        })
        kept = _noted_duplicate(out)
        self.assertEqual((kept["identifier"], kept["account"]),
                         ("orders-ro", "999999999999"))

    def test_an_excluded_file_does_not_cost_the_entry(self):
        out = _harvest({
            "providers.tf": ('provider "aws" {\n  region              = "us-east-1"\n'
                             '  allowed_account_ids = ["111111111111"]\n}\n'),
            "rds.tf": self.LOCAL_REPLICA,
            "unused.tf": 'variable "spare" {\n  type = string\n}\n',
            "iam.tf": ('resource "aws_iam_policy" "p" {\n  policy = jsonencode({ Resource = '
                       '"arn:aws:rds:eu-west-1:111111111111:db:orders-ro" })\n}\n'),
        }, scope={"excluded": ["unused.tf"]})
        self.assertEqual(_noted_duplicate(out)["identifier"], "orders-ro")

    def test_the_duplicate_note_names_the_arn_so_it_can_be_acted_on(self):
        out = _harvest({
            "rds.tf": self.LOCAL_REPLICA,
            "iam.tf": ('resource "aws_iam_policy" "p" {\n  policy = jsonencode({ Resource = '
                       '"arn:aws:rds:us-east-1:111111111111:db:orders-ro" })\n}\n'),
        })
        note = next(n for n in out["notes"]
                    if "may name a resource this scan already counts" in n)
        self.assertIn("arn:aws:rds:us-east-1:111111111111:db:orders-ro", note)

    def test_a_declared_secrets_handle_does_not_depend_on_walk_order(self):
        declared = ('resource "aws_secretsmanager_secret" "db" {\n'
                    '  name = "prod/db"\n}\n')
        console = ('resource "aws_iam_policy" "a" {\n  policy = jsonencode({ Resource = '
                   '"arn:aws:secretsmanager:us-east-1:111111111111:secret:prod/db-AbC1dE" })\n}\n')
        policy = ('resource "aws_iam_policy" "b" {\n  policy = jsonencode({ Resource = '
                  '"arn:aws:secretsmanager:us-east-1:111111111111:secret:prod/db" })\n}\n')
        first = _harvest({"sm.tf": declared, "a.tf": console, "b.tf": policy})["entries"]
        second = _harvest({"sm.tf": declared, "a.tf": policy, "b.tf": console})["entries"]
        self.assertEqual(len(first), 1)
        self.assertEqual(len(second), 1)
        self.assertEqual(first[0]["arn"], second[0]["arn"])
        self.assertEqual(first[0]["arn"],
                         "arn:aws:secretsmanager:us-east-1:111111111111:secret:prod/db")


class ReviewRoundTwentyFiveTest(unittest.TestCase):
    """Regressions from the twenty-fifth adversarial review round: both
    literal views must agree about what counts as prose."""

    def test_an_arn_only_a_description_states_is_named_in_a_scan_note(self):
        out = _harvest({"k8s.tf": (
            'resource "kubernetes_config_map" "orders" {\n'
            '  metadata { name = "orders-config" }\n'
            '  data = {\n'
            '    description = "arn:aws:s3:::acme-invoice-archive"\n'
            '    region      = "us-east-1"\n'
            '  }\n'
            '}\n')})
        # Still not recorded — a description is prose wherever it sits —
        # but no longer silently.
        self.assertEqual(out["entries"], [])
        self.assertTrue(any("appear only in a `description`" in n
                            and "acme-invoice-archive" in n for n in out["notes"]),
                        out["notes"])

    def test_a_real_arn_beside_a_description_earns_no_such_note(self):
        out = _harvest({"iam.tf": (
            'resource "aws_iam_policy" "p" {\n'
            '  description = "grants the archive"\n'
            '  policy      = jsonencode({ Resource = "arn:aws:s3:::acme-real" })\n'
            '}\n')})
        self.assertEqual([e["identifier"] for e in out["entries"]], ["acme-real"])
        self.assertFalse(any("appear only in a `description`" in n
                             for n in out["notes"]), out["notes"])

    def test_the_unknown_region_note_prefix_is_not_the_account_one(self):
        self.assertFalse(
            datastores.UNKNOWN_ACCOUNT_NOTE_PREFIX.startswith(
                datastores.UNKNOWN_REGION_NOTE_PREFIX))
        self.assertFalse(
            datastores.UNKNOWN_REGION_NOTE_PREFIX.startswith(
                datastores.UNKNOWN_ACCOUNT_NOTE_PREFIX))


class ProseScanCostTest(unittest.TestCase):
    """The prose-only ARN comparison is per file, not per ARN in it."""

    def test_a_large_policy_file_with_a_description_stays_linear(self):
        import time
        arns = ", ".join(
            f'"arn:aws:sqs:us-east-1:111111111111:queue-{i}"' for i in range(1200))
        body = ('resource "aws_iam_policy" "p" {\n'
                '  description = "the grant"\n'
                '  policy      = jsonencode({ Resource = [' + arns + '] })\n'
                '}\n')
        started = time.monotonic()
        out = _harvest({"iam.tf": body})
        elapsed = time.monotonic() - started
        self.assertEqual(len(out["entries"]), 1200)
        # Quadratic took ~12s at this size; linear is well under a second.
        self.assertLess(elapsed, 5.0)


class ReviewRoundTwentySevenTest(unittest.TestCase):
    """Regressions from the twenty-seventh adversarial review round."""

    def test_the_prose_note_does_not_claim_an_unread_tail_is_prose(self):
        out = _harvest({"m.tf": (
            'resource "aws_iam_policy" "p" {\n'
            '  description = "the grant"\n'
            '  policy      = <<EOF\n'
            '{"Resource": "arn:aws:s3:::live-archive"}\n'
            '}\n'
            '// below an unterminated heredoc: arn:aws:s3:::tail-bucket\n')})
        # The tail was never read; it must not be reported as prose.
        self.assertFalse(any("appear only in a `description`" in n
                             and "tail-bucket" in n for n in out["notes"]),
                         out["notes"])

    def test_the_merge_is_linear_in_the_number_of_entries(self):
        import time
        entries = [datastores._referenced_entry(
            datastores.find_arns(
                f'"arn:aws:sqs:us-east-1:111111111111:queue-{i}"')[0], "iam.tf")
            for i in range(6000)]
        inventory = {"data_dependencies": []}
        started = time.monotonic()
        datastores.merge_datastores(inventory, entries)
        elapsed = time.monotonic() - started
        self.assertEqual(len(inventory["data_dependencies"]), 6000)
        # Quadratic was seconds at this size; indexed is well under one.
        self.assertLess(elapsed, 2.0)


class ReviewRoundTwentyEightTest(unittest.TestCase):
    """Regressions from the twenty-eighth adversarial review round."""

    def test_an_arn_in_a_description_heredoc_is_still_named_in_a_note(self):
        out = _harvest({"variables.tf": (
            'variable "archive" {\n'
            '  description = <<EOT\n'
            'The archive bucket, e.g. arn:aws:s3:::acme-invoice-archive\n'
            'EOT\n'
            '  type = string\n'
            '}\n')})
        self.assertEqual(out["entries"], [])
        self.assertTrue(any("appear only in a `description`" in n
                            and "acme-invoice-archive" in n for n in out["notes"]),
                        out["notes"])

    def test_the_merge_is_linear_for_console_form_secret_arns(self):
        import time
        entries = [datastores._referenced_entry(
            datastores.find_arns(
                '"arn:aws:secretsmanager:us-east-1:111111111111:secret:'
                f'prod/app{i}-AbC1dE"')[0], "iam.tf")
            for i in range(6000)]
        inventory = {"data_dependencies": []}
        started = time.monotonic()
        datastores.merge_datastores(inventory, entries)
        elapsed = time.monotonic() - started
        self.assertEqual(len(inventory["data_dependencies"]), 6000)
        self.assertLess(elapsed, 2.0)


class ReviewRoundThirtyOneTest(unittest.TestCase):
    """Regressions from the thirty-first adversarial review round."""

    DESCRIPTION = ('variable "archive_arn" {\n'
                   '  description = "The invoice archive, e.g. '
                   'arn:aws:s3:::acme-invoice-archive"\n'
                   '  type        = string\n'
                   '}\n')
    GRANT = ('resource "aws_iam_policy" "p" {\n'
             '  policy = jsonencode({ Statement = [{ Resource = '
             '"arn:aws:s3:::acme-invoice-archive" }] })\n'
             '}\n')

    def test_an_arn_real_in_another_file_is_not_called_prose_only(self):
        out = _harvest({"vars.tf": self.DESCRIPTION, "policy.tf": self.GRANT})
        self.assertEqual([e["identifier"] for e in out["entries"]],
                         ["acme-invoice-archive"])
        self.assertFalse(any("appear only in a `description`" in n
                             for n in out["notes"]), out["notes"])

    def test_an_arn_no_file_grants_is_still_called_prose_only(self):
        out = _harvest({"vars.tf": self.DESCRIPTION})
        self.assertEqual(out["entries"], [])
        self.assertTrue(any("appear only in a `description`" in n
                            and "acme-invoice-archive" in n for n in out["notes"]),
                        out["notes"])


class ReviewRoundThirtyTwoTest(unittest.TestCase):
    """Regressions from the thirty-second adversarial review round."""

    def test_only_a_random_looking_suffix_is_stripped(self):
        for name, bare in (("prod/db-AbC1dE", "prod/db"),
                           ("prod/db-mVCwt7", "prod/db")):
            self.assertEqual(datastores._strip_secret_suffix(name), bare, name)
        for name in ("prod/db-Backup", "prod/db-Master", "prod/db-202401",
                     "prod/db-shard1", "prod/db-2024Q1", "prod/db-reader",
                     "prod/db-BACKUP"):
            self.assertIsNone(datastores._strip_secret_suffix(name), name)

    def test_a_capitalised_word_suffix_is_a_second_secret(self):
        out = _harvest({"iam.tf": (
            'resource "aws_iam_policy" "p" {\n  policy = jsonencode({ Resource = ['
            '"arn:aws:secretsmanager:us-east-1:111111111111:secret:prod/db", '
            '"arn:aws:secretsmanager:us-east-1:111111111111:secret:prod/db-Backup"'
            '] })\n}\n')})
        self.assertEqual(sorted(e["identifier"] for e in out["entries"]),
                         ["prod/db", "prod/db-Backup"])

    def test_a_console_suffix_merge_says_what_it_merged(self):
        out = _harvest({"iam.tf": (
            'resource "aws_iam_policy" "p" {\n  policy = jsonencode({ Resource = ['
            '"arn:aws:secretsmanager:us-east-1:111111111111:secret:prod/db", '
            '"arn:aws:secretsmanager:us-east-1:111111111111:secret:prod/db-AbC1dE"'
            '] })\n}\n')})
        self.assertEqual([e["identifier"] for e in out["entries"]], ["prod/db"])
        self.assertTrue(any(n.startswith(datastores.SECRET_MERGED_NOTE_PREFIX)
                            for n in out["entries"][0]["notes"]),
                        out["entries"][0]["notes"])

    def test_an_interpolated_partition_is_an_expression_not_a_silence(self):
        out = _harvest({"iam.tf": (
            'resource "aws_iam_policy" "p" {\n'
            '  policy = jsonencode({ Statement = [{ Resource = [\n'
            '    "arn:${data.aws_partition.current.partition}:s3:::acme-invoice-archive"\n'
            '  ] }] })\n}\n')})
        self.assertEqual(out["entries"], [])
        self.assertTrue(any("build the resource name from a variable" in n
                            or "ARN(s) or endpoint(s) build" in n
                            for n in out["notes"]), out["notes"])

    def test_a_standalone_entry_gets_the_unverified_region_note(self):
        out = _harvest({
            "providers.tf": ('provider "aws" {\n  region              = var.region\n'
                             '  allowed_account_ids = ["111111111111"]\n}\n'),
            "iam.tf": ('resource "aws_iam_policy" "p" {\n  policy = jsonencode({ Resource = '
                       '"arn:aws:rds:eu-west-1:111111111111:db:standalone" })\n}\n'),
        })
        entry = out["entries"][0]
        self.assertEqual(entry["detection"], "referenced")
        self.assertTrue(any(n.startswith(datastores.UNKNOWN_REGION_NOTE_PREFIX)
                            for n in entry["notes"]), entry["notes"])

    def test_a_value_ends_at_the_closer_of_its_container(self):
        out = _harvest({"m.tf": (
            'resource "aws_iam_policy" "p" {\n'
            '  policy_statements = [{\n'
            '    description = "read the archive" }, '
            '{ resources = ["arn:aws:s3:::acme-invoice-archive"] }]\n'
            '}\n')})
        self.assertEqual([e["identifier"] for e in out["entries"]],
                         ["acme-invoice-archive"])
        self.assertFalse(any("appear only in a `description`" in n
                             for n in out["notes"]), out["notes"])


class ReviewRoundThirtyThreeTest(unittest.TestCase):
    """Regressions from the thirty-third adversarial review round."""

    def test_a_suffix_missing_one_class_is_not_stripped_but_is_noted(self):
        # The accepted cost of never merging two secrets: a real console
        # suffix carrying no digit is left alone, so the section holds two
        # entries for one secret — and says which pair to merge.
        self.assertIsNone(datastores._strip_secret_suffix("prod/orders-db-XsWpFm"))
        out = _harvest({"main.tf": (
            'resource "aws_secretsmanager_secret" "db" {\n'
            '  name = "prod/orders-db"\n}\n'
            'resource "aws_iam_policy" "p" {\n  policy = jsonencode({ Resource = '
            '"arn:aws:secretsmanager:us-east-1:111111111111:secret:'
            'prod/orders-db-XsWpFm" })\n}\n')})
        self.assertEqual(sorted(e["identifier"] for e in out["entries"]),
                         ["prod/orders-db", "prod/orders-db-XsWpFm"])
        noted = [e for e in out["entries"]
                 if e["identifier"] == "prod/orders-db-XsWpFm"][0]
        self.assertTrue(any(n.startswith(datastores.SECRET_SUFFIX_NOTE_PREFIX)
                            for n in noted["notes"]), noted["notes"])

    def test_an_elasticache_cluster_arn_is_not_a_replication_group(self):
        out = _harvest({"main.tf": (
            'resource "aws_elasticache_replication_group" "r" {\n'
            '  replication_group_id = "redis"\n}\n'
            'resource "aws_iam_role_policy" "p" {\n  role = "r"\n  policy = '
            '"arn:aws:elasticache:us-east-1:111111111111:cluster:redis"\n}\n')})
        self.assertEqual(sorted(e["detection"] for e in out["entries"]),
                         ["declared", "referenced"])
        declared = [e for e in out["entries"] if e["detection"] == "declared"][0]
        # The group must not end up holding the standalone cache's ARN, which
        # is the handle a durable correction is keyed on.
        self.assertIsNone(declared.get("arn"))

    def test_an_elasticache_arn_of_the_declared_kind_still_folds(self):
        out = _harvest({"main.tf": (
            'resource "aws_elasticache_replication_group" "r" {\n'
            '  replication_group_id = "redis"\n}\n'
            'resource "aws_iam_role_policy" "p" {\n  role = "r"\n  policy = '
            '"arn:aws:elasticache:us-east-1:111111111111:replicationgroup:redis"'
            '\n}\n')})
        self.assertEqual([e["detection"] for e in out["entries"]], ["declared"])
        self.assertEqual(
            out["entries"][0]["arn"],
            "arn:aws:elasticache:us-east-1:111111111111:replicationgroup:redis")

    def test_a_module_declared_cache_still_folds_either_kind(self):
        # A module's address names the module, not the block type, so the
        # kind is undecidable and the fold is left alone.
        out = _harvest({"main.tf": (
            'module "redis" {\n'
            '  source = "terraform-aws-modules/elasticache/aws"\n'
            '  replication_group_id = "redis"\n}\n'
            'resource "aws_iam_role_policy" "p" {\n  role = "r"\n  policy = '
            '"arn:aws:elasticache:us-east-1:111111111111:cluster:redis"\n}\n')})
        self.assertEqual([e["detection"] for e in out["entries"]], ["declared"])

class ReviewRoundThirtyFiveTest(unittest.TestCase):
    """Regressions from the thirty-fifth adversarial review round."""

    def test_a_description_does_not_blank_the_argument_after_its_comma(self):
        out = _harvest({"main.tf": (
            'locals {\n  grants = [{\n'
            '    description = "orders read path", '
            'resources = ["arn:aws:s3:::acme-archive"]\n'
            '  }]\n}\n')})
        self.assertEqual([e["identifier"] for e in out["entries"]],
                         ["acme-archive"])
        self.assertFalse(any("appear only in a `description`" in n
                             for n in out["notes"]), out["notes"])

    def test_a_top_level_tag_arn_is_recorded_and_unattributed(self):
        # The two literal views differ here on purpose: the harvest records
        # evidence of a dependency, the consumer walk records grants, and a
        # tag on an IAM role is evidence but not a grant.
        out = _harvest({"main.tf": (
            'resource "aws_iam_role" "orders" {\n'
            '  assume_role_policy = jsonencode({})\n'
            '  tags = { legacy_archive = "arn:aws:s3:::acme-tagged-archive" }\n'
            '}\n')})
        entry = [e for e in out["entries"]
                 if e["identifier"] == "acme-tagged-archive"][0]
        self.assertEqual(entry["detection"], "referenced")
        self.assertEqual(entry["consumers"], [])

class ReviewRoundThirtySixTest(unittest.TestCase):
    """Regressions from the thirty-sixth adversarial review round."""

    def test_two_spellings_of_one_arn_agree(self):
        agree = datastores.arn_spellings_agree
        self.assertTrue(agree("arn:aws:sqs:*:*:orders",
                              "arn:aws:sqs:us-east-1:111111111111:orders"))
        self.assertTrue(agree("arn:aws:sqs:::orders",
                              "arn:aws:sqs:us-east-1:111111111111:orders"))
        self.assertTrue(agree(
            "arn:aws:secretsmanager:us-east-1:111111111111:secret:prod/db-AbC1dE",
            "arn:aws:secretsmanager:us-east-1:111111111111:secret:prod/db"))

    def test_two_different_literals_are_two_resources(self):
        agree = datastores.arn_spellings_agree
        self.assertFalse(agree("arn:aws:sqs:eu-west-1:111111111111:orders",
                               "arn:aws:sqs:us-east-1:111111111111:orders"))
        self.assertFalse(agree("arn:aws:sqs:us-east-1:222222222222:orders",
                               "arn:aws:sqs:us-east-1:111111111111:orders"))
        self.assertFalse(agree("arn:aws-cn:s3:::logs", "arn:aws:s3:::logs"))
        self.assertFalse(agree(
            "arn:aws:secretsmanager:us-east-1:111111111111:secret:prod/db-Backup",
            "arn:aws:secretsmanager:us-east-1:111111111111:secret:prod/db"))

    def test_the_handle_really_does_move_between_scans(self):
        # What the widened lookup is for: the same estate scanned twice, the
        # second time with a fully-qualified sighting of the same queue.
        loose = ('resource "aws_iam_role_policy" "a" {\n  role = "r"\n'
                 '  policy = "arn:aws:sqs:*:*:orders"\n}\n')
        exact = ('resource "aws_iam_role_policy" "b" {\n  role = "r"\n'
                 '  policy = "arn:aws:sqs:us-east-1:111111111111:orders"\n}\n')
        first = _harvest({"a.tf": loose})["entries"][0]["address"]
        second = _harvest({"a.tf": loose, "b.tf": exact})["entries"][0]["address"]
        self.assertNotEqual(first, second)
        self.assertTrue(datastores.arn_spellings_agree(first, second))

class ReviewRoundThirtySevenTest(unittest.TestCase):
    """Regressions from the thirty-seventh adversarial review round."""

    _SECRET = "arn:aws:secretsmanager:us-east-1:111111111111:secret:"

    def test_the_secret_tolerance_strips_one_side_only(self):
        # `acme/db-Prod01` and `acme/db-Dev001` are two real secrets whose
        # endings both pass for AWS's random suffix. Reducing both to
        # `acme/db` would let a correction keyed on one land on the other.
        self.assertFalse(datastores.arn_spellings_agree(
            self._SECRET + "acme/db-Prod01", self._SECRET + "acme/db-Dev001"))
        self.assertTrue(datastores.arn_spellings_agree(
            self._SECRET + "acme/db-AbC1dE", self._SECRET + "acme/db"))
        self.assertTrue(datastores.arn_spellings_agree(
            self._SECRET + "acme/db", self._SECRET + "acme/db-AbC1dE"))

    def test_a_minified_description_is_still_prose(self):
        # A generated or hand-minified policy puts the whole statement on one
        # line, where a line-anchored match read neither the prose nor the
        # note and the retired bucket became a first-class dependency.
        out = _harvest({"main.tf": (
            'resource "aws_iam_policy" "p" {\n'
            '  policy = jsonencode({ Statement = [{ description = "we removed '
            'the grant on arn:aws:s3:::retired-bucket last year", Resource = '
            '"arn:aws:s3:::live-bucket" }] })\n}\n')})
        self.assertEqual([e["identifier"] for e in out["entries"]],
                         ["live-bucket"])
        self.assertTrue(any("appear only in a `description`" in n
                            for n in out["notes"]), out["notes"])

    def test_a_same_line_description_does_not_lose_the_depth_count(self):
        # The lookbehind is one character wide, so the `{` still counts
        # towards the depth walk and a nested description stays nested.
        out = _harvest({"main.tf": (
            'resource "aws_iam_policy" "p" {\n'
            '  policy = jsonencode({ Statement = [{ description = "see '
            'arn:aws:s3:::noted-bucket", Resource = "arn:aws:sqs:us-east-1:'
            '111111111111:orders" }] })\n}\n')})
        self.assertEqual(sorted(e["identifier"] for e in out["entries"]),
                         ["orders"])

class ReviewRoundThirtyEightTest(unittest.TestCase):
    """Regressions from the thirty-eighth adversarial review round."""

    _LOOSE = ('resource "aws_iam_policy" "c" {\n  policy = jsonencode({ Resource = '
              '"arn:aws:sqs:*:*:orders" })\n}\n')
    _ONE = ('resource "aws_iam_policy" "a" {\n  policy = jsonencode({ Resource = '
            '"arn:aws:sqs:us-east-1:111111111111:orders" })\n}\n')
    _TWO = ('resource "aws_iam_policy" "b" {\n  policy = jsonencode({ Resource = '
            '"arn:aws:sqs:us-east-1:222222222222:orders" })\n}\n')

    def _arns(self, files):
        return sorted(e["arn"] for e in _harvest(files)["entries"])

    def test_walk_order_does_not_decide_which_account_a_loose_arn_joins(self):
        # Identical contents, different file names. An unstated region and
        # account is compatible with every literal spelling of the name, so
        # whether it folded — and onto WHICH account — used to depend on how
        # many literal siblings had been processed when it arrived.
        first = self._arns({"aa-one.tf": self._ONE, "bb-two.tf": self._TWO,
                            "cc-loose.tf": self._LOOSE})
        second = self._arns({"aa-loose.tf": self._LOOSE, "bb-two.tf": self._TWO,
                             "zz-one.tf": self._ONE})
        self.assertEqual(first, second)
        self.assertEqual(len(first), 3, first)

    def test_a_loose_arn_still_folds_when_there_is_only_one_literal(self):
        entries = _harvest({"aa-loose.tf": self._LOOSE, "zz-one.tf": self._ONE})["entries"]
        self.assertEqual([e["arn"] for e in entries],
                         ["arn:aws:sqs:us-east-1:111111111111:orders"])

    def test_an_ambiguous_loose_arn_says_why_it_stands_alone(self):
        entries = _harvest({"aa-one.tf": self._ONE, "bb-two.tf": self._TWO,
                            "cc-loose.tf": self._LOOSE})["entries"]
        loose = [e for e in entries if e["arn"] == "arn:aws:sqs:::orders"][0]
        self.assertTrue(any(n.startswith(datastores.AMBIGUOUS_ACCOUNT_NOTE_PREFIX)
                            for n in loose["notes"]), loose["notes"])

    def test_a_description_inside_a_quoted_string_blanks_nothing(self):
        # The `{`/`,` anchor can fire inside a string, which the line anchor
        # could not; matching in the mask keeps it out of string contents.
        out = _harvest({"a.tf": (
            'locals {\n  n = "prefix, description = y arn:aws:s3:::lost-one"\n}\n')})
        self.assertEqual([e["identifier"] for e in out["entries"]], ["lost-one"])

class ReviewRoundFortyOneTest(unittest.TestCase):
    """Regressions from the forty-first adversarial review round."""

    def test_two_referenced_elasticache_kinds_stay_apart(self):
        # Both spellings resolve to `elasticache`, so `_same_service`'s
        # equality cannot part them the way it accidentally parts RDS.
        out = _harvest({"a.tf": (
            'resource "aws_iam_policy" "p" {\n  policy = jsonencode({ Resource = ['
            '"arn:aws:elasticache:us-east-1:111111111111:cluster:redis", '
            '"arn:aws:elasticache:us-east-1:111111111111:replicationgroup:redis"'
            '] })\n}\n')})
        self.assertEqual(
            sorted(e["arn"] for e in out["entries"]),
            ["arn:aws:elasticache:us-east-1:111111111111:cluster:redis",
             "arn:aws:elasticache:us-east-1:111111111111:replicationgroup:redis"])

    def test_agreeing_secret_spellings_are_all_flagged(self):
        # The ambiguous-ACCOUNT note groups on (service, identifier) and the
        # secret tolerance matches ACROSS identifiers, so these three carried
        # no flag while `arn_spellings_agree` said the bare form matched both.
        out = _harvest({"a.tf": (
            'resource "aws_iam_policy" "p" {\n  policy = jsonencode({ Resource = ['
            '"arn:aws:secretsmanager:us-east-1:111111111111:secret:app-AbC1dE", '
            '"arn:aws:secretsmanager:eu-west-1:222222222222:secret:app-XyZ2wQ", '
            '"arn:aws:secretsmanager:*:*:secret:app"] })\n}\n')})
        self.assertEqual(len(out["entries"]), 3)
        for entry in out["entries"]:
            self.assertTrue(datastores.spelling_is_ambiguous(entry),
                            entry["identifier"])

    def test_an_unambiguous_entry_is_not_flagged(self):
        out = _harvest({"a.tf": (
            'resource "aws_iam_policy" "p" {\n  policy = jsonencode({ Resource = '
            '"arn:aws:secretsmanager:us-east-1:111111111111:secret:app-AbC1dE" '
            '})\n}\n')})
        self.assertEqual(len(out["entries"]), 1)
        self.assertFalse(datastores.spelling_is_ambiguous(out["entries"][0]))

    def test_two_spellings_that_folded_leave_nothing_flagged(self):
        # One entry cannot be ambiguous with itself, and the fold is the
        # ordinary case the tolerance exists for.
        out = _harvest({"a.tf": (
            'resource "aws_iam_policy" "p" {\n  policy = jsonencode({ Resource = ['
            '"arn:aws:sqs:*:*:orders", '
            '"arn:aws:sqs:us-east-1:111111111111:orders"] })\n}\n')})
        self.assertEqual(len(out["entries"]), 1)
        self.assertFalse(datastores.spelling_is_ambiguous(out["entries"][0]))


class ReviewRoundFortyTwoTest(unittest.TestCase):
    """Regressions from the forty-second adversarial review round."""

    def test_a_folded_declaration_is_flagged_against_a_console_spelling(self):
        # Round 41 flagged agreeing spellings by bucketing on `name_keys`,
        # which reads the IDENTIFIER; the guards compare the name inside the
        # ARN. A fold is where the two diverge: the declaration keeps
        # `acme/db` and carries `secret:acme/db-Prod01`, so the referenced
        # `-Prod01-Xy7Zq2` sat in a bucket the declaration was not in and
        # neither was flagged — while `arn_spellings_agree` said they match.
        out = _harvest({"a.tf": (
            'resource "aws_secretsmanager_secret" "db" {\n'
            '  name = "acme/db"\n}\n'
            'resource "aws_iam_policy" "p" {\n  policy = jsonencode({ Resource = ['
            '"arn:aws:secretsmanager:us-east-1:111111111111:secret:acme/db-Prod01", '
            '"arn:aws:secretsmanager:us-east-1:111111111111:'
            'secret:acme/db-Prod01-Xy7Zq2"] })\n}\n')})
        self.assertEqual(len(out["entries"]), 2)
        self.assertTrue(datastores.arn_spellings_agree(
            *[e["arn"] for e in out["entries"]]))
        for entry in out["entries"]:
            self.assertTrue(datastores.spelling_is_ambiguous(entry),
                            entry["identifier"])

    def test_a_module_cache_takes_one_arn_and_the_other_kind_stands_apart(self):
        # A module address names the module, not the block type, so ONE ARN
        # against it is undecidable and folds. The second is not undecidable:
        # at most one of `cluster:` and `replicationgroup:` is this block.
        # Folds are judged pairwise, so each ARN arrived alone, both were
        # permitted, and the second's ARN was dropped by `_compose_arn`.
        out = _harvest({"a.tf": (
            'module "redis" {\n'
            '  source = "cloudposse/elasticache-redis/aws"\n'
            '  name   = "redis"\n}\n'
            'resource "aws_iam_policy" "p" {\n  policy = jsonencode({ Resource = ['
            '"arn:aws:elasticache:us-east-1:111111111111:cluster:redis", '
            '"arn:aws:elasticache:us-east-1:111111111111:replicationgroup:redis"'
            '] })\n}\n')})
        self.assertEqual(
            sorted(e["arn"] for e in out["entries"]),
            ["arn:aws:elasticache:us-east-1:111111111111:cluster:redis",
             "arn:aws:elasticache:us-east-1:111111111111:replicationgroup:redis"])

    def test_a_module_cache_still_folds_a_single_arn(self):
        # The permission the round-41 change argued for is intact: one ARN
        # against an undecidable declaration folds rather than duplicating.
        out = _harvest({"a.tf": (
            'module "redis" {\n'
            '  source = "cloudposse/elasticache-redis/aws"\n'
            '  name   = "redis"\n}\n'
            'resource "aws_iam_policy" "p" {\n  policy = jsonencode({ Resource = '
            '"arn:aws:elasticache:us-east-1:111111111111:replicationgroup:redis"'
            ' })\n}\n')})
        self.assertEqual(len(out["entries"]), 1)
        self.assertEqual(out["entries"][0]["detection"], "declared")
        self.assertEqual(
            out["entries"][0]["arn"],
            "arn:aws:elasticache:us-east-1:111111111111:replicationgroup:redis")

    def test_two_declared_elasticache_kinds_in_one_directory_stay_apart(self):
        # `_key` gained the cluster/instance discriminator for the RDS family
        # and left ElastiCache keyed on (service, identifier, directory): both
        # blocks map to `elasticache`, `IDENTIFIER_ARGS` reads `cluster_id`
        # and `replication_group_id` alike, so the replication group merged
        # into the standalone cache and left the section a gate reads.
        out = _harvest({"a.tf": (
            'resource "aws_elasticache_cluster" "redis" {\n'
            '  cluster_id = "redis"\n}\n'
            'resource "aws_elasticache_replication_group" "redis" {\n'
            '  replication_group_id = "redis"\n}\n')})
        self.assertEqual(
            sorted(e["address"] for e in out["entries"]),
            ["aws_elasticache_cluster.redis",
             "aws_elasticache_replication_group.redis"])

    def test_one_cache_split_across_two_files_still_merges(self):
        # The discriminator must not split a resource that `_override.tf` or
        # a second declaration extends — the case `_key`'s directory scope
        # exists for.
        out = _harvest({
            "a.tf": ('resource "aws_elasticache_cluster" "redis" {\n'
                     '  cluster_id = "redis"\n}\n'),
            "override.tf": ('resource "aws_elasticache_cluster" "redis" {\n'
                            '  cluster_id = "redis"\n  engine = "redis"\n}\n')})
        self.assertEqual(len(out["entries"]), 1)


class ReviewRoundFortyThreeTest(unittest.TestCase):
    """Regressions from the forty-third adversarial review round."""

    _S = "arn:aws:secretsmanager:us-east-1:111111111111:secret:"

    def _declared_and(self, *names):
        return _harvest({"a.tf": (
            'resource "aws_secretsmanager_secret" "db" {\n  name = "acme/db"\n}\n'
            'resource "aws_iam_policy" "p" {\n  policy = jsonencode({ Resource = ['
            + ", ".join(f'"{self._S}{n}"' for n in names) + '] })\n}\n')})

    def _referenced(self, *names):
        return _harvest({"a.tf": (
            'resource "aws_iam_policy" "p" {\n  policy = jsonencode({ Resource = ['
            + ", ".join(f'"{self._S}{n}"' for n in names) + '] })\n}\n')})

    def test_a_declaration_absorbs_one_console_form_and_refuses_a_second(self):
        # A secret has one suffix, so at most one console spelling is the
        # declared secret's. Judged pairwise each matched the declaration on
        # its own, `_fold` reduced both to the bare name, and two secrets
        # became one entry with no note — the ElastiCache hole, on the one
        # service whose disposition gates.
        out = self._declared_and("acme/db-Prod01", "acme/db-Dev001")
        self.assertEqual(len(out["entries"]), 2)
        declared = [e for e in out["entries"] if e["detection"] == "declared"]
        apart = [e for e in out["entries"] if e["detection"] == "referenced"]
        self.assertEqual((len(declared), len(apart)), (1, 1))
        self.assertIn(declared[0]["console_form"], ("acme/db-Prod01", "acme/db-Dev001"))
        self.assertNotEqual(apart[0]["identifier"], declared[0]["console_form"])
        self.assertTrue(any(n.startswith(datastores.SECRET_OTHER_FORM_NOTE_PREFIX)
                            for n in apart[0]["notes"]), apart[0]["notes"])
        for entry in out["entries"]:
            self.assertTrue(datastores.spelling_is_ambiguous(entry), entry["identifier"])

    def test_which_console_form_folds_does_not_depend_on_walk_order(self):
        def shape(out):
            return sorted((e["detection"], e["identifier"], e.get("console_form"))
                          for e in out["entries"])
        self.assertEqual(shape(self._declared_and("acme/db-Prod01", "acme/db-Dev001")),
                         shape(self._declared_and("acme/db-Dev001", "acme/db-Prod01")))

    def test_a_third_console_form_stands_apart_too(self):
        out = self._declared_and("acme/db-Prod01", "acme/db-Dev001", "acme/db-Tst991")
        self.assertEqual(len(out["entries"]), 3)
        self.assertEqual(sum(e["detection"] == "declared" for e in out["entries"]), 1)

    def test_the_policy_form_and_one_console_form_still_fold_onto_a_declaration(self):
        # The ordinary case the tolerance exists for is intact: `-??????` and
        # one console form are two spellings of the declared secret.
        out = self._declared_and("acme/db-??????", "acme/db-AbC1dE")
        self.assertEqual(len(out["entries"]), 1)
        self.assertEqual(out["entries"][0]["console_form"], "acme/db-AbC1dE")
        self.assertFalse(datastores.spelling_is_ambiguous(out["entries"][0]))

    def test_the_same_console_form_under_two_region_spellings_still_folds(self):
        out = _harvest({"a.tf": (
            'resource "aws_secretsmanager_secret" "db" {\n  name = "acme/db"\n}\n'
            'resource "aws_iam_policy" "p" {\n  policy = jsonencode({ Resource = ['
            f'"{self._S}acme/db-AbC1dE", '
            '"arn:aws:secretsmanager:*:*:secret:acme/db-AbC1dE"] })\n}\n')})
        self.assertEqual(len(out["entries"]), 1)
        self.assertEqual(out["entries"][0]["console_form"], "acme/db-AbC1dE")

    def test_two_console_forms_do_not_meet_through_the_bare_form(self):
        # The milder instance: with no declaration the bare form bridged the
        # two suffixed ones and all three merged — noisily, but into one.
        out = self._referenced("acme/db", "acme/db-Prod01", "acme/db-Dev001")
        self.assertEqual(len(out["entries"]), 2)
        merged = [e for e in out["entries"] if e["identifier"] == "acme/db"]
        self.assertEqual(len(merged), 1)
        self.assertIn(merged[0]["console_form"], ("acme/db-Prod01", "acme/db-Dev001"))
        for entry in out["entries"]:
            self.assertTrue(datastores.spelling_is_ambiguous(entry), entry["identifier"])

    def test_a_declaration_folding_a_merged_referenced_secret_keeps_its_console_form(self):
        # The other direction: the section already holds the entry the bare
        # and console forms merged into, and the declaration arrives later.
        inventory = {"data_dependencies": []}
        with tempfile.TemporaryDirectory() as root:
            _write(root, "iam.tf",
                   'resource "aws_iam_policy" "p" {\n  policy = jsonencode({ Resource = ['
                   f'"{self._S}acme/db", "{self._S}acme/db-Prod01"] }})\n}}\n')
            datastores.harvest_datastores(inventory, root)
            self.assertEqual(inventory["data_dependencies"][0]["console_form"],
                             "acme/db-Prod01")
            _write(root, "sm.tf",
                   'resource "aws_secretsmanager_secret" "db" {\n  name = "acme/db"\n}\n')
            datastores.harvest_datastores(inventory, root)
        self.assertEqual(len(inventory["data_dependencies"]), 1)
        entry = inventory["data_dependencies"][0]
        self.assertEqual((entry["detection"], entry["console_form"]),
                         ("declared", "acme/db-Prod01"))

    def test_a_declared_name_that_looks_suffixed_is_still_never_stripped(self):
        # `_console_form` reads a declaration's `console_form` only, never its
        # identifier: `app-AbC1dE` is a declared NAME, and the console form of
        # that secret is `app-AbC1dE-XyZ9wQ`, which must still fold.
        out = _harvest({"a.tf": (
            'resource "aws_secretsmanager_secret" "db" {\n  name = "app-AbC1dE"\n}\n'
            'resource "aws_iam_policy" "p" {\n  policy = jsonencode({ Resource = '
            f'"{self._S}app-AbC1dE-XyZ9wQ" }})\n}}\n')})
        self.assertEqual(len(out["entries"]), 1)
        self.assertEqual(out["entries"][0]["console_form"], "app-AbC1dE-XyZ9wQ")

    def test_a_stored_msk_handle_round_trips_through_the_arn_name_keys(self):
        # The harvest stores `cluster/<name>` with the UUID dropped, and the
        # kafka branch demanded three components, so `_arn_name_keys` was
        # empty for every MSK entry — the one namespace the pass was blind to.
        self.assertEqual(
            datastores._arn_name_keys({
                "service": "msk", "identifier": "orders",
                "arn": "arn:aws:kafka:us-east-1:111111111111:cluster/orders"}),
            {("msk", "orders")})

    def test_every_stored_handle_round_trips_through_the_arn_name_keys(self):
        # The structural pin behind the MSK case: for every namespace this
        # scan records, the handle it stores must re-resolve to a name key the
        # entry is already indexed under, or the ambiguity pass cannot see it.
        arns = [
            "arn:aws:s3:::acme-archive",
            "arn:aws:dynamodb:us-east-1:111111111111:table/orders",
            "arn:aws:rds:us-east-1:111111111111:db:orders",
            "arn:aws:rds:us-east-1:111111111111:cluster:orders-cluster",
            "arn:aws:elasticache:us-east-1:111111111111:replicationgroup:redis",
            "arn:aws:memorydb:us-east-1:111111111111:cluster/sessions",
            "arn:aws:sqs:us-east-1:111111111111:orders",
            "arn:aws:sns:us-east-1:111111111111:events",
            "arn:aws:kinesis:us-east-1:111111111111:stream/clicks",
            "arn:aws:firehose:us-east-1:111111111111:deliverystream/clicks",
            "arn:aws:kafka:us-east-1:111111111111:cluster/orders/"
            "1a2b3c4d-1111-2222-3333-444455556666-1",
            "arn:aws:mq:us-east-1:111111111111:broker:orders:b-1a2b3c4d",
            "arn:aws:es:us-east-1:111111111111:domain/search",
            "arn:aws:redshift:us-east-1:111111111111:cluster:warehouse",
            f"{self._S}acme/db-AbC1dE",
            "arn:aws:ssm:us-east-1:111111111111:parameter/db/password",
            "arn:aws:elasticfilesystem:us-east-1:111111111111:file-system/fs-0123456789abcdef0",
            "arn:aws:fsx:us-east-1:111111111111:file-system/fs-0123456789abcdef0",
        ]
        out = _harvest({"a.tf": (
            'resource "aws_iam_policy" "p" {\n  policy = jsonencode({ Resource = ['
            + ", ".join(f'"{a}"' for a in arns) + '] })\n}\n')})
        self.assertEqual(len(out["entries"]), len(arns))
        for entry in out["entries"]:
            self.assertTrue(
                datastores._arn_name_keys(entry) & datastores.name_keys(entry),
                entry["arn"])


class ReviewRoundFortyFourTest(unittest.TestCase):
    """Regressions from the forty-fourth adversarial review round."""

    _S = "arn:aws:secretsmanager:us-east-1:111111111111:secret:"

    def test_the_policy_form_of_a_suffixed_looking_declared_name_still_folds(self):
        # `_console_form` took a referenced entry's own identifier for a
        # console spelling whenever it LOOKED suffixed. `app-AbC1dE` is the
        # bare side of this pair, and once the console form had folded the
        # exact policy form of the declared secret was refused as a twin —
        # one secret became two gating entries, the second carrying the
        # referenced note that no declaration states its name.
        out = _harvest({"a.tf": (
            'resource "aws_secretsmanager_secret" "db" {\n  name = "app-AbC1dE"\n}\n'
            'resource "aws_iam_policy" "p" {\n  policy = jsonencode({ Resource = ['
            f'"{self._S}app-AbC1dE-XyZ9wQ", '
            '"arn:aws:secretsmanager:*:*:secret:app-AbC1dE"] })\n}\n')})
        self.assertEqual(len(out["entries"]), 1)
        entry = out["entries"][0]
        self.assertEqual((entry["detection"], entry["console_form"]),
                         ("declared", "app-AbC1dE-XyZ9wQ"))
        self.assertFalse(datastores.spelling_is_ambiguous(entry))

    def test_two_undeclared_spellings_of_a_suffixed_looking_name_still_merge(self):
        out = _harvest({"a.tf": (
            'resource "aws_iam_policy" "p" {\n  policy = jsonencode({ Resource = ['
            f'"{self._S}app-AbC1dE", "{self._S}app-AbC1dE-XyZ9wQ"] }})\n}}\n')})
        self.assertEqual(len(out["entries"]), 1)
        entry = out["entries"][0]
        self.assertEqual((entry["identifier"], entry["console_form"]),
                         ("app-AbC1dE", "app-AbC1dE-XyZ9wQ"))
        self.assertTrue(any(n.startswith(datastores.SECRET_MERGED_NOTE_PREFIX)
                            for n in entry["notes"]), entry["notes"])

    def test_a_second_console_form_is_still_refused_under_the_pair_rule(self):
        # `acme/db-Dev001` strips to the declared name, so it IS the suffixed
        # side here and the round-43 refusal holds.
        out = _harvest({"a.tf": (
            'resource "aws_secretsmanager_secret" "db" {\n  name = "acme/db"\n}\n'
            'resource "aws_iam_policy" "p" {\n  policy = jsonencode({ Resource = ['
            f'"{self._S}acme/db-Prod01", "{self._S}acme/db-Dev001"] }})\n}}\n')})
        self.assertEqual(len(out["entries"]), 2)

    def test_a_declared_fold_says_which_console_form_it_took(self):
        # The undeclared branch wrote the merge note and the declared one was
        # silent, and `console_form` is printed nowhere: a reviewer could not
        # see, let alone dispute, which spelling was taken for a secret.
        out = _harvest({"a.tf": (
            'resource "aws_secretsmanager_secret" "db" {\n  name = "acme/db"\n}\n'
            'resource "aws_iam_policy" "p" {\n  policy = jsonencode({ Resource = '
            f'"{self._S}acme/db-Prod01" }})\n}}\n')})
        self.assertEqual(len(out["entries"]), 1)
        notes = out["entries"][0]["notes"]
        merged = [n for n in notes if n.startswith(datastores.SECRET_MERGED_NOTE_PREFIX)]
        self.assertEqual(len(merged), 1, notes)
        self.assertIn("'acme/db-Prod01'", merged[0])

    def test_a_declaration_folding_a_merged_referenced_secret_says_so_too(self):
        # The other direction drops every referenced-side note on the fold,
        # the merge note included; the declaration writes its own.
        inventory = {"data_dependencies": []}
        with tempfile.TemporaryDirectory() as root:
            _write(root, "iam.tf",
                   'resource "aws_iam_policy" "p" {\n  policy = jsonencode({ Resource = ['
                   f'"{self._S}acme/db", "{self._S}acme/db-Prod01"] }})\n}}\n')
            datastores.harvest_datastores(inventory, root)
            _write(root, "sm.tf",
                   'resource "aws_secretsmanager_secret" "db" {\n  name = "acme/db"\n}\n')
            datastores.harvest_datastores(inventory, root)
        self.assertEqual(len(inventory["data_dependencies"]), 1)
        entry = inventory["data_dependencies"][0]
        self.assertEqual(entry["detection"], "declared")
        self.assertTrue(any(n.startswith(datastores.SECRET_MERGED_NOTE_PREFIX)
                            for n in entry["notes"]), entry["notes"])

    def test_find_arns_records_the_uuid_less_msk_cluster_form(self):
        # The kafka branch's two-component acceptance is a scan-output change,
        # not only a round trip: pinned here so it cannot drift unnoticed.
        def kinds(resource):
            found = datastores.find_arns(
                f'"arn:aws:kafka:us-east-1:111111111111:{resource}"')
            return [(m.kind, m.service, m.identifier) for m in found]
        recorded = [("recorded", "msk", "orders")]
        self.assertEqual(kinds("cluster/orders"), recorded)
        self.assertEqual(
            kinds("cluster/orders/1a2b3c4d-1111-2222-3333-444455556666-1"), recorded)
        self.assertEqual(kinds("cluster/orders/*"), recorded)
        self.assertNotIn("recorded", [k for k, *_ in kinds("topic/orders")])
        self.assertIn("wildcard", [k for k, *_ in kinds("cluster/*")])


class ReviewRoundFortyFiveTest(unittest.TestCase):
    """Pins from the forty-fifth adversarial review round."""

    def test_an_arn_named_only_under_deny_is_still_recorded(self):
        # Pinned, not endorsed. The literal harvest reads the whole policy
        # text and does not parse statements, so a guardrail bucket under
        # `Effect = "Deny"` mints a `migrate`-graded entry exactly as a grant
        # does, and gates the consuming component. DESIGN records it beside
        # the join's Deny-as-grant reading; a scan note naming Deny-only ARNs
        # is the proportionate fix and is not built. This test is what will
        # fail the day that changes, so the document is updated with it.
        out = _harvest({"a.tf": (
            'resource "aws_iam_policy" "p" {\n  policy = jsonencode({ Statement = ['
            '{ Effect = "Allow", Action = "s3:GetObject", '
            'Resource = "arn:aws:s3:::acme-invoice-archive" }, '
            '{ Effect = "Deny", Action = "s3:*", '
            'Resource = "arn:aws:s3:::acme-prod-ledger" }] })\n}\n')})
        self.assertEqual(sorted(e["identifier"] for e in out["entries"]),
                         ["acme-invoice-archive", "acme-prod-ledger"])
        self.assertEqual({e["disposition"] for e in out["entries"]}, {"migrate"})


class ReviewRoundFortySixTest(unittest.TestCase):
    """Regressions from the forty-sixth adversarial review round."""

    def test_a_foreign_account_does_not_make_the_estates_own_arn_ambiguous(self):
        # With the account set complete, the 333 ARN is answered — cross-
        # account, never folded — yet grouping it with the 111 ARN flagged
        # both, refused the estate's own ARN its fold onto the declared
        # replica, and recorded one database twice, each gating.
        out = _harvest({
            "providers.tf": ('provider "aws" {\n  region = "us-east-1"\n'
                             '  allowed_account_ids = ["111111111111"]\n}\n'),
            "rds.tf": ('resource "aws_db_instance" "replica" {\n'
                       '  identifier = "orders-replica"\n  engine = "postgres"\n}\n'),
            "iam.tf": ('resource "aws_iam_policy" "p" {\n  policy = jsonencode({ Resource = ['
                       '"arn:aws:rds:us-east-1:111111111111:db:orders-replica", '
                       '"arn:aws:rds:us-east-1:333333333333:db:orders-replica"] })\n}\n')})
        by_detection = {e["detection"]: e for e in out["entries"]}
        self.assertEqual(sorted(by_detection), ["declared", "referenced"])
        self.assertEqual(by_detection["declared"]["arn"],
                         "arn:aws:rds:us-east-1:111111111111:db:orders-replica")
        self.assertEqual(by_detection["referenced"]["account"], "333333333333")
        self.assertTrue(any(n.startswith(datastores.CROSS_ACCOUNT_NOTE_PREFIX)
                            for n in by_detection["referenced"]["notes"]))
        for entry in out["entries"]:
            self.assertFalse(datastores.spelling_is_ambiguous(entry), entry["notes"])

    def test_two_own_accounts_still_make_the_name_ambiguous(self):
        # The narrowing is only ever about accounts the estate does NOT own:
        # two of its own accounts naming one queue is the case the rule is for.
        out = _harvest({
            "providers.tf": ('provider "aws" {\n  region = "us-east-1"\n'
                             '  allowed_account_ids = ["111111111111", "222222222222"]\n}\n'),
            "iam.tf": ('resource "aws_iam_policy" "p" {\n  policy = jsonencode({ Resource = ['
                       '"arn:aws:sqs:us-east-1:111111111111:orders", '
                       '"arn:aws:sqs:us-east-1:222222222222:orders"] })\n}\n')})
        self.assertEqual(len(out["entries"]), 2)
        for entry in out["entries"]:
            self.assertTrue(datastores.spelling_is_ambiguous(entry), entry["notes"])

    _CARTS = (
        'resource "aws_dynamodb_table" "carts" {\n  name = "carts"\n}\n'
        'resource "aws_iam_policy" "carts" {\n  policy = jsonencode({ Statement = '
        '[{ Resource = aws_dynamodb_table.carts.arn }] })\n}\n')
    _KEYED_LITERALS = (
        '  policy_statements = {\n'
        '    read  = { resources = ["arn:aws:s3:::acme-invoice-archive"] }\n'
        '    write = { resources = ["arn:aws:s3:::acme-invoice-staging"] }\n'
        '  }\n')

    def _carts_consumers(self, module_body):
        out = _harvest({"main.tf": self._CARTS + module_body})
        table = next(e for e in out["entries"] if e["service"] == "dynamodb")
        return [c["workload"] for c in table["consumers"]]

    def test_a_literal_per_app_verdict_does_not_strand_the_reference_grants(self):
        # The keyed-literal test "gates the LITERAL clause only", said the
        # comment — and one `per_app` verdict scoped both clauses. A wrapper
        # whose inline trust policy gives the subject a slot of its own
        # (`StringEquals :sub` beside `StringLike :aud`) beside keyed
        # `policy_statements` flipped per-app, and the declared table reached
        # through `role_policy_arns` lost the consumer origin/main gave it.
        trust = (
            'module "irsa" {\n  source = "./modules/irsa-role"\n'
            '  role_policy_arns = { carts = aws_iam_policy.carts.arn }\n'
            '  assume_role_policy = jsonencode({ Statement = [{ Condition = {\n'
            '    StringEquals = { "x:sub" = "system:serviceaccount:carts:carts-sa" }\n'
            '    StringLike   = { "x:aud" = "sts.amazonaws.com" }\n'
            '  } }] })\n' + self._KEYED_LITERALS + '}\n')
        self.assertEqual(self._carts_consumers(trust), ["carts-sa"])
        second_provider = (
            'module "irsa" {\n'
            '  source = "terraform-aws-modules/iam/aws//modules/iam-role-for-service-accounts-eks"\n'
            '  role_policy_arns = { carts = aws_iam_policy.carts.arn }\n'
            '  oidc_providers = {\n'
            '    main     = { provider_arn = module.eks.oidc_provider_arn, '
            'namespace_service_accounts = ["carts:carts-sa"] }\n'
            '    defaults = { provider_arn = "" }\n  }\n' + self._KEYED_LITERALS + '}\n')
        self.assertEqual(self._carts_consumers(second_provider), ["carts-sa"])

    def test_a_quoted_description_key_is_prose_too(self):
        # `_DESCRIPTION_ARG_RE` matched the bare key in the mask, where a
        # quoted key is blank, so `{ "description" = "arn:…" }` minted a
        # dependency with no prose note.
        out = _harvest({"a.tf": (
            'resource "aws_iam_policy" "p" {\n  policy = jsonencode({ Statement = ['
            '{ "description" = "retired: arn:aws:s3:::acme-old-archive", '
            'Resource = "arn:aws:s3:::acme-invoice-archive" }] })\n}\n')})
        self.assertEqual([e["identifier"] for e in out["entries"]], ["acme-invoice-archive"])
        self.assertTrue(any("acme-old-archive" in n for n in out["notes"]), out["notes"])


class ReviewRoundFortySevenTest(unittest.TestCase):
    """Regressions from the forty-seventh adversarial review round."""

    def test_a_foreign_spelling_is_not_a_sibling_of_the_estates_own(self):
        # Round 46 kept the foreign ARN out of the ambiguity GROUP; the
        # sibling search still counted it, so the unstated spelling was
        # refused its fold onto the estate's own ARN and the estate's queue
        # stood twice, each flagged, with nothing declaring the name.
        out = _harvest({
            "providers.tf": ('provider "aws" {\n  region = "us-east-1"\n'
                             '  allowed_account_ids = ["111111111111"]\n}\n'),
            "iam.tf": ('resource "aws_iam_policy" "p" {\n  policy = jsonencode({ Resource = ['
                       '"arn:aws:sqs:us-east-1:111111111111:orders", '
                       '"arn:aws:sqs:us-east-1:333333333333:orders", '
                       '"arn:aws:sqs:*:*:orders"] })\n}\n')})
        self.assertEqual(sorted(e["arn"] for e in out["entries"]),
                         ["arn:aws:sqs:us-east-1:111111111111:orders",
                          "arn:aws:sqs:us-east-1:333333333333:orders"])
        for entry in out["entries"]:
            self.assertFalse(datastores.spelling_is_ambiguous(entry), entry["notes"])


class ReviewRoundFortyEightTest(unittest.TestCase):
    """Regressions from the forty-eighth adversarial review round."""

    def test_a_cross_region_spelling_does_not_make_the_estates_own_arn_ambiguous(self):
        # The account axis got this in round 46; the region axis did not,
        # so a parameter in the estate's one region gated twice because a
        # policy also named its eu-west-1 twin.
        out = _harvest({"envs/prod/main.tf": (
            'provider "aws" {\n  region = "us-east-1"\n'
            '  allowed_account_ids = ["111111111111"]\n}\n'
            'resource "aws_ssm_parameter" "db" {\n  name = "/orders/db"\n'
            '  type = "SecureString"\n  value = "x"\n}\n'
            'resource "aws_iam_policy" "p" {\n  policy = jsonencode({ Statement = [{ Resource = ['
            '"arn:aws:ssm:us-east-1:111111111111:parameter/orders/db", '
            '"arn:aws:ssm:eu-west-1:111111111111:parameter/orders/db"] }] })\n}\n')})
        by_detection = {e["detection"]: e for e in out["entries"]}
        self.assertEqual(sorted(by_detection), ["declared", "referenced"])
        self.assertEqual(by_detection["declared"]["arn"],
                         "arn:aws:ssm:us-east-1:111111111111:parameter/orders/db")
        self.assertEqual(by_detection["referenced"]["region"], "eu-west-1")
        for entry in out["entries"]:
            self.assertFalse(datastores.spelling_is_ambiguous(entry), entry["notes"])


class ReviewRoundFortyNineTest(unittest.TestCase):
    """Regressions from the forty-ninth adversarial review round."""

    def test_a_cross_partition_spelling_does_not_make_the_estates_own_bucket_ambiguous(self):
        # The region filter could not reach a region-less S3 ARN, so the
        # `aws-cn` spelling still flagged the estate's own bucket and
        # refused it its fold.
        out = _harvest({"envs/prod/main.tf": (
            'provider "aws" {\n  region = "us-east-1"\n'
            '  allowed_account_ids = ["111111111111"]\n}\n'
            'resource "aws_s3_bucket" "logs" {\n  bucket = "acme-logs"\n}\n'
            'resource "aws_iam_policy" "p" {\n  policy = jsonencode({ Statement = [{ Resource = ['
            '"arn:aws:s3:::acme-logs", "arn:aws-cn:s3:::acme-logs"] }] })\n}\n')})
        by_detection = {e["detection"]: e for e in out["entries"]}
        self.assertEqual(sorted(by_detection), ["declared", "referenced"])
        self.assertEqual(by_detection["declared"]["arn"], "arn:aws:s3:::acme-logs")
        self.assertEqual(by_detection["referenced"]["arn"], "arn:aws-cn:s3:::acme-logs")
        for entry in out["entries"]:
            self.assertFalse(datastores.spelling_is_ambiguous(entry), entry["notes"])


class ReviewRoundFiftyThreeTest(unittest.TestCase):
    """Regressions from the fifty-third adversarial review round."""

    _PROVIDER = ('provider "aws" {\n  region = "us-east-1"\n'
                 '  allowed_account_ids = ["111111111111"]\n}\n')

    @staticmethod
    def _policy(*arns):
        return ('resource "aws_iam_role_policy" "p" {\n  role = "x"\n  policy = jsonencode({ '
                'Statement = [{ Effect = "Allow", Action = ["*"], Resource = ['
                + ", ".join(f'"{a}"' for a in arns) + '] }] })\n}\n')

    def test_a_database_name_is_never_matched_against_an_instance_arn(self):
        # `IDENTIFIER_ARGS["rds"]` falls back to `db_name` when `identifier`
        # is an expression; a `db:` ARN always carries the INSTANCE
        # identifier. Matched as equals, `db:orders` folded onto
        # `prod-orders-primary`, and the instance's own ARN stood as a second
        # gating database.
        instance = ('variable "env" {\n  default = "prod"\n}\n'
                    'resource "aws_db_instance" "main" {\n'
                    '  identifier = "${var.env}-orders-primary"\n  db_name = "orders"\n'
                    '  engine = "postgres"\n  allocated_storage = 20\n}\n')
        out = _harvest({"envs/dev/main.tf": self._PROVIDER + instance + self._policy(
            "arn:aws:rds:us-east-1:111111111111:db:orders")})
        self.assertEqual(len(out["entries"]), 2, [e["address"] for e in out["entries"]])
        declared = next(e for e in out["entries"] if e["detection"] == "declared")
        self.assertIsNone(declared["arn"])
        self.assertTrue(any("database name" in n for n in declared["notes"]), declared["notes"])

    def test_a_replica_region_is_the_estates_own(self):
        # A replica is a second resource the primary's replication re-creates,
        # not a second spelling of one: it stands apart, non-gating, with the
        # note saying so, and draws no cross-region verdict. Folding it took
        # three review rounds to get wrong three ways.
        secret = ('resource "aws_secretsmanager_secret" "db" {\n  name = "prod/db"\n'
                  '  replica {\n    region = "eu-west-1"\n  }\n}\n')
        out = _harvest({"envs/dev/main.tf": self._PROVIDER + secret + self._policy(
            "arn:aws:secretsmanager:eu-west-1:111111111111:secret:prod/db-??????")})
        by_detection = {e["detection"]: e for e in out["entries"]}
        self.assertEqual(sorted(by_detection), ["declared", "referenced"])
        self.assertIsNone(by_detection["declared"]["arn"])
        replica = by_detection["referenced"]
        self.assertEqual(replica["disposition"], "rebuild")
        self.assertTrue(any(n.startswith(datastores.REPLICA_NOTE_PREFIX) for n in replica["notes"]))
        self.assertFalse(any(n.startswith(datastores.CROSS_REGION_NOTE_PREFIX)
                             for n in replica["notes"]))
        self.assertFalse(datastores.spelling_is_ambiguous(replica))

    def test_a_cluster_named_by_name_is_not_a_database_name(self):
        # Round 53 marked every RDS-family entry whose identifier came from
        # `name`; for Aurora, DocumentDB and Neptune `name` IS the cluster
        # identifier, so the sample estate's clusters carried a false note
        # and the cluster ARN's fold was refused.
        out = _harvest({"envs/dev/main.tf": self._PROVIDER + (
            'module "aurora" {\n  source = "terraform-aws-modules/rds-aurora/aws"\n'
            '  name = "orders"\n  engine = "aurora-postgresql"\n}\n') + self._policy(
            "arn:aws:rds:us-east-1:111111111111:cluster:orders")})
        self.assertEqual(len(out["entries"]), 1, [e["address"] for e in out["entries"]])
        entry = out["entries"][0]
        self.assertFalse(entry.get("identifier_is_db_name"))
        self.assertEqual(entry["arn"], "arn:aws:rds:us-east-1:111111111111:cluster:orders")
        self.assertFalse(any("database name" in n for n in entry["notes"]), entry["notes"])

    def test_two_instances_sharing_a_db_name_are_two_databases(self):
        instances = ('resource "aws_db_instance" "c" {\n  identifier = "app"\n'
                     '  engine = "postgres"\n  allocated_storage = 20\n}\n'
                     'resource "aws_db_instance" "a" {\n  identifier = local.a\n  db_name = "app"\n'
                     '  engine = "postgres"\n  allocated_storage = 20\n}\n')
        out = _harvest({"envs/dev/main.tf": self._PROVIDER + instances + self._policy(
            "arn:aws:rds:us-east-1:111111111111:db:app")})
        by_address = {e["address"]: e for e in out["entries"]}
        self.assertEqual(sorted(by_address), ["aws_db_instance.a", "aws_db_instance.c"])
        self.assertEqual(by_address["aws_db_instance.c"]["arn"],
                         "arn:aws:rds:us-east-1:111111111111:db:app")
        self.assertIsNone(by_address["aws_db_instance.a"]["arn"])
        self.assertTrue(by_address["aws_db_instance.a"].get("identifier_is_db_name"))

    def test_a_replica_region_excuses_only_the_replicated_resource(self):
        # Round 53 joined the replica's region to the ESTATE's set, and a
        # same-named queue in that region then folded across the
        # cross-region rule.
        secret = ('resource "aws_secretsmanager_secret" "db" {\n  name = "prod/db"\n'
                  '  replica {\n    region = "eu-west-1"\n  }\n}\n')
        queue = 'resource "aws_sqs_queue" "orders" {\n  name = "orders"\n}\n'
        out = _harvest({"envs/dev/main.tf": self._PROVIDER + secret + queue + self._policy(
            "arn:aws:sqs:eu-west-1:111111111111:orders",
            "arn:aws:secretsmanager:eu-west-1:111111111111:secret:prod/db-??????")})
        by_service = {}
        for entry in out["entries"]:
            by_service.setdefault(entry["service"], []).append(entry)
        self.assertEqual({e["detection"] for e in by_service["secretsmanager"]},
                         {"declared", "referenced"})
        replica = next(e for e in by_service["secretsmanager"] if e["detection"] == "referenced")
        self.assertTrue(any(n.startswith(datastores.REPLICA_NOTE_PREFIX) for n in replica["notes"]))
        self.assertEqual(len(by_service["sqs"]), 2, [e["address"] for e in by_service["sqs"]])
        referenced_queue = next(e for e in by_service["sqs"] if e["detection"] == "referenced")
        self.assertTrue(any(n.startswith(datastores.CROSS_REGION_NOTE_PREFIX)
                            for n in referenced_queue["notes"]), referenced_queue["notes"])

    def test_a_secret_named_in_both_its_regions_keeps_its_primary_and_answers_its_replica(self):
        # The primary spelling folds onto the declaration, whatever order the
        # spellings arrive in and whether or not the replica's region is
        # also a provider region; the replica stands apart, non-gating, and
        # flags nothing. The fold-based rule this replaces recorded the
        # replica's region, kept the replica's ARN, and let a same-named
        # secret in a third region in.
        secret = ('resource "aws_secretsmanager_secret" "db" {\n  name = "prod/db"\n'
                  '  replica {\n    region = "eu-west-1"\n  }\n}\n')
        for provider_region, extra_provider in (("us-east-1", ""), ("ap-south-1", ""),
                                                ("us-east-1", 'provider "aws" {\n  alias = "eu"\n'
                                                              '  region = "eu-west-1"\n'
                                                              '  allowed_account_ids = ["111111111111"]\n}\n')):
            with self.subTest(primary=provider_region, aliased=bool(extra_provider)):
                provider = (f'provider "aws" {{\n  region = "{provider_region}"\n'
                            '  allowed_account_ids = ["111111111111"]\n}\n' + extra_provider)
                primary = f"arn:aws:secretsmanager:{provider_region}:111111111111:secret:prod/db"
                both = self._policy(primary + "-??????",
                                    "arn:aws:secretsmanager:eu-west-1:111111111111:secret:prod/db-??????")
                out = _harvest({"envs/dev/main.tf": provider + secret + both})
                by_detection = {e["detection"]: e for e in out["entries"]}
                self.assertEqual(sorted(by_detection), ["declared", "referenced"],
                                 [e["address"] for e in out["entries"]])
                self.assertEqual((by_detection["declared"]["arn"], by_detection["declared"]["region"]),
                                 (primary, provider_region))
                replica = by_detection["referenced"]
                self.assertEqual(replica["region"], "eu-west-1")
                self.assertEqual(replica["disposition"], "rebuild")
                for entry in out["entries"]:
                    self.assertFalse(datastores.spelling_is_ambiguous(entry), entry["notes"])

    def test_the_console_form_of_a_replicated_secret_is_answered_as_the_replica(self):
        secret = ('resource "aws_secretsmanager_secret" "db" {\n  name = "prod/db"\n'
                  '  replica {\n    region = "eu-west-1"\n  }\n}\n')
        out = _harvest({"envs/dev/main.tf": self._PROVIDER + secret + self._policy(
            "arn:aws:secretsmanager:eu-west-1:111111111111:secret:prod/db-AbC1dE")})
        replica = next(e for e in out["entries"] if e["detection"] == "referenced")
        self.assertTrue(any(n.startswith(datastores.REPLICA_NOTE_PREFIX) for n in replica["notes"]))
        self.assertFalse(any(n.startswith(datastores.CROSS_REGION_NOTE_PREFIX)
                             for n in replica["notes"]))
        self.assertEqual(replica["disposition"], "rebuild")

    def test_a_same_named_secret_in_a_third_region_is_never_the_replica(self):
        # The fold rule's worst case: a same-named secret in a region the
        # declaration never names, absorbed because a replica's spelling
        # had arrived first. Now it is what it is — cross-region, apart.
        provider = ('provider "aws" {\n  region = "us-east-1"\n'
                    '  allowed_account_ids = ["111111111111"]\n}\n')
        secret = ('resource "aws_secretsmanager_secret" "db" {\n  name = "prod/db"\n'
                  '  replica {\n    region = "eu-west-1"\n  }\n}\n')
        out = _harvest({"envs/dev/main.tf": provider + secret + self._policy(
            "arn:aws:secretsmanager:eu-west-1:111111111111:secret:prod/db-??????",
            "arn:aws:secretsmanager:ap-south-1:111111111111:secret:prod/db-AbC1dE")})
        third = next(e for e in out["entries"] if e.get("region") == "ap-south-1")
        self.assertEqual(third["detection"], "referenced")
        self.assertTrue(any(n.startswith(datastores.CROSS_REGION_NOTE_PREFIX) for n in third["notes"]))
        self.assertIsNone(next(e for e in out["entries"] if e["detection"] == "declared")["arn"])

    def test_an_unclosed_block_is_a_note_and_the_verdicts_stand(self):
        # Round 56 withheld both verdicts for a truncated file; one stray
        # unclosed block anywhere then folded every confirmed cross-account
        # ARN onto a same-named declaration. The provider pass reads every
        # file whole on its own, so the verdicts stand; what the unread tail
        # hides — here a replica block — is answered in the safe direction.
        files = {
            "envs/dev/a_main.tf": self._PROVIDER + (
                'resource "aws_sqs_queue" "orders" {\n  name = "orders"\n}\n'
                'resource "aws_s3_bucket" "bad" {\n  bucket = "x"\n'   # never closed
                'resource "aws_secretsmanager_secret" "db" {\n  name = "prod/db"\n'
                '  replica {\n    region = "eu-west-1"\n  }\n}\n'),
            "envs/dev/iam.tf": self._policy(
                "arn:aws:secretsmanager:eu-west-1:111111111111:secret:prod/db-??????",
                "arn:aws:sqs:us-east-1:999999999999:orders"),
        }
        out = _harvest(files)
        self.assertTrue(any("never closed" in n for n in out["notes"]))
        foreign_queue = next(e for e in out["entries"] if e.get("account") == "999999999999")
        self.assertEqual(foreign_queue["detection"], "referenced")
        self.assertTrue(any(n.startswith(datastores.CROSS_ACCOUNT_NOTE_PREFIX)
                            for n in foreign_queue["notes"]), foreign_queue["notes"])
        unseen_replica = next(e for e in out["entries"] if e["service"] == "secretsmanager")
        self.assertEqual(unseen_replica["detection"], "referenced")
        self.assertTrue(any(n.startswith(datastores.CROSS_REGION_NOTE_PREFIX)
                            for n in unseen_replica["notes"]), unseen_replica["notes"])

    def test_an_override_stating_only_db_name_gets_the_right_reason(self):
        files = {
            "envs/dev/main.tf": self._PROVIDER + (
                'resource "aws_db_instance" "orders" {\n  identifier = "prod-orders"\n'
                '  db_name = "orders"\n  engine = "postgres"\n  allocated_storage = 20\n}\n'),
            "envs/dev/main_override.tf": (
                'resource "aws_db_instance" "orders" {\n  db_name = "orders"\n}\n'),
        }
        out = _harvest(files)
        flagged = [e for e in out["entries"] if e.get("identifier_is_db_name")]
        self.assertEqual(len(flagged), 1)
        note = next(n for n in flagged[0]["notes"] if "database name" in n)
        self.assertIn("does not state", note)
        self.assertNotIn("builds from an expression", note)

    def test_a_foreign_account_in_the_replica_region_is_not_the_replica(self):
        # A replica lives in the primary's account. Answered as a replica, the
        # finance account's secret was graded `rebuild` and gated nothing.
        secret = ('resource "aws_secretsmanager_secret" "db" {\n  name = "prod/db"\n'
                  '  replica {\n    region = "eu-west-1"\n  }\n}\n')
        out = _harvest({"envs/dev/main.tf": self._PROVIDER + secret + self._policy(
            "arn:aws:secretsmanager:eu-west-1:999999999999:secret:prod/db-??????")})
        foreign = next(e for e in out["entries"] if e["detection"] == "referenced")
        self.assertEqual(foreign["disposition"], "migrate")
        self.assertTrue(any(n.startswith(datastores.CROSS_ACCOUNT_NOTE_PREFIX) for n in foreign["notes"]))
        self.assertFalse(any(n.startswith(datastores.REPLICA_NOTE_PREFIX) for n in foreign["notes"]))

    def test_a_workload_reaching_the_secret_through_its_replica_is_the_primarys_consumer(self):
        # Consumers attach by address before the merge and the replica never
        # folds, so a role granted only the eu-west-1 ARN was attributed to
        # the `rebuild` replica and the `migrate` primary never learned of it.
        secret = ('resource "aws_secretsmanager_secret" "db" {\n  name = "prod/db"\n'
                  '  replica {\n    region = "eu-west-1"\n  }\n}\n')
        irsa = ('module "%s_irsa" {\n'
                '  source = "terraform-aws-modules/iam/aws//modules/iam-role-for-service-accounts-eks"\n'
                '  role_policy_arns = {}\n'
                '  oidc_providers = { main = { provider_arn = "x", '
                'namespace_service_accounts = ["default:%s"] } }\n'
                '  policy_statements = [{ actions = ["secretsmanager:GetSecretValue"], '
                'resources = ["%s"] }]\n}\n')
        out = _harvest({"envs/dev/main.tf": self._PROVIDER + secret
                        + irsa % ("carts", "carts-sa", "arn:aws:secretsmanager:us-east-1:111111111111:secret:prod/db-??????")
                        + irsa % ("orders", "orders-sa", "arn:aws:secretsmanager:eu-west-1:111111111111:secret:prod/db-??????")})
        primary = next(e for e in out["entries"] if e["detection"] == "declared")
        replica = next(e for e in out["entries"] if e["detection"] == "referenced")
        self.assertEqual(sorted(c["workload"] for c in primary["consumers"]), ["carts-sa", "orders-sa"])
        carried = next(c for c in primary["consumers"] if c["workload"] == "orders-sa")
        self.assertIn("replica", carried.get("note", ""))
        self.assertEqual([c["workload"] for c in replica["consumers"]], ["orders-sa"])
        # And the replica is not the forgotten out-of-band dependency.
        self.assertNotIn(datastores.REFERENCED_NOTE, replica["notes"])
        self.assertFalse(any("known only from a literal ARN and were not matched" in n
                             for n in out["notes"]), out["notes"])

    def test_the_default_estate_with_no_stated_account_still_gets_its_replica(self):
        # Round 57 required the ARN's account IN the estate's set; a provider
        # stating only a region — the default shape — leaves the set empty,
        # so every replica was `migrate`, gating, and its consumers were
        # never carried. The test is negative now: refused only when the
        # account is KNOWN to be another's.
        provider = 'provider "aws" {\n  region = "us-east-1"\n}\n'
        secret = ('resource "aws_secretsmanager_secret" "db" {\n  name = "prod/db"\n'
                  '  replica {\n    region = "eu-west-1"\n  }\n}\n')
        out = _harvest({"envs/dev/main.tf": provider + secret + self._policy(
            "arn:aws:secretsmanager:eu-west-1:111111111111:secret:prod/db-??????")})
        replica = next(e for e in out["entries"] if e["detection"] == "referenced")
        self.assertTrue(datastores.is_replica(replica), replica["notes"])
        self.assertEqual(replica["disposition"], "rebuild")
        self.assertTrue(any(n.startswith(datastores.UNKNOWN_ACCOUNT_NOTE_PREFIX)
                            for n in replica["notes"]), replica["notes"])

    def test_an_unverified_provider_region_does_not_fold_the_replica(self):
        # With `region = var.region` the estate's partitions are unknown, the
        # positive partition test failed, and the replica fell through to the
        # unknown-region branch — which FOLDS: the withdrawn fold back in.
        provider = ('provider "aws" {\n  region = var.region\n'
                    '  allowed_account_ids = ["111111111111"]\n}\n')
        secret = ('resource "aws_secretsmanager_secret" "db" {\n  name = "prod/db"\n'
                  '  replica {\n    region = "eu-west-1"\n  }\n}\n')
        out = _harvest({"envs/dev/main.tf": provider + secret + self._policy(
            "arn:aws:secretsmanager:eu-west-1:111111111111:secret:prod/db-??????")})
        by_detection = {e["detection"]: e for e in out["entries"]}
        self.assertEqual(sorted(by_detection), ["declared", "referenced"])
        self.assertIsNone(by_detection["declared"]["arn"])
        self.assertIsNone(by_detection["declared"]["region"])
        self.assertTrue(datastores.is_replica(by_detection["referenced"]))

    def test_a_replica_with_twin_primaries_carries_to_neither(self):
        # Two root modules declare the replicated name; the fold refuses to
        # choose, and the carry chose by walk order — prod's grant on dev's
        # secret. It carries to neither and says so.
        secret = ('resource "aws_secretsmanager_secret" "db" {\n  name = "prod/db"\n'
                  '  replica {\n    region = "eu-west-1"\n  }\n}\n')
        irsa = ('module "orders_irsa" {\n'
                '  source = "terraform-aws-modules/iam/aws//modules/iam-role-for-service-accounts-eks"\n'
                '  role_policy_arns = {}\n'
                '  oidc_providers = { main = { provider_arn = "x", '
                'namespace_service_accounts = ["default:orders-sa"] } }\n'
                '  policy_statements = [{ actions = ["secretsmanager:GetSecretValue"], '
                'resources = ["arn:aws:secretsmanager:eu-west-1:111111111111:secret:prod/db-??????"] }]\n}\n')
        out = _harvest({"envs/dev/main.tf": self._PROVIDER + secret,
                        "envs/prod/main.tf": self._PROVIDER + secret + irsa})
        declared = [e for e in out["entries"] if e["detection"] == "declared"]
        self.assertEqual(len(declared), 2)
        for entry in declared:
            self.assertEqual(entry["consumers"], [], entry["address"])
        replica = next(e for e in out["entries"] if e["detection"] == "referenced")
        self.assertEqual([c["workload"] for c in replica["consumers"]], ["orders-sa"])
        self.assertTrue(any(n.startswith(datastores.REPLICA_TWINS_NOTE_PREFIX)
                            for n in replica["notes"]), replica["notes"])

    def test_a_wildcard_primary_spelling_is_not_flagged_against_the_replica(self):
        secret = ('resource "aws_secretsmanager_secret" "db" {\n  name = "prod/db"\n'
                  '  replica {\n    region = "eu-west-1"\n  }\n}\n')
        out = _harvest({"envs/dev/main.tf": self._PROVIDER + secret + self._policy(
            "arn:aws:secretsmanager:*:111111111111:secret:prod/db-??????",
            "arn:aws:secretsmanager:eu-west-1:111111111111:secret:prod/db-??????")})
        for entry in out["entries"]:
            self.assertFalse(datastores.spelling_is_ambiguous(entry), entry["notes"])
        primary = next(e for e in out["entries"] if e["detection"] == "declared")
        self.assertEqual(primary["arn"], "arn:aws:secretsmanager::111111111111:secret:prod/db")

    def test_a_kinesis_consumer_arn_is_a_dependency_on_the_stream(self):
        out = _harvest({"envs/dev/main.tf": self._PROVIDER
                        + 'resource "aws_kinesis_stream" "clicks" {\n  name = "clicks"\n}\n'
                        + self._policy("arn:aws:kinesis:us-east-1:111111111111:stream/clicks/"
                                       "consumer/app-consumer:1585251534")})
        self.assertEqual([e["identifier"] for e in out["entries"]], ["clicks"])
        self.assertEqual(out["entries"][0]["arn"],
                         "arn:aws:kinesis:us-east-1:111111111111:stream/clicks")


class EndpointTest(unittest.TestCase):
    """Data stores known from a hostname or URL rather than an ARN."""

    def test_endpoint_forms_resolve_to_a_service_and_a_name(self):
        cases = [
            ("orders.c9akciq32xyz.us-east-1.rds.amazonaws.com", "rds", "orders",
             "orders.c9akciq32xyz.us-east-1.rds.amazonaws.com", "us-east-1"),
            ("orders.c9akciq32xyz.us-east-1.rds.amazonaws.com:5432", "rds", "orders",
             "orders.c9akciq32xyz.us-east-1.rds.amazonaws.com", "us-east-1"),
            ("catalog.cluster-c9akciq32xyz.eu-west-1.rds.amazonaws.com", "aurora",
             "catalog", "catalog.cluster-c9akciq32xyz.eu-west-1.rds.amazonaws.com",
             "eu-west-1"),
            ("catalog.cluster-ro-c9akciq32xyz.eu-west-1.rds.amazonaws.com", "aurora",
             "catalog", "catalog.cluster-c9akciq32xyz.eu-west-1.rds.amazonaws.com",
             "eu-west-1"),
            ("docs.cluster-c9akciq32xyz.us-east-1.docdb.amazonaws.com", "docdb", "docs",
             "docs.cluster-c9akciq32xyz.us-east-1.docdb.amazonaws.com", "us-east-1"),
            ("graph.cluster-c9akciq32xyz.us-east-1.neptune.amazonaws.com", "neptune",
             "graph", "graph.cluster-c9akciq32xyz.us-east-1.neptune.amazonaws.com",
             "us-east-1"),
            ("sessions.abc123.0001.use1.cache.amazonaws.com", "elasticache", "sessions",
             "sessions.abc123.use1.cache.amazonaws.com", None),
            ("master.sessions.abc123.use1.cache.amazonaws.com:6379", "elasticache",
             "sessions", "sessions.abc123.use1.cache.amazonaws.com", None),
            ("clustercfg.sessions.abc123.use1.cache.amazonaws.com", "elasticache",
             "sessions", "sessions.abc123.use1.cache.amazonaws.com", None),
            ("clustercfg.board.abc123.memorydb.us-east-1.amazonaws.com:6379", "memorydb",
             "board", "clustercfg.board.abc123.memorydb.us-east-1.amazonaws.com",
             "us-east-1"),
            ("https://sqs.us-east-1.amazonaws.com/123456789012/orders-events", "sqs",
             "orders-events",
             "https://sqs.us-east-1.amazonaws.com/123456789012/orders-events",
             "us-east-1"),
            ("https://sqs.us-east-1.amazonaws.com/123456789012/orders.fifo", "sqs",
             "orders.fifo",
             "https://sqs.us-east-1.amazonaws.com/123456789012/orders.fifo",
             "us-east-1"),
            ("https://acme-media.s3.amazonaws.com/uploads/x.png", "s3", "acme-media",
             "s3://acme-media", None),
            ("acme-media.s3.eu-west-1.amazonaws.com", "s3", "acme-media",
             "s3://acme-media", "eu-west-1"),
            ("https://s3.eu-west-1.amazonaws.com/acme-media/uploads", "s3",
             "acme-media", "s3://acme-media", "eu-west-1"),
            ("s3://acme-media/exports/", "s3", "acme-media", "s3://acme-media", None),
            ("https://search-catalog-abcdefghijklmnopqrstuvwxyz.us-east-1.es.amazonaws.com",
             "opensearch", "catalog",
             "search-catalog.us-east-1.es.amazonaws.com", "us-east-1"),
            ("b-1.events.abc123.c2.kafka.us-east-1.amazonaws.com:9092", "msk",
             "events", "events.abc123.kafka.us-east-1.amazonaws.com", "us-east-1"),
            ("b-1234abcd-12ab-34cd-56ef-123456abcdef-1.mq.us-east-1.amazonaws.com:5671",
             "mq", "b-1234abcd-12ab-34cd-56ef-123456abcdef",
             "b-1234abcd-12ab-34cd-56ef-123456abcdef.mq.us-east-1.amazonaws.com",
             "us-east-1"),
            ("warehouse.abcdefghijkl.us-east-1.redshift.amazonaws.com:5439", "redshift",
             "warehouse", "warehouse.abcdefghijkl.us-east-1.redshift.amazonaws.com",
             "us-east-1"),
        ]
        for token, service, identifier, handle, region in cases:
            with self.subTest(token=token):
                found = datastores.find_endpoints(f'"{token}"')
                self.assertEqual(len(found), 1, found)
                self.assertEqual(found[0].kind, "recorded")
                self.assertEqual(
                    (found[0].service, found[0].identifier, found[0].handle,
                     found[0].region),
                    (service, identifier, handle, region))
                self.assertIsNone(found[0].arn)

    def test_a_queue_url_states_the_account(self):
        found = datastores.find_endpoints(
            '"https://sqs.us-east-1.amazonaws.com/123456789012/orders"')
        self.assertEqual(found[0].account, "123456789012")

    def test_hostnames_that_are_not_data_stores_are_ignored(self):
        for token in (
            "https://sts.amazonaws.com",
            "oidc.eks.us-east-1.amazonaws.com/id/ABC",
            "123456789012.dkr.ecr.us-east-1.amazonaws.com/orders:1.2",
            "ec2.us-east-1.amazonaws.com",
            "https://s3.amazonaws.com",
            "eks.amazonaws.com/role-arn",
        ):
            with self.subTest(token=token):
                self.assertEqual(datastores.find_endpoints(f'"{token}"'), [])

    def test_an_endpoint_built_from_a_variable_is_an_expression(self):
        for token in ("${var.db}.c9akciq32xyz.us-east-1.rds.amazonaws.com",
                      "https://sqs.${var.region}.amazonaws.com/123456789012/orders",
                      "s3://${var.bucket}/exports"):
            with self.subTest(token=token):
                found = datastores.find_endpoints(f'"{token}"')
                self.assertEqual([f.kind for f in found], ["expression"])

    def test_a_config_map_holding_an_rds_endpoint_records_and_attributes_it(self):
        out = _harvest({"apps.tf": '''
resource "kubernetes_config_map" "orders" {
  metadata {
    name      = "orders-config"
    namespace = "acme-shop"
  }
  data = {
    DB_HOST = "orders.c9akciq32xyz.us-east-1.rds.amazonaws.com"
    DB_PORT = "5432"
  }
}
'''})
        self.assertEqual(len(out["entries"]), 1)
        entry = out["entries"][0]
        self.assertEqual((entry["service"], entry["identifier"], entry["detection"],
                          entry["region"], entry["arn"]),
                         ("rds", "orders", "referenced", "us-east-1", None))
        self.assertEqual(entry["address"],
                         "orders.c9akciq32xyz.us-east-1.rds.amazonaws.com")
        self.assertEqual(entry["disposition"], "migrate")
        self.assertEqual([(c["workload"], c["kind"], c["detection"])
                          for c in entry["consumers"]],
                         [("orders-config", "kubernetes_config_map", "terraform_wiring")])

    def test_a_queue_named_by_arn_and_by_url_is_one_entry(self):
        out = _harvest({
            "iam.tf": '''
resource "aws_iam_policy" "p" {
  policy = jsonencode({ Resource = "arn:aws:sqs:us-east-1:123456789012:orders-events" })
}
''',
            "apps.tf": '''
resource "helm_release" "orders" {
  name  = "orders"
  chart = "./charts/orders"
  set {
    name  = "queueUrl"
    value = "https://sqs.us-east-1.amazonaws.com/123456789012/orders-events"
  }
}
''',
        })
        self.assertEqual(len(out["entries"]), 1)
        entry = out["entries"][0]
        self.assertEqual(entry["arn"], "arn:aws:sqs:us-east-1:123456789012:orders-events")
        self.assertCountEqual(entry["evidence"], ["iam.tf", "apps.tf"])
        self.assertEqual([c["workload"] for c in entry["consumers"]], ["orders"])

    def test_two_regions_with_one_name_stay_two_entries(self):
        out = _harvest({"apps.tf": '''
resource "kubernetes_config_map" "orders" {
  metadata { name = "orders-config" }
  data = {
    PRIMARY = "orders.c9akciq32xyz.us-east-1.rds.amazonaws.com"
    DR      = "orders.d8bkdjr43abc.eu-west-1.rds.amazonaws.com"
  }
}
'''})
        self.assertEqual(sorted(e["region"] for e in out["entries"]),
                         ["eu-west-1", "us-east-1"])

    def test_an_endpoint_folds_onto_its_declared_twin(self):
        out = _harvest({
            "rds.tf": 'resource "aws_db_instance" "orders" {\n  identifier = "orders"\n}\n',
            "apps.tf": '''
resource "kubernetes_secret" "orders" {
  metadata { name = "orders-db" }
  data = { host = "orders.c9akciq32xyz.us-east-1.rds.amazonaws.com" }
}
''',
        })
        self.assertEqual(len(out["entries"]), 1)
        entry = out["entries"][0]
        self.assertEqual(entry["detection"], "declared")
        self.assertEqual(entry["region"], "us-east-1")
        self.assertEqual([c["workload"] for c in entry["consumers"]], ["orders-db"])



class Cl6RoundOneTest(unittest.TestCase):
    """Regressions from the first adversarial review round of the endpoint,
    YAML and guess change."""

    _RDS = "orders.c9akciq32xyz.us-east-1.rds.amazonaws.com"

    def test_a_connection_url_records_its_host_and_never_a_credential(self):
        out = _harvest({"apps.tf": f'''
resource "kubernetes_config_map" "orders" {{
  metadata {{ name = "orders-config" }}
  data = {{
    DATABASE_URL = "postgres://app:hunter2-very-secret@{self._RDS}:5432/orders"
    JDBC_URL     = "jdbc:postgresql://{self._RDS}:5432/orders"
    REDIS_URL    = "redis://sessions.abc123.use1.cache.amazonaws.com:6379/0"
  }}
}}
'''})
        by_service = {e["service"]: e for e in out["entries"]}
        self.assertEqual(sorted(by_service), ["elasticache", "rds"])
        self.assertEqual((by_service["rds"]["identifier"], by_service["rds"]["address"]),
                         ("orders", self._RDS))
        self.assertEqual(by_service["elasticache"]["address"],
                         "sessions.abc123.use1.cache.amazonaws.com")
        # The credential reaches nothing: no field, no note.
        import json
        self.assertNotIn("hunter2", json.dumps(out["entries"]) + json.dumps(out["notes"]))

    def test_an_interpolated_image_host_is_not_a_variable_built_endpoint(self):
        out = _harvest({"apps.tf": '''
resource "helm_release" "orders" {
  name  = "orders"
  chart = "./charts/orders"
  set {
    name  = "image"
    value = "${var.account_id}.dkr.ecr.us-east-1.amazonaws.com/orders:1.2"
  }
  set {
    name  = "queueUrl"
    value = "https://sqs.${var.region}.amazonaws.com/123456789012/orders"
  }
}
'''})
        note = next((n for n in out["notes"] if "build the resource name" in n), "")
        self.assertIn("sqs.${var.region}", note)
        self.assertNotIn("dkr.ecr", note)

    def test_a_queue_seen_by_url_keeps_the_url_when_the_arn_becomes_the_handle(self):
        url = "https://sqs.us-east-1.amazonaws.com/123456789012/orders-events"
        arn = "arn:aws:sqs:us-east-1:123456789012:orders-events"
        apps = f'''
resource "kubernetes_config_map" "orders" {{
  metadata {{ name = "orders-config" }}
  data = {{ QUEUE_URL = "{url}" }}
}}
'''
        (alone,) = _harvest({"apps.tf": apps})["entries"]
        self.assertEqual((alone["address"], alone["arn"], alone["endpoint"]), (url, None, url))
        (both,) = _harvest({"apps.tf": apps, "iam.tf": (
            'resource "aws_iam_policy" "p" {\n  policy = jsonencode({ Resource = "'
            + arn + '" })\n}\n')})["entries"]
        # The ARN is the handle; the URL survives on the entry so a decision
        # recorded under it still reaches the entry (`endpoint` is a handle
        # for every reader: entries_at, handles_of, both _target resolvers).
        self.assertEqual((both["address"], both["arn"], both["endpoint"]), (arn, arn, url))
        self.assertTrue(datastores.is_literal_handle(url))
        self.assertTrue(datastores.is_literal_handle("s3:acme-invoice-archive"))
        self.assertFalse(datastores.is_literal_handle("aws_s3_bucket.archive"))
        self.assertFalse(datastores.is_literal_handle("module.orders_queue"))



class Cl6RoundTwoTest(unittest.TestCase):
    """Regressions from the second adversarial review round."""

    _DEV = "orders.c9akdev00001.us-east-1.rds.amazonaws.com"
    _PROD = "orders.c9akprod0001.us-east-1.rds.amazonaws.com"

    @staticmethod
    def _config_map(name, host):
        return (f'resource "kubernetes_config_map" "{name}" {{\n  metadata {{ name = "{name}" }}\n'
                f'  data = {{ DB_HOST = "{host}" }}\n}}\n')

    def test_two_same_named_hosts_are_two_databases(self):
        out = _harvest({"envs/dev/cm.tf": self._config_map("orders-dev", self._DEV),
                        "envs/prod/cm.tf": self._config_map("orders-prod", self._PROD)})
        by_endpoint = {e["endpoint"]: e for e in out["entries"]}
        self.assertEqual(sorted(by_endpoint), [self._DEV, self._PROD])
        self.assertEqual([c["workload"] for c in by_endpoint[self._DEV]["consumers"]],
                         ["orders-dev"])
        self.assertEqual([c["workload"] for c in by_endpoint[self._PROD]["consumers"]],
                         ["orders-prod"])
        # Round 3: two hosts met in one merge are flagged on both sides.
        self.assertTrue(all(any(n.startswith(datastores.AMBIGUOUS_ENDPOINT_NOTE_PREFIX)
                                for n in e["notes"]) for e in out["entries"]))

    def test_one_host_folds_onto_its_declaration_and_two_fold_onto_neither(self):
        rds = 'resource "aws_db_instance" "orders" {\n  identifier = "orders"\n}\n'
        out = _harvest({"rds.tf": rds, "a.tf": self._config_map("orders-a", self._DEV)})
        (declared,) = out["entries"]
        self.assertEqual((declared["detection"], declared["endpoint"]), ("declared", self._DEV))
        # Two hosts of the name: neither folds (round 3), the declaration
        # keeps no endpoint and no consumer it cannot vouch for.
        out = _harvest({"rds.tf": rds, "a.tf": self._config_map("orders-a", self._DEV),
                        "b.tf": self._config_map("orders-b", self._PROD)})
        declared = next(e for e in out["entries"] if e["detection"] == "declared")
        apart = [e for e in out["entries"] if e["detection"] == "referenced"]
        self.assertEqual(len(apart), 2)
        self.assertIsNone(declared.get("endpoint"))
        # Reader and writer endpoints of one cluster still share a handle.
        both = _harvest({"cm.tf": self._config_map(
            "orders-rw", "catalog.cluster-c9akciq32xyz.eu-west-1.rds.amazonaws.com")
            + self._config_map("orders-ro", "catalog.cluster-ro-c9akciq32xyz.eu-west-1.rds.amazonaws.com")})
        self.assertEqual(len(both["entries"]), 1)

    def test_the_arn_is_the_handle_for_a_bucket_seen_by_url_too(self):
        out = _harvest({"iam.tf": (
            'resource "aws_iam_policy" "p" {\n  policy = jsonencode({ Resource = '
            '"arn:aws:s3:::acme-logs" })\n}\n'),
            "cm.tf": self._config_map("logs", "s3://acme-logs/exports/")})
        (entry,) = out["entries"]
        self.assertEqual((entry["address"], entry["endpoint"]),
                         ("arn:aws:s3:::acme-logs", "s3://acme-logs"))

    def test_notes_never_carry_a_credential_and_unread_hosts_are_said(self):
        out = _harvest({"locals.tf": (
            'locals {\n  url   = "postgres://app:hunter2@${var.db_host}.us-east-1.rds.amazonaws.com:5432/orders"\n'
            '  cn    = "orders.c9akciq32xyz.cn-north-1.rds.amazonaws.com.cn"\n'
            '  queue = "https://sqs.us-east-1.amazonaws.com/123456789012/orders?Action=SendMessage"\n}\n')})
        import json
        self.assertNotIn("hunter2", json.dumps(out["notes"]) + json.dumps(out["entries"]))
        self.assertTrue(any("build the resource name" in n and "${var.db_host}" in n
                            for n in out["notes"]), out["notes"])
        self.assertTrue(any("could not be read" in n and "amazonaws.com.cn" in n
                            for n in out["notes"]), out["notes"])
        (queue,) = out["entries"]
        self.assertEqual(queue["address"], "https://sqs.us-east-1.amazonaws.com/123456789012/orders")



class Cl6RoundThreeTest(unittest.TestCase):
    """Regressions from the third adversarial review round."""

    _RDS = "orders.c9akciq32xyz.us-east-1.rds.amazonaws.com"

    @staticmethod
    def _config_map(name, **data):
        body = "".join(f'    {k} = "{v}"\n' for k, v in data.items())
        return (f'resource "kubernetes_config_map" "{name}" {{\n  metadata {{ name = "{name}" }}\n'
                f'  data = {{\n{body}  }}\n}}\n')

    def test_a_password_with_a_slash_or_an_at_sign_never_reaches_the_notes(self):
        import json
        out = _harvest({"cm.tf": self._config_map(
            "orders",
            A=f"postgres://app:Zx9/qLm+w@{self._RDS}:5432/orders",
            B=f"postgres://app:p@ss@{self._RDS}:5432/orders",
            C="redis://:s3cr/et@master.sessions.abc123.use1.cache.amazonaws.com:6379/0",
            # Unreadable AND credentialled: the note shows the host on.
            D="postgres://app:Zx9/qLm+w@orders.c9akciq32xyz.cn-north-1.rds.amazonaws.com.cn:5432/x")})
        blob = json.dumps(out["entries"]) + json.dumps(out["notes"])
        for secret in ("Zx9/qLm", "p@ss", "s3cr/et", "app:"):
            self.assertNotIn(secret, blob)
        self.assertEqual(sorted(e["address"] for e in out["entries"]),
                         [self._RDS, "sessions.abc123.use1.cache.amazonaws.com"])
        self.assertTrue(any("could not be read" in n and "amazonaws.com.cn" in n
                            for n in out["notes"]), out["notes"])

    def test_two_hosts_beside_one_declaration_fold_onto_neither(self):
        dev = "orders.czzzzzzzzzzz.us-east-1.rds.amazonaws.com"
        prod = "orders.caaaaaaaaaaa.us-east-1.rds.amazonaws.com"
        files = {"envs/prod/main.tf": 'resource "aws_db_instance" "orders" {\n  identifier = "orders"\n}\n',
                 "a.tf": self._config_map("orders-dev", DB_HOST=dev),
                 "b.tf": self._config_map("orders-prod", DB_HOST=prod)}
        for swap in (False, True):
            if swap:
                files["a.tf"], files["b.tf"] = files["b.tf"], files["a.tf"]
            out = _harvest(files)
            declared = next(e for e in out["entries"] if e["detection"] == "declared")
            hosts = {e["endpoint"]: e for e in out["entries"] if e["detection"] == "referenced"}
            self.assertEqual(sorted(hosts), [prod, dev])
            self.assertEqual(declared["consumers"], [])
            self.assertIsNone(declared.get("endpoint"))
            for host in hosts.values():
                self.assertTrue(any(n.startswith(datastores.AMBIGUOUS_ENDPOINT_NOTE_PREFIX)
                                    for n in host["notes"]), host["notes"])
                self.assertTrue(datastores._foreign(host))

    def test_tooling_buckets_and_proxies_are_not_gating_databases(self):
        out = _harvest({"main.tf": (
            'module "vpc" {\n  source = "s3::https://s3-eu-west-1.amazonaws.com/examplecorp-terraform-modules/vpc.zip"\n}\n'
            'resource "helm_release" "orders" {\n  name = "orders"\n  repository = "https://acme-charts.s3.amazonaws.com/"\n  chart = "orders"\n}\n'
            + self._config_map("orders", PROXY="orders-proxy.proxy-c9akciq32xyz.us-east-1.rds.amazonaws.com:5432"))})
        by_id = {e["identifier"]: e for e in out["entries"]}
        self.assertNotIn("examplecorp-terraform-modules", by_id)
        self.assertEqual(by_id["acme-charts"]["disposition"], "undecided")
        self.assertNotIn("orders-proxy", by_id)
        self.assertFalse(any("could not be read" in n for n in out["notes"]), out["notes"])

    def test_endpoint_forms_that_used_to_read_as_unreadable(self):
        cases = {
            "acme-site.s3-website-us-east-1.amazonaws.com": ("s3", "acme-site", "s3://acme-site"),
            "acme-fast.s3-accelerate.amazonaws.com": ("s3", "acme-fast", "s3://acme-fast"),
            "acme-ds.s3.dualstack.eu-west-1.amazonaws.com": ("s3", "acme-ds", "s3://acme-ds"),
            "s3a://acme-data/warehouse/": ("s3", "acme-data", "s3://acme-data"),
            "orders-0001-001.abc123.0001.use1.cache.amazonaws.com:6379":
                ("elasticache", "orders", "orders.abc123.use1.cache.amazonaws.com"),
            "sessions.serverless.use1.cache.amazonaws.com:6379":
                ("elasticache", "sessions", "sessions.serverless.use1.cache.amazonaws.com"),
        }
        for token, (service, identifier, handle) in cases.items():
            with self.subTest(token=token):
                (found,) = datastores.find_endpoints(f'"{token}"')
                self.assertEqual((found.kind, found.service, found.identifier, found.handle),
                                 ("recorded", service, identifier, handle))
        # A cluster-mode node and its configuration endpoint are one cache.
        out = _harvest({"cm.tf": self._config_map(
            "orders", A="clustercfg.orders.abc123.use1.cache.amazonaws.com:6379",
            B="orders-0001-001.abc123.0001.use1.cache.amazonaws.com:6379")})
        self.assertEqual([e["identifier"] for e in out["entries"]], ["orders"])



class Cl6RoundFourTest(unittest.TestCase):
    """Regressions from the fourth adversarial review round."""

    @staticmethod
    def _config_map(name, **data):
        body = "".join(f'    {k} = "{v}"\n' for k, v in data.items())
        return (f'resource "kubernetes_config_map" "{name}" {{\n  metadata {{ name = "{name}" }}\n'
                f'  data = {{\n{body}  }}\n}}\n')

    def test_the_endpoint_ambiguity_pass_ignores_hosts_the_verdicts_placed(self):
        own = "orders.cxyzabcd1234.us-east-1.rds.amazonaws.com"
        away = "orders.zzzzzzzzzzzz.eu-west-1.rds.amazonaws.com"
        out = _harvest({"main.tf": ('provider "aws" {\n  region = "us-east-1"\n}\n'
                                    'resource "aws_db_instance" "orders" {\n  identifier = "orders"\n}\n'),
                        "a.tf": self._config_map("orders-api", DB_HOST=own),
                        "b.tf": self._config_map("orders-dr", DB_HOST=away)})
        declared = next(e for e in out["entries"] if e["detection"] == "declared")
        # The estate's own host folds; the cross-region one stands apart on
        # its own verdict, not on an ambiguity that never was.
        self.assertEqual((declared["endpoint"], [c["workload"] for c in declared["consumers"]]),
                         (own, ["orders-api"]))
        (apart,) = [e for e in out["entries"] if e["detection"] == "referenced"]
        self.assertEqual(apart["endpoint"], away)
        self.assertFalse(any(n.startswith(datastores.AMBIGUOUS_ENDPOINT_NOTE_PREFIX)
                             for e in out["entries"] for n in e["notes"]))

    def test_hosts_of_two_services_do_not_flag_each_other(self):
        out = _harvest({"cm.tf": self._config_map(
            "orders", DOCS="orders.cluster-c9akciq32xyz.us-east-1.docdb.amazonaws.com",
            DB="orders.c9akciq32xyz.us-east-1.rds.amazonaws.com")})
        self.assertEqual(sorted(e["service"] for e in out["entries"]), ["docdb", "rds"])
        self.assertFalse(any(n.startswith(datastores.AMBIGUOUS_ENDPOINT_NOTE_PREFIX)
                             for e in out["entries"] for n in e["notes"]))

    def test_a_variable_password_beside_a_literal_host_records_the_host(self):
        host = "orders.cxyzabcd1234.us-east-1.rds.amazonaws.com"
        out = _harvest({"cm.tf": self._config_map(
            "orders", URL=f"postgres://app:${{DB_PASSWORD}}@{host}/orders?password:hunter2")})
        (entry,) = out["entries"]
        self.assertEqual(entry["address"], host)
        import json
        self.assertNotIn("hunter2", json.dumps(out["entries"]) + json.dumps(out["notes"]))
        self.assertFalse(any("build the resource name" in n for n in out["notes"]))

    def test_chart_is_a_word_of_a_bucket_name_not_a_substring(self):
        out = _harvest({"cm.tf": self._config_map(
            "x", A="s3://acme-charts/", B="s3://chartered-bank-statements/", C="s3://orgchart-data/")})
        by_id = {e["identifier"]: e["disposition"] for e in out["entries"]}
        self.assertEqual(by_id, {"acme-charts": "undecided", "chartered-bank-statements": "migrate",
                                 "orgchart-data": "migrate"})



class Cl6RoundFiveTest(unittest.TestCase):
    """Regressions from the fifth adversarial review round."""

    def test_a_kubernetes_secret_blocks_values_are_never_typed(self):
        out = _harvest({"secret.tf": '''
resource "kubernetes_secret" "creds" {
  metadata { name = "orders-creds" }
  data = {
    S3_BUCKET    = "hunter2-opaque-value"
    DYNAMO_TABLE = "SuperSecretValue"
    DB_HOST      = "orders.c9akciq32xyz.us-east-1.rds.amazonaws.com"
  }
}
'''})
        self.assertEqual([(e["detection"], e["service"]) for e in out["entries"]],
                         [("referenced", "rds")])
        import json
        self.assertNotIn("hunter2", json.dumps(out["entries"]) + json.dumps(out["notes"]))

    def test_the_s3a_credential_form_records_the_bucket_and_not_the_key(self):
        for token in ("s3a://AKIAIOSFODNN7EXAMPLE:wJalrXUtnFEMI/K7MDENG@acme-data/path",
                      "s3a://AKIAIOSFODNN7EXAMPLE:wJa$lrXUtnFEMI@acme-data/path"):
            with self.subTest(token=token):
                found = datastores.find_endpoints(f'"{token}"')
                self.assertEqual([(f.kind, f.handle) for f in found],
                                 [("recorded", "s3://acme-data")])
                self.assertNotIn("wJa", found[0].token)

    def test_the_guess_handle_and_the_review_handles(self):
        fact = {"service": "s3", "identifier": "acme-logs", "address": "aws_s3_bucket.logs",
                "detection": "declared", "evidence": ["s3.tf"]}
        self.assertEqual(datastores.inferred_handle(fact), "s3:acme-logs")
        self.assertTrue(datastores.is_inferred_handle("s3:acme-logs"))
        for other in ("arn:aws:s3:::acme-logs", "s3://acme-logs", "aws_s3_bucket.logs",
                      "orders.c9akciq32xyz.us-east-1.rds.amazonaws.com"):
            self.assertFalse(datastores.is_inferred_handle(other), other)
        self.assertIsNone(datastores.inferred_handle(dict(fact, identifier_is_fallback=True)))


if __name__ == "__main__":
    unittest.main()
