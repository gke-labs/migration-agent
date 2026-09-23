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

"""Unit tests for runbook selection and the rendered-runbook contract."""

import os
import unittest

from servers.phases.deployment import datamigration as dm
from servers.phases.deployment import runbooks as rb


def entry(**kwargs) -> dict:
    base = {"service": "rds", "identifier": "orders-db",
            "address": "aws_db_instance.orders", "disposition": "migrate"}
    base.update(kwargs)
    return base


class SelectionTest(unittest.TestCase):

    def test_rds_engine_decides_the_file(self):
        for engine, expected in (
                # The spellings an operator answers the tool's own question
                # with, alongside the ones a declaration states.
                ("SQL Server", "rds-sqlserver"),
                ("PostgreSQL", "rds-postgres"),
                ("MariaDB", "rds-mariadb"),
                ("postgres", "rds-postgres"),
                ("POSTGRES", "rds-postgres"),
                ("mysql", "rds-mysql"),
                ("mariadb", "rds-mariadb"),
                ("sqlserver-se", "rds-sqlserver"),
                ("sqlserver-ex", "rds-sqlserver")):
            self.assertEqual(rb.name_for(entry(engine=engine)), expected,
                             f"engine {engine}")

    def test_rds_without_an_engine_selects_nothing(self):
        """The majority engine is PostgreSQL and guessing it would hand an
        Oracle instance a pglogical procedure."""
        self.assertIsNone(rb.name_for(entry(engine=None)))
        self.assertIsNone(rb.name_for(entry(engine="  ")))

    def test_rds_oracle_selects_nothing(self):
        self.assertIsNone(rb.name_for(entry(engine="oracle-se2")))

    def test_normalisation_does_not_swallow_the_aurora_engines(self):
        """Dropping punctuation must not make `aurora-mysql` look like
        `mysql`: Aurora is an escalation and has no procedure here."""
        self.assertIsNone(rb.name_for(entry(engine="aurora-mysql")))
        self.assertIsNone(rb.name_for(entry(engine="aurora-postgresql")))

    def test_one_procedure_per_simple_service(self):
        for service, expected in (("s3", "s3"), ("efs", "efs"),
                                  ("fsx", "fsx"), ("memorydb", "memorydb"),
                                  ("secretsmanager", "secretsmanager"),
                                  ("ssm", "ssm"),
                                  ("elasticache", "elasticache-persistent")):
            self.assertEqual(rb.name_for(entry(service=service)), expected)

    def test_escalate_and_replatform_services_have_none(self):
        """And each says why. "Not found" reads as a gap in the tool; "this is
        a re-platform" is the answer."""
        for service in ("aurora", "docdb", "neptune", "dynamodb", "redshift",
                        "kinesis", "firehose", "msk", "mq", "sqs", "sns",
                        "eventbridge", "opensearch"):
            self.assertIsNone(rb.name_for(entry(service=service)), service)
            self.assertGreater(len(rb.NO_RUNBOOK_REASON.get(service, "")), 40,
                               f"{service} has no stated reason")

    def test_every_selectable_name_has_a_file(self):
        """The mapping and the directory cannot drift: a name with no file is
        an offer the tool makes and then cannot honour."""
        names = {name for _, name in rb._RDS_ENGINES}
        names |= set(rb._BY_SERVICE.values())
        for name in names:
            self.assertTrue(
                os.path.exists(os.path.join(rb.RUNBOOK_DIR, f"{name}.md")),
                f"{name}.md is selectable but not shipped")

    def test_no_service_is_both_offered_and_refused(self):
        """A service in NO_RUNBOOK_REASON with a file behind it would print
        the refusal after the listing had already offered help."""
        for service in rb.NO_RUNBOOK_REASON:
            self.assertIsNone(rb.name_for(entry(service=service)), service)

    def test_every_shipped_file_is_reachable(self):
        """The other direction: a file nothing selects is dead weight that
        looks like coverage."""
        selectable = {name for _, name in rb._RDS_ENGINES}
        selectable |= set(rb._BY_SERVICE.values())
        for filename in os.listdir(rb.RUNBOOK_DIR):
            if not filename.endswith(".md") or filename == "README.md":
                continue
            self.assertIn(filename[:-3], selectable,
                          f"{filename} is shipped but nothing selects it")

    def test_gating_services_are_covered_or_explained(self):
        """Every service the harvester grades `migrate` either has a procedure
        or says why it has none. A gating service with neither is a workload
        blocked with nothing to hand the person unblocking it."""
        from servers.phases.discovery.discovery_init_1 import datastores
        for service, disposition in datastores.DISPOSITIONS.items():
            if disposition != dm.GATING_DISPOSITION:
                continue
            has_file = rb.name_for(entry(service=service, engine="postgres"
                                         if service == "rds" else None))
            self.assertTrue(has_file or service in rb.NO_RUNBOOK_REASON,
                            f"{service} gates a workload with no procedure "
                            "and no stated reason")


class LoadTest(unittest.TestCase):

    def test_every_shipped_runbook_states_the_user_runs_it(self):
        """The rule that matters most, in the document the operator reads."""
        for name in sorted(set(rb._BY_SERVICE.values())
                           | {n for _, n in rb._RDS_ENGINES}):
            text = rb.load(name)
            self.assertIn("yours to run", text, name)

    def test_every_shipped_runbook_declares_its_placeholders(self):
        """A placeholder used in the steps but absent from the substitution
        table is one the agent has to reverse-engineer."""
        for name in sorted(set(rb._BY_SERVICE.values())
                           | {n for _, n in rb._RDS_ENGINES}):
            text = rb.load(name)
            table, _, body = text.partition("## 1.")
            if not body:
                # fsx.md leads with the flavour decision rather than a step 1.
                table, _, body = text.partition("## Lustre")
            for placeholder in rb.unresolved(body):
                self.assertIn(placeholder, table,
                              f"{name}: {placeholder} is used but not declared")

    def test_no_template_hardcodes_a_bare_address_in_a_tool_call(self):
        """The calls inside a saved runbook are read weeks later by an
        operator with no session behind them. `address="…"` alone is refused
        for any address two root modules declare, and the working call was in
        a chat response that is gone — so the templates carry `<TARGET_ARGS>`,
        which the hand-out fills with the full targeting clause."""
        for name in sorted(set(rb._BY_SERVICE.values())
                           | {n for _, n in rb._RDS_ENGINES}):
            text = rb.load(name)
            self.assertNotIn('address="<', text, f"{name}: bare address")
            for call in ("mark_data_service_migrated(",
                         "mark_data_service_migrating(",
                         "annotate_data_dependency("):
                for line in text.splitlines():
                    if call in line:
                        self.assertIn("<TARGET_ARGS>", line,
                                      f"{name}: {line.strip()}")

    def test_every_runbook_carries_the_required_sections(self):
        """The README's contract, enforced. A runbook without a rollback is a
        one-way door dressed up as a procedure, and `fsx.md` shipped without
        one — the file whose moves (SnapMirror, robocopy, a ZFS send) are the
        hardest of the set to reverse."""
        for name in sorted(set(rb._BY_SERVICE.values())
                           | {n for _, n in rb._RDS_ENGINES}):
            text = rb.load(name)
            for heading in ("## Substitutions", "alidation gates",
                            "## Rollback", "## Known limitations"):
                self.assertIn(heading, text, f"{name} has no {heading}")
            self.assertIn("**Cutover class:**", text,
                          f"{name} does not state its cutover class")

    def test_every_declared_placeholder_is_used(self):
        """The other direction from the test above it. `save` refuses while
        any placeholder remains, including one that only ever appears in the
        substitution table — so a declared-but-unused row is a question the
        agent has to put to the operator for a value nothing spends."""
        for name in sorted(set(rb._BY_SERVICE.values())
                           | {n for _, n in rb._RDS_ENGINES}):
            text = rb.load(name)
            table, _, body = text.partition("## 1.")
            if not body:
                table, _, body = text.partition("## Lustre")
            for placeholder in rb.unresolved(table):
                self.assertIn(placeholder, body,
                              f"{name}: {placeholder} is declared but no step "
                              "uses it")

    def test_no_runbook_asks_for_a_credential_as_a_placeholder(self):
        """A rendered runbook is saved to the ledger and shown in the review
        UI, and `save` refuses while a placeholder is unresolved — so a
        placeholder named for a secret is a demand that the secret be typed
        into the transcript and stored. Credentials are referenced by PATH or
        left to a `--prompt-for-password` flag; `rds-sqlserver.md` shipped a
        `<TARGET_ROOT_PASSWORD>` before this test existed."""
        # Words that name a VALUE. "SECRET" is deliberately absent:
        # `<GCP_SECRET>` is the name of a Secret Manager resource, which is
        # not sensitive and belongs in the document.
        secretish = ("PASSWORD", "PASSWD", "TOKEN", "CREDENTIAL", "APIKEY")
        for name in sorted(set(rb._BY_SERVICE.values())
                           | {n for _, n in rb._RDS_ENGINES}):
            for placeholder in rb.unresolved(rb.load(name)):
                body = placeholder.strip("<>")
                if not any(word in body for word in secretish):
                    continue
                self.assertTrue(
                    body.endswith("_FILE") or body.endswith("_PATH"),
                    f"{name}: {placeholder} names a credential value; use a "
                    "file path or a prompt flag instead")

    def test_missing_file_raises_rather_than_returning_empty(self):
        with self.assertRaises(OSError):
            rb.load("no-such-runbook")


class RenderedTest(unittest.TestCase):

    def test_unresolved_finds_placeholders_and_ignores_prose(self):
        self.assertEqual(
            rb.unresolved("copy <SOURCE_HOST> to <TARGET_BUCKET> now"),
            ["<SOURCE_HOST>", "<TARGET_BUCKET>"])
        self.assertEqual(rb.unresolved("a <b> c <Ab> d <AB> e"), [])
        self.assertEqual(rb.unresolved(""), [])
        self.assertEqual(rb.unresolved(None), [])

    def test_blob_name_carries_the_directory_when_there_is_one(self):
        """Two root modules can declare the same database. One file for both
        would say prod moved when dev did."""
        prod = entry(evidence=["envs/prod/main.tf"])
        dev = entry(evidence=["envs/dev/main.tf"])
        self.assertNotEqual(rb.rendered_blob(prod), rb.rendered_blob(dev))
        self.assertEqual(rb.rendered_blob(prod),
                         "platform/deployment/runbooks/"
                         "envs-prod--rds-orders-db--aws-db-instance-orders.md")

    def test_two_blocks_with_one_name_in_one_directory_do_not_collide(self):
        """`key_of` keys on the address as well, and the file has to. One
        root module can declare `aws_db_instance.orders` and
        `module.db.aws_db_instance.this` both identified `orders-db`."""
        first = entry(evidence=["main.tf"])
        second = entry(address="module.db.aws_db_instance.this",
                       evidence=["main.tf"])
        self.assertNotEqual(rb.rendered_blob(first), rb.rendered_blob(second))

    def test_blob_name_survives_an_awkward_identifier(self):
        self.assertEqual(
            rb.rendered_blob(entry(service="s3", identifier="shop.orders/v2",
                                   address="aws_s3_bucket.shop",
                                   evidence=["main.tf"])),
            "platform/deployment/runbooks/"
            "s3-shop-orders-v2--aws-s3-bucket-shop.md")

    def test_blob_name_falls_back_rather_than_producing_a_bare_prefix(self):
        self.assertEqual(rb.rendered_blob({"service": "s3"}),
                         "platform/deployment/runbooks/s3-unnamed--unnamed.md")


class FactsTest(unittest.TestCase):

    def test_only_stated_facts_are_offered(self):
        """A null means the declaration used a variable. Printed as a fact it
        becomes a runbook that states a version nobody declared."""
        self.assertEqual(
            rb.facts(entry(engine="postgres", engine_version=None,
                           allocated_storage=100, storage_type="")),
            [("engine", "postgres"), ("declared size (GiB)", "100")])

    def test_multi_az_false_is_a_fact(self):
        """`False` is what the declaration said, not a missing value — and it
        is the one that decides whether the cutover needs a failover step."""
        self.assertIn(("multi-AZ", "False"), rb.facts(entry(multi_az=False)))

    def test_offer_names_the_service_the_engine_and_the_target(self):
        self.assertEqual(rb.offer(entry(engine="postgres"), "Cloud SQL"),
                         "orders-db (RDS, postgres) → Cloud SQL")

    def test_the_offer_says_the_engine_the_way_the_declaration_does(self):
        """The match key drops punctuation; the operator is asked about a
        database, not about `sqlserverse`."""
        self.assertEqual(rb.offer(entry(engine="sqlserver-se"), "Cloud SQL"),
                         "orders-db (RDS, sqlserver-se) → Cloud SQL")

    def test_offer_without_an_engine_says_nothing_about_one(self):
        self.assertEqual(rb.offer(entry(service="s3", identifier="exports",
                                        engine=None), "Cloud Storage"),
                         "exports (S3) → Cloud Storage")


def entry_selecting(name: str) -> dict:
    """A representative entry for each shipped runbook."""
    for prefix, selected in rb._RDS_ENGINES:
        if selected == name:
            return entry(service="rds", engine=prefix)
    for service, selected in rb._BY_SERVICE.items():
        if selected == name:
            return entry(service=service, engine=None)
    raise AssertionError(f"no entry selects {name}")


class AnnouncedCutoverClassTest(unittest.TestCase):
    """What the listing announces has to match what the runbook says.

    The worklist said "Cloud SQL (Database Migration Service, continuous)" over
    a MariaDB database whose own procedure opens "There is no continuous path".
    An operator sizing the window from the announcement plans for the class the
    knowledge document defines as "stop writes, wait for lag, minutes", and
    needs hours with the writers stopped. The label is load-bearing.
    """

    def _cutover_class(self, name: str) -> str:
        """The file's own declared class line, which every runbook carries and
        `test_every_runbook_carries_the_required_sections` enforces.

        Read from that line rather than from free prose: the first version of
        this test grepped the body for "no continuous path" and was silently
        inert for `elasticache-persistent.md`, which says "No continuous
        replication path" instead. A guard that depends on phrasing covers
        whichever files happened to be written when it was added.
        """
        for line in rb.load(name).splitlines():
            if line.startswith("**Cutover class:**"):
                return line[len("**Cutover class:**"):].strip().lower()
        raise AssertionError(f"{name} declares no cutover class")

    def test_no_runbook_is_announced_as_the_class_it_denies(self):
        for name in sorted(set(rb._BY_SERVICE.values())
                           | {n for _, n in rb._RDS_ENGINES}):
            declared = self._cutover_class(name)
            _, how = dm.target_and_how(entry_selecting(name))
            file_says_continuous = ("continuous" in declared
                                    and "no continuous" not in declared)
            announced = how.lower()
            announced_continuous = ("continuous" in announced
                                    and "no continuous" not in announced)
            self.assertEqual(
                file_says_continuous, announced_continuous,
                f"{name}: declares {declared!r} and is announced as {how!r}")

    def test_an_unknown_engine_is_not_announced_as_continuous(self):
        """The branch the test above cannot reach, because its fixtures all
        select a runbook. An RDS whose Terraform builds the engine from a
        variable selects none, and the service-level default says "Database
        Migration Service, continuous" — over what may be a MariaDB needing
        hours with the writers stopped."""
        _, how = dm.target_and_how(entry(engine=None))
        self.assertIn("depends on the engine", how)
        self.assertIn("no continuous path for MariaDB", how)


class TargetingTest(unittest.TestCase):
    """Both printed calls name one entry, or neither does."""

    def test_a_duplicated_address_gets_the_narrowing_arguments(self):
        prod = entry(evidence=["envs/prod/main.tf"])
        dev = entry(evidence=["envs/dev/main.tf"])
        repeated, pairs = dm.ambiguity([prod, dev])

        self.assertEqual(
            dm.runbook_call(prod, repeated, pairs),
            'get_data_migration_runbook(address="aws_db_instance.orders", '
            'directory="envs/prod")')
        # The two builders agree, which is the point of one shared helper:
        # `_target` refuses the same call for both tools.
        self.assertIn('directory="envs/prod"',
                      dm.reporting_call(prod, repeated, pairs))

    def test_a_unique_address_stays_short(self):
        only = entry(evidence=["main.tf"])
        repeated, pairs = dm.ambiguity([only])
        self.assertEqual(
            dm.runbook_call(only, repeated, pairs),
            'get_data_migration_runbook(address="aws_db_instance.orders")')


class NeedsEngineTest(unittest.TestCase):

    def test_only_rds_can_be_missing_an_engine(self):
        self.assertTrue(rb.needs_engine(entry(engine=None)))
        self.assertTrue(rb.needs_engine(entry(engine="   ")))
        self.assertFalse(rb.needs_engine(entry(engine="postgres")))
        self.assertFalse(rb.needs_engine(entry(service="dynamodb")))

    def test_a_declared_engine_cloud_sql_cannot_run_is_its_own_case(self):
        """Three kinds of "no RDS runbook", and folding any two together
        misinforms: asking which engine an instance declared as `oracle-se2`
        runs reads as not having looked."""
        self.assertFalse(rb.needs_engine(entry(engine="oracle-se2")))
        self.assertEqual(rb.unsupported_engine(entry(engine="oracle-se2")),
                         "oracle-se2")
        self.assertIsNone(rb.unsupported_engine(entry(engine=None)))
        self.assertIsNone(rb.unsupported_engine(entry(engine="postgres")))
        self.assertIsNone(rb.unsupported_engine(entry(service="s3",
                                                      engine=None)))

    def test_the_worklist_calls_an_engine_change_what_it_is(self):
        text = dm.runbook([entry(engine="oracle-se2")], dm.empty_document(),
                          "acme", "2026-09-04T00:00:00Z")
        self.assertIn("no `oracle-se2` engine", text)
        self.assertIn("engine change", text)
        self.assertNotIn("Name the engine", text)


class WorklistTest(unittest.TestCase):
    """The procedure line in the artifact a later reader opens."""

    def test_names_the_shipped_file_when_nothing_is_rendered_yet(self):
        text = dm.runbook([entry(engine="postgres")], dm.empty_document(),
                          "acme", "2026-09-04T00:00:00Z")
        self.assertIn("`rds-postgres.md` ships with the server", text)
        self.assertIn("get_data_migration_runbook", text)

    def test_names_the_rendered_copy_once_it_exists(self):
        owed = entry(engine="postgres")
        text = dm.runbook([owed], dm.empty_document(), "acme",
                          "2026-09-04T00:00:00Z",
                          rendered={rb.rendered_blob(owed)})
        self.assertIn(rb.rendered_blob(owed), text)
        self.assertNotIn("not yet adapted", text)

    def test_says_why_there_is_none_rather_than_staying_silent(self):
        text = dm.runbook([entry(service="dynamodb", engine=None)],
                          dm.empty_document(), "acme", "2026-09-04T00:00:00Z")
        self.assertIn("no generated procedure", text)
        self.assertIn("re-platform", text)

    def test_an_undeclared_engine_is_a_question_not_a_dead_end(self):
        """Four RDS procedures ship and one of them is this database's. The
        reader three weeks later must not be told there is none."""
        text = dm.runbook([entry(engine=None)], dm.empty_document(), "acme",
                          "2026-09-04T00:00:00Z")
        self.assertIn("does not state an engine", text)
        self.assertIn("Name the engine", text)
        self.assertNotIn("no generated procedure", text)


if __name__ == "__main__":
    unittest.main()
