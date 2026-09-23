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

"""Tests for the workload -> data service join."""

import os
import re
import tempfile
import unittest

from servers.phases.discovery.discovery_init_1 import consumers, datastores


def _harvest(files: dict, scope: dict = None) -> list:
    """Runs the whole pipeline over a synthetic checkout, returns entries."""
    with tempfile.TemporaryDirectory() as root:
        for rel_path, content in files.items():
            full = os.path.join(root, rel_path)
            os.makedirs(os.path.dirname(full), exist_ok=True)
            with open(full, "w") as f:
                f.write(content)
        inventory = {"data_dependencies": []}
        datastores.harvest_datastores(inventory, root, scope)
        return inventory["data_dependencies"]


def _by_identifier(entries: list, identifier: str) -> dict:
    for entry in entries:
        if entry["identifier"] == identifier:
            return entry
    raise AssertionError(
        f"no entry {identifier!r} in {[e['identifier'] for e in entries]}")


class TerraformWiringTest(unittest.TestCase):
    """The retail-store shape: a helm_release referencing a module output."""

    WIRED = {
        "infra/deps/main.tf": '''
module "orders_rds" {
  source     = "terraform-aws-modules/rds/aws"
  identifier = "acme-orders"
}
''',
        "infra/deps/outputs.tf": '''
output "orders_endpoint" {
  value = module.orders_rds.cluster_endpoint
}
''',
        "infra/live/main.tf": '''
module "deps" {
  source = "../deps"
}

resource "helm_release" "orders" {
  name      = "orders"
  chart     = "../../charts/orders"
  namespace = "orders"

  values = [
    templatefile("values.yaml", {
      database_endpoint = module.deps.orders_endpoint
    })
  ]
}
''',
    }

    def test_a_module_output_resolves_to_the_datastore_that_backs_it(self):
        entry = _by_identifier(_harvest(self.WIRED), "acme-orders")
        self.assertEqual(len(entry["consumers"]), 1)
        consumer = entry["consumers"][0]
        self.assertEqual(consumer["workload"], "orders")
        self.assertEqual(consumer["kind"], "helm_release")
        self.assertEqual(consumer["detection"], "terraform_wiring")
        self.assertEqual(consumer["namespace"], "orders")
        self.assertEqual(consumer["evidence"], os.path.join("infra", "live", "main.tf"))

    def test_the_chart_path_is_resolved_against_the_file_that_declares_it(self):
        """The link to a component scope, which is a set of paths. A relative
        chart path is meaningless without the file it was written in."""
        entry = _by_identifier(_harvest(self.WIRED), "acme-orders")
        self.assertEqual(entry["consumers"][0]["source_path"],
                         os.path.join("charts", "orders"))

    def test_an_output_whose_value_spans_lines_still_resolves(self):
        """Assignments are read a line at a time, so an output holding a map
        would resolve to the `{` it opens with. The whole block body is
        searched instead."""
        files = dict(self.WIRED)
        files["infra/deps/outputs.tf"] = '''
output "orders_endpoint" {
  value = {
    host = module.orders_rds.cluster_endpoint
    port = module.orders_rds.cluster_port
  }
}
'''
        entry = _by_identifier(_harvest(files), "acme-orders")
        self.assertEqual([c["workload"] for c in entry["consumers"]], ["orders"])

    GROUPED = {
        "deps/main.tf": '''
module "orders_rds" {
  source     = "terraform-aws-modules/rds/aws"
  identifier = "acme-orders"
}
module "catalog_rds" {
  source     = "terraform-aws-modules/rds/aws"
  identifier = "acme-catalog"
}
''',
        "deps/outputs.tf": '''
output "endpoints" {
  value = {
    orders  = module.orders_rds.cluster_endpoint
    catalog = module.catalog_rds.cluster_endpoint
  }
}
''',
    }

    def test_a_subscripted_map_output_attributes_only_its_own_key(self):
        """A grouped `endpoints` map with one entry per service is an ordinary
        way to publish them. Expanding the whole output body for any reference
        into it makes `endpoints["orders"]` and `endpoints["catalog"]` the same
        reference, so every consumer of any key collects every datastore in the
        map — an orders migration parks the catalog team and vice versa. This
        is the only over-attribution that links two different teams."""
        files = dict(self.GROUPED)
        files["live/main.tf"] = '''
module "deps" { source = "../deps" }

resource "helm_release" "orders" {
  name = "orders"
  set { value = module.deps.endpoints["orders"] }
}

resource "helm_release" "catalog" {
  name = "catalog"
  set { value = module.deps.endpoints["catalog"] }
}
'''
        entries = _harvest(files)
        self.assertEqual(
            [c["workload"] for c in _by_identifier(entries, "acme-orders")["consumers"]],
            ["orders"])
        self.assertEqual(
            [c["workload"] for c in _by_identifier(entries, "acme-catalog")["consumers"]],
            ["catalog"])

    def test_every_map_shape_attributes_only_the_subscripted_key(self):
        """A line-based reader handles only the flat multi-line map. A map of
        OBJECTS puts the key line as `orders = {` with its contents a level
        down, and a one-line map puts every key after the same `=` — both make
        the reader find no entry, fall back to the whole body, and hand every
        key's datastore to every key's consumer. Delimiter counting covers all
        of them."""
        live = '''
module "deps" { source = "../deps" }
resource "helm_release" "orders" {
  name = "orders"
  set { value = module.deps.endpoints["orders"].host }
}
resource "helm_release" "catalog" {
  name = "catalog"
  set { value = module.deps.endpoints["catalog"].host }
}
'''
        shapes = {
            "objects across lines": '''
output "endpoints" {
  value = {
    orders = {
      host = module.orders_rds.cluster_endpoint
    }
    catalog = {
      host = module.catalog_rds.cluster_endpoint
    }
  }
}
''',
            "objects inline": '''
output "endpoints" {
  value = {
    orders  = { host = module.orders_rds.cluster_endpoint,  port = 5432 }
    catalog = { host = module.catalog_rds.cluster_endpoint, port = 3306 }
  }
}
''',
            "whole map on one line": '''
output "endpoints" {
  value = { orders = module.orders_rds.x, catalog = module.catalog_rds.x }
}
''',
        }
        for label, outputs in shapes.items():
            with self.subTest(shape=label):
                files = dict(self.GROUPED)
                files["deps/outputs.tf"] = outputs
                files["live/main.tf"] = live
                entries = _harvest(files)
                self.assertEqual(
                    [c["workload"]
                     for c in _by_identifier(entries, "acme-orders")["consumers"]],
                    ["orders"], f"[{label}] catalog's consumer reached orders")
                self.assertEqual(
                    [c["workload"]
                     for c in _by_identifier(entries, "acme-catalog")["consumers"]],
                    ["catalog"], f"[{label}] orders' consumer reached catalog")

    def test_a_datastore_reached_at_the_hop_limit_is_still_recorded(self):
        """The limit exists to stop a cycle in a malformed repository, not to
        discard a datastore whose address is already in hand. Returning
        nothing at the boundary loses the entry AND leaves it claiming no
        workload reaches it."""
        # Each level is a LOCAL wrapper whose source names a service, so every
        # hop is itself a recorded datastore — otherwise there is nothing at
        # the boundary for the walk to record and the fix is untestable.
        files = {}
        for level in range(6):
            inner = (f'module "inner" {{\n  source = "../rds-{level - 1}"\n}}\n'
                     if level else
                     'resource "aws_db_instance" "leaf" {\n'
                     '  identifier = "acme-leaf"\n}\n')
            files[f"rds-{level}/main.tf"] = inner
            value = ("module.inner.e" if level
                     else "aws_db_instance.leaf.address")
            files[f"rds-{level}/outputs.tf"] = (
                'output "e" {\n  value = ' + value + '\n}\n')
        files["live/main.tf"] = '''
module "deps" { source = "../rds-5" }
resource "helm_release" "orders" {
  name = "orders"
  set { value = module.deps.e }
}
'''
        entries = _harvest(files)
        # Named by the file each wrapper is declared in, because every one of
        # them carries the same label, `inner`.
        attributed = {(e.get("evidence") or [""])[0]
                      for e in entries if e["consumers"]}
        # The wrapper the walk stopped ON is the assertion. Everything nearer
        # is attributed whether the limit discards its address or not, so a
        # test that only asks whether anything was attributed passes either
        # way — which is what it did.
        self.assertIn(
            os.path.join("rds-2", "main.tf"), attributed,
            "the datastore reached at exactly the hop limit was discarded")
        # And the limit still holds: the hop past it is not recorded.
        self.assertNotIn(os.path.join("rds-1", "main.tf"), attributed)

    def test_the_key_survives_a_module_that_re_exports_the_whole_map(self):
        """`output "endpoints" { value = module.inner.endpoints }` carries no
        subscript of its own, so dropping the caller's key there expands the
        inner map entirely — the same cross-attribution, one hop out."""
        files = dict(self.GROUPED)
        files["mid/main.tf"] = 'module "inner" { source = "../deps" }\n'
        files["mid/outputs.tf"] = ('output "endpoints" {\n'
                                   '  value = module.inner.endpoints\n}\n')
        files["live/main.tf"] = '''
module "deps" { source = "../mid" }
resource "helm_release" "orders" {
  name = "orders"
  set { value = module.deps.endpoints["orders"] }
}
'''
        entries = _harvest(files)
        self.assertEqual(
            [c["workload"] for c in _by_identifier(entries, "acme-orders")["consumers"]],
            ["orders"])
        self.assertEqual(
            _by_identifier(entries, "acme-catalog")["consumers"], [],
            "the key was dropped across the re-export hop")

    def test_an_unsubscripted_reference_to_a_map_still_reaches_every_entry(self):
        """The other direction: a consumer taking the whole map genuinely does
        depend on all of it, so narrowing must not cost that."""
        files = dict(self.GROUPED)
        files["live/main.tf"] = '''
module "deps" { source = "../deps" }

resource "helm_release" "all" {
  name = "all"
  set { value = jsonencode(module.deps.endpoints) }
}
'''
        entries = _harvest(files)
        for identifier in ("acme-orders", "acme-catalog"):
            self.assertEqual(
                [c["workload"] for c in _by_identifier(entries, identifier)["consumers"]],
                ["all"], f"{identifier} lost its consumer")

    def test_a_variable_key_is_not_captured_and_reaches_every_entry(self):
        """`endpoints[var.which]` has no literal key, so the subscript group
        does not match and no narrowing is attempted at all. Named for what it
        actually exercises: an earlier version of this called itself the
        unknown-key fallback test, but never reached that branch."""
        files = dict(self.GROUPED)
        files["live/main.tf"] = '''
module "deps" { source = "../deps" }

resource "helm_release" "app" {
  name = "app"
  set { value = module.deps.endpoints[var.which] }
}
'''
        entries = _harvest(files)
        for identifier in ("acme-orders", "acme-catalog"):
            self.assertEqual(
                [c["workload"] for c in _by_identifier(entries, identifier)["consumers"]],
                ["app"])

    def test_a_key_selected_from_a_list_output_falls_back(self):
        """A list is not a map, so a subscripted reference into one has no key
        to select and must reach everything the list names.

        Named for the list, because that is what the fixture exercises. An
        earlier version called itself the `for`-expression fallback test while
        referencing a list output, so the `for` output it also declared was
        never read — and the `for` case has its own test below.
        """
        files = dict(self.GROUPED)
        files["deps/outputs.tf"] = '''
output "raw" {
  value = [module.orders_rds.cluster_endpoint, module.catalog_rds.cluster_endpoint]
}
'''
        files["live/main.tf"] = '''
module "deps" { source = "../deps" }

resource "helm_release" "app" {
  name = "app"
  set { value = module.deps.raw["orders"] }
}
'''
        entries = _harvest(files)
        for identifier in ("acme-orders", "acme-catalog"):
            self.assertEqual(
                [c["workload"] for c in _by_identifier(entries, identifier)["consumers"]],
                ["app"], f"{identifier} lost its consumer to an over-eager narrowing")

    def test_a_conditional_map_output_isolates_its_keys(self):
        """`var.enabled ? { ... } : {}` is the routine spelling for a
        conditionally-created resource. Both arms are map literals, but their
        braces are preceded by `?` and `:` rather than `=` — so a value-position
        test missing those characters classifies both as block bodies, parses
        no keys at all, and falls back to the whole body. Unlike the
        for-expression fallback, both arms hold live datastore references, so
        that fallback is a genuine cross-attribution.

        Third member of the family, after the `=>` of a for-expression and an
        output's `precondition`. The set of value-position delimiters IS the
        contract: one missing character silently changes the answer.
        """
        for label, value in {
            "gated map with an empty else": '''var.enabled ? {
    orders  = module.orders_rds.cluster_endpoint
    catalog = module.catalog_rds.cluster_endpoint
  } : {}''',
            "one key in each arm": '''var.enabled ? {
    orders = module.orders_rds.cluster_endpoint
  } : {
    catalog = module.catalog_rds.cluster_endpoint
  }''',
        }.items():
            with self.subTest(shape=label):
                files = dict(self.GROUPED)
                files["deps/outputs.tf"] = (
                    'output "endpoints" {\n  value = ' + value + '\n}\n')
                files["live/main.tf"] = '''
module "deps" { source = "../deps" }
resource "helm_release" "orders" {
  name = "orders"
  set { value = module.deps.endpoints["orders"] }
}
'''
                entries = _harvest(files)
                self.assertEqual(
                    [c["workload"]
                     for c in _by_identifier(entries, "acme-orders")["consumers"]],
                    ["orders"])
                self.assertEqual(
                    _by_identifier(entries, "acme-catalog")["consumers"], [],
                    f"[{label}] the orders release reached the catalog database")

    def test_a_numeric_map_key_is_read_like_any_other(self):
        """The subscript charset accepts a leading digit and the key charset
        did not, so `versions["5"]` read as "map parsed, key absent" and
        expanded nothing. The comment above the pattern claimed the two
        charsets matched; they differed in the first character only."""
        files = dict(self.GROUPED)
        files["deps/outputs.tf"] = '''
output "endpoints" {
  value = {
    "5" = module.orders_rds.cluster_endpoint
    "6" = module.catalog_rds.cluster_endpoint
  }
}
'''
        files["live/main.tf"] = '''
module "deps" { source = "../deps" }
resource "helm_release" "orders" {
  name = "orders"
  set { value = module.deps.endpoints["5"] }
}
'''
        entries = _harvest(files)
        self.assertEqual(
            [c["workload"] for c in _by_identifier(entries, "acme-orders")["consumers"]],
            ["orders"])
        self.assertEqual(
            _by_identifier(entries, "acme-catalog")["consumers"], [])

    def test_an_output_precondition_is_not_mistaken_for_a_map(self):
        """An `output` may carry a `precondition`, and its `condition = ...` /
        `error_message = ...` lines match the map-key pattern exactly. That
        made a bare re-export — `value = module.db.endpoints` — count as a
        parsed map, so a subscripted reference expanded nothing instead of
        falling back, and the datastore claimed nothing reaches it.

        Same failure as the `for`-expression one, through a different door: a
        sibling BLOCK made a non-map look parsed. Only map literals are walked
        now — a `{` in value position, not one following an identifier.
        """
        files = dict(self.GROUPED)
        files["deps/outputs.tf"] = '''
output "endpoints" {
  value = module.orders_rds.endpoints
  precondition {
    condition     = true
    error_message = "no endpoints"
  }
}
'''
        files["live/main.tf"] = '''
module "deps" { source = "../deps" }
resource "helm_release" "orders" {
  name = "orders"
  set { value = module.deps.endpoints["orders"] }
}
'''
        entries = _harvest(files)
        self.assertEqual(
            [c["workload"] for c in _by_identifier(entries, "acme-orders")["consumers"]],
            ["orders"], "a precondition made the re-export look like a map")

    def test_a_real_map_beside_a_precondition_still_isolates(self):
        """The other direction: skipping block bodies must not skip the map."""
        for label, body in {
            "precondition after the value": '''
  value = {
    orders  = module.orders_rds.cluster_endpoint
    catalog = module.catalog_rds.cluster_endpoint
  }
  precondition {
    condition     = true
    error_message = "no endpoints"
  }
''',
            "precondition before the value": '''
  precondition {
    condition     = true
    error_message = "no endpoints"
  }
  value = {
    orders  = module.orders_rds.cluster_endpoint
    catalog = module.catalog_rds.cluster_endpoint
  }
''',
        }.items():
            with self.subTest(order=label):
                files = dict(self.GROUPED)
                files["deps/outputs.tf"] = 'output "endpoints" {' + body + '}\n'
                files["live/main.tf"] = '''
module "deps" { source = "../deps" }
resource "helm_release" "orders" {
  name = "orders"
  set { value = module.deps.endpoints["orders"] }
}
resource "helm_release" "catalog" {
  name = "catalog"
  set { value = module.deps.endpoints["catalog"] }
}
'''
                entries = _harvest(files)
                self.assertEqual(
                    [c["workload"]
                     for c in _by_identifier(entries, "acme-orders")["consumers"]],
                    ["orders"], f"[{label}] isolation lost")
                self.assertEqual(
                    [c["workload"]
                     for c in _by_identifier(entries, "acme-catalog")["consumers"]],
                    ["catalog"], f"[{label}] isolation lost")

    def test_a_for_expression_output_falls_back_however_it_is_wrapped(self):
        """`k => v` on a line of its own — what `terraform fmt` produces for a
        long `for` — parsed as a key called `k` with the value `> v`. The map
        then counted as parsed, so a reference to a key it does not have
        expanded NOTHING instead of falling back, and the datastore went on to
        claim no workload reaches it while the chain plainly does.

        `k` is a loop variable, not a key. All three spellings must fall back.
        """
        spellings = {
            "one line":
                'value = { for k, v in module.orders_rds.endpoints : k => v }',
            "wrapped": '''value = {
    for k, v in module.orders_rds.endpoints :
    k => v
  }''',
            "with a filter": '''value = {
    for name, cfg in module.orders_rds.instances :
    name => cfg.endpoint
    if cfg.enabled
  }''',
        }
        for label, body in spellings.items():
            with self.subTest(spelling=label):
                files = dict(self.GROUPED)
                files["deps/outputs.tf"] = (
                    'output "endpoints" {\n  ' + body + '\n}\n')
                files["live/main.tf"] = '''
module "deps" { source = "../deps" }
resource "helm_release" "app" {
  name = "app"
  set { value = module.deps.endpoints["writer"] }
}
'''
                entries = _harvest(files)
                self.assertEqual(
                    [c["workload"]
                     for c in _by_identifier(entries, "acme-orders")["consumers"]],
                    ["app"], f"[{label}] the for-expression did not fall back")

    def test_a_composed_map_isolates_keys_in_every_group(self):
        """`merge({ orders = ... }, { catalog = ... })` puts later keys in
        later top-level groups. Stopping at the first close reports them as
        absent, which is indistinguishable from "not a map", so the caller
        falls back and cross-attributes. The tell is the asymmetry: the first
        key isolates correctly and the rest silently do not."""
        files = dict(self.GROUPED)
        files["deps/outputs.tf"] = '''
output "endpoints" {
  value = merge(
    { orders  = module.orders_rds.cluster_endpoint  },
    { catalog = module.catalog_rds.cluster_endpoint },
  )
}
'''
        files["live/main.tf"] = '''
module "deps" { source = "../deps" }
resource "helm_release" "orders" {
  name = "orders"
  set { value = module.deps.endpoints["orders"] }
}
resource "helm_release" "catalog" {
  name = "catalog"
  set { value = module.deps.endpoints["catalog"] }
}
'''
        entries = _harvest(files)
        self.assertEqual(
            [c["workload"] for c in _by_identifier(entries, "acme-orders")["consumers"]],
            ["orders"])
        self.assertEqual(
            [c["workload"] for c in _by_identifier(entries, "acme-catalog")["consumers"]],
            ["catalog"])

    def test_a_partly_parseable_map_does_not_attribute_the_keys_it_did_parse(self):
        """`merge(local.shared, { orders = ... })` parses some keys and not
        others. Treating "map parsed, key absent" as "not a map" falls back to
        the whole body, so the orders release collects the catalog database —
        and the catalog database, left with nothing, goes on to claim nothing
        references it. Both errors from one conflation."""
        files = dict(self.GROUPED)
        files["deps/outputs.tf"] = '''
locals {
  shared = { catalog = module.catalog_rds.endpoint }
}

output "endpoints" {
  value = merge(local.shared, {
    orders = module.orders_rds.endpoint
  })
}
'''
        files["live/main.tf"] = '''
module "deps" { source = "../deps" }
resource "helm_release" "orders" {
  name = "orders"
  set { value = module.deps.endpoints["orders"] }
}
resource "helm_release" "catalog" {
  name = "catalog"
  set { value = module.deps.endpoints["catalog"] }
}
'''
        entries = _harvest(files)
        self.assertEqual(
            [c["workload"] for c in _by_identifier(entries, "acme-orders")["consumers"]],
            ["orders"], "orders collected the catalog team's release")
        self.assertEqual(
            _by_identifier(entries, "acme-catalog")["consumers"], [],
            "catalog was attributed from a key the map does not assign")

    def test_a_map_key_containing_a_dot_still_isolates(self):
        """The subscript pattern accepts a dot and the map-key pattern did
        not, so a map whose keys all contain one parsed as no map at all — and
        the caller fell back to the whole body, handing every key's datastore
        to every key's consumer. Two charsets describing the same thing have
        to agree."""
        files = dict(self.GROUPED)
        files["deps/outputs.tf"] = '''
output "endpoints" {
  value = {
    "orders.db"  = module.orders_rds.cluster_endpoint
    "catalog.db" = module.catalog_rds.cluster_endpoint
  }
}
'''
        files["live/main.tf"] = '''
module "deps" { source = "../deps" }
resource "helm_release" "orders" {
  name = "orders"
  set { value = module.deps.endpoints["orders.db"] }
}
'''
        entries = _harvest(files)
        self.assertEqual(
            [c["workload"] for c in _by_identifier(entries, "acme-orders")["consumers"]],
            ["orders"])
        self.assertEqual(
            _by_identifier(entries, "acme-catalog")["consumers"], [],
            "a dotted key fell back to the whole map")

    def test_a_map_key_starting_with_a_dash_still_resolves(self):
        """The same two charsets, at the first character. A subscript accepts
        a dot or a dash there and the key pattern did not, so `endpoints
        ["-legacy"]` read as a key the map does not assign — the map parsed on
        its other entries, so the reference expanded nothing at all and the
        database it names went on claiming no workload reaches it. The two
        patterns now share one class, which is the only way they stay in
        step."""
        files = dict(self.GROUPED)
        files["deps/outputs.tf"] = '''
output "endpoints" {
  value = {
    "-legacy" = module.orders_rds.cluster_endpoint
    catalog   = module.catalog_rds.cluster_endpoint
  }
}
'''
        files["live/main.tf"] = '''
module "deps" { source = "../deps" }
resource "helm_release" "legacy" {
  name = "legacy"
  set { value = module.deps.endpoints["-legacy"] }
}
'''
        entries = _harvest(files)
        self.assertEqual(
            [c["workload"]
             for c in _by_identifier(entries, "acme-orders")["consumers"]],
            ["legacy"], "a dash-leading key resolved to nothing")
        self.assertEqual(
            _by_identifier(entries, "acme-catalog")["consumers"], [],
            "and it must still isolate rather than fall back to the whole map")

    def test_a_key_absent_from_a_fully_parsed_map_attributes_nothing(self):
        """The map parsed and this key is not in it, so the reference reaches
        nothing known. Expanding the whole body here is the cross-attribution;
        expanding nothing is the under-detection this module prefers."""
        files = dict(self.GROUPED)
        files["deps/outputs.tf"] = '''
output "endpoints" {
  value = {
    orders = module.orders_rds.endpoint
  }
}
'''
        files["live/main.tf"] = '''
module "deps" { source = "../deps" }
resource "helm_release" "catalog" {
  name = "catalog"
  set { value = module.deps.endpoints["catalog"] }
}
'''
        entries = _harvest(files)
        for identifier in ("acme-orders", "acme-catalog"):
            self.assertEqual(
                _by_identifier(entries, identifier)["consumers"], [],
                f"{identifier} was attributed from a key the map does not assign")

    def test_a_consumed_key_is_not_carried_into_the_next_hop(self):
        """Once `_map_entry` resolves a key, the key is used up. Carrying it on
        re-applies it to whatever that entry resolves to — and when that is
        itself a map (`orders = module.db.conf`, `conf = { host = ..., port =
        ... }`) the stale key is absent there, the absent-key state expands
        nothing, and the chain dies with a false unattributed claim."""
        entries = _harvest({
            "deps/main.tf": '''
module "orders_db" {
  source     = "./modules/db"
  identifier = "orders-db"
}
''',
            "deps/outputs.tf": '''
output "endpoints" {
  value = {
    orders = module.orders_db.conf
  }
}
''',
            "deps/modules/db/main.tf": '''
resource "aws_db_instance" "this" {
  identifier = "inner-db"
}
''',
            "deps/modules/db/outputs.tf": '''
output "conf" {
  value = {
    host = aws_db_instance.this.address
    port = 5432
  }
}
''',
            "live/main.tf": '''
module "dependencies" { source = "../deps" }
resource "helm_release" "orders" {
  name = "orders"
  set { value = module.dependencies.endpoints["orders"] }
}
''',
        })
        self.assertEqual(
            [c["workload"] for c in _by_identifier(entries, "inner-db")["consumers"]],
            ["orders"], "a consumed key was re-applied and killed the chain")

    def test_a_comma_inside_a_quoted_value_does_not_split_an_entry(self):
        """Entries are split on delimiters counted in the lexer's mask, where
        string contents are blanked. Splitting on the raw text lets a comma in
        a value start a bogus entry, and a fragment that happens to look like
        `orders = ...` then answers for the real key."""
        files = dict(self.GROUPED)
        files["deps/outputs.tf"] = '''
output "endpoints" {
  value = {
    catalog = "shared,orders = ${module.catalog_rds.cluster_endpoint}"
    orders  = module.orders_rds.cluster_endpoint
  }
}
'''
        files["live/main.tf"] = '''
module "deps" { source = "../deps" }
resource "helm_release" "orders" {
  name = "orders"
  set { value = module.deps.endpoints["orders"] }
}
'''
        entries = _harvest(files)
        self.assertEqual(
            [c["workload"] for c in _by_identifier(entries, "acme-orders")["consumers"]],
            ["orders"])
        self.assertEqual(
            _by_identifier(entries, "acme-catalog")["consumers"], [],
            "a comma inside a quoted value split the map and matched the wrong key")

    def test_a_chart_path_climbing_out_of_the_checkout_is_dropped(self):
        """The schema promises a repository-relative directory, and the field
        exists to be matched against a component scope. `../charts` matches
        nothing in the repository, so it is worse than absent."""
        files = dict(self.WIRED)
        files["infra/live/main.tf"] = files["infra/live/main.tf"].replace(
            'chart     = "../../charts/orders"',
            'chart     = "../../../../elsewhere/orders"')
        entry = _by_identifier(_harvest(files), "acme-orders")
        self.assertIsNone(entry["consumers"][0]["source_path"])

    def test_an_absolute_chart_path_is_dropped(self):
        """A chart on the operator's disk is not in the repository."""
        files = dict(self.WIRED)
        files["infra/live/main.tf"] = files["infra/live/main.tf"].replace(
            'chart     = "../../charts/orders"', 'chart     = "/opt/charts/orders"')
        entry = _by_identifier(_harvest(files), "acme-orders")
        self.assertIsNone(entry["consumers"][0]["source_path"])

    def test_a_registry_chart_has_no_source_path_rather_than_a_fake_one(self):
        files = dict(self.WIRED)
        files["infra/live/main.tf"] = files["infra/live/main.tf"].replace(
            'chart     = "../../charts/orders"', 'chart     = "bitnami/postgresql"')
        entry = _by_identifier(_harvest(files), "acme-orders")
        self.assertIsNone(entry["consumers"][0]["source_path"])

    def test_an_expression_namespace_is_null_not_the_block_label(self):
        """A null is never a guess — the same rule datastores.py applies to an
        engine it cannot read literally."""
        files = dict(self.WIRED)
        files["infra/live/main.tf"] = files["infra/live/main.tf"].replace(
            'namespace = "orders"',
            'namespace = kubernetes_namespace_v1.orders.metadata[0].name')
        entry = _by_identifier(_harvest(files), "acme-orders")
        self.assertIsNone(entry["consumers"][0]["namespace"])

    def test_a_commented_out_reference_does_not_attribute_a_consumer(self):
        """The body is read from the lexer's text view, not the raw bytes.
        Reading raw would make a reference somebody deliberately disabled gate
        a pipeline."""
        files = dict(self.WIRED)
        files["infra/live/main.tf"] = files["infra/live/main.tf"].replace(
            "database_endpoint = module.deps.orders_endpoint",
            "# database_endpoint = module.deps.orders_endpoint")
        entry = _by_identifier(_harvest(files), "acme-orders")
        self.assertEqual(entry["consumers"], [])

    def test_depends_on_is_ordering_not_a_data_dependency(self):
        """`depends_on` on a helm_release is an apply-ordering device — it is
        routinely written on cluster add-ons to serialise them behind the data
        layer. Reading it as a data need holds a metrics-server release on an
        orders-database migration, which is the wrong team entirely."""
        entries = _harvest({"main.tf": '''
module "orders_rds" {
  source     = "terraform-aws-modules/rds/aws"
  identifier = "acme-orders"
}

resource "helm_release" "metrics_server" {
  name       = "metrics-server"
  chart      = "metrics-server"
  depends_on = [module.orders_rds]
}
'''})
        self.assertEqual(
            _by_identifier(entries, "acme-orders")["consumers"], [],
            "an ordering-only reference attributed a consumer")

    def test_a_subscripted_element_does_not_end_the_depends_on_span(self):
        """`depends_on = [aws_eks_addon.this["vpc-cni"], module.orders_rds]`.
        A non-greedy regex ends the span at the subscript's `]`, leaving the
        real reference live — and node groups, namespaces and add-ons are
        routinely `count`/`for_each`-ed, so this is the common spelling rather
        than an exotic one. Extents come from counting in the mask instead."""
        for element in ('aws_eks_addon.this["vpc-cni"]',
                        'kubernetes_namespace.this[0]',
                        'module.eks_managed_node_group["default"]'):
            with self.subTest(element=element):
                entries = _harvest({"main.tf": f'''
module "orders_rds" {{
  source     = "terraform-aws-modules/rds/aws"
  identifier = "acme-orders"
}}

resource "helm_release" "addon" {{
  name       = "aws-load-balancer-controller"
  depends_on = [{element}, module.orders_rds]
}}
'''})
                self.assertEqual(
                    _by_identifier(entries, "acme-orders")["consumers"], [],
                    "a subscript closed the depends_on span early")

    def test_a_nested_block_does_not_end_the_lifecycle_span(self):
        """Same defect on the other branch: a regex ending at the first
        line-leading `}` closes on `precondition`'s brace, so the
        `replace_triggered_by` after it stays live."""
        entries = _harvest({"main.tf": '''
module "orders_rds" {
  source     = "terraform-aws-modules/rds/aws"
  identifier = "acme-orders"
}

resource "helm_release" "metrics" {
  name = "metrics-server"
  lifecycle {
    precondition {
      condition     = true
      error_message = "x"
    }
    replace_triggered_by = [module.orders_rds]
  }
}
'''})
        self.assertEqual(_by_identifier(entries, "acme-orders")["consumers"], [])

    def test_meta_arguments_are_blanked_on_the_irsa_path_too(self):
        """`_ROLE_TYPES`/`_POLICY_TYPES` filtering is the defence against
        expanding non-grants, and an ordering argument walks straight through
        it: a role's `depends_on` naming a policy it never attaches would hand
        the service account everything that policy names."""
        entries = _harvest({"iam.tf": '''
resource "aws_dynamodb_table" "admin_only" {
  name = "acme-admin"
}

resource "aws_iam_policy" "admin" {
  policy = <<EOF
{"Resource": "${aws_dynamodb_table.admin_only.arn}"}
EOF
}

resource "aws_iam_role" "orders" {
  depends_on         = [aws_iam_policy.admin]
  assume_role_policy = <<EOF
{"Condition":{"StringLike":{"oidc:sub":"system:serviceaccount:orders:orders-sa"}}}
EOF
}
'''})
        self.assertEqual(
            _by_identifier(entries, "acme-admin")["consumers"], [],
            "a role's depends_on was read as a policy attachment")

    def test_a_policys_depends_on_is_not_a_grant(self):
        entries = _harvest({"iam.tf": '''
resource "aws_dynamodb_table" "carts" {
  name = "acme-carts"
}

resource "aws_dynamodb_table" "other" {
  name = "acme-other"
}

resource "aws_iam_policy" "p" {
  depends_on = [aws_dynamodb_table.other]
  policy     = <<EOF
{"Resource": "${aws_dynamodb_table.carts.arn}"}
EOF
}

resource "aws_iam_role" "orders" {
  assume_role_policy = <<EOF
{"Condition":{"StringLike":{"oidc:sub":"system:serviceaccount:orders:orders-sa"}}}
EOF
}

resource "aws_iam_role_policy_attachment" "a" {
  role       = aws_iam_role.orders.name
  policy_arn = aws_iam_policy.p.arn
}
'''})
        self.assertEqual(
            [c["workload"] for c in _by_identifier(entries, "acme-carts")["consumers"]],
            ["orders-sa"], "the real grant was lost")
        self.assertEqual(
            _by_identifier(entries, "acme-other")["consumers"], [],
            "a policy's depends_on was read as a grant")

    def test_a_description_naming_a_policy_is_prose_not_a_reference(self):
        entries = _harvest({"iam.tf": '''
resource "aws_dynamodb_table" "carts" {
  name = "acme-carts"
}

resource "aws_iam_policy" "admin" {
  policy = <<EOF
{"Resource": "${aws_dynamodb_table.carts.arn}"}
EOF
}

resource "aws_iam_role" "orders" {
  description        = "superseded by aws_iam_policy.admin, do not attach"
  assume_role_policy = <<EOF
{"Condition":{"StringLike":{"oidc:sub":"system:serviceaccount:orders:orders-sa"}}}
EOF
}
'''})
        self.assertEqual(_by_identifier(entries, "acme-carts")["consumers"], [])

    def test_a_wrapped_tag_map_naming_a_table_is_metadata_not_a_grant(self):
        """`tags` is prose with a value, and a tag map is written across lines
        — so blanking it only to the end of the `tags =` line is the same hole
        as not blanking it at all. Both spellings of that mistake attribute a
        table the policy plainly does not grant: the grant is in `policy`, and
        the tag names what the policy replaced."""
        entries = _harvest({"iam.tf": '''
resource "aws_dynamodb_table" "legacy" {
  name = "acme-legacy"
}

resource "aws_s3_bucket" "orders" {
  bucket = "acme-orders"
}

resource "aws_iam_policy" "orders" {
  tags = {
    SupersededBy = aws_dynamodb_table.legacy.name
  }
  policy = <<EOF
{"Resource": "${aws_s3_bucket.orders.arn}"}
EOF
}

resource "aws_iam_role" "orders" {
  assume_role_policy = <<POL
{"Condition":{"StringLike":{"oidc:sub":"system:serviceaccount:orders:orders-sa"}}}
POL
}

resource "aws_iam_role_policy_attachment" "orders" {
  role       = aws_iam_role.orders.name
  policy_arn = aws_iam_policy.orders.arn
}
'''})
        self.assertEqual(
            [c["workload"]
             for c in _by_identifier(entries, "acme-orders")["consumers"]],
            ["orders-sa"], "the real grant was lost")
        self.assertEqual(
            _by_identifier(entries, "acme-legacy")["consumers"], [],
            "a tag naming a table was read as a grant")

    def test_a_lifecycle_trigger_is_not_a_data_dependency(self):
        entries = _harvest({"main.tf": '''
resource "aws_db_instance" "d" {
  identifier = "acme-orders"
}

resource "helm_release" "unrelated" {
  name = "unrelated"
  lifecycle {
    replace_triggered_by = [aws_db_instance.d]
  }
}
'''})
        self.assertEqual(_by_identifier(entries, "acme-orders")["consumers"], [])

    def test_a_containers_lifecycle_hook_is_not_a_meta_argument(self):
        """`lifecycle` is a Terraform meta-argument at a block's own level and
        a Kubernetes container's own field one level down, holding its
        `pre_stop` hook — and `kubernetes_deployment` is a workload this scan
        reads. A depth-blind blanker deletes the hook, so the database its
        command connects to reports that no workload reaches it."""
        entries = _harvest({"main.tf": '''
resource "aws_db_instance" "d" {
  identifier = "acme-orders"
}

resource "kubernetes_deployment" "app" {
  metadata { name = "orders-api" }
  spec { template { spec { container {
    lifecycle {
      pre_stop { exec { command = ["/bin/sh", "-c", aws_db_instance.d.endpoint] } }
    }
  } } } }
}
'''})
        self.assertEqual(
            [c["workload"]
             for c in _by_identifier(entries, "acme-orders")["consumers"]],
            ["orders-api"], "a container's lifecycle hook was blanked")

    def test_a_map_key_named_tags_is_not_a_meta_argument(self):
        """The same collision without a nested block: `tags` is prose at a
        block's own level and an ordinary ConfigMap key one level down."""
        entries = _harvest({"main.tf": '''
resource "aws_db_instance" "d" {
  identifier = "acme-orders"
}

resource "kubernetes_config_map" "conf" {
  metadata { name = "orders-conf" }
  data = {
    tags = aws_db_instance.d.endpoint
  }
}
'''})
        self.assertEqual(
            [c["workload"]
             for c in _by_identifier(entries, "acme-orders")["consumers"]],
            ["orders-conf"], "a data key called tags was blanked")

    def test_real_wiring_beside_depends_on_still_attributes(self):
        """Guards the two above from passing by blanking too much."""
        entries = _harvest({"main.tf": '''
module "orders_rds" {
  source     = "terraform-aws-modules/rds/aws"
  identifier = "acme-orders"
}

resource "helm_release" "orders" {
  name       = "orders"
  depends_on = [module.eks]
  set { value = module.orders_rds.endpoint }
}
'''})
        self.assertEqual(
            [c["workload"] for c in _by_identifier(entries, "acme-orders")["consumers"]],
            ["orders"])

    def test_a_deploy_block_with_no_second_label_does_not_abort_the_scan(self):
        """Malformed HCL. Every other bad-input path here degrades to a note;
        emitting a null workload would fail schema validation inside
        save_inventory and throw the whole scan away with nothing written."""
        entries = _harvest({"main.tf": '''
resource "aws_db_instance" "d" {
  identifier = "acme-orders"
}

resource "helm_release" {
  set { value = aws_db_instance.d.endpoint }
}
'''})
        entry = _by_identifier(entries, "acme-orders")
        for consumer in entry["consumers"]:
            self.assertIsInstance(consumer["workload"], str)
            self.assertTrue(consumer["workload"])

    def test_an_empty_name_on_an_unlabelled_block_still_names_the_workload(self):
        """`name = ""` is a literal, so it reaches the consumer as the
        workload — and with no block label there is nothing to fall back to.
        Testing only for a missing argument leaves this open: an empty string
        IS a string, so a check on the type alone passes it straight through
        and the schema rejects the finished section."""
        entries = _harvest({"main.tf": '''
resource "aws_db_instance" "d" {
  identifier = "acme-orders"
}

resource "helm_release" {
  name = ""
  set { value = aws_db_instance.d.endpoint }
}
'''})
        entry = _by_identifier(entries, "acme-orders")
        self.assertTrue(entry["consumers"], "the malformed block attributed nothing")
        for consumer in entry["consumers"]:
            self.assertIsInstance(consumer["workload"], str)
            self.assertTrue(consumer["workload"], f"empty workload in {consumer}")

    def test_a_namespace_is_not_a_consumer(self):
        """It contains the workload rather than using the datastore, and
        recording it would attribute the dependency to the wrong thing."""
        files = dict(self.WIRED)
        files["infra/live/ns.tf"] = '''
resource "kubernetes_namespace_v1" "orders" {
  metadata { name = "orders" }
  depends_on = [module.deps.orders_endpoint]
}
'''
        entry = _by_identifier(_harvest(files), "acme-orders")
        self.assertEqual([c["kind"] for c in entry["consumers"]], ["helm_release"])

    def test_a_local_wrapper_module_keeps_the_consumer_it_resolves_through(self):
        """A wrapper whose source path matches a service pattern
        (`./modules/rds`) is recorded as a datastore in its own right, AND the
        resource inside it is recorded as a template. Returning only the leaf
        of the resolution attaches the consumer to the template and leaves the
        real, named entry claiming nothing references it — an affirmative
        falsehood, and the one this module works hardest to avoid."""
        entries = _harvest({
            "infra/main.tf": '''
module "orders_rds" {
  source     = "./modules/rds"
  identifier = "acme-orders"
}

resource "helm_release" "orders" {
  name = "orders"
  set { value = module.orders_rds.endpoint }
}
''',
            "infra/modules/rds/main.tf":
                'resource "aws_db_instance" "this" {\n  identifier = var.identifier\n}\n',
            "infra/modules/rds/outputs.tf":
                'output "endpoint" {\n  value = aws_db_instance.this.endpoint\n}\n',
        })
        named = _by_identifier(entries, "acme-orders")
        self.assertEqual([c["workload"] for c in named["consumers"]], ["orders"])
        self.assertFalse(
            consumers.UNATTRIBUTED_NOTE in named["notes"],
            "the named entry was reported as unattributed")

    def test_an_instance_selector_does_not_stop_the_walk_at_the_wrapper(self):
        """`count` and `for_each` on a module call put a selector between the
        address and the attribute. Read as part of the address it swallows the
        attribute, `_resolve` takes its no-attribute path, and the walk stops
        at the wrapper — so every datastore behind a per-environment module
        reports that neither chain reaches it, while chain (a) plainly does.
        The selector is skipped rather than kept: every instance of one module
        block is that one block."""
        for label, call, reference in (
                ("for_each", 'for_each = toset(["prod"])',
                 'module.deps["prod"].orders_endpoint'),
                ("count", "count = 1", "module.deps[0].orders_endpoint")):
            with self.subTest(meta_argument=label):
                entries = _harvest({
                    "deps/main.tf": '''
module "orders_rds" {
  source     = "terraform-aws-modules/rds/aws"
  identifier = "acme-orders"
}
''',
                    "deps/outputs.tf": '''
output "orders_endpoint" {
  value = module.orders_rds.cluster_endpoint
}
''',
                    "live/main.tf": f'''
module "deps" {{
  {call}
  source = "../deps"
}}

resource "helm_release" "orders" {{
  name = "orders"
  set {{ value = {reference} }}
}}
''',
                })
                self.assertEqual(
                    [c["workload"]
                     for c in _by_identifier(entries, "acme-orders")["consumers"]],
                    ["orders"], f"the {label} selector hid the attribute")

    def test_a_selector_does_not_swallow_a_map_key_on_the_same_reference(self):
        """Both subscripts on one reference: the instance selector, which is
        skipped, and the map key, which is not. Reading the first as the key is
        the failure that matters — it would narrow the map to a key it does not
        have and expand nothing."""
        files = dict(self.GROUPED)
        files["live/main.tf"] = '''
module "deps" {
  for_each = toset(["prod"])
  source   = "../deps"
}

resource "helm_release" "orders" {
  name = "orders"
  set { value = module.deps["prod"].endpoints["orders"] }
}
'''
        entries = _harvest(files)
        self.assertEqual(
            [c["workload"]
             for c in _by_identifier(entries, "acme-orders")["consumers"]],
            ["orders"])
        self.assertEqual(
            _by_identifier(entries, "acme-catalog")["consumers"], [],
            "the instance selector was read as the map key")

    def test_a_module_source_pointing_at_the_checkout_root_resolves(self):
        """Every directory key in the index comes from `os.path.dirname`, which
        spells the checkout root "". `os.path.normpath` spells it "." — so a
        module whose source climbs back to the root recorded a target directory
        no output was ever keyed under, and the database behind it went on
        claiming that no workload reaches it."""
        entries = _harvest({
            "main.tf": '''
module "orders_rds" {
  source     = "terraform-aws-modules/rds/aws"
  identifier = "acme-orders"
}
''',
            "outputs.tf": '''
output "orders_endpoint" {
  value = module.orders_rds.cluster_endpoint
}
''',
            "envs/prod/main.tf": '''
module "root" { source = "../.." }

resource "helm_release" "orders" {
  name = "orders"
  set { value = module.root.orders_endpoint }
}
''',
        })
        self.assertEqual(
            [c["workload"]
             for c in _by_identifier(entries, "acme-orders")["consumers"]],
            ["orders"], "a root-directed module source resolved to nothing")

    def test_two_directories_declaring_the_same_module_name_do_not_share(self):
        """Terraform resolves module.deps against the directory it is written
        in, and the per-environment duplicate-name layout is ordinary. Both
        environments must declare the alias and both must wire a release, or
        there is no ambiguous lookup for a flat key to get wrong — an earlier
        version of this test set up only one side and passed either way.
        """
        files = {}
        for env in ("staging", "prod"):
            files[f"envs/{env}/deps/main.tf"] = (
                'module "db" {\n  source = "terraform-aws-modules/rds/aws"\n'
                f'  identifier = "{env}-db"\n}}\n')
            files[f"envs/{env}/deps/outputs.tf"] = (
                'output "endpoint" {\n  value = module.db.endpoint\n}\n')
            files[f"envs/{env}/main.tf"] = f'''
module "deps" {{
  source = "./deps"
}}

resource "helm_release" "app" {{
  name  = "app-{env}"
  value = module.deps.endpoint
}}
'''
        entries = _harvest(files)
        for env in ("staging", "prod"):
            entry = _by_identifier(entries, f"{env}-db")
            self.assertEqual([c["workload"] for c in entry["consumers"]],
                             [f"app-{env}"],
                             f"{env}-db collected the wrong environment's consumer")


class IrsaTest(unittest.TestCase):
    """The eks-workshop-v2 shape: a role naming a service account, and a
    policy naming the resource. Finds services a pod reaches by name, which
    no endpoint or hostname scan can see."""

    IRSA = {
        "iam.tf": '''
resource "aws_dynamodb_table" "carts" {
  name = "acme-carts"
}

module "carts_role" {
  source                        = "terraform-aws-modules/iam/aws//modules/iam-assumable-role-with-oidc"
  role_policy_arns              = [aws_iam_policy.carts_dynamo.arn]
  oidc_fully_qualified_subjects = ["system:serviceaccount:carts:carts-sa"]
}

resource "aws_iam_policy" "carts_dynamo" {
  name = "carts-dynamo"

  policy = <<EOF
{
  "Statement": [
    {
      "Effect": "Allow",
      "Action": "dynamodb:*",
      "Resource": "arn:aws:dynamodb:us-east-1:111:table/${aws_dynamodb_table.carts.name}"
    }
  ]
}
EOF
}
''',
    }

    def test_a_role_and_its_policy_attribute_the_table_to_its_service_account(self):
        entry = _by_identifier(_harvest(self.IRSA), "acme-carts")
        self.assertEqual(len(entry["consumers"]), 1)
        consumer = entry["consumers"][0]
        self.assertEqual(consumer["workload"], "carts-sa")
        self.assertEqual(consumer["namespace"], "carts")
        self.assertEqual(consumer["kind"], "service_account")
        self.assertEqual(consumer["detection"], "irsa")

    def test_the_lexer_separates_heredoc_data_from_heredoc_references(self):
        """Both things the join needs out of a policy document, and they pull
        in opposite directions. The subject is a literal string, so it can only
        be read from the whole body. A reference is live Terraform, so it must
        be read from the interpolations alone — the whole body would make
        surrounding prose read as a reference."""
        content = self.IRSA["iam.tf"]
        _text, _mask, _notes, heredocs = datastores.scan_source(content)
        self.assertTrue(heredocs, "the lexer reported no heredoc at all")

        body = "".join(content[start:stop] for start, stop in heredocs)
        self.assertIn("dynamodb:*", body, "the policy document was not restored")

        refs = []
        for start, stop in heredocs:
            refs.extend(datastores.interpolation_spans(content, start, stop))
        narrowed = "".join(content[start:stop] for start, stop in refs)
        self.assertIn("aws_dynamodb_table.carts", narrowed)
        # The JSON scaffolding around it is data, and stays out of the
        # narrowed view even though it is in the body.
        self.assertNotIn("Effect", narrowed)
        self.assertNotIn("dynamodb:*", narrowed)

    def test_a_subscript_in_the_subject_list_does_not_hide_later_subjects(self):
        """The subject list's extent is found by counting brackets. A lazy
        match to the first `]` closes on the subscript in
        `["a:b", local.x["y"], "c:d"]`, and every subject after it goes
        unread — the same defect the meta-argument blanking counts delimiters
        to avoid."""
        entries = _harvest({"iam.tf": '''
resource "aws_dynamodb_table" "carts" {
  name = "acme-carts"
}

resource "aws_iam_policy" "carts" {
  policy = <<EOF
{"Resource": "${aws_dynamodb_table.carts.arn}"}
EOF
}

module "carts_irsa" {
  source           = "terraform-aws-modules/iam/aws//modules/iam-role-for-service-accounts-eks"
  role_policy_arns = { carts = aws_iam_policy.carts.arn }
  oidc_providers = {
    main = {
      namespace_service_accounts = ["carts:first-sa", local.extra["x"], "carts:second-sa"]
    }
  }
}
'''})
        self.assertEqual(
            sorted(c["workload"]
                   for c in _by_identifier(entries, "acme-carts")["consumers"]),
            ["first-sa", "second-sa"],
            "a subscript in the list hid the subjects after it")

    def test_a_bracket_inside_a_quoted_element_does_not_close_the_list(self):
        """The other way the count can close early. A subscript's brackets are
        structure and the count has to follow them; a bracket inside a quoted
        element is data and it must not. The mask already tells the two apart,
        so the count reads it rather than the raw body."""
        entries = _harvest({"iam.tf": '''
resource "aws_dynamodb_table" "carts" {
  name = "acme-carts"
}

resource "aws_iam_policy" "carts" {
  policy = <<EOF
{"Resource": "${aws_dynamodb_table.carts.arn}"}
EOF
}

module "carts_irsa" {
  source           = "terraform-aws-modules/iam/aws//modules/iam-role-for-service-accounts-eks"
  role_policy_arns = { carts = aws_iam_policy.carts.arn }
  oidc_providers = {
    main = {
      namespace_service_accounts = ["carts:legacy]sa", "carts:carts-sa"]
    }
  }
}
'''})
        self.assertEqual(
            [c["workload"]
             for c in _by_identifier(entries, "acme-carts")["consumers"]],
            ["carts-sa"],
            "a bracket inside a quoted element closed the subject list")

    def test_the_bare_subject_form_of_the_current_irsa_module_is_read(self):
        """`iam-role-for-service-accounts-eks` supersedes
        `iam-assumable-role-with-oidc` and is what current estates write. It
        takes the subject unqualified in `namespace_service_accounts`, so a
        pattern matching only `system:serviceaccount:` finds nothing and the
        whole chain goes dark on the module people actually use."""
        entries = _harvest({"iam.tf": '''
resource "aws_dynamodb_table" "carts" {
  name = "acme-carts"
}

resource "aws_iam_policy" "carts_dynamo" {
  policy = <<EOF
{"Resource": "${aws_dynamodb_table.carts.arn}"}
EOF
}

module "carts_irsa" {
  source           = "terraform-aws-modules/iam/aws//modules/iam-role-for-service-accounts-eks"
  role_policy_arns = { carts = aws_iam_policy.carts_dynamo.arn }
  oidc_providers = {
    main = {
      provider_arn               = module.eks.oidc_provider_arn
      namespace_service_accounts = ["carts:carts-sa"]
    }
  }
}
'''})
        entry = _by_identifier(entries, "acme-carts")
        self.assertEqual([(c["workload"], c["namespace"])
                          for c in entry["consumers"]], [("carts-sa", "carts")])

    def test_a_colon_string_outside_that_argument_is_not_a_subject(self):
        """The bare form is read only inside `namespace_service_accounts`.
        Matching `"<a>:<b>"` anywhere would make `"redis:6379"` a service
        account and attribute datastores to a port number."""
        entries = _harvest({"iam.tf": '''
resource "aws_dynamodb_table" "carts" {
  name = "acme-carts"
}

resource "aws_iam_policy" "p" {
  policy = <<EOF
{"Resource": "${aws_dynamodb_table.carts.arn}"}
EOF
}

module "not_a_role" {
  source        = "terraform-aws-modules/iam/aws"
  cache_address = "redis:6379"
  policy_arns   = [aws_iam_policy.p.arn]
}
'''})
        self.assertEqual(_by_identifier(entries, "acme-carts")["consumers"], [])

    def test_the_module_argument_grant_form_is_followed(self):
        """`iam-role-for-service-accounts-eks` takes the resource ARNs as its
        OWN arguments and builds the policy internally, so there is no policy
        block for the second hop to expand. This is the shape in the
        eks-workshop-v2 fixture, where the bucket was reported as referenced by
        nothing while the module right there named its ARN."""
        entries = _harvest({"main.tf": '''
resource "aws_s3_bucket" "mountpoint_s3" {
  bucket = "acme-mountpoint"
}

module "mountpoint_s3_csi_driver_irsa" {
  source                        = "terraform-aws-modules/iam/aws//modules/iam-role-for-service-accounts-eks"
  mountpoint_s3_csi_bucket_arns = [aws_s3_bucket.mountpoint_s3.arn]
  oidc_providers = {
    main = {
      namespace_service_accounts = ["kube-system:s3-csi-driver-sa"]
    }
  }
}
'''})
        entry = _by_identifier(entries, "acme-mountpoint")
        self.assertEqual([(c["workload"], c["namespace"])
                          for c in entry["consumers"]],
                         [("s3-csi-driver-sa", "kube-system")])

    def test_per_app_module_config_does_not_cross_product(self):
        """One module call routinely carries per-app configuration keyed by
        app. Reading the whole body for every subject it names makes that a
        cross-product: carts-sa becomes a consumer of the orders table and
        vice versa. A subject is scoped to the group it was declared in when
        that group carries grants of its own."""
        entries = _harvest({"main.tf": '''
resource "aws_dynamodb_table" "carts" {
  name = "carts-table"
}

resource "aws_dynamodb_table" "orders" {
  name = "orders-table"
}

module "platform" {
  source = "./modules/platform"
  apps = {
    carts = {
      namespace_service_accounts = ["carts:carts-sa"]
      table_arn                  = aws_dynamodb_table.carts.arn
    }
    orders = {
      namespace_service_accounts = ["orders:orders-sa"]
      table_arn                  = aws_dynamodb_table.orders.arn
    }
  }
}
'''})
        self.assertEqual(
            [c["workload"] for c in _by_identifier(entries, "carts-table")["consumers"]],
            ["carts-sa"])
        self.assertEqual(
            [c["workload"] for c in _by_identifier(entries, "orders-table")["consumers"]],
            ["orders-sa"])

    def test_per_app_config_granting_through_per_app_policies_does_not_cross(self):
        """The scoping has to cover the POLICY hop, not just direct ARNs.
        Computing the policy set once from the whole body, outside the subject
        loop, brings the cross-product straight back the moment a per-app map
        grants through a per-app policy — which is as idiomatic as a bare
        ARN."""
        entries = _harvest({"main.tf": '''
resource "aws_dynamodb_table" "carts" {
  name = "carts-table"
}

resource "aws_dynamodb_table" "orders" {
  name = "orders-table"
}

resource "aws_iam_policy" "carts" {
  policy = <<EOF
{"Resource": "${aws_dynamodb_table.carts.arn}"}
EOF
}

resource "aws_iam_policy" "orders" {
  policy = <<EOF
{"Resource": "${aws_dynamodb_table.orders.arn}"}
EOF
}

module "platform" {
  source = "./modules/platform"
  apps = {
    carts = {
      namespace_service_accounts = ["carts:carts-sa"]
      role_policy_arns           = { policy = aws_iam_policy.carts.arn }
    }
    orders = {
      namespace_service_accounts = ["orders:orders-sa"]
      role_policy_arns           = { policy = aws_iam_policy.orders.arn }
    }
  }
}
'''})
        self.assertEqual(
            [c["workload"] for c in _by_identifier(entries, "carts-table")["consumers"]],
            ["carts-sa"])
        self.assertEqual(
            [c["workload"] for c in _by_identifier(entries, "orders-table")["consumers"]],
            ["orders-sa"])

    def test_per_app_scoping_holds_when_the_subject_nests_deeper(self):
        """The subject is often one level below the grant it belongs to —
        `carts = { table_arns = [...], oidc = { namespace_service_accounts =
        [...] } }`. Taking only the innermost group finds no grant in `oidc`,
        and falling straight back to the whole body from there restores the
        cross-product. The scope is the first enclosing group that carries a
        grant, walking outward."""
        tables = '''
resource "aws_dynamodb_table" "carts" {
  name = "carts-tbl"
}

resource "aws_dynamodb_table" "orders" {
  name = "orders-tbl"
}
'''
        shapes = {
            "sibling": '''
  apps = {
    carts = {
      dynamodb_table_arns        = [aws_dynamodb_table.carts.arn]
      namespace_service_accounts = ["carts:carts-sa"]
    }
    orders = {
      dynamodb_table_arns        = [aws_dynamodb_table.orders.arn]
      namespace_service_accounts = ["orders:orders-sa"]
    }
  }
''',
            "one level deeper": '''
  apps = {
    carts = {
      dynamodb_table_arns = [aws_dynamodb_table.carts.arn]
      oidc = {
        namespace_service_accounts = ["carts:carts-sa"]
      }
    }
    orders = {
      dynamodb_table_arns = [aws_dynamodb_table.orders.arn]
      oidc = {
        namespace_service_accounts = ["orders:orders-sa"]
      }
    }
  }
''',
            "two levels deeper": '''
  apps = {
    carts = {
      dynamodb_table_arns = [aws_dynamodb_table.carts.arn]
      oidc = {
        main = {
          namespace_service_accounts = ["carts:carts-sa"]
        }
      }
    }
    orders = {
      dynamodb_table_arns = [aws_dynamodb_table.orders.arn]
      oidc = {
        main = {
          namespace_service_accounts = ["orders:orders-sa"]
        }
      }
    }
  }
''',
        }
        for label, apps in shapes.items():
            with self.subTest(nesting=label):
                entries = _harvest({"main.tf": tables + '''
module "irsa" {
  source = "terraform-aws-modules/iam/aws//modules/iam-role-for-service-accounts-eks"
''' + apps + "}\n"})
                self.assertEqual(
                    [c["workload"]
                     for c in _by_identifier(entries, "carts-tbl")["consumers"]],
                    ["carts-sa"], f"[{label}] carts collected orders' subject")
                self.assertEqual(
                    [c["workload"]
                     for c in _by_identifier(entries, "orders-tbl")["consumers"]],
                    ["orders-sa"], f"[{label}] orders collected carts' subject")

    def test_asymmetric_sibling_apps_do_not_cross_attribute(self):
        """The shapes above are all SYMMETRIC — every app group carries a
        recorded grant — which is why an outward walk passes them while
        cross-attributing here. Asymmetry is ordinary: one app has no grant
        yet, or grants a service the scan does not record. The walk then
        widens past the shared parent map, finds the SIBLING's grant there,
        and hands this service account the other team's table.

        Crossing a group shared with another subject yields no scope at all,
        not the whole body — at that point the block does not say what this
        subject may reach, and saying nothing is the safer answer.
        """
        tables = '''
resource "aws_dynamodb_table" "carts" {
  name = "carts-tbl"
}

resource "aws_dynamodb_table" "orders" {
  name = "orders-tbl"
}
'''
        shapes = {
            "one app is subject-only": '''
    carts = {
      namespace_service_accounts = ["carts:carts-sa"]
    }
    orders = {
      dynamodb_table_arns        = [aws_dynamodb_table.orders.arn]
      namespace_service_accounts = ["orders:orders-sa"]
    }
''',
            "one app grants an unrecorded service": '''
    carts = {
      sqs_queue_arns             = [aws_sqs_queue.jobs.arn]
      namespace_service_accounts = ["carts:carts-sa"]
    }
    orders = {
      dynamodb_table_arns        = [aws_dynamodb_table.orders.arn]
      namespace_service_accounts = ["orders:orders-sa"]
    }
''',
            "asymmetric and nested deeper": '''
    carts = {
      oidc = {
        namespace_service_accounts = ["carts:carts-sa"]
      }
    }
    orders = {
      dynamodb_table_arns = [aws_dynamodb_table.orders.arn]
      oidc = {
        namespace_service_accounts = ["orders:orders-sa"]
      }
    }
''',
        }
        for label, apps in shapes.items():
            with self.subTest(shape=label):
                entries = _harvest({"main.tf": tables + '''
module "irsa" {
  source = "terraform-aws-modules/iam/aws//modules/iam-role-for-service-accounts-eks"
  apps = {
''' + apps + "  }\n}\n"})
                self.assertEqual(
                    [c["workload"]
                     for c in _by_identifier(entries, "orders-tbl")["consumers"]],
                    ["orders-sa"],
                    f"[{label}] the carts service account reached the orders table")

    GRANTS = '''
resource "aws_dynamodb_table" "carts" {
  name = "carts-tbl"
}

resource "aws_dynamodb_table" "orders" {
  name = "orders-tbl"
}

resource "aws_iam_policy" "carts" {
  policy = <<EOF
{"Resource": "${aws_dynamodb_table.carts.arn}"}
EOF
}

resource "aws_iam_policy" "orders" {
  policy = <<EOF
{"Resource": "${aws_dynamodb_table.orders.arn}"}
EOF
}
'''

    def test_a_plain_role_is_never_read_as_per_app(self):
        """Only a module call can hand different apps different things. One
        `aws_iam_role` is one set of permissions however its policies are
        written — but two `inline_policy` blocks look like grant-bearing
        siblings, and a `jsonencode` trust policy with two Statements looks
        like differing group sets. Either misfire drops EVERY grant the role
        has and leaves the entry claiming the IRSA chain never reached it."""
        entries = _harvest({"main.tf": self.GRANTS + '''
resource "aws_iam_role" "app" {
  assume_role_policy = <<EOT
{"Statement":[{"Condition":{"StringEquals":{"oidc:sub":"system:serviceaccount:shop:app"}}}]}
EOT
  inline_policy {
    policy = jsonencode({ Statement = [{ Resource = aws_dynamodb_table.carts.arn }] })
  }
  inline_policy {
    policy = jsonencode({ Statement = [{ Resource = aws_dynamodb_table.orders.arn }] })
  }
}
'''})
        for identifier in ("carts-tbl", "orders-tbl"):
            self.assertEqual(
                [c["workload"] for c in _by_identifier(entries, identifier)["consumers"]],
                ["app"], f"{identifier} lost its grant to a per-app misfire")

    def test_a_two_statement_trust_policy_does_not_void_the_role(self):
        """The other misfire: two OIDC Statements put the subjects in
        different group sets, the per-app walk then breaks on the group
        holding the other subject, and even the attachments are dropped."""
        entries = _harvest({"main.tf": self.GRANTS + '''
resource "aws_iam_role" "app" {
  assume_role_policy = jsonencode({
    Statement = [
      {
        Condition = {
          StringEquals = { "oidc:sub" = "system:serviceaccount:shop:blue" }
        }
      },
      {
        Condition = {
          StringEquals = { "oidc:sub" = "system:serviceaccount:shop:green" }
        }
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "app" {
  role       = aws_iam_role.app.name
  policy_arn = aws_iam_policy.carts.arn
}
'''})
        self.assertEqual(
            sorted(c["workload"]
                   for c in _by_identifier(entries, "carts-tbl")["consumers"]),
            ["blue", "green"])

    def test_per_app_detection_does_not_depend_on_recognising_every_subject(self):
        """Deciding per-app from subject diversity alone only works when the
        scan recognises a subject in more than one app. `_NS_SA_RE` matches a
        quoted literal only, so an app whose subject list is a variable is
        invisible while its grant is still read out of the whole body — and
        the one app the scan CAN see collects every sibling's datastore.

        The block's shape is consulted too: sibling groups that each carry a
        grant are a per-app map however many subjects were recognised.
        """
        shapes = {
            "sibling subject is a variable":
                'namespace_service_accounts = var.orders_service_accounts',
            "sibling subject is interpolated":
                'namespace_service_accounts = ["${var.ns}:orders-sa"]',
            "sibling app has no subject at all": '',
        }
        for label, orders_subject in shapes.items():
            with self.subTest(shape=label):
                entries = _harvest({"main.tf": self.GRANTS + '''
module "irsa" {
  source = "terraform-aws-modules/iam/aws//modules/iam-role-for-service-accounts-eks"
  apps = {
    carts = {
      dynamodb_table_arns        = [aws_dynamodb_table.carts.arn]
      namespace_service_accounts = ["carts:carts-sa"]
    }
    orders = {
      dynamodb_table_arns        = [aws_dynamodb_table.orders.arn]
      ''' + orders_subject + '''
    }
  }
}
'''})
                self.assertEqual(
                    [c["workload"]
                     for c in _by_identifier(entries, "carts-tbl")["consumers"]],
                    ["carts-sa"])
                self.assertEqual(
                    _by_identifier(entries, "orders-tbl")["consumers"], [],
                    f"[{label}] the carts account reached the orders table")

    def test_a_shared_trust_policys_sibling_groups_are_not_per_app(self):
        """The structural test must stay quiet on a shared role. A trust
        policy has sibling groups too — `Condition`, `StringLike` — but they
        name no grants, which is what separates them from an apps map."""
        entries = _harvest({"main.tf": self.GRANTS + '''
resource "aws_iam_role" "shared" {
  assume_role_policy = jsonencode({
    Statement = [
      {
        Condition = {
          StringLike = {
            "oidc:sub" = ["system:serviceaccount:a:a-sa", "system:serviceaccount:b:b-sa"]
          }
        }
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "shared" {
  role       = aws_iam_role.shared.name
  policy_arn = aws_iam_policy.carts.arn
}
'''})
        self.assertEqual(
            sorted(c["workload"]
                   for c in _by_identifier(entries, "carts-tbl")["consumers"]),
            ["a-sa", "b-sa"])

    def test_a_per_app_attachment_does_not_widen_every_subject(self):
        """`_attached_policies` matches on the role address and cannot tell
        one app of a wrapper module from another, so unioning attachments into
        a narrowed scope undoes the narrowing completely. They apply only when
        the scope is the whole block."""
        entries = _harvest({"main.tf": self.GRANTS + '''
module "irsa" {
  source = "terraform-aws-modules/iam/aws//modules/iam-role-for-service-accounts-eks"
  apps = {
    carts = {
      policy = aws_iam_policy.carts.arn
      oidc = {
        namespace_service_accounts = ["carts:carts-sa"]
      }
    }
    orders = {
      policy = aws_iam_policy.orders.arn
      oidc = {
        namespace_service_accounts = ["orders:orders-sa"]
      }
    }
  }
}

resource "aws_iam_role_policy_attachment" "carts" {
  role       = module.irsa.iam_role_name["carts"]
  policy_arn = aws_iam_policy.carts.arn
}
'''})
        self.assertEqual(
            [c["workload"] for c in _by_identifier(entries, "carts-tbl")["consumers"]],
            ["carts-sa"], "an attachment handed the carts table to orders too")

    def test_a_subject_outside_every_app_group_gets_nothing(self):
        """A subject at the block's own level has no enclosing groups, so a
        guard that only inspects enclosing groups can never fire for it — and
        the whole body, which is every app's, becomes its scope. In a per-app
        block such a subject gets nothing."""
        entries = _harvest({"main.tf": self.GRANTS + '''
module "irsa" {
  source                     = "terraform-aws-modules/iam/aws//modules/iam-role-for-service-accounts-eks"
  namespace_service_accounts = ["platform:shared-sa"]
  apps = {
    carts = {
      table_arns = [aws_dynamodb_table.carts.arn]
      oidc = {
        namespace_service_accounts = ["carts:carts-sa"]
      }
    }
  }
}
'''})
        self.assertEqual(
            [c["workload"] for c in _by_identifier(entries, "carts-tbl")["consumers"]],
            ["carts-sa"], "a block-level subject collected an app's table")

    def test_one_account_under_two_providers_is_one_consumer(self):
        """One app across two clusters is one role and one service account,
        not two competing ones. Subjects are deduplicated by identity before
        the per-app question is asked, so this stays a single-subject block
        and the top-level grant still lands."""
        entries = _harvest({"main.tf": self.GRANTS + '''
module "irsa" {
  source              = "terraform-aws-modules/iam/aws//modules/iam-role-for-service-accounts-eks"
  dynamodb_table_arns = [aws_dynamodb_table.carts.arn]
  oidc_providers = {
    east = {
      namespace_service_accounts = ["carts:carts-sa"]
    }
    west = {
      namespace_service_accounts = ["carts:carts-sa"]
    }
  }
}
'''})
        self.assertEqual(
            [c["workload"] for c in _by_identifier(entries, "carts-tbl")["consumers"]],
            ["carts-sa"])

    def test_a_jsonencode_trust_policy_still_grants_every_subject(self):
        """The heredoc trust policy has no live braces, so its subjects have
        no enclosing groups. A `jsonencode` one does — but both subjects sit
        in the SAME groups, which is what distinguishes one shared role from a
        per-app wrapper. Testing "has groups" instead would void both."""
        entries = _harvest({"main.tf": self.GRANTS + '''
resource "aws_iam_role" "shared" {
  assume_role_policy = jsonencode({
    Statement = [
      {
        Condition = {
          StringLike = {
            "oidc:sub" = ["system:serviceaccount:a:a-sa", "system:serviceaccount:b:b-sa"]
          }
        }
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "shared" {
  role       = aws_iam_role.shared.name
  policy_arn = aws_iam_policy.carts.arn
}
'''})
        self.assertEqual(
            sorted(c["workload"]
                   for c in _by_identifier(entries, "carts-tbl")["consumers"]),
            ["a-sa", "b-sa"])

    def test_a_per_app_boundary_does_not_halt_the_outward_walk(self):
        """A permissions boundary IS an `aws_iam_policy`, so a grant test that
        does not subtract it stops the walk on a group that grants nothing,
        and the real grant further out is never read."""
        entries = _harvest({"main.tf": '''
resource "aws_dynamodb_table" "carts" {
  name = "carts-tbl"
}

resource "aws_iam_policy" "boundary" {
  policy = <<EOF
{"Resource": "*"}
EOF
}

module "irsa" {
  source              = "terraform-aws-modules/iam/aws//modules/iam-role-for-service-accounts-eks"
  dynamodb_table_arns = [aws_dynamodb_table.carts.arn]
  apps = {
    carts = {
      role_permissions_boundary_arn = aws_iam_policy.boundary.arn
      namespace_service_accounts    = ["carts:carts-sa"]
    }
  }
}
'''})
        self.assertEqual(
            [c["workload"] for c in _by_identifier(entries, "carts-tbl")["consumers"]],
            ["carts-sa"])

    def test_a_boundary_alone_in_the_subjects_group_does_not_stop_the_walk(self):
        """The per-app walk stops at the first enclosing group that carries a
        grant, and a permissions boundary IS an `aws_iam_policy`. Where the
        subjects sit one level below their app's grants — the shape
        `iam-role-for-service-accounts-eks` produces — counting the boundary
        stops the walk on the group that holds nothing else, and the app's own
        table one level out is never read.

        The single-app case above never reaches this code at all: with one
        subject and no grant-bearing siblings the block is not per-app, so the
        whole body applies and the grant test is never called."""
        entries = _harvest({"main.tf": '''
resource "aws_dynamodb_table" "carts" {
  name = "carts-tbl"
}

resource "aws_dynamodb_table" "orders" {
  name = "orders-tbl"
}

resource "aws_iam_policy" "boundary" {
  policy = <<EOF
{"Resource": "*"}
EOF
}

module "irsa" {
  source = "terraform-aws-modules/iam/aws//modules/iam-role-for-service-accounts-eks"
  apps = {
    carts = {
      dynamodb_table_arns = [aws_dynamodb_table.carts.arn]
      oidc = {
        role_permissions_boundary_arn = aws_iam_policy.boundary.arn
        namespace_service_accounts    = ["carts:carts-sa"]
      }
    }
    orders = {
      dynamodb_table_arns = [aws_dynamodb_table.orders.arn]
      oidc = {
        namespace_service_accounts = ["orders:orders-sa"]
      }
    }
  }
}
'''})
        self.assertEqual(
            [c["workload"] for c in _by_identifier(entries, "carts-tbl")["consumers"]],
            ["carts-sa"], "the boundary stopped the walk short of the real grant")
        self.assertEqual(
            [c["workload"] for c in _by_identifier(entries, "orders-tbl")["consumers"]],
            ["orders-sa"], "the sibling app lost its own table")

    def test_two_top_level_argument_maps_are_not_a_per_app_wrapper(self):
        """The block body is not a parent for the sibling test. Two maps
        written as two top-level arguments are two kinds of grant handed to
        the same app, not two apps — and a per-app map always keys its apps
        under ONE argument. Counting them made a single-subject module read as
        per-app: the subject's own group carries no grant, the walk finds none
        outward either, and every grant the block makes is dropped."""
        entries = _harvest({"main.tf": '''
resource "aws_dynamodb_table" "carts" {
  name = "carts-tbl"
}

resource "aws_s3_bucket" "assets" {
  bucket = "acme-assets"
}

resource "aws_iam_policy" "carts" {
  policy = <<EOF
{"Resource": "${aws_dynamodb_table.carts.arn}"}
EOF
}

module "carts_irsa" {
  source           = "terraform-aws-modules/iam/aws//modules/iam-role-for-service-accounts-eks"
  role_policy_arns = { main = aws_iam_policy.carts.arn }
  bucket_arns      = { assets = aws_s3_bucket.assets.arn }
  oidc_providers = {
    main = {
      namespace_service_accounts = ["carts:carts-sa"]
    }
  }
}
'''})
        for identifier in ("carts-tbl", "acme-assets"):
            self.assertEqual(
                [c["workload"]
                 for c in _by_identifier(entries, identifier)["consumers"]],
                ["carts-sa"], f"{identifier} lost its only consumer")

    def test_a_map_and_the_entry_inside_it_are_not_siblings(self):
        """The structural per-app test counts sibling groups — groups under
        ONE parent — that each carry a grant of their own. A map and an entry
        inside it both contain that entry's grant, so pooling every group in
        the block into one family makes a single-app wrapper look per-app. The
        subject is then scoped to its own app group, and the shared grant at
        the block's top level, which that app also has, is dropped."""
        entries = _harvest({"main.tf": '''
resource "aws_dynamodb_table" "shared" {
  name = "shared-tbl"
}

resource "aws_dynamodb_table" "carts" {
  name = "carts-tbl"
}

module "irsa" {
  source              = "terraform-aws-modules/iam/aws//modules/iam-role-for-service-accounts-eks"
  dynamodb_table_arns = [aws_dynamodb_table.shared.arn]
  apps = {
    carts = {
      extra_table_arns           = [aws_dynamodb_table.carts.arn]
      namespace_service_accounts = ["carts:carts-sa"]
    }
  }
}
'''})
        for identifier in ("shared-tbl", "carts-tbl"):
            self.assertEqual(
                [c["workload"]
                 for c in _by_identifier(entries, identifier)["consumers"]],
                ["carts-sa"], f"{identifier} lost its consumer")

    def test_one_role_shared_by_several_service_accounts_grants_all_of_them(self):
        """The whole-body case must survive the scoping. A role really does
        give every service account named in its trust policy everything it is
        granted — there are no enclosing groups in the mask, because the
        subjects sit in a heredoc."""
        entries = _harvest({"main.tf": '''
resource "aws_dynamodb_table" "carts" {
  name = "carts-tbl"
}

resource "aws_iam_policy" "shared" {
  policy = <<EOF
{"Resource": "${aws_dynamodb_table.carts.arn}"}
EOF
}

resource "aws_iam_role" "shared" {
  assume_role_policy = <<POL
{"Condition":{"StringLike":{"oidc:sub":["system:serviceaccount:a:a-sa","system:serviceaccount:b:b-sa"]}}}
POL
}

resource "aws_iam_role_policy_attachment" "shared" {
  role       = aws_iam_role.shared.name
  policy_arn = aws_iam_policy.shared.arn
}
'''})
        self.assertEqual(
            sorted(c["workload"]
                   for c in _by_identifier(entries, "carts-tbl")["consumers"]),
            ["a-sa", "b-sa"])

    def test_a_heredoc_description_does_not_mint_a_subject(self):
        """Meta-argument blanking follows a delimiter only when the value
        opens one, and `<<` is not one — so a heredoc `description` survived
        into `_subject_body`, which restores heredoc bodies whole. The result
        was a decommissioned service account in another namespace recorded as
        the consumer, and the real one never recorded at all."""
        entries = _harvest({"main.tf": '''
resource "aws_dynamodb_table" "carts" {
  name = "carts-table"
}

resource "aws_iam_policy" "carts" {
  policy = <<EOF
{"Resource": "${aws_dynamodb_table.carts.arn}"}
EOF
}

resource "aws_iam_role" "carts" {
  description = <<DESC
Formerly assumed by system:serviceaccount:legacy:old-sa before the migration.
DESC
  assume_role_policy = <<POL
{"Condition":{"StringLike":{"oidc:sub":"system:serviceaccount:carts:carts-sa"}}}
POL
}

resource "aws_iam_role_policy_attachment" "carts" {
  role       = aws_iam_role.carts.name
  policy_arn = aws_iam_policy.carts.arn
}
'''})
        self.assertEqual(
            [c["workload"] for c in _by_identifier(entries, "carts-table")["consumers"]],
            ["carts-sa"])

    def test_provider_arn_in_the_subject_group_does_not_kill_the_fallback(self):
        """The group-scoping fallback is keyed on the group carrying a GRANT,
        not on it referencing anything. `provider_arn = module.eks.
        oidc_provider_arn` grants nothing but is a reference, so an
        emptiness test stops firing the moment it appears — and it appears in
        the canonical spelling of this module."""
        entries = _harvest({"main.tf": '''
resource "aws_s3_bucket" "assets" {
  bucket = "acme-assets"
}

module "s3_csi_irsa" {
  source                        = "terraform-aws-modules/iam/aws//modules/iam-role-for-service-accounts-eks"
  mountpoint_s3_csi_bucket_arns = [aws_s3_bucket.assets.arn]
  oidc_providers = {
    main = {
      provider_arn               = module.eks.oidc_provider_arn
      namespace_service_accounts = ["kube-system:s3-csi-driver-sa"]
    }
  }
}
'''})
        self.assertEqual(
            [c["workload"] for c in _by_identifier(entries, "acme-assets")["consumers"]],
            ["s3-csi-driver-sa"])

    def test_provider_arn_does_not_stop_the_walk_in_a_per_app_block(self):
        """The per-app counterpart of the test above, and the one that reaches
        the grant test at all: with a single subject the block is not per-app,
        the whole body applies and `names_a_grant` is never called. Two apps
        make it per-app, and then the innermost subject group carries only
        `provider_arn = module.eks.oidc_provider_arn` — a reference that grants
        nothing. A rule keyed on "references anything" stops there, and both
        apps lose the table one level out."""
        entries = _harvest({"main.tf": '''
resource "aws_dynamodb_table" "carts" {
  name = "carts-tbl"
}

resource "aws_dynamodb_table" "orders" {
  name = "orders-tbl"
}

module "irsa" {
  source = "terraform-aws-modules/iam/aws//modules/iam-role-for-service-accounts-eks"
  apps = {
    carts = {
      dynamodb_table_arns = [aws_dynamodb_table.carts.arn]
      oidc = {
        provider_arn               = module.eks.oidc_provider_arn
        namespace_service_accounts = ["carts:carts-sa"]
      }
    }
    orders = {
      dynamodb_table_arns = [aws_dynamodb_table.orders.arn]
      oidc = {
        provider_arn               = module.eks.oidc_provider_arn
        namespace_service_accounts = ["orders:orders-sa"]
      }
    }
  }
}
'''})
        self.assertEqual(
            [c["workload"] for c in _by_identifier(entries, "carts-tbl")["consumers"]],
            ["carts-sa"])
        self.assertEqual(
            [c["workload"] for c in _by_identifier(entries, "orders-tbl")["consumers"]],
            ["orders-sa"])

    def test_a_subject_in_a_grant_free_group_still_sees_the_whole_body(self):
        """The scoping must not be unconditional. `iam-role-for-service-
        accounts-eks` nests its subjects under `oidc_providers` while the ARNs
        sit at the block's top level — scoping there finds nothing and loses
        the link. Falling back when the group carries no references keeps both
        this and the per-app case above. Caught by the sample repository, not
        by a unit test, the first time."""
        entries = _harvest({"main.tf": '''
resource "aws_s3_bucket" "mountpoint_s3" {
  bucket = "acme-mountpoint"
}

module "csi_irsa" {
  source                        = "terraform-aws-modules/iam/aws//modules/iam-role-for-service-accounts-eks"
  mountpoint_s3_csi_bucket_arns = [aws_s3_bucket.mountpoint_s3.arn]
  oidc_providers = {
    main = {
      namespace_service_accounts = ["kube-system:s3-csi-driver-sa"]
    }
  }
}
'''})
        self.assertEqual(
            [c["workload"] for c in _by_identifier(entries, "acme-mountpoint")["consumers"]],
            ["s3-csi-driver-sa"])

    def test_widening_to_direct_references_did_not_reopen_the_boundary_hole(self):
        """The direct-reference widening must not undo the narrowing that
        stopped a shared boundary attributing the estate: the boundary is
        referenced directly from the role body, so it is the exact shape the
        widening could have let back in."""
        entries = _harvest({"iam.tf": '''
resource "aws_dynamodb_table" "carts" {
  name = "acme-carts"
}

resource "aws_iam_policy" "org_boundary" {
  policy = <<EOF
{"Resource": ["${aws_dynamodb_table.carts.arn}"]}
EOF
}

module "orders_role" {
  source                        = "terraform-aws-modules/iam/aws//modules/iam-assumable-role-with-oidc"
  role_permissions_boundary_arn = aws_iam_policy.org_boundary.arn
  oidc_fully_qualified_subjects = ["system:serviceaccount:orders:orders"]
}
'''})
        self.assertEqual(_by_identifier(entries, "acme-carts")["consumers"], [])

    def test_a_role_with_no_subject_attributes_nothing(self):
        """Without a service account the role says a policy exists, not who
        uses it. Recording it would name a workload the files never name."""
        files = {"iam.tf": self.IRSA["iam.tf"].replace(
            'oidc_fully_qualified_subjects = ["system:serviceaccount:carts:carts-sa"]',
            'oidc_fully_qualified_subjects = []')}
        entry = _by_identifier(_harvest(files), "acme-carts")
        self.assertEqual(entry["consumers"], [])


class SecondHopTest(unittest.TestCase):
    """The IRSA second hop expands a policy the role points at. It must not
    expand everything else a role body happens to name."""

    def test_a_permissions_boundary_does_not_attribute_the_whole_estate(self):
        """A boundary is a ceiling, not a grant, and a shared org-wide one
        names most of the estate. It is also an aws_iam_policy, so filtering
        by block type cannot catch it — the argument it arrives on has to."""
        entries = _harvest({"iam.tf": '''
resource "aws_dynamodb_table" "carts" {
  name = "acme-carts"
}

resource "aws_iam_policy" "org_boundary" {
  policy = <<EOF
{"Resource": ["${aws_dynamodb_table.carts.arn}"]}
EOF
}

resource "aws_iam_role" "orders" {
  permissions_boundary = aws_iam_policy.org_boundary.arn
  assume_role_policy   = <<EOF
{"Condition":{"StringLike":{"oidc:sub":"system:serviceaccount:orders:orders"}}}
EOF
}
'''})
        entry = _by_identifier(entries, "acme-carts")
        self.assertEqual(
            entry["consumers"], [],
            "the carts table was attributed to orders through a boundary")

    def test_a_non_policy_block_the_role_names_is_not_expanded(self):
        """A role body also names its OIDC provider module, its cluster and
        whatever produced its URL. Expanding those attributes every datastore
        they mention to this one service account."""
        entries = _harvest({"iam.tf": '''
resource "aws_dynamodb_table" "carts" {
  name = "acme-carts"
}

module "eks" {
  source     = "terraform-aws-modules/eks/aws"
  extra_data = aws_dynamodb_table.carts.arn
}

resource "aws_iam_role" "orders" {
  provider_url       = module.eks.oidc_provider_url
  assume_role_policy = <<EOF
{"Condition":{"StringLike":{"oidc:sub":"system:serviceaccount:orders:orders"}}}
EOF
}
'''})
        entry = _by_identifier(entries, "acme-carts")
        self.assertEqual(
            entry["consumers"], [],
            "a non-policy block the role references was expanded as a grant")

    def test_the_module_spelling_of_a_permissions_boundary_is_excluded_too(self):
        """The resource argument is `permissions_boundary`; the IRSA modules
        call it `role_permissions_boundary_arn`, and the module form is the one
        real estates use. An exact-name list fixed the rarer spelling and left
        the common one attributing the whole estate."""
        entries = _harvest({"iam.tf": '''
resource "aws_dynamodb_table" "carts" {
  name = "acme-carts"
}

resource "aws_iam_policy" "org_boundary" {
  policy = <<EOF
{"Resource": ["${aws_dynamodb_table.carts.arn}"]}
EOF
}

module "orders_role" {
  source                        = "terraform-aws-modules/iam/aws//modules/iam-assumable-role-with-oidc"
  role_permissions_boundary_arn = aws_iam_policy.org_boundary.arn
  oidc_fully_qualified_subjects = ["system:serviceaccount:orders:orders"]
}
'''})
        self.assertEqual(
            _by_identifier(entries, "acme-carts")["consumers"], [],
            "the org boundary attributed the carts table to orders")

    def test_a_boundary_wrapped_across_lines_is_excluded(self):
        """Declared arguments are read one line at a time, so a wrapped value
        would escape subtraction and take the estate with it."""
        entries = _harvest({"iam.tf": '''
resource "aws_dynamodb_table" "carts" {
  name = "acme-carts"
}

resource "aws_iam_policy" "org_boundary" {
  policy = <<EOF
{"Resource": ["${aws_dynamodb_table.carts.arn}"]}
EOF
}

resource "aws_iam_role" "orders" {
  permissions_boundary = (
    aws_iam_policy.org_boundary.arn
  )
  assume_role_policy = <<EOF
{"Condition":{"StringLike":{"oidc:sub":"system:serviceaccount:orders:orders"}}}
EOF
}
'''})
        self.assertEqual(_by_identifier(entries, "acme-carts")["consumers"], [])

    def test_a_boundary_naming_a_data_service_module_is_still_not_a_grant(self):
        """Two separate subtractions keep a boundary out, and this is the one
        on the DIRECT references. Widening those — so a role that names a
        table in its own arguments attributes it — is safe only because a
        reference has to resolve to a recorded datastore before anything is
        recorded, and a boundary is an `aws_iam_policy`, which is not one.

        Not always: a local module that provisions the database and the
        guard-rail policy fencing it publishes both, so the boundary argument
        names a block that IS a recorded data service. Without the boundary
        subtraction on this path too, the widening hands the service account
        that database."""
        entries = _harvest({"main.tf": '''
module "carts_rds" {
  source     = "./modules/rds"
  identifier = "acme-carts"
}

resource "aws_iam_role" "carts" {
  permissions_boundary = module.carts_rds.boundary_policy_arn
  assume_role_policy   = <<POL
{"Condition":{"StringLike":{"oidc:sub":"system:serviceaccount:carts:carts-sa"}}}
POL
}
'''})
        self.assertEqual(
            _by_identifier(entries, "acme-carts")["consumers"], [],
            "a reference inside the boundary expression was read as a grant")

    def test_an_inline_role_policy_naming_its_role_is_followed(self):
        """The other hand-written wiring: the policy names the role, and the
        role names nothing. Symmetric with the attachment case."""
        entries = _harvest({"iam.tf": '''
resource "aws_dynamodb_table" "carts" {
  name = "acme-carts"
}

resource "aws_iam_role" "carts" {
  assume_role_policy = <<EOF
{"Condition":{"StringLike":{"oidc:sub":"system:serviceaccount:carts:carts-sa"}}}
EOF
}

resource "aws_iam_role_policy" "carts" {
  role   = aws_iam_role.carts.id
  policy = <<EOF
{"Resource": "${aws_dynamodb_table.carts.arn}"}
EOF
}
'''})
        entry = _by_identifier(entries, "acme-carts")
        self.assertEqual([(c["workload"], c["namespace"])
                          for c in entry["consumers"]], [("carts-sa", "carts")])

    def test_an_attachment_for_a_different_principal_is_not_matched(self):
        """`module.carts` and `aws_iam_role.carts` are two different
        principals that share a label, and `this` makes the collision routine.
        Matching on the bare label hands one the other's grants."""
        entries = _harvest({"iam.tf": '''
resource "aws_dynamodb_table" "nodes" {
  name = "acme-node-data"
}

module "carts" {
  source                        = "terraform-aws-modules/iam/aws//modules/iam-assumable-role-with-oidc"
  oidc_fully_qualified_subjects = ["system:serviceaccount:carts:carts"]
}

resource "aws_iam_role" "carts" {
  name = "carts-nodegroup"
}

resource "aws_iam_policy" "node_data" {
  policy = <<EOF
{"Resource": "${aws_dynamodb_table.nodes.arn}"}
EOF
}

resource "aws_iam_role_policy_attachment" "carts" {
  role       = aws_iam_role.carts.name
  policy_arn = aws_iam_policy.node_data.arn
}
'''})
        self.assertEqual(
            _by_identifier(entries, "acme-node-data")["consumers"], [],
            "the node role's policy was attributed to the carts service account")

    def test_a_boundary_under_an_unrecognised_argument_name_still_attributes(self):
        """Pinning where the heuristic stops working, not a guarantee.

        Subtraction keys on the argument name, so a boundary reaching a role
        any other way — a module input called something else, a boundary set
        by a wrapper — reads as a grant. This is the canary for that edge: if
        the name-based approach is ever replaced with something structural,
        this test should start failing, and that is the point of it."""
        entries = _harvest({"iam.tf": '''
resource "aws_dynamodb_table" "carts" {
  name = "acme-carts"
}

resource "aws_iam_policy" "org_ceiling" {
  policy = <<EOF
{"Resource": ["${aws_dynamodb_table.carts.arn}"]}
EOF
}

module "orders_role" {
  source                        = "acme/iam-wrapper/aws"
  ceiling_policy_arn            = aws_iam_policy.org_ceiling.arn
  oidc_fully_qualified_subjects = ["system:serviceaccount:orders:orders"]
}
'''})
        self.assertEqual(
            [c["workload"] for c in _by_identifier(entries, "acme-carts")["consumers"]],
            ["orders"])

    def test_a_non_policy_resource_the_role_names_is_not_expanded(self):
        """The module case is covered above; this is the resource case. Only
        an IAM policy grants access, so only an IAM policy is expanded — every
        other block a role happens to name is never treated as a source of
        grants.

        A trust policy naming another role is the ordinary way a role comes to
        reference a non-policy block, and the reference has to be in a LIVE
        argument for the filter to be exercised at all: written in `tags`, the
        meta-argument blanking removes it before the filter is ever reached and
        the test passes for the wrong reason."""
        entries = _harvest({"iam.tf": '''
resource "aws_dynamodb_table" "carts" {
  name = "acme-carts"
}

resource "aws_iam_role" "batch" {
  name = "batch"
  inline_policy {
    policy = jsonencode({ Resource = aws_dynamodb_table.carts.arn })
  }
}

resource "aws_iam_role" "orders" {
  assume_role_policy = <<EOF
{"Statement":[{"Principal":{"AWS":"${aws_iam_role.batch.arn}"},
"Condition":{"StringLike":{"oidc:sub":"system:serviceaccount:orders:orders"}}}]}
EOF
}
'''})
        entry = _by_identifier(entries, "acme-carts")
        self.assertEqual(
            entry["consumers"], [],
            "a non-policy resource the role references was expanded as a grant")

    def test_a_policy_bound_by_a_separate_attachment_resource_is_followed(self):
        """The textbook hand-written IRSA wiring: the role never names the
        policy and the policy never names a subject — the edge lives on a
        third resource. Following only the role's own references finds
        nothing."""
        entries = _harvest({"iam.tf": '''
resource "aws_dynamodb_table" "carts" {
  name = "acme-carts"
}

resource "aws_iam_role" "carts" {
  assume_role_policy = <<EOF
{"Condition":{"StringLike":{"oidc:sub":"system:serviceaccount:carts:carts-sa"}}}
EOF
}

resource "aws_iam_policy" "carts" {
  policy = <<EOF
{"Resource": "${aws_dynamodb_table.carts.arn}"}
EOF
}

resource "aws_iam_role_policy_attachment" "carts" {
  role       = aws_iam_role.carts.name
  policy_arn = aws_iam_policy.carts.arn
}
'''})
        entry = _by_identifier(entries, "acme-carts")
        self.assertEqual([(c["workload"], c["namespace"])
                          for c in entry["consumers"]], [("carts-sa", "carts")])


class HeredocTest(unittest.TestCase):

    def test_an_interpolation_on_a_commented_yaml_line_still_attributes(self):
        """Pinning the trade-off, not a guarantee the code does not give.

        A `#` inside a heredoc is a comment to whatever reads the rendered
        text — YAML here — but not to Terraform, which interpolates `${...}`
        wherever it appears in the body and records a real dependency edge for
        it. So the reference is followed, and the cache is attributed to a
        release whose chart ignores the line.

        Recognising it would mean parsing the embedded language, and a heredoc
        can hold YAML, JSON, shell or anything else. Recorded as a known
        over-attribution in DESIGN.md issue 27 rather than guessed at. Change
        this test when that is addressed.
        """
        entries = _harvest({"main.tf": '''
module "orders_rds" {
  source     = "terraform-aws-modules/rds/aws"
  identifier = "acme-orders"
}

module "carts_redis" {
  source     = "terraform-aws-modules/elasticache/aws"
  cluster_id = "acme-carts-cache"
}

resource "helm_release" "orders" {
  name   = "orders"
  values = [<<EOT
database:
  host: ${module.orders_rds.endpoint}
# disabled during the migration:
#   cacheHost: ${module.carts_redis.endpoint}
EOT
  ]
}
'''})
        orders = _by_identifier(entries, "acme-orders")
        self.assertEqual([c["workload"] for c in orders["consumers"]], ["orders"])
        cache = _by_identifier(entries, "acme-carts-cache")
        self.assertEqual([c["workload"] for c in cache["consumers"]], ["orders"])

    def test_a_terraform_comment_outside_a_heredoc_still_does_not_attribute(self):
        """The distinction that does hold: a `#` in Terraform source is a
        Terraform comment, and the lexer has already blanked it."""
        entries = _harvest({"main.tf": '''
module "carts_redis" {
  source     = "terraform-aws-modules/elasticache/aws"
  cluster_id = "acme-carts-cache"
}

resource "helm_release" "orders" {
  name = "orders"
  # cache_host = module.carts_redis.endpoint
}
'''})
        self.assertEqual(
            _by_identifier(entries, "acme-carts-cache")["consumers"], [])

    def test_a_deny_statement_still_reads_as_a_grant(self):
        """Pinning a known over-attribution, not a guarantee. The join follows
        references; it does not read IAM policy semantics, so a resource named
        only to deny access to it is recorded as a consumer. DESIGN.md issue 27
        carries it. Change this test when that is addressed."""
        entries = _harvest({"iam.tf": '''
resource "aws_dynamodb_table" "carts" {
  name = "acme-carts"
}

resource "aws_iam_policy" "deny_carts" {
  policy = <<EOF
{"Statement":[{"Effect":"Deny","Resource":"${aws_dynamodb_table.carts.arn}"}]}
EOF
}

resource "aws_iam_role" "orders" {
  assume_role_policy = <<EOF
{"Condition":{"StringLike":{"oidc:sub":"system:serviceaccount:orders:orders"}}}
EOF
}

resource "aws_iam_role_policy_attachment" "orders" {
  role       = aws_iam_role.orders.name
  policy_arn = aws_iam_policy.deny_carts.arn
}
'''})
        self.assertEqual(
            [c["workload"] for c in _by_identifier(entries, "acme-carts")["consumers"]],
            ["orders"])

    def test_a_reference_inside_a_template_directive_is_found(self):
        """A reference living INSIDE the `%{ ... }` rather than beside it. A
        `${ ... }` alongside would be found by the other branch regardless, so
        this is the only shape that actually exercises `%{` narrowing."""
        entries = _harvest({"main.tf": '''
module "orders_rds" {
  source     = "terraform-aws-modules/rds/aws"
  identifier = "acme-orders"
}

resource "helm_release" "orders" {
  name   = "orders"
  values = [<<EOT
%{ for host in module.orders_rds.endpoints }
  - host
%{ endfor }
EOT
  ]
}
'''})
        self.assertEqual(
            [c["workload"] for c in _by_identifier(entries, "acme-orders")["consumers"]],
            ["orders"])

    def test_a_heredoc_opened_on_the_last_line_records_no_stale_span(self):
        """Two reviewers independently read `scan_source` as leaving a stale
        `heredoc_start` when a heredoc opens on the final line and the
        `stop == n` branch breaks before the assignment — which would restore
        the whole file, turning every commented-out reference into a live one.

        It cannot happen: `_HEREDOC_RE` ends in a `(?=\\r?\\n)` lookahead, so
        with no trailing newline the opener never matches at all, and with one
        `line_end` finds it and the break is not taken. Pinned here with a
        prior closed heredoc, which is what would make a stale value visible.
        """
        commented = ('module "carts_redis" {\n'
                     '  source     = "terraform-aws-modules/elasticache/aws"\n'
                     '  cluster_id = "acme-cache"\n}\n'
                     'resource "helm_release" "orders" {\n'
                     '  name = "orders"\n'
                     '  # host = "${module.carts_redis.endpoint}"\n}\n')
        for label, tail in (("no trailing newline", "\n  policy = <<EOF"),
                            ("trailing newline", "\n  policy = <<EOF\n"),
                            ("trailing spaces", "\n  policy = <<EOF   ")):
            with self.subTest(tail=label):
                content = 'x = <<A\nbody\nA\n' + commented + tail
                _text, _mask, _notes, heredocs = datastores.scan_source(content)
                for start, stop in heredocs:
                    self.assertNotIn(
                        "carts_redis", content[start:stop],
                        "a heredoc span reached back over a Terraform comment")
                entries = _harvest({"m.tf": content})
                self.assertEqual(
                    _by_identifier(entries, "acme-cache")["consumers"], [])

    def test_an_escaped_interpolation_is_a_literal_not_a_reference(self):
        """`$${` and `%%{` are HCL's escapes for a literal `${`/`%{`, and a
        values heredoc that ships a template to the workload — a shell script,
        a Helm value read later — writes them exactly so Terraform does NOT
        interpolate. Reading one as live makes the release a consumer of a
        database it only mentions the name of."""
        entries = _harvest({"main.tf": '''
module "orders_rds" {
  source     = "terraform-aws-modules/rds/aws"
  identifier = "acme-orders"
}

resource "helm_release" "unrelated" {
  name   = "unrelated"
  values = [<<EOT
script: |
  echo "resolve $${module.orders_rds.endpoint} at run time"
  %%{ if false }never%%{ endif }
EOT
  ]
}
'''})
        self.assertEqual(
            _by_identifier(entries, "acme-orders")["consumers"], [],
            "an escaped interpolation was read as a live reference")

    def test_a_live_interpolation_beside_an_escaped_one_still_attributes(self):
        """Guards the test above from passing by skipping too much."""
        entries = _harvest({"main.tf": '''
module "orders_rds" {
  source     = "terraform-aws-modules/rds/aws"
  identifier = "acme-orders"
}

resource "helm_release" "orders" {
  name   = "orders"
  values = [<<EOT
literal: $${NOT_TERRAFORM}
host: ${module.orders_rds.endpoint}
EOT
  ]
}
'''})
        self.assertEqual(
            [c["workload"]
             for c in _by_identifier(entries, "acme-orders")["consumers"]],
            ["orders"])

    def test_an_unterminated_heredoc_leaves_the_entry_unknown_not_answered(self):
        """The reference below an unterminated heredoc is never read: the
        block holding it never closes either, so `iter_terraform_blocks`
        reports truncation and drops it. What must not happen is the entry
        going on to say nothing references it — the file is recorded as unread
        and the entry hedges instead."""
        inventory = {"data_dependencies": []}
        with tempfile.TemporaryDirectory() as root:
            with open(os.path.join(root, "main.tf"), "w") as f:
                f.write('''
module "orders_rds" {
  source     = "terraform-aws-modules/rds/aws"
  identifier = "acme-orders"
}

resource "helm_release" "orders" {
  name   = "orders"
  values = [<<EOT
host: ${module.orders_rds.endpoint}
''')
            datastores.harvest_datastores(inventory, root)
        entry = _by_identifier(inventory["data_dependencies"], "acme-orders")
        self.assertEqual(entry["consumers"], [])
        self.assertIn(consumers.TRUNCATED_NOTE, entry["notes"])
        self.assertNotIn(consumers.UNATTRIBUTED_NOTE, entry["notes"])

    def test_plain_prose_in_a_heredoc_is_not_a_reference(self):
        entries = _harvest({"main.tf": '''
resource "aws_db_instance" "d" {
  identifier = "acme-orders"
}

resource "helm_release" "unrelated" {
  name   = "unrelated"
  values = [<<EOT
notes: see aws_db_instance.d for the connection details
EOT
  ]
}
'''})
        self.assertEqual(_by_identifier(entries, "acme-orders")["consumers"], [])


class KubernetesResourceTest(unittest.TestCase):
    """Every kubernetes_* resource states its identity inside `metadata`,
    which the top-level argument reader does not descend into."""

    SECRET = '''
module "orders_rds" {
  source     = "terraform-aws-modules/rds/aws"
  identifier = "acme-orders"
}

resource "kubernetes_secret" "db" {
  metadata {
    name      = "orders-db-creds"
    namespace = "orders"
  }
  data = {
    host = module.orders_rds.endpoint
  }
}
'''

    def test_the_name_and_namespace_come_from_the_metadata_block(self):
        entry = _by_identifier(_harvest({"main.tf": self.SECRET}), "acme-orders")
        self.assertEqual(len(entry["consumers"]), 1)
        consumer = entry["consumers"][0]
        self.assertEqual(consumer["workload"], "orders-db-creds",
                         "fell back to the Terraform block label")
        self.assertEqual(consumer["namespace"], "orders")
        self.assertEqual(consumer["kind"], "kubernetes_secret")

    def _metadata_case(self, metadata_body: str) -> dict:
        return _by_identifier(_harvest({"main.tf": f'''
module "catalog_rds" {{
  source     = "terraform-aws-modules/rds/aws"
  identifier = "acme-catalog"
}}

resource "kubernetes_deployment" "catalog" {{
  metadata {{
{metadata_body}
  }}
  spec {{ template {{ spec {{ container {{ env {{ value = module.catalog_rds.endpoint }} }} }} }} }}
}}
'''}), "acme-catalog")

    def test_a_later_labels_map_does_not_overwrite_the_workload_name(self):
        """Nested maps AFTER the real name.

        Both orderings are kept because the depth counter is updated after the
        line is read: the line that OPENS a map is seen at the block's own
        level, and the line that closes one has to put the next line back
        there. Neither ordering exercises first-wins — the depth guard alone
        decides both, and the repeated-argument test below is what covers
        `setdefault`.

        The maps must span lines here too. An inline `labels = { name = "x" }`
        never presents `name` at the start of a line, so the argument regex
        never matches it at any depth and the depth guard does no work — an
        earlier version of this test used that form and claimed coverage it
        did not have.
        """
        entry = self._metadata_case('''    name      = "catalog"
    namespace = "catalog"
    labels = {
      name = "catalog-deployment"
    }
    annotations = {
      namespace = "kube-system"
    }''')
        self.assertEqual(len(entry["consumers"]), 1)
        self.assertEqual(entry["consumers"][0]["workload"], "catalog")
        self.assertEqual(entry["consumers"][0]["namespace"], "catalog")

    def test_the_pod_templates_metadata_is_not_read_as_the_workloads(self):
        """`_METADATA_RE` finds the first match in the body, but a Deployment
        nests a second metadata block inside `spec { template { ... } }` — and
        HCL does not order blocks, so `spec` can come first. Anchoring on the
        first DEPTH-0 block is what makes the right one win."""
        entry = _by_identifier(_harvest({"main.tf": '''
module "catalog_rds" {
  source     = "terraform-aws-modules/rds/aws"
  identifier = "acme-catalog"
}

resource "kubernetes_deployment" "catalog" {
  spec {
    template {
      metadata {
        name      = "catalog-pod"
        namespace = "wrong"
      }
      spec { container { env { value = module.catalog_rds.endpoint } } }
    }
  }
  metadata {
    name      = "catalog"
    namespace = "catalog"
  }
}
'''}), "acme-catalog")
        self.assertEqual(len(entry["consumers"]), 1)
        self.assertEqual(entry["consumers"][0]["workload"], "catalog",
                         "read the pod template's metadata, not the object's")
        self.assertEqual(entry["consumers"][0]["namespace"], "catalog")

    def test_an_earlier_labels_map_does_not_supply_the_workload_name(self):
        """Nested maps BEFORE the real name: the ordering where a stale value
        would be taken first and no later match could displace it.

        The maps must span lines: arguments are read a line at a time, so an
        inline `labels = { name = "x" }` never presents `name` at the start of
        a line and would leave the depth guard unexercised too."""
        entry = self._metadata_case('''    labels = {
      name = "catalog-deployment"
    }
    annotations = {
      namespace = "kube-system"
    }
    name      = "catalog"
    namespace = "catalog"''')
        self.assertEqual(len(entry["consumers"]), 1)
        self.assertEqual(entry["consumers"][0]["workload"], "catalog",
                         "a label one level down was read as the object's name")
        self.assertEqual(entry["consumers"][0]["namespace"], "catalog")

    def test_a_repeated_metadata_argument_resolves_to_the_first(self):
        """What first-wins actually covers, once the depth guard has taken
        the nested maps. HCL rejects a repeated argument, but the scan reads
        whatever is on disk — generated Terraform, a merge resolved by
        concatenation — and the metadata reader has to settle it the same way
        `_top_level_args` settles the block's own arguments, or the two report
        different names for the same block."""
        entry = self._metadata_case('''    name      = "catalog"
    namespace = "catalog"
    name      = "catalog-old"''')
        self.assertEqual(len(entry["consumers"]), 1)
        self.assertEqual(entry["consumers"][0]["workload"], "catalog")

    def test_cluster_machinery_is_not_recorded_as_a_workload(self):
        """A StorageClass matches a kubernetes_ prefix but names nothing a
        gate can hold a release on."""
        entries = _harvest({"main.tf": '''
resource "aws_s3_bucket" "b" {
  bucket = "acme-data"
}

resource "kubernetes_storage_class" "fast" {
  metadata { name = "fast" }
  parameters = { bucket = aws_s3_bucket.b.id }
}
'''})
        self.assertEqual(_by_identifier(entries, "acme-data")["consumers"], [])


class ScopeTest(unittest.TestCase):

    def test_an_excluded_file_contributes_no_consumer(self):
        """The one property here with a confidentiality consequence. A consumer
        built from an excluded file carries that file's path in `evidence` and
        a `source_path` derived from it, and `data_dependencies` crosses into
        the member-readable exports object — so an operator who removed a
        directory from discovery would see it reappear there."""
        # Discovery's algebra is default-IN: a scope subtracts, and an
        # `included` entry would un-exclude rather than narrow.
        scope = {"excluded": ["infra/live/**"]}
        entry = _by_identifier(_harvest(dict(TerraformWiringTest.WIRED), scope),
                               "acme-orders")
        self.assertEqual(entry["consumers"], [])

    def test_an_excluded_file_does_not_license_the_no_workload_claim(self):
        """Every other reason a file goes unread feeds the truncation set; a
        scope exclusion did not, so the entry went on asserting that nothing
        references it while its consumer sat in a directory the scan was told
        to skip. The wiring chain crosses directories, so excluding an
        environment while keeping a shared modules tree in scope is exactly
        the shape that produces it.

        The claim is downgraded, not the exclusion honoured differently: an
        excluded file is a decision, not a failure, and it gets its own note
        rather than being folded in with the unreadable ones.
        """
        files = {
            "modules/rds/main.tf":
                'resource "aws_db_instance" "orders" {\n'
                '  identifier = "acme-orders"\n}\n',
            "modules/rds/outputs.tf":
                'output "endpoint" {\n  value = aws_db_instance.orders.address\n}\n',
            "envs/prod/main.tf":
                'module "db" {\n  source = "../../modules/rds"\n}\n'
                'resource "helm_release" "orders" {\n'
                '  name = "orders"\n  set { value = module.db.endpoint }\n}\n',
        }
        inventory = {"data_dependencies": []}
        with tempfile.TemporaryDirectory() as root:
            for rel, content in files.items():
                full = os.path.join(root, rel)
                os.makedirs(os.path.dirname(full), exist_ok=True)
                with open(full, "w") as f:
                    f.write(content)
            datastores.harvest_datastores(inventory, root,
                                          {"excluded": ["envs/**"]})
        entry = _by_identifier(inventory["data_dependencies"], "acme-orders")
        self.assertEqual(entry["consumers"], [])
        # The per-entry note hedges rather than asserting the link does not
        # exist: it names an excluded directory as one of the places it could
        # be. A separate note category was tried and removed — excluding any
        # single file relabelled every unattributed entry, which took the
        # headline count to zero and drained it of meaning.
        # The whole clause, not the word: "excluded" survives almost any
        # rewording of the sentence around it, so asserting on it alone lets
        # the note stop naming an excluded directory without failing here.
        self.assertIn("in a directory the confirmed scope excluded",
                      consumers.UNATTRIBUTED_NOTE)
        self.assertIn(consumers.UNATTRIBUTED_NOTE, entry["notes"])
        self.assertTrue(
            any("did not read" in note
                for note in inventory["data_dependency_scan_notes"]),
            "the scan did not say the exclusion could have hidden a link")

    def test_a_truncation_elsewhere_does_not_swallow_the_exclusion_caveat(self):
        """The two reasons a consumer can go unseen are independent, and one
        file that ran out mid-read used to silence the other for the whole
        scan: every consumer-less entry is routed to the unreadable list, so
        the unattributed list empties and the exclusion note stops firing.
        The exclusion is the likelier explanation of the two — a place the
        scan was told not to look, rather than one it failed to finish."""
        files = {
            "modules/rds/main.tf":
                'resource "aws_db_instance" "orders" {\n'
                '  identifier = "acme-orders"\n}\n',
            "modules/rds/outputs.tf":
                'output "endpoint" {\n  value = aws_db_instance.orders.address\n}\n',
            "modules/rds/broken.tf":
                'resource "aws_iam_role" "never_closed" {\n  name = "x"\n',
            "envs/prod/main.tf":
                'module "db" {\n  source = "../../modules/rds"\n}\n'
                'resource "helm_release" "orders" {\n'
                '  name = "orders"\n  set { value = module.db.endpoint }\n}\n',
        }
        inventory = {"data_dependencies": []}
        with tempfile.TemporaryDirectory() as root:
            for rel, content in files.items():
                full = os.path.join(root, rel)
                os.makedirs(os.path.dirname(full), exist_ok=True)
                with open(full, "w") as f:
                    f.write(content)
            datastores.harvest_datastores(inventory, root,
                                          {"excluded": ["envs/**"]})
        notes = inventory["data_dependency_scan_notes"]
        self.assertTrue(
            any("did not read" in note for note in notes),
            f"the exclusion caveat was swallowed by the truncation: {notes}")
        # And it says the consequence without restating the count, which the
        # datastore walk already reported. The agent relays every note
        # verbatim, so the same number in two sentences reads as two
        # exclusions.
        self.assertEqual(
            [n for n in notes if re.search(r"\d+ Terraform file\(s\)", n)
             and "exclud" in n],
            ["1 Terraform file(s) were not scanned: the confirmed discovery "
             "scope excludes them"], f"the exclusion count is stated twice: {notes}")

    def test_an_unexcluded_file_still_contributes(self):
        """Guards the test above from passing because the scope happened to
        exclude everything, or because the fixture stopped wiring anything."""
        scope = {"excluded": ["somewhere/else/**"]}
        entry = _by_identifier(_harvest(dict(TerraformWiringTest.WIRED), scope),
                               "acme-orders")
        self.assertEqual([c["workload"] for c in entry["consumers"]], ["orders"])


class UnattributedTest(unittest.TestCase):

    def test_an_unreferenced_datastore_is_kept_and_says_it_is_unattributed(self):
        """Dropping it would hide a real database. Attributing it by name
        similarity would gate the wrong team. So it stays, and says so."""
        entries = _harvest({"main.tf":
                            'resource "aws_db_instance" "lonely" {\n'
                            '  identifier = "acme-lonely"\n}\n'})
        entry = _by_identifier(entries, "acme-lonely")
        self.assertEqual(entry["consumers"], [])
        self.assertTrue(
            consumers.UNATTRIBUTED_NOTE in entry["notes"],
            f"expected an unattributed note, got {entry['notes']}")

    def test_a_truncated_file_suppresses_the_no_workload_claim(self):
        """The scan-level truncation note is not enough on its own. The entry
        itself must stop saying "no workload references this", because a
        workload in the unread part of the same file does reference it — and
        the per-entry note is what a reader acts on."""
        inventory = {"data_dependencies": []}
        with tempfile.TemporaryDirectory() as root:
            with open(os.path.join(root, "main.tf"), "w") as f:
                f.write('resource "aws_db_instance" "d" {\n'
                        '  identifier = "acme-orders"\n}\n'
                        'resource "aws_iam_role" "broken" {\n'
                        '  name = "x"\n'
                        'resource "helm_release" "orders" {\n'
                        '  name = "orders"\n'
                        '  set { value = aws_db_instance.d.endpoint }\n}\n')
            datastores.harvest_datastores(inventory, root)
        entry = inventory["data_dependencies"][0]
        self.assertEqual(entry["identifier"], "acme-orders")
        self.assertFalse(
            consumers.UNATTRIBUTED_NOTE in entry["notes"],
            "claimed nothing references a database referenced in the same file")
        self.assertTrue(
            consumers.TRUNCATED_NOTE in entry["notes"],
            f"no truncation caveat on the entry: {entry['notes']}")

    def test_truncation_suppresses_the_claim_across_directories(self):
        """The suppression cannot be scoped to the entry's own directory. The
        terraform-wiring chain runs from a deploying resource in one directory
        to a datastore in another by construction, so the truncation that hides
        a reference is almost never in the same place as the entry it hides it
        from."""
        inventory = {"data_dependencies": []}
        with tempfile.TemporaryDirectory() as root:
            files = {
                "infra/deps/main.tf":
                    'module "orders_rds" {\n'
                    '  source     = "terraform-aws-modules/rds/aws"\n'
                    '  identifier = "acme-orders"\n}\n',
                "infra/deps/outputs.tf":
                    'output "e" {\n  value = module.orders_rds.endpoint\n}\n',
                "infra/live/main.tf":
                    'module "deps" {\n  source = "../deps"\n}\n'
                    'resource "aws_iam_role" "broken" {\n  name = "x"\n'
                    'resource "helm_release" "orders" {\n'
                    '  name = "orders"\n  set { value = module.deps.e }\n}\n',
            }
            for rel, content in files.items():
                full = os.path.join(root, rel)
                os.makedirs(os.path.dirname(full), exist_ok=True)
                with open(full, "w") as f:
                    f.write(content)
            datastores.harvest_datastores(inventory, root)
        entry = _by_identifier(inventory["data_dependencies"], "acme-orders")
        self.assertFalse(
            consumers.UNATTRIBUTED_NOTE in entry["notes"],
            "claimed nothing references an entry whose consumer was in the "
            "unread part of another directory")

    def _unread_case(self, iam_tf: str, extra: dict = None) -> dict:
        """A datastore in one file, its workload in a file that goes unread."""
        inventory = {"data_dependencies": []}
        files = {
            "infra/db.tf":
                'resource "aws_dynamodb_table" "carts" {\n  name = "acme-carts"\n}\n',
            "infra/iam.tf": iam_tf,
        }
        files.update(extra or {})
        with tempfile.TemporaryDirectory() as root:
            for rel, content in files.items():
                full = os.path.join(root, rel)
                os.makedirs(os.path.dirname(full), exist_ok=True)
                with open(full, "w") as f:
                    f.write(content)
            datastores.harvest_datastores(inventory, root)
        return _by_identifier(inventory["data_dependencies"], "acme-carts")

    def test_every_way_of_not_reading_a_file_suppresses_the_claim(self):
        """`iter_terraform_blocks` reports truncation only for an unclosed
        resource/module/output block. Four other paths stop the walk reading a
        file, and each one used to leave the entry asserting that no workload
        references it while the workload sat in the unread part.

        The heredoc case is the sharp one: broken INSIDE a block the extent is
        never measured and truncation is reported correctly, but broken inside
        a `data`/`locals`/`variable` block the kind filter skips it first, so
        nothing notices.
        """
        workload = ('resource "helm_release" "carts" {\n'
                    '  name = "carts"\n'
                    '  set { value = aws_dynamodb_table.carts.name }\n}\n')
        cases = {
            "heredoc unterminated outside a scanned block":
                'data "aws_iam_policy_document" "p" {\n'
                '  policy = <<EOT\n{"x": 1}\nEOF\n}\n' + workload,
            "quoted string unterminated":
                'locals {\n  broken = "oops\n}\n' + workload,
        }
        for label, iam_tf in cases.items():
            with self.subTest(case=label):
                entry = self._unread_case(iam_tf)
                self.assertFalse(
                    consumers.UNATTRIBUTED_NOTE in entry["notes"],
                    f"[{label}] claimed nothing references a database whose "
                    "workload is in the unread part of the repository")
                self.assertTrue(
                    consumers.TRUNCATED_NOTE in entry["notes"],
                    f"[{label}] no unknown caveat: {entry['notes']}")

    def test_an_oversized_file_suppresses_the_claim_too(self):
        big = ('resource "helm_release" "carts" {\n'
               '  name = "carts"\n'
               '  set { value = aws_dynamodb_table.carts.name }\n}\n'
               + "# padding\n" * 300000)
        entry = self._unread_case('# nothing here\n', {"infra/helm.tf": big})
        self.assertFalse(
            consumers.UNATTRIBUTED_NOTE in entry["notes"],
            "claimed nothing references a database whose workload is in a "
            "file too large to scan")

    def test_an_unclosed_block_comment_does_not_suppress_the_claim(self):
        """The one lexer bail-out that keeps reading. It over-reads the tail
        rather than skipping it, so absence there is still meaningful and the
        affirmative claim is still allowed."""
        entry = self._unread_case('/* never closed\n')
        self.assertTrue(
            consumers.UNATTRIBUTED_NOTE in entry["notes"],
            "an over-reading comment was treated as unread content")

    def _scan(self, body: str) -> dict:
        inventory = {"data_dependencies": []}
        with tempfile.TemporaryDirectory() as root:
            with open(os.path.join(root, "main.tf"), "w") as f:
                f.write(body)
            datastores.harvest_datastores(inventory, root)
        return inventory

    def _notes_for(self, body: str) -> list:
        return self._scan(body)["data_dependency_scan_notes"]

    def test_an_unclosed_output_is_reported_by_the_consumer_scan(self):
        """The break the datastore walk cannot see. Its kind filter skips an
        `output` before the extent is measured, so it reads straight past the
        missing brace and reports nothing — while this walk, which does read
        outputs, loses every reference from there on. Without the note the
        entries before it fall silent for a reason nothing in the ledger
        explains."""
        notes = self._notes_for(
            'resource "aws_db_instance" "d" {\n  identifier = "acme-orders"\n}\n'
            'output "never_closed" {\n  value = 1\n')
        self.assertTrue(
            any("an output block is never closed" in n for n in notes),
            f"no consumer-side truncation note in {notes}")

    def test_an_unclosed_resource_is_reported_once_not_twice(self):
        """The datastore walk reads the same file for the same kinds, so it
        has already said this. The agent relays every note verbatim, so a
        second wording of one fact reads as two malformed files."""
        inventory = self._scan(
            'resource "aws_db_instance" "d" {\n  identifier = "acme-orders"\n}\n'
            'resource "helm_release" "never_closed" {\n  name = "orders"\n')
        notes = inventory["data_dependency_scan_notes"]
        self.assertEqual(
            [n for n in notes if "never closed" in n],
            ["main.tf: a block is never closed, so that block and everything "
             "after it were not read"], f"in {notes}")
        # Dropping the note must not drop what it explains: it is the
        # truncated set, not the note, that stops the entry claiming nothing
        # references it.
        entry = _by_identifier(inventory["data_dependencies"], "acme-orders")
        self.assertIn(consumers.TRUNCATED_NOTE, entry["notes"])

    def test_the_unattributed_list_is_capped_rather_than_dumped(self):
        """The note goes into the ledger and the agent relays it verbatim, so
        an estate with fifty unattributed databases must not put fifty names
        in one sentence. The count stays exact; the list is what is cut."""
        body = "".join(
            f'resource "aws_db_instance" "d{i}" {{\n'
            f'  identifier = "acme-{i:02d}"\n}}\n' for i in range(8))
        inventory = {"data_dependencies": []}
        with tempfile.TemporaryDirectory() as root:
            with open(os.path.join(root, "main.tf"), "w") as f:
                f.write(body)
            datastores.harvest_datastores(inventory, root)
        note = next(n for n in inventory["data_dependency_scan_notes"]
                    if "could not be attributed" in n)
        self.assertIn("8 of 8", note)
        self.assertIn("and 3 more", note)
        self.assertNotIn("acme-07", note)

    def test_the_scan_notes_name_what_could_not_be_attributed(self):
        """A count in the ledger, not just per-entry notes: the platform
        engineer reading the section needs to see the size of the gap without
        opening every entry."""
        inventory = {"data_dependencies": []}
        with tempfile.TemporaryDirectory() as root:
            with open(os.path.join(root, "main.tf"), "w") as f:
                f.write('resource "aws_db_instance" "lonely" {\n'
                        '  identifier = "acme-lonely"\n}\n')
            datastores.harvest_datastores(inventory, root)
        self.assertTrue(
            any("could not be attributed" in n
                for n in inventory["data_dependency_scan_notes"]),
            inventory["data_dependency_scan_notes"])


class AddressTest(unittest.TestCase):

    def test_the_address_is_recorded_even_when_a_real_name_is_known(self):
        """The join matches on the address. An RDS whose identifier came from
        db_name no longer looks like the block that declares it, so matching
        on identifier would fail exactly where the declaration is richest."""
        entries = _harvest({"main.tf":
                            'resource "aws_db_instance" "primary" {\n'
                            '  db_name = "catalog"\n}\n'})
        entry = _by_identifier(entries, "catalog")
        self.assertEqual(entry["address"], "aws_db_instance.primary")


class LiteralArnTest(unittest.TestCase):
    """Attribution through an ARN written literally, for a data store the
    estate reaches but does not declare."""

    TRUST = '''
resource "aws_iam_role" "orders" {
  assume_role_policy = <<EOF
{"Statement": [{"Condition": {"StringEquals": {
  "oidc.eks.us-east-1.amazonaws.com:sub": "system:serviceaccount:acme-shop:orders"}}}]}
EOF
}
'''

    def test_an_attached_policy_naming_an_arn_grants_the_role_subject(self):
        """The textbook three-block IRSA wiring, with the bucket in another
        repository: role, managed policy, attachment. The policy body names
        the bucket only by ARN."""
        entries = _harvest({"iam.tf": self.TRUST + '''
resource "aws_iam_policy" "orders_s3" {
  policy = <<EOF
{"Statement": [{"Action": "s3:*", "Resource": "arn:aws:s3:::acme-invoice-archive/*"}]}
EOF
}

resource "aws_iam_role_policy_attachment" "orders_s3" {
  role       = aws_iam_role.orders.name
  policy_arn = aws_iam_policy.orders_s3.arn
}
'''})
        entry = _by_identifier(entries, "acme-invoice-archive")
        self.assertEqual(entry["detection"], "referenced")
        self.assertEqual([(c["workload"], c["kind"], c["detection"])
                          for c in entry["consumers"]],
                         [("orders", "service_account", "irsa")])

    def test_the_irsa_module_taking_arns_directly(self):
        """`iam-role-for-service-accounts-eks` takes resource ARNs as its own
        arguments and builds the policy internally — no policy block, and
        here no bucket block either. The ARN in the module call is the only
        thing that names the dependency."""
        entries = _harvest({"iam.tf": '''
module "orders_irsa" {
  source = "terraform-aws-modules/iam/aws//modules/iam-role-for-service-accounts-eks"
  attach_mountpoint_s3_csi_policy = true
  mountpoint_s3_csi_bucket_arns   = ["arn:aws:s3:::acme-invoice-archive"]
  oidc_providers = {
    main = {
      provider_arn               = module.eks.oidc_provider_arn
      namespace_service_accounts = ["acme-shop:orders"]
    }
  }
}
'''})
        entry = _by_identifier(entries, "acme-invoice-archive")
        self.assertEqual([c["workload"] for c in entry["consumers"]], ["orders"])

    def test_a_helm_release_handed_an_arn_is_a_consumer(self):
        entries = _harvest({"apps.tf": '''
resource "helm_release" "orders" {
  name      = "orders"
  namespace = "acme-shop"
  chart     = "./charts/orders"
  set {
    name  = "queueArn"
    value = "arn:aws:sqs:us-east-1:123456789012:orders-events"
  }
}
'''})
        entry = _by_identifier(entries, "orders-events")
        self.assertEqual(entry["service"], "sqs")
        self.assertEqual([(c["workload"], c["kind"], c["detection"], c["source_path"])
                          for c in entry["consumers"]],
                         [("orders", "helm_release", "terraform_wiring", "charts/orders")])

    def test_a_config_map_carrying_an_arn_is_a_consumer(self):
        entries = _harvest({"apps.tf": '''
resource "kubernetes_config_map" "orders" {
  metadata {
    name      = "orders-config"
    namespace = "acme-shop"
  }
  data = {
    TABLE_ARN = "arn:aws:dynamodb:us-east-1:123456789012:table/orders"
  }
}
'''})
        entry = _by_identifier(entries, "orders")
        self.assertEqual([(c["workload"], c["kind"]) for c in entry["consumers"]],
                         [("orders-config", "kubernetes_config_map")])

    def test_per_app_groups_do_not_cross_attribute_literal_arns(self):
        """The per-app narrowing has to hold for literal ARNs exactly as it
        does for references: carts must not become a consumer of the orders
        table because both sit in one module call."""
        entries = _harvest({"main.tf": '''
module "platform" {
  source = "./modules/platform"
  apps = {
    carts = {
      namespace_service_accounts = ["carts:carts-sa"]
      table_arns                 = ["arn:aws:dynamodb:us-east-1:123456789012:table/carts"]
    }
    orders = {
      namespace_service_accounts = ["orders:orders-sa"]
      table_arns                 = ["arn:aws:dynamodb:us-east-1:123456789012:table/orders"]
    }
  }
}
'''})
        self.assertEqual([c["workload"] for c in _by_identifier(entries, "carts")["consumers"]],
                         ["carts-sa"])
        self.assertEqual([c["workload"] for c in _by_identifier(entries, "orders")["consumers"]],
                         ["orders-sa"])

    def test_a_group_naming_only_a_non_data_arn_is_not_a_grant(self):
        """`provider_arn = "arn:aws:iam::...:oidc-provider/..."` is a literal
        ARN too, and it grants nothing. Counting it as a grant would make the
        oidc group self-sufficient, stop the fall back to the whole body, and
        lose the real grant beside it."""
        entries = _harvest({"iam.tf": '''
module "orders_irsa" {
  source = "terraform-aws-modules/iam/aws//modules/iam-role-for-service-accounts-eks"
  mountpoint_s3_csi_bucket_arns = ["arn:aws:s3:::acme-invoice-archive"]
  oidc_providers = {
    main = {
      provider_arn               = "arn:aws:iam::123456789012:oidc-provider/oidc.eks.us-east-1.amazonaws.com/id/ABC"
      namespace_service_accounts = ["acme-shop:orders"]
    }
  }
}
'''})
        entry = _by_identifier(entries, "acme-invoice-archive")
        self.assertEqual([c["workload"] for c in entry["consumers"]], ["orders"])

    def test_a_boundary_policy_arn_is_not_a_grant(self):
        """A permissions boundary caps what a role may do; it grants nothing.
        Its literal ARNs must not attribute the role's subject, or a shared
        boundary hands the whole estate to every service account."""
        entries = _harvest({"iam.tf": '''
resource "aws_iam_policy" "boundary" {
  policy = <<EOF
{"Statement": [{"Action": "s3:*", "Resource": "arn:aws:s3:::acme-everything/*"}]}
EOF
}

resource "aws_iam_role" "orders" {
  permissions_boundary = aws_iam_policy.boundary.arn
  assume_role_policy = <<EOF
{"Statement": [{"Condition": {"StringEquals": {
  "oidc.eks.us-east-1.amazonaws.com:sub": "system:serviceaccount:acme-shop:orders"}}}]}
EOF
}
'''})
        entry = _by_identifier(entries, "acme-everything")
        self.assertEqual(entry["consumers"], [])
        self.assertIn(consumers.UNATTRIBUTED_NOTE, entry["notes"])

    def test_a_commented_out_arn_in_a_policy_attributes_nothing(self):
        entries = _harvest({"iam.tf": self.TRUST + '''
resource "aws_iam_role_policy" "orders" {
  role = aws_iam_role.orders.id
  # was: arn:aws:s3:::acme-invoice-archive
  policy = "{}"
}
resource "aws_iam_policy" "elsewhere" {
  policy = <<EOF
{"Resource": "arn:aws:s3:::acme-invoice-archive"}
EOF
}
'''})
        entry = _by_identifier(entries, "acme-invoice-archive")
        self.assertEqual(entry["consumers"], [])

    def test_an_arn_the_scan_did_not_record_attributes_nothing(self):
        """A wildcard ARN in a policy is not a grant on a recorded entry, so a
        role granted `arn:aws:s3:::acme-*` is not a consumer of a bucket the
        estate declares under that prefix — that would be name matching."""
        entries = _harvest({"iam.tf": self.TRUST + '''
resource "aws_s3_bucket" "archive" {
  bucket = "acme-invoice-archive"
}
resource "aws_iam_role_policy" "orders" {
  role = aws_iam_role.orders.id
  policy = <<EOF
{"Statement": [{"Resource": "arn:aws:s3:::acme-*"}]}
EOF
}
'''})
        entry = _by_identifier(entries, "acme-invoice-archive")
        self.assertEqual(entry["consumers"], [])

    def test_a_declared_twin_unions_both_chains_consumers(self):
        """The bucket is declared AND named by ARN in a policy. The release
        reaches it by reference, the service account by ARN; after the fold
        the one entry carries both."""
        entries = _harvest({"main.tf": self.TRUST + '''
resource "aws_s3_bucket" "archive" {
  bucket = "acme-invoice-archive"
}
resource "aws_iam_role_policy" "orders" {
  role = aws_iam_role.orders.id
  policy = <<EOF
{"Statement": [{"Resource": "arn:aws:s3:::acme-invoice-archive/*"}]}
EOF
}
resource "helm_release" "orders" {
  name      = "orders"
  namespace = "acme-shop"
  chart     = "./charts/orders"
  set {
    name  = "bucket"
    value = aws_s3_bucket.archive.bucket
  }
}
'''})
        self.assertEqual(len(entries), 1)
        entry = entries[0]
        self.assertEqual(entry["detection"], "declared")
        self.assertEqual(sorted((c["workload"], c["kind"]) for c in entry["consumers"]),
                         [("orders", "helm_release"), ("orders", "service_account")])


class LiteralArnsDoNotFlipPerAppTest(unittest.TestCase):
    """Regression from the first adversarial review: recording two queue
    ARNs must not turn a single-app module call into a per-app one and drop
    the declared datastore its subject was attributed to before."""

    def test_sibling_statements_naming_recorded_arns_do_not_make_a_block_per_app(self):
        entries = _harvest({"main.tf": '''
resource "aws_s3_bucket" "archive" {
  bucket = "acme-invoice-archive"
}

module "orders_irsa" {
  source                     = "./modules/irsa"
  namespace_service_accounts = ["acme-shop:orders"]
  bucket_arn                 = aws_s3_bucket.archive.arn
  extra = jsonencode({ Statement = [
    { Resource = "arn:aws:sqs:us-east-1:123456789012:a" },
    { Resource = "arn:aws:sqs:us-east-1:123456789012:b" },
  ]})
}
'''})
        archive = _by_identifier(entries, "acme-invoice-archive")
        self.assertEqual([c["workload"] for c in archive["consumers"]], ["orders"])
        # And the queues, named in the same body, are the subject's too.
        for queue in ("a", "b"):
            self.assertEqual([c["workload"] for c in _by_identifier(entries, queue)["consumers"]],
                             ["orders"], queue)

    def test_a_commented_arn_in_a_heredoc_values_block_attributes_nothing(self):
        entries = _harvest({"apps.tf": '''
resource "aws_iam_policy" "elsewhere" {
  policy = jsonencode({ Resource = "arn:aws:s3:::acme-old-archive" })
}
resource "helm_release" "orders" {
  name   = "orders"
  chart  = "./charts/orders"
  values = [<<EOT
    # legacy: arn:aws:s3:::acme-old-archive
  EOT
  ]
}
'''})
        self.assertEqual(_by_identifier(entries, "acme-old-archive")["consumers"], [])


class DescriptionValueTest(unittest.TestCase):
    """A description is prose on the consumer side too — and this view is
    what literal ARNs are attributed from."""

    def test_a_heredoc_in_a_multi_line_description_attributes_nothing(self):
        entries = _harvest({"main.tf": (
            'resource "aws_iam_policy" "real" {\n'
            '  policy = jsonencode({ Resource = "arn:aws:s3:::acme-invoice-archive" })\n'
            '}\n'
            'module "orders_irsa" {\n'
            '  source = "terraform-aws-modules/iam/aws//modules/'
            'iam-role-for-service-accounts-eks"\n'
            '  description = join("\\n", [\n'
            '    <<-EOT\n'
            '    Example grant: arn:aws:s3:::acme-invoice-archive\n'
            '    EOT\n'
            '  ])\n'
            '  oidc_providers = {\n'
            '    main = { namespace_service_accounts = ["acme-shop:orders"] }\n'
            '  }\n'
            '}\n')})
        entry = _by_identifier(entries, "acme-invoice-archive")
        self.assertEqual(entry["consumers"], [])

    def test_a_real_grant_in_the_same_module_still_attributes(self):
        entries = _harvest({"main.tf": (
            'module "orders_irsa" {\n'
            '  source = "terraform-aws-modules/iam/aws//modules/'
            'iam-role-for-service-accounts-eks"\n'
            '  description = join("\\n", ["the orders role"])\n'
            '  bucket_arns = ["arn:aws:s3:::acme-invoice-archive"]\n'
            '  oidc_providers = {\n'
            '    main = { namespace_service_accounts = ["acme-shop:orders"] }\n'
            '  }\n'
            '}\n')})
        entry = _by_identifier(entries, "acme-invoice-archive")
        self.assertEqual([c["workload"] for c in entry["consumers"]], ["orders"])


class NestedDescriptionTest(unittest.TestCase):
    """A description is prose at any depth on this side too — this view
    is what literal ARNs are attributed from."""

    def test_a_nested_description_does_not_attribute_a_workload(self):
        entries = _harvest({"main.tf": (
            'resource "aws_iam_policy" "p" {\n'
            '  policy = jsonencode({ Resource = "arn:aws:s3:::acme-invoice-archive" })\n'
            '}\n'
            'module "irsa" {\n'
            '  source = "terraform-aws-modules/iam/aws//modules/'
            'iam-role-for-service-accounts-eks"\n'
            '  oidc_providers = {\n'
            '    main = { namespace_service_accounts = ["orders:orders-sa"] }\n'
            '  }\n'
            '  policy_statements = [{\n'
            '    description = "we removed the grant on '
            'arn:aws:s3:::acme-invoice-archive last year"\n'
            '    resources   = ["arn:aws:s3:::acme-orders"]\n'
            '  }]\n'
            '}\n')})
        # The bucket the description mentions is recorded (the policy
        # states it) but the description must not make orders-sa its
        # consumer; the bucket the module really grants must.
        self.assertEqual(
            _by_identifier(entries, "acme-invoice-archive")["consumers"], [])
        self.assertEqual([c["workload"] for c in
                          _by_identifier(entries, "acme-orders")["consumers"]],
                         ["orders-sa"])

    def test_a_nested_configuration_key_still_attributes(self):
        entries = _harvest({"apps.tf": (
            'resource "kubernetes_config_map" "orders" {\n'
            '  metadata { name = "orders-config" }\n'
            '  data = {\n'
            '    archive = "arn:aws:s3:::acme-invoice-archive"\n'
            '  }\n'
            '}\n')})
        self.assertEqual([c["workload"] for c in
                          _by_identifier(entries, "acme-invoice-archive")["consumers"]],
                         ["orders-config"])


class NestedDescriptionDepthTest(unittest.TestCase):
    """A nested `description` whose value opens a delimiter must not
    disturb the block-level depth the other meta-argument guards read."""

    def test_a_nested_list_description_does_not_unblank_depends_on(self):
        entries = _harvest({"main.tf": (
            'module "orders_rds" {\n'
            '  source     = "terraform-aws-modules/rds/aws"\n'
            '  identifier = "orders-db"\n'
            '  engine     = "postgres"\n'
            '}\n'
            'resource "helm_release" "metrics_server" {\n'
            '  name      = "metrics-server"\n'
            '  namespace = "kube-system"\n'
            '  values = [\n'
            '    {\n'
            '      description = [\n'
            '        "history: this chart used to read arn:aws:s3:::old-bucket",\n'
            '      ]\n'
            '    },\n'
            '  ]\n'
            '  depends_on = [module.orders_rds]\n'
            '}\n')})
        # `depends_on` is apply ordering, not need: the release must not
        # become a consumer of the database.
        self.assertEqual(_by_identifier(entries, "orders-db")["consumers"], [])

    def test_a_nested_call_description_does_not_blank_a_later_config_key(self):
        entries = _harvest({"main.tf": (
            'resource "aws_iam_policy" "real" {\n'
            '  policy = jsonencode({ Resource = "arn:aws:s3:::acme-archive" })\n'
            '}\n'
            'resource "kubernetes_config_map" "orders" {\n'
            '  metadata { name = "orders-config" }\n'
            '  data = {\n'
            '    description = ("see the runbook")\n'
            '    archive     = "arn:aws:s3:::acme-archive"\n'
            '  }\n'
            '}\n')})
        self.assertEqual([c["workload"] for c in
                          _by_identifier(entries, "acme-archive")["consumers"]],
                         ["orders-config"])


class LiteralArnPerAppTest(unittest.TestCase):
    """A per-app map whose grants are literal ARNs, with only one
    subject recognisable, must still scope each app to its own."""

    def test_one_recognised_subject_does_not_collect_a_siblings_arn(self):
        entries = _harvest({"main.tf": '''
module "irsa" {
  source = "terraform-aws-modules/iam/aws//modules/iam-role-for-service-accounts-eks"
  apps = {
    carts = {
      bucket_arns                = ["arn:aws:s3:::carts-data"]
      namespace_service_accounts = ["carts:carts-sa"]
    }
    orders = {
      bucket_arns                = ["arn:aws:s3:::orders-data"]
      namespace_service_accounts = var.orders_service_accounts
    }
  }
}
'''})
        self.assertEqual([c["workload"] for c in
                          _by_identifier(entries, "carts-data")["consumers"]],
                         ["carts-sa"])
        self.assertEqual(_by_identifier(entries, "orders-data")["consumers"], [])

    def test_a_single_app_module_with_literal_statements_keeps_its_grants(self):
        """The other direction, which is why literal ARNs were kept out
        of the per-app test: two statement groups are not two apps."""
        entries = _harvest({"main.tf": '''
resource "aws_s3_bucket" "archive" {
  bucket = "acme-invoice-archive"
}

module "orders_irsa" {
  source                     = "./modules/irsa"
  namespace_service_accounts = ["acme-shop:orders"]
  bucket_arn                 = aws_s3_bucket.archive.arn
  extra = jsonencode({ Statement = [
    { Resource = "arn:aws:sqs:us-east-1:123456789012:a" },
    { Resource = "arn:aws:sqs:us-east-1:123456789012:b" },
  ]})
}
'''})
        for identifier in ("acme-invoice-archive", "a", "b"):
            self.assertEqual([c["workload"] for c in
                              _by_identifier(entries, identifier)["consumers"]],
                             ["orders"], identifier)


class PerAppSubjectOutsideGrantsTest(unittest.TestCase):
    """A wrapper that keys grants under one argument and subjects under
    another is still per-app when the grants are references."""

    def test_a_subject_outside_the_bearing_siblings_collects_nothing(self):
        entries = _harvest({"main.tf": '''
resource "aws_dynamodb_table" "carts" {
  name = "carts-tbl"
}
resource "aws_dynamodb_table" "orders" {
  name = "orders-tbl"
}
module "irsa" {
  source = "./modules/irsa"
  oidc_providers = {
    carts  = { namespace_service_accounts = ["carts:carts-sa"] }
    orders = { namespace_service_accounts = var.orders_service_accounts }
  }
  app_policies = {
    carts  = { table_arns = [aws_dynamodb_table.carts.arn] }
    orders = { table_arns = [aws_dynamodb_table.orders.arn] }
  }
}
'''})
        # Per-app by its reference-bearing siblings. carts-sa's own
        # groups carry no grant, so it honestly gets nothing — and above
        # all does not get the orders team's table.
        for identifier in ("carts-tbl", "orders-tbl"):
            self.assertEqual(
                _by_identifier(entries, identifier)["consumers"], [],
                identifier)


class PerAppLiteralGrantsElsewhereTest(unittest.TestCase):
    """A per-app map is told from a list of policy statements by how its
    siblings are keyed, not by where a subject happens to sit."""

    WRAPPER = '''
module "irsa" {
  source = "./modules/irsa"
  oidc_providers = {
    carts  = { namespace_service_accounts = ["carts:carts-sa"] }
    orders = { namespace_service_accounts = var.orders_service_accounts }
  }
  app_policies = {
    carts  = { table_arns = [%s] }
    orders = { table_arns = [%s] }
  }
}
'''

    def test_literal_grants_with_the_subject_elsewhere_do_not_cross_attribute(self):
        entries = _harvest({"main.tf": self.WRAPPER % (
            '"arn:aws:dynamodb:us-east-1:111111111111:table/carts-tbl"',
            '"arn:aws:dynamodb:us-east-1:111111111111:table/orders-tbl"')})
        for identifier in ("carts-tbl", "orders-tbl"):
            self.assertEqual(
                _by_identifier(entries, identifier)["consumers"], [], identifier)

    def test_a_mixed_reference_and_literal_wrapper_does_not_cross_attribute(self):
        entries = _harvest({"main.tf": (
            'resource "aws_dynamodb_table" "carts" {\n  name = "carts-tbl"\n}\n'
            + self.WRAPPER % (
                'aws_dynamodb_table.carts.arn',
                '"arn:aws:dynamodb:us-east-1:111111111111:table/orders-tbl"'))})
        for identifier in ("carts-tbl", "orders-tbl"):
            self.assertEqual(
                _by_identifier(entries, identifier)["consumers"], [], identifier)


class PerAppKeySpellingTest(unittest.TestCase):
    """HCL keys a map entry with `=` or `:`, quoted or bare. All four
    spellings are a per-app map; a list element is none of them."""

    WRAPPER = '''
module "irsa" {
  source = "./modules/irsa"
  oidc_providers = {
    %(carts)s { namespace_service_accounts = ["carts:carts-sa"] }
    %(orders)s { namespace_service_accounts = var.orders }
  }
  app_policies = {
    %(carts)s { table_arns = ["arn:aws:dynamodb:us-east-1:111111111111:table/carts-tbl"] }
    %(orders)s { table_arns = ["arn:aws:dynamodb:us-east-1:111111111111:table/orders-tbl"] }
  }
}
'''

    def test_every_key_spelling_is_a_per_app_map(self):
        for carts, orders in (("carts =", "orders ="),
                              ('"carts" =', '"orders" ='),
                              ("carts:", "orders:"),
                              ('"carts":', '"orders":')):
            with self.subTest(key=carts):
                entries = _harvest({"main.tf": self.WRAPPER % {
                    "carts": carts, "orders": orders}})
                for identifier in ("carts-tbl", "orders-tbl"):
                    self.assertEqual(
                        _by_identifier(entries, identifier)["consumers"], [],
                        f"{identifier} with {carts}")

    def test_a_json_colon_statement_list_is_still_one_app(self):
        entries = _harvest({"main.tf": '''
resource "aws_s3_bucket" "archive" {
  bucket = "acme-invoice-archive"
}

module "orders_irsa" {
  source                     = "./modules/irsa"
  namespace_service_accounts = ["acme-shop:orders"]
  bucket_arn                 = aws_s3_bucket.archive.arn
  extra = jsonencode({ "Statement": [
    { "Resource": "arn:aws:sqs:us-east-1:123456789012:a" },
    { "Resource": "arn:aws:sqs:us-east-1:123456789012:b" },
  ]})
}
'''})
        for identifier in ("acme-invoice-archive", "a", "b"):
            self.assertEqual([c["workload"] for c in
                              _by_identifier(entries, identifier)["consumers"]],
                             ["orders"], identifier)


class KeyedStatementLabelsTest(unittest.TestCase):
    """A map key is not always an app. `policy_statements = { read = {...},
    write = {...} }` keys ONE app's statements by label, and reading it as
    two apps strands the subject under a single-entry `oidc_providers` —
    which drops every grant the block has, references included."""

    _BLOCK = (
        'resource "aws_dynamodb_table" "carts" {\n  name = "carts"\n}\n'
        'resource "aws_iam_policy" "carts" {\n  name = "carts"\n'
        '  policy = jsonencode({ Statement = [{ Resource = '
        'aws_dynamodb_table.carts.arn }] })\n}\n'
        'module "irsa" {\n'
        '  source = "terraform-aws-modules/iam/aws//modules/'
        'iam-role-for-service-accounts-eks"\n'
        '  role_policy_arns = { carts = aws_iam_policy.carts.arn }\n'
        '  oidc_providers = {\n'
        '    main = { namespace_service_accounts = ["carts:carts-sa"] }\n'
        '  }\n'
        '  policy_statements = {\n'
        '    read  = { resources = ["arn:aws:s3:::acme-invoice-archive"] }\n'
        '    write = { resources = ["arn:aws:s3:::acme-invoice-staging"] }\n'
        '  }\n}\n')

    def test_statement_labels_do_not_strand_the_reference_chain(self):
        entries = _harvest({"main.tf": self._BLOCK})
        self.assertEqual([c["workload"] for c in
                          _by_identifier(entries, "carts")["consumers"]],
                         ["carts-sa"])

    def test_statement_labels_do_not_strand_the_literal_arns(self):
        entries = _harvest({"main.tf": self._BLOCK})
        for bucket in ("acme-invoice-archive", "acme-invoice-staging"):
            self.assertEqual([c["workload"] for c in
                              _by_identifier(entries, bucket)["consumers"]],
                             ["carts-sa"], bucket)

    def test_two_provider_entries_for_one_app_are_not_two_apps(self):
        # Blue/green, two regions, or two clusters trusting one app: both
        # `oidc_providers` entries name the SAME service account, so neither
        # sibling tells an app from another and the block is single-app.
        entries = _harvest({"main.tf": (
            'resource "aws_dynamodb_table" "carts" {\n  name = "carts"\n}\n'
            'resource "aws_iam_policy" "carts" {\n  name = "carts"\n'
            '  policy = jsonencode({ Statement = [{ Resource = '
            'aws_dynamodb_table.carts.arn }] })\n}\n'
            'module "irsa" {\n'
            '  source = "terraform-aws-modules/iam/aws//modules/'
            'iam-role-for-service-accounts-eks"\n'
            '  role_policy_arns = { carts = aws_iam_policy.carts.arn }\n'
            '  oidc_providers = {\n'
            '    blue  = { provider_arn = "x"\n'
            '              namespace_service_accounts = ["carts:carts-sa"] }\n'
            '    green = { provider_arn = "y"\n'
            '              namespace_service_accounts = ["carts:carts-sa"] }\n'
            '  }\n'
            '  policy_statements = {\n'
            '    read  = { resources = ["arn:aws:s3:::acme-invoice-archive"] }\n'
            '    write = { resources = ["arn:aws:s3:::acme-invoice-staging"] }\n'
            '  }\n}\n')})
        for name in ("carts", "acme-invoice-archive", "acme-invoice-staging"):
            self.assertEqual([c["workload"] for c in
                              _by_identifier(entries, name)["consumers"]],
                             ["carts-sa"], name)

    def test_a_real_per_app_wrapper_still_narrows(self):
        # The counter-direction: subjects keyed per app, so the subject's own
        # group HAS a sibling and the literal clause is free to fire.
        entries = _harvest({"main.tf": (
            'module "irsa" {\n  source = "./modules/irsa"\n'
            '  oidc_providers = {\n'
            '    carts  = { namespace_service_accounts = ["carts:carts-sa"] }\n'
            '    orders = { namespace_service_accounts = var.orders }\n'
            '  }\n'
            '  app_policies = {\n'
            '    carts  = { table_arns = ['
            '"arn:aws:dynamodb:us-east-1:111111111111:table/carts-tbl"] }\n'
            '    orders = { table_arns = ['
            '"arn:aws:dynamodb:us-east-1:111111111111:table/orders-tbl"] }\n'
            '  }\n}\n')})
        for table in ("carts-tbl", "orders-tbl"):
            self.assertEqual(_by_identifier(entries, table)["consumers"], [],
                             table)


class NestedDescriptionIsConfigurationTest(unittest.TestCase):
    """A nested `description` is prose to the LITERAL view and ordinary
    configuration to the REFERENCE view. A ConfigMap `data` key called
    `description` is a real consumer edge, exactly as its `tags` key is."""

    def test_a_config_map_description_key_still_attributes(self):
        entries = _harvest({"main.tf": (
            'resource "aws_s3_bucket" "archive" {\n'
            '  bucket = "acme-invoice-archive"\n}\n'
            'resource "kubernetes_config_map" "orders" {\n'
            '  metadata {\n    name      = "orders-cfg"\n    namespace = "shop"\n  }\n'
            '  data = { description = aws_s3_bucket.archive.id }\n}\n')})
        self.assertEqual([c["workload"] for c in
                          _by_identifier(entries, "acme-invoice-archive")["consumers"]],
                         ["orders-cfg"])

    def test_a_top_level_description_is_still_prose_to_both_views(self):
        entries = _harvest({"main.tf": (
            'resource "aws_s3_bucket" "archive" {\n'
            '  bucket = "acme-invoice-archive"\n}\n'
            'resource "kubernetes_config_map" "orders" {\n'
            '  metadata {\n    name      = "orders-cfg"\n    namespace = "shop"\n  }\n'
            '  description = aws_s3_bucket.archive.id\n'
            '  data = { replicas = "1" }\n}\n')})
        self.assertEqual(
            _by_identifier(entries, "acme-invoice-archive")["consumers"], [])

class DescriptionEndsAtItsCommaTest(unittest.TestCase):
    """A depth-zero comma ends an HCL object element, so a `description`
    sharing a line with a real grant must not blank it."""

    def test_a_grant_after_a_description_comma_still_attributes(self):
        entries = _harvest({"main.tf": (
            'resource "aws_s3_bucket" "archive" {\n'
            '  bucket = "acme-invoice-archive"\n}\n'
            'module "irsa" {\n'
            '  source = "terraform-aws-modules/iam/aws//modules/'
            'iam-role-for-service-accounts-eks"\n'
            '  oidc_providers = {\n'
            '    main = { namespace_service_accounts = ["orders:orders-sa"] }\n'
            '  }\n'
            '  policy_statements = [{\n'
            '    description = "legacy", '
            'resources = ["arn:aws:s3:::acme-invoice-archive"]\n'
            '  }]\n}\n')})
        self.assertEqual([c["workload"] for c in
                          _by_identifier(entries, "acme-invoice-archive")["consumers"]],
                         ["orders-sa"])


class ReviewRoundFortySevenTest(unittest.TestCase):
    """Regressions from the forty-seventh adversarial review round."""

    _CARTS = (
        'resource "aws_dynamodb_table" "carts" {\n  name = "carts-table"\n}\n'
        'resource "aws_iam_policy" "carts" {\n  policy = jsonencode({ Statement = '
        '[{ Resource = aws_dynamodb_table.carts.arn }] })\n}\n')
    _KEYED = ('  policy_statements = {\n'
              '    read  = { resources = ["arn:aws:s3:::acme-a"] }\n'
              '    write = { resources = ["arn:aws:s3:::acme-b"] }\n  }\n')

    def _consumers_of(self, module_body, identifier="carts-table"):
        entries = _harvest({"main.tf": self._CARTS + module_body})
        return [c["workload"] for c in _by_identifier(entries, identifier)["consumers"]]

    def test_a_quoted_key_is_read(self):
        # `_group_key` skipped whitespace in the mask, where a quoted string
        # is blanked WITH its quotes, so it strode past the key and read the
        # tail of the previous line. The JSON-style trust policy — the
        # ordinary spelling — therefore still stranded the block.
        quoted_trust = (
            'module "irsa" {\n  source = "./modules/irsa"\n'
            '  role_policy_arns = { carts = aws_iam_policy.carts.arn }\n'
            '  trust = jsonencode({ "Statement" = [{ "Condition" = {\n'
            '    "StringEquals" = { "oidc:sub" = "system:serviceaccount:carts:carts-sa" }\n'
            '    "StringLike"   = { "oidc:aud" = "sts.amazonaws.com" }\n'
            '  } }] })\n' + self._KEYED + '}\n')
        self.assertEqual(self._consumers_of(quoted_trust), ["carts-sa"])
        quoted_providers = (
            'module "irsa" {\n'
            '  source = "terraform-aws-modules/iam/aws//modules/iam-role-for-service-accounts-eks"\n'
            '  role_policy_arns = { carts = aws_iam_policy.carts.arn }\n'
            '  oidc_providers = {\n'
            '    "main"     = { provider_arn = "x", namespace_service_accounts = ["carts:carts-sa"] }\n'
            '    "defaults" = { provider_arn = "" }\n  }\n'
            '  policy_statements = {\n'
            '    "read"  = { resources = ["arn:aws:s3:::acme-a"] }\n'
            '    "write" = { resources = ["arn:aws:s3:::acme-b"] }\n  }\n}\n')
        self.assertEqual(self._consumers_of(quoted_providers), ["carts-sa"])

    def test_disjoint_keys_with_a_subject_slot_stay_per_app(self):
        # Round 29's error class, kept: a second slot that carries a subject
        # argument of its own is another app whatever it is called, and the
        # block is per-app — nothing attributed, never cross-attributed.
        wrapper = (
            'module "irsa" {\n  source = "./modules/irsa"\n'
            '  oidc_providers = {\n'
            '    carts_sa  = { namespace_service_accounts = ["carts:carts-sa"] }\n'
            '    orders_sa = { namespace_service_accounts = var.orders_sas }\n  }\n'
            '  app_policies = {\n'
            '    carts  = { resources = ["arn:aws:s3:::carts-bucket"] }\n'
            '    orders = { resources = ["arn:aws:s3:::orders-bucket"] }\n  }\n}\n')
        entries = _harvest({"main.tf": wrapper})
        for identifier in ("carts-bucket", "orders-bucket"):
            self.assertEqual(_by_identifier(entries, identifier)["consumers"], [], identifier)


class ReviewRoundFortyEightTest(unittest.TestCase):
    """Regressions from the forty-eighth adversarial review round."""

    def test_a_house_wrapper_naming_its_subject_argument_otherwise_stays_per_app(self):
        # "Strong" was keyed on the upstream module's argument name; a
        # wrapper spelling it `subject = …` with slot keys unlike its grant
        # keys read as single-app and handed carts-sa the orders bucket. The
        # argument the subject sits under is read from its own slot now.
        wrapper = (
            'module "irsa" {\n  source = "./modules/irsa"\n'
            '  apps = {\n'
            '    carts_sa  = { subject = "system:serviceaccount:carts:carts-sa" }\n'
            '    orders_sa = { subject = var.orders_subject }\n  }\n'
            '  app_policies = {\n'
            '    carts  = { resources = ["arn:aws:s3:::carts-bucket"] }\n'
            '    orders = { resources = ["arn:aws:s3:::orders-bucket"] }\n  }\n}\n')
        entries = _harvest({"main.tf": wrapper})
        for identifier in ("carts-bucket", "orders-bucket"):
            self.assertEqual(_by_identifier(entries, identifier)["consumers"], [], identifier)

    def test_a_house_wrapper_with_a_defaults_slot_is_still_single_app(self):
        # The same custom spelling, one real slot beside a defaults entry
        # that carries no `subject`: weak slot, disjoint keys, single-app —
        # the reference grant keeps its consumer.
        table = ('resource "aws_dynamodb_table" "carts" {\n  name = "carts-table"\n}\n'
                 'resource "aws_iam_policy" "carts" {\n  policy = jsonencode({ Statement = '
                 '[{ Resource = aws_dynamodb_table.carts.arn }] })\n}\n')
        wrapper = (
            'module "irsa" {\n  source = "./modules/irsa"\n'
            '  role_policy_arns = { carts = aws_iam_policy.carts.arn }\n'
            '  apps = {\n'
            '    main     = { subject = "system:serviceaccount:carts:carts-sa" }\n'
            '    defaults = { provider_arn = "" }\n  }\n'
            '  policy_statements = {\n'
            '    read  = { resources = ["arn:aws:s3:::acme-a"] }\n'
            '    write = { resources = ["arn:aws:s3:::acme-b"] }\n  }\n}\n')
        entries = _harvest({"main.tf": table + wrapper})
        self.assertEqual([c["workload"] for c in _by_identifier(entries, "carts-table")["consumers"]],
                         ["carts-sa"])

    def test_a_quoted_description_key_is_prose_in_the_consumer_view_too(self):
        # The harvest blanked `"description" =`; the consumer literal view
        # blanked only the bare spelling, so once the ARN was recorded from a
        # real grant elsewhere the prose mention handed this role's service
        # account the bucket. The two literal views have to agree.
        entries = _harvest({"main.tf": (
            'resource "aws_iam_policy" "orders" {\n  policy = jsonencode({ Statement = '
            '[{ Resource = ["arn:aws:s3:::acme-a"] }] })\n}\n'
            'module "irsa" {\n'
            '  source = "terraform-aws-modules/iam/aws//modules/iam-role-for-service-accounts-eks"\n'
            '  oidc_providers = { main = { provider_arn = "x", '
            'namespace_service_accounts = ["carts:carts-sa"] } }\n'
            '  policy_statements = [{ "description" = "we removed the grant on '
            'arn:aws:s3:::acme-a last year", resources = ["arn:aws:s3:::acme-b"] }]\n}\n')})
        self.assertEqual(_by_identifier(entries, "acme-a")["consumers"], [])
        self.assertEqual([c["workload"] for c in _by_identifier(entries, "acme-b")["consumers"]],
                         ["carts-sa"])


class ReviewRoundFortyNineTest(unittest.TestCase):
    """Regressions from the forty-ninth adversarial review round."""

    _TABLE = ('resource "aws_dynamodb_table" "carts" {\n  name = "carts-table"\n}\n'
              'resource "aws_iam_policy" "carts" {\n  policy = jsonencode({ Statement = '
              '[{ Resource = aws_dynamodb_table.carts.arn }] })\n}\n')
    _GRANTS = ('  app_policies = {\n'
               '    carts  = { resources = ["arn:aws:s3:::carts-bucket"] }\n'
               '    orders = { resources = ["arn:aws:s3:::orders-bucket"] }\n  }\n')
    _KEYED = ('  policy_statements = {\n'
              '    read  = { resources = ["arn:aws:s3:::acme-a"] }\n'
              '    write = { resources = ["arn:aws:s3:::acme-b"] }\n  }\n')

    def test_a_ternary_subject_does_not_turn_a_slot_weak(self):
        # The conditional's `:` read as a key separator, making the first
        # branch's string the "holding argument"; the sibling then carried
        # nothing of that name, the slot was weak, and carts-sa collected
        # the orders bucket.
        entries = _harvest({"main.tf": (
            'module "irsa" {\n  source = "./m"\n  apps = {\n'
            '    carts_sa  = { subject = var.prod ? "system:serviceaccount:carts:carts-sa" '
            ': "system:serviceaccount:carts-dev:carts-sa" }\n'
            '    orders_sa = { subject = var.orders_subject }\n  }\n' + self._GRANTS + '}\n')})
        for identifier in ("carts-bucket", "orders-bucket"):
            self.assertEqual(_by_identifier(entries, identifier)["consumers"], [], identifier)

    def test_a_wildcard_condition_is_not_another_app(self):
        # `StringLike …:sub` beside `StringEquals …:sub` is one app's trust
        # widened by a wildcard; the sibling carries the very argument the
        # subject sits under, which made it STRONG, and a condition-operator
        # group is never a slot. (`carts:*` names no second subject; a
        # wildcard that still parses as one — `canary-*` — reads as two
        # subjects in two groups, which is the older per-app rule and not
        # this CL's.)
        entries = _harvest({"main.tf": self._TABLE + (
            'module "irsa" {\n  source = "./modules/irsa"\n'
            '  role_policy_arns = { carts = aws_iam_policy.carts.arn }\n'
            '  trust = jsonencode({ Statement = [{ Condition = {\n'
            '    StringEquals = { "oidc:sub" = "system:serviceaccount:carts:carts-sa" }\n'
            '    StringLike   = { "oidc:sub" = "system:serviceaccount:carts:*" }\n'
            '  } }] })\n' + self._KEYED + '}\n')})
        self.assertEqual([c["workload"] for c in _by_identifier(entries, "carts-table")["consumers"]],
                         ["carts-sa"])

    def test_a_string_mentioning_the_argument_does_not_make_a_slot_strong(self):
        entries = _harvest({"main.tf": self._TABLE + (
            'module "irsa" {\n  source = "./modules/irsa"\n'
            '  role_policy_arns = { carts = aws_iam_policy.carts.arn }\n'
            '  apps = {\n'
            '    main     = { subject = "system:serviceaccount:carts:carts-sa" }\n'
            '    defaults = { note = "subject = unset disables the role" }\n  }\n'
            + self._KEYED + '}\n')})
        self.assertEqual([c["workload"] for c in _by_identifier(entries, "carts-table")["consumers"]],
                         ["carts-sa"])

    def test_a_nested_subject_holds_under_its_innermost_key(self):
        # `main = { trust = { oidc = { subject = … } } }` holds under
        # `subject`, not `trust`; a `defaults = { trust = { provider_arn }
        # }` sibling is weak, while a sibling nesting its own `subject` is
        # strong.
        module = ('module "irsa" {\n  source = "./modules/irsa"\n'
                  '  role_policy_arns = { carts = aws_iam_policy.carts.arn }\n'
                  '  apps = {\n'
                  '    main     = { trust = { oidc = { subject = "system:serviceaccount:carts:carts-sa" } } }\n'
                  '    %s\n  }\n' + self._KEYED + '}\n')
        weak = _harvest({"main.tf": self._TABLE + module % 'defaults = { trust = { provider_arn = "" } }'})
        self.assertEqual([c["workload"] for c in _by_identifier(weak, "carts-table")["consumers"]],
                         ["carts-sa"])
        strong = _harvest({"main.tf": self._TABLE + module % 'orders = { trust = { oidc = { subject = var.orders } } }'})
        self.assertEqual(_by_identifier(strong, "carts-table")["consumers"], [])


class ReviewRoundFiftyTest(unittest.TestCase):
    """Regressions from the fiftieth adversarial review round."""

    def test_a_subject_in_a_list_element_holds_under_the_key_naming_the_list(self):
        # `subjects = [{ name = "system:…" }]` held under `name`, a generic
        # key a `defaults = { name = "irsa-default" }` sibling carries, so
        # the slot read strong and the block per-app. It holds under
        # `subjects`, the key of the innermost KEYED group.
        table = ('resource "aws_dynamodb_table" "carts" {\n  name = "carts-table"\n}\n'
                 'resource "aws_iam_policy" "carts" {\n  policy = jsonencode({ Statement = '
                 '[{ Resource = aws_dynamodb_table.carts.arn }] })\n}\n')
        wrapper = (
            'module "irsa" {\n  source = "./modules/irsa"\n'
            '  role_policy_arns = { carts = aws_iam_policy.carts.arn }\n'
            '  apps = {\n'
            '    main     = { subjects = [{ name = "system:serviceaccount:carts:carts-sa" }] }\n'
            '    defaults = { name = "irsa-default" }\n  }\n'
            '  policy_statements = {\n'
            '    read  = { resources = ["arn:aws:s3:::acme-a"] }\n'
            '    write = { resources = ["arn:aws:s3:::acme-b"] }\n  }\n}\n')
        entries = _harvest({"main.tf": table + wrapper})
        self.assertEqual([c["workload"] for c in _by_identifier(entries, "carts-table")["consumers"]],
                         ["carts-sa"])


if __name__ == "__main__":
    unittest.main()
