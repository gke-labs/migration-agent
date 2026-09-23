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

"""Unit tests for the target-range proposal. Pure code, no ledger."""

import unittest

from servers.phases.landingzone.landingzone_design_2 import ranges

ACME = {
    "vpcs": [{"name": "acme_prod", "cidr": "10.42.0.0/16", "secondary_cidrs": [], "cluster_vpc": True}],
    "subnets": [{"name": "private_a", "cidr": "10.42.0.0/19", "tier": "private"}],
    "clusters": [{"name": "acme-prod", "service_ipv4_cidr": None,
                  "remote_node_cidrs": [], "remote_pod_cidrs": []}],
    "routes": [{"destination": "10.20.0.0/16", "via": "vpc_peering_connection"}],
    "unresolved": [],
}


class SourceRangesTest(unittest.TestCase):

    def test_every_stated_range_is_listed_once_with_its_role(self):
        space = {
            "vpcs": [{"name": "v", "cidr": "10.0.0.0/16", "secondary_cidrs": ["100.64.0.0/16"],
                      "cluster_vpc": True},
                     {"name": "d", "cidr": "192.168.0.0/16", "cluster_vpc": True, "defaulted": True}],
            "subnets": [{"name": "a", "cidr": "10.0.0.0/19", "tier": "private"},
                        {"name": "dup", "cidr": "10.0.0.0/19", "tier": "private"}],
            "clusters": [{"name": "c", "service_ipv4_cidr": "172.20.0.0/16",
                          "remote_node_cidrs": ["10.52.0.0/16"], "remote_pod_cidrs": ["10.53.0.0/16"]}],
            "routes": [{"destination": "10.20.0.0/16", "via": "transit_gateway"}],
        }
        # The subnet 10.0.0.0/19 sits inside VPC v and is not repeated.
        self.assertEqual(ranges.source_ranges(space), [
            ("10.0.0.0/16", "cluster VPC v"),
            ("100.64.0.0/16", "secondary range of VPC v"),
            ("192.168.0.0/16", "cluster VPC d (eksctl default)"),
            ("172.20.0.0/16", "service range of cluster c"),
            ("10.52.0.0/16", "remote node range of cluster c"),
            ("10.53.0.0/16", "remote pod range of cluster c"),
            ("10.20.0.0/16", "routed via transit_gateway"),
        ])

    def test_an_ipv6_subnet_beside_an_ipv4_vpc_is_listed_not_an_error(self):
        # subnet_of across IP versions raises; the version check keeps the
        # IPv6 subnet out of the containment test and in the listing.
        out = ranges.source_ranges({"vpcs": [{"name": "v", "cidr": "10.0.0.0/16", "cluster_vpc": True}],
                                    "subnets": [{"name": "s6", "cidr": "2600:1f14::/64", "tier": "private"}]})
        self.assertEqual([cidr for cidr, _ in out], ["10.0.0.0/16", "2600:1f14::/64"])

    def test_same_named_vpcs_are_told_apart_by_their_place(self):
        out = ranges.source_ranges({"vpcs": [
            {"name": "main", "path": "envs/dev/vpc.tf", "cidr": "10.1.0.0/16", "cluster_vpc": True},
            {"name": "main", "path": "envs/prod/vpc.tf", "cidr": "10.2.0.0/16", "cluster_vpc": True},
            {"name": "shared", "path": "envs/prod/peer.tf", "cidr": "10.3.0.0/16", "cluster_vpc": False}]})
        self.assertEqual(out, [("10.1.0.0/16", "cluster VPC main in envs/dev"),
                               ("10.2.0.0/16", "cluster VPC main in envs/prod"),
                               ("10.3.0.0/16", "VPC shared")])
        # Two copies of one local module share a directory: the address
        # tells them apart.
        out = ranges.source_ranges({"vpcs": [
            {"name": "this", "address": "module.vpc_a.aws_vpc.this", "path": "modules/vpc/main.tf",
             "cidr": "10.0.0.0/16", "cluster_vpc": False},
            {"name": "this", "address": "module.vpc_b.aws_vpc.this", "path": "modules/vpc/main.tf",
             "cidr": "10.1.0.0/16", "cluster_vpc": False}]})
        self.assertEqual([label for _, label in out],
                         ["VPC this (module.vpc_a.aws_vpc.this)", "VPC this (module.vpc_b.aws_vpc.this)"])
        # Two templates in one directory: the file is the place, as the
        # harvester keys them.
        out = ranges.source_ranges({"vpcs": [
            {"name": "VPC", "path": "cfn/a.yaml", "cidr": "10.1.0.0/16", "cluster_vpc": False},
            {"name": "VPC", "path": "cfn/b.yaml", "cidr": "10.2.0.0/16", "cluster_vpc": False}]})
        self.assertEqual([label for _, label in out], ["VPC VPC in cfn/a.yaml", "VPC VPC in cfn/b.yaml"])

    def test_a_tierless_subnet_is_labelled_once(self):
        out = ranges.source_ranges({"vpcs": [], "subnets": [{"name": "a", "cidr": "10.50.1.0/24", "tier": None}]})
        self.assertEqual(out, [("10.50.1.0/24", "subnet a")])

    def test_a_subnet_whose_vpc_has_no_stated_range_is_kept(self):
        space = {"vpcs": [{"name": "existing", "cidr": None, "cluster_vpc": True}],
                 "subnets": [{"name": "a", "cidr": "10.7.0.0/24", "tier": "private"}]}
        self.assertEqual(ranges.source_ranges(space), [("10.7.0.0/24", "private subnet a")])

    def test_null_and_malformed_cidrs_are_skipped(self):
        space = {"vpcs": [{"name": "v", "cidr": None}, {"name": "w", "cidr": "not-a-cidr"}]}
        self.assertEqual(ranges.source_ranges(space), [])


class ProposalTest(unittest.TestCase):

    def test_baseline_kept_when_clear(self):
        proposal = ranges.propose_target_ranges(
            {"vpcs": [{"name": "v", "cidr": "10.42.0.0/16", "cluster_vpc": True}]})
        self.assertEqual(proposal["proposed"],
                         {"nodes": "10.0.0.0/22", "pods": "10.4.0.0/16", "services": "10.20.0.0/20"})
        self.assertEqual(proposal["moved"], [])

    def test_the_acme_peered_range_moves_the_service_block(self):
        # 10.20.0.0/16 is the peered shared-services VPC: the baseline
        # services block sits inside it. The first clear /20 after the
        # nodes block is 10.0.16.0/20.
        proposal = ranges.propose_target_ranges(ACME)
        self.assertEqual(proposal["proposed"],
                         {"nodes": "10.0.0.0/22", "pods": "10.4.0.0/16", "services": "10.0.16.0/20"})
        self.assertEqual(proposal["moved"], ["services"])

    def test_a_source_vpc_on_the_baseline_moves_everything_clear_of_it(self):
        proposal = ranges.propose_target_ranges(
            {"vpcs": [{"name": "v", "cidr": "10.0.0.0/8", "cluster_vpc": True}]})
        # 172.17.0.0/16 is skipped: the default Docker bridge uses it.
        self.assertEqual(proposal["proposed"],
                         {"nodes": "172.16.0.0/22", "pods": "172.18.0.0/16", "services": "172.16.16.0/20"})
        self.assertEqual(proposal["moved"], ["nodes", "pods", "services"])

    def test_a_moved_block_does_not_evict_a_later_baseline_that_is_clear(self):
        # 10.0.0.0/14 takes the node baseline only. The first clear /22
        # after it would be 10.4.0.0/22, inside the pod baseline: nodes go
        # past it instead, and pods and services stay where they were.
        proposal = ranges.propose_target_ranges(
            {"vpcs": [{"name": "v", "cidr": "10.0.0.0/14", "cluster_vpc": True}]})
        self.assertEqual(proposal["proposed"],
                         {"nodes": "10.5.0.0/22", "pods": "10.4.0.0/16", "services": "10.20.0.0/20"})
        self.assertEqual(proposal["moved"], ["nodes"])

    def test_proposed_blocks_never_overlap_each_other_or_the_source(self):
        import ipaddress
        proposal = ranges.propose_target_ranges(ACME)
        blocks = [ipaddress.ip_network(c) for c in proposal["proposed"].values()]
        taken = [ipaddress.ip_network(c) for c, _ in proposal["avoid"]]
        for i, block in enumerate(blocks):
            self.assertFalse(any(block.overlaps(t) for t in taken), block)
            self.assertFalse(any(block.overlaps(o) for o in blocks[i + 1:]), block)

    def test_unresolved_entries_are_counted(self):
        space = dict(ACME, unresolved=[{"address": "module.vpc", "argument": "private_subnets",
                                        "expression": "local.private_subnets"}])
        self.assertEqual(ranges.propose_target_ranges(space)["unresolved"], 1)

    def test_hub_and_spoke_summary_routes_send_pods_to_shared_address_space(self):
        # Transit-gateway summaries for all of 10/8 and 172.16/12: a pod /16
        # cannot fit beside the nodes block inside 192.168/16, so it takes
        # the shared space GKE accepts for pods.
        space = {"routes": [{"destination": "10.0.0.0/8", "via": "transit_gateway"},
                            {"destination": "172.16.0.0/12", "via": "transit_gateway"}]}
        proposal = ranges.propose_target_ranges(space)
        self.assertEqual(proposal["proposed"],
                         {"nodes": "192.168.0.0/22", "pods": "100.64.0.0/16", "services": "192.168.16.0/20"})

    def test_a_kind_with_no_free_block_is_said_so(self):
        space = {"routes": [{"destination": "10.0.0.0/8", "via": "transit_gateway"},
                            {"destination": "172.16.0.0/12", "via": "transit_gateway"},
                            {"destination": "192.168.0.0/16", "via": "transit_gateway"},
                            {"destination": "100.64.0.0/10", "via": "transit_gateway"}]}
        text = ranges.describe_proposal(space)
        self.assertIn("no_free_block_for: nodes, pods, services", text)

    def test_covered_unresolved_entries_do_not_block(self):
        space = {"vpcs": [{"name": "v", "address": "module.vpc", "path": "main.tf", "cidr": "10.0.0.0/16", "cluster_vpc": True}],
                 "subnets": [{"name": "a", "address": "aws_subnet.a", "path": "main.tf", "cidr": None, "vpc": "module.vpc"}],
                 "clusters": [], "routes": [],
                 "unresolved": [{"address": "module.vpc", "path": "main.tf", "argument": "private_subnets", "expression": "local.x"},
                                {"address": "aws_subnet.a", "path": "main.tf", "argument": "cidr_block", "expression": "cidrsubnet(...)"},
                                {"address": "module.eks", "path": "main.tf", "argument": "public_access_cidrs", "expression": "var.office"},
                                {"address": "aws_route.peer", "path": "main.tf", "argument": "destination_cidr_block", "expression": "var.peer (no default)"}]}
        proposal = ranges.propose_target_ranges(space)
        self.assertEqual((proposal["unresolved"], proposal["covered"]), (1, 3))
        text = ranges.describe_proposal(space)
        self.assertIn("unresolved_source_ranges: 1", text)
        self.assertIn("unresolved_but_covered: 3", text)

    def test_the_reserved_docker_bridge_range_is_never_proposed(self):
        import ipaddress
        proposal = ranges.propose_target_ranges(
            {"vpcs": [{"name": "v", "cidr": "10.0.0.0/8", "cluster_vpc": True},
                      {"name": "w", "cidr": "172.16.0.0/16", "cluster_vpc": False}]})
        reserved = ipaddress.ip_network("172.17.0.0/16")
        for cidr in proposal["proposed"].values():
            self.assertFalse(ipaddress.ip_network(cidr).overlaps(reserved), cidr)

    def test_triage_keeps_sibling_directories_apart(self):
        # dev's module.vpc states its range; prod's does not. Prod's primary
        # range is a real question, not a subnet covered by dev's VPC.
        space = {"vpcs": [{"name": "vpc", "address": "module.vpc", "path": "envs/dev/main.tf", "cidr": "10.1.0.0/16"},
                          {"name": "vpc", "address": "module.vpc", "path": "envs/prod/main.tf", "cidr": None}],
                 "subnets": [{"name": "vpc/private_subnets[0]", "address": "module.vpc", "path": "envs/dev/main.tf",
                              "cidr": "10.1.0.0/19", "vpc": "module.vpc"}],
                 "clusters": [], "routes": [],
                 "unresolved": [{"address": "module.vpc", "path": "envs/prod/main.tf", "argument": "cidr",
                                 "expression": "var.vpc_cidr (no default)"}]}
        proposal = ranges.propose_target_ranges(space)
        self.assertEqual((proposal["unresolved"], proposal["covered"]), (1, 0))

    def test_a_subnet_beside_an_unstated_vpc_is_not_covered_by_a_namesake_elsewhere(self):
        # dev's aws_vpc.main states its range; prod's comes from an IPAM
        # pool. Prod's counted subnets sit in prod's VPC, whose range nobody
        # wrote down: a question, not "inside a stated VPC range".
        space = {"vpcs": [{"name": "main", "address": "aws_vpc.main", "path": "envs/dev/vpc.tf", "cidr": "10.1.0.0/16"},
                          {"name": "main", "address": "aws_vpc.main", "path": "envs/prod/vpc.tf", "cidr": None}],
                 "subnets": [{"name": "p", "address": "aws_subnet.p", "path": "envs/prod/vpc.tf",
                              "cidr": None, "vpc": "aws_vpc.main"}],
                 "clusters": [], "routes": [],
                 "unresolved": [{"address": "aws_subnet.p", "path": "envs/prod/vpc.tf", "argument": "cidr_block",
                                 "expression": "cidrsubnet(aws_vpc.main.cidr_block, 4, count.index) over for_each/count = 3"}]}
        proposal = ranges.propose_target_ranges(space)
        self.assertEqual((proposal["unresolved"], proposal["covered"]), (1, 0))

    def test_the_no_range_echo_carries_the_same_questions(self):
        # A VPC by id, a cluster whose VPC is not recorded, an allow list in
        # unresolved and a defaulted service range: every line the checked
        # echo would carry is here too, under the baseline caveat.
        space = {"vpcs": [{"name": "existing", "address": "aws_vpc.existing", "path": "vpc.tf", "cidr": None}],
                 "subnets": [], "routes": [],
                 "clusters": [{"name": "c", "address": "aws_eks_cluster.c", "path": "eks.tf",
                               "vpc": "data.aws_vpc.other", "service_ipv4_cidr": None}],
                 "unresolved": [{"address": "aws_eks_cluster.c", "path": "eks.tf",
                                 "argument": "public_access_cidrs", "expression": "var.office"},
                                {"address": "aws_route.peer", "path": "vpc.tf",
                                 "argument": "destination_cidr_block", "expression": "var.peer (no default)"}]}
        text = ranges.describe_proposal(space)
        self.assertIn("source_address_space: no range stated — the files name the network (existing)", text)
        self.assertIn("proposed_target_ranges: nodes 10.0.0.0/22, pods 10.4.0.0/16, services 10.20.0.0/20 "
                      "(the baseline, not checked against the source)", text)
        self.assertIn("clusters_without_a_recorded_vpc: c (vpc: data.aws_vpc.other)", text)
        self.assertIn("clusters_with_a_defaulted_service_range: c", text)
        self.assertIn("unresolved_source_ranges: 1", text)
        self.assertIn("unresolved_but_covered: 1", text)
        self.assertEqual(text.count("vpcs_without_a_stated_range"), 0)

    def test_triage_keeps_two_templates_in_one_directory_apart(self):
        # Two templates in one directory, both with a VPC and a subnet named
        # the same. a's VPC is stated, b's is not: a's subnet is covered and
        # b's is a question. Keyed by file, not directory, or the two would
        # merge and both answers would be wrong in one direction or the other.
        space = {"vpcs": [{"name": "VPC", "address": "VPC", "path": "cfn/a.yaml", "cidr": "10.1.0.0/16"},
                          {"name": "VPC", "address": "VPC", "path": "cfn/b.yaml", "cidr": None}],
                 "subnets": [{"name": "S", "address": "S", "path": "cfn/a.yaml", "cidr": None, "vpc": "VPC"},
                             {"name": "S", "address": "S", "path": "cfn/b.yaml", "cidr": None, "vpc": "VPC"}],
                 "clusters": [], "routes": [],
                 "unresolved": [{"address": "S", "path": "cfn/a.yaml", "argument": "CidrBlock", "expression": "{}"},
                                {"address": "S", "path": "cfn/b.yaml", "argument": "CidrBlock", "expression": "{}"}]}
        proposal = ranges.propose_target_ranges(space)
        self.assertEqual((proposal["unresolved"], proposal["covered"]), (1, 1))

    def test_a_subnet_of_a_vpc_whose_range_is_asked_for_is_covered(self):
        space = {"vpcs": [{"name": "main", "address": "aws_vpc.main", "path": "main.tf", "cidr": None}],
                 "subnets": [{"name": "s", "address": "aws_subnet.s", "path": "main.tf", "cidr": None, "vpc": "aws_vpc.main"}],
                 "clusters": [], "routes": [],
                 "unresolved": [{"address": "aws_vpc.main", "path": "main.tf", "argument": "cidr_block",
                                 "expression": "var.vpc_cidr (no default)"},
                                {"address": "aws_subnet.s", "path": "main.tf", "argument": "cidr_block",
                                 "expression": "cidrsubnet(var.vpc_cidr, 4, count.index) over for_each/count = 3"}]}
        proposal = ranges.propose_target_ranges(space)
        self.assertEqual((proposal["unresolved"], proposal["covered"]), (1, 1))
        self.assertIn("unresolved_but_covered: 1", ranges.describe_proposal(space))

    def test_two_copies_under_one_key_cover_only_when_every_copy_is_stated(self):
        # Two copies of a module VPC share a key once written; one is
        # stated, the other allocated from IPAM with no question. A subnet
        # under that key may belong to either: a question.
        space = {"vpcs": [{"name": "this", "address": "module.network.aws_vpc.this",
                           "path": "modules/network/main.tf", "cidr": "10.1.0.0/16"},
                          {"name": "this", "address": "module.network.aws_vpc.this",
                           "path": "modules/network/main.tf", "cidr": None}],
                 "subnets": [{"name": "p", "address": "module.network.aws_subnet.p",
                              "path": "modules/network/main.tf", "cidr": None, "vpc": "module.network.aws_vpc.this"}],
                 "clusters": [], "routes": [],
                 "unresolved": [{"address": "module.network.aws_subnet.p", "path": "modules/network/main.tf",
                                 "argument": "cidr_block", "expression": "cidrsubnet(var.x, 4, count.index)"}]}
        proposal = ranges.propose_target_ranges(space)
        self.assertEqual((proposal["unresolved"], proposal["covered"]), (1, 0))

    def test_a_subnet_without_a_recorded_place_rides_on_an_asked_carrier(self):
        # An entry written without vpc_path (a hand-built or older section):
        # the carrier fallback treats a VPC whose own range is a question as
        # settled, as the keyed rule does.
        space = {"vpcs": [{"name": "main", "address": "aws_vpc.main", "path": "main.tf", "cidr": None}],
                 "subnets": [{"name": "s", "address": "module.net.aws_subnet.s", "path": "modules/net/main.tf",
                              "cidr": None, "vpc": "aws_vpc.main"}],
                 "clusters": [], "routes": [],
                 "unresolved": [{"address": "aws_vpc.main", "path": "main.tf", "argument": "cidr_block",
                                 "expression": "var.vpc_cidr (no default)"},
                                {"address": "module.net.aws_subnet.s", "path": "modules/net/main.tf",
                                 "argument": "cidr_block", "expression": "cidrsubnet(var.vpc_cidr, 4, count.index)"}]}
        proposal = ranges.propose_target_ranges(space)
        self.assertEqual((proposal["unresolved"], proposal["covered"]), (1, 1))

    def test_a_subnet_with_no_recorded_vpc_is_never_covered(self):
        # No carrier at all (vpc null, or a data source nothing recorded):
        # nothing can cover the subnet, whatever all() says of an empty list.
        for vpc in (None, "data.aws_vpc.selected"):
            space = {"vpcs": [], "clusters": [], "routes": [],
                     "subnets": [{"name": "s", "address": "aws_subnet.s", "path": "main.tf", "cidr": None, "vpc": vpc}],
                     "unresolved": [{"address": "aws_subnet.s", "path": "main.tf", "argument": "cidr_block",
                                     "expression": "var.x (no default)"}]}
            proposal = ranges.propose_target_ranges(space)
            self.assertEqual((proposal["unresolved"], proposal["covered"]), (1, 0), vpc)

    def test_a_cloudformation_subnet_never_borrows_a_vpc_from_another_template(self):
        # b.yaml's subnet names VpcId through a Parameter; a.yaml declares
        # a stated VPC under that logical id. Only Terraform references
        # cross files, so b's subnet is a question, not covered by a's VPC.
        space = {"vpcs": [{"name": "VPC", "address": "VPC", "path": "cfn/a.yaml", "cidr": "10.1.0.0/16"}],
                 "subnets": [{"name": "S", "address": "S", "path": "cfn/b.yaml", "cidr": None, "vpc": "VPC"}],
                 "clusters": [], "routes": [],
                 "unresolved": [{"address": "S", "path": "cfn/b.yaml", "argument": "CidrBlock", "expression": "{}"}]}
        proposal = ranges.propose_target_ranges(space)
        self.assertEqual((proposal["unresolved"], proposal["covered"]), (1, 0))

    def test_a_list_filed_under_a_stated_inner_vpc_is_covered(self):
        space = {"vpcs": [{"name": "main", "address": "aws_vpc.main", "path": "modules/vpc/main.tf", "cidr": "10.0.0.0/16"}],
                 "subnets": [], "clusters": [], "routes": [],
                 "unresolved": [{"address": "aws_vpc.main", "path": "modules/vpc/main.tf",
                                 "argument": "private_subnets", "expression": "[for ...]"}]}
        proposal = ranges.propose_target_ranges(space)
        self.assertEqual((proposal["unresolved"], proposal["covered"]), (0, 1))

    def test_a_subnet_in_a_module_is_covered_by_the_root_vpc(self):
        space = {"vpcs": [{"name": "main", "address": "aws_vpc.main", "path": "main.tf", "cidr": "10.0.0.0/16"}],
                 "subnets": [{"name": "a", "address": "aws_subnet.a", "path": "modules/net/main.tf", "cidr": None,
                              "vpc": "aws_vpc.main"}],
                 "clusters": [], "routes": [],
                 "unresolved": [{"address": "aws_subnet.a", "path": "modules/net/main.tf", "argument": "cidr_block",
                                 "expression": "cidrsubnet(...)"}]}
        proposal = ranges.propose_target_ranges(space)
        self.assertEqual((proposal["unresolved"], proposal["covered"]), (0, 1))

    def test_covered_entries_are_recognised_in_every_dialect(self):
        space = {"vpcs": [{"name": "c", "address": "ClusterConfig:c.yaml#vpc", "cidr": "10.0.0.0/16", "cluster_vpc": True, "path": "c.yaml"},
                          {"name": "VPC", "address": "VPC", "cidr": "10.30.0.0/16", "cluster_vpc": True, "path": "t.yaml"}],
                 "subnets": [{"name": "a", "address": "ClusterConfig:c.yaml#vpc.subnets.private.a", "cidr": None,
                              "vpc": "ClusterConfig:c.yaml#vpc", "path": "c.yaml"},
                             {"name": "PublicSubnetA", "address": "PublicSubnetA", "cidr": None, "vpc": "VPC", "path": "t.yaml"}],
                 "clusters": [], "routes": [],
                 "unresolved": [{"address": "ClusterConfig:c.yaml", "path": "c.yaml", "argument": "vpc.publicAccessCIDRs", "expression": "${OFFICE_IP}/32"},
                                {"address": "ClusterConfig:c.yaml#vpc.subnets.private.a", "path": "c.yaml", "argument": "cidr", "expression": "${SUBNET_A}"},
                                {"address": "Cluster", "path": "t.yaml", "argument": "ResourcesVpcConfig.PublicAccessCidrs", "expression": "{\"Fn::Split\": ...}"},
                                {"address": "PublicSubnetA", "path": "t.yaml", "argument": "CidrBlock", "expression": "{\"Fn::Select\": ...}"}]}
        proposal = ranges.propose_target_ranges(space)
        self.assertEqual((proposal["unresolved"], proposal["covered"]), (0, 4))

    def test_ipv6_ranges_do_not_constrain_the_proposal(self):
        proposal = ranges.propose_target_ranges(
            {"vpcs": [{"name": "v", "cidr": "2600:1f14::/56", "cluster_vpc": True}]})
        self.assertEqual(proposal["moved"], [])


class DescribeTest(unittest.TestCase):

    def test_lines_for_the_acme_estate(self):
        text = ranges.describe_proposal(ACME)
        # acme-prod sets no service range: said, not avoided.
        self.assertIn("clusters_with_a_defaulted_service_range: acme-prod", text)
        self.assertIn("source_address_space: 10.42.0.0/16 (cluster VPC acme_prod); "
                      "10.20.0.0/16 (routed via vpc_peering_connection)", text)
        self.assertIn("proposed_target_ranges: nodes 10.0.0.0/22, pods 10.4.0.0/16, "
                      "services 10.0.16.0/20", text)
        self.assertIn("moved_off_the_baseline: services", text)
        self.assertNotIn("unresolved_source_ranges", text)

    def test_the_echo_lists_each_question_not_only_the_count(self):
        space = dict(ACME)
        space["unresolved"] = [{"address": "aws_route.peer", "path": "terraform/vpc.tf",
                                "argument": "destination_cidr_block", "expression": "var.peer_cidr (no default)"}]
        text = ranges.describe_proposal(space)
        self.assertIn("unresolved_source_ranges: 1 — ranges the files build from something the scan does not "
                      "follow; ask the user for each of these before the design is final: aws_route.peer "
                      "(terraform/vpc.tf) destination_cidr_block = var.peer_cidr (no default)", text)

    def test_a_template_clusters_vpc_has_to_be_in_its_own_template(self):
        # a.yaml declares VPC; b.yaml names VPC through a Parameter and
        # runs the cluster over it. b's VPC is not recorded, whatever a says.
        space = {"vpcs": [{"name": "VPC", "address": "VPC", "path": "cfn/a.yaml", "cidr": "10.0.0.0/16"}],
                 "subnets": [], "routes": [], "unresolved": [],
                 "clusters": [{"name": "c", "address": "Cluster", "path": "cfn/b.yaml", "vpc": "VPC",
                               "service_ipv4_cidr": "172.20.0.0/16"}]}
        proposal = ranges.propose_target_ranges(space)
        self.assertEqual(proposal["clusters_without_a_vpc"], ["c (vpc: VPC)"])
        # A Terraform cluster may name a VPC declared in another directory.
        space = {"vpcs": [{"name": "this", "address": "module.network.aws_vpc.this", "path": "modules/network/main.tf",
                           "cidr": "10.0.0.0/16"}],
                 "subnets": [], "routes": [], "unresolved": [],
                 "clusters": [{"name": "c", "address": "aws_eks_cluster.c", "path": "envs/prod/eks.tf",
                               "vpc": "module.network.aws_vpc.this", "service_ipv4_cidr": "172.20.0.0/16"}]}
        self.assertEqual(ranges.propose_target_ranges(space)["clusters_without_a_vpc"], [])

    def test_unresolved_ranges_are_a_question(self):
        space = dict(ACME, unresolved=[{"address": "a", "argument": "b", "expression": "c"}])
        self.assertIn("unresolved_source_ranges: 1", ranges.describe_proposal(space))

    def test_empty_section_says_so_and_offers_only_the_baseline(self):
        text = ranges.describe_proposal({})
        self.assertIn("source_address_space: not recorded", text)
        self.assertIn("not checked against the source", text)

    def test_a_vpc_named_without_a_range_is_a_question_not_a_checked_proposal(self):
        # eksctl `vpc: {id: vpc-…}`, or an IPAM-allocated aws_vpc: the section
        # exists, states no range, and the baseline must not be presented as
        # checked.
        space = {"vpcs": [{"name": "outpost", "cidr": None, "secondary_cidrs": [], "cluster_vpc": True}],
                 "subnets": [], "clusters": [], "routes": [], "unresolved": []}
        text = ranges.describe_proposal(space)
        self.assertIn("source_address_space: no range stated", text)
        self.assertIn("(outpost)", text)
        self.assertIn("ask the user for the source ranges", text)
        self.assertIn("not checked against the source", text)

    def test_an_ipv6_cluster_is_not_asked_about_a_defaulted_ipv4_service_range(self):
        space = dict(ACME)
        space["clusters"] = [{"name": "v6", "address": "aws_eks_cluster.v6", "vpc": "aws_vpc.acme_prod",
                              "service_ipv4_cidr": None, "ip_family": "ipv6"},
                             {"name": "v4", "address": "aws_eks_cluster.v4", "vpc": "aws_vpc.acme_prod",
                              "service_ipv4_cidr": None, "ip_family": None}]
        proposal = ranges.propose_target_ranges(space)
        self.assertEqual(proposal["clusters_with_a_defaulted_service_range"], ["v4"])
        text = ranges.describe_proposal(space)
        self.assertIn("usually 10.100.0.0/16 or 172.20.0.0/16, another block for a cluster with remote networks",
                      text)

    def test_a_range_already_asked_for_is_not_also_called_unset(self):
        # The VPC's range and the cluster's service range are variables
        # with no default: questions under `unresolved`, not "unset" values
        # EKS or an IPAM pool chose. Drop the questions and both are named.
        space = {"vpcs": [{"name": "main", "address": "aws_vpc.main", "path": "main.tf", "cidr": None}],
                 "subnets": [], "routes": [],
                 "clusters": [{"name": "c", "address": "aws_eks_cluster.c", "path": "eks.tf",
                               "vpc": "aws_vpc.main", "service_ipv4_cidr": None}],
                 "unresolved": [{"address": "aws_vpc.main", "path": "main.tf", "argument": "cidr_block",
                                 "expression": "var.vpc_cidr (no default)"},
                                {"address": "aws_eks_cluster.c", "path": "eks.tf", "argument": "service_ipv4_cidr",
                                 "expression": "var.svc_cidr (no default)"}]}
        proposal = ranges.propose_target_ranges(space)
        self.assertEqual((proposal["unstated_vpcs"], proposal["clusters_with_a_defaulted_service_range"],
                          proposal["unresolved"]), ([], [], 2))
        text = ranges.describe_proposal(space)
        self.assertNotIn("vpcs_without_a_stated_range", text)
        self.assertNotIn("clusters_with_a_defaulted_service_range", text)
        space["unresolved"] = []
        proposal = ranges.propose_target_ranges(space)
        self.assertEqual((proposal["unstated_vpcs"], proposal["clusters_with_a_defaulted_service_range"]),
                         (["main"], ["c"]))
        # The eksctl and CloudFormation spellings of the service range too.
        for path, argument in (("eksctl/c.yaml", "kubernetesNetworkConfig.serviceIPv4CIDR"),
                               ("cfn/eks.yaml", "KubernetesNetworkConfig.ServiceIpv4Cidr")):
            space = {"vpcs": [], "subnets": [], "routes": [],
                     "clusters": [{"name": "c", "address": "c", "path": path, "vpc": None, "service_ipv4_cidr": None}],
                     "unresolved": [{"address": "c", "path": path, "argument": argument, "expression": "${SVC}"}]}
            self.assertEqual(ranges.propose_target_ranges(space)["clusters_with_a_defaulted_service_range"], [],
                             argument)

    def test_the_no_range_echo_names_a_vpc_whose_range_is_a_question(self):
        space = {"vpcs": [{"name": "main", "address": "aws_vpc.main", "path": "main.tf", "cidr": None}],
                 "subnets": [], "clusters": [], "routes": [],
                 "unresolved": [{"address": "aws_vpc.main", "path": "main.tf", "argument": "cidr_block",
                                 "expression": "var.vpc_cidr (no default)"}]}
        text = ranges.describe_proposal(space)
        self.assertIn("the files name the network (main) without stating its ranges", text)
        self.assertIn("unresolved_source_ranges: 1", text)
        self.assertNotIn("vpcs_without_a_stated_range", text)

    def test_name_only_echo_lists_tell_same_named_entries_apart(self):
        space = {"vpcs": [{"name": "main", "address": "aws_vpc.main", "path": "main.tf", "cidr": "10.0.0.0/16"},
                          {"name": "main", "address": "aws_vpc.main", "path": "modules/vpc/main.tf", "cidr": None}],
                 "subnets": [], "routes": [], "unresolved": [],
                 "clusters": [{"name": "c", "address": "aws_eks_cluster.c", "path": "envs/dev/eks.tf",
                               "vpc": "aws_vpc.main", "service_ipv4_cidr": None},
                              {"name": "c", "address": "aws_eks_cluster.c", "path": "envs/prod/eks.tf",
                               "vpc": "aws_vpc.main", "service_ipv4_cidr": None}]}
        proposal = ranges.propose_target_ranges(space)
        self.assertEqual(proposal["unstated_vpcs"], ["main in modules/vpc"])
        self.assertEqual(proposal["clusters_with_a_defaulted_service_range"], ["c in envs/dev", "c in envs/prod"])

    def test_a_cluster_whose_vpc_is_not_recorded_is_flagged(self):
        # `vpc_id = data.aws_vpc.selected.id`: the estate's only stated range
        # is a peering route, and the cluster's own VPC is unknown.
        space = {"vpcs": [], "subnets": [],
                 "clusters": [{"name": "c", "vpc": "data.aws_vpc.selected", "service_ipv4_cidr": None,
                               "remote_node_cidrs": [], "remote_pod_cidrs": []}],
                 "routes": [{"destination": "10.20.0.0/16", "via": "vpc_peering_connection"}],
                 "unresolved": []}
        text = ranges.describe_proposal(space)
        self.assertIn("clusters_without_a_recorded_vpc: c (vpc: data.aws_vpc.selected)", text)
        self.assertIn("proposed_target_ranges:", text)

    def test_an_unstated_vpc_beside_stated_ranges_is_flagged(self):
        space = dict(ACME)
        space["vpcs"] = ACME["vpcs"] + [{"name": "ipam_vpc", "cidr": None, "secondary_cidrs": [],
                                         "cluster_vpc": False}]
        text = ranges.describe_proposal(space)
        self.assertIn("proposed_target_ranges: nodes 10.0.0.0/22", text)
        self.assertIn("vpcs_without_a_stated_range: ipam_vpc", text)


if __name__ == "__main__":
    unittest.main()
