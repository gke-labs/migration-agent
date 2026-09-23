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

"""Unit tests for the address-space harvest. No GCS, no LLM.

The harvest states only what the files state; these tests hold it to that.
Every recorded CIDR is traced to a literal, a default or a `locals` value,
and everything else is an `unresolved` entry with its expression.
"""

import os
import tempfile
import unittest

from servers.phases.discovery.discovery_init_1 import addressspace


def _write(root, rel_path, content):
    full = os.path.join(root, rel_path)
    os.makedirs(os.path.dirname(full) or full, exist_ok=True)
    with open(full, "w") as f:
        f.write(content)


def _harvest(files: dict, scope: dict = None, chart_roots=()) -> tuple:
    with tempfile.TemporaryDirectory() as root:
        for rel_path, content in files.items():
            _write(root, rel_path, content)
        result = addressspace.harvest_address_space(root, scope, chart_roots)
        return result.section, result.notes


def _section(files: dict, **kw) -> dict:
    return _harvest(files, **kw)[0]


# The acme estate's shape: a VPC resource, two tagged private subnets, a
# cluster that names them, and a peering route.
ACME_VPC_TF = (
    'resource "aws_vpc" "acme_prod" {\n'
    '  cidr_block           = "10.42.0.0/16"\n'
    '  enable_dns_hostnames = true\n'
    '  tags = { Name = "acme-prod-vpc" }\n'
    '}\n'
    'resource "aws_subnet" "private_a" {\n'
    '  vpc_id            = aws_vpc.acme_prod.id\n'
    '  cidr_block        = "10.42.0.0/19"\n'
    '  availability_zone = "us-east-1a"\n'
    '  tags = {\n'
    '    "kubernetes.io/role/internal-elb" = "1"\n'
    '  }\n'
    '}\n'
    'resource "aws_subnet" "public_a" {\n'
    '  vpc_id                  = aws_vpc.acme_prod.id\n'
    '  cidr_block              = "10.42.64.0/19"\n'
    '  availability_zone       = "us-east-1a"\n'
    '  map_public_ip_on_launch = true\n'
    '}\n'
    'resource "aws_route" "to_shared_services" {\n'
    '  route_table_id            = aws_vpc.acme_prod.main_route_table_id\n'
    '  destination_cidr_block    = "10.20.0.0/16"\n'
    '  vpc_peering_connection_id = aws_vpc_peering_connection.shared.id\n'
    '}\n'
    'resource "aws_route" "default" {\n'
    '  route_table_id         = aws_vpc.acme_prod.main_route_table_id\n'
    '  destination_cidr_block = "0.0.0.0/0"\n'
    '  gateway_id             = aws_internet_gateway.igw.id\n'
    '}\n')

ACME_EKS_TF = (
    'resource "aws_eks_cluster" "acme_prod" {\n'
    '  name     = "acme-prod"\n'
    '  role_arn = aws_iam_role.eks.arn\n'
    '  vpc_config {\n'
    '    subnet_ids              = [aws_subnet.private_a.id, aws_subnet.public_a.id]\n'
    '    endpoint_public_access  = true\n'
    '    public_access_cidrs     = ["203.0.113.0/24"]\n'
    '  }\n'
    '  kubernetes_network_config {\n'
    '    service_ipv4_cidr = "172.20.0.0/16"\n'
    '    ip_family         = "ipv4"\n'
    '  }\n'
    '  remote_network_config {\n'
    '    remote_node_networks {\n'
    '      cidrs = ["10.52.0.0/16"]\n'
    '    }\n'
    '    remote_pod_networks {\n'
    '      cidrs = ["10.53.0.0/16"]\n'
    '    }\n'
    '  }\n'
    '}\n')


class TerraformResourceTest(unittest.TestCase):

    def test_vpc_subnets_cluster_and_route_from_literals(self):
        section = _section({"terraform/vpc.tf": ACME_VPC_TF, "terraform/eks.tf": ACME_EKS_TF})
        vpc, = section["vpcs"]
        self.assertEqual(vpc["cidr"], "10.42.0.0/16")
        self.assertEqual(vpc["address"], "aws_vpc.acme_prod")
        self.assertEqual(vpc["form"], "resource")
        self.assertTrue(vpc["cluster_vpc"])
        self.assertEqual(vpc["evidence"], ["terraform/vpc.tf:1: cidr_block = 10.42.0.0/16"])

        private, public = section["subnets"]
        self.assertEqual((private["cidr"], private["tier"], private["availability_zone"], private["vpc"]),
                         ("10.42.0.0/19", "private", "us-east-1a", "aws_vpc.acme_prod"))
        self.assertEqual((public["cidr"], public["tier"]), ("10.42.64.0/19", "public"))

        cluster, = section["clusters"]
        self.assertEqual(cluster["name"], "acme-prod")
        self.assertEqual(cluster["vpc"], "aws_vpc.acme_prod")
        self.assertEqual(cluster["subnets"], ["aws_subnet.private_a", "aws_subnet.public_a"])
        self.assertEqual(cluster["service_ipv4_cidr"], "172.20.0.0/16")
        self.assertEqual(cluster["ip_family"], "ipv4")
        self.assertEqual(cluster["public_access_cidrs"], ["203.0.113.0/24"])
        self.assertEqual(cluster["remote_node_cidrs"], ["10.52.0.0/16"])
        self.assertEqual(cluster["remote_pod_cidrs"], ["10.53.0.0/16"])

        # The peering route is recorded; the default route is not a range
        # the design has to keep clear of.
        route, = section["routes"]
        self.assertEqual((route["destination"], route["via"]), ("10.20.0.0/16", "vpc_peering_connection"))
        self.assertEqual(section["unresolved"], [])

    def test_cluster_vpc_is_unknown_without_a_cluster(self):
        section = _section({"vpc.tf": ACME_VPC_TF})
        self.assertIsNone(section["vpcs"][0]["cluster_vpc"])

    def test_cluster_vpc_false_when_a_cluster_names_another_vpc(self):
        tf = (ACME_VPC_TF
              + 'resource "aws_vpc" "other" {\n  cidr_block = "10.99.0.0/16"\n}\n'
              + 'resource "aws_eks_cluster" "c" {\n  name = "c"\n'
                '  vpc_config {\n    subnet_ids = [aws_subnet.private_a.id]\n  }\n}\n')
        section = _section({"main.tf": tf})
        by_name = {v["name"]: v["cluster_vpc"] for v in section["vpcs"]}
        self.assertEqual(by_name, {"acme_prod": True, "other": False})

    def test_secondary_cidr_association_attaches_to_its_vpc(self):
        tf = ('resource "aws_vpc" "main" {\n  cidr_block = "10.0.0.0/16"\n}\n'
              'resource "aws_vpc_ipv4_cidr_block_association" "pods" {\n'
              '  vpc_id     = aws_vpc.main.id\n'
              '  cidr_block = "100.64.0.0/16"\n}\n')
        vpc, = _section({"main.tf": tf})["vpcs"]
        self.assertEqual(vpc["secondary_cidrs"], ["100.64.0.0/16"])
        self.assertIn("main.tf:4: aws_vpc_ipv4_cidr_block_association.pods cidr_block = 100.64.0.0/16",
                      vpc["evidence"])

    def test_secondary_association_in_an_earlier_file_still_attaches(self):
        # The walk reads a directory's files in name order; "a.tf" sorts
        # before "vpc.tf", so the association is read before the VPC it names.
        files = {"a.tf": ('resource "aws_vpc_ipv4_cidr_block_association" "pods" {\n'
                          '  vpc_id     = aws_vpc.main.id\n'
                          '  cidr_block = "100.64.0.0/16"\n}\n'),
                 "vpc.tf": 'resource "aws_vpc" "main" {\n  cidr_block = "10.0.0.0/16"\n}\n'}
        vpc, = _section(files)["vpcs"]
        self.assertEqual((vpc["cidr"], vpc["secondary_cidrs"]), ("10.0.0.0/16", ["100.64.0.0/16"]))

    def test_a_terraform_vpc_stays_unknown_when_only_an_eksctl_cluster_joins_by_id(self):
        files = {"terraform/vpc.tf": 'resource "aws_vpc" "main" {\n  cidr_block = "10.42.0.0/16"\n}\n',
                 "eksctl/cluster.yaml": ("apiVersion: eksctl.io/v1alpha5\nkind: ClusterConfig\n"
                                         "metadata:\n  name: c\nvpc:\n  id: vpc-0abc\n")}
        section = _section(files)
        by_form = {v["form"]: v["cluster_vpc"] for v in section["vpcs"]}
        self.assertEqual(by_form, {"resource": None, "eksctl": True})

    def test_two_associations_on_one_undeclared_vpc_through_a_module_share_a_placeholder(self):
        files = {"modules/net/main.tf": ('resource "aws_vpc_ipv4_cidr_block_association" "a" {\n'
                                         '  vpc_id = var.vpc_id\n  cidr_block = "100.64.0.0/16"\n}\n'
                                         'resource "aws_vpc_ipv4_cidr_block_association" "b" {\n'
                                         '  vpc_id = var.vpc_id\n  cidr_block = "100.65.0.0/16"\n}\n'),
                 "envs/prod/main.tf": ('module "net" {\n  source = "../../modules/net"\n'
                                       '  vpc_id = data.aws_vpc.selected.id\n}\n')}
        vpc, = _section(files)["vpcs"]
        self.assertEqual((vpc["address"], vpc["secondary_cidrs"]),
                         ("data.aws_vpc.selected", ["100.64.0.0/16", "100.65.0.0/16"]))

    def test_associations_on_two_unreadable_vpcs_stay_apart(self):
        tf = ('resource "aws_vpc_ipv4_cidr_block_association" "a" {\n'
              '  vpc_id = var.vpc_a_id\n  cidr_block = "100.64.0.0/16"\n}\n'
              'resource "aws_vpc_ipv4_cidr_block_association" "b" {\n'
              '  vpc_id = var.vpc_b_id\n  cidr_block = "100.65.0.0/16"\n}\n')
        section = _section({"main.tf": tf})
        self.assertEqual({v["address"]: v["secondary_cidrs"] for v in section["vpcs"]},
                         {"var.vpc_a_id": ["100.64.0.0/16"], "var.vpc_b_id": ["100.65.0.0/16"]})

    def test_secondary_cidr_on_an_undeclared_vpc_records_a_placeholder(self):
        tf = ('resource "aws_vpc_ipv4_cidr_block_association" "pods" {\n'
              '  vpc_id     = data.aws_vpc.selected.id\n'
              '  cidr_block = "100.64.0.0/16"\n}\n')
        vpc, = _section({"main.tf": tf})["vpcs"]
        self.assertEqual((vpc["address"], vpc["cidr"], vpc["secondary_cidrs"]),
                         ("data.aws_vpc.selected", None, ["100.64.0.0/16"]))

    def test_an_unstated_secondary_range_is_a_question_under_its_vpc_and_uncovers_its_subnets(self):
        # EKS custom networking: the pod range is a secondary block, the pod
        # subnets are cut from it. From an IPAM pool or from a variable with
        # no default alike, the files do not state it: a question filed
        # under the VPC, and the counted subnets are questions too, not
        # "inside a stated VPC range".
        for label, association in (
                ("ipam", '  ipv4_ipam_pool_id   = "ipam-pool-1"\n  ipv4_netmask_length = 16\n'),
                ("variable", '  cidr_block = var.pods\n')):
            with self.subTest(label):
                tf = ('variable "pods" {}\n'
                      'resource "aws_vpc" "main" {\n  cidr_block = "10.0.0.0/16"\n}\n'
                      'resource "aws_vpc_ipv4_cidr_block_association" "pods" {\n  vpc_id = aws_vpc.main.id\n'
                      + association + '}\n'
                      'resource "aws_subnet" "pods" {\n  count = 2\n  vpc_id = aws_vpc.main.id\n'
                      '  cidr_block = cidrsubnet(aws_vpc_ipv4_cidr_block_association.pods.cidr_block, 4, count.index)\n}\n')
                section = _section({"main.tf": tf})
                self.assertEqual(section["vpcs"][0]["secondary_cidrs"], [])
                self.assertEqual([(e["address"], e["argument"]) for e in section["unresolved"]],
                                 [("aws_vpc.main", "secondary_cidr_blocks"), ("aws_subnet.pods", "cidr_block")])
                if label == "ipam":
                    self.assertEqual(section["unresolved"][0]["expression"],
                                     "aws_vpc_ipv4_cidr_block_association.pods: allocated from an IPAM pool "
                                     "(ipv4_ipam_pool_id), so the files do not state it")
                blocking, covered = addressspace.triage_unresolved(section)
                self.assertEqual((len(blocking), covered), (2, []))

    def test_an_unstated_secondary_list_on_a_local_vpc_instance_is_filed_under_the_inner_vpc(self):
        # The instance stands for aws_vpc.main inside modules/vpc and passes
        # a secondary list nobody states. The question belongs to the inner
        # entry, so that VPC is not "stated" and the pod subnets cut from
        # its range are questions, not covered.
        files = {"modules/vpc/main.tf": 'variable "cidr" {}\nresource "aws_vpc" "main" {\n  cidr_block = var.cidr\n}\n',
                 "main.tf": ('variable "extra" {}\n'
                             'module "vpc" {\n  source = "./modules/vpc"\n  cidr = "10.0.0.0/16"\n'
                             '  secondary_cidr_blocks = var.extra\n}\n'
                             'resource "aws_subnet" "pods" {\n  count = 2\n  vpc_id = module.vpc.vpc_id\n'
                             '  cidr_block = cidrsubnet(module.vpc.vpc_cidr_block, 4, count.index)\n}\n')}
        section = _section(files)
        self.assertEqual([(e["address"], e["path"], e["argument"]) for e in section["unresolved"]],
                         [("aws_vpc.main", "modules/vpc/main.tf", "secondary_cidr_blocks"),
                          ("aws_subnet.pods", "main.tf", "cidr_block")])
        blocking, covered = addressspace.triage_unresolved(section)
        self.assertEqual((len(blocking), covered), (2, []))

    def test_an_unstated_secondary_on_a_vpc_in_another_directory_is_filed_under_that_vpc(self):
        # The association sits at the root, the VPC inside modules/network.
        # The question is filed under the VPC's own path, so that VPC is
        # not "stated" and the pod subnets cut from the unstated range are
        # questions, not covered.
        files = {"modules/network/main.tf": 'variable "cidr" {}\nresource "aws_vpc" "this" {\n  cidr_block = var.cidr\n}\n',
                 "main.tf": ('variable "pod_cidr" {}\n'
                             'module "network" {\n  source = "./modules/network"\n  cidr = "10.0.0.0/16"\n}\n'
                             'resource "aws_vpc_ipv4_cidr_block_association" "pods" {\n'
                             '  vpc_id = module.network.vpc_id\n  cidr_block = var.pod_cidr\n}\n'
                             'resource "aws_subnet" "pods" {\n  count = 2\n  vpc_id = module.network.vpc_id\n'
                             '  cidr_block = cidrsubnet(var.pod_cidr, 4, count.index)\n}\n')}
        section = _section(files)
        self.assertEqual([(e["address"], e["path"], e["argument"]) for e in section["unresolved"]],
                         [("aws_vpc.this", "modules/network/main.tf", "secondary_cidr_blocks"),
                          ("aws_subnet.pods", "main.tf", "cidr_block")])
        blocking, covered = addressspace.triage_unresolved(section)
        self.assertEqual((len(blocking), covered), (2, []))

    def test_ipam_allocated_vpc_is_evidence_not_a_guess(self):
        tf = ('resource "aws_vpc" "main" {\n'
              '  ipv4_ipam_pool_id   = aws_vpc_ipam_pool.p.id\n'
              '  ipv4_netmask_length = 16\n}\n')
        section = _section({"main.tf": tf})
        vpc, = section["vpcs"]
        self.assertIsNone(vpc["cidr"])
        self.assertIn("IPAM pool", vpc["evidence"][0])
        self.assertEqual(section["unresolved"], [])

    def test_route_table_inline_routes(self):
        tf = ('resource "aws_route_table" "private" {\n'
              '  vpc_id = aws_vpc.main.id\n'
              '  route {\n'
              '    cidr_block         = "10.200.0.0/16"\n'
              '    transit_gateway_id = "tgw-0123"\n'
              '  }\n'
              '  route {\n'
              '    cidr_block     = "0.0.0.0/0"\n'
              '    nat_gateway_id = aws_nat_gateway.n.id\n'
              '  }\n'
              '}\n')
        route, = _section({"main.tf": tf})["routes"]
        self.assertEqual((route["destination"], route["via"], route["address"]),
                         ("10.200.0.0/16", "transit_gateway", "aws_route_table.private.route[0]"))

    def test_dynamic_route_blocks_are_a_question(self):
        tf = ('resource "aws_route_table" "private" {\n'
              '  vpc_id = aws_vpc.main.id\n'
              '  dynamic "route" {\n'
              '    for_each = var.peering_routes\n'
              '    content {\n'
              '      cidr_block                = route.value.cidr\n'
              '      vpc_peering_connection_id = route.value.pcx\n'
              '    }\n'
              '  }\n'
              '}\n'
              'resource "aws_eks_cluster" "c" {\n  name = "c"\n'
              '  remote_network_config {\n'
              '    dynamic "remote_node_networks" {\n'
              '      for_each = var.remote\n'
              '      content {\n        cidrs = remote_node_networks.value\n      }\n'
              '    }\n'
              '  }\n'
              '}\n')
        section = _section({"main.tf": tf})
        self.assertEqual(section["routes"], [])
        self.assertCountEqual([(u["argument"], u["expression"]) for u in section["unresolved"]],
                              [("remote_node_networks.cidrs", "var.remote (no default)"),
                               ("dynamic route for_each", "var.peering_routes (no default)")])

    def test_default_route_table_inline_routes(self):
        tf = ('resource "aws_default_route_table" "main" {\n'
              '  default_route_table_id = aws_vpc.main.default_route_table_id\n'
              '  route {\n'
              '    cidr_block                = "10.20.0.0/16"\n'
              '    vpc_peering_connection_id = "pcx-1"\n'
              '  }\n'
              '}\n')
        route, = _section({"main.tf": tf})["routes"]
        self.assertEqual((route["destination"], route["via"], route["address"]),
                         ("10.20.0.0/16", "vpc_peering_connection", "aws_default_route_table.main.route[0]"))

    def test_a_for_each_route_names_what_it_iterates_over(self):
        tf = ('resource "aws_route" "peer" {\n'
              '  for_each                  = toset(var.peer_cidrs)\n'
              '  destination_cidr_block    = each.value\n'
              '  vpc_peering_connection_id = "pcx-1"\n'
              '}\n')
        entry, = _section({"main.tf": tf})["unresolved"]
        self.assertEqual(entry["expression"], "each.value over for_each/count = toset(var.peer_cidrs)")

    def test_a_for_each_vpc_names_what_it_iterates_over(self):
        tf = ('resource "aws_vpc" "this" {\n  for_each   = var.vpcs\n  cidr_block = each.value.cidr\n}\n')
        section = _section({"main.tf": tf})
        self.assertEqual(section["unresolved"][0]["expression"],
                         "each.value.cidr over for_each/count = var.vpcs (no default)")

    def test_routes_through_an_instance_a_vpn_connection_and_a_transit_gateway_table(self):
        tf = ('resource "aws_route" "appliance" {\n  route_table_id = "rtb-1"\n'
              '  destination_cidr_block = "192.168.0.0/16"\n  instance_id = "i-0abc"\n}\n'
              'resource "aws_vpn_connection_route" "office" {\n'
              '  destination_cidr_block = "10.100.0.0/16"\n  vpn_connection_id = "vpn-1"\n}\n'
              'resource "aws_ec2_transit_gateway_route" "dc" {\n'
              '  destination_cidr_block         = "10.200.0.0/16"\n'
              '  transit_gateway_attachment_id  = "tgw-attach-1"\n'
              '  transit_gateway_route_table_id = "tgw-rtb-1"\n}\n')
        section = _section({"main.tf": tf})
        self.assertEqual([(r["destination"], r["via"]) for r in section["routes"]],
                         [("192.168.0.0/16", "instance"), ("10.100.0.0/16", "vpn_connection"),
                          ("10.200.0.0/16", "transit_gateway_attachment")])

    def test_a_split_default_route_pair_is_not_a_source_range(self):
        # A full-tunnel VPN pushes 0.0.0.0/1 and 128.0.0.0/1: where
        # everything else goes, not ranges the source uses. Recorded, they
        # would leave no candidate block free. The office range beside them
        # is a range.
        # The edge: a transit gateway summary route to 10.0.0.0/8 is the
        # widest range an estate routes and is kept; one bit wider is not.
        tf = ('resource "aws_route" "low" {\n  route_table_id = "rtb-1"\n'
              '  destination_cidr_block = "0.0.0.0/1"\n  vpn_gateway_id = "vgw-1"\n}\n'
              'resource "aws_route" "high" {\n  route_table_id = "rtb-1"\n'
              '  destination_cidr_block = "128.0.0.0/1"\n  vpn_gateway_id = "vgw-1"\n}\n'
              'resource "aws_route" "wide" {\n  route_table_id = "rtb-1"\n'
              '  destination_cidr_block = "10.0.0.0/7"\n  transit_gateway_id = "tgw-1"\n}\n'
              'resource "aws_route" "office" {\n  route_table_id = "rtb-1"\n'
              '  destination_cidr_block = "10.100.0.0/16"\n  vpn_gateway_id = "vgw-1"\n}\n'
              'resource "aws_route" "summary" {\n  route_table_id = "rtb-1"\n'
              '  destination_cidr_block = "10.0.0.0/8"\n  transit_gateway_id = "tgw-1"\n}\n')
        section = _section({"main.tf": tf})
        self.assertEqual([r["destination"] for r in section["routes"]], ["10.100.0.0/16", "10.0.0.0/8"])
        self.assertEqual(section["unresolved"], [])

    def test_a_bare_address_is_not_a_range(self):
        section = _section({"main.tf": 'resource "aws_vpc" "main" {\n  cidr_block = "10.0.0.1"\n}\n'})
        self.assertIsNone(section["vpcs"][0]["cidr"])
        self.assertEqual([(e["argument"], e["expression"]) for e in section["unresolved"]],
                         [("cidr_block", '"10.0.0.1"')])

    def test_a_route_to_an_odb_network_is_read(self):
        tf = ('resource "aws_route" "odb" {\n  route_table_id = "rtb-1"\n'
              '  destination_cidr_block = "10.60.0.0/16"\n  odb_network_arn = "arn:aws:odb:us-east-1:1:odb-network/x"\n}\n')
        section = _section({"main.tf": tf})
        self.assertEqual([(r["destination"], r["via"]) for r in section["routes"]], [("10.60.0.0/16", "odb_network_arn")])

    def test_a_route_table_in_attribute_form_is_a_question(self):
        # `route = [ { ... } ]` is valid Terraform the scan does not walk;
        # the table is a question, not a table with no routes. `route = []`
        # clears the table and asks nothing.
        tf = ('resource "aws_route_table" "private" {\n  vpc_id = "vpc-1"\n'
              '  route = [\n    {\n      cidr_block = "10.50.0.0/16"\n'
              '      vpc_peering_connection_id = "pcx-1"\n    },\n  ]\n}\n'
              'resource "aws_route_table" "empty" {\n  vpc_id = "vpc-1"\n  route = []\n}\n')
        section = _section({"main.tf": tf})
        self.assertEqual(section["routes"], [])
        self.assertEqual([(e["address"], e["argument"]) for e in section["unresolved"]],
                         [("aws_route_table.private.route", "route (attribute form)")])

    def test_a_prefix_list_route_is_a_question(self):
        tf = ('resource "aws_route" "onprem" {\n'
              '  route_table_id             = aws_route_table.private.id\n'
              '  destination_prefix_list_id = aws_ec2_managed_prefix_list.onprem.id\n'
              '  transit_gateway_id         = "tgw-1"\n}\n')
        section = _section({"main.tf": tf})
        self.assertEqual(section["routes"], [])
        self.assertEqual([(u["argument"], u["expression"]) for u in section["unresolved"]],
                         [("destination_prefix_list_id", "aws_ec2_managed_prefix_list.onprem.id")])

    def test_a_cidr_in_a_comment_is_not_read(self):
        tf = ('resource "aws_vpc" "main" {\n'
              '  # cidr_block = "10.1.0.0/16"\n'
              '  cidr_block = "10.2.0.0/16" // was 10.3.0.0/16\n'
              '}\n')
        vpc, = _section({"main.tf": tf})["vpcs"]
        self.assertEqual(vpc["cidr"], "10.2.0.0/16")


class TerraformResolutionTest(unittest.TestCase):
    """`var.` and `local.` chains within a directory, and nothing beyond."""

    WORKSHOP_VARS = ('variable "vpc_cidr" {\n'
                     '  description = "VPC CIDR"\n'
                     '  type        = string\n'
                     '  default     = "10.42.0.0/16"\n'
                     '}\n'
                     'variable "remote_network_cidr" {\n'
                     '  default = "10.52.0.0/16"\n'
                     '}\n'
                     'variable "cluster_name" {\n'
                     '  type = string\n'
                     '}\n')
    WORKSHOP_VPC = ('locals {\n'
                    '  private_subnets = [for k, v in local.azs : cidrsubnet(var.vpc_cidr, 3, k + 3)]\n'
                    '  azs             = slice(data.aws_availability_zones.available.names, 0, 3)\n'
                    '}\n'
                    'module "vpc" {\n'
                    '  source  = "terraform-aws-modules/vpc/aws"\n'
                    '  version = "~> 6.0"\n'
                    '  name    = var.cluster_name\n'
                    '  cidr    = var.vpc_cidr\n'
                    '  azs             = local.azs\n'
                    '  private_subnets = local.private_subnets\n'
                    '  enable_nat_gateway = true\n'
                    '}\n')
    WORKSHOP_EKS = ('locals {\n'
                    '  remote_node_cidr = var.remote_network_cidr\n'
                    '}\n'
                    'module "eks" {\n'
                    '  source  = "terraform-aws-modules/eks/aws"\n'
                    '  version = "~> 21.0"\n'
                    '  name               = var.cluster_name\n'
                    '  kubernetes_version = "1.33"\n'
                    '  vpc_id     = module.vpc.vpc_id\n'
                    '  subnet_ids = module.vpc.private_subnets\n'
                    '  remote_network_config = {\n'
                    '    remote_node_networks = {\n'
                    '      cidrs = [local.remote_node_cidr]\n'
                    '    }\n'
                    '  }\n'
                    '}\n')

    def test_module_vpc_through_a_variable_default(self):
        section = _section({"tf/variables.tf": self.WORKSHOP_VARS, "tf/vpc.tf": self.WORKSHOP_VPC,
                            "tf/eks.tf": self.WORKSHOP_EKS})
        vpc, = section["vpcs"]
        self.assertEqual((vpc["address"], vpc["form"], vpc["cidr"]), ("module.vpc", "module", "10.42.0.0/16"))
        self.assertTrue(vpc["cluster_vpc"])
        cluster, = section["clusters"]
        self.assertEqual((cluster["address"], cluster["vpc"], cluster["subnets"]),
                         ("module.eks", "module.vpc", ["module.vpc.private_subnets"]))
        self.assertEqual(cluster["remote_node_cidrs"], ["10.52.0.0/16"])
        # The name is a variable with no default: null, not the expression.
        self.assertIsNone(cluster["name"])

    def test_for_expression_is_unresolved_and_spelled_out(self):
        section = _section({"tf/variables.tf": self.WORKSHOP_VARS, "tf/vpc.tf": self.WORKSHOP_VPC})
        entry, = section["unresolved"]
        self.assertEqual((entry["address"], entry["argument"]), ("module.vpc", "private_subnets"))
        self.assertEqual(entry["expression"],
                         "local.private_subnets = [for k, v in local.azs : cidrsubnet(var.vpc_cidr, 3, k + 3)]")
        self.assertEqual(section["subnets"], [])

    def test_variable_without_a_default_is_unresolved(self):
        tf = ('variable "cidr" {\n  type = string\n}\n'
              'resource "aws_vpc" "main" {\n  cidr_block = var.cidr\n}\n')
        section = _section({"main.tf": tf})
        self.assertIsNone(section["vpcs"][0]["cidr"])
        self.assertEqual(section["unresolved"][0]["expression"], "var.cidr (no default)")

    def test_evidence_says_where_a_value_the_line_does_not_state_came_from(self):
        # A literal is shown as itself. A default, an auto-loaded .tfvars
        # value (with the default it overrode), a local and an expression
        # are named, so a reviewer can tell them from a literal that CI
        # cannot override with -var.
        files = {"variables.tf": ('variable "vpc_cidr" {\n  default = "10.0.0.0/16"\n}\n'
                                  'variable "peer" {\n  default = "10.9.0.0/16"\n}\n'),
                 "terraform.tfvars": 'peer = "10.8.0.0/16"\n',
                 "main.tf": ('locals {\n  pods = cidrsubnet(var.vpc_cidr, 4, 15)\n}\n'
                             'resource "aws_vpc" "main" {\n  cidr_block = var.vpc_cidr\n}\n'
                             'resource "aws_subnet" "pods" {\n  vpc_id = aws_vpc.main.id\n  cidr_block = local.pods\n}\n'
                             'resource "aws_subnet" "lit" {\n  vpc_id = aws_vpc.main.id\n  cidr_block = "10.0.1.0/24"\n}\n'
                             'resource "aws_subnet" "cut" {\n  vpc_id = aws_vpc.main.id\n'
                             '  cidr_block = cidrsubnet(aws_vpc.main.cidr_block, 8, 2)\n}\n'
                             'resource "aws_route" "peer" {\n  destination_cidr_block = var.peer\n'
                             '  vpc_peering_connection_id = "pcx-1"\n}\n')}
        section = _section(files)
        ends = {v["name"]: v["evidence"][0].split(": ", 1)[1] for v in section["vpcs"]}
        ends.update({s["name"]: s["evidence"][0].split(": ", 1)[1] for s in section["subnets"]})
        ends["route"] = section["routes"][0]["evidence"][0].split(": ", 1)[1]
        self.assertEqual(ends, {
            "main": "cidr_block = 10.0.0.0/16 (var.vpc_cidr, the variable's default)",
            "pods": "cidr_block = 10.0.240.0/20 (local.pods, a local)",
            "lit": "cidr_block = 10.0.1.0/24",
            "cut": "cidr_block = 10.0.2.0/24 (cidrsubnet(aws_vpc.main.cidr_block, 8, 2), an expression over stated values)",
            "route": "destination 10.8.0.0/16 (var.peer, from an auto-loaded .tfvars file, over the default "
                     "\"10.9.0.0/16\") via vpc_peering_connection",
        })

    def test_a_locals_cycle_is_a_question_not_a_crash(self):
        tf = ('locals {\n  a = local.b\n  b = local.a\n}\n'
              'resource "aws_vpc" "main" {\n  cidr_block = local.a\n}\n')
        section = _section({"main.tf": tf})
        self.assertIsNone(section["vpcs"][0]["cidr"])
        self.assertTrue(section["unresolved"][0]["expression"].startswith("local.a = local.b = local.a"),
                        section["unresolved"][0]["expression"])

    def test_variables_do_not_cross_directories(self):
        files = {"prod/variables.tf": 'variable "cidr" {\n  default = "10.1.0.0/16"\n}\n',
                 "prod/vpc.tf": 'resource "aws_vpc" "main" {\n  cidr_block = var.cidr\n}\n',
                 "dev/vpc.tf": 'resource "aws_vpc" "main" {\n  cidr_block = var.cidr\n}\n'}
        section = _section(files)
        by_path = {v["path"]: v["cidr"] for v in section["vpcs"]}
        self.assertEqual(by_path, {"prod/vpc.tf": "10.1.0.0/16", "dev/vpc.tf": None})
        self.assertEqual(section["unresolved"][0]["path"], "dev/vpc.tf")

    def test_a_declared_vpcs_own_range_is_followed_through_its_attribute(self):
        # The subnet and the route sort before the VPC file: VPCs finish
        # first whatever the walk order, so the attribute is there to read.
        files = {"a.tf": ('resource "aws_subnet" "a" {\n  vpc_id = aws_vpc.main.id\n'
                          '  cidr_block = cidrsubnet(aws_vpc.main.cidr_block, 4, 1)\n}\n'
                          'resource "aws_route" "peer" {\n'
                          '  destination_cidr_block    = aws_vpc.peer.cidr_block\n'
                          '  vpc_peering_connection_id = "pcx-1"\n}\n'),
                 "vpc.tf": ('resource "aws_vpc" "main" {\n  cidr_block = "10.0.0.0/16"\n}\n'
                            'resource "aws_vpc" "peer" {\n  cidr_block = "10.9.0.0/16"\n}\n')}
        section = _section(files)
        self.assertEqual(section["subnets"][0]["cidr"], "10.0.16.0/20")
        self.assertEqual([(r["destination"], r["via"]) for r in section["routes"]],
                         [("10.9.0.0/16", "vpc_peering_connection")])
        self.assertEqual(section["unresolved"], [])

    def test_the_community_modules_range_is_followed_through_its_output(self):
        tf = ('module "vpc" {\n  source = "terraform-aws-modules/vpc/aws"\n  cidr = "10.0.0.0/16"\n}\n'
              'resource "aws_subnet" "extra" {\n  vpc_id = module.vpc.vpc_id\n'
              '  cidr_block = cidrsubnet(module.vpc.vpc_cidr_block, 8, 200)\n}\n')
        section = _section({"main.tf": tf})
        self.assertEqual(section["subnets"][0]["cidr"], "10.0.200.0/24")
        self.assertEqual(section["unresolved"], [])

    def test_other_attributes_data_sources_and_other_directories_stay_unresolved(self):
        # The rule is "a declared VPC's own primary range, in this
        # directory": a data source, a secondary range's attribute and a
        # VPC declared elsewhere are each still a question.
        files = {"main.tf": ('resource "aws_vpc" "main" {\n  cidr_block = "10.0.0.0/16"\n}\n'
                             'resource "aws_vpc_ipv4_cidr_block_association" "pods" {\n'
                             '  vpc_id = aws_vpc.main.id\n  cidr_block = "100.64.0.0/16"\n}\n'
                             'resource "aws_subnet" "a" {\n  vpc_id = data.aws_vpc.sel.id\n'
                             '  cidr_block = cidrsubnet(data.aws_vpc.sel.cidr_block, 4, 1)\n}\n'
                             'resource "aws_route" "pods" {\n'
                             '  destination_cidr_block    = aws_vpc_ipv4_cidr_block_association.pods.cidr_block\n'
                             '  vpc_peering_connection_id = "pcx-1"\n}\n'),
                 "dev/main.tf": ('resource "aws_subnet" "b" {\n  vpc_id = aws_vpc.main.id\n'
                                 '  cidr_block = cidrsubnet(aws_vpc.main.cidr_block, 4, 1)\n}\n')}
        section = _section(files)
        self.assertEqual([s["cidr"] for s in section["subnets"]], [None, None])
        self.assertEqual(section["routes"], [])
        self.assertEqual([(e["path"], e["expression"]) for e in section["unresolved"]],
                         [("main.tf", "cidrsubnet(data.aws_vpc.sel.cidr_block, 4, 1)"),
                          ("main.tf", "aws_vpc_ipv4_cidr_block_association.pods.cidr_block"),
                          ("dev/main.tf", "cidrsubnet(aws_vpc.main.cidr_block, 4, 1)")])

    def test_a_per_instance_vpc_range_does_not_leak_between_instances(self):
        # The same module, two instances, two ranges: a subnet inside it
        # reads its own copy's VPC range, not the other's.
        files = {"modules/network/main.tf": ('resource "aws_vpc" "this" {\n  cidr_block = var.vpc_cidr\n}\n'
                                             'resource "aws_subnet" "a" {\n  vpc_id = aws_vpc.this.id\n'
                                             '  cidr_block = cidrsubnet(aws_vpc.this.cidr_block, 4, 1)\n}\n'),
                 "envs/dev/main.tf": 'module "network" {\n  source = "../../modules/network"\n  vpc_cidr = "10.1.0.0/16"\n}\n',
                 "envs/prod/main.tf": 'module "network" {\n  source = "../../modules/network"\n  vpc_cidr = "10.2.0.0/16"\n}\n'}
        section = _section(files)
        self.assertEqual(sorted(s["cidr"] for s in section["subnets"]),
                         ["10.1.16.0/20", "10.2.16.0/20"])
        self.assertEqual(section["unresolved"], [])

    def test_a_local_module_that_is_the_vpc_exposes_its_range_and_owns_the_subnets(self):
        # The instance stands for the aws_vpc inside modules/vpc. A subnet
        # at the root reads its range through module.vpc.vpc_cidr_block and
        # belongs to that inner entry, so a counted sibling is a covered
        # question, not one the user is asked. The same through a local
        # wrapper around the community module.
        root = ('resource "aws_subnet" "one" {\n  vpc_id = module.vpc.vpc_id\n'
                '  cidr_block = cidrsubnet(module.vpc.vpc_cidr_block, 4, 1)\n}\n'
                'resource "aws_subnet" "many" {\n  count = 3\n  vpc_id = module.vpc.vpc_id\n'
                '  cidr_block = cidrsubnet(module.vpc.vpc_cidr_block, 4, count.index)\n}\n')
        shapes = {
            "resource inside": {
                "modules/vpc/main.tf": 'variable "cidr" {}\nresource "aws_vpc" "main" {\n  cidr_block = var.cidr\n}\n',
                "main.tf": 'module "vpc" {\n  source = "./modules/vpc"\n  cidr = "10.0.0.0/16"\n}\n' + root},
            "community module inside": {
                "modules/vpc/main.tf": ('variable "cidr" {}\nmodule "vpc" {\n'
                                        '  source = "terraform-aws-modules/vpc/aws"\n  cidr = var.cidr\n}\n'),
                "main.tf": 'module "vpc" {\n  source = "./modules/vpc"\n  cidr = "10.0.0.0/16"\n}\n' + root},
        }
        for label, files in shapes.items():
            with self.subTest(label):
                section = _section(files)
                vpc, = section["vpcs"]
                self.assertEqual(vpc["cidr"], "10.0.0.0/16")
                self.assertEqual({(s["cidr"], s["vpc"]) for s in section["subnets"]},
                                 {("10.0.16.0/20", vpc["address"]), (None, vpc["address"])})
                blocking, covered = addressspace.triage_unresolved(section)
                self.assertEqual(([e["address"] for e in blocking], [e["address"] for e in covered]),
                                 ([], ["aws_subnet.many"]))

    def test_a_vpc_resource_named_like_a_module_label_is_not_remapped(self):
        # `aws_vpc.vpc` beside `module "vpc"`: a subnet and a cluster that
        # name the resource keep it; the label match must not send them
        # into the module's own VPC.
        files = {"modules/vpc/main.tf": 'variable "cidr" {}\nresource "aws_vpc" "inner" {\n  cidr_block = var.cidr\n}\n',
                 "main.tf": ('resource "aws_vpc" "vpc" {\n  cidr_block = "10.0.0.0/16"\n}\n'
                             'module "vpc" {\n  source = "./modules/vpc"\n  cidr = "10.5.0.0/16"\n}\n'
                             'resource "aws_subnet" "a" {\n  vpc_id = aws_vpc.vpc.id\n  cidr_block = "10.0.1.0/24"\n}\n'
                             'resource "aws_eks_cluster" "c" {\n  name = "c"\n'
                             '  vpc_config {\n    subnet_ids = [aws_subnet.a.id]\n  }\n}\n')}
        section = _section(files)
        self.assertEqual(section["subnets"][0]["vpc"], "aws_vpc.vpc")
        self.assertEqual(section["clusters"][0]["vpc"], "aws_vpc.vpc")
        self.assertEqual({(v["address"], v["cluster_vpc"]) for v in section["vpcs"]},
                         {("aws_vpc.vpc", True), ("aws_vpc.inner", False)})

    def test_two_copies_of_a_module_share_a_key_and_are_covered_only_together(self):
        # dev passes a literal, prod a variable with no default. The section
        # cannot tell the two copies' subnets apart, and need not: dev's sit
        # inside a stated range, prod's are settled by prod's VPC question.
        files = {"modules/network/main.tf": ('variable "vpc_cidr" {}\n'
                                             'resource "aws_vpc" "this" {\n  cidr_block = var.vpc_cidr\n}\n'
                                             'resource "aws_subnet" "private" {\n  count = 3\n  vpc_id = aws_vpc.this.id\n'
                                             '  cidr_block = cidrsubnet(aws_vpc.this.cidr_block, 4, count.index)\n}\n'),
                 "envs/dev/main.tf": 'module "network" {\n  source = "../../modules/network"\n  vpc_cidr = "10.1.0.0/16"\n}\n',
                 "envs/prod/main.tf": ('variable "prod_cidr" {}\n'
                                       'module "network" {\n  source = "../../modules/network"\n  vpc_cidr = var.prod_cidr\n}\n')}
        section = _section(files)
        self.assertEqual([v["cidr"] for v in section["vpcs"]], ["10.1.0.0/16", None])
        blocking, covered = addressspace.triage_unresolved(section)
        self.assertEqual([(e["address"], e["argument"]) for e in blocking],
                         [("module.network.aws_vpc.this", "cidr_block")])
        self.assertEqual({e["address"] for e in covered}, {"module.network.aws_subnet.private"})

    def test_two_environments_each_stating_their_copy_cover_their_own_subnets(self):
        # envs/dev and envs/prod each instantiate modules/vpc with a literal
        # and each declare counted subnets beside it: two stated carriers
        # of module.vpc.aws_vpc.main, and every one of those subnets is a
        # covered question. The literal-index subnet resolves outright.
        env = ('module "vpc" {{\n  source = "../../modules/vpc"\n  cidr = "{cidr}"\n}}\n'
               'resource "aws_subnet" "many" {{\n  count = 2\n  vpc_id = module.vpc.vpc_id\n'
               '  cidr_block = cidrsubnet(module.vpc.vpc_cidr_block, 4, count.index)\n}}\n'
               'resource "aws_subnet" "one" {{\n  vpc_id = module.vpc.vpc_id\n'
               '  cidr_block = cidrsubnet(module.vpc.vpc_cidr_block, 4, 1)\n}}\n')
        files = {"modules/vpc/main.tf": 'variable "cidr" {}\nresource "aws_vpc" "main" {\n  cidr_block = var.cidr\n}\n',
                 "envs/dev/main.tf": env.format(cidr="10.1.0.0/16"),
                 "envs/prod/main.tf": env.format(cidr="10.2.0.0/16")}
        section = _section(files)
        self.assertEqual({v["cidr"] for v in section["vpcs"]}, {"10.1.0.0/16", "10.2.0.0/16"})
        self.assertEqual({(s["path"], s["cidr"]) for s in section["subnets"] if s["cidr"]},
                         {("envs/dev/main.tf", "10.1.16.0/20"), ("envs/prod/main.tf", "10.2.16.0/20")})
        blocking, covered = addressspace.triage_unresolved(section)
        self.assertEqual(blocking, [])
        self.assertEqual(sorted(e["path"] for e in covered), ["envs/dev/main.tf", "envs/prod/main.tf"])

    def test_a_local_module_of_any_name_that_declares_the_vpc_exposes_its_range(self):
        # The module directory is `network`, not `vpc`: the output is
        # followed all the same, from the VPC entry the instance stands for.
        files = {"modules/network/main.tf": 'variable "cidr" {}\nresource "aws_vpc" "this" {\n  cidr_block = var.cidr\n}\n',
                 "main.tf": ('module "network" {\n  source = "./modules/network"\n  cidr = "10.10.0.0/16"\n}\n'
                             'resource "aws_subnet" "a" {\n  vpc_id = module.network.vpc_id\n'
                             '  cidr_block = cidrsubnet(module.network.vpc_cidr_block, 4, 5)\n}\n')}
        section = _section(files)
        self.assertEqual([(s["cidr"], s["vpc"]) for s in section["subnets"]], [("10.10.80.0/20", "aws_vpc.this")])
        self.assertEqual(section["unresolved"], [])

    def test_a_tfvars_file_in_a_module_directory_is_not_loaded(self):
        # Terraform loads .tfvars for the root module only: the module's
        # default stands when the instance passes nothing, the instance's
        # value when it does, and the file is noted, never applied.
        module = {"modules/vpc/main.tf": ('variable "cidr" {\n  default = "10.0.0.0/16"\n}\n'
                                          'resource "aws_vpc" "main" {\n  cidr_block = var.cidr\n}\n'),
                  "modules/vpc/terraform.tfvars": 'cidr = "10.9.0.0/16"\n'}
        section, notes = _harvest({**module, "main.tf": 'module "vpc" {\n  source = "./modules/vpc"\n}\n'})
        self.assertEqual(section["vpcs"][0]["cidr"], "10.0.0.0/16")
        self.assertIn("modules/vpc/terraform.tfvars: not read; Terraform loads .tfvars files for the "
                      "root module only, and modules/vpc is instantiated as a module", notes)
        section, _ = _harvest({**module, "main.tf": 'module "vpc" {\n  source = "./modules/vpc"\n  cidr = "10.1.0.0/16"\n}\n'})
        self.assertEqual(section["vpcs"][0]["cidr"], "10.1.0.0/16")

    def test_cidrsubnet_functions_keep_terraforms_rules(self):
        # cidrsubnets aligns each block to its own size; cidrsubnet refuses
        # an index outside the parent instead of returning an address past it.
        self.assertEqual(addressspace._cidrsubnets("10.0.0.0/16", [8, 4, 8]),
                         ["10.0.0.0/24", "10.0.16.0/20", "10.0.32.0/24"])
        self.assertEqual(addressspace._cidrsubnet("10.0.0.0/16", 4, 15), "10.0.240.0/20")
        self.assertIsNone(addressspace._cidrsubnet("10.0.0.0/16", 4, 16))
        self.assertIsNone(addressspace._cidrsubnet("10.0.0.0/16", 4, -1))
        # cidrsubnets must extend the prefix by at least one bit (Terraform's
        # implementation refuses zero); zero would hand back the parent range
        # as a subnet.
        self.assertIsNone(addressspace._cidrsubnets("10.0.0.0/16", [0, 8]))

    def test_subnets_of_a_vpc_whose_range_is_a_question_are_settled_by_it(self):
        # The VPC's range is a variable with no default: one question. The
        # subnets cut from it are settled by that answer, not asked again.
        tf = ('variable "vpc_cidr" {}\n'
              'resource "aws_vpc" "main" {\n  cidr_block = var.vpc_cidr\n}\n'
              'resource "aws_subnet" "private" {\n  count = 3\n  vpc_id = aws_vpc.main.id\n'
              '  cidr_block = cidrsubnet(var.vpc_cidr, 4, count.index)\n}\n'
              'resource "aws_subnet" "public" {\n  count = 3\n  vpc_id = aws_vpc.main.id\n'
              '  cidr_block = cidrsubnet(var.vpc_cidr, 4, count.index + 3)\n}\n')
        section = _section({"main.tf": tf})
        blocking, covered = addressspace.triage_unresolved(section)
        self.assertEqual([(e["address"], e["argument"]) for e in blocking], [("aws_vpc.main", "cidr_block")])
        self.assertEqual(sorted(e["address"] for e in covered), ["aws_subnet.private", "aws_subnet.public"])

    def test_an_unresolved_subnet_list_on_an_ipam_module_vpc_is_a_question(self):
        # The module VPC has no range and no question of its own (an IPAM
        # pool); its unresolved subnet list has nothing to ride on.
        tf = ('variable "subnets" {}\n'
              'module "vpc" {\n  source = "terraform-aws-modules/vpc/aws"\n'
              '  ipv4_ipam_pool_id = "ipam-pool-1"\n  private_subnets = var.subnets\n}\n')
        section = _section({"main.tf": tf})
        blocking, covered = addressspace.triage_unresolved(section)
        self.assertEqual(([(e["address"], e["argument"]) for e in blocking], covered),
                         ([("module.vpc", "private_subnets")], []))

    def test_a_list_of_literals_is_evidence_without_an_origin_note(self):
        tf = ('module "vpc" {\n  source = "terraform-aws-modules/vpc/aws"\n  cidr = "10.0.0.0/16"\n'
              '  private_subnets = ["10.0.1.0/24", "10.0.2.0/24"]\n}\n')
        section = _section({"main.tf": tf})
        self.assertEqual([s["evidence"][0].split(": ", 1)[1] for s in section["subnets"]],
                         ["private_subnets[0] = 10.0.1.0/24", "private_subnets[1] = 10.0.2.0/24"])

    def test_list_functions_over_a_stated_list_resolve_as_terraform_does(self):
        # tolist and flatten pass a flat list through; sort orders it,
        # distinct dedupes it, toset dedupes and orders it, compact drops
        # empty members, so azs[index] pairs with the right range downstream.
        tf = ('variable "cidrs" {\n  default = ["10.0.2.0/24", "10.0.1.0/24", "10.0.2.0/24"]\n}\n'
              'variable "gappy" {\n  default = ["10.0.3.0/24", ""]\n}\n'
              'module "vpc" {\n  source = "terraform-aws-modules/vpc/aws"\n  cidr = "10.0.0.0/16"\n'
              '  private_subnets    = tolist(var.cidrs)\n'
              '  public_subnets     = sort(var.cidrs)\n'
              '  database_subnets   = distinct(var.cidrs)\n'
              '  intra_subnets      = tolist(toset(var.cidrs))\n'
              '  elasticache_subnets = compact(var.gappy)\n}\n')
        section = _section({"main.tf": tf})
        by_tier = {}
        for s in section["subnets"]:
            by_tier.setdefault(s["tier"], []).append(s["cidr"])
        self.assertEqual(by_tier, {"private": ["10.0.2.0/24", "10.0.1.0/24", "10.0.2.0/24"],
                                   "public": ["10.0.1.0/24", "10.0.2.0/24", "10.0.2.0/24"],
                                   "database": ["10.0.2.0/24", "10.0.1.0/24"],
                                   "intra": ["10.0.1.0/24", "10.0.2.0/24"],
                                   "elasticache": ["10.0.3.0/24"]})
        self.assertEqual(section["unresolved"], [])

    def test_the_scan_summary_passes_the_eksctl_default_flag_through(self):
        # The datascan instructions tell the agent to say when a VPC range is
        # eksctl's own default; the flag has to reach the summary.
        from servers.phases.discovery.discovery_datascan_3 import tools as datascan
        doc = ("apiVersion: eksctl.io/v1alpha5\nkind: ClusterConfig\n"
               "metadata:\n  name: simple\n  region: us-west-2\n")
        section = _section({"c.yaml": doc})
        summary = datascan._address_space_summary({"address_space": section})
        self.assertEqual([(v["cidr"], v.get("defaulted")) for v in summary["vpcs"]], [("192.168.0.0/16", True)])

    def test_every_module_subnet_list_is_read_with_its_tier_and_triaged(self):
        # All seven lists the community module accepts: each records its
        # tier, and each, unresolved under a stated VPC, is covered.
        # Spelled out, not read from the module's own table: the test must
        # notice a name dropped from it.
        tiers = [("private_subnets", "private"), ("public_subnets", "public"), ("intra_subnets", "intra"),
                 ("database_subnets", "database"), ("elasticache_subnets", "elasticache"),
                 ("redshift_subnets", "redshift"), ("outpost_subnets", "outpost")]
        lists = "".join(f'  {arg} = ["10.0.{i}.0/24"]\n' for i, (arg, _) in enumerate(tiers))
        tf = 'module "vpc" {\n  source = "terraform-aws-modules/vpc/aws"\n  cidr = "10.0.0.0/16"\n' + lists + '}\n'
        section = _section({"main.tf": tf})
        self.assertEqual([s["tier"] for s in section["subnets"]], [tier for _, tier in tiers])
        unresolved = "".join(f'  {arg} = var.x{i}\n' for i, (arg, _) in enumerate(tiers))
        variables = "".join(f'variable "x{i}" {{}}\n' for i in range(len(tiers)))
        tf = variables + 'module "vpc" {\n  source = "terraform-aws-modules/vpc/aws"\n  cidr = "10.0.0.0/16"\n' + unresolved + '}\n'
        blocking, covered = addressspace.triage_unresolved(_section({"main.tf": tf}))
        self.assertEqual((blocking, sorted(e["argument"] for e in covered)),
                         ([], sorted(arg for arg, _ in tiers)))

    def test_a_vpc_modules_own_range_question_does_not_cover_itself(self):
        # The module's subnet entries carry the module's address. Its own
        # `cidr` question must stay a question, and its unresolved subnet
        # list is settled by that question, as a counted resource subnet is.
        tf = ('variable "vpc_cidr" {}\nvariable "subnets" {}\n'
              'module "vpc" {\n  source = "terraform-aws-modules/vpc/aws"\n  cidr = var.vpc_cidr\n'
              '  private_subnets = ["10.0.1.0/24", "10.0.2.0/24"]\n  public_subnets = var.subnets\n}\n')
        section = _section({"main.tf": tf})
        blocking, covered = addressspace.triage_unresolved(section)
        self.assertEqual([(e["address"], e["argument"]) for e in blocking], [("module.vpc", "cidr")])
        self.assertEqual([(e["address"], e["argument"]) for e in covered], [("module.vpc", "public_subnets")])

    def test_a_vpc_question_in_another_directory_settles_the_subnets_cut_from_it(self):
        # The VPC lives in modules/vpc, its range is the root's variable
        # with no default; the root's counted subnets are cut from it. One
        # question, the VPC's, and the subnets ride on its answer.
        files = {"modules/vpc/main.tf": 'variable "cidr" {}\nresource "aws_vpc" "main" {\n  cidr_block = var.cidr\n}\n',
                 "main.tf": ('variable "vpc_cidr" {}\n'
                             'module "vpc" {\n  source = "./modules/vpc"\n  cidr = var.vpc_cidr\n}\n'
                             'resource "aws_subnet" "pods" {\n  count = 2\n  vpc_id = module.vpc.vpc_id\n'
                             '  cidr_block = cidrsubnet(module.vpc.vpc_cidr_block, 4, count.index)\n}\n')}
        section = _section(files)
        blocking, covered = addressspace.triage_unresolved(section)
        self.assertEqual([(e["address"], e["argument"]) for e in blocking], [("aws_vpc.main", "cidr_block")])
        self.assertEqual([e["address"] for e in covered], ["aws_subnet.pods"])

    def test_a_namesake_vpc_in_the_subnets_directory_does_not_decide(self):
        # The root declares aws_vpc.main and so does modules/vpc; the root's
        # subnets sit in the module's. When the module's VPC is IPAM-allocated
        # (no range, no question) the subnets are questions, whatever the
        # root's namesake states; the other way round they are covered.
        def files(root_vpc, module_vpc):
            return {"modules/vpc/main.tf": 'variable "pool" {\n  default = "ipam-pool-1"\n}\n' + module_vpc,
                    "main.tf": (root_vpc + 'module "vpc" {\n  source = "./modules/vpc"\n}\n'
                                'resource "aws_subnet" "pod" {\n  count = 4\n  vpc_id = module.vpc.vpc_id\n'
                                '  cidr_block = cidrsubnet(module.vpc.vpc_cidr_block, 4, count.index)\n}\n')}
        stated = 'resource "aws_vpc" "main" {\n  cidr_block = "10.0.0.0/16"\n}\n'
        ipam = 'resource "aws_vpc" "main" {\n  ipv4_ipam_pool_id = var.pool\n  ipv4_netmask_length = 16\n}\n'
        section = _section(files(stated, ipam))
        self.assertEqual(section["subnets"][0]["vpc_path"], "modules/vpc/main.tf")
        blocking, covered = addressspace.triage_unresolved(section)
        self.assertEqual(([e["address"] for e in blocking], covered), (["aws_subnet.pod"], []))
        section = _section(files(ipam, stated))
        blocking, covered = addressspace.triage_unresolved(section)
        self.assertEqual((blocking, [e["address"] for e in covered]), ([], ["aws_subnet.pod"]))

    def test_two_copies_of_a_subnets_module_are_covered_only_when_both_vpcs_are(self):
        # envs/dev and envs/prod each instantiate modules/subnets over their
        # own aws_vpc.main; dev's is stated, prod's comes from an IPAM pool
        # with no question. The two copies' entries share a key, so both
        # stay questions, whichever root the walk reads first.
        module = ('variable "vpc_id" {}\nvariable "vpc_cidr" {}\n'
                  'resource "aws_subnet" "s" {\n  count = 2\n  vpc_id = var.vpc_id\n'
                  '  cidr_block = cidrsubnet(var.vpc_cidr, 4, count.index)\n}\n')
        instance = ('module "subnets" {\n  source = "../../modules/subnets"\n  vpc_id = aws_vpc.main.id\n'
                    '  vpc_cidr = aws_vpc.main.cidr_block\n}\n')
        stated = 'resource "aws_vpc" "main" {\n  cidr_block = "10.1.0.0/16"\n}\n' + instance
        ipam = 'resource "aws_vpc" "main" {\n  ipv4_ipam_pool_id = "ipam-pool-1"\n  ipv4_netmask_length = 16\n}\n' + instance
        for stated_root, ipam_root in (("envs/dev", "envs/prod"), ("envs/beta", "envs/alpha")):
            with self.subTest(stated_root):
                section = _section({"modules/subnets/main.tf": module,
                                    f"{stated_root}/main.tf": stated, f"{ipam_root}/main.tf": ipam})
                blocking, covered = addressspace.triage_unresolved(section)
                self.assertEqual(sorted(e["address"] for e in blocking),
                                 ["module.subnets.aws_subnet.s", "module.subnets.aws_subnet.s"])
                self.assertEqual(covered, [])

    def test_a_counted_subnet_over_the_vpcs_range_is_a_covered_question(self):
        # The commonest subnet idiom. The range depends on count.index, so
        # it is unresolved, but the VPC's range is stated, so the design
        # needs no answer: the triage says so, and the scan summary and the
        # proposal both read the triage.
        tf = ('resource "aws_vpc" "main" {\n  cidr_block = "10.0.0.0/16"\n}\n'
              'resource "aws_subnet" "private" {\n  count = 3\n  vpc_id = aws_vpc.main.id\n'
              '  cidr_block = cidrsubnet(aws_vpc.main.cidr_block, 4, count.index)\n}\n'
              'resource "aws_route" "peer" {\n  destination_cidr_block = var.peer\n'
              '  vpc_peering_connection_id = "pcx-1"\n}\n')
        section = _section({"main.tf": tf})
        self.assertEqual([(e["argument"], e["expression"]) for e in section["unresolved"]],
                         [("cidr_block", "cidrsubnet(aws_vpc.main.cidr_block, 4, count.index) "
                                         "over for_each/count = 3"),
                          ("destination_cidr_block", "var.peer (no default)")])
        blocking, covered = addressspace.triage_unresolved(section)
        self.assertEqual([e["argument"] for e in blocking], ["destination_cidr_block"])
        self.assertEqual([e["argument"] for e in covered], ["cidr_block"])

    def test_auto_loaded_tfvars_override_the_default_and_others_are_noted(self):
        files = {"variables.tf": 'variable "cidr" {\n  default = "10.1.0.0/16"\n}\n',
                 "terraform.tfvars": 'cidr = "10.9.0.0/16"\n',
                 "prod.tfvars": 'cidr = "10.7.0.0/16"\n',
                 "vpc.tf": 'resource "aws_vpc" "main" {\n  cidr_block = var.cidr\n}\n'}
        section, notes = _harvest(files)
        self.assertEqual(section["vpcs"][0]["cidr"], "10.9.0.0/16")
        self.assertIn("1 .tfvars file(s) Terraform does not load automatically were not read: "
                      "which one applies is decided at plan time with -var-file", notes)

    def test_tfvars_json_is_auto_loaded_in_terraforms_order(self):
        # terraform.tfvars, then terraform.tfvars.json, then the *.auto.*
        # files of either form in lexical order; a *.tfvars.json Terraform
        # does not load automatically is counted with the rest.
        base = {"variables.tf": ('variable "cidr" {\n  default = "10.0.0.0/16"\n}\n'
                                 'variable "subnets" {\n  default = []\n}\n'),
                "vpc.tf": ('resource "aws_vpc" "main" {\n  cidr_block = var.cidr\n}\n'
                           'module "vpc" {\n  source = "terraform-aws-modules/vpc/aws"\n'
                           '  cidr = var.cidr\n  private_subnets = var.subnets\n}\n'),
                "terraform.tfvars": 'cidr = "10.1.0.0/16"\n',
                "terraform.tfvars.json": '{"cidr": "10.2.0.0/16", "subnets": ["10.2.0.0/24", "10.2.1.0/24"]}',
                "prod.tfvars.json": '{"cidr": "10.7.0.0/16"}'}
        section, notes = _harvest(base)
        self.assertEqual([v["cidr"] for v in section["vpcs"]], ["10.2.0.0/16", "10.2.0.0/16"])
        self.assertEqual([s["cidr"] for s in section["subnets"]], ["10.2.0.0/24", "10.2.1.0/24"])
        self.assertIn("1 .tfvars file(s) Terraform does not load automatically were not read: "
                      "which one applies is decided at plan time with -var-file", notes)
        section, notes = _harvest({**base, "a.auto.tfvars.json": '{"cidr": "10.3.0.0/16"}'})
        self.assertEqual(section["vpcs"][0]["cidr"], "10.3.0.0/16")
        section, notes = _harvest({**base, "a.auto.tfvars.json": '{"cidr": '})
        self.assertEqual(section["vpcs"][0]["cidr"], "10.2.0.0/16")
        self.assertTrue(any(n.startswith("a.auto.tfvars.json: not parseable as JSON") for n in notes), notes)

    def test_later_tfvars_files_override_earlier_ones(self):
        # Terraform: terraform.tfvars first, then *.auto.tfvars in lexical
        # order, the last assignment winning.
        files = {"variables.tf": 'variable "cidr" {\n  default = "10.0.0.0/16"\n}\n',
                 "terraform.tfvars": 'cidr = "10.1.0.0/16"\n',
                 "a.auto.tfvars": 'cidr = "10.2.0.0/16"\n',
                 "b.auto.tfvars": 'cidr = "10.3.0.0/16"\n',
                 "vpc.tf": 'resource "aws_vpc" "main" {\n  cidr_block = var.cidr\n}\n'}
        self.assertEqual(_section(files)["vpcs"][0]["cidr"], "10.3.0.0/16")

    def test_cidrsubnet_and_cidrsubnets_are_computed(self):
        tf = ('variable "cidr" {\n  default = "10.42.0.0/16"\n}\n'
              'locals {\n'
              '  azs = ["us-east-1a", "us-east-1b"]\n'
              '}\n'
              'resource "aws_subnet" "one" {\n'
              '  vpc_id     = aws_vpc.main.id\n'
              '  cidr_block = cidrsubnet(var.cidr, 8, 1)\n'
              '}\n'
              'module "vpc" {\n'
              '  source          = "terraform-aws-modules/vpc/aws"\n'
              '  cidr            = var.cidr\n'
              '  azs             = local.azs\n'
              '  private_subnets = cidrsubnets(var.cidr, 3, 3)\n'
              '  public_subnets  = [\n'
              '    cidrsubnet(var.cidr, 3, 6),\n'
              '    "10.42.224.0/19",\n'
              '  ]\n'
              '}\n')
        section = _section({"main.tf": tf})
        subnets = {s["name"]: (s["cidr"], s["tier"], s["availability_zone"]) for s in section["subnets"]}
        self.assertEqual(subnets, {
            "one": ("10.42.1.0/24", None, None),
            "vpc/private_subnets[0]": ("10.42.0.0/19", "private", "us-east-1a"),
            "vpc/private_subnets[1]": ("10.42.32.0/19", "private", "us-east-1b"),
            "vpc/public_subnets[0]": ("10.42.192.0/19", "public", "us-east-1a"),
            "vpc/public_subnets[1]": ("10.42.224.0/19", "public", "us-east-1b"),
        })
        self.assertEqual(section["unresolved"], [])

    def test_module_secondary_cidr_blocks(self):
        tf = ('module "vpc" {\n'
              '  source                = "terraform-aws-modules/vpc/aws"\n'
              '  cidr                  = "10.0.0.0/16"\n'
              '  secondary_cidr_blocks = ["100.64.0.0/16", "100.65.0.0/16"]\n'
              '}\n')
        vpc, = _section({"main.tf": tf})["vpcs"]
        self.assertEqual(vpc["secondary_cidrs"], ["100.64.0.0/16", "100.65.0.0/16"])

    def test_eks_module_v20_argument_names(self):
        tf = ('module "eks" {\n'
              '  source                               = "terraform-aws-modules/eks/aws"\n'
              '  version                              = "~> 20.0"\n'
              '  cluster_name                         = "legacy"\n'
              '  cluster_service_ipv4_cidr            = "10.100.0.0/16"\n'
              '  cluster_ip_family                    = "ipv4"\n'
              '  cluster_endpoint_public_access_cidrs = ["198.51.100.0/24"]\n'
              '  vpc_id                               = aws_vpc.main.id\n'
              '  subnet_ids                           = [aws_subnet.a.id, aws_subnet.b.id]\n'
              '}\n')
        cluster, = _section({"main.tf": tf})["clusters"]
        self.assertEqual(cluster["name"], "legacy")
        self.assertEqual(cluster["service_ipv4_cidr"], "10.100.0.0/16")
        self.assertEqual(cluster["public_access_cidrs"], ["198.51.100.0/24"])
        self.assertEqual(cluster["vpc"], "aws_vpc.main")
        self.assertEqual(cluster["subnets"], ["aws_subnet.a", "aws_subnet.b"])

    def test_registry_submodules_and_lookalike_sources_are_not_the_vpc_or_the_cluster(self):
        # The review's phantom-entry case: the EKS module's karpenter
        # sub-module, the VPC module's endpoints sub-module, and the
        # blueprints add-ons module are none of them a VPC or a cluster.
        tf = ('module "karpenter" {\n'
              '  source       = "terraform-aws-modules/eks/aws//modules/karpenter"\n'
              '  cluster_name = module.eks.cluster_name\n}\n'
              'module "vpc_endpoints" {\n'
              '  source = "terraform-aws-modules/vpc/aws//modules/vpc-endpoints"\n'
              '  vpc_id = module.vpc.vpc_id\n}\n'
              'module "addons" {\n'
              '  source       = "aws-ia/eks-blueprints-addons/aws"\n'
              '  cluster_name = module.eks.cluster_name\n}\n'
              'module "eks" {\n'
              '  source       = "terraform-aws-modules/eks/aws"\n'
              '  cluster_name = "real"\n}\n'
              'module "network" {\n'
              '  source = "./modules/vpc"\n'
              '  cidr   = "10.0.0.0/16"\n}\n')
        section = _section({"main.tf": tf})
        self.assertEqual([c["address"] for c in section["clusters"]], ["module.eks"])
        self.assertEqual([v["address"] for v in section["vpcs"]], ["module.network"])

    def test_empty_secondary_list_is_not_a_question(self):
        tf = ('module "vpc" {\n'
              '  source                = "terraform-aws-modules/vpc/aws"\n'
              '  cidr                  = "10.0.0.0/16"\n'
              '  secondary_cidr_blocks = []\n'
              '}\n')
        section = _section({"main.tf": tf})
        self.assertEqual(section["vpcs"][0]["secondary_cidrs"], [])
        self.assertEqual(section["unresolved"], [])

    def test_same_named_resources_in_sibling_directories_stay_apart(self):
        # envs/dev and envs/prod both declare aws_vpc.main; only prod has the
        # cluster and the secondary range. dev sorts first in the walk.
        vpc = 'resource "aws_vpc" "main" {\n  cidr_block = "10.%d.0.0/16"\n}\n'
        prod_extra = ('resource "aws_vpc_ipv4_cidr_block_association" "pods" {\n'
                      '  vpc_id     = aws_vpc.main.id\n  cidr_block = "100.64.0.0/16"\n}\n'
                      'resource "aws_subnet" "a" {\n  vpc_id = aws_vpc.main.id\n'
                      '  cidr_block = "10.2.0.0/24"\n}\n'
                      'resource "aws_eks_cluster" "c" {\n  name = "c"\n'
                      '  vpc_config {\n    subnet_ids = [aws_subnet.a.id]\n  }\n}\n')
        section = _section({"envs/dev/main.tf": vpc % 1, "envs/prod/main.tf": vpc % 2 + prod_extra})
        by_dir = {v["directory"]: (v["cidr"], v["secondary_cidrs"], v["cluster_vpc"]) for v in section["vpcs"]}
        self.assertEqual(by_dir, {"envs/dev": ("10.1.0.0/16", [], False),
                                  "envs/prod": ("10.2.0.0/16", ["100.64.0.0/16"], True)})

    def test_unresolved_subnet_keeps_its_vpc_link_for_the_cluster(self):
        # The most common shape there is: subnets cut with cidrsubnet over a
        # resource attribute and count.index. The CIDRs are questions; the
        # cluster's VPC is not.
        tf = ('resource "aws_vpc" "main" {\n  cidr_block = "10.0.0.0/16"\n}\n'
              'resource "aws_vpc" "other" {\n  cidr_block = "10.9.0.0/16"\n}\n'
              'resource "aws_subnet" "private" {\n'
              '  count      = 3\n'
              '  vpc_id     = aws_vpc.main.id\n'
              '  cidr_block = cidrsubnet(aws_vpc.main.cidr_block, 4, count.index)\n}\n'
              'resource "aws_eks_cluster" "c" {\n  name = "c"\n'
              '  vpc_config {\n    subnet_ids = aws_subnet.private[*].id\n  }\n}\n')
        section = _section({"main.tf": tf})
        subnet, = section["subnets"]
        self.assertEqual((subnet["cidr"], subnet["vpc"]), (None, "aws_vpc.main"))
        self.assertEqual(section["clusters"][0]["vpc"], "aws_vpc.main")
        self.assertEqual({v["name"]: v["cluster_vpc"] for v in section["vpcs"]},
                         {"main": True, "other": False})
        self.assertEqual(section["unresolved"][0]["argument"], "cidr_block")

    def test_a_cluster_whose_vpc_cannot_be_followed_leaves_others_unknown(self):
        tf = ('resource "aws_vpc" "main" {\n  cidr_block = "10.0.0.0/16"\n}\n'
              'resource "aws_eks_cluster" "c" {\n  name = "c"\n'
              '  vpc_config {\n    subnet_ids = var.subnet_ids\n  }\n}\n')
        section = _section({"main.tf": tf})
        self.assertIsNone(section["clusters"][0]["vpc"])
        self.assertIsNone(section["vpcs"][0]["cluster_vpc"])

    def test_local_module_inputs_are_answered_at_the_instance(self):
        # modules/ plus envs/: the module reads var.cidr, the instance states it.
        files = {"modules/vpc/main.tf": 'resource "aws_vpc" "main" {\n  cidr_block = var.cidr\n}\n',
                 "modules/vpc/variables.tf": 'variable "cidr" {\n  type = string\n}\n',
                 "modules/eks/main.tf": ('resource "aws_eks_cluster" "c" {\n  name = var.name\n'
                                         '  vpc_config {\n    subnet_ids = var.subnet_ids\n  }\n}\n'),
                 "envs/prod/main.tf": ('module "vpc" {\n  source = "../../modules/vpc"\n'
                                       '  cidr   = "10.0.0.0/16"\n}\n'
                                       'module "eks" {\n  source     = "../../modules/eks"\n'
                                       '  name       = "prod"\n'
                                       '  vpc_id     = module.vpc.vpc_id\n'
                                       '  subnet_ids = module.vpc.private_subnets\n}\n')}
        section, notes = _harvest(files)
        # One VPC: the aws_vpc inside the module, with the range the instance
        # passes; the instance itself is not a second entry.
        vpc, = section["vpcs"]
        self.assertEqual((vpc["address"], vpc["cidr"], vpc["cluster_vpc"]),
                         ("aws_vpc.main", "10.0.0.0/16", True))
        self.assertIn("modules/vpc/main.tf:1: cidr_block = var.cidr, passed by the instance "
                      "module.vpc (envs/prod/main.tf): cidr = 10.0.0.0/16", vpc["evidence"])
        # Likewise one cluster: the aws_eks_cluster inside modules/eks, named
        # and wired by the instance; the instance is not a second entry.
        self.assertEqual([(c["address"], c["name"], c["vpc"]) for c in section["clusters"]],
                         [("aws_eks_cluster.c", "prod", "aws_vpc.main")])
        self.assertEqual(section["unresolved"], [])
        self.assertIn("modules/vpc: a local module, instantiated by module.vpc (envs/prod/main.tf); "
                      "a range it reads from an input variable is resolved through the value the "
                      "instance passes", notes)

    def test_a_renamed_module_input_is_resolved_through_the_instance(self):
        # The round-3 regression: the wrapper is not recognised as a VPC
        # module (no `cidr` argument, source not named vpc), so the range
        # exists only inside it, fed by `vpc_cidr` at the instance.
        files = {"modules/network/main.tf": 'resource "aws_vpc" "this" {\n  cidr_block = var.vpc_cidr\n}\n',
                 "modules/network/variables.tf": 'variable "vpc_cidr" {\n  type = string\n}\n',
                 "envs/prod/main.tf": ('module "network" {\n  source   = "../../modules/network"\n'
                                       '  vpc_cidr = "10.0.0.0/16"\n}\n'
                                       'module "eks" {\n  source = "terraform-aws-modules/eks/aws"\n'
                                       '  cluster_name = "prod"\n  vpc_id = module.network.vpc_id\n}\n')}
        section = _section(files)
        vpc, = section["vpcs"]
        self.assertEqual((vpc["address"], vpc["cidr"], vpc["cluster_vpc"]), ("aws_vpc.this", "10.0.0.0/16", True))
        self.assertEqual(section["clusters"][0]["vpc"], "aws_vpc.this")
        self.assertEqual(section["unresolved"], [])

    def test_the_instance_value_overrides_the_module_default(self):
        # Terraform semantics: the argument passed at the instance wins over
        # the variable's default. Reading the default would record a range the
        # estate does not use.
        files = {"modules/vpc/main.tf": 'resource "aws_vpc" "main" {\n  cidr_block = var.cidr\n}\n',
                 "modules/vpc/variables.tf": 'variable "cidr" {\n  default = "10.0.0.0/16"\n}\n',
                 "envs/prod/main.tf": 'module "vpc" {\n  source = "../../modules/vpc"\n  cidr = "10.50.0.0/16"\n}\n'}
        section = _section(files)
        vpc, = section["vpcs"]
        self.assertEqual(vpc["cidr"], "10.50.0.0/16")
        self.assertIn("passed by the instance module.vpc (envs/prod/main.tf): cidr = 10.50.0.0/16",
                      vpc["evidence"][0])

    def test_the_default_applies_when_no_instance_passes_the_input(self):
        files = {"modules/vpc/main.tf": 'resource "aws_vpc" "main" {\n  cidr_block = var.cidr\n}\n',
                 "modules/vpc/variables.tf": 'variable "cidr" {\n  default = "10.0.0.0/16"\n}\n',
                 "envs/prod/main.tf": 'module "vpc" {\n  source = "../../modules/vpc"\n}\n'}
        self.assertEqual(_section(files)["vpcs"][0]["cidr"], "10.0.0.0/16")

    def test_a_cluster_inside_a_local_module_is_resolved_through_its_instance(self):
        # The instance is not recognised as an EKS module (source not named
        # eks, no community argument names), so the cluster exists only
        # inside the module, fed by the instance's arguments.
        files = {"modules/vpc/main.tf": 'resource "aws_vpc" "main" {\n  cidr_block = var.cidr\n}\n',
                 "modules/cluster/main.tf": ('resource "aws_eks_cluster" "c" {\n  name = var.name\n'
                                             '  vpc_config {\n    subnet_ids = var.subnet_ids\n  }\n'
                                             '  kubernetes_network_config {\n'
                                             '    service_ipv4_cidr = var.service_cidr\n  }\n}\n'),
                 "envs/prod/main.tf": ('module "vpc" {\n  source = "../../modules/vpc"\n  cidr = "10.0.0.0/16"\n}\n'
                                       'module "cluster" {\n  source = "../../modules/cluster"\n'
                                       '  name         = "prod"\n'
                                       '  service_cidr = "10.20.0.0/16"\n'
                                       '  subnet_ids   = module.vpc.private_subnets\n}\n')}
        section = _section(files)
        cluster, = section["clusters"]
        self.assertEqual((cluster["address"], cluster["name"], cluster["vpc"], cluster["service_ipv4_cidr"]),
                         ("aws_eks_cluster.c", "prod", "aws_vpc.main", "10.20.0.0/16"))
        self.assertTrue(section["vpcs"][0]["cluster_vpc"])
        self.assertEqual(section["unresolved"], [])

    def test_a_literal_in_the_module_cluster_body_does_not_make_a_second_cluster(self):
        files = {"modules/eks/main.tf": ('resource "aws_eks_cluster" "c" {\n  name = var.name\n'
                                         '  vpc_config {\n    subnet_ids          = var.subnet_ids\n'
                                         '    public_access_cidrs = ["203.0.113.0/24"]\n  }\n}\n'),
                 "envs/prod/main.tf": ('module "eks" {\n  source     = "../../modules/eks"\n'
                                       '  name       = "prod"\n  subnet_ids = [aws_subnet.a.id]\n}\n'
                                       'resource "aws_vpc" "main" {\n  cidr_block = "10.0.0.0/16"\n}\n'
                                       'resource "aws_subnet" "a" {\n  vpc_id = aws_vpc.main.id\n'
                                       '  cidr_block = "10.0.1.0/24"\n}\n')}
        section = _section(files)
        cluster, = section["clusters"]
        self.assertEqual((cluster["name"], cluster["vpc"], cluster["public_access_cidrs"]),
                         ("prod", "aws_vpc.main", ["203.0.113.0/24"]))
        self.assertTrue(section["vpcs"][0]["cluster_vpc"])

    def test_a_wrapper_around_a_wrapper_is_followed_one_hop(self):
        files = {"modules/vpc/main.tf": 'resource "aws_vpc" "main" {\n  cidr_block = var.cidr\n}\n',
                 "modules/stack/main.tf": 'module "vpc" {\n  source = "../vpc"\n  cidr = var.vpc_cidr\n}\n',
                 "envs/prod/main.tf": 'module "stack" {\n  source = "../../modules/stack"\n  vpc_cidr = "10.0.0.0/16"\n}\n'}
        section = _section(files)
        self.assertEqual([(v["address"], v["cidr"]) for v in section["vpcs"]], [("aws_vpc.main", "10.0.0.0/16")])
        self.assertEqual(section["unresolved"], [])

    def test_secondary_range_on_a_module_instance_attaches_to_the_inner_vpc(self):
        files = {"modules/vpc/main.tf": 'resource "aws_vpc" "main" {\n  cidr_block = "10.0.0.0/16"\n}\n',
                 "envs/prod/main.tf": ('module "vpc" {\n  source = "../../modules/vpc"\n}\n'
                                       'resource "aws_vpc_ipv4_cidr_block_association" "pods" {\n'
                                       '  vpc_id     = module.vpc.vpc_id\n'
                                       '  cidr_block = "100.64.0.0/16"\n}\n')}
        section = _section(files)
        vpc, = section["vpcs"]
        self.assertEqual((vpc["address"], vpc["secondary_cidrs"]), ("aws_vpc.main", ["100.64.0.0/16"]))

    def test_a_subnet_without_a_cidr_block_says_so(self):
        tf = ('resource "aws_subnet" "v6" {\n  vpc_id = aws_vpc.main.id\n'
              '  ipv6_cidr_block = cidrsubnet(aws_vpc.main.ipv6_cidr_block, 8, 1)\n}\n')
        section = _section({"main.tf": tf})
        self.assertIn("no cidr_block declared", section["subnets"][0]["evidence"][0])
        self.assertEqual(section["unresolved"], [])

    def test_a_module_input_no_instance_passes_is_a_question(self):
        files = {"modules/network/main.tf": 'resource "aws_vpc" "this" {\n  cidr_block = var.vpc_cidr\n}\n',
                 "envs/prod/main.tf": 'module "network" {\n  source = "../../modules/network"\n}\n'}
        section = _section(files)
        self.assertEqual(section["unresolved"][0]["expression"], "var.vpc_cidr (no default)")

    def test_a_module_instantiated_twice_is_two_vpcs(self):
        # envs/dev and envs/prod each instantiate modules/network with their
        # own range: two stated VPCs, recorded under Terraform's absolute
        # addresses, not one question.
        files = {"modules/network/main.tf": 'resource "aws_vpc" "this" {\n  cidr_block = var.vpc_cidr\n}\n',
                 "envs/dev/main.tf": 'module "network" {\n  source = "../../modules/network"\n  vpc_cidr = "10.1.0.0/16"\n}\n',
                 "envs/prod/main.tf": ('module "network" {\n  source = "../../modules/network"\n  vpc_cidr = "10.2.0.0/16"\n}\n'
                                       'module "eks" {\n  source = "terraform-aws-modules/eks/aws"\n'
                                       '  cluster_name = "prod"\n  vpc_id = module.network.vpc_id\n}\n')}
        section = _section(files)
        self.assertEqual([(v["address"], v["cidr"], v["cluster_vpc"]) for v in section["vpcs"]],
                         [("module.network.aws_vpc.this", "10.1.0.0/16", False),
                          ("module.network.aws_vpc.this", "10.2.0.0/16", True)])
        self.assertEqual(section["clusters"][0]["vpc"], "module.network.aws_vpc.this")
        self.assertEqual(section["unresolved"], [])

    def test_blue_and_green_clusters_from_one_module(self):
        files = {"modules/eks/main.tf": ('resource "aws_eks_cluster" "c" {\n  name = var.name\n'
                                         '  vpc_config {\n    subnet_ids = var.subnet_ids\n  }\n}\n'),
                 "main.tf": ('module "blue" {\n  source = "./modules/eks"\n  name = "blue"\n'
                             '  subnet_ids = [aws_subnet.a.id]\n}\n'
                             'module "green" {\n  source = "./modules/eks"\n  name = "green"\n'
                             '  subnet_ids = [aws_subnet.a.id]\n}\n'
                             'resource "aws_vpc" "main" {\n  cidr_block = "10.0.0.0/16"\n}\n'
                             'resource "aws_subnet" "a" {\n  vpc_id = aws_vpc.main.id\n'
                             '  cidr_block = "10.0.1.0/24"\n}\n')}
        section = _section(files)
        self.assertEqual([(c["address"], c["name"], c["vpc"]) for c in section["clusters"]],
                         [("module.blue.aws_eks_cluster.c", "blue", "aws_vpc.main"),
                          ("module.green.aws_eks_cluster.c", "green", "aws_vpc.main")])
        self.assertTrue(section["vpcs"][0]["cluster_vpc"])

    def test_everything_inside_a_twice_instantiated_module_stays_in_its_copy(self):
        # The round-6 case: VPC, subnet and cluster all inside one module,
        # instantiated as dev and prod. Each copy links to its own VPC, and
        # neither is written false.
        files = {"modules/env/main.tf": ('resource "aws_vpc" "main" {\n  cidr_block = var.cidr\n}\n'
                                         'resource "aws_subnet" "a" {\n  vpc_id = aws_vpc.main.id\n'
                                         '  cidr_block = cidrsubnet(var.cidr, 8, 1)\n}\n'
                                         'resource "aws_eks_cluster" "c" {\n  name = var.env\n'
                                         '  vpc_config {\n    subnet_ids = [aws_subnet.a.id]\n  }\n}\n'),
                 "main.tf": ('module "dev" {\n  source = "./modules/env"\n  cidr = "10.1.0.0/16"\n  env = "dev"\n}\n'
                             'module "prod" {\n  source = "./modules/env"\n  cidr = "10.2.0.0/16"\n  env = "prod"\n}\n')}
        section = _section(files)
        self.assertEqual([(v["address"], v["cidr"], v["cluster_vpc"]) for v in section["vpcs"]],
                         [("module.dev.aws_vpc.main", "10.1.0.0/16", True),
                          ("module.prod.aws_vpc.main", "10.2.0.0/16", True)])
        # cidrsubnet over the instance's value resolves, per copy.
        self.assertEqual([(s["address"], s["cidr"], s["vpc"]) for s in section["subnets"]],
                         [("module.dev.aws_subnet.a", "10.1.1.0/24", "module.dev.aws_vpc.main"),
                          ("module.prod.aws_subnet.a", "10.2.1.0/24", "module.prod.aws_vpc.main")])
        self.assertEqual([(c["name"], c["vpc"]) for c in section["clusters"]],
                         [("dev", "module.dev.aws_vpc.main"), ("prod", "module.prod.aws_vpc.main")])
        self.assertEqual(section["unresolved"], [])

    def test_secondary_range_inside_a_twice_instantiated_module(self):
        files = {"modules/net/main.tf": ('resource "aws_vpc" "main" {\n  cidr_block = var.cidr\n}\n'
                                         'resource "aws_vpc_ipv4_cidr_block_association" "pods" {\n'
                                         '  vpc_id = aws_vpc.main.id\n  cidr_block = "100.64.0.0/16"\n}\n'),
                 "envs/dev/main.tf": 'module "net" {\n  source = "../../modules/net"\n  cidr = "10.1.0.0/16"\n}\n',
                 "envs/prod/main.tf": 'module "net" {\n  source = "../../modules/net"\n  cidr = "10.2.0.0/16"\n}\n'}
        section = _section(files)
        self.assertEqual([(v["cidr"], v["secondary_cidrs"]) for v in section["vpcs"]],
                         [("10.1.0.0/16", ["100.64.0.0/16"]), ("10.2.0.0/16", ["100.64.0.0/16"])])
        self.assertFalse(any("not declared" in e for v in section["vpcs"] for e in v["evidence"]))

    def test_an_unresolvable_instance_argument_is_a_question_not_the_default(self):
        # The instance means to supply the range but the scan cannot read it
        # (an SSM parameter): the module's default must not stand in.
        files = {"modules/vpc/main.tf": 'resource "aws_vpc" "main" {\n  cidr_block = var.cidr\n}\n',
                 "modules/vpc/variables.tf": 'variable "cidr" {\n  default = "10.0.0.0/16"\n}\n',
                 "envs/prod/main.tf": ('module "vpc" {\n  source = "../../modules/vpc"\n'
                                       '  cidr   = data.aws_ssm_parameter.vpc_cidr.value\n}\n')}
        section = _section(files)
        vpc, = section["vpcs"]
        self.assertIsNone(vpc["cidr"])
        entry, = section["unresolved"]
        self.assertIn("data.aws_ssm_parameter.vpc_cidr.value", entry["expression"])
        self.assertIn("passed by the instance", entry["expression"])

    def test_a_wrapper_instantiated_twice_is_two_copies_all_the_way_down(self):
        files = {"modules/vpc/main.tf": 'resource "aws_vpc" "main" {\n  cidr_block = var.cidr\n}\n',
                 "modules/stack/main.tf": ('module "vpc" {\n  source = "../vpc"\n  cidr = var.vpc_cidr\n}\n'
                                           'module "eks" {\n  source = "terraform-aws-modules/eks/aws"\n'
                                           '  cluster_name = var.name\n  vpc_id = module.vpc.vpc_id\n}\n'),
                 "envs/dev/main.tf": 'module "stack" {\n  source = "../../modules/stack"\n  vpc_cidr = "10.1.0.0/16"\n  name = "dev"\n}\n',
                 "envs/prod/main.tf": 'module "stack" {\n  source = "../../modules/stack"\n  vpc_cidr = "10.2.0.0/16"\n  name = "prod"\n}\n'}
        section = _section(files)
        self.assertEqual([(v["address"], v["cidr"], v["cluster_vpc"]) for v in section["vpcs"]],
                         [("module.stack.module.vpc.aws_vpc.main", "10.1.0.0/16", True),
                          ("module.stack.module.vpc.aws_vpc.main", "10.2.0.0/16", True)])
        self.assertEqual([(c["name"], c["vpc"]) for c in section["clusters"]],
                         [("dev", "module.stack.module.vpc.aws_vpc.main"),
                          ("prod", "module.stack.module.vpc.aws_vpc.main")])
        self.assertEqual(section["unresolved"], [])

    def test_a_module_with_two_vpcs_leaves_the_question_open(self):
        files = {"modules/network/main.tf": ('resource "aws_vpc" "a" {\n  cidr_block = "10.1.0.0/16"\n}\n'
                                             'resource "aws_vpc" "b" {\n  cidr_block = "10.2.0.0/16"\n}\n'),
                 "envs/prod/main.tf": ('module "network" {\n  source = "../../modules/network"\n}\n'
                                       'module "eks" {\n  source = "terraform-aws-modules/eks/aws"\n'
                                       '  cluster_name = "prod"\n  vpc_id = module.network.vpc_id\n}\n')}
        section = _section(files)
        self.assertEqual([v["cluster_vpc"] for v in section["vpcs"]], [None, None])

    def test_two_copies_of_a_wrapper_whose_cluster_names_only_subnets(self):
        # The resource form has no vpc_id: the cluster reaches its VPC through
        # the wrapper's inner module output, per copy.
        files = {"modules/vpc/main.tf": 'resource "aws_vpc" "main" {\n  cidr_block = var.cidr\n}\n',
                 "modules/stack/main.tf": ('module "vpc" {\n  source = "../vpc"\n  cidr = var.vpc_cidr\n}\n'
                                           'resource "aws_eks_cluster" "c" {\n  name = var.name\n'
                                           '  vpc_config {\n    subnet_ids = module.vpc.private_subnets\n  }\n}\n'),
                 "envs/dev/main.tf": 'module "stack" {\n  source = "../../modules/stack"\n  vpc_cidr = "10.1.0.0/16"\n  name = "dev"\n}\n',
                 "envs/prod/main.tf": 'module "stack" {\n  source = "../../modules/stack"\n  vpc_cidr = "10.2.0.0/16"\n  name = "prod"\n}\n'}
        section = _section(files)
        self.assertEqual([(v["cidr"], v["cluster_vpc"]) for v in section["vpcs"]],
                         [("10.1.0.0/16", True), ("10.2.0.0/16", True)])
        self.assertEqual([c["vpc"] for c in section["clusters"]],
                         ["module.stack.module.vpc.aws_vpc.main", "module.stack.module.vpc.aws_vpc.main"])

    def test_a_wrapper_holding_two_vpc_instances_follows_the_named_one(self):
        files = {"modules/vpc/main.tf": 'resource "aws_vpc" "main" {\n  cidr_block = var.cidr\n}\n',
                 "modules/stack/main.tf": ('module "vpc_a" {\n  source = "../vpc"\n  cidr = "10.1.0.0/16"\n}\n'
                                           'module "vpc_b" {\n  source = "../vpc"\n  cidr = "10.2.0.0/16"\n}\n'
                                           'module "eks" {\n  source = "terraform-aws-modules/eks/aws"\n'
                                           '  cluster_name = "c"\n  vpc_id = module.vpc_a.vpc_id\n}\n'),
                 "envs/prod/main.tf": 'module "stack" {\n  source = "../../modules/stack"\n}\n'}
        section = _section(files)
        self.assertEqual({v["address"]: v["cluster_vpc"] for v in section["vpcs"]},
                         {"module.vpc_a.aws_vpc.main": True, "module.vpc_b.aws_vpc.main": False})

    def test_a_local_module_with_a_cidr_argument_is_not_a_vpc(self):
        files = {"modules/subnet/main.tf": ('resource "aws_subnet" "this" {\n  vpc_id = var.vpc_id\n'
                                            '  cidr_block = var.cidr\n}\n'),
                 "main.tf": ('resource "aws_vpc" "main" {\n  cidr_block = "10.0.0.0/16"\n}\n'
                             'module "subnet_a" {\n  source = "./modules/subnet"\n'
                             '  vpc_id = aws_vpc.main.id\n  cidr = "10.0.1.0/24"\n}\n')}
        section = _section(files)
        self.assertEqual([v["address"] for v in section["vpcs"]], ["aws_vpc.main"])
        self.assertEqual([(s["cidr"], s["vpc"]) for s in section["subnets"]], [("10.0.1.0/24", "aws_vpc.main")])

    def test_the_repository_root_instantiated_from_a_subdirectory_is_followed(self):
        # A module repository with an examples tree: the root declares the
        # VPC from an input, envs/prod instantiates the root with `../..`.
        files = {"variables.tf": 'variable "cidr" {}\n',
                 "main.tf": 'resource "aws_vpc" "this" {\n  cidr_block = var.cidr\n}\n',
                 "terraform.tfvars": 'cidr = "10.9.0.0/16"\n',
                 "envs/prod/main.tf": 'module "vpc" {\n  source = "../.."\n  cidr = "10.0.0.0/16"\n}\n'}
        section, notes = _harvest(files)
        self.assertEqual([(v["address"], v["cidr"]) for v in section["vpcs"]], [("aws_vpc.this", "10.0.0.0/16")])
        self.assertEqual(section["unresolved"], [])
        self.assertIn("terraform.tfvars: not read; Terraform loads .tfvars files for the root module only, "
                      "and the repository root is instantiated as a module", notes)
        self.assertIn("the repository root: a local module, instantiated by module.vpc (envs/prod/main.tf); "
                      "a range it reads from an input variable is resolved through the value the instance passes",
                      notes)

    def test_a_module_that_instantiates_itself_does_not_hang(self):
        # At the root and in a subdirectory alike: the VPC beside the
        # self-instance is recorded once, under its plain address.
        for root in ("", "envs/prod/"):
            with self.subTest(root or "."):
                section = _section({root + "main.tf": ('module "me" {\n  source = "./"\n}\n'
                                                       'resource "aws_vpc" "main" {\n  cidr_block = "10.0.0.0/16"\n}\n')})
                self.assertEqual([(v["address"], v["cidr"]) for v in section["vpcs"]],
                                 [("aws_vpc.main", "10.0.0.0/16")])

    def test_wrapper_order_does_not_depend_on_directory_names(self):
        # modules/network sorts before modules/vpc, yet the leaf (the
        # registry module inside modules/vpc) must be finished first.
        files = {"modules/vpc/main.tf": ('module "this" {\n  source = "terraform-aws-modules/vpc/aws"\n'
                                         '  cidr = var.cidr\n}\n'),
                 "modules/network/main.tf": 'module "vpc" {\n  source = "../vpc"\n  cidr = var.vpc_cidr\n}\n',
                 "envs/prod/main.tf": ('module "network" {\n  source = "../../modules/network"\n'
                                       '  vpc_cidr = "10.0.0.0/16"\n}\n'
                                       'module "eks" {\n  source = "terraform-aws-modules/eks/aws"\n'
                                       '  cluster_name = "prod"\n  vpc_id = module.network.vpc_id\n}\n')}
        section = _section(files)
        vpc, = section["vpcs"]
        self.assertEqual((vpc["address"], vpc["cidr"], vpc["cluster_vpc"]), ("module.this", "10.0.0.0/16", True))
        self.assertEqual(section["clusters"][0]["vpc"], "module.this")

    def test_a_spoke_cut_from_the_hubs_output_resolves_whatever_declares_the_hub(self):
        # The spoke's range is cidrsubnet() over the hub's output. It has to
        # resolve whether the hub is the community module, a local module,
        # or a resource in a root that sorts before or after modules/, and
        # whether the instance passes the expression or a local that holds
        # it. Every case: two stated VPCs, no question.
        spoke = 'variable "cidr" {}\nresource "aws_vpc" "this" {\n  cidr_block = var.cidr\n}\n'
        hub_module = 'variable "cidr" {}\nresource "aws_vpc" "this" {\n  cidr_block = var.cidr\n}\n'
        cases = {
            "hub is the community module at the root": {
                "modules/spoke/main.tf": spoke,
                "envs/prod/main.tf": ('module "hub" {\n  source = "terraform-aws-modules/vpc/aws"\n  cidr = "10.0.0.0/16"\n}\n'
                                      'module "spoke" {\n  source = "../../modules/spoke"\n'
                                      '  cidr = cidrsubnet(module.hub.vpc_cidr_block, 2, 1)\n}\n')},
            "hub and spoke are both local modules": {
                "modules/hub/main.tf": hub_module, "modules/spoke/main.tf": spoke,
                "envs/prod/main.tf": ('module "hub" {\n  source = "../../modules/hub"\n  cidr = "10.0.0.0/16"\n}\n'
                                      'module "spoke" {\n  source = "../../modules/spoke"\n'
                                      '  cidr = cidrsubnet(module.hub.vpc_cidr_block, 2, 1)\n}\n')},
            "hub is a resource in a root sorting before modules/": {
                "modules/spoke/main.tf": spoke,
                "envs/prod/main.tf": ('resource "aws_vpc" "hub" {\n  cidr_block = "10.0.0.0/16"\n}\n'
                                      'module "spoke" {\n  source = "../../modules/spoke"\n'
                                      '  cidr = cidrsubnet(aws_vpc.hub.cidr_block, 2, 1)\n}\n')},
            "hub is a resource in a root sorting after modules/": {
                "modules/spoke/main.tf": spoke,
                "stacks/prod/main.tf": ('resource "aws_vpc" "hub" {\n  cidr_block = "10.0.0.0/16"\n}\n'
                                        'module "spoke" {\n  source = "../../modules/spoke"\n'
                                        '  cidr = cidrsubnet(aws_vpc.hub.cidr_block, 2, 1)\n}\n')},
            "the spoke range sits in a local": {
                "modules/spoke/main.tf": spoke,
                "stacks/prod/main.tf": ('resource "aws_vpc" "hub" {\n  cidr_block = "10.0.0.0/16"\n}\n'
                                        'locals {\n  spoke = cidrsubnet(aws_vpc.hub.cidr_block, 2, 1)\n}\n'
                                        'module "spoke" {\n  source = "../../modules/spoke"\n  cidr = local.spoke\n}\n')},
            "the spoke is a resource beside the community module, through a local": {
                "main.tf": ('module "hub" {\n  source = "terraform-aws-modules/vpc/aws"\n  cidr = "10.0.0.0/16"\n}\n'
                            'locals {\n  spoke = cidrsubnet(module.hub.vpc_cidr_block, 2, 1)\n}\n'
                            'resource "aws_vpc" "spoke" {\n  cidr_block = local.spoke\n}\n')},
            "the hub output sits in a local inside the call, spoke file sorting first": {
                "a_spoke.tf": ('locals {\n  hub = aws_vpc.hub.cidr_block\n}\n'
                               'resource "aws_vpc" "spoke" {\n  cidr_block = cidrsubnet(local.hub, 2, 1)\n}\n'),
                "b_hub.tf": 'resource "aws_vpc" "hub" {\n  cidr_block = "10.0.0.0/16"\n}\n'},
            "the hub output sits in a local inside the instance argument": {
                "modules/spoke/main.tf": spoke,
                "stacks/prod/main.tf": ('resource "aws_vpc" "hub" {\n  cidr_block = "10.0.0.0/16"\n}\n'
                                        'locals {\n  hub = aws_vpc.hub.cidr_block\n}\n'
                                        'module "spoke" {\n  source = "../../modules/spoke"\n'
                                        '  cidr = cidrsubnet(local.hub, 2, 1)\n}\n')},
            "the spoke sits two local modules deep, the cut made at the root": {
                "modules/wrapper/main.tf": 'variable "cidr" {}\nmodule "spoke" {\n  source = "../spoke"\n  cidr = var.cidr\n}\n',
                "modules/spoke/main.tf": spoke,
                "stacks/prod/main.tf": ('resource "aws_vpc" "hub" {\n  cidr_block = "10.0.0.0/16"\n}\n'
                                        'module "wrapper" {\n  source = "../../modules/wrapper"\n'
                                        '  cidr = cidrsubnet(aws_vpc.hub.cidr_block, 2, 1)\n}\n')},
        }
        for label, files in cases.items():
            with self.subTest(label):
                section = _section(files)
                self.assertEqual(sorted(v["cidr"] for v in section["vpcs"]), ["10.0.0.0/16", "10.0.64.0/18"])
                self.assertEqual(section["unresolved"], [])

    def test_a_wrapper_that_is_the_vpc_waits_for_the_vpc_inside_it(self):
        # The instance stands for aws_vpc.main inside modules/vpc, and passes
        # the hub's range as another input. Finished before the inner VPC
        # (which waits for the hub), it would record itself as a third VPC
        # and file its subnets under that phantom.
        files = {"modules/vpc/main.tf": ('variable "cidr" {}\nvariable "peer" {}\n'
                                         'resource "aws_vpc" "main" {\n  cidr_block = var.cidr\n}\n'),
                 "main.tf": ('resource "aws_vpc" "hub" {\n  cidr_block = "10.0.0.0/16"\n}\n'
                             'module "vpc" {\n  source = "./modules/vpc"\n  cidr = "10.8.0.0/16"\n'
                             '  peer = aws_vpc.hub.cidr_block\n  private_subnets = ["10.8.1.0/24"]\n}\n')}
        section = _section(files)
        self.assertEqual([(v["address"], v["cidr"]) for v in section["vpcs"]],
                         [("aws_vpc.hub", "10.0.0.0/16"), ("aws_vpc.main", "10.8.0.0/16")])
        self.assertEqual([(s["cidr"], s["vpc"]) for s in section["subnets"]], [("10.8.1.0/24", "aws_vpc.main")])
        self.assertEqual(section["unresolved"], [])

    def test_two_copies_through_two_single_instance_wrappers_stay_two_entries(self):
        # modules/net once through modules/blue and once through
        # modules/green, each instantiated once from the root: two VPCs,
        # each the VPC of the cluster beside it. And the same module used
        # once at the root and once inside a wrapper.
        net = 'variable "cidr" {}\nresource "aws_vpc" "main" {\n  cidr_block = var.cidr\n}\n'
        def wrapper(name, cidr):
            return (f'module "net" {{\n  source = "../net"\n  cidr = "{cidr}"\n}}\n'
                    f'module "eks" {{\n  source = "terraform-aws-modules/eks/aws"\n'
                    f'  cluster_name = "{name}"\n  vpc_id = module.net.vpc_id\n}}\n')
        files = {"modules/net/main.tf": net,
                 "modules/blue/main.tf": wrapper("blue", "10.1.0.0/16"),
                 "modules/green/main.tf": wrapper("green", "10.2.0.0/16"),
                 "main.tf": 'module "blue" {\n  source = "./modules/blue"\n}\nmodule "green" {\n  source = "./modules/green"\n}\n'}
        section = _section(files)
        self.assertEqual(sorted((v["cidr"], v["cluster_vpc"]) for v in section["vpcs"]),
                         [("10.1.0.0/16", True), ("10.2.0.0/16", True)])
        self.assertEqual({c["vpc"] for c in section["clusters"]}, {"module.net.aws_vpc.main"})
        files = {"modules/net/main.tf": net,
                 "modules/blue/main.tf": 'module "net" {\n  source = "../net"\n  cidr = "10.2.0.0/16"\n}\n',
                 "main.tf": ('module "net" {\n  source = "./modules/net"\n  cidr = "10.1.0.0/16"\n}\n'
                             'module "blue" {\n  source = "./modules/blue"\n}\n')}
        section = _section(files)
        self.assertEqual(sorted(v["cidr"] for v in section["vpcs"]), ["10.1.0.0/16", "10.2.0.0/16"])

    def test_a_wrapper_named_vpc_whose_vpc_sits_two_modules_down_is_one_entry(self):
        # modules/vpc declares no VPC itself; the module it instantiates
        # does. The instance stands for that inner VPC all the same, so it
        # must not record itself beside it as a second VPC at the same
        # range, marked as not the cluster's.
        files = {"modules/base/main.tf": 'variable "cidr" {}\nresource "aws_vpc" "this" {\n  cidr_block = var.cidr\n}\n',
                 "modules/vpc/main.tf": 'variable "cidr" {}\nmodule "core" {\n  source = "../base"\n  cidr = var.cidr\n}\n',
                 "main.tf": ('module "vpc" {\n  source = "./modules/vpc"\n  cidr = "10.0.0.0/16"\n}\n'
                             'resource "aws_eks_cluster" "c" {\n  name = "c"\n'
                             '  vpc_config {\n    subnet_ids = module.vpc.private_subnets\n  }\n}\n')}
        section = _section(files)
        self.assertEqual([(v["address"], v["cidr"], v["cluster_vpc"]) for v in section["vpcs"]],
                         [("aws_vpc.this", "10.0.0.0/16", True)])
        self.assertEqual(section["clusters"][0]["vpc"], "aws_vpc.this")

    def test_a_wrapper_two_modules_above_the_cluster_is_not_a_second_cluster(self):
        # The platform wrapper passes the VPC, the subnets and the service
        # range down two hops. One cluster in the estate, one in the section.
        files = {"modules/platform/cluster/main.tf": (
                     'variable "vpc_id" {}\nvariable "subnet_ids" {}\nvariable "service_cidr" {}\n'
                     'resource "aws_eks_cluster" "this" {\n  name = "c"\n'
                     '  vpc_config {\n    subnet_ids = var.subnet_ids\n  }\n'
                     '  kubernetes_network_config {\n    service_ipv4_cidr = var.service_cidr\n  }\n}\n'),
                 "modules/platform/main.tf": (
                     'variable "vpc_id" {}\nvariable "subnet_ids" {}\nvariable "cluster_service_ipv4_cidr" {}\n'
                     'module "cluster" {\n  source = "./cluster"\n  vpc_id = var.vpc_id\n'
                     '  subnet_ids = var.subnet_ids\n  service_cidr = var.cluster_service_ipv4_cidr\n}\n'),
                 "main.tf": ('resource "aws_vpc" "main" {\n  cidr_block = "10.0.0.0/16"\n}\n'
                             'resource "aws_subnet" "a" {\n  vpc_id = aws_vpc.main.id\n  cidr_block = "10.0.1.0/24"\n}\n'
                             'module "platform" {\n  source = "./modules/platform"\n  vpc_id = aws_vpc.main.id\n'
                             '  subnet_ids = [aws_subnet.a.id]\n  cluster_service_ipv4_cidr = "172.20.0.0/16"\n}\n')}
        section = _section(files)
        self.assertEqual([(c["address"], c["service_ipv4_cidr"], c["vpc"], c["vpc_path"]) for c in section["clusters"]],
                         [("aws_eks_cluster.this", "172.20.0.0/16", "aws_vpc.main", "main.tf")])

    def test_a_git_module_of_any_name_with_a_cidr_argument_is_the_vpc(self):
        tf = 'module "net" {\n  source = "git::https://example.com/tf-modules.git//network?ref=v1"\n  cidr = "10.0.0.0/16"\n}\n'
        section = _section({"main.tf": tf})
        self.assertEqual([(v["address"], v["cidr"]) for v in section["vpcs"]], [("module.net", "10.0.0.0/16")])

    def test_the_v20_spelling_of_the_remote_network_block_is_read(self):
        # v20 of the EKS module prefixes the block with `cluster_`; a hybrid
        # estate's on-prem ranges must not vanish over a spelling.
        tf = ('module "k" {\n  source = "git::https://example.com/k8s.git?ref=v1"\n'
              '  cluster_remote_network_config = {\n'
              '    remote_node_networks = {\n      cidrs = ["10.50.0.0/16"]\n    }\n'
              '    remote_pod_networks = {\n      cidrs = ["10.51.0.0/16"]\n    }\n  }\n}\n')
        cluster, = _section({"main.tf": tf})["clusters"]
        self.assertEqual((cluster["remote_node_cidrs"], cluster["remote_pod_cidrs"]),
                         (["10.50.0.0/16"], ["10.51.0.0/16"]))

    def test_a_one_line_object_keeps_every_member(self):
        tf = ('module "k" {\n  source = "git::https://example.com/k8s.git?ref=v1"\n'
              '  remote_network_config = { remote_node_networks = { cidrs = ["10.50.0.0/16"] }, '
              'remote_pod_networks = { cidrs = ["10.51.0.0/16"] } }\n}\n')
        cluster, = _section({"main.tf": tf})["clusters"]
        self.assertEqual((cluster["remote_node_cidrs"], cluster["remote_pod_cidrs"]),
                         (["10.50.0.0/16"], ["10.51.0.0/16"]))

    def test_a_module_with_a_remote_network_config_alone_is_a_cluster(self):
        tf = ('module "k" {\n  source = "git::https://example.com/k8s.git?ref=v1"\n'
              '  remote_network_config {\n'
              '    remote_node_networks {\n      cidrs = ["10.50.0.0/16"]\n    }\n'
              '    remote_pod_networks {\n      cidrs = ["10.51.0.0/16"]\n    }\n  }\n}\n')
        section = _section({"main.tf": tf})
        cluster, = section["clusters"]
        self.assertEqual((cluster["address"], cluster["remote_node_cidrs"], cluster["remote_pod_cidrs"]),
                         ("module.k", ["10.50.0.0/16"], ["10.51.0.0/16"]))

    def test_a_modules_own_range_survives_an_argument_that_never_resolves(self):
        # The hub's range comes from an IPAM pool, so `peer` never resolves
        # and the instance's scope is rebuilt on every call; the range its
        # own VPC registered must still be there for the route beside it.
        files = {"modules/net/main.tf": ('variable "cidr" {}\nvariable "peer" {}\n'
                                         'resource "aws_vpc" "main" {\n  cidr_block = var.cidr\n}\n'
                                         'resource "aws_route" "r" {\n'
                                         '  destination_cidr_block    = cidrsubnet(aws_vpc.main.cidr_block, 8, 200)\n'
                                         '  vpc_peering_connection_id = "pcx-1"\n}\n'),
                 "main.tf": ('resource "aws_vpc" "hub" {\n  ipv4_ipam_pool_id = "ipam-pool-1"\n}\n'
                             'module "net" {\n  source = "./modules/net"\n  cidr = "10.8.0.0/16"\n'
                             '  peer = aws_vpc.hub.cidr_block\n}\n')}
        section = _section(files)
        self.assertEqual([(r["destination"], r["via"]) for r in section["routes"]],
                         [("10.8.200.0/24", "vpc_peering_connection")])
        self.assertEqual(section["unresolved"], [])

    def test_two_vpcs_cut_from_each_others_output_are_questions_not_a_hang(self):
        module = 'variable "cidr" {}\nresource "aws_vpc" "this" {\n  cidr_block = var.cidr\n}\n'
        files = {"modules/a/main.tf": module, "modules/b/main.tf": module,
                 "main.tf": ('module "a" {\n  source = "./modules/a"\n  cidr = cidrsubnet(module.b.vpc_cidr_block, 1, 0)\n}\n'
                             'module "b" {\n  source = "./modules/b"\n  cidr = cidrsubnet(module.a.vpc_cidr_block, 1, 1)\n}\n')}
        section = _section(files)
        self.assertEqual([v["cidr"] for v in section["vpcs"]], [None, None])
        self.assertEqual(len(section["unresolved"]), 2)

    def test_two_directories_instantiating_each_other_do_not_hang(self):
        files = {"modules/a/main.tf": ('module "b" {\n  source = "../b"\n}\n'
                                       'resource "aws_vpc" "a" {\n  cidr_block = "10.1.0.0/16"\n}\n'),
                 "modules/b/main.tf": 'module "a" {\n  source = "../a"\n}\n'}
        section = _section(files)
        self.assertEqual(section["vpcs"][0]["cidr"], "10.1.0.0/16")

    def test_a_local_wrapper_around_the_community_vpc_module(self):
        # The round-5 case: the wrapper passes the lists through, so every
        # range is stated, and the cluster's VPC is the wrapper's inner module.
        files = {"modules/network/main.tf": ('module "vpc" {\n  source = "terraform-aws-modules/vpc/aws"\n'
                                             '  cidr            = var.vpc_cidr\n'
                                             '  private_subnets = var.private_subnets\n'
                                             '  secondary_cidr_blocks = var.secondary\n}\n'),
                 "envs/prod/main.tf": ('module "network" {\n  source = "../../modules/network"\n'
                                       '  vpc_cidr        = "10.0.0.0/16"\n'
                                       '  private_subnets = ["10.0.0.0/19", "10.0.32.0/19"]\n'
                                       '  secondary       = ["100.64.0.0/16"]\n}\n'
                                       'module "eks" {\n  source = "terraform-aws-modules/eks/aws"\n'
                                       '  cluster_name = "prod"\n  vpc_id = module.network.vpc_id\n'
                                       '  subnet_ids = module.network.private_subnets\n}\n')}
        section = _section(files)
        vpc, = section["vpcs"]
        self.assertEqual((vpc["address"], vpc["form"], vpc["cidr"], vpc["secondary_cidrs"], vpc["cluster_vpc"]),
                         ("module.vpc", "module", "10.0.0.0/16", ["100.64.0.0/16"], True))
        self.assertEqual([(s["cidr"], s["vpc"]) for s in section["subnets"]],
                         [("10.0.0.0/19", "module.vpc"), ("10.0.32.0/19", "module.vpc")])
        self.assertEqual(section["clusters"][0]["vpc"], "module.vpc")
        self.assertEqual(section["unresolved"], [])

    def test_an_unresolved_list_at_a_wrapper_instance_is_filed_under_the_inner_vpc(self):
        files = {"modules/vpc/main.tf": 'resource "aws_vpc" "main" {\n  cidr_block = var.cidr\n}\n',
                 "envs/prod/main.tf": ('module "vpc" {\n  source = "../../modules/vpc"\n'
                                       '  cidr = "10.0.0.0/16"\n'
                                       '  private_subnets = [for k, v in local.azs : cidrsubnet("10.0.0.0/16", 3, k)]\n}\n')}
        section = _section(files)
        entry, = section["unresolved"]
        self.assertEqual((entry["address"], entry["path"], entry["argument"]),
                         ("aws_vpc.main", "modules/vpc/main.tf", "private_subnets"))

    def test_old_style_interpolation_resolves(self):
        tf = ('variable "cidr" {\n  default = "10.0.0.0/16"\n}\n'
              'resource "aws_vpc" "main" {\n  cidr_block = "${var.cidr}"\n}\n')
        self.assertEqual(_section({"main.tf": tf})["vpcs"][0]["cidr"], "10.0.0.0/16")

    def test_an_unresolved_secondary_list_at_a_wrapper_is_filed_once(self):
        files = {"modules/network/main.tf": ('module "vpc" {\n  source = "terraform-aws-modules/vpc/aws"\n'
                                             '  cidr = "10.0.0.0/16"\n'
                                             '  secondary_cidr_blocks = var.secondary_cidr_blocks\n}\n'),
                 "envs/prod/main.tf": ('module "network" {\n  source = "../../modules/network"\n'
                                       '  secondary_cidr_blocks = var.secondary_cidr_blocks\n}\n')}
        section = _section(files)
        self.assertEqual(len(section["unresolved"]), 1)
        self.assertEqual(section["unresolved"][0]["argument"], "secondary_cidr_blocks")

    def test_a_vpc_module_without_a_cidr_argument_says_why(self):
        tf = 'module "vpc" {\n  source = "cloudposse/vpc/aws"\n  ipv4_primary_cidr_block = "10.0.0.0/16"\n}\n'
        vpc, = _section({"main.tf": tf})["vpcs"]
        self.assertIsNone(vpc["cidr"])
        self.assertIn("passes no `cidr`", vpc["evidence"][0])

    def test_a_wrapper_named_vpc_does_not_repeat_the_inner_modules_subnets(self):
        files = {"modules/vpc/main.tf": ('module "this" {\n  source = "terraform-aws-modules/vpc/aws"\n'
                                         '  cidr = var.cidr\n  private_subnets = var.private_subnets\n}\n'),
                 "envs/prod/main.tf": ('module "vpc" {\n  source = "../../modules/vpc"\n'
                                       '  cidr = "10.0.0.0/16"\n  private_subnets = ["10.0.0.0/19"]\n}\n')}
        section = _section(files)
        self.assertEqual([(s["name"], s["cidr"]) for s in section["subnets"]],
                         [("this/private_subnets[0]", "10.0.0.0/19")])

    def test_a_subnet_vpc_input_is_followed_through_the_instance(self):
        files = {"modules/cluster/main.tf": ('resource "aws_subnet" "a" {\n  vpc_id = var.vpc_id\n'
                                             '  cidr_block = "10.0.1.0/24"\n}\n'
                                             'resource "aws_eks_cluster" "c" {\n  name = "c"\n'
                                             '  vpc_config {\n    subnet_ids = [aws_subnet.a.id]\n  }\n}\n'),
                 "envs/prod/main.tf": ('resource "aws_vpc" "main" {\n  cidr_block = "10.0.0.0/16"\n}\n'
                                       'module "cluster" {\n  source = "../../modules/cluster"\n'
                                       '  vpc_id = aws_vpc.main.id\n}\n')}
        section = _section(files)
        self.assertEqual(section["subnets"][0]["vpc"], "aws_vpc.main")
        self.assertEqual(section["clusters"][0]["vpc"], "aws_vpc.main")
        self.assertTrue(section["vpcs"][0]["cluster_vpc"])

    def test_instance_secondary_blocks_attach_to_the_inner_vpc(self):
        files = {"modules/vpc/main.tf": 'resource "aws_vpc" "main" {\n  cidr_block = "10.0.0.0/16"\n}\n',
                 "envs/prod/main.tf": ('module "vpc" {\n  source = "../../modules/vpc"\n'
                                       '  secondary_cidr_blocks = ["100.64.0.0/16"]\n}\n')}
        vpc, = _section(files)["vpcs"]
        self.assertEqual((vpc["address"], vpc["secondary_cidrs"]), ("aws_vpc.main", ["100.64.0.0/16"]))

    def test_a_literal_inside_a_local_module_is_still_recorded(self):
        files = {"modules/vpc/main.tf": 'resource "aws_vpc" "main" {\n  cidr_block = "10.5.0.0/16"\n}\n',
                 "envs/prod/main.tf": 'module "vpc" {\n  source = "../../modules/vpc"\n}\n'}
        section, notes = _harvest(files)
        self.assertEqual([(v["address"], v["cidr"]) for v in section["vpcs"]],
                         [("aws_vpc.main", "10.5.0.0/16")])
        # No input was consulted, so no note about the module.
        self.assertFalse(any("a local module" in n for n in notes))

    def test_git_sourced_community_modules_are_recognised(self):
        tf = ('module "vpc" {\n'
              '  source = "git::https://github.com/terraform-aws-modules/terraform-aws-vpc.git?ref=v5.0.0"\n'
              '  cidr   = "10.0.0.0/16"\n}\n'
              'module "eks" {\n'
              '  source       = "github.com/terraform-aws-modules/terraform-aws-eks"\n'
              '  cluster_name = "git"\n'
              '  vpc_id       = module.vpc.vpc_id\n}\n')
        section = _section({"main.tf": tf})
        self.assertEqual([c["name"] for c in section["clusters"]], ["git"])
        self.assertTrue(section["vpcs"][0]["cluster_vpc"])

    def test_string_interpolation_is_not_a_literal(self):
        tf = ('variable "octet" {\n  default = "42"\n}\n'
              'resource "aws_vpc" "main" {\n  cidr_block = "10.${var.octet}.0.0/16"\n}\n')
        section = _section({"main.tf": tf})
        self.assertIsNone(section["vpcs"][0]["cidr"])
        self.assertEqual(section["unresolved"][0]["expression"], '"10.${var.octet}.0.0/16"')

    def test_unterminated_block_is_noted(self):
        _, notes = _harvest({"main.tf": 'resource "aws_vpc" "main" {\n  cidr_block = "10.0.0.0/16"\n'})
        self.assertIn("main.tf: a block is never closed, so that block and everything after it "
                      "were not read", notes)


EKSCTL_CLUSTER = (
    "apiVersion: eksctl.io/v1alpha5\n"
    "kind: ClusterConfig\n"
    "metadata:\n"
    "  name: cluster-4\n"
    "  region: eu-north-1\n"
    "vpc:\n"
    '  cidr: "192.168.0.0/16"\n'
    '  publicAccessCIDRs: ["1.1.1.1/32"]\n'
    "  subnets:\n"
    "    private:\n"
    "      eu-north-1a:\n"
    '        id: "subnet-0b25"\n'
    '        cidr: "192.168.128.0/19"\n'
    "      eu-north-1b:\n"
    '        cidr: "192.168.64.0/19"\n'
    "    public:\n"
    "      public-one:\n"
    '        id: "subnet-0324"\n'
    "kubernetesNetworkConfig:\n"
    "  serviceIPv4CIDR: 10.100.0.0/16\n"
    "  ipFamily: IPv4\n"
    "remoteNetworkConfig:\n"
    "  remoteNodeNetworks:\n"
    '    - cidrs: ["10.80.146.0/24"]\n'
    "  remotePodNetworks:\n"
    '    - cidrs: ["10.86.30.0/23"]\n'
    "nodeGroups:\n"
    "  - name: ng-1\n"
    "    instanceType: m5.xlarge\n")


class EksctlTest(unittest.TestCase):

    def test_cluster_config_is_read_in_full(self):
        section = _section({"cluster.yaml": EKSCTL_CLUSTER})
        vpc, = section["vpcs"]
        self.assertEqual((vpc["name"], vpc["form"], vpc["cidr"], vpc["cluster_vpc"]),
                         ("cluster-4", "eksctl", "192.168.0.0/16", True))
        self.assertNotIn("defaulted", vpc)
        subnets = {s["name"]: (s["cidr"], s["tier"], s["availability_zone"]) for s in section["subnets"]}
        self.assertEqual(subnets, {
            "eu-north-1a": ("192.168.128.0/19", "private", "eu-north-1a"),
            "eu-north-1b": ("192.168.64.0/19", "private", "eu-north-1b"),
            "public-one": (None, "public", None),
        })
        cluster, = section["clusters"]
        self.assertEqual(cluster["name"], "cluster-4")
        self.assertEqual(cluster["vpc"], vpc["address"])
        self.assertEqual(cluster["service_ipv4_cidr"], "10.100.0.0/16")
        self.assertEqual(cluster["ip_family"], "ipv4")
        self.assertEqual(cluster["public_access_cidrs"], ["1.1.1.1/32"])
        self.assertEqual(cluster["remote_node_cidrs"], ["10.80.146.0/24"])
        self.assertEqual(cluster["remote_pod_cidrs"], ["10.86.30.0/23"])
        self.assertEqual(section["unresolved"], [])

    def test_subnet_keys_that_are_zone_codes_of_any_kind_become_the_zone(self):
        # An Availability Zone, a GovCloud zone, a Local Zone and a
        # Wavelength Zone are zone codes; a plain name is not.
        doc = ("apiVersion: eksctl.io/v1alpha5\nkind: ClusterConfig\n"
               "metadata:\n  name: simple\n  region: us-west-2\n"
               "vpc:\n  cidr: 10.7.0.0/16\n  subnets:\n    private:\n"
               "      us-west-2a:\n        cidr: 10.7.1.0/24\n"
               "      us-gov-west-1a:\n        cidr: 10.7.2.0/24\n"
               "      us-west-2-lax-1a:\n        cidr: 10.7.3.0/24\n"
               "      us-east-1-wl1-bos-wlz-1:\n        cidr: 10.7.4.0/24\n"
               "      private-a:\n        cidr: 10.7.5.0/24\n"
               "      us-east-1-private:\n        cidr: 10.7.6.0/24\n"
               "      us-east-1-foe-wlz-1a:\n        cidr: 10.7.7.0/24\n")
        section = _section({"c.yaml": doc})
        self.assertEqual([(s["name"], s["availability_zone"]) for s in section["subnets"]],
                         [("us-west-2a", "us-west-2a"), ("us-gov-west-1a", "us-gov-west-1a"),
                          ("us-west-2-lax-1a", "us-west-2-lax-1a"),
                          ("us-east-1-wl1-bos-wlz-1", "us-east-1-wl1-bos-wlz-1"), ("private-a", None),
                          ("us-east-1-private", None), ("us-east-1-foe-wlz-1a", "us-east-1-foe-wlz-1a")])

    def test_extra_cidrs_are_the_vpcs_secondary_ranges(self):
        head = ("apiVersion: eksctl.io/v1alpha5\nkind: ClusterConfig\n"
                "metadata:\n  name: simple\n  region: us-west-2\n")
        vpc, = _section({"c.yaml": head + "vpc:\n  cidr: 10.0.0.0/16\n  extraCIDRs: [\"100.64.0.0/16\"]\n"})["vpcs"]
        self.assertEqual(vpc["secondary_cidrs"], ["100.64.0.0/16"])
        self.assertIn("c.yaml: vpc.extraCIDRs = ['100.64.0.0/16']", vpc["evidence"])
        # A placeholder among them: a question under the VPC, and a subnet
        # placeholder beside it is no longer covered by the stated primary.
        section = _section({"c.yaml": head + "vpc:\n  cidr: 10.0.0.0/16\n  extraCIDRs: [\"${POD_CIDR}\"]\n"
                            "  subnets:\n    private:\n      us-west-2a:\n        cidr: ${SUBNET_A}\n"})
        self.assertEqual([(e["argument"]) for e in section["unresolved"]], ["secondary_cidr_blocks", "cidr"])
        blocking, covered = addressspace.triage_unresolved(section)
        self.assertEqual((len(blocking), covered), (2, []))

    def test_a_subnet_spec_with_neither_cidr_nor_id_is_skipped(self):
        # eksctl then picks a subnet in that zone from an existing VPC;
        # nothing here states a range, and an empty `cidr:` is the same
        # spec. Recorded, it would carry evidence about a placeholder that
        # does not exist.
        doc = ("apiVersion: eksctl.io/v1alpha5\nkind: ClusterConfig\n"
               "metadata:\n  name: simple\n  region: us-west-2\n"
               "vpc:\n  cidr: 10.7.0.0/16\n  subnets:\n    private:\n      us-west-2a:\n        az: us-west-2a\n"
               "      us-west-2c:\n        cidr:\n"
               "      us-west-2b:\n        cidr: 10.7.1.0/24\n")
        section = _section({"c.yaml": doc})
        self.assertEqual([(s["name"], s["cidr"]) for s in section["subnets"]], [("us-west-2b", "10.7.1.0/24")])
        self.assertEqual(section["unresolved"], [])

    def test_eksctl_placeholder_subnets_are_settled_by_the_vpcs_own_question(self):
        # Only the same-place rule decides an eksctl subnet (the carrier
        # fallback is Terraform-only): the VPC's placeholder is the one
        # question, the two subnet placeholders ride on its answer.
        doc = ("apiVersion: eksctl.io/v1alpha5\nkind: ClusterConfig\n"
               "metadata:\n  name: simple\n  region: us-west-2\n"
               "vpc:\n  cidr: ${VPC_CIDR}\n  subnets:\n    private:\n"
               "      us-west-2a:\n        cidr: ${SUBNET_A}\n      us-west-2b:\n        cidr: ${SUBNET_B}\n")
        section = _section({"c.yaml": doc})
        blocking, covered = addressspace.triage_unresolved(section)
        self.assertEqual([e["argument"] for e in blocking], ["vpc.cidr"])
        self.assertEqual([e["argument"] for e in covered], ["cidr", "cidr"])

    def test_default_vpc_when_neither_cidr_nor_id_is_declared(self):
        doc = ("apiVersion: eksctl.io/v1alpha5\nkind: ClusterConfig\n"
               "metadata:\n  name: simple\n  region: us-west-2\n"
               "nodeGroups:\n  - name: ng\n")
        vpc, = _section({"c.yaml": doc})["vpcs"]
        self.assertEqual(vpc["cidr"], "192.168.0.0/16")
        self.assertTrue(vpc["defaulted"])
        self.assertIn("eksctl creates the VPC with its default", vpc["evidence"][0])

    def test_an_empty_vpc_cidr_key_is_the_default_and_a_placeholder_is_not(self):
        # `vpc:\n  cidr:` is the key with no value: unset to eksctl, so the
        # default. A placeholder is a value the file does not state: a
        # question, never the default.
        head = ("apiVersion: eksctl.io/v1alpha5\nkind: ClusterConfig\n"
                "metadata:\n  name: simple\n  region: us-west-2\n")
        vpc, = _section({"c.yaml": head + "vpc:\n  cidr:\n"})["vpcs"]
        self.assertEqual((vpc["cidr"], vpc.get("defaulted")), ("192.168.0.0/16", True))
        section = _section({"c.yaml": head + "vpc:\n  cidr: ${VPC_CIDR}\n"})
        vpc, = section["vpcs"]
        self.assertEqual((vpc["cidr"], vpc.get("defaulted")), (None, None))
        self.assertEqual([e["argument"] for e in section["unresolved"]], ["vpc.cidr"])

    def test_existing_vpc_by_id_has_no_cidr_and_no_default(self):
        doc = ("apiVersion: eksctl.io/v1alpha5\nkind: ClusterConfig\n"
               "metadata:\n  name: outpost\n"
               "vpc:\n  id: vpc-1234\n")
        section = _section({"c.yaml": doc})
        vpc, = section["vpcs"]
        self.assertIsNone(vpc["cidr"])
        self.assertNotIn("defaulted", vpc)
        self.assertIn("vpc.id = vpc-1234", vpc["evidence"][0])
        self.assertEqual(section["unresolved"], [])

    def test_subnets_named_by_id_mean_an_existing_vpc_not_the_default(self):
        # eksctl's 04-existing-vpc shape without the optional vpc.id: the
        # subnets are imported by id, so no VPC is created and the default
        # CIDR must not be recorded as a source range.
        doc = ("apiVersion: eksctl.io/v1alpha5\nkind: ClusterConfig\n"
               "metadata:\n  name: existing\n"
               "vpc:\n  subnets:\n    private:\n      eu-north-1a:\n        id: subnet-0b25\n")
        section = _section({"c.yaml": doc})
        vpc, = section["vpcs"]
        self.assertIsNone(vpc["cidr"])
        self.assertNotIn("defaulted", vpc)
        self.assertIn("subnets are named by id", vpc["evidence"][0])

    def test_environment_placeholders(self):
        doc = ("apiVersion: eksctl.io/v1alpha5\nkind: ClusterConfig\n"
               "metadata:\n  name: ${EKS_CLUSTER_NAME}\n  region: ${AWS_REGION}\n"
               "vpc:\n  cidr: ${VPC_CIDR}\n")
        section = _section({"c.yaml": doc})
        vpc, = section["vpcs"]
        # A placeholder name falls back to the file; a placeholder range is
        # unresolved; the name itself is not a range and is not listed.
        self.assertEqual((vpc["name"], vpc["cidr"]), ("c", None))
        self.assertIsNone(section["clusters"][0]["name"])
        self.assertEqual([(u["argument"], u["expression"]) for u in section["unresolved"]],
                         [("vpc.cidr", "${VPC_CIDR}")])

    def test_a_malformed_subnet_group_does_not_take_the_section_down(self):
        doc = ("apiVersion: eksctl.io/v1alpha5\nkind: ClusterConfig\n"
               "metadata:\n  name: odd\n"
               "vpc:\n  subnets:\n    private:\n      - subnet-1\n")
        vpc, = _section({"c.yaml": doc})["vpcs"]
        self.assertEqual(vpc["cidr"], "192.168.0.0/16")

    def test_a_placeholder_subnet_cidr_keeps_the_subnet(self):
        doc = ("apiVersion: eksctl.io/v1alpha5\nkind: ClusterConfig\n"
               "metadata:\n  name: c\n"
               "vpc:\n  cidr: 10.0.0.0/16\n  subnets:\n    private:\n      eu-north-1a:\n        cidr: ${SUBNET_A}\n")
        section = _section({"c.yaml": doc})
        subnet, = section["subnets"]
        self.assertEqual((subnet["cidr"], subnet["vpc"]), (None, section["vpcs"][0]["address"]))
        self.assertEqual(section["unresolved"][0]["argument"], "cidr")

    def test_other_cluster_configs_are_ignored(self):
        doc = ("apiVersion: kind.x-k8s.io/v1alpha4\nkind: ClusterConfig\n"
               "metadata:\n  name: kind\nvpc:\n  cidr: 10.0.0.0/16\n")
        self.assertEqual(_section({"kind.yaml": doc}), {})

    def test_chart_roots_are_skipped_with_a_note(self):
        files = {"charts/app/templates/c.yaml": "kind: ClusterConfig\napiVersion: eksctl.io/v1alpha5\n{{ .Values.x }}\n"}
        section, notes = _harvest(files, chart_roots=["charts/app"])
        self.assertEqual(section, {})
        self.assertIn("1 YAML file(s) under Helm chart roots (charts/app) were not read for the "
                      "network address space: chart templates are not parsed, and no rendered "
                      "form of them is read either", notes)


CFN_TEMPLATE = (
    "AWSTemplateFormatVersion: '2010-09-09'\n"
    "Parameters:\n"
    "  VpcCidr:\n"
    "    Type: String\n"
    "    Default: 10.30.0.0/16\n"
    "  PodCidr:\n"
    "    Type: String\n"
    "Resources:\n"
    "  VPC:\n"
    "    Type: AWS::EC2::VPC\n"
    "    Properties:\n"
    "      CidrBlock: !Ref VpcCidr\n"
    "  PodRange:\n"
    "    Type: AWS::EC2::VPCCidrBlock\n"
    "    Properties:\n"
    "      VpcId: !Ref VPC\n"
    "      CidrBlock: 100.64.0.0/16\n"
    "  PrivateSubnetA:\n"
    "    Type: AWS::EC2::Subnet\n"
    "    Properties:\n"
    "      VpcId: !Ref VPC\n"
    "      CidrBlock: 10.30.0.0/19\n"
    "      AvailabilityZone: !Select [0, !GetAZs '']\n"
    "      Tags:\n"
    "        - Key: kubernetes.io/role/internal-elb\n"
    "          Value: '1'\n"
    "  PublicSubnetA:\n"
    "    Type: AWS::EC2::Subnet\n"
    "    Properties:\n"
    "      VpcId: !Ref VPC\n"
    "      CidrBlock: !Sub '${PodCidr}'\n"
    "      MapPublicIpOnLaunch: true\n"
    "  Cluster:\n"
    "    Type: AWS::EKS::Cluster\n"
    "    Properties:\n"
    "      Name: cfn-cluster\n"
    "      ResourcesVpcConfig:\n"
    "        SubnetIds:\n"
    "          - !Ref PrivateSubnetA\n"
    "        PublicAccessCidrs: [203.0.113.0/24]\n"
    "      KubernetesNetworkConfig:\n"
    "        ServiceIpv4Cidr: 172.20.0.0/16\n"
    "  PeerRoute:\n"
    "    Type: AWS::EC2::Route\n"
    "    Properties:\n"
    "      RouteTableId: !Ref RouteTable\n"
    "      DestinationCidrBlock: 10.20.0.0/16\n"
    "      VpcPeeringConnectionId: pcx-1234\n")


class CloudFormationTest(unittest.TestCase):

    def test_yaml_template_with_short_form_intrinsics(self):
        section = _section({"vpc.yaml": CFN_TEMPLATE})
        vpc, = section["vpcs"]
        self.assertEqual((vpc["address"], vpc["form"], vpc["cidr"], vpc["secondary_cidrs"], vpc["cluster_vpc"]),
                         ("VPC", "cloudformation", "10.30.0.0/16", ["100.64.0.0/16"], True))
        private, public = section["subnets"]
        self.assertEqual((private["address"], private["cidr"], private["tier"], private["availability_zone"], private["vpc"]),
                         ("PrivateSubnetA", "10.30.0.0/19", "private", None, "VPC"))
        # The !Sub subnet stays, without a range, so the cluster link survives.
        self.assertEqual((public["address"], public["cidr"], public["tier"], public["vpc"]),
                         ("PublicSubnetA", None, "public", "VPC"))
        cluster, = section["clusters"]
        self.assertEqual((cluster["name"], cluster["vpc"], cluster["subnets"], cluster["service_ipv4_cidr"],
                          cluster["public_access_cidrs"]),
                         ("cfn-cluster", "VPC", ["PrivateSubnetA"], "172.20.0.0/16", ["203.0.113.0/24"]))
        route, = section["routes"]
        self.assertEqual((route["destination"], route["via"]), ("10.20.0.0/16", "vpc_peering_connection"))
        # The !Sub range is an intrinsic the scan does not evaluate.
        unresolved, = section["unresolved"]
        self.assertEqual((unresolved["address"], unresolved["argument"]), ("PublicSubnetA", "CidrBlock"))
        self.assertIn("Fn::Sub", unresolved["expression"])

    def test_two_templates_in_one_directory_keep_their_vpcs_apart(self):
        vpc_only = ("Resources:\n  VPC:\n    Type: AWS::EC2::VPC\n    Properties:\n      CidrBlock: 10.9.0.0/16\n")
        section = _section({"cfn/dev.yaml": vpc_only, "cfn/prod.yaml": CFN_TEMPLATE})
        by_path = {v["path"]: v["cluster_vpc"] for v in section["vpcs"]}
        self.assertEqual(by_path, {"cfn/dev.yaml": False, "cfn/prod.yaml": True})

    def test_a_secondary_range_on_a_vpc_declared_elsewhere_is_kept(self):
        template = ("Parameters:\n  ExistingVpcId:\n    Type: AWS::EC2::VPC::Id\n"
                    "Resources:\n  Pods:\n    Type: AWS::EC2::VPCCidrBlock\n    Properties:\n"
                    "      VpcId: !Ref ExistingVpcId\n      CidrBlock: 100.64.0.0/16\n")
        vpc, = _section({"t.yaml": template})["vpcs"]
        self.assertEqual((vpc["address"], vpc["cidr"], vpc["secondary_cidrs"]),
                         ("ExistingVpcId", None, ["100.64.0.0/16"]))
        self.assertIn("not declared in this template", vpc["evidence"][0])

    def test_a_literal_that_is_not_a_cidr_is_a_question(self):
        template = ("Parameters:\n  Extra:\n    Type: String\n    Default: ''\n"
                    "Resources:\n  VPC:\n    Type: AWS::EC2::VPC\n    Properties:\n      CidrBlock: !Ref Extra\n")
        section = _section({"t.yaml": template})
        self.assertIsNone(section["vpcs"][0]["cidr"])
        self.assertEqual([(u["argument"], u["expression"]) for u in section["unresolved"]],
                         [("CidrBlock", '""')])

    def test_a_cloudformation_prefix_list_route_is_a_question(self):
        template = ("Resources:\n  R:\n    Type: AWS::EC2::Route\n    Properties:\n"
                    "      RouteTableId: rtb-1\n      DestinationPrefixListId: pl-0123\n      TransitGatewayId: tgw-1\n")
        section = _section({"t.yaml": template})
        self.assertEqual(section["unresolved"][0]["argument"], "DestinationPrefixListId")

    def test_json_template(self):
        template = ('{"AWSTemplateFormatVersion": "2010-09-09", "Resources": {'
                    '"VPC": {"Type": "AWS::EC2::VPC", "Properties": {"CidrBlock": "10.31.0.0/16"}}}}')
        vpc, = _section({"stack.json": template})["vpcs"]
        self.assertEqual(vpc["cidr"], "10.31.0.0/16")

    def test_an_intrinsic_cluster_name_is_not_a_question(self):
        template = ("Parameters:\n  ClusterName:\n    Type: String\n"
                    "Resources:\n  Cluster:\n    Type: AWS::EKS::Cluster\n    Properties:\n"
                    "      Name: !Sub '${AWS::StackName}-eks'\n"
                    "  Other:\n    Type: AWS::EKS::Cluster\n    Properties:\n      Name: !Ref ClusterName\n")
        section = _section({"t.yaml": template})
        self.assertEqual([c["name"] for c in section["clusters"]], [None, None])
        self.assertEqual(section["unresolved"], [])

    def test_comma_delimited_list_parameter(self):
        template = ("Parameters:\n  Allowed:\n    Type: CommaDelimitedList\n    Default: 203.0.113.0/24, 198.51.100.0/24\n"
                    "Resources:\n  Cluster:\n    Type: AWS::EKS::Cluster\n    Properties:\n"
                    "      Name: c\n      ResourcesVpcConfig:\n        PublicAccessCidrs: !Ref Allowed\n")
        cluster, = _section({"t.yaml": template})["clusters"]
        self.assertEqual(cluster["public_access_cidrs"], ["203.0.113.0/24", "198.51.100.0/24"])

    def test_ref_to_a_parameter_without_a_default_is_unresolved(self):
        template = ("Parameters:\n  Cidr:\n    Type: String\n"
                    "Resources:\n  VPC:\n    Type: AWS::EC2::VPC\n    Properties:\n      CidrBlock: !Ref Cidr\n")
        section = _section({"t.yaml": template})
        self.assertIsNone(section["vpcs"][0]["cidr"])
        self.assertEqual(section["unresolved"][0]["expression"], '{"Ref": "Cidr"}')

    def test_yaml_alias_is_refused_with_a_note(self):
        template = ("AWSTemplateFormatVersion: '2010-09-09'\n"
                    "Resources:\n  VPC: &v\n    Type: AWS::EC2::VPC\n  Copy: *v\n")
        section, notes = _harvest({"t.yaml": template})
        self.assertEqual(section, {})
        self.assertTrue(any("t.yaml: looks like a CloudFormation template but was not read" in n
                            for n in notes))

    def test_a_template_carrying_eksctl_tags_is_read_as_cloudformation(self):
        template = ("AWSTemplateFormatVersion: '2010-09-09'\n"
                    "Resources:\n  VPC:\n    Type: AWS::EC2::VPC\n    Properties:\n"
                    "      CidrBlock: 192.168.0.0/16\n"
                    "      Tags:\n        - Key: alpha.eksctl.io/cluster-name\n          Value: !Ref AWS::StackName\n")
        section, notes = _harvest({"stack.yaml": template})
        self.assertEqual(section["vpcs"][0]["cidr"], "192.168.0.0/16")
        self.assertFalse(any("was not read" in n for n in notes))

    def test_json_that_is_not_a_template_is_ignored(self):
        self.assertEqual(_section({"package.json": '{"name": "AWS::EC2::VPC lookalike"}'}), {})


class HarvestShapeTest(unittest.TestCase):

    def test_empty_estate_is_an_empty_section_with_the_coverage_note(self):
        section, notes = _harvest({"README.md": "nothing here\n"})
        self.assertEqual(section, {})
        self.assertEqual(notes, [addressspace.COVERAGE_NOTE])

    def test_an_excluded_tfvars_json_is_counted_once(self):
        # Read by the Terraform walk, left alone by the JSON walk: one note,
        # not one per walk.
        _, notes = _harvest({"legacy/terraform.tfvars.json": '{"cidr": "10.9.0.0/16"}'},
                            scope={"root_dir": "/", "excluded": ["legacy/"], "included": []})
        self.assertEqual([n for n in notes if "confirmed scope excludes" in n],
                         ["1 Terraform file(s) the confirmed scope excludes were not read for the "
                          "network address space"])

    def test_scope_exclusion_names_the_purpose(self):
        section, notes = _harvest({"legacy/vpc.tf": ACME_VPC_TF},
                                  scope={"root_dir": "/", "excluded": ["legacy/"], "included": []})
        self.assertEqual(section, {})
        self.assertIn("1 Terraform file(s) the confirmed scope excludes were not read for the "
                      "network address space", notes)

    def test_deterministic(self):
        files = {"terraform/vpc.tf": ACME_VPC_TF, "terraform/eks.tf": ACME_EKS_TF,
                 "eksctl/cluster.yaml": EKSCTL_CLUSTER, "cfn/vpc.yaml": CFN_TEMPLATE}
        self.assertEqual(_harvest(files), _harvest(files))

    def test_every_dialect_validates_against_the_inventory_schema(self):
        import json
        import jsonschema
        here = os.path.dirname(os.path.abspath(__file__))
        schema_path = os.path.join(here, "..", "..", "..", "dag", "server", "schema", "inventory.json")
        with open(schema_path) as f:
            schema = json.load(f)
        files = {"terraform/vpc.tf": ACME_VPC_TF, "terraform/eks.tf": ACME_EKS_TF,
                 "modules/main.tf": TerraformResolutionTest.WORKSHOP_VPC + TerraformResolutionTest.WORKSHOP_EKS,
                 "modules/variables.tf": TerraformResolutionTest.WORKSHOP_VARS,
                 "eksctl/cluster.yaml": EKSCTL_CLUSTER, "cfn/vpc.yaml": CFN_TEMPLATE}
        section, notes = _harvest(files)
        self.assertEqual({v["form"] for v in section["vpcs"]},
                         {"resource", "module", "eksctl", "cloudformation"})
        jsonschema.validate({"address_space": section, "address_space_scan_notes": notes}, schema)
        # The schema allows extra properties, so it cannot catch a working
        # key (`_instance_dir`, `_vpc_dir`) leaking into the ledger.
        for entries in (section["vpcs"], section["subnets"], section["clusters"], section["routes"]):
            for entry in entries:
                self.assertEqual([k for k in entry if k.startswith("_")], [], entry)

    CFN_JSON = ('{"AWSTemplateFormatVersion": "2010-09-09", "Resources": {"VPC": '
                '{"Type": "AWS::EC2::VPC", "Properties": {"CidrBlock": "10.30.0.0/16"}}}}')

    def test_a_clusterconfig_under_another_apiversion_is_not_eksctl(self):
        # The file carries a real eksctl document, so it is read; the
        # `kind: ClusterConfig` under apiVersion v1 beside it is not one.
        stream = ("apiVersion: eksctl.io/v1alpha5\nkind: ClusterConfig\nmetadata:\n  name: real\n"
                  "vpc:\n  cidr: 10.7.0.0/16\n"
                  "---\napiVersion: v1\nkind: ClusterConfig\nmetadata:\n  name: fake\n"
                  "vpc:\n  cidr: 10.8.0.0/16\n")
        section = _section({"eksctl/cluster.yaml": stream})
        self.assertEqual([v["cidr"] for v in section["vpcs"]], ["10.7.0.0/16"])

    def test_cloudformation_evidence_names_a_parameter_default_as_the_origin(self):
        # `CidrBlock: !Ref VpcCidr` with a Default: recorded, and the
        # evidence says the line did not state it, as a parent stack may
        # pass another value. A literal property stays plain.
        section = _section({"cfn/vpc.yaml": CFN_TEMPLATE})
        vpc = section["vpcs"][0]
        self.assertTrue(vpc["evidence"][0].endswith(
            "Properties.CidrBlock = 10.30.0.0/16 (Ref VpcCidr, the parameter's default)"), vpc["evidence"])
        literal = [s for s in section["subnets"] if s["cidr"] == "10.30.0.0/19"][0]
        self.assertTrue(literal["evidence"][0].endswith("Properties.CidrBlock = 10.30.0.0/19"), literal["evidence"])

    def test_a_cloudformation_default_route_and_an_instance_route(self):
        template = ("AWSTemplateFormatVersion: '2010-09-09'\nResources:\n"
                    "  Default:\n    Type: AWS::EC2::Route\n    Properties:\n"
                    "      RouteTableId: rtb-1\n      DestinationCidrBlock: 0.0.0.0/0\n      GatewayId: igw-1\n"
                    "  Appliance:\n    Type: AWS::EC2::Route\n    Properties:\n"
                    "      RouteTableId: rtb-1\n      DestinationCidrBlock: 192.168.0.0/16\n      InstanceId: i-0abc\n")
        section = _section({"cfn/routes.yaml": template})
        self.assertEqual([(r["destination"], r["via"]) for r in section["routes"]],
                         [("192.168.0.0/16", "instance")])

    def test_a_cloudformation_ipam_secondary_range_is_a_question_under_its_vpc(self):
        template = ("AWSTemplateFormatVersion: '2010-09-09'\nResources:\n"
                    "  VPC:\n    Type: AWS::EC2::VPC\n    Properties:\n      CidrBlock: 10.30.0.0/16\n"
                    "  PodRange:\n    Type: AWS::EC2::VPCCidrBlock\n    Properties:\n"
                    "      VpcId: !Ref VPC\n      Ipv4IpamPoolId: ipam-pool-1\n      Ipv4NetmaskLength: 16\n"
                    "  PodSubnet:\n    Type: AWS::EC2::Subnet\n    Properties:\n"
                    "      VpcId: !Ref VPC\n      CidrBlock: !Select [0, !Cidr [!GetAtt PodRange.CidrBlock, 4, 12]]\n")
        section = _section({"cfn/vpc.yaml": template})
        self.assertEqual(section["vpcs"][0]["secondary_cidrs"], [])
        self.assertEqual([(e["address"], e["argument"]) for e in section["unresolved"]],
                         [("VPC", "secondary_cidr_blocks"), ("PodSubnet", "CidrBlock")])
        blocking, covered = addressspace.triage_unresolved(section)
        self.assertEqual((len(blocking), covered), (2, []))

    def test_a_cloudformation_subnet_is_not_covered_by_another_templates_vpc(self):
        # b.yaml's subnet names VPC through a Parameter; a.yaml declares a
        # stated VPC under that logical id. Only Terraform crosses files:
        # b's subnet is a question.
        a = ("AWSTemplateFormatVersion: '2010-09-09'\nResources:\n  VPC:\n    Type: AWS::EC2::VPC\n"
             "    Properties:\n      CidrBlock: 10.1.0.0/16\n")
        b = ("AWSTemplateFormatVersion: '2010-09-09'\nParameters:\n  VPC:\n    Type: String\n"
             "Resources:\n  S:\n    Type: AWS::EC2::Subnet\n    Properties:\n      VpcId: !Ref VPC\n"
             "      CidrBlock: !Select [0, !Cidr [!Ref VPC, 4, 12]]\n")
        section = _section({"cfn/a.yaml": a, "cfn/b.yaml": b})
        blocking, covered = addressspace.triage_unresolved(section)
        self.assertEqual(([(e["path"], e["argument"]) for e in blocking], covered),
                         ([("cfn/b.yaml", "CidrBlock")], []))

    def test_a_cloudformation_secondary_range_attaches_only_within_its_template(self):
        # b.yaml's VPCCidrBlock names VPC through a Parameter; a.yaml in the
        # same directory declares a VPC under that logical id. The range
        # belongs to b's placeholder, not to a's VPC.
        a = ("AWSTemplateFormatVersion: '2010-09-09'\nResources:\n  VPC:\n    Type: AWS::EC2::VPC\n"
             "    Properties:\n      CidrBlock: 10.1.0.0/16\n")
        b = ("AWSTemplateFormatVersion: '2010-09-09'\nParameters:\n  VPC:\n    Type: String\n"
             "Resources:\n  PodRange:\n    Type: AWS::EC2::VPCCidrBlock\n"
             "    Properties:\n      VpcId: !Ref VPC\n      CidrBlock: 100.64.0.0/16\n")
        section = _section({"cfn/a.yaml": a, "cfn/b.yaml": b})
        self.assertEqual({(v["path"], v["cidr"], tuple(v["secondary_cidrs"])) for v in section["vpcs"]},
                         {("cfn/a.yaml", "10.1.0.0/16", ()), ("cfn/b.yaml", None, ("100.64.0.0/16",))})

    def test_the_readiness_report_is_handed_the_triage_as_data(self):
        # The report prompt must not re-derive which unresolved entries are
        # questions: the same split the scan summary and the design use
        # travels in the payload.
        import json
        from servers.phases.discovery.discovery_extract_3 import reporter
        tf = ('variable "peer" {}\n'
              'resource "aws_vpc" "main" {\n  cidr_block = "10.0.0.0/16"\n}\n'
              'resource "aws_subnet" "pods" {\n  count = 2\n  vpc_id = aws_vpc.main.id\n'
              '  cidr_block = cidrsubnet(aws_vpc.main.cidr_block, 4, count.index)\n}\n'
              'resource "aws_route" "peer" {\n  destination_cidr_block = var.peer\n'
              '  vpc_peering_connection_id = "pcx-1"\n}\n')
        section = _section({"main.tf": tf})
        prompt = reporter.build_report_prompt({"address_space": section})
        payload = json.loads(prompt.split("Input data:\n", 1)[1])
        questions = payload["address_space_questions"]
        self.assertEqual([e["argument"] for e in questions["blocking"]], ["destination_cidr_block"])
        self.assertEqual([e["argument"] for e in questions["covered"]], ["cidr_block"])
        self.assertIn("address_space_questions.blocking", prompt)

    def test_oversized_json_is_noted_only_when_it_looks_like_a_template(self):
        # dashboard.json is not a lock file, so the size hint alone decides
        # that it gets no note; the template does.
        from servers.phases.discovery.discovery_init_1 import datastores
        big = "x" * (datastores.MAX_FILE_BYTES + 1)
        _, notes = _harvest({"dashboard.json": '{"panels": [], "pad": "' + big + '"}',
                             "stack.json": '{"AWSTemplateFormatVersion": "2010-09-09", "pad": "' + big + '"}'})
        skipped = [n for n in notes if "skipped, larger than" in n]
        self.assertEqual(skipped, [f"stack.json: skipped, larger than {datastores.MAX_FILE_BYTES} bytes"])

    def test_lock_files_are_skipped_even_when_they_read_as_a_template(self):
        # Under its own name the template is read; under a lock file's name
        # the same bytes are never opened, and leave no note either.
        section, notes = _harvest({"stack.json": self.CFN_JSON})
        self.assertEqual(section["vpcs"][0]["cidr"], "10.30.0.0/16")
        section, notes = _harvest({"package-lock.json": self.CFN_JSON})
        self.assertEqual(section, {})
        self.assertEqual([n for n in notes if "package-lock" in n], [])

    def test_a_manifest_stream_that_mentions_a_cloudformation_type_gets_no_note(self):
        manifests = ("apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: notes\n"
                     "data:\n  readme: the stack declares an AWS::EC2::VPC and two subnets\n"
                     "---\napiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: other\n")
        _, notes = _harvest({"k8s/cm.yaml": manifests})
        self.assertEqual([n for n in notes if "looks like a CloudFormation template" in n], [])
        # The positive control: two templates in one stream are a template
        # file the single-document loader could not read, and say so. In
        # long form (`Ref:` maps, no short tags), so the manifest loader
        # reads the stream and only the apiVersion/kind rule keeps the note.
        long_form = ("AWSTemplateFormatVersion: '2010-09-09'\nResources:\n  VPC:\n    Type: AWS::EC2::VPC\n"
                     "    Properties:\n      CidrBlock: 10.1.0.0/16\n  S:\n    Type: AWS::EC2::Subnet\n"
                     "    Properties:\n      VpcId:\n        Ref: VPC\n      CidrBlock: 10.1.1.0/24\n")
        two = long_form + "---\n" + long_form
        _, notes = _harvest({"cfn/two.yaml": two})
        self.assertEqual(len([n for n in notes if n.startswith("cfn/two.yaml: looks like a "
                                                                "CloudFormation template but was not read")]), 1)
        # And a stream that is a manifest first and a template second: one
        # document with the envelope does not make it a manifest stream.
        mixed = "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: notes\n---\n" + long_form
        _, notes = _harvest({"k8s/mixed.yaml": mixed})
        self.assertEqual(len([n for n in notes if n.startswith("k8s/mixed.yaml: looks like a "
                                                                "CloudFormation template but was not read")]), 1)

    def test_entry_cap_is_a_note(self):
        many = "".join(f'resource "aws_subnet" "s{i}" {{\n  cidr_block = "10.0.{i % 256}.0/24"\n}}\n'
                       for i in range(addressspace.MAX_ENTRIES + 3))
        section, notes = _harvest({"main.tf": many})
        self.assertEqual(len(section["subnets"]), addressspace.MAX_ENTRIES)
        self.assertIn(f"address_space.subnets: {addressspace.MAX_ENTRIES + 3} entries found, the first "
                      f"{addressspace.MAX_ENTRIES} recorded", notes)


if __name__ == "__main__":
    unittest.main()
