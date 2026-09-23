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

"""Tests for the AWS cloud-plane walk.

Every AWS call is a plain method on a fake client the test injects through
`client_factory`, so the walk is exercised with no boto3, no credentials and
no network — the seam the module exists to keep. The behaviours under test
are the two the design leans on: full assembly of a cluster's cloud context,
and failure discipline (one dead region/cluster becomes a note, not a lost
estate).
"""

import unittest

from servers.phases.discovery.discovery_livescan_0 import aws_live, projection


class FakeClient:
    """A canned AWS client: each API is a method returning a stored dict.

    Methods raise if a test did not supply a response, which surfaces an
    unexpected call rather than silently returning empty.
    """

    def __init__(self, responses):
        self._responses = responses

    def __getattr__(self, name):
        if name not in self._responses:
            raise AssertionError(f"unexpected AWS call: {name}")
        value = self._responses[name]
        if callable(value):
            return value
        return lambda **kwargs: value


def make_factory(by_service):
    """(service, region) -> FakeClient, from a {service: responses} map."""
    def factory(service, region):
        if service not in by_service:
            raise AssertionError(f"unexpected client requested: {service}")
        return FakeClient(by_service[service])
    return factory


def _cluster_response(name):
    return {"cluster": {
        "arn": f"arn:aws:eks:us-east-1:1:cluster/{name}",
        "status": "ACTIVE", "version": "1.29", "platformVersion": "eks.5",
        "endpoint": f"https://{name}.eks.amazonaws.com",
        "certificateAuthority": {"data": "Q0FDRVJU"},
        "resourcesVpcConfig": {
            "vpcId": "vpc-1", "subnetIds": ["subnet-1"],
            "securityGroupIds": ["sg-1"], "clusterSecurityGroupId": "sg-cluster",
            "endpointPublicAccess": True, "endpointPrivateAccess": False,
            "publicAccessCidrs": ["0.0.0.0/0"]},
        "identity": {"oidc": {"issuer": "https://oidc.eks/id"}},
        "logging": {"clusterLogging": [
            {"enabled": True, "types": ["api", "audit"]},
            {"enabled": False, "types": ["scheduler"]}]},
        "tags": {"env": "prod"},
    }}


class DiscoverEstateTest(unittest.TestCase):

    def _full_services(self):
        return {
            "eks": {
                "list_clusters": {"clusters": ["prod"]},
                "describe_cluster": lambda **kw: _cluster_response(kw["name"]),
                "list_addons": {"addons": ["vpc-cni"]},
                "describe_addon": {"addon": {
                    "addonVersion": "v1.16", "status": "ACTIVE",
                    "serviceAccountRoleArn": "arn:aws:iam::1:role/cni"}},
                "list_nodegroups": {"nodegroups": ["ng-1"]},
                "describe_nodegroup": {"nodegroup": {
                    "status": "ACTIVE", "capacityType": "ON_DEMAND",
                    "instanceTypes": ["m5.large"], "amiType": "AL2_x86_64",
                    "releaseVersion": "1.29.0", "diskSize": 50,
                    "scalingConfig": {"minSize": 1, "maxSize": 5,
                                      "desiredSize": 2},
                    "labels": {"role": "worker"},
                    "taints": [{"key": "dedicated", "value": "gpu",
                                "effect": "NO_SCHEDULE"}],
                    "subnets": ["subnet-1"], "nodeRole": "arn:aws:iam::1:role/ng",
                    "launchTemplate": {"id": "lt-1"},
                    "resources": {"autoScalingGroups": [{"name": "asg-1"}]}}},
            },
            "autoscaling": {"describe_auto_scaling_groups": {
                "AutoScalingGroups": [{
                    "AutoScalingGroupName": "asg-1", "MinSize": 1, "MaxSize": 5,
                    "DesiredCapacity": 2,
                    "Instances": [{"InstanceId": "i-1"}, {"InstanceId": "i-2"}],
                    "AvailabilityZones": ["us-east-1a", "us-east-1b"]}]}},
            "ec2": {
                "describe_vpcs": {"Vpcs": [{
                    "CidrBlockAssociationSet": [{"CidrBlock": "10.0.0.0/16"}]}]},
                "describe_subnets": {"Subnets": [{
                    "SubnetId": "subnet-1", "AvailabilityZone": "us-east-1a",
                    "CidrBlock": "10.0.1.0/24", "AvailableIpAddressCount": 200,
                    "MapPublicIpOnLaunch": False,
                    "Tags": [{"Key": "Name", "Value": "private-1"}]}]},
                "describe_security_groups": {"SecurityGroups": [{
                    "GroupId": "sg-1", "GroupName": "nodes",
                    "Description": "node sg",
                    "IpPermissions": [{}], "IpPermissionsEgress": [{}, {}]}]},
                "describe_route_tables": {"RouteTables": [
                    {"RouteTableId": "rtb-1"}, {"RouteTableId": "rtb-2"}]},
            },
            "elbv2": {
                "describe_load_balancers": {"LoadBalancers": [{
                    "LoadBalancerArn": "arn:lb/1", "LoadBalancerName": "web-alb",
                    "Type": "application", "Scheme": "internet-facing",
                    "DNSName": "web-alb.elb.amazonaws.com", "VpcId": "vpc-1"}]},
                "describe_tags": {"TagDescriptions": [{
                    "ResourceArn": "arn:lb/1",
                    "Tags": [{"Key": "elbv2.k8s.aws/cluster", "Value": "prod"},
                             {"Key": "ingress.k8s.aws/stack",
                              "Value": "shop/web"}]}]},
            },
        }

    def test_full_cluster_assembly(self):
        estate = aws_live.discover_estate(
            make_factory(self._full_services()), ["us-east-1"])
        self.assertEqual(estate["notes"], [])
        self.assertEqual(len(estate["clusters"]), 1)
        cluster = estate["clusters"][0]
        self.assertEqual(cluster["name"], "prod")
        self.assertEqual(cluster["kubernetes_version"], "1.29")
        self.assertEqual(cluster["certificate_authority_data"], "Q0FDRVJU")
        self.assertEqual(cluster["oidc_issuer"], "https://oidc.eks/id")
        self.assertEqual(cluster["logging_enabled"], ["api", "audit"])
        self.assertEqual(cluster["endpoint_access"]["public"], True)
        # Addons.
        self.assertEqual(cluster["addons"][0]["name"], "vpc-cni")
        # Nodegroups with scaling and taints.
        nodegroup = cluster["nodegroups"][0]
        self.assertEqual(nodegroup["scaling"],
                         {"min": 1, "max": 5, "desired": 2})
        self.assertEqual(nodegroup["taints"][0],
                         {"key": "dedicated", "value": "gpu", "effect": "NO_SCHEDULE"})
        self.assertEqual(nodegroup["asg_names"], ["asg-1"])
        # Free-form customer text is key names only, or not recorded at all.
        self.assertEqual(cluster["tags"], {"env": projection.OMITTED})
        self.assertEqual(nodegroup["labels"], {"role": projection.OMITTED})
        # ASG detail with live instance count.
        asg = cluster["autoscaling_groups"][0]
        self.assertEqual(asg["running_instances"], 2)
        self.assertEqual(asg["availability_zones"], ["us-east-1a", "us-east-1b"])
        # Network.
        network = cluster["network"]
        self.assertEqual(network["vpc"]["cidr_blocks"], ["10.0.0.0/16"])
        self.assertEqual(network["subnets"][0]["availability_zone"], "us-east-1a")
        self.assertEqual(network["subnets"][0]["tags"], {"Name": projection.OMITTED})
        self.assertNotIn("description", network["security_groups"][0])
        self.assertEqual(network["security_groups"][0]["ingress_rule_count"], 1)
        self.assertEqual(network["security_groups"][0]["egress_rule_count"], 2)
        # Route tables are thin: counts and ids only.
        self.assertEqual(network["route_tables"]["count"], 2)
        # Load balancer attributed to the cluster by tag.
        self.assertEqual(cluster["load_balancers"][0]["name"], "web-alb")
        self.assertEqual(cluster["load_balancers"][0]["ingress_group"],
                         "shop/web")

    def test_cluster_names_filter_and_missing_name_noted(self):
        services = self._full_services()
        services["eks"]["list_clusters"] = {"clusters": ["prod", "staging"]}
        estate = aws_live.discover_estate(
            make_factory(services), ["us-east-1"],
            cluster_names=["prod", "ghost"])
        names = [c["name"] for c in estate["clusters"]]
        self.assertEqual(names, ["prod"])
        # The typo'd name is reported, not silently dropped.
        self.assertTrue(any("ghost" in note and "not found" in note
                            for note in estate["notes"]))

    def test_region_that_cannot_be_listed_becomes_a_note(self):
        def factory(service, region):
            raise RuntimeError("AccessDenied: sts not permitted here")
        estate = aws_live.discover_estate(factory, ["ap-south-1"])
        self.assertEqual(estate["clusters"], [])
        self.assertEqual(len(estate["notes"]), 1)
        self.assertIn("ap-south-1", estate["notes"][0])
        self.assertIn("not scanned", estate["notes"][0])
        # Not a scanned region: the caller decides what an estate with no
        # listable region is.
        self.assertEqual(estate["regions"], [])
        self.assertEqual(estate["regions_unreachable"], ["ap-south-1"])

    def test_a_region_denied_among_others_is_left_out_of_regions(self):
        inner = make_factory(self._full_services())

        def factory(service, region):
            if region == "eu-west1":
                raise RuntimeError("Could not connect to the endpoint URL")
            return inner(service, region)
        estate = aws_live.discover_estate(
            factory, ["us-east-1", "eu-west1", "us-west-2"])
        self.assertEqual(estate["regions"], ["us-east-1", "us-west-2"])
        self.assertEqual(estate["regions_unreachable"], ["eu-west1"])
        self.assertEqual([c["region"] for c in estate["clusters"]],
                         ["us-east-1", "us-west-2"])
        self.assertTrue(any("eu-west1" in note and "not scanned" in note
                            for note in estate["notes"]))

    def test_a_region_named_twice_is_scanned_once_and_noted(self):
        estate = aws_live.discover_estate(
            make_factory(self._full_services()), ["us-east-1", "us-east-1"])
        self.assertEqual(estate["regions"], ["us-east-1"])
        self.assertEqual(estate["regions_unreachable"], [])
        self.assertEqual([c["name"] for c in estate["clusters"]], ["prod"])
        self.assertTrue(any("more than once" in note and "us-east-1" in note
                            for note in estate["notes"]))

    def test_one_dead_cluster_does_not_sink_the_others(self):
        services = self._full_services()
        services["eks"]["list_clusters"] = {"clusters": ["prod", "broken"]}

        def describe(**kw):
            if kw["name"] == "broken":
                raise RuntimeError("cluster describe timeout")
            return _cluster_response(kw["name"])
        services["eks"]["describe_cluster"] = describe
        estate = aws_live.discover_estate(make_factory(services), ["us-east-1"])
        self.assertEqual([c["name"] for c in estate["clusters"]], ["prod"])
        self.assertTrue(any("broken" in note for note in estate["notes"]))

    def test_addon_failure_is_noted_but_cluster_survives(self):
        services = self._full_services()

        def list_addons(**kw):
            raise RuntimeError("addons denied")
        services["eks"]["list_addons"] = list_addons
        estate = aws_live.discover_estate(make_factory(services), ["us-east-1"])
        cluster = estate["clusters"][0]
        self.assertEqual(cluster["addons"], [])
        self.assertTrue(any("addons not read" in note
                            for note in estate["notes"]))

    def test_one_undescribable_nodegroup_is_a_note_and_the_others_stand(self):
        services = self._full_services()
        services["eks"]["list_nodegroups"] = {"nodegroups": ["a", "b", "c"]}
        good = services["eks"]["describe_nodegroup"]

        def describe_nodegroup(**kw):
            if kw["nodegroupName"] == "b":
                raise RuntimeError("b denied")
            return good
        services["eks"]["describe_nodegroup"] = describe_nodegroup
        estate = aws_live.discover_estate(make_factory(services), ["us-east-1"])
        cluster = estate["clusters"][0]
        self.assertEqual([ng["name"] for ng in cluster["nodegroups"]], ["a", "c"])
        self.assertEqual([ng["asg_names"] for ng in cluster["nodegroups"]],
                         [["asg-1"], ["asg-1"]])
        self.assertTrue(any("nodegroup b not read — b denied" in note
                            for note in estate["notes"]))
        self.assertFalse(any("nodegroups not read" in note
                             for note in estate["notes"]))

    def test_one_undescribable_addon_is_a_note_and_the_others_stand(self):
        services = self._full_services()
        services["eks"]["list_addons"] = {
            "addons": ["vpc-cni", "coredns", "kube-proxy"]}
        good = services["eks"]["describe_addon"]

        def describe_addon(**kw):
            if kw["addonName"] == "coredns":
                raise RuntimeError("coredns denied")
            return good
        services["eks"]["describe_addon"] = describe_addon
        estate = aws_live.discover_estate(make_factory(services), ["us-east-1"])
        cluster = estate["clusters"][0]
        self.assertEqual([a["name"] for a in cluster["addons"]],
                         ["vpc-cni", "kube-proxy"])
        self.assertTrue(any("addon coredns not read — coredns denied" in note
                            for note in estate["notes"]))

    def test_an_unlistable_nodegroup_set_is_still_one_note(self):
        services = self._full_services()

        def list_nodegroups(**kw):
            raise RuntimeError("nodegroups denied")
        services["eks"]["list_nodegroups"] = list_nodegroups
        estate = aws_live.discover_estate(make_factory(services), ["us-east-1"])
        self.assertEqual(estate["clusters"][0]["nodegroups"], [])
        self.assertTrue(any("nodegroups not read — nodegroups denied" in note
                            for note in estate["notes"]))


class PagedTest(unittest.TestCase):

    def test_next_token_pagination_is_drained(self):
        pages = [
            {"clusters": ["a", "b"], "nextToken": "t1"},
            {"clusters": ["c"], "nextToken": None},
        ]
        asked = []

        def call(**kwargs):
            asked.append(kwargs)
            return pages[len(asked) - 1]
        notes = []
        result = aws_live._paged(call, "clusters", notes, "list")
        self.assertEqual(result, ["a", "b", "c"])
        self.assertEqual(notes, [])
        self.assertEqual(asked, [{}, {"nextToken": "t1"}])

    def test_iam_marker_pagination_uses_is_truncated(self):
        pages = [
            {"PolicyNames": ["p1"], "IsTruncated": True, "Marker": "m1"},
            {"PolicyNames": ["p2"], "IsTruncated": False},
        ]
        asked = []

        def call(**kwargs):
            asked.append(kwargs)
            return pages[len(asked) - 1]
        result = aws_live._paged(call, "PolicyNames", [], "policies",
                                 request_token="Marker", response_token="Marker")
        self.assertEqual(result, ["p1", "p2"])
        self.assertEqual(asked, [{}, {"Marker": "m1"}])

    def test_page_cap_is_noted(self):
        def call(**kwargs):
            return {"clusters": ["x"], "nextToken": "always-more"}
        notes = []
        result = aws_live._paged(call, "clusters", notes, "runaway list")
        self.assertEqual(len(result), aws_live._MAX_PAGES)
        self.assertEqual(len(notes), 1)
        self.assertIn("stopped after", notes[0])


class ResolveIrsaRolesTest(unittest.TestCase):

    def test_existing_role_trust_and_policies(self):
        arn = "arn:aws:iam::1:role/app"
        services = {"iam": {
            "get_role": {"Role": {"AssumeRolePolicyDocument": {"Statement": [{
                "Condition": {"StringEquals": {
                    "oidc.eks:sub": "system:serviceaccount:shop:web"}}}]}}},
            "list_attached_role_policies": {
                "AttachedPolicies": [{"PolicyArn": "arn:aws:iam::1:policy/s3"}],
                "IsTruncated": False},
            "list_role_policies": {"PolicyNames": ["inline-1"],
                                   "IsTruncated": False},
        }}
        resolved = aws_live.resolve_irsa_roles(make_factory(services), [arn])
        entry = resolved[arn]
        self.assertTrue(entry["exists"])
        self.assertEqual(entry["trusted_subjects"],
                         ["system:serviceaccount:shop:web"])
        self.assertEqual(entry["attached_policies"],
                         ["arn:aws:iam::1:policy/s3"])
        self.assertEqual(entry["inline_policy_names"], ["inline-1"])

    def test_missing_role_records_the_error(self):
        arn = "arn:aws:iam::1:role/gone"

        def get_role(**kw):
            raise RuntimeError("NoSuchEntity")
        services = {"iam": {"get_role": get_role}}
        resolved = aws_live.resolve_irsa_roles(make_factory(services), [arn])
        self.assertFalse(resolved[arn]["exists"])
        self.assertIn("NoSuchEntity", resolved[arn]["error"])

    def test_arns_are_deduplicated(self):
        arn = "arn:aws:iam::1:role/app"
        seen = {"n": 0}

        def get_role(**kw):
            seen["n"] += 1
            return {"Role": {}}
        services = {"iam": {
            "get_role": get_role,
            "list_attached_role_policies": {"AttachedPolicies": []},
            "list_role_policies": {"PolicyNames": []},
        }}
        aws_live.resolve_irsa_roles(make_factory(services), [arn, arn, arn])
        self.assertEqual(seen["n"], 1)


class TrustedSubjectsTest(unittest.TestCase):

    def test_collects_string_equals_and_string_like(self):
        doc = {"Statement": [
            {"Condition": {"StringEquals": {"o:sub": "sa-a"}}},
            {"Condition": {"StringLike": {"o:sub": ["sa-b", "sa-*"]}}},
        ]}
        self.assertEqual(sorted(aws_live._trusted_subjects(doc)),
                         ["sa-*", "sa-a", "sa-b"])

    def test_single_statement_object_is_accepted(self):
        doc = {"Statement": {"Condition": {"StringEquals": {"o:sub": "sa-x"}}}}
        self.assertEqual(aws_live._trusted_subjects(doc), ["sa-x"])

    def test_ignores_non_sub_conditions_and_bad_input(self):
        doc = {"Statement": [{"Condition": {"StringEquals": {"o:aud": "sts"}}}]}
        self.assertEqual(aws_live._trusted_subjects(doc), [])
        self.assertEqual(aws_live._trusted_subjects(None), [])


class LoadBalancerOwnerTest(unittest.TestCase):

    def test_owner_by_controller_tag(self):
        self.assertEqual(
            aws_live._lb_owner({"elbv2.k8s.aws/cluster": "prod"}, {"prod"}),
            "prod")

    def test_owner_by_legacy_cluster_tag_prefix(self):
        self.assertEqual(
            aws_live._lb_owner({"kubernetes.io/cluster/prod": "owned"}, {"prod"}),
            "prod")

    def test_unowned_load_balancer_returns_none(self):
        self.assertIsNone(
            aws_live._lb_owner({"Name": "unrelated"}, {"prod"}))


class LoadBalancerChunkingTest(unittest.TestCase):

    def test_tags_are_requested_in_chunks_of_twenty(self):
        # 25 load balancers force two describe_tags calls (20 + 5).
        lbs = [{"LoadBalancerArn": f"arn:lb/{i}",
                "LoadBalancerName": f"lb{i}", "Type": "network"}
               for i in range(25)]
        chunk_sizes = []

        def describe_tags(**kwargs):
            arns = kwargs["ResourceArns"]
            chunk_sizes.append(len(arns))
            return {"TagDescriptions": [
                {"ResourceArn": arn,
                 "Tags": [{"Key": "kubernetes.io/cluster/prod",
                           "Value": "owned"}]}
                for arn in arns]}
        services = {"elbv2": {
            "describe_load_balancers": {"LoadBalancers": lbs},
            "describe_tags": describe_tags,
        }}
        attributed = aws_live._cluster_load_balancers(
            make_factory(services), "us-east-1", {"prod"}, [])
        self.assertEqual(sorted(chunk_sizes, reverse=True), [20, 5])
        self.assertEqual(len(attributed["prod"]), 25)


class _ClientError(Exception):
    """The shape of botocore's ClientError: the code rides in `response`."""

    def __init__(self, code, message):
        super().__init__(f"An error occurred ({code}) when calling the GetRole "
                         f"operation: {message}")
        self.response = {"Error": {"Code": code, "Message": message}}


class IrsaRoleOutcomesTest(unittest.TestCase):
    """Three outcomes, three findings: gone (exists false), unreadable
    (exists null), and cross-account (exists null, IAM never asked)."""

    def test_no_such_entity_means_the_role_is_gone(self):
        arn = "arn:aws:iam::1:role/gone"

        def get_role(**kw):
            raise _ClientError("NoSuchEntity", "The role with name gone cannot be found.")
        resolved = aws_live.resolve_irsa_roles(
            make_factory({"iam": {"get_role": get_role}}), [arn], account="1")
        self.assertIs(resolved[arn]["exists"], False)
        self.assertIn("NoSuchEntity", resolved[arn]["error"])

    def test_a_denied_read_is_unresolved_not_missing(self):
        arn = "arn:aws:iam::1:role/app"

        def get_role(**kw):
            raise _ClientError("AccessDenied", "not authorized to perform iam:GetRole")
        resolved = aws_live.resolve_irsa_roles(
            make_factory({"iam": {"get_role": get_role}}), [arn], account="1")
        self.assertIsNone(resolved[arn]["exists"])
        self.assertIn("AccessDenied", resolved[arn]["error"])

    def test_a_role_in_another_account_is_not_asked_for(self):
        arn = "arn:aws:iam::222222222222:role/shared"
        calls = []

        def get_role(**kw):
            calls.append(kw)
            return {"Role": {}}
        resolved = aws_live.resolve_irsa_roles(
            make_factory({"iam": {"get_role": get_role}}), [arn],
            account="111111111111")
        self.assertIsNone(resolved[arn]["exists"])
        self.assertIn("account 222222222222", resolved[arn]["error"])
        self.assertIn("111111111111", resolved[arn]["error"])
        self.assertEqual(calls, [])

    def test_an_annotation_that_is_no_role_arn_is_not_sent_to_iam(self):
        calls = []

        def get_role(**kw):
            calls.append(kw)
            return {"Role": {}}
        resolved = aws_live.resolve_irsa_roles(
            make_factory({"iam": {"get_role": get_role}}), ["app-role"],
            account="111111111111")
        self.assertIsNone(resolved["app-role"]["exists"])
        self.assertIn("not an IAM role ARN", resolved["app-role"]["error"])
        self.assertEqual(calls, [])

    def test_without_a_known_account_every_role_is_asked_for(self):
        arn = "arn:aws:iam::222222222222:role/shared"
        services = {"iam": {
            "get_role": {"Role": {}},
            "list_attached_role_policies": {"AttachedPolicies": []},
            "list_role_policies": {"PolicyNames": []}}}
        resolved = aws_live.resolve_irsa_roles(make_factory(services), [arn])
        self.assertTrue(resolved[arn]["exists"])

    def test_a_client_that_cannot_be_built_propagates(self):
        with self.assertRaises(AssertionError):
            aws_live.resolve_irsa_roles(make_factory({}),
                                        ["arn:aws:iam::1:role/app"])

    def test_a_policy_list_that_cannot_be_read_keeps_the_role_as_read(self):
        # GetRole answered: the role exists and its trust policy is known.
        # A denied ListAttachedRolePolicies is a gap in the detail, noted on
        # the entry — not a role that was read reported as unreadable, with
        # its trusted subjects sitting beside an `exists: null`.
        arn = "arn:aws:iam::1:role/app"

        def list_attached(**kw):
            raise _ClientError("AccessDenied",
                               "not authorized to perform iam:ListAttachedRolePolicies")
        services = {"iam": {
            "get_role": {"Role": {"AssumeRolePolicyDocument": {"Statement": [{
                "Condition": {"StringEquals": {
                    "oidc.eks:sub": "system:serviceaccount:shop:web"}}}]}}},
            "list_attached_role_policies": list_attached,
            "list_role_policies": {"PolicyNames": ["inline-1"],
                                   "IsTruncated": False}}}
        resolved = aws_live.resolve_irsa_roles(make_factory(services), [arn],
                                               account="1")
        entry = resolved[arn]
        self.assertIs(entry["exists"], True)
        self.assertNotIn("error", entry)
        self.assertEqual(entry["trusted_subjects"],
                         ["system:serviceaccount:shop:web"])
        self.assertNotIn("attached_policies", entry)   # not read, not empty
        self.assertEqual(entry["inline_policy_names"], ["inline-1"])
        self.assertEqual(len(entry["notes"]), 1)
        self.assertIn("attached policies of app: not read", entry["notes"][0])
        self.assertIn("AccessDenied", entry["notes"][0])


class AutoScalingGroupChunkingTest(unittest.TestCase):

    def test_names_are_described_fifty_at_a_time(self):
        calls = []

        def describe(**kw):
            names = kw["AutoScalingGroupNames"]
            calls.append(len(names))
            return {"AutoScalingGroups": [
                {"AutoScalingGroupName": n, "MinSize": 0, "MaxSize": 1,
                 "DesiredCapacity": 0, "Instances": [], "AvailabilityZones": []}
                for n in names]}
        nodegroups = [{"asg_names": [f"asg-{i}"]} for i in range(120)]
        notes = []
        groups = aws_live._autoscaling_groups(
            make_factory({"autoscaling": {"describe_auto_scaling_groups": describe}}),
            "us-east-1", nodegroups, notes)
        self.assertEqual(calls, [50, 50, 20])
        self.assertEqual([g["name"] for g in groups],
                         [f"asg-{i}" for i in range(120)])
        self.assertEqual(notes, [])

    def test_each_call_asks_for_a_page_as_large_as_the_names_it_sends(self):
        # Without MaxRecords the call is right only while the service's
        # default page equals the 50 names sent; a smaller default would
        # silently drop the tail of every chunk.
        pages = []

        def describe(**kw):
            pages.append(kw.get("MaxRecords"))
            return {"AutoScalingGroups": []}
        nodegroups = [{"asg_names": [f"asg-{i}"]} for i in range(60)]
        aws_live._autoscaling_groups(
            make_factory({"autoscaling": {"describe_auto_scaling_groups": describe}}),
            "us-east-1", nodegroups, [])
        self.assertEqual(pages, [aws_live._ASG_NAMES_PER_CALL] * 2)

    def test_a_throttled_chunk_keeps_the_others_and_is_noted_by_name(self):
        calls = []

        def describe(**kw):
            names = kw["AutoScalingGroupNames"]
            calls.append(len(names))
            if len(calls) == 3:
                raise RuntimeError("Throttling")
            return {"AutoScalingGroups": [
                {"AutoScalingGroupName": n, "MinSize": 0, "MaxSize": 1,
                 "DesiredCapacity": 0, "Instances": [], "AvailabilityZones": []}
                for n in names]}
        nodegroups = [{"asg_names": [f"asg-{i}"]} for i in range(120)]
        notes = []
        groups = aws_live._autoscaling_groups(
            make_factory({"autoscaling": {"describe_auto_scaling_groups": describe}}),
            "us-east-1", nodegroups, notes)
        self.assertEqual(calls, [50, 50, 20])
        self.assertEqual([g["name"] for g in groups],
                         [f"asg-{i}" for i in range(100)])
        self.assertEqual(len(notes), 1)
        self.assertIn("not read — Throttling", notes[0])
        self.assertIn("'asg-100'", notes[0])
        self.assertIn("'asg-119'", notes[0])
        self.assertNotIn("'asg-99'", notes[0])

    def test_a_chunk_that_pages_is_drained_by_token(self):
        calls = []

        def group(name):
            return {"AutoScalingGroupName": name, "MinSize": 0, "MaxSize": 1,
                    "DesiredCapacity": 0, "Instances": [],
                    "AvailabilityZones": []}

        def describe(**kw):
            calls.append(kw.get("NextToken"))
            if kw.get("NextToken") is None:
                return {"AutoScalingGroups": [group("asg-a")],
                        "NextToken": "page-2"}
            return {"AutoScalingGroups": [group("asg-b")]}
        notes = []
        groups = aws_live._autoscaling_groups(
            make_factory({"autoscaling": {"describe_auto_scaling_groups": describe}}),
            "us-east-1", [{"asg_names": ["asg-a", "asg-b"]}], notes)
        self.assertEqual(calls, [None, "page-2"])
        self.assertEqual([g["name"] for g in groups], ["asg-a", "asg-b"])
        self.assertEqual(notes, [])


if __name__ == "__main__":
    unittest.main()
