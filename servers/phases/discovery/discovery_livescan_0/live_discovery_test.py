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

"""Tests for the live-discovery orchestrator.

The cloud walk (aws_live) and the in-cluster walk (k8s_live) are stubbed —
their own logic is covered by their own tests — so these tests exercise what
the orchestrator owns and nothing else: the per-cluster reachability
handling, the IRSA join that stitches AWS role detail back onto each binding,
partial-result discipline (an unreachable cluster keeps its AWS record), and
the summary the tool response leads with.
"""

import base64
import json
import unittest
from unittest.mock import patch

from servers.phases.discovery.discovery_livescan_0 import (live_discovery,
                                                          live_schema)


def _kubernetes_ir():
    return {
        "notes": ["Karpenter CRDs not present"],
        "identity": {
            "irsa": [{"namespace": "shop", "sa": "web-sa",
                      "role_arn": "arn:aws:iam::1:role/web"}]},
        "autoscaling": {"karpenter": {"nodepools": [{"metadata": {"name": "d"}}]}},
        "counts": {"Deployment": 2, "Secret": 1},
    }


class RunLiveDiscoveryTest(unittest.TestCase):

    def setUp(self):
        self.estate = {
            "regions": ["us-east-1"],
            "clusters": [{"name": "prod", "region": "us-east-1"},
                         {"name": "staging", "region": "us-east-1"}],
            "notes": ["region us-west-2: not scanned — denied"],
        }
        self.roles = {"arn:aws:iam::1:role/web": {
            "exists": True, "trusted_subjects": ["system:sa:shop:web-sa"],
            "attached_policies": ["arn:aws:iam::1:policy/s3"]}}

    def _get_json_factory(self, unreachable=("staging",)):
        def factory(cluster):
            if cluster["name"] in unreachable:
                raise RuntimeError("i/o timeout to private endpoint")
            return object()  # a sentinel get_json; discover_cluster is stubbed
        return factory

    def _run(self, discover_side_effect=None, resolve_side_effect=None,
             regions=("us-east-1",)):
        with patch.object(live_discovery.aws_live, "discover_estate",
                          return_value=self.estate), \
             patch.object(live_discovery.k8s_live, "discover_cluster",
                          side_effect=discover_side_effect,
                          **({} if discover_side_effect
                             else {"return_value": _kubernetes_ir()})), \
             patch.object(live_discovery.aws_live, "resolve_irsa_roles",
                          side_effect=resolve_side_effect,
                          **({} if resolve_side_effect
                             else {"return_value": self.roles})):
            return live_discovery.run_live_discovery(
                client_factory=lambda s, r: None,
                regions=list(regions),
                get_json_factory=self._get_json_factory())

    def test_irsa_is_resolved_in_a_region_that_answered(self):
        # IAM is global; the client goes to the first region that listed
        # its clusters, not to the first requested, which may be the typo.
        seen = []

        def resolve(client_factory, arns, region=None, account=None):
            seen.append(region)
            return self.roles
        self.estate["regions"] = ["us-west-2"]
        self._run(resolve_side_effect=resolve, regions=["eu-west1", "us-west-2"])
        self.assertEqual(seen, ["us-west-2"])

    def test_irsa_falls_back_to_the_first_requested_region(self):
        seen = []

        def resolve(client_factory, arns, region=None, account=None):
            seen.append(region)
            return self.roles
        self.estate["regions"] = []
        self._run(resolve_side_effect=resolve, regions=["eu-west-1", "us-west-2"])
        self.assertEqual(seen, ["eu-west-1"])

    def test_reachable_cluster_is_walked_and_unreachable_is_marked(self):
        result = self._run()
        clusters = {c["name"]: c for c in result["ir"]["clusters"]}
        self.assertIn("kubernetes", clusters["prod"])
        self.assertNotIn("kubernetes_error", clusters["prod"])
        # The unreachable cluster keeps its AWS-side record with the error.
        self.assertNotIn("kubernetes", clusters["staging"])
        self.assertIn("i/o timeout", clusters["staging"]["kubernetes_error"])

    def test_irsa_binding_is_joined_with_aws_role_detail(self):
        result = self._run()
        prod = result["ir"]["clusters"][0]
        binding = prod["kubernetes"]["identity"]["irsa"][0]
        self.assertIn("role", binding)
        self.assertTrue(binding["role"]["exists"])
        self.assertEqual(binding["role"]["attached_policies"],
                         ["arn:aws:iam::1:policy/s3"])

    def test_notes_surface_at_the_top_level(self):
        notes = self._run()["ir"]["notes"]
        joined = "\n".join(notes)
        self.assertIn("region us-west-2", joined)                  # from estate
        self.assertIn("cluster prod: Karpenter CRDs not present", joined)  # k8s
        self.assertIn("staging", joined)                           # unreachable

    def test_summary_aggregates_the_walk(self):
        summary = self._run()["ir"]["summary"]
        self.assertEqual(summary["clusters_found"], 2)
        self.assertEqual(summary["clusters_walked"], 1)
        self.assertEqual(summary["clusters_unreachable"], ["staging"])
        self.assertEqual(summary["clusters_using_karpenter"], 1)
        self.assertEqual(summary["workload_totals"], {"Deployment": 2, "Secret": 1})
        self.assertEqual(summary["regions_unreachable"], [])

    def test_summary_counts_listed_regions_and_names_the_unreachable(self):
        self.estate["regions"] = ["us-east-1", "us-west-2"]
        self.estate["regions_unreachable"] = ["eu-west1"]
        ir = self._run()["ir"]
        self.assertEqual(ir["regions"], ["us-east-1", "us-west-2"])
        self.assertEqual(ir["summary"]["regions_scanned"], 2)
        self.assertEqual(ir["summary"]["regions_unreachable"], ["eu-west1"])

    def test_in_cluster_walk_failure_is_recorded_per_cluster(self):
        result = self._run(discover_side_effect=RuntimeError("403 on /api"))
        prod = result["ir"]["clusters"][0]
        self.assertIn("403 on /api", prod["kubernetes_error"])
        self.assertNotIn("kubernetes", prod)
        # Both clusters end unreachable, so the summary says so.
        self.assertEqual(result["ir"]["summary"]["clusters_walked"], 0)

    def test_401_walk_failure_names_the_access_entry_fix(self):
        # A 401 from an EKS API server that a fresh token did not cure means
        # the AWS identity is mapped to nothing in that cluster — or that a
        # session credential expired mid-walk, which presigning cannot tell
        # (it is offline). The error text alone ("HTTP 401 Unauthorized")
        # names neither fix; the advice names both.
        class Unauthorized(Exception):
            status = 401

        result = self._run(
            discover_side_effect=Unauthorized("HTTP 401 Unauthorized"))
        prod = result["ir"]["clusters"][0]
        self.assertEqual(prod["kubernetes_error"], "HTTP 401 Unauthorized")
        joined = "\n".join(result["ir"]["notes"])
        self.assertIn("cluster prod: in-cluster walk failed — HTTP 401 "
                      "Unauthorized. A freshly minted token was refused too: "
                      "either the AWS identity is not mapped in this cluster "
                      "— add an EKS access entry", joined)
        self.assertIn("or the session credentials it was signed with expired "
                      "during the walk", joined)
        # An error without a status gets no advice bolted on (see the test
        # above: its note ends at the error text).
        plain = self._run(discover_side_effect=RuntimeError("403 on /api"))
        self.assertTrue(any(n.endswith("walk failed — 403 on /api")
                            for n in plain["ir"]["notes"]))

    def test_iam_wide_failure_leaves_bindings_without_role_detail(self):
        result = self._run(resolve_side_effect=RuntimeError("AccessDenied iam"))
        binding = result["ir"]["clusters"][0]["kubernetes"]["identity"]["irsa"][0]
        self.assertNotIn("role", binding)          # the ARN still stands
        self.assertEqual(binding["role_arn"], "arn:aws:iam::1:role/web")
        self.assertTrue(any("IRSA roles not resolved" in note
                            for note in result["ir"]["notes"]))

    def test_tables_are_rendered(self):
        result = self._run()
        self.assertIn("clusters", result["tables"])
        self.assertTrue(result["tables"]["clusters"].startswith("region,"))

    def test_provisioners_only_cluster_counts_as_using_karpenter(self):
        # A v1alpha5 estate has Provisioners and no NodePools; for capacity
        # planning it uses Karpenter no less.
        kubernetes_ir = _kubernetes_ir()
        kubernetes_ir["autoscaling"]["karpenter"] = {
            "nodepools": [], "provisioners": [{"metadata": {"name": "p"}}]}
        result = self._run(discover_side_effect=[kubernetes_ir])
        self.assertEqual(
            result["ir"]["summary"]["clusters_using_karpenter"], 1)

    def test_namespaces_parameter_reaches_every_cluster_walk(self):
        captured = []

        def fake_discover(get_json, namespaces=None):
            captured.append(namespaces)
            return _kubernetes_ir()

        with patch.object(live_discovery.aws_live, "discover_estate",
                          return_value=self.estate), \
             patch.object(live_discovery.k8s_live, "discover_cluster",
                          side_effect=fake_discover), \
             patch.object(live_discovery.aws_live, "resolve_irsa_roles",
                          return_value=self.roles):
            live_discovery.run_live_discovery(
                client_factory=lambda s, r: None,
                regions=["us-east-1"],
                get_json_factory=self._get_json_factory(),
                namespaces=["shop", "billing"])
        # Only prod is reachable; its walk got the caller's namespace filter.
        self.assertEqual(captured, [["shop", "billing"]])

    def test_the_aws_side_record_stands_as_the_walk_returned_it(self):
        # The keys-only treatment of tags and labels is aws_live's, applied
        # before the record reaches this orchestrator (aws_live_test); the
        # orchestrator adds nothing to and takes nothing from an unreachable
        # cluster's AWS-side record.
        self.estate["clusters"][1]["tags"] = {"team": "<omitted>"}
        self.estate["clusters"][1]["nodegroups"] = [{
            "name": "ng-1", "labels": {"role": "<omitted>"}}]
        result = self._run()
        staging = {c["name"]: c for c in result["ir"]["clusters"]}["staging"]
        self.assertEqual(staging["tags"], {"team": "<omitted>"})
        self.assertEqual(staging["nodegroups"][0]["labels"],
                         {"role": "<omitted>"})
        self.assertTrue(staging["kubernetes_error"])
        self.assertNotIn("kubernetes", staging)


_AKIA_LITERAL = "AKIAIOSFODNN7EXAMPLE"
_ASIA_LITERAL = "ASIAIOSFODNN7EXAMPLE"


class _FakeAwsClient:
    """Duck-typed boto3 client: methods provided as a dict of callables."""

    def __init__(self, api):
        for name, call in api.items():
            setattr(self, name, call)


def _fake_client_factory():
    """The exact read-only AWS surface the real walk touches, over one
    single-cluster estate. No response carries a pagination token, so every
    _paged loop terminates on its first page."""
    ca_data = base64.b64encode(b"fake-ca-pem").decode("ascii")
    apis = {
        "eks": {
            "list_clusters": lambda **kw: {"clusters": ["prod"]},
            "describe_cluster": lambda **kw: {"cluster": {
                "arn": "arn:aws:eks:us-east-1:1:cluster/prod",
                "status": "ACTIVE", "version": "1.29",
                "platformVersion": "eks.5",
                "endpoint": "https://prod.eks.example.com",
                "certificateAuthority": {"data": ca_data},
                "identity": {"oidc": {"issuer": "https://oidc.example/id"}},
                "resourcesVpcConfig": {
                    "vpcId": "vpc-1", "subnetIds": ["subnet-1"],
                    "securityGroupIds": ["sg-1"],
                    "endpointPublicAccess": True,
                    "endpointPrivateAccess": False},
                # Free-form customer text: only the key names may reach
                # the record, whatever the values look like.
                "tags": {"team": "shop", "db-password": "hunter2aws",
                         "backup-key": _ASIA_LITERAL}}},
            "list_addons": lambda **kw: {"addons": ["vpc-cni"]},
            "describe_addon": lambda **kw: {"addon": {
                "addonVersion": "v1.16.0", "status": "ACTIVE"}},
            "list_nodegroups": lambda **kw: {"nodegroups": ["ng-1"]},
            "describe_nodegroup": lambda **kw: {"nodegroup": {
                "status": "ACTIVE", "capacityType": "ON_DEMAND",
                "instanceTypes": ["m5.large"], "amiType": "AL2_x86_64",
                "scalingConfig": {"minSize": 1, "maxSize": 5,
                                  "desiredSize": 2},
                "labels": {"role": "web", "api-token": "tok123aws"},
                "resources": {"autoScalingGroups": [{"name": "asg-1"}]}}},
        },
        "autoscaling": {
            "describe_auto_scaling_groups": lambda **kw: {
                "AutoScalingGroups": [{
                    "AutoScalingGroupName": "asg-1", "MinSize": 1,
                    "MaxSize": 5, "DesiredCapacity": 2,
                    "Instances": [{}, {}],
                    "AvailabilityZones": ["us-east-1a"]}]},
        },
        "ec2": {
            "describe_vpcs": lambda **kw: {"Vpcs": [
                {"CidrBlock": "10.0.0.0/16"}]},
            "describe_subnets": lambda **kw: {"Subnets": [{
                "SubnetId": "subnet-1", "AvailabilityZone": "us-east-1a",
                "CidrBlock": "10.0.1.0/24", "AvailableIpAddressCount": 200,
                "MapPublicIpOnLaunch": False}]},
            "describe_security_groups": lambda **kw: {"SecurityGroups": [{
                "GroupId": "sg-1", "GroupName": "eks-cluster",
                "Description": "cluster SG",
                "IpPermissions": [{}], "IpPermissionsEgress": [{}]}]},
            "describe_route_tables": lambda **kw: {"RouteTables": [
                {"RouteTableId": "rtb-1"}]},
        },
        "elbv2": {
            "describe_load_balancers": lambda **kw: {"LoadBalancers": [{
                "LoadBalancerArn": "arn:lb-1", "LoadBalancerName": "web-alb",
                "Type": "application", "Scheme": "internet-facing",
                "DNSName": "web-alb.elb.example.com", "VpcId": "vpc-1"}]},
            "describe_tags": lambda **kw: {"TagDescriptions": [{
                "ResourceArn": "arn:lb-1",
                "Tags": [{"Key": "elbv2.k8s.aws/cluster",
                          "Value": "prod"}]}]},
        },
        "iam": {
            "get_role": lambda **kw: {"Role": {"AssumeRolePolicyDocument": {
                "Statement": [{"Condition": {"StringEquals": {
                    "oidc.example/id:sub":
                        "system:serviceaccount:shop:web-sa"}}}]}}},
            "list_attached_role_policies": lambda **kw: {
                "AttachedPolicies": [
                    {"PolicyArn": "arn:aws:iam::1:policy/s3"}]},
            "list_role_policies": lambda **kw: {"PolicyNames": []},
        },
    }
    return lambda service, region: _FakeAwsClient(apis[service])


def _fake_get_json_factory(cluster):
    """A dict-backed control plane for the one cluster: unknown paths answer
    None (a 404), which the walk treats as an absent resource."""
    responses = {
        "/version": {"gitVersion": "v1.29.4-eks-x"},
        "/apis/apps/v1/deployments": {"items": [{
            "metadata": {"name": "web", "namespace": "shop", "uid": "u-1"},
            "spec": {"replicas": 2, "template": {"spec": {
                "serviceAccountName": "web-sa",
                "containers": [{
                    "name": "app", "image": "1.dkr.ecr.example/web:1.0",
                    "env": [{"name": "AWS_ACCESS_KEY_ID",
                             "value": _AKIA_LITERAL}]}]}}}}]},
        "/api/v1/serviceaccounts": {"items": [{
            "metadata": {"name": "web-sa", "namespace": "shop",
                         "annotations": {"eks.amazonaws.com/role-arn":
                                         "arn:aws:iam::1:role/web"}}}]},
    }

    def get_json(path):
        return responses.get(path.partition("?")[0])

    return get_json


class SeamIntegrationTest(unittest.TestCase):
    """One pass with the real aws_live, k8s_live and live_csv underneath the
    orchestrator — only the boto3 clients and the control-plane reader are
    faked. The per-module tests stub their neighbours; this is where a drift
    in the seams (a renamed key, a changed call shape) fails first."""

    def setUp(self):
        self.result = live_discovery.run_live_discovery(
            _fake_client_factory(), ["us-east-1"], _fake_get_json_factory)
        self.prod = self.result["ir"]["clusters"][0]

    def test_cloud_and_cluster_planes_are_joined(self):
        self.assertEqual(self.prod["name"], "prod")
        self.assertNotIn("kubernetes_error", self.prod)
        self.assertEqual(self.prod["kubernetes"]["server_version"],
                         "v1.29.4-eks-x")
        self.assertEqual(self.result["ir"]["summary"]["clusters_walked"], 1)
        # The ALB found in AWS is attributed to this cluster by its tag.
        self.assertEqual(
            [lb["name"] for lb in self.prod["load_balancers"]], ["web-alb"])

    def test_irsa_binding_carries_the_aws_side_of_the_join(self):
        binding = self.prod["kubernetes"]["identity"]["irsa"][0]
        self.assertEqual(binding["role_arn"], "arn:aws:iam::1:role/web")
        self.assertTrue(binding["role"]["exists"])
        self.assertEqual(binding["role"]["trusted_subjects"],
                         ["system:serviceaccount:shop:web-sa"])
        self.assertEqual(binding["role"]["attached_policies"],
                         ["arn:aws:iam::1:policy/s3"])

    def test_no_key_material_survives_into_ir_or_tables(self):
        everything = (json.dumps(self.result["ir"])
                      + "".join(self.result["tables"].values()))
        self.assertNotIn(_AKIA_LITERAL, everything)

    def test_tables_render_from_the_joined_ir(self):
        tables = self.result["tables"]
        self.assertIn("prod", tables["clusters"])
        self.assertIn("web", tables["workloads"])
        self.assertIn("web-alb", tables["networking"])

    def test_aws_side_tags_and_labels_are_key_names_only(self):
        everything = (json.dumps(self.result["ir"])
                      + "".join(self.result["tables"].values()))
        for value in ("hunter2aws", _ASIA_LITERAL, "tok123aws", "shop", "web"):
            self.assertNotIn(f'"{value}"', json.dumps(self.prod["tags"]))
        self.assertEqual(self.prod["tags"], {
            "team": "<omitted>", "db-password": "<omitted>",
            "backup-key": "<omitted>"})
        self.assertEqual(self.prod["nodegroups"][0]["labels"],
                         {"role": "<omitted>", "api-token": "<omitted>"})
        for leaked in ("hunter2aws", _ASIA_LITERAL, "tok123aws"):
            self.assertNotIn(leaked, everything)
        self.assertNotIn("findings", json.dumps(self.prod))
        # The Deployment's env literal is the marker; its name and the
        # image beside it survive for the inventory.
        container = (self.prod["kubernetes"]["workloads"]["Deployment"][0]
                     ["spec"]["template"]["spec"]["containers"][0])
        self.assertEqual(container["env"],
                         [{"name": "AWS_ACCESS_KEY_ID", "value": "<omitted>"}])
        self.assertEqual(container["image"], "1.dkr.ecr.example/web:1.0")

    def test_joined_ir_matches_its_schema(self):
        live_schema.validate_live_ir(self.result["ir"])

class FifthRoundOrchestrationTest(RunLiveDiscoveryTest):
    """The advice on a 401 whose re-mint failed, and the account the IRSA
    resolution is told about."""

    def test_a_failed_re_mint_advises_fresh_credentials_not_a_mapping(self):
        class Unauthorized(Exception):
            status = 401
            token_refresh_error = RuntimeError("no credentials")

        result = self._run(discover_side_effect=Unauthorized(
            "HTTP 401 Unauthorized: the token was refused and a fresh one "
            "could not be minted for the retry (RuntimeError: no credentials)"))
        joined = "\n".join(result["ir"]["notes"])
        self.assertIn("AWS credentials themselves have expired", joined)
        self.assertNotIn("add an EKS access entry", joined)

    def test_a_pre_call_refresh_failure_tells_one_story(self):
        # The error says a token could not be minted before the call; the
        # advice must not then say the token was refused.
        class Unauthorized(Exception):
            status = 401
            token_refresh_error = RuntimeError("sso session ended")

        result = self._run(discover_side_effect=Unauthorized(
            "HTTP 401 Unauthorized: the token needed refreshing and a fresh "
            "one could not be minted (RuntimeError: sso session ended)"))
        joined = "\n".join(result["ir"]["notes"])
        self.assertNotIn("was refused", joined)
        self.assertIn("No fresh token could be minted", joined)
        self.assertIn("AWS credentials themselves have expired", joined)

    def _resolve_call(self, arns):
        for cluster, arn in zip(self.estate["clusters"], arns):
            if arn:
                cluster["arn"] = arn
        with patch.object(live_discovery.aws_live, "discover_estate",
                          return_value=self.estate), \
             patch.object(live_discovery.k8s_live, "discover_cluster",
                          return_value=_kubernetes_ir()), \
             patch.object(live_discovery.aws_live, "resolve_irsa_roles",
                          return_value=self.roles) as resolve:
            live_discovery.run_live_discovery(
                client_factory=lambda s, r: None, regions=["us-east-1"],
                get_json_factory=self._get_json_factory())
        return resolve.call_args

    def test_the_clusters_account_is_passed_to_the_irsa_resolution(self):
        call = self._resolve_call(["arn:aws:eks:us-east-1:111111111111:cluster/prod",
                                   "arn:aws:eks:us-east-1:111111111111:cluster/staging"])
        self.assertEqual(call.kwargs["account"], "111111111111")
        self.assertEqual(call.kwargs["region"], "us-east-1")

    def test_clusters_across_accounts_or_without_arns_leave_it_unknown(self):
        self.assertIsNone(self._resolve_call([None, None]).kwargs["account"])
        self.setUp()
        self.assertIsNone(self._resolve_call(
            ["arn:aws:eks:us-east-1:111111111111:cluster/prod",
             "arn:aws:eks:us-east-1:222222222222:cluster/staging"]).kwargs["account"])


class NoRegionsTest(unittest.TestCase):

    def test_no_regions_is_refused_before_any_aws_call(self):
        with patch.object(live_discovery.aws_live, "discover_estate") as estate:
            with self.assertRaises(ValueError) as ctx:
                live_discovery.run_live_discovery(
                    client_factory=lambda s, r: None, regions=[],
                    get_json_factory=lambda cluster: object())
        self.assertIn("no regions to scan", str(ctx.exception))
        estate.assert_not_called()


if __name__ == "__main__":
    unittest.main()
