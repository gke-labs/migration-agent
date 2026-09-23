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

"""AWS cloud-plane walk for live discovery.

Deterministic traversal, no boto3 import: every AWS call goes through the
injected `client_factory(service, region)`, which production wires to
boto3 clients (eks_auth.make_client_factory) and tests wire to fakes. The
walk therefore stays importable and testable on a machine with neither
boto3 nor credentials — the same seam datastores.py keeps between the
harvest and the filesystem.

Read-only by construction: every operation used here is a List/Describe/Get.
Nothing in this module can mutate the customer estate.

Failure discipline: one unreadable region, cluster, addon or nodegroup
becomes a note and the walk continues — a 40-cluster estate must not lose
39 clusters to one SCP-denied region, nor a cluster its third nodegroup to
its second's describe error. The notes are persisted with the IR because an
estate that was half-walked must never read as an estate half that size.
"""

import re

from . import projection

# A page cap is a coverage cap, so it is generous and every hit is noted.
_MAX_PAGES = 50
# describe_tags accepts at most 20 load balancer ARNs per call.
_ELBV2_TAG_CHUNK = 20

# Tags the AWS Load Balancer Controller and the legacy in-tree controller
# stamp on the load balancers they own. Ownership by tag, not by name
# pattern: LB names are free-form.
_LB_CLUSTER_TAGS = ("elbv2.k8s.aws/cluster",)
_LB_CLUSTER_TAG_PREFIX = "kubernetes.io/cluster/"


def discover_estate(client_factory, regions, cluster_names=None) -> dict:
    """Walks EKS clusters and their cloud surroundings across regions.

    Returns {"regions", "regions_unreachable", "clusters", "notes"}:
    `regions` is the regions whose clusters were listed, in the order named;
    `regions_unreachable` the named regions that could not be listed (a
    mistyped name, an SCP, a denied eks:ListClusters), each also a note —
    what a scan with no listable region at all is, is the caller's call. A
    region named twice is scanned once and noted, because a duplicate would
    describe and walk every cluster in it twice. `cluster_names`, when
    given, restricts the walk to those names; names that no scanned region
    contains are noted rather than silently ignored, because a typo in a
    cluster name must not read as "that cluster has nothing in it".
    """
    estate = {"regions": [], "regions_unreachable": [], "clusters": [],
              "notes": []}
    notes = estate["notes"]
    wanted = set(cluster_names) if cluster_names else None
    seen = set()
    named = list(regions)
    repeated = sorted({region for region in named if named.count(region) > 1})
    if repeated:
        notes.append("region(s) named more than once, scanned once: "
                     + ", ".join(repeated))

    for region in dict.fromkeys(named):
        try:
            eks = client_factory("eks", region)
            names = _paged(eks.list_clusters, "clusters", notes,
                           f"eks list_clusters in {region}")
        except Exception as e:
            estate["regions_unreachable"].append(region)
            notes.append(f"region {region}: not scanned — {e}")
            continue
        estate["regions"].append(region)
        if wanted is not None:
            names = [name for name in names if name in wanted]
        seen.update(names)
        lbs_by_cluster = _cluster_load_balancers(client_factory, region,
                                                 names, notes)
        for name in names:
            try:
                cluster = _describe_cluster(client_factory, region, name, notes)
            except Exception as e:
                notes.append(f"cluster {name} ({region}): not described — {e}")
                continue
            cluster["load_balancers"] = lbs_by_cluster.get(name, [])
            estate["clusters"].append(cluster)

    if wanted:
        for name in sorted(wanted - seen):
            notes.append(f"requested cluster '{name}' was not found in any "
                         "scanned region")
    return estate


# An IAM role ARN, the one value an IRSA annotation can carry. Anything
# else (a bare role name, a typo) is not sent to GetRole: the account's
# role of that name, if one exists, is not what the annotation binds.
_ROLE_ARN_RE = re.compile(r"^arn:[^:]+:iam::\d+:role/.+")


def resolve_irsa_roles(client_factory, role_arns, region="us-east-1",
                       account=None) -> dict:
    """Fetches trust policy and attached policies for each IRSA role ARN.

    Runs after the in-cluster walk, which is where the ARNs come from
    (eks.amazonaws.com/role-arn annotations). This is the half of IRSA the
    manifests cannot show: whether the role behind the annotation still
    exists, whom its trust policy actually admits, and what it grants —
    exactly the cloud binding a static scan takes on faith. Three outcomes
    are told apart, because they are three different migration findings:
    `exists` true with the detail; `exists` false when IAM says the role is
    gone (NoSuchEntity) — the annotation dangles; and `exists` null with
    the error when the role could not be read — the credentials were
    denied GetRole, or the role lives in another account than the clusters
    (`account`, from their ARNs) and is not readable from this one at
    all, so IAM is not even asked — nor for an annotation that is no role
    ARN at all (a bare name): GetRole would answer for whatever role of
    that name this account has, which IRSA cannot assume, so the entry is
    `exists` null with an error saying so. `exists` answers for GetRole
    alone: a
    policy list that could not be read once the role was (ListRolePolicies
    denied, say) leaves `exists` true and the trust policy in place, omits
    that list, and says so in the entry's `notes` — a role that was read
    is not reported as unreadable. A failure to build the IAM client is
    not per-role and propagates to the caller. IAM is global, but the
    client still needs a region to pick an endpoint, and the endpoint's
    partition must match the credential's — so the caller passes a region
    it just scanned rather than a hardcoded one.
    """
    resolved = {}
    iam = client_factory("iam", region)
    for arn in sorted(set(role_arns)):
        entry = {"role_arn": arn}
        role_notes = []
        if not _ROLE_ARN_RE.match(arn):
            entry["exists"] = None
            entry["error"] = ("annotation is not an IAM role ARN; IRSA "
                              "cannot assume it")
            resolved[arn] = entry
            continue
        role_account = arn.split(":")[4] if arn.count(":") >= 5 else None
        if account and role_account and role_account != account:
            entry["exists"] = None
            entry["error"] = (f"role is in account {role_account}, the "
                              f"clusters in {account}: not readable with "
                              "these credentials")
            resolved[arn] = entry
            continue
        role_name = arn.rsplit("/", 1)[-1]
        try:
            role = iam.get_role(RoleName=role_name).get("Role", {})
        except Exception as e:
            entry["exists"] = False if _is_no_such_entity(e) else None
            entry["error"] = str(e)
            resolved[arn] = entry
            continue
        entry["exists"] = True
        entry["trusted_subjects"] = _trusted_subjects(
            role.get("AssumeRolePolicyDocument"))
        try:
            entry["attached_policies"] = [
                policy.get("PolicyArn")
                for policy in _paged(
                    lambda **kw: iam.list_attached_role_policies(
                        RoleName=role_name, **kw),
                    "AttachedPolicies", role_notes, f"policies of {role_name}",
                    request_token="Marker", response_token="Marker")]
        except Exception as e:
            role_notes.append(f"attached policies of {role_name}: not read — {e}")
        try:
            entry["inline_policy_names"] = _paged(
                lambda **kw: iam.list_role_policies(RoleName=role_name, **kw),
                "PolicyNames", role_notes, f"inline policies of {role_name}",
                request_token="Marker", response_token="Marker")
        except Exception as e:
            role_notes.append(f"inline policies of {role_name}: not read — {e}")
        if role_notes:
            entry["notes"] = role_notes
        resolved[arn] = entry
    return resolved


def _is_no_such_entity(error) -> bool:
    """Whether an IAM error says the role does not exist (as opposed to
    could not be read): botocore's ClientError carries the code in its
    response; anything else is judged by its text."""
    response = getattr(error, "response", None)
    if isinstance(response, dict):
        code = (response.get("Error") or {}).get("Code")
        if code:
            return code == "NoSuchEntity"
    return "NoSuchEntity" in str(error)


def _describe_cluster(client_factory, region, name, notes) -> dict:
    eks = client_factory("eks", region)
    raw = eks.describe_cluster(name=name).get("cluster", {})
    vpc_config = raw.get("resourcesVpcConfig", {})
    oidc_issuer = ((raw.get("identity") or {}).get("oidc") or {}).get("issuer")
    cluster = {
        "name": name,
        "region": region,
        "arn": raw.get("arn"),
        "status": raw.get("status"),
        "kubernetes_version": raw.get("version"),
        "platform_version": raw.get("platformVersion"),
        "endpoint": raw.get("endpoint"),
        # Kept in the IR: reconnecting to the cluster later needs it, and a
        # CA certificate is not a credential.
        "certificate_authority_data": (raw.get("certificateAuthority") or {}).get("data"),
        "endpoint_access": {
            "public": vpc_config.get("endpointPublicAccess"),
            "private": vpc_config.get("endpointPrivateAccess"),
            "public_access_cidrs": vpc_config.get("publicAccessCidrs") or [],
        },
        "oidc_issuer": oidc_issuer,
        "logging_enabled": [
            log_type
            for setup in (raw.get("logging") or {}).get("clusterLogging") or []
            if setup.get("enabled")
            for log_type in setup.get("types") or []],
        "tags": projection.keys_only(raw.get("tags")),
        "vpc": {
            "vpc_id": vpc_config.get("vpcId"),
            "subnet_ids": vpc_config.get("subnetIds") or [],
            "security_group_ids": vpc_config.get("securityGroupIds") or [],
            "cluster_security_group_id": vpc_config.get("clusterSecurityGroupId"),
        },
    }
    cluster["addons"] = _addons(eks, name, notes)
    cluster["nodegroups"] = _nodegroups(eks, name, notes)
    cluster["autoscaling_groups"] = _autoscaling_groups(
        client_factory, region, cluster["nodegroups"], notes)
    cluster["network"] = _network_detail(client_factory, region,
                                         cluster["vpc"], notes)
    return cluster


def _addons(eks, cluster_name, notes) -> list:
    """The cluster's addons: the listing in one try, each describe in its
    own, so one addon that cannot be described is one note and the others
    stand."""
    addons = []
    try:
        names = _paged(
            lambda **kw: eks.list_addons(clusterName=cluster_name, **kw),
            "addons", notes, f"addons of {cluster_name}")
    except Exception as e:
        notes.append(f"cluster {cluster_name}: addons not read — {e}")
        return addons
    for addon_name in names:
        try:
            detail = eks.describe_addon(
                clusterName=cluster_name, addonName=addon_name).get("addon", {})
        except Exception as e:
            notes.append(
                f"cluster {cluster_name}: addon {addon_name} not read — {e}")
            continue
        addons.append({
            "name": addon_name,
            "version": detail.get("addonVersion"),
            "status": detail.get("status"),
            "service_account_role_arn": detail.get("serviceAccountRoleArn"),
        })
    return addons


def _nodegroups(eks, cluster_name, notes) -> list:
    """The cluster's managed nodegroups: the listing in one try, each
    describe in its own, so one nodegroup that cannot be described is one
    note naming it and the others — and their ASGs — stand."""
    nodegroups = []
    try:
        names = _paged(
            lambda **kw: eks.list_nodegroups(clusterName=cluster_name, **kw),
            "nodegroups", notes, f"nodegroups of {cluster_name}")
    except Exception as e:
        notes.append(f"cluster {cluster_name}: nodegroups not read — {e}")
        return nodegroups
    for nodegroup_name in names:
        try:
            raw = eks.describe_nodegroup(
                clusterName=cluster_name,
                nodegroupName=nodegroup_name).get("nodegroup", {})
        except Exception as e:
            notes.append(f"cluster {cluster_name}: nodegroup {nodegroup_name} "
                         f"not read — {e}")
            continue
        scaling = raw.get("scalingConfig") or {}
        nodegroups.append({
            "name": nodegroup_name,
            "status": raw.get("status"),
            "capacity_type": raw.get("capacityType"),
            "instance_types": raw.get("instanceTypes") or [],
            "ami_type": raw.get("amiType"),
            "release_version": raw.get("releaseVersion"),
            "disk_size_gb": raw.get("diskSize"),
            "scaling": {"min": scaling.get("minSize"),
                        "max": scaling.get("maxSize"),
                        "desired": scaling.get("desiredSize")},
            "labels": projection.keys_only(raw.get("labels")),
            "taints": [
                {"key": taint.get("key"), "value": taint.get("value"),
                 "effect": taint.get("effect")}
                for taint in raw.get("taints") or []],
            "subnet_ids": raw.get("subnets") or [],
            "node_role_arn": raw.get("nodeRole"),
            "launch_template": (raw.get("launchTemplate") or {}).get("id"),
            "asg_names": [
                group.get("name")
                for group in (raw.get("resources") or {})
                .get("autoScalingGroups") or []],
        })
    return nodegroups


_ASG_NAMES_PER_CALL = 50


def _autoscaling_groups(client_factory, region, nodegroups, notes) -> list:
    """AZ spread and live instance counts for the nodegroups' ASGs.

    Secondary detail (the scaling intent already came from the nodegroup),
    but it is the only place the actual running instance count and the
    concrete AZ layout show up — the drift a static scan cannot see.
    DescribeAutoScalingGroups takes at most 50 names per call, and each
    call asks for that many records back — the service's page size is 50
    today, but a page smaller than the names sent would silently drop the
    rest of the chunk. Each call stands alone, so a throttled chunk is
    noted by its names and the groups the other calls returned are kept.
    """
    names = [name for nodegroup in nodegroups
             for name in nodegroup.get("asg_names", []) if name]
    if not names:
        return []
    try:
        autoscaling = client_factory("autoscaling", region)
    except Exception as e:
        notes.append(f"ASGs {names} in {region}: not read — {e}")
        return []
    raw_groups = []
    for start in range(0, len(names), _ASG_NAMES_PER_CALL):
        chunk = names[start:start + _ASG_NAMES_PER_CALL]
        try:
            # A chunk fits one page by construction (MaxRecords equals the
            # names sent), and the token is read all the same: the service,
            # not this code, decides when a page is full.
            raw_groups.extend(_paged(
                lambda **kw: autoscaling.describe_auto_scaling_groups(
                    AutoScalingGroupNames=chunk,
                    MaxRecords=_ASG_NAMES_PER_CALL, **kw),
                "AutoScalingGroups", notes, f"ASGs {chunk} in {region}",
                request_token="NextToken", response_token="NextToken"))
        except Exception as e:
            notes.append(f"ASGs {chunk} in {region}: not read — {e}")
    return [{
        "name": group.get("AutoScalingGroupName"),
        "min": group.get("MinSize"),
        "max": group.get("MaxSize"),
        "desired": group.get("DesiredCapacity"),
        "running_instances": len(group.get("Instances") or []),
        "availability_zones": group.get("AvailabilityZones") or [],
    } for group in raw_groups]


def _network_detail(client_factory, region, vpc_ref, notes) -> dict:
    """VPC CIDRs, subnet layout, security groups; route tables as counts.

    Route tables are deliberately thin — GKE's topology comes from subnet
    IP allocation and Cloud NAT, so their contents inform nothing the
    translation generates; the count records that they were seen.
    """
    detail = {"vpc": None, "subnets": [], "security_groups": [],
              "route_tables": {"count": 0, "ids": []}}
    vpc_id = vpc_ref.get("vpc_id")
    if not vpc_id:
        return detail
    try:
        ec2 = client_factory("ec2", region)
        vpcs = ec2.describe_vpcs(VpcIds=[vpc_id]).get("Vpcs", [])
        if vpcs:
            vpc = vpcs[0]
            detail["vpc"] = {
                "vpc_id": vpc_id,
                "cidr_blocks": [assoc.get("CidrBlock")
                                for assoc in vpc.get("CidrBlockAssociationSet")
                                or [{"CidrBlock": vpc.get("CidrBlock")}]],
            }
        subnet_ids = vpc_ref.get("subnet_ids") or []
        if subnet_ids:
            detail["subnets"] = [{
                "subnet_id": subnet.get("SubnetId"),
                "availability_zone": subnet.get("AvailabilityZone"),
                "cidr_block": subnet.get("CidrBlock"),
                "available_ips": subnet.get("AvailableIpAddressCount"),
                "public": subnet.get("MapPublicIpOnLaunch"),
                "tags": projection.keys_only(
                    {tag.get("Key"): tag.get("Value")
                     for tag in subnet.get("Tags") or []}),
            } for subnet in ec2.describe_subnets(SubnetIds=subnet_ids)
                .get("Subnets", [])]
        group_ids = list(vpc_ref.get("security_group_ids") or [])
        if vpc_ref.get("cluster_security_group_id"):
            group_ids.append(vpc_ref["cluster_security_group_id"])
        if group_ids:
            # Identifiers and rule counts; the group's free-text
            # description stays behind like any other customer prose.
            detail["security_groups"] = [{
                "group_id": group.get("GroupId"),
                "group_name": group.get("GroupName"),
                "ingress_rule_count": len(group.get("IpPermissions") or []),
                "egress_rule_count": len(group.get("IpPermissionsEgress") or []),
            } for group in ec2.describe_security_groups(
                GroupIds=sorted(set(group_ids))).get("SecurityGroups", [])]
        tables = ec2.describe_route_tables(
            Filters=[{"Name": "vpc-id", "Values": [vpc_id]}]
        ).get("RouteTables", [])
        detail["route_tables"] = {
            "count": len(tables),
            "ids": [table.get("RouteTableId") for table in tables],
        }
    except Exception as e:
        notes.append(f"VPC {vpc_id} ({region}): network detail incomplete — {e}")
    return detail


def _cluster_load_balancers(client_factory, region, cluster_names, notes) -> dict:
    """ALBs/NLBs in the region, attributed to clusters by controller tag."""
    if not cluster_names:
        return {}
    try:
        elbv2 = client_factory("elbv2", region)
        load_balancers = _paged(
            elbv2.describe_load_balancers, "LoadBalancers", notes,
            f"load balancers in {region}",
            request_token="Marker", response_token="NextMarker")
    except Exception as e:
        notes.append(f"load balancers in {region}: not read — {e}")
        return {}
    by_arn = {lb.get("LoadBalancerArn"): lb for lb in load_balancers
              if lb.get("LoadBalancerArn")}
    arns = list(by_arn)
    attributed = {}
    for start in range(0, len(arns), _ELBV2_TAG_CHUNK):
        chunk = arns[start:start + _ELBV2_TAG_CHUNK]
        try:
            descriptions = elbv2.describe_tags(
                ResourceArns=chunk).get("TagDescriptions", [])
        except Exception as e:
            notes.append(f"load balancer tags in {region}: partial — {e}")
            continue
        for description in descriptions:
            tags = {tag.get("Key"): tag.get("Value")
                    for tag in description.get("Tags") or []}
            owner = _lb_owner(tags, cluster_names)
            if owner is None:
                continue
            lb = by_arn.get(description.get("ResourceArn"), {})
            attributed.setdefault(owner, []).append({
                "name": lb.get("LoadBalancerName"),
                "arn": lb.get("LoadBalancerArn"),
                "type": lb.get("Type"),
                "scheme": lb.get("Scheme"),
                "dns_name": lb.get("DNSName"),
                "vpc_id": lb.get("VpcId"),
                # The two tags the in-cluster controller writes, naming
                # the Ingress group or Service this balancer fronts: the
                # join back to the workload, not customer free text. Every
                # other tag stays behind.
                "ingress_group": tags.get("ingress.k8s.aws/stack"),
                "service": tags.get("kubernetes.io/service-name"),
            })
    return attributed


def _lb_owner(tags, cluster_names):
    for tag_key in _LB_CLUSTER_TAGS:
        if tags.get(tag_key) in cluster_names:
            return tags[tag_key]
    for key in tags:
        if key.startswith(_LB_CLUSTER_TAG_PREFIX):
            name = key[len(_LB_CLUSTER_TAG_PREFIX):]
            if name in cluster_names:
                return name
    return None


def _trusted_subjects(trust_document) -> list:
    """The service-account subjects an IRSA trust policy admits.

    boto3 returns the document already URL-decoded and parsed. Every
    `...:sub` condition value is collected, StringEquals and StringLike
    alike — a wildcard subject is worth surfacing, not normalizing away.
    """
    subjects = []
    if not isinstance(trust_document, dict):
        return subjects
    statements = trust_document.get("Statement")
    if isinstance(statements, dict):
        statements = [statements]
    for statement in statements or []:
        for operator_block in (statement.get("Condition") or {}).values():
            if not isinstance(operator_block, dict):
                continue
            for condition_key, value in operator_block.items():
                if not re.search(r":sub$", condition_key):
                    continue
                if isinstance(value, str):
                    subjects.append(value)
                elif isinstance(value, list):
                    subjects.extend(v for v in value if isinstance(v, str))
    return subjects


def _paged(call, result_key, notes, what,
           request_token="nextToken", response_token="nextToken") -> list:
    """Drains a paginated List call into one list.

    Manual token loop rather than boto3 paginators so fakes stay plain
    dict-returning callables. The page cap exists to bound a misbehaving
    endpoint, and hitting it is recorded — a capped listing must not read
    as a complete one.
    """
    results = []
    kwargs = {}
    for _ in range(_MAX_PAGES):
        response = call(**kwargs)
        results.extend(response.get(result_key) or [])
        token = response.get(response_token)
        if not token or (response_token == "Marker"
                         and not response.get("IsTruncated")):
            return results
        kwargs = {request_token: token}
    if isinstance(notes, list):
        notes.append(f"{what}: listing stopped after {_MAX_PAGES} pages — "
                     "results beyond that are missing")
    return results
