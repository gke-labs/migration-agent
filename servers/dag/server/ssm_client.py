import logging
import time
import re
from google.cloud import securesourcemanager_v1
from google.api_core import exceptions
from google.longrunning import operations_pb2
from typing import Dict, Any, Optional

logger = logging.getLogger("migration-dag")

# Secure Source Manager hostnames, as documented for an instance:
#   INSTANCE_ID-PROJECT_NUMBER.LOCATION.sourcemanager.dev        web UI
#   INSTANCE_ID-PROJECT_NUMBER-git.LOCATION.sourcemanager.dev    git over HTTPS
#   INSTANCE_ID-PROJECT_NUMBER-ssh.LOCATION.sourcemanager.dev    git over SSH
#   INSTANCE_ID-PROJECT_NUMBER-api.LOCATION.sourcemanager.dev    data-plane API
# A Private Service Connect instance inserts a `p` label: LOCATION.p.sourcemanager.dev.
# The repository path is PROJECT_ID/REPOSITORY_ID, optionally followed by `.git`.
# https://cloud.google.com/secure-source-manager/docs/create-instance
# https://cloud.google.com/secure-source-manager/docs/create-clone-repository
SSM_URL_PATTERN = re.compile(
    r"^(?:https|ssh)://(?:[A-Za-z0-9._-]+@)?"
    r"(?P<instance>[a-z][a-z0-9-]*?)-(?P<project_num>\d+)(?:-(?P<endpoint>api|git|ssh))?"
    r"\.(?P<location>[a-z0-9-]+?)(?P<psc>\.p)?\.sourcemanager\.dev(?::\d+)?"
    r"/(?P<project_id>[a-z][a-z0-9-]{4,28}[a-z0-9])"
    r"/(?P<repo>[A-Za-z0-9][A-Za-z0-9._-]*?)(?:\.git)?/?$"
)

# scp-style SSH remote: git@HOST:PROJECT_ID/REPO.git
_SCP_STYLE_RE = re.compile(r"^(?P<user>[A-Za-z0-9._-]+)@(?P<host>[A-Za-z0-9.-]+):(?P<path>[^:]+)$")


def parse_ssm_url(url: str) -> Dict[str, str]:
    """Parses a Secure Source Manager repository URL into its GCP resource components.

    Accepts the web UI, git-over-HTTPS and git-over-SSH forms of a repository URL,
    with or without a trailing `.git`, for both public and Private Service Connect
    instances. `project` is the project *number* carried by the hostname (valid in
    `projects/{project}/...` resource names); `project_id` is the ID from the path.
    """
    candidate = url.strip()
    scp = _SCP_STYLE_RE.match(candidate)
    if scp:
        candidate = f"ssh://{scp.group('user')}@{scp.group('host')}/{scp.group('path')}"
    match = SSM_URL_PATTERN.match(candidate)
    if not match:
        raise ValueError(f"URL is not a valid Secure Source Manager repository URL: {url}")

    components = match.groupdict()
    return {
        "project": components["project_num"],
        "project_id": components["project_id"],
        "location": components["location"],
        "instance": components["instance"],
        "repository": components["repo"],
        "endpoint": components["endpoint"] or "html",
        "private_service_connect": components["psc"] is not None,
    }

class SSMClient:
    def __init__(self):
        self._clients = {}

    def _get_client(self, location: str) -> securesourcemanager_v1.SecureSourceManagerClient:
        if location not in self._clients:
            endpoint = f"securesourcemanager.{location}.rep.googleapis.com"
            logger.debug(f"Initializing SSM client for regional endpoint: {endpoint}")
            client_options = {"api_endpoint": endpoint}
            self._clients[location] = securesourcemanager_v1.SecureSourceManagerClient(client_options=client_options)
        return self._clients[location]

    def check_repository_exists(self, repo_url: str) -> bool:
        """Checks if a repository exists in Secure Source Manager."""
        try:
            comps = parse_ssm_url(repo_url)
        except ValueError:
            return False
        
        repo = self.get_repository_by_details(comps["project"], comps["location"], comps["instance"], comps["repository"])
        return repo is not None

    def create_repository(self, repo_url: str) -> bool:
        """Creates a new repository in Secure Source Manager and blocks until completion."""
        comps = parse_ssm_url(repo_url)
        try:
            self.create_repository_by_details(comps["project"], comps["location"], comps["instance"], comps["repository"])
            return True
        except Exception:
            raise

    def get_repository_by_details(self, project: str, location: str, instance: str, repository: str) -> securesourcemanager_v1.Repository | None:
        """Retrieves a repository resource from Secure Source Manager, returning None if not found."""
        repo_resource_name = f"projects/{project}/locations/{location}/repositories/{repository}"
        logger.debug(f"Calling SSM get_repository for resource: {repo_resource_name}")
        client = self._get_client(location)
        try:
            return client.get_repository(name=repo_resource_name)
        except exceptions.NotFound:
            logger.debug(f"SSM repository {repo_resource_name} was not found.")
            return None
        except exceptions.PermissionDenied as e:
            logger.error(f"Permission denied accessing SSM repository {repo_resource_name}: {e}")
            raise PermissionError(
                f"Permission Denied. You do not have access to Secure Source Manager in project '{project}'. "
                f"Ensure your Google identity has the 'roles/securesourcemanager.instanceAccessor' or 'roles/securesourcemanager.repoAdmin' role. Details: {e}"
            )
        except Exception as e:
            logger.error(f"Error retrieving SSM repository {repo_resource_name}: {e}")
            raise

    def get_instance_by_details(self, project: str, location: str, instance: str) -> securesourcemanager_v1.Instance | None:
        """Retrieves an instance resource from Secure Source Manager, returning None if not found."""
        instance_resource_name = f"projects/{project}/locations/{location}/instances/{instance}"
        logger.debug(f"Calling SSM get_instance for resource: {instance_resource_name}")
        client = self._get_client(location)
        try:
            return client.get_instance(name=instance_resource_name)
        except exceptions.NotFound:
            logger.debug(f"SSM instance {instance_resource_name} was not found.")
            return None
        except exceptions.PermissionDenied as e:
            logger.error(f"Permission denied accessing SSM instance {instance_resource_name}: {e}")
            raise PermissionError(
                f"Permission Denied. You do not have permission to access Secure Source Manager instances in project '{project}'. "
                f"Ensure your Google identity has the 'roles/securesourcemanager.instanceAccessor' or 'roles/securesourcemanager.admin' role. Details: {e}"
            )
        except Exception as e:
            logger.error(f"Error retrieving SSM instance {instance_resource_name}: {e}")
            raise

    def create_instance_by_details(self, project: str, location: str, instance_id: str) -> securesourcemanager_v1.Instance:
        """Creates a new instance in Secure Source Manager and blocks until LRO completion."""
        parent = f"projects/{project}/locations/{location}"
        logger.info(f"Creating SSM instance {instance_id} under parent location {parent}")
        
        instance = securesourcemanager_v1.Instance()
        
        client = self._get_client(location)
        try:
            operation = client.create_instance(
                parent=parent,
                instance=instance,
                instance_id=instance_id
            )
            logger.debug(f"SSM create_instance LRO started: {operation.operation.name}")
            
            timeout = 1800.0
            start_time = time.time()
            while not operation.done():
                if time.time() - start_time > timeout:
                    raise TimeoutError(f"SSM instance creation timed out after {timeout} seconds.")
                time.sleep(10)
                
            res = operation.result()
            logger.info(f"SSM instance {instance_id} created successfully.")
            return res
        except exceptions.AlreadyExists:
            logger.info(f"SSM instance {instance_id} already exists, fetching details.")
            return client.get_instance(name=f"{parent}/instances/{instance_id}")
        except exceptions.PermissionDenied as e:
            logger.error(f"Permission denied creating SSM instance {instance_id}: {e}")
            raise PermissionError(
                f"Permission Denied. You do not have permissions to create Secure Source Manager instances in project '{project}'. "
                f"Ensure your Google identity has the 'roles/securesourcemanager.admin' role. Details: {e}"
            )
        except Exception as e:
            logger.error(f"Failed to create SSM instance: {e}")
            raise

    def create_repository_by_details(self, project: str, location: str, instance: str, repository: str) -> securesourcemanager_v1.Repository:
        """Creates a new repository in Secure Source Manager and blocks until LRO completion. Auto-provisions instance if missing."""
        inst = self.get_instance_by_details(project, location, instance)
        if not inst:
            logger.info(f"SSM instance '{instance}' not found in location '{location}'. Creating instance first...")
            self.create_instance_by_details(project, location, instance)
        elif inst.state == securesourcemanager_v1.Instance.State.CREATING:
            logger.info(f"SSM instance '{instance}' is currently provisioning. Waiting for active status...")
            timeout = 1800.0
            start_time = time.time()
            while inst.state == securesourcemanager_v1.Instance.State.CREATING:
                if time.time() - start_time > timeout:
                    raise TimeoutError(f"Timed out waiting for SSM instance '{instance}' to become active.")
                time.sleep(10)
                inst = self.get_instance_by_details(project, location, instance)
                if not inst:
                    raise ValueError(f"SSM instance '{instance}' disappeared while provisioning.")
        
        if inst and inst.state not in (securesourcemanager_v1.Instance.State.ACTIVE, securesourcemanager_v1.Instance.State.CREATING):
            raise ValueError(f"SSM instance '{instance}' exists but is in an invalid state for repository creation: {inst.state.name}")

        parent = f"projects/{project}/locations/{location}"
        instance_resource = f"projects/{project}/locations/{location}/instances/{instance}"
        logger.info(f"Creating SSM repository {repository} under parent location {parent} (instance: {instance_resource})")
        
        repo = securesourcemanager_v1.Repository()
        repo.instance = instance_resource
        
        client = self._get_client(location)
        try:
            operation = client.create_repository(
                parent=parent,
                repository=repo,
                repository_id=repository
            )
            logger.debug(f"SSM create_repository LRO started: {operation.operation.name}")
            
            timeout = 60.0
            start_time = time.time()
            while not operation.done():
                if time.time() - start_time > timeout:
                    raise TimeoutError(f"SSM repository creation timed out after {timeout} seconds.")
                time.sleep(2)
                
            res = operation.result()
            logger.info(f"SSM repository {repository} created successfully.")
            return res
        except exceptions.AlreadyExists:
            logger.info(f"SSM repository {repository} already exists, fetching details.")
            return client.get_repository(name=f"{parent}/repositories/{repository}")
        except exceptions.NotFound as e:
            logger.error(f"Failed to create SSM repository (Not Found): {e}")
            if "instances/" in str(e):
                raise ValueError(
                    f"SSM instance '{instance}' was not found in location '{location}' under project '{project}'. "
                    f"Secure Source Manager instances must be created by administrators before repositories can be provisioned. "
                    f"Please verify the instance name or create it in GCP Console first."
                )
            raise
        except exceptions.PermissionDenied as e:
            logger.error(f"Permission denied creating SSM repository {repository}: {e}")
            raise PermissionError(
                f"Permission Denied. You do not have permissions to create Secure Source Manager repositories in project '{project}'. "
                f"Ensure your Google identity has the 'roles/securesourcemanager.repoAdmin' or 'roles/securesourcemanager.admin' role. Details: {e}"
            )
        except Exception as e:
            logger.error(f"Failed to create SSM repository: {e}")
            raise

    def trigger_create_instance_by_details(self, project: str, location: str, instance_id: str) -> str:
        """Triggers instance creation and returns the LRO operation name immediately."""
        parent = f"projects/{project}/locations/{location}"
        logger.info(f"Triggering SSM instance {instance_id} creation under parent location {parent}")
        
        instance = securesourcemanager_v1.Instance()
        client = self._get_client(location)
        try:
            operation = client.create_instance(
                parent=parent,
                instance=instance,
                instance_id=instance_id
            )
            lro_name = operation.operation.name
            if lro_name.startswith("operations/"):
                op_id = lro_name.split("/", 1)[1]
                lro_name = f"projects/{project}/locations/{location}/operations/{op_id}"
            logger.info(f"SSM instance creation LRO started: {lro_name}")
            return lro_name
        except exceptions.PermissionDenied as e:
            logger.error(f"Permission denied creating SSM instance {instance_id}: {e}")
            raise PermissionError(
                f"Permission Denied. You do not have permissions to create Secure Source Manager instances in project '{project}'. "
                f"Ensure your Google identity has the 'roles/securesourcemanager.admin' role. Details: {e}"
            )
        except Exception as e:
            logger.error(f"Failed to trigger SSM instance creation: {e}")
            raise

    def trigger_create_repository_by_details(self, project: str, location: str, instance: str, repository: str) -> str:
        """Triggers repository creation and returns the LRO operation name immediately."""
        parent = f"projects/{project}/locations/{location}"
        instance_resource = f"projects/{project}/locations/{location}/instances/{instance}"
        logger.info(f"Triggering SSM repository {repository} creation under parent {parent} (instance: {instance_resource})")
        
        repo = securesourcemanager_v1.Repository()
        repo.instance = instance_resource
        
        client = self._get_client(location)
        try:
            operation = client.create_repository(
                parent=parent,
                repository=repo,
                repository_id=repository
            )
            lro_name = operation.operation.name
            if lro_name.startswith("operations/"):
                op_id = lro_name.split("/", 1)[1]
                lro_name = f"projects/{project}/locations/{location}/operations/{op_id}"
            logger.info(f"SSM repository creation LRO started: {lro_name}")
            return lro_name
        except exceptions.PermissionDenied as e:
            logger.error(f"Permission denied creating SSM repository {repository}: {e}")
            raise PermissionError(
                f"Permission Denied. You do not have permissions to create Secure Source Manager repositories in project '{project}'. "
                f"Ensure your Google identity has the 'roles/securesourcemanager.repoAdmin' or 'roles/securesourcemanager.admin' role. Details: {e}"
            )
        except Exception as e:
            logger.error(f"Failed to trigger SSM repository creation: {e}")
            raise

    def get_operation_status(self, location: str, operation_name: str) -> tuple[bool, Optional[str], Optional[Any]]:
        """Checks the status of an LRO operation.
        
        Returns:
            (done, error_message, response_any)
        """
        logger.debug(f"Checking SSM operation status for: {operation_name}")
        client = self._get_client(location)
        try:
            op_proto = client.transport.operations_client.get_operation(name=operation_name)
            
            if not op_proto.done:
                return False, None, None
                
            if op_proto.HasField("error"):
                return True, op_proto.error.message, None
                
            return True, None, op_proto.response
        except exceptions.NotFound as e:
            logger.warning(f"Operation {operation_name} not found. It might be transient (eventual consistency). Treating as pending: {e}")
            return False, None, None
        except Exception as e:
            logger.error(f"Failed to check operation status {operation_name}: {e}")
            raise

    def create_pull_request(self, repo_url: str, title: str, body: str, source_branch: str, target_branch: str) -> str:
        """Creates a Pull Request in Secure Source Manager and returns the PR console URL."""
        comps = parse_ssm_url(repo_url)
        project = comps["project"]
        location = comps["location"]
        instance = comps["instance"]
        repository = comps["repository"]
        
        parent = f"projects/{project}/locations/{location}/repositories/{repository}"
        logger.info(f"Creating SSM Pull Request '{title}' under repository '{parent}'")
        
        pull_request = {
            "title": title,
            "body": body,
            "base": {"ref": f"refs/heads/{target_branch}"},
            "head": {"ref": f"refs/heads/{source_branch}"}
        }
        
        client = self._get_client(location)
        try:
            operation = client.create_pull_request(
                parent=parent,
                pull_request=pull_request
            )
            logger.debug(f"SSM create_pull_request LRO started: {operation.operation.name}")
            
            # Wait for creation to complete (usually very fast)
            timeout = 60.0
            start_time = time.time()
            while not operation.done():
                if time.time() - start_time > timeout:
                    raise TimeoutError(f"SSM Pull Request creation timed out after {timeout} seconds.")
                time.sleep(2)
                
            res = operation.result()
            pr_name = res.name
            logger.info(f"SSM Pull Request created successfully: {pr_name}")
            
            pr_id = pr_name.split("/")[-1]
            pr_console_url = f"https://console.cloud.google.com/secure-source-manager/locations/{location}/instances/{instance}/repositories/{repository}/pull-requests/{pr_id}?project={project}"
            return pr_console_url
        except exceptions.PermissionDenied as e:
            logger.error(f"Permission denied creating SSM Pull Request: {e}")
            raise PermissionError(
                f"Permission Denied. You do not have permissions to create Pull Requests in SSM repository '{repository}'. Details: {e}"
            )
        except Exception as e:
            logger.error(f"Failed to create SSM Pull Request: {e}")
            raise
