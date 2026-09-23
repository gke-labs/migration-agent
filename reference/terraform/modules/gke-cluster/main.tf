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

###############################################################################
# GKE Agentic Migration — GKE cluster module                                                #
# Production-grade GKE cluster opinionated for EKS-to-GKE migrations.          #
###############################################################################

variable "project_id" {
  description = "GCP project hosting the cluster."
  type        = string
}

variable "name" {
  description = "Cluster name."
  type        = string
}

variable "region" {
  description = "Regional cluster location (e.g., us-central1)."
  type        = string
}

variable "network_project" {
  description = "Project hosting the Shared VPC (host project). May equal project_id."
  type        = string
}

variable "network" {
  description = "Shared VPC name."
  type        = string
}

variable "subnet" {
  description = "Subnet name within the Shared VPC."
  type        = string
}

variable "pods_range_name" {
  description = "Secondary range name for pods."
  type        = string
  default     = "pods"
}

variable "services_range_name" {
  description = "Secondary range name for services."
  type        = string
  default     = "services"
}

variable "master_ipv4_cidr_block" {
  description = "CIDR for the cluster control plane endpoint (private)."
  type        = string
}

variable "authorized_networks" {
  description = "CIDRs allowed to reach the control plane endpoint."
  type        = list(string)
  default     = []
}

variable "release_channel" {
  description = "RAPID, REGULAR, or STABLE."
  type        = string
  default     = "REGULAR"
}

variable "kms_key_id" {
  description = "Cloud KMS key for cluster DB encryption (CMEK)."
  type        = string
}

variable "enable_binary_authorization" {
  description = "Whether to enable Binary Authorization."
  type        = bool
  default     = true
}

variable "binary_authorization_mode" {
  description = "PROJECT_SINGLETON_POLICY_ENFORCE or DISABLED (start in EVALUATION via policy)."
  type        = string
  default     = "PROJECT_SINGLETON_POLICY_ENFORCE"
}

variable "deletion_protection" {
  description = "Prevent accidental cluster deletion."
  type        = bool
  default     = true
}

# Cluster DNS. Cloud DNS for GKE is the standing default; kube-dns is not an
# option here. Cluster scope behaves like kube-dns (names resolve inside the
# cluster, stub domains and upstream nameservers come from the kube-dns
# ConfigMap); VPC scope publishes the cluster's Services to the whole VPC
# under a custom domain and ignores that ConfigMap. Autopilot clusters get
# Cloud DNS cluster scope and NodeLocal DNSCache regardless of these values.
variable "cluster_dns_scope" {
  description = "CLUSTER_SCOPE (default) or VPC_SCOPE. VPC scope needs cluster_dns_domain, is immutable once set, and is Standard-only."
  type        = string
  default     = "CLUSTER_SCOPE"
  validation {
    condition     = contains(["CLUSTER_SCOPE", "VPC_SCOPE"], var.cluster_dns_scope)
    error_message = "cluster_dns_scope must be CLUSTER_SCOPE or VPC_SCOPE."
  }
}

variable "cluster_dns_domain" {
  description = "Custom cluster domain, required under VPC_SCOPE (unique in the VPC, must not end in .local). Ignored under CLUSTER_SCOPE."
  type        = string
  default     = null
}

variable "enable_node_local_dns" {
  description = "NodeLocal DNSCache. Enabling or disabling it on an existing cluster recreates every node."
  type        = bool
  default     = true
}

###############################################################################
# Cluster                                                                      #
###############################################################################

resource "google_container_cluster" "this" {
  project  = var.project_id
  name     = var.name
  location = var.region

  network    = "projects/${var.network_project}/global/networks/${var.network}"
  subnetwork = "projects/${var.network_project}/regions/${var.region}/subnetworks/${var.subnet}"

  ip_allocation_policy {
    cluster_secondary_range_name  = var.pods_range_name
    services_secondary_range_name = var.services_range_name
  }

  private_cluster_config {
    enable_private_nodes    = true
    enable_private_endpoint = true
    master_ipv4_cidr_block  = var.master_ipv4_cidr_block
  }

  master_authorized_networks_config {
    dynamic "cidr_blocks" {
      for_each = var.authorized_networks
      content {
        cidr_block   = cidr_blocks.value
        display_name = "authorized-${replace(cidr_blocks.value, "/", "-")}"
      }
    }
  }

  workload_identity_config {
    workload_pool = "${var.project_id}.svc.id.goog"
  }

  datapath_provider = "ADVANCED_DATAPATH"
  network_policy { enabled = false }

  release_channel { channel = var.release_channel }

  logging_config {
    enable_components = [
      "SYSTEM_COMPONENTS",
      "WORKLOADS",
      "APISERVER",
      "SCHEDULER",
      "CONTROLLER_MANAGER",
    ]
  }

  monitoring_config {
    enable_components = [
      "SYSTEM_COMPONENTS",
      "APISERVER",
      "SCHEDULER",
      "CONTROLLER_MANAGER",
      "STORAGE",
      "HPA",
      "POD",
      "DAEMONSET",
      "DEPLOYMENT",
      "STATEFULSET",
      "CADVISOR",
      "KUBELET",
    ]
    managed_prometheus { enabled = true }
  }

  gateway_api_config { channel = "CHANNEL_STANDARD" }

  dns_config {
    cluster_dns        = "CLOUD_DNS"
    cluster_dns_scope  = var.cluster_dns_scope
    cluster_dns_domain = var.cluster_dns_scope == "VPC_SCOPE" ? var.cluster_dns_domain : null
  }

  addons_config {
    dns_cache_config { enabled = var.enable_node_local_dns }
  }

  dynamic "binary_authorization" {
    for_each = var.enable_binary_authorization ? [1] : []
    content { evaluation_mode = var.binary_authorization_mode }
  }

  database_encryption {
    state    = "ENCRYPTED"
    key_name = var.kms_key_id
  }

  remove_default_node_pool = true
  initial_node_count       = 1

  deletion_protection = var.deletion_protection

  lifecycle {
    ignore_changes = [
      initial_node_count,
      node_config,
    ]
  }
}

###############################################################################
# Outputs                                                                      #
###############################################################################

output "cluster_id" {
  value = google_container_cluster.this.id
}

output "cluster_endpoint" {
  value     = google_container_cluster.this.endpoint
  sensitive = true
}

output "cluster_ca_cert" {
  value     = google_container_cluster.this.master_auth[0].cluster_ca_certificate
  sensitive = true
}

output "workload_identity_pool" {
  value = "${var.project_id}.svc.id.goog"
}
