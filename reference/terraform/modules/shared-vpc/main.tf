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
# GKE Agentic Migration — Shared VPC module                                                #
# Creates a Shared VPC with regional subnets, secondary ranges for pods and   #
# services, Cloud NAT for egress, Cloud Router, and Private Google Access.    #
###############################################################################

variable "host_project" {
  description = "Host project for the Shared VPC."
  type        = string
}

variable "name" {
  description = "VPC name."
  type        = string
}

variable "regions" {
  description = "Map of region → CIDRs."
  type = map(object({
    nodes_cidr    = string
    pods_cidr     = string
    services_cidr = string
  }))
}

variable "service_projects" {
  description = "Service projects to attach to this Shared VPC."
  type        = list(string)
  default     = []
}

###############################################################################
# Network                                                                      #
###############################################################################

resource "google_compute_network" "this" {
  project                 = var.host_project
  name                    = var.name
  auto_create_subnetworks = false
  routing_mode            = "REGIONAL"
}

resource "google_compute_subnetwork" "gke" {
  for_each = var.regions

  project                  = var.host_project
  name                     = "gke-${each.key}"
  region                   = each.key
  network                  = google_compute_network.this.id
  ip_cidr_range            = each.value.nodes_cidr
  private_ip_google_access = true

  secondary_ip_range {
    range_name    = "pods"
    ip_cidr_range = each.value.pods_cidr
  }
  secondary_ip_range {
    range_name    = "services"
    ip_cidr_range = each.value.services_cidr
  }

  log_config {
    aggregation_interval = "INTERVAL_5_SEC"
    flow_sampling        = 0.5
    metadata             = "INCLUDE_ALL_METADATA"
  }
}

###############################################################################
# Cloud Router + Cloud NAT                                                     #
###############################################################################

resource "google_compute_router" "this" {
  for_each = var.regions
  project  = var.host_project
  name     = "r-${each.key}"
  region   = each.key
  network  = google_compute_network.this.id
}

resource "google_compute_router_nat" "this" {
  for_each = var.regions

  project = var.host_project
  name    = "nat-${each.key}"
  region  = each.key
  router  = google_compute_router.this[each.key].name

  nat_ip_allocate_option             = "AUTO_ONLY"
  source_subnetwork_ip_ranges_to_nat = "ALL_SUBNETWORKS_ALL_IP_RANGES"
  enable_endpoint_independent_mapping = false
  enable_dynamic_port_allocation       = true
  min_ports_per_vm                     = 64

  log_config {
    enable = true
    filter = "ERRORS_ONLY"
  }
}

###############################################################################
# Shared VPC                                                                   #
###############################################################################

resource "google_compute_shared_vpc_host_project" "this" {
  project = var.host_project
}

resource "google_compute_shared_vpc_service_project" "this" {
  for_each        = toset(var.service_projects)
  host_project    = var.host_project
  service_project = each.value
}

###############################################################################
# Outputs                                                                      #
###############################################################################

output "network" {
  value = google_compute_network.this.name
}

output "subnets" {
  value = { for k, v in google_compute_subnetwork.gke : k => v.name }
}
