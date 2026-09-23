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
# GKE Agentic Migration — Artifact Registry module                                         #
# Central container registry the migrated workloads pull from. Image paths     #
# from the source registry are preserved verbatim beneath this repository      #
# (an Artifact Registry repository is a namespace: slashes in image names are  #
# allowed), so replication from ECR is a prefix substitution:                  #
#   <acct>.dkr.ecr.<region>.amazonaws.com/payments/api:1.4.2                   #
#     -> <location>-docker.pkg.dev/<project>/<repository_id>/payments/api:1.4.2 #
###############################################################################

variable "project_id" {
  description = "GCP project hosting the registry (the shared artifact-registry project)."
  type        = string
}

variable "location" {
  description = "Regional location (e.g., us-central1). Use the primary GKE region."
  type        = string
}

variable "repository_id" {
  description = "Repository name. Default scheme: the migration workspace name, slugified."
  type        = string
}

variable "description" {
  description = "Human-readable description shown in the console."
  type        = string
  default     = "Container images migrated from the source EKS estate."
}

variable "immutable_tags" {
  description = "Reject re-pushing an existing tag. Recommended once replication completes."
  type        = bool
  default     = false
}

variable "kms_key_id" {
  description = "CMEK key for the repository. Empty string uses Google-managed encryption."
  type        = string
  default     = ""
}

variable "reader_members" {
  description = "IAM members granted artifactregistry.reader (node service accounts pulling images)."
  type        = list(string)
  default     = []
}

variable "writer_members" {
  description = "IAM members granted artifactregistry.writer (CI/CD and the replication identity)."
  type        = list(string)
  default     = []
}

resource "google_artifact_registry_repository" "this" {
  project       = var.project_id
  location      = var.location
  repository_id = var.repository_id
  description   = var.description
  format        = "DOCKER"

  kms_key_name = var.kms_key_id != "" ? var.kms_key_id : null

  docker_config {
    immutable_tags = var.immutable_tags
  }
}

resource "google_artifact_registry_repository_iam_member" "readers" {
  for_each = toset(var.reader_members)

  project    = var.project_id
  location   = var.location
  repository = google_artifact_registry_repository.this.repository_id
  role       = "roles/artifactregistry.reader"
  member     = each.value
}

resource "google_artifact_registry_repository_iam_member" "writers" {
  for_each = toset(var.writer_members)

  project    = var.project_id
  location   = var.location
  repository = google_artifact_registry_repository.this.repository_id
  role       = "roles/artifactregistry.writer"
  member     = each.value
}

###############################################################################
# Outputs                                                                      #
###############################################################################

output "repository_url" {
  description = "Image prefix for pushes and pulls: <location>-docker.pkg.dev/<project>/<repository_id>."
  value       = "${var.location}-docker.pkg.dev/${var.project_id}/${google_artifact_registry_repository.this.repository_id}"
}

output "repository_name" {
  value = google_artifact_registry_repository.this.name
}
