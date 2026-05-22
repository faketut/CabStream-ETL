variable "project_id" {
  description = "GCP project ID (required; no default)."
  type        = string
}

variable "region" {
  description = "Region for GCP resources."
  type        = string
  default     = "us-central1"
}

variable "admin_cidr" {
  description = "CIDR allowed to reach the Airflow webserver on :8080 (e.g. 203.0.113.4/32)."
  type        = string
}

variable "allow_destroy" {
  description = "When true, allows `terraform destroy` to wipe the bucket/dataset contents. Keep false in prod."
  type        = bool
  default     = false
}

variable "tfstate_bucket" {
  description = "Optional: existing GCS bucket name to hold remote Terraform state. Configure the matching backend block separately."
  type        = string
  default     = ""
}
