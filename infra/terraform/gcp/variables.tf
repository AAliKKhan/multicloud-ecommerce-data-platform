variable "project_id" {
  description = "GCP project that will own the resources (matches GCP_PROJECT_ID)."
  type        = string
}

variable "region" {
  description = "Default region for the provider (matches GCP_REGION)."
  type        = string
  default     = "us-central1"
}

variable "location" {
  description = "Location for the bucket and BigQuery datasets. Keep them identical to avoid cross-location transfer, e.g. \"US\" or \"us-central1\"."
  type        = string
  default     = "US"
}

variable "environment" {
  description = "Environment name. Use a separate project or at least separate names per environment."
  type        = string

  validation {
    condition     = contains(["dev", "prod-like"], var.environment)
    error_message = "environment must be \"dev\" or \"prod-like\" (local mode needs no Terraform)."
  }
}

variable "project_name" {
  description = "Short prefix used in names and labels."
  type        = string
  default     = "ecomm"
}

variable "raw_bucket_name" {
  description = "Globally unique name of the raw-zone bucket (matches GCS_BUCKET)."
  type        = string

  validation {
    condition     = can(regex("^[a-z0-9][a-z0-9._-]{1,61}[a-z0-9]$", var.raw_bucket_name))
    error_message = "raw_bucket_name must be 3-63 lowercase letters, digits, dots, underscores or hyphens."
  }
}

variable "bigquery_dataset" {
  description = "Raw dataset ID (matches BIGQUERY_DATASET). dbt writes to <this>_analytics."
  type        = string

  validation {
    condition     = can(regex("^[A-Za-z0-9_]+$", var.bigquery_dataset)) && length(var.bigquery_dataset) <= 1024
    error_message = "bigquery_dataset may contain only letters, digits and underscores."
  }
}

variable "enable_apis" {
  description = "Enable the Storage and BigQuery APIs on the project (never disabled on destroy)."
  type        = bool
  default     = true
}

variable "force_destroy" {
  description = "Allow `terraform destroy` to delete a non-empty bucket and datasets with tables. Sandbox only."
  type        = bool
  default     = false
}

variable "enable_versioning" {
  description = "Keep previous versions of overwritten objects."
  type        = bool
  default     = true
}

variable "noncurrent_version_expiration_days" {
  description = "Days after which non-current object versions are deleted."
  type        = number
  default     = 30
}

variable "raw_expiration_days" {
  description = "Delete current raw objects after this many days. 0 keeps them forever."
  type        = number
  default     = 0

  validation {
    condition     = var.raw_expiration_days >= 0
    error_message = "raw_expiration_days must be 0 (disabled) or a positive number of days."
  }
}

variable "table_expiration_days" {
  description = "Default expiration for tables in both datasets. Null keeps tables forever (use a small value for dev)."
  type        = number
  default     = null
}

variable "kms_key_name" {
  description = "Optional customer-managed key (full resource name) for the bucket. Null uses Google-managed encryption."
  type        = string
  default     = null
}

variable "analyst_members" {
  description = "IAM members (e.g. \"user:you@example.com\", \"group:analysts@example.com\") granted read-only access to the bucket and analytics dataset."
  type        = list(string)
  default     = []
}

variable "billing_account_id" {
  description = "Billing account for the optional budget. Empty disables the budget."
  type        = string
  default     = ""
}

variable "monthly_budget_usd" {
  description = "Monthly budget in USD (only used when billing_account_id is set)."
  type        = number
  default     = 0
}
