variable "aws_region" {
  description = "AWS region for the raw bucket and Glue catalog (matches AWS_REGION)."
  type        = string
  default     = "us-east-1"
}

variable "environment" {
  description = "Environment name. Use separate buckets and state per environment."
  type        = string

  validation {
    condition     = contains(["dev", "prod-like"], var.environment)
    error_message = "environment must be \"dev\" or \"prod-like\" (local mode needs no Terraform)."
  }
}

variable "project_name" {
  description = "Short project prefix used in names and tags."
  type        = string
  default     = "ecomm"
}

variable "raw_bucket_name" {
  description = "Globally unique name of the raw-zone bucket (matches AWS_S3_BUCKET)."
  type        = string

  validation {
    condition     = can(regex("^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$", var.raw_bucket_name))
    error_message = "raw_bucket_name must be 3-63 lowercase letters, digits, dots or hyphens."
  }
}

variable "source_name" {
  description = "Source segment of the raw object keys (the Kafka topic prefix)."
  type        = string
  default     = "commerce"
}

variable "force_destroy" {
  description = "Allow `terraform destroy` to delete a non-empty bucket. Keep false outside throwaway sandboxes."
  type        = bool
  default     = false
}

variable "enable_versioning" {
  description = "Keep previous versions of overwritten objects (protects against bad overwrites)."
  type        = bool
  default     = true
}

variable "noncurrent_version_expiration_days" {
  description = "Days after which non-current object versions are deleted."
  type        = number
  default     = 30
}

variable "raw_expiration_days" {
  description = "Delete current raw objects after this many days. 0 keeps them forever (use a small value for demo buckets to cap cost)."
  type        = number
  default     = 0

  validation {
    condition     = var.raw_expiration_days >= 0
    error_message = "raw_expiration_days must be 0 (disabled) or a positive number of days."
  }
}

variable "kms_key_arn" {
  description = "Optional customer-managed KMS key. Null uses S3-managed encryption (SSE-S3)."
  type        = string
  default     = null
}

variable "pipeline_principal_arns" {
  description = "IAM principals (ARNs) allowed to assume the pipeline role. Empty creates no role; only the managed policies are created."
  type        = list(string)
  default     = []
}

variable "create_glue_catalog" {
  description = "Create the Glue database and the raw_events table (partition projection, no crawler)."
  type        = bool
  default     = true
}

variable "monthly_budget_usd" {
  description = "Monthly cost budget in USD. 0 disables the budget."
  type        = number
  default     = 0
}

variable "budget_alert_emails" {
  description = "Email addresses notified by the budget (required when monthly_budget_usd > 0)."
  type        = list(string)
  default     = []
}
