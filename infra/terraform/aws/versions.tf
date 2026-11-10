terraform {
  required_version = ">= 1.6"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0"
    }
  }

  # State: local state is acceptable ONLY for a personal sandbox. For anything
  # shared, copy backend.tf.example to backend.tf and use a remote backend.
}

provider "aws" {
  region = var.aws_region

  # Credentials are NEVER configured here. Use the standard AWS chain
  # (AWS_PROFILE / SSO / role assumption / environment variables).
  default_tags {
    tags = local.tags
  }
}
