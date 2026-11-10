terraform {
  required_version = ">= 1.6"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 6.0"
    }
  }

  # State: local state is acceptable ONLY for a personal sandbox. For anything
  # shared, copy backend.tf.example to backend.tf and use a remote backend.
}

provider "google" {
  project = var.project_id
  region  = var.region

  # Credentials are NEVER configured here. Use Application Default Credentials
  # (`gcloud auth application-default login`), service-account impersonation or
  # workload identity federation. No service-account key files, ever.
}
