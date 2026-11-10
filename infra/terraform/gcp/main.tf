locals {
  labels = {
    project     = var.project_name
    environment = var.environment
    managed_by  = "terraform"
    data        = "synthetic"
  }

  name_prefix         = "${var.project_name}-${var.environment}"
  analytics_dataset   = "${var.bigquery_dataset}_analytics"
  table_expiration_ms = var.table_expiration_days == null ? null : var.table_expiration_days * 24 * 60 * 60 * 1000
  budget_enabled      = var.billing_account_id != "" && var.monthly_budget_usd > 0
  pipeline_account_id = "${local.name_prefix}-pipeline"
  analyst_member_set  = toset(var.analyst_members)
  required_api_set    = var.enable_apis ? toset(["storage.googleapis.com", "bigquery.googleapis.com"]) : toset([])
  budget_api_set      = local.budget_enabled && var.enable_apis ? toset(["billingbudgets.googleapis.com"]) : toset([])
  all_api_set         = setunion(local.required_api_set, local.budget_api_set)
}

resource "google_project_service" "apis" {
  for_each = local.all_api_set

  project            = var.project_id
  service            = each.value
  disable_on_destroy = false # never switch an API off for other workloads
}

# --- Raw-zone bucket ---------------------------------------------------------

resource "google_storage_bucket" "raw" {
  name                        = var.raw_bucket_name
  location                    = var.location
  force_destroy               = var.force_destroy
  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"
  labels                      = local.labels

  versioning {
    enabled = var.enable_versioning
  }

  dynamic "encryption" {
    for_each = var.kms_key_name != null ? [1] : []

    content {
      default_kms_key_name = var.kms_key_name
    }
  }

  lifecycle_rule {
    action {
      type = "AbortIncompleteMultipartUpload"
    }

    condition {
      age = 7
    }
  }

  dynamic "lifecycle_rule" {
    for_each = var.enable_versioning ? [1] : []

    content {
      action {
        type = "Delete"
      }

      condition {
        days_since_noncurrent_time = var.noncurrent_version_expiration_days
        with_state                 = "ARCHIVED"
      }
    }
  }

  dynamic "lifecycle_rule" {
    for_each = var.raw_expiration_days > 0 ? [1] : []

    content {
      action {
        type = "Delete"
      }

      condition {
        age        = var.raw_expiration_days
        with_state = "LIVE"
      }
    }
  }

  depends_on = [google_project_service.apis]
}

# --- Identity: one service account, no keys ----------------------------------
# Authenticate as this account through impersonation or workload identity
# federation. This configuration deliberately creates NO service-account key.

resource "google_service_account" "pipeline" {
  project      = var.project_id
  account_id   = local.pipeline_account_id
  display_name = "E-commerce data pipeline (${var.environment})"

  lifecycle {
    precondition {
      condition     = length(local.pipeline_account_id) >= 6 && length(local.pipeline_account_id) <= 30
      error_message = "The service account id must be 6-30 characters; shorten project_name."
    }
  }
}

# objectUser (not objectCreator): the retry protocol overwrites objects, and
# GCS overwrites need delete permission on the object being replaced. The grant
# is scoped to this one bucket.
resource "google_storage_bucket_iam_member" "pipeline_objects" {
  bucket = google_storage_bucket.raw.name
  role   = "roles/storage.objectUser"
  member = "serviceAccount:${google_service_account.pipeline.email}"
}

resource "google_storage_bucket_iam_member" "analyst_read" {
  for_each = local.analyst_member_set

  bucket = google_storage_bucket.raw.name
  role   = "roles/storage.objectViewer"
  member = each.value
}

# --- BigQuery ----------------------------------------------------------------

resource "google_bigquery_dataset" "raw" {
  project                     = var.project_id
  dataset_id                  = var.bigquery_dataset
  location                    = var.location
  description                 = "Raw event ledger (synthetic data)."
  delete_contents_on_destroy  = var.force_destroy
  default_table_expiration_ms = local.table_expiration_ms
  labels                      = local.labels

  depends_on = [google_project_service.apis]
}

resource "google_bigquery_dataset" "analytics" {
  project                     = var.project_id
  dataset_id                  = local.analytics_dataset
  location                    = var.location
  description                 = "dbt models: staging, intermediate and marts (synthetic data)."
  delete_contents_on_destroy  = var.force_destroy
  default_table_expiration_ms = local.table_expiration_ms
  labels                      = local.labels

  depends_on = [google_project_service.apis]
}

resource "google_bigquery_dataset_iam_member" "pipeline_raw" {
  project    = var.project_id
  dataset_id = google_bigquery_dataset.raw.dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.pipeline.email}"
}

resource "google_bigquery_dataset_iam_member" "pipeline_analytics" {
  project    = var.project_id
  dataset_id = google_bigquery_dataset.analytics.dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.pipeline.email}"
}

resource "google_bigquery_dataset_iam_member" "analyst_read" {
  for_each = local.analyst_member_set

  project    = var.project_id
  dataset_id = google_bigquery_dataset.analytics.dataset_id
  role       = "roles/bigquery.dataViewer"
  member     = each.value
}

# Running query/load jobs needs jobUser on the project (non-authoritative).
resource "google_project_iam_member" "pipeline_jobs" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.pipeline.email}"
}

resource "google_project_iam_member" "analyst_jobs" {
  for_each = local.analyst_member_set

  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = each.value
}

# --- Budget (optional) ---------------------------------------------------------

data "google_project" "this" {
  count      = local.budget_enabled ? 1 : 0
  project_id = var.project_id
}

resource "google_billing_budget" "monthly" {
  count = local.budget_enabled ? 1 : 0

  billing_account = var.billing_account_id
  display_name    = "${local.name_prefix}-monthly"

  budget_filter {
    projects = ["projects/${data.google_project.this[0].number}"]
  }

  amount {
    specified_amount {
      currency_code = "USD"
      units         = tostring(var.monthly_budget_usd)
    }
  }

  threshold_rules {
    threshold_percent = 0.8
  }

  threshold_rules {
    threshold_percent = 1.0
    spend_basis       = "FORECASTED_SPEND"
  }

  depends_on = [google_project_service.apis]
}
