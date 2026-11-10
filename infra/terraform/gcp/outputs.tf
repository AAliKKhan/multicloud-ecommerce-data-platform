output "raw_bucket_name" {
  description = "Value for GCS_BUCKET."
  value       = google_storage_bucket.raw.name
}

output "raw_dataset_id" {
  description = "Value for BIGQUERY_DATASET (raw event ledger)."
  value       = google_bigquery_dataset.raw.dataset_id
}

output "analytics_dataset_id" {
  description = "Dataset dbt writes to."
  value       = google_bigquery_dataset.analytics.dataset_id
}

output "pipeline_service_account_email" {
  description = "Identity for the pipeline. Impersonate it; no key is created."
  value       = google_service_account.pipeline.email
}
