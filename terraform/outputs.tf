output "bucket_name" {
  description = "GCS data lake bucket"
  value       = google_storage_bucket.lake.name
}

output "dataset_raw" {
  description = "Raw BigQuery dataset id"
  value       = google_bigquery_dataset.raw.dataset_id
}

output "dataset_marts" {
  description = "Marts BigQuery dataset id"
  value       = google_bigquery_dataset.marts.dataset_id
}

output "service_account_email" {
  description = "Pipeline service account (create a key for Kestra, keep it gitignored)"
  value       = google_service_account.pipeline.email
}
