variable "project_id" {
  description = "GCP project id"
  type        = string
}

variable "region" {
  description = "Location for BigQuery datasets and GCS bucket (us-central1: GCS always-free tier is US-only)"
  type        = string
  default     = "us-central1"
}

variable "bucket_name" {
  description = "Globally unique GCS bucket name for the data lake"
  type        = string
}

variable "dataset_raw" {
  description = "BigQuery dataset for raw loaded events"
  type        = string
  default     = "github_pulse_raw"
}

variable "dataset_marts" {
  description = "BigQuery dataset for dbt marts"
  type        = string
  default     = "github_pulse_marts"
}

variable "lake_retention_days" {
  description = "Auto-delete lake objects after N days to stay under free storage"
  type        = number
  default     = 30
}
