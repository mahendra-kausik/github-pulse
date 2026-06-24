terraform {
  required_version = ">= 1.5"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

# --- Data lake bucket ---
resource "google_storage_bucket" "lake" {
  name                        = var.bucket_name
  location                    = var.region
  force_destroy               = true
  uniform_bucket_level_access = true

  # Free-tier guardrail: expire raw/parquet objects so storage never creeps up.
  lifecycle_rule {
    condition {
      age = var.lake_retention_days
    }
    action {
      type = "Delete"
    }
  }
}

# --- BigQuery datasets ---
resource "google_bigquery_dataset" "raw" {
  dataset_id    = var.dataset_raw
  location      = var.region
  description   = "Raw GH Archive events (slim projection), loaded by ingestion/load_bq.py"
  delete_contents_on_destroy = true
}

resource "google_bigquery_dataset" "marts" {
  dataset_id    = var.dataset_marts
  location      = var.region
  description   = "dbt marts: fct_events, dim_repo, agg_*"
  delete_contents_on_destroy = true
}

# --- Least-privilege service account (used by Kestra-in-Docker) ---
resource "google_service_account" "pipeline" {
  account_id   = "github-pulse-pipeline"
  display_name = "GitHub Pulse pipeline (ingest + dbt)"
}

# GCS object read/write on the lake bucket only.
resource "google_storage_bucket_iam_member" "pipeline_storage" {
  bucket = google_storage_bucket.lake.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.pipeline.email}"
}

# BigQuery job + data editing (project-scoped; BQ has no per-dataset jobUser).
resource "google_project_iam_member" "pipeline_bq_job" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.pipeline.email}"
}

resource "google_bigquery_dataset_iam_member" "pipeline_raw_editor" {
  dataset_id = google_bigquery_dataset.raw.dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.pipeline.email}"
}

resource "google_bigquery_dataset_iam_member" "pipeline_marts_editor" {
  dataset_id = google_bigquery_dataset.marts.dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.pipeline.email}"
}
