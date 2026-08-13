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

# APIs a fresh project doesn't enable by default; apply fails with SERVICE_DISABLED without these.
resource "google_project_service" "apis" {
  for_each           = toset(["storage.googleapis.com", "bigquery.googleapis.com", "iam.googleapis.com", "cloudresourcemanager.googleapis.com"])
  project            = var.project_id
  service            = each.value
  disable_on_destroy = false
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

  depends_on = [google_project_service.apis]
}

# --- BigQuery datasets ---
resource "google_bigquery_dataset" "raw" {
  dataset_id                 = var.dataset_raw
  location                   = var.region
  description                = "Raw GH Archive events (slim projection), loaded by ingestion/load_bq.py"
  delete_contents_on_destroy = true

  # 30-day rolling retention on the live table: without this, storage grows unbounded and blows
  # past the 10 GB free tier. dbt's marts (+materialized: table) fully rebuild every run, so they
  # follow the retained window automatically — no dbt-side change needed when partitions expire.
  default_partition_expiration_ms = 2592000000 # 30 days

  depends_on = [google_project_service.apis]
}

resource "google_bigquery_dataset" "marts" {
  dataset_id                 = var.dataset_marts
  location                   = var.region
  description                = "dbt marts: fct_events, dim_repo, agg_*"
  delete_contents_on_destroy = true

  depends_on = [google_project_service.apis]
}

# --- Least-privilege service account (used by Kestra-in-Docker) ---
resource "google_service_account" "pipeline" {
  account_id   = "github-pulse-pipeline"
  display_name = "GitHub Pulse pipeline (ingest + dbt)"

  depends_on = [google_project_service.apis]
}

# GCS object read/write on the lake bucket only.
resource "google_storage_bucket_iam_member" "pipeline_storage" {
  bucket = google_storage_bucket.lake.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.pipeline.email}"
}

# BigQuery job + data editing (project-scoped; BQ has no per-dataset jobUser).
resource "google_project_iam_member" "pipeline_bq_job" {
  project    = var.project_id
  role       = "roles/bigquery.jobUser"
  member     = "serviceAccount:${google_service_account.pipeline.email}"
  depends_on = [google_project_service.apis]
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
