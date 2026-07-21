# GitHub Pulse

A batch data pipeline that turns raw [GH Archive](https://www.gharchive.org/) public-event data
into a **"which languages and projects are gaining momentum"** dashboard — built end-to-end on
GCP and kept entirely inside the always-free tier.

> DataTalksClub Data Engineering Zoomcamp capstone. Also a resume project: the interesting part
> isn't "I moved JSON to BigQuery", it's the **cost-engineering** (staying under 10 GB storage /
> 1 TB queries a month) and the **reproducibility** (one clone + documented steps → working
> pipeline).

## Problem

GH Archive publishes every public GitHub event as hourly `.json.gz` files. Raw, a single day is
~6–10 GB of JSON and a useful window (7 days) is ~50 GB — which alone blows past BigQuery's 10 GB
free storage. GitHub Pulse answers "what's trending on GitHub this week?" without ever leaving
the free tier, by projecting only the needed fields into columnar Parquet **before** loading.

## Architecture

```
GH Archive .json.gz ──► transform (slim Parquet) ──► GCS lake ──► BigQuery (partitioned/clustered)
                                                                        │
                                                                        ▼
                                                          dbt (staging → marts) ──► Looker Studio
```

- **Ingestion** (`ingestion/`): Python, parametrized by date. Downloads 24 hourly files,
  stream-parses them, keeps only needed fields (incl. PR `base.repo.language`), writes Parquet,
  uploads to GCS, loads to a partitioned + clustered BigQuery table.
- **Warehouse** (`dbt/`): staging (cast/dedupe) → marts (`fct_events`, `dim_repo`,
  `agg_repo_trending_daily`, `agg_language_daily`, `agg_repo_momentum`, `agg_event_type_daily`).
- **Orchestration** (`orchestration/`): Kestra flow chaining the stages, with a daily schedule
  and a 7-day backfill.
- **Infra** (`terraform/`): GCS bucket + `raw`/`marts` BQ datasets + least-privilege service account.
- **CI** (`.github/workflows/ci.yml`): creds-free lint + tests + `dbt build --empty`.

![Architecture](images/architecture.png)

## Dashboard

Three tiles in Looker Studio, with date / repo / language filters:
1. **Trending repos** — top repos by star (WatchEvent) count this week (`agg_repo_trending_daily`).
2. **Language momentum** — daily PR-event counts per language over time (`agg_language_daily`).
3. **Momentum bursts** — repos ranked by cross-signal burst score (watch + fork + PR on the same day, `agg_repo_momentum`).

![Dashboard](images/dashboard.png)

## How I kept it free

- **Project columns at ingest** and store **Parquet, not raw JSON** — 7 days stays < ~1 GB.
- **Partition** raw + `fct_events` by `event_date`, **cluster** by `event_type`; mark them
  `require_partition_filter` so unfiltered scans error instead of scanning everything.
- **`maximum_bytes_billed`** set in the dbt profile — a runaway query fails instead of billing.
- A **GCP billing budget alert** at a low threshold as a backstop.

## Run it (fresh clone)

Prereqs: Python 3.11+, a GCP project with billing enabled, `gcloud`, Terraform, Docker.

```bash
# 1. auth + deps
gcloud auth application-default login
make setup

# 2. infra
make tf-apply

# 3. ingest a 7-day window
make backfill START=2024-01-01 DAYS=7

# 4. transform
make dbt

# 5. (optional) run the whole thing on a schedule via Kestra
make up        # then open http://localhost:8080 and trigger the github_pulse flow
```

Then point Looker Studio at the `marts` dataset and rebuild the two tiles.

## Design notes

- **Why Kestra, not Airflow?** A single `docker-compose` (Kestra + Postgres, ~1–2 GB RAM) runs
  the daily-batch demo locally — no always-on VM. Airflow (~4 GB) won't fit a free `e2-micro`.
  Orchestration concepts transfer; I picked the tool that fit the constraint.
- **Why is language only on PR events?** GH Archive `PushEvent`/`WatchEvent`/`IssuesEvent` carry
  only a bare `repo`. Language lives at `PullRequestEvent.payload.pull_request.base.repo.language`,
  so the language tile is PR-derived while the categorical tile uses the always-present event type.

## What's next

- Streaming variant via **Pub/Sub** for near-real-time ingestion.
- **dlt** for declarative, schema-evolving extract/load.
- A small **FastAPI/CLI "trends API"** over the `agg_*` tables, so the project reads as software
  (typed endpoints, tests, OpenAPI) — not just a pipeline.
