# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview & goal

**GitHub Pulse** is a batch data pipeline that turns raw [GH Archive](https://www.gharchive.org/)
public-event data into a "which languages and projects are gaining momentum" dashboard. It is a
DataTalksClub **Data Engineering Zoomcamp** capstone and a resume anchor for SWE interviews.

How each component maps to the Zoomcamp rubric:

| Rubric area              | Component in this repo                                        |
| ------------------------ | ------------------------------------------------------------ |
| Cloud + IaC              | GCP (GCS + BigQuery) provisioned by Terraform (`terraform/`) |
| Orchestration            | Kestra flow, daily schedule + 7-day backfill (`orchestration/`) |
| Data lake → DWH          | Parquet in GCS → BigQuery raw table                          |
| Partitioned/clustered DWH| BQ raw + `fct_events` partitioned by event date, clustered by event type |
| Transformations          | dbt staging → marts (`dbt/`)                                 |
| Dashboard (3 tiles)      | Looker Studio: trending repos + language momentum + momentum bursts |
| Reproducibility          | `make setup` + README run from a fresh clone                 |

**Honest framing:** for SWE roles this is a *supporting* project anchor that demonstrates
systems thinking, cloud, and cost discipline — not a substitute for DSA / CS-fundamentals prep.
Build it over ~2–3 weekends, then refocus.

## Architecture

```
GH Archive .json.gz (24 files/day)
        │  download.py
        ▼
Python extract/transform  ──►  slim columnar Parquet   (transform.py: project only needed
        │                          fields, incl. PR base.repo.language)
        │  upload_gcs.py
        ▼
GCS data lake (raw .gz + parquet)
        │  load_bq.py
        ▼
BigQuery raw table   (partitioned by event date, clustered by event type)
        │  dbt
        ▼
dbt staging → marts  (fct_events, dim_repo, agg_repo_trending_daily, agg_language_daily, agg_repo_momentum)
        │
        ▼
Looker Studio dashboard (3 tiles + date / repo / language filters)
```

Orchestrated by **Kestra**, infra by **Terraform**, CI by **GitHub Actions**.

## Design Decisions

See [`DESIGN_DECISIONS.md`](DESIGN_DECISIONS.md) for the full record — every major choice with
what was chosen, what was rejected, and why, written for interview prep.

**Every time a new component is added to this project, update `DESIGN_DECISIONS.md` with the
rationale before closing the task.**

## Repo layout

```
.
├── terraform/        # GCS bucket + raw/marts BQ datasets + least-priv service account
├── orchestration/    # Kestra docker-compose + the github_pulse flow
├── ingestion/        # Python package: download → transform → upload_gcs → load_bq
├── dbt/              # dbt project: models/staging + models/marts
├── tests/            # pytest (transform field projection + PR language extraction)
├── images/           # architecture diagram + dashboard screenshots
└── .github/workflows # creds-free CI (lint + tests + dbt build --empty)
```

## Commands

The `Makefile` is the primary interface:

| Command                 | What it does                                                       |
| ----------------------- | ----------------------------------------------------------------- |
| `make setup`            | create venv + install `requirements.txt`                          |
| `make tf-apply`         | `terraform init && terraform apply` in `terraform/`               |
| `make ingest DATE=YYYY-MM-DD` | run download→transform→upload→load for one day              |
| `make backfill START=YYYY-MM-DD DAYS=7` | loop `ingest` over the window                   |
| `make dbt`              | `dbt build` in `dbt/`                                              |
| `make up` / `make down` | start / stop Kestra (`orchestration/docker-compose.yml`)          |
| `make lint`             | `ruff check .` + `sqlfluff lint dbt/models`                        |
| `make test`             | `pytest`                                                           |

Raw invocations:

```bash
# single ingestion stage for one day
python -m ingestion.download   --date 2024-01-01
python -m ingestion.transform  --date 2024-01-01
python -m ingestion.upload_gcs --date 2024-01-01
python -m ingestion.load_bq    --date 2024-01-01

# dbt
cd dbt && dbt deps && dbt build                  # full run
cd dbt && dbt build --select stg_github_events   # one model + its tests
cd dbt && dbt build --empty                      # creds-free structural check (CI)

# terraform
cd terraform && terraform init && terraform plan && terraform apply

# tests
pytest                                  # all
pytest tests/test_transform.py::test_pr_language_extraction   # a single test
```

## Conventions & invariants

**Free-tier rules (10 GB BQ storage, 1 TB queries/month) — do not violate:**
- **Always filter on the partition column** (`event_date`) in any BQ query. Raw + `fct_events`
  set `require_partition_filter = true`, so unfiltered scans error out by design.
- **Project columns at ingest time** — `transform.py` selects only the fields the marts need.
  Never load full raw JSON into BQ.
- **Write Parquet, not raw JSON**, before loading to BQ. 7 days of slim Parquet stays < ~1 GB.
- `maximum_bytes_billed` is set in the dbt profile so a runaway query fails instead of billing.

**Data semantics:**
- **Language is only available on PR events** — `PullRequestEvent.payload.pull_request.base.repo.language`.
  `PushEvent`/`WatchEvent`/`IssuesEvent` carry only a bare `repo` (id/name/url). So
  `agg_language_daily` is derived from PR events only — it reflects language activity via PRs,
  not all commits. Known limitation; enrichment via GitHub REST API is a documented "what's next".
- **Star signal = WatchEvent.** A "star" on GitHub fires a `WatchEvent`. `agg_repo_trending_daily`
  counts these per repo per day for the trending-repos tile.

**Secrets:**
- Local dev uses ADC (`gcloud auth application-default login`) — no key file.
- Kestra-in-Docker mounts a `.gitignore`d service-account key via env (`GOOGLE_APPLICATION_CREDENTIALS`).
- Never commit `*key*.json` or `.env`. The dbt profile is env-var driven and safe to commit.

## Data model

- **`stg_github_events`** — staging: cast types, dedupe by event `id`, one row per event.
- **`fct_events`** — fact, grain = **one GH Archive event**. Partitioned by `event_date`,
  clustered by `event_type`. FK `repo_id` → `dim_repo`.
- **`dim_repo`** — distinct repos (`repo_id`, `repo_name`, latest known `language` from PR events).
- **`agg_repo_trending_daily`** — grain = (`event_date`, `repo_id`, `repo_name`); WatchEvent
  (star) counts per repo per day. **Feeds tile 1: trending repos.**
- **`agg_language_daily`** — grain = (`event_date`, `language`); PR-event counts only (language
  not available on other event types). **Feeds tile 2: language momentum.**
- **`agg_repo_momentum`** — grain = (`event_date`, `repo_id`, `repo_name`); cross-signal:
  `watch_count + fork_count + pr_count` summed per repo per day. Repos spiking across all three
  signals simultaneously likely went viral. **Feeds tile 3: momentum bursts.**
- **`agg_event_type_daily`** — grain = (`event_date`, `event_type`); supporting/diagnostic table,
  not a primary dashboard tile.

## Build roadmap / status checklist

- [ ] 0. Walking skeleton — ADC login; one bucket + one dataset by hand; one script does
      download→Parquet→GCS→BQ for a single day; eyeball rows in BQ. No Terraform/Kestra/CI.
- [ ] 1. git init + scaffold + `.gitignore` (keys excluded before any real creds exist).
- [ ] 2. Terraform — bucket + `raw`/`marts` datasets + least-priv SA; `apply`; confirm no drift.
- [ ] 3. Ingestion package — parametrized by date; run the 7-day window; verify Parquet sizes.
- [ ] 4. dbt — staging → marts; tests (not_null/unique/accepted_values/relationships);
      `dbt build` green; `maximum_bytes_billed` set.
- [ ] 5. Kestra — `docker compose up`; flow chains tasks; daily schedule + 7-day backfill; one
      end-to-end run populates marts.
- [ ] 6. Looker Studio — 3 tiles + filters; screenshots → `images/`.
      - Tile 1: top trending repos this week (bar chart, `agg_repo_trending_daily`, filter by date)
      - Tile 2: language momentum (line chart, `agg_language_daily`, filter by language/date)
      - Tile 3: momentum burst repos (table ranked by burst score, `agg_repo_momentum`, filter by date)
- [ ] 7. Extra mile — Makefile, pytest, ruff + sqlfluff, GitHub Actions CI.
- [ ] 8. README + cleanup — problem, diagram, run steps, "how I kept it free", "what's next".

## What's next (documented, not built)

- Streaming variant via Pub/Sub for near-real-time event ingestion.
- `dlt` for declarative, schema-evolving extract/load.
- **SWE stretch:** a small FastAPI/CLI "trends API" over the `agg_*` tables so the project reads
  as *software* (typed endpoints, tests, OpenAPI), not just a pipeline.
