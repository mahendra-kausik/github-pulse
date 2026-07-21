# GitHub Pulse — Project Information Sheet

> **Purpose of this file:** a complete, neutral reference of what this project is, how it's
> built, and what it demonstrates. Feed it to Claude alongside a specific job description and
> ask it to re-phrase / re-weight the relevant parts for that role. It is written to be
> factual and reusable, not pre-tailored to any one JD.

---

## 1. One-line summary

A batch data pipeline that turns raw GitHub public-event data (GH Archive) into a
"which languages and projects are gaining momentum" analytics dashboard, engineered end-to-end
on Google Cloud and kept entirely inside the always-free tier.

**Context:** DataTalksClub Data Engineering Zoomcamp capstone, also built as a software/data
engineering portfolio project. Solo project.

---

## 2. The problem it solves

[GH Archive](https://www.gharchive.org/) publishes every public GitHub event as hourly
`.json.gz` files (24 files/day). Raw, a single day is ~6–10 GB of JSON; a useful 7-day window
is ~50–70 GB. That alone exceeds BigQuery's 10 GB free-storage limit — loading raw JSON would
blow the budget on day 2.

The project answers **"what's trending on GitHub this week?"** (repos, languages, viral
momentum) without ever leaving the free tier. The interesting engineering is not "move JSON to
a warehouse" — it's the **cost engineering** (staying under 10 GB storage / 1 TB queries per
month) and **reproducibility** (one clone + documented commands → a working pipeline).

---

## 3. What it does (end to end)

```
GH Archive .json.gz ──► transform (slim Parquet) ──► GCS lake ──► BigQuery (partitioned/clustered)
                                                                        │
                                                                        ▼
                                                          dbt (staging → marts) ──► Looker Studio
```

1. **Ingest** — Python, parametrized by date. Downloads the 24 hourly files for a day,
   stream-parses newline-delimited JSON line by line, projects each event down to **8 fields**,
   and writes a single **Snappy-compressed columnar Parquet** file per day.
2. **Land** — uploads Parquet to a **GCS** data-lake bucket.
3. **Load** — loads the day's Parquet into a **BigQuery** raw table that is partitioned by
   `event_date` and clustered by `event_type`. Writes are scoped to the day's partition
   (`$YYYYMMDD` decorator, `WRITE_TRUNCATE`) so re-running a day is **idempotent**.
4. **Transform** — **dbt** builds a staging layer (cast/dedupe) then mart tables (fact, dim,
   aggregates) with data-quality tests.
5. **Visualize** — **Looker Studio** dashboard with 3 signal-based tiles + filters.
6. **Orchestrate** — a **Kestra** flow chains stages 1–4 with a daily schedule and a 7-day
   backfill.
7. **Provision** — all cloud infra is defined in **Terraform**.
8. **Guard** — **GitHub Actions** CI runs lint + tests + dbt structural checks with no cloud
   credentials.

---

## 4. Technology stack

| Layer | Tool / detail |
| --- | --- |
| Language | Python 3.11 |
| Ingestion libs | `requests`, `pyarrow` (Parquet), `gzip`/`json` streaming, `google-cloud-storage`, `google-cloud-bigquery` |
| Data lake | Google Cloud Storage (bucket with lifecycle-expiry rule) |
| Data warehouse | Google BigQuery (partitioned + clustered tables) |
| Transformations | dbt (`dbt-bigquery` 1.8), staging → marts, with tests |
| Orchestration | Kestra (single `docker-compose`, ~1–2 GB RAM) |
| Infrastructure as Code | Terraform (`hashicorp/google` provider) |
| BI / Dashboard | Looker Studio |
| CI | GitHub Actions (ruff, sqlfluff, pytest, `dbt parse` / `--empty`) |
| Dev tooling | `Makefile` interface, `ruff` (Python lint), `sqlfluff` (SQL lint), `pytest` |
| Auth | Application Default Credentials locally; gitignored service-account key for Kestra |

---

## 5. Data model (dbt marts)

- **`stg_github_events`** — staging: cast types, dedupe by event `id`, one row per event.
- **`fct_events`** — fact table, grain = one GH Archive event. Partitioned by `event_date`,
  clustered by `event_type`, `require_partition_filter = true`.
- **`dim_repo`** — distinct repos (`repo_id`, `repo_name`, latest known `language`).
- **`agg_repo_trending_daily`** — WatchEvent (star) count per repo per day → **Tile 1**.
- **`agg_language_daily`** — PR-event count per language per day → **Tile 2**.
- **`agg_repo_momentum`** — cross-signal burst score (`watch + fork + PR`) per repo per day →
  **Tile 3**.
- **`agg_event_type_daily`** — supporting/diagnostic aggregate.

**Projected schema (8 columns):** `id`, `event_type`, `created_at`, `event_date`,
`actor_login`, `repo_id`, `repo_name`, `language`.

**Dashboard (Looker Studio, 3 tiles + date/repo/language filters):**
1. **Trending repos** — top repos by star (WatchEvent) count this week.
2. **Language momentum** — daily PR-event counts per language over time.
3. **Momentum bursts** — repos ranked by a cross-signal burst score (star + fork + PR spiking
   on the same day = likely genuinely viral).

---

## 6. Key engineering decisions & challenges (the substance)

These are the parts worth surfacing in interviews or on a resume, phrased as
problem → decision → outcome.

1. **50–70× storage reduction via field projection + Parquet.**
   Instead of loading raw JSON into BigQuery, the pipeline stream-parses each file and keeps
   only the 8 fields the marts need, written as columnar Snappy Parquet. A 7-day window drops
   from ~50–70 GB to under ~1 GB. This single decision is what makes the free tier feasible.

2. **Streaming parse, bounded memory.**
   Hourly files are hundreds of MB. The transform reads newline-delimited JSON line by line
   (`gzip` + `json`) rather than loading whole files into memory, keeping peak RAM low
   regardless of file size.

3. **Billing guardrails enforced in the schema, not by convention.**
   Raw + `fct_events` are partitioned by `event_date` with `require_partition_filter = true`,
   so BigQuery *rejects* any query lacking a partition filter — an accidental full-table scan
   is physically impossible. `maximum_bytes_billed` is set in the dbt profile so a runaway
   query fails instead of billing. A GCS lifecycle rule expires old objects; a GCP budget alert
   is the backstop.

4. **Idempotent, partition-scoped loads.**
   Each day loads into its own partition with `WRITE_TRUNCATE`, so backfills and re-runs never
   duplicate rows.

5. **A schema correctness trap, found and handled.**
   Repository `language` exists **only** on `PullRequestEvent`
   (`payload.pull_request.base.repo.language`); all other event types carry a bare repo object.
   Naively joining language onto all events yields nulls for most rows. The language signal is
   deliberately isolated to PR events, the limitation is documented, and the REST-API enrichment
   path is noted as future work.

6. **Orchestrator chosen against a hard constraint.**
   Airflow's minimum footprint (~4 GB RAM) won't fit a free-tier VM; Kestra runs the same
   daily-batch DAG in ~1–2 GB via one `docker-compose`. Orchestration concepts transfer; the
   tool was picked to fit the resource budget.

7. **Least-privilege IAM.**
   Terraform provisions a service account scoped to exactly what it needs: `storage.objectAdmin`
   on the one bucket, `bigquery.jobUser` at project level, `bigquery.dataEditor` on the two
   datasets only — not project-wide roles.

8. **Secure auth split.**
   Humans use identity-based auth (ADC via `gcloud`); the machine (Kestra-in-Docker) uses a
   service-account key that is gitignored before it's ever created and mounted read-only. No
   secrets in source control.

9. **Creds-free CI.**
   GitHub Actions never touches live GCP — it runs `ruff`, `sqlfluff`, `pytest`, and
   `dbt parse` / `dbt build --empty`, which validate import correctness, SQL syntax, transform
   logic, and the full dbt model/ref DAG *without* billing risk or a cloud key in CI secrets.

10. **Reproducibility as a first-class goal.**
    `Makefile` targets + Terraform mean a fresh clone reaches a working pipeline through
    documented commands (`make setup` → `make tf-apply` → `make backfill` → `make dbt`), not a
    list of console clicks.

11. **Dashboard designed backwards from real questions.**
    The initial "categorical event-type share" tile answered no real question; it was redesigned
    into three tiles that each answer something a developer would actually open the dashboard to
    learn (what's popular, which languages are active, what went viral). The burst score sums
    three *uncorrelated* signals precisely because any single signal is gameable by bots.

---

## 7. Results / outcomes

- A working end-to-end pipeline: raw GitHub events → GCS → BigQuery → dbt marts → Looker Studio
  dashboard, runnable from a clean clone.
- **~50–70× data-size reduction** (≈50–70 GB of raw JSON for a 7-day window → <1 GB of Parquet),
  which is what keeps the entire project inside GCP's always-free tier (10 GB storage,
  1 TB queries/month).
- Full **infrastructure-as-code** (Terraform) with a **least-privilege** service account.
- **Idempotent** daily ingestion with a 7-day backfill, orchestrated on a schedule via Kestra.
- **Data-quality tested** warehouse (dbt `not_null` / `unique` / `accepted_values` /
  `relationships` tests running on every build).
- **Creds-free CI** gating every push and pull request.
- Satisfies all DataTalksClub DE Zoomcamp rubric areas (cloud, IaC, orchestration,
  data-lake→warehouse, partitioning/clustering, transformations, dashboard, reproducibility).

---

## 8. Skills demonstrated (menu to pull from per JD)

- **Data engineering:** batch ELT, data lake + warehouse modeling, partitioning/clustering,
  dbt (staging/marts, tests, `ref` dependency graph), idempotent loads, backfills.
- **Cloud (GCP):** GCS, BigQuery, IAM, service accounts, ADC.
- **Infrastructure & DevOps:** Terraform, Docker / docker-compose, GitHub Actions CI,
  Makefile-driven workflows.
- **Orchestration:** Kestra flows, scheduling, backfill.
- **Software engineering:** typed/config-driven Python package, streaming parsing, unit tests
  (pytest), linting (ruff/sqlfluff), separation of concerns (config vs. stage modules).
- **Cost & security engineering:** free-tier budget discipline, schema-enforced query guardrails,
  least-privilege IAM, secrets isolation.
- **Data analysis / BI:** Looker Studio dashboard design driven by real user questions.

---

## 9. Honest scope & limitations (do not overstate)

- Solo **portfolio / capstone** project, not a production system with live users or SLAs.
- Designed and validated on a **7-day data window** on the free tier — not run at petabyte
  scale.
- `language` reflects **PR activity only**, not all commit activity (a GH Archive schema
  limitation, documented; REST-API enrichment is planned but not built).
- Streaming/near-real-time ingestion (Pub/Sub), a declarative `dlt` extract/load, and a
  FastAPI/CLI "trends API" over the aggregate tables are documented as **"what's next,"**
  not implemented.

---

## 10. Quick facts (for tables / bullets)

- **Domain:** developer-ecosystem analytics (GitHub trends).
- **Data source:** GH Archive public events, 24 hourly `.json.gz` files/day.
- **Data volume handled:** ~6–10 GB raw JSON/day; ~50–70 GB per 7-day window → <1 GB Parquet.
- **Cloud:** Google Cloud (GCS + BigQuery), region `asia-south1` (Mumbai).
- **Cost:** $0 — entirely within GCP always-free tier.
- **Repo layout:** `ingestion/` (Python) · `dbt/` · `terraform/` · `orchestration/` (Kestra) ·
  `tests/` · `.github/workflows/`.
- **Type:** solo project · DataTalksClub DE Zoomcamp capstone.
