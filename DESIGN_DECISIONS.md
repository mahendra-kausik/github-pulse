# Design Decisions — GitHub Pulse

Every significant design choice made in this project: what was chosen, what was rejected, and
why. Written so you can answer "why did you build it this way?" in SWE interviews.

**Keep this updated.** Every time a new component is added to the project, document its design
rationale here before closing the task.

---

## 1. Orchestrator: Kestra, not Airflow

**Chosen:** Kestra (single `docker-compose`, ~1–2 GB RAM, free OSS)
**Rejected:** Apache Airflow

**Why Kestra won:**
A free GCP `e2-micro` instance has ~1 GB RAM. Airflow's minimum footprint — scheduler +
webserver + worker — needs ~4 GB. It won't run on a free-tier VM. Kestra runs in 1–2 GB.
Beyond RAM: Kestra has native GCS, BigQuery, and dbt plugins with no extra provider packages,
and it's the current DataTalksClub Zoomcamp orchestrator so documentation and community answers
are aligned with this project.

**What's the same between them:** Both model work as DAGs of tasks with dependencies, schedules,
and backfill capabilities. The orchestration concepts are identical; the orchestrator is an
implementation detail.

**Interview answer:** "I evaluated Airflow first. The resource constraint ruled it out — it
won't fit on a free-tier VM. The orchestration concepts are identical across both tools; the
orchestrator is an implementation detail, and I picked the one that fit the constraint."

---

## 2. Storage format: Parquet with field projection, not raw JSON

**Chosen:** Stream-parse each `.json.gz`, project only 8 needed fields, write columnar Parquet
to GCS, then load Parquet to BigQuery
**Rejected:** Loading raw `.json.gz` files or full-row JSON directly into BigQuery

**The storage math:**
One GH Archive day = 24 hourly files, ~6–10 GB of JSON. Seven days = ~50–70 GB. BigQuery's
free storage limit is 10 GB total. Loading raw JSON hits that limit on day 2 of a 7-day window.

By projecting only the fields the marts actually need at ingest time and writing columnar Parquet
(which compresses far better than JSON), 7 days of data stays under ~1 GB — a 50–70× reduction.

**Why columnar Parquet specifically:** Columnar storage means reading only `event_type` scans
only that column, not whole rows. This reduces both storage and BQ query cost on load.

**Why stream-parse instead of loading into pandas/memory:** Each hourly file can be hundreds of
MB. Loading 24 of them into RAM would need ~8–15 GB. `ijson` + `gzip` stream-parses line by
line, so peak RAM usage stays under ~500 MB regardless of file size.

**Interview answer:** "The data ingestion layer isn't transforming for cleanliness — it's
transforming because the storage math doesn't work otherwise. The 50× size reduction is the
most load-bearing engineering decision in the project."

---

## 3. Language data: PR events only — a schema fact, not a design choice

**Chosen:** Extract `language` only from `PullRequestEvent`
**Rejected:** Assuming language is present across all event types

**The GH Archive schema reality:**
`PushEvent`, `WatchEvent`, `IssuesEvent`, and `ForkEvent` all carry the same minimal repo
object: `{ "id": 123, "name": "owner/repo", "url": "..." }` — no language. Language only
appears at `PullRequestEvent.payload.pull_request.base.repo.language`. This is a GH Archive
schema fact. Any query that expects language on a non-PR event returns null for every row.

**Implication for the dashboard:** The language momentum tile reflects PR activity, not overall
commit activity. A solo developer pushing commits to a Rust project with no PRs is invisible
in the language tile.

**Known limitation and how you'd fix it:**
Enrich `dim_repo.language` by calling `GET /repos/{owner}/{repo}` from the GitHub REST API
after ingestion. That endpoint always returns language regardless of event type. Documented
as a "what's next" item.

**Interview answer:** "I discovered this while reviewing the raw GH Archive schema — it's a
correctness trap. If you naively join language onto all events, you get nulls for 80% of rows.
I isolated the language signal to PR events, documented the limitation honestly, and noted the
API enrichment path rather than papering over it."

---

## 4. BigQuery partitioning and clustering

**Chosen:** Partition `events` (raw) and `fct_events` by `event_date` (DATE), cluster by
`event_type`, `require_partition_filter = true` on both tables
**Rejected:** No partitioning; partitioning by timestamp/hour

**Why partitioning:**
BigQuery charges by bytes scanned, not rows returned. Without partitioning, a query for "just
yesterday's data" scans the entire table — all 7 days. With date partitioning, the same query
scans 1/7th of the data and costs 1/7th.

**Why `require_partition_filter = true`:**
This setting makes BigQuery return an error if a query doesn't include a filter on `event_date`.
It makes it physically impossible to accidentally run a full-table scan — not a code convention
to follow, an enforced schema constraint. This is the difference between "we agreed not to do
that" and "the database won't let you do that."

**Why DATE not TIMESTAMP:**
The ingestion cadence is one Parquet file per day. Hourly partitions would create 24× more
partition metadata per day with no benefit — queries are always in day-granularity anyway.

**Why cluster by `event_type`:**
After filtering by date, the next most common filter is event type (e.g. "only WatchEvents for
trending repos"). Clustering physically co-locates rows of the same type, so BQ scans even less.

**Interview answer:** "Partitioning + `require_partition_filter` is a billing guardrail enforced
in the schema, not in application code. A future teammate can't write a query that blows through
the free-tier budget by accident — the database itself rejects it."

---

## 5. Dashboard: 3 signal-based tiles, not raw event-type counts

**Original design (rejected):** 2 tiles — event-type share pie chart + language bar chart
**Chosen:** 3 tiles — trending repos (star velocity) + language momentum + momentum bursts

**Why the original was weak:**
"30% of GitHub events were PushEvents this week" answers no question anyone has. The categorical
tile was filling the Zoomcamp rubric's "categorical tile" requirement without actually being
useful.

**The redesign — working backwards from real questions:**

| Question a developer would ask | Tile | Signal | Source |
|---|---|---|---|
| What repos are people excited about right now? | Trending repos | WatchEvent (= star) count per repo per day | All WatchEvents |
| Which languages are seeing active development? | Language momentum | PR count per language per day | PullRequestEvents only |
| Did any repo go viral this week? | Momentum bursts | watch + fork + PR all spiking together | All three event types |

**Why "momentum bursts" is the most defensible tile analytically:**
A single signal can be noise — a bot could star a repo 500 times. Three uncorrelated signals
(someone starred it, someone forked it, someone opened a PR against it) spiking on the same day
is a reliable indicator that a real human audience discovered it.

**Interview answer:** "I initially built the categorical tile to satisfy the rubric, then realised
it answered no real question. I redesigned around what a developer would actually open the
dashboard to find out, and worked backwards to the SQL from there."

---

## 6. Authentication: ADC locally, service-account key for Kestra

**Chosen:** Application Default Credentials (ADC) for local development; a gitignored
service-account key file mounted into the Kestra Docker container
**Rejected:** Service-account key for local dev; hardcoded credentials anywhere

**Why ADC for local:**
`gcloud auth application-default login` binds your personal GCP identity to all SDK calls on
your machine. No key file to create, accidentally commit, or rotate. Your personal credentials
already have the right permissions if you're the project owner.

**Why SA key for Kestra:**
Kestra runs inside Docker. It can't access your local `gcloud` session or OS credential store.
It needs explicit credentials. The SA key is:
- Gitignored via `*key*.json` (committed to `.gitignore` before the key is ever created)
- Mounted as a read-only Docker volume (`./secrets:/secrets:ro`)
- Never baked into the container image or printed in logs

**Principle:** Humans use identity-based auth (ADC). Machines use key-based auth, with the key
isolated to the environment that needs it and excluded from source control.

**Interview answer:** "ADC for humans, SA key for machines — and the machine key is isolated to
the container that needs it and gitignored before it's ever created. No secrets in source control."

---

## 7. CI: creds-free lint and structural checks, no live BigQuery

**Chosen:** GitHub Actions runs `ruff` + `sqlfluff` + `pytest` (mocked) + `dbt parse` — no
GCP authentication, no real BQ queries
**Rejected:** CI that stores a GCP service-account key in GitHub Secrets and runs real dbt builds

**Why no live BQ in CI:**
1. **Billing risk:** Any pull request — including one from a fork — could trigger expensive BQ
   queries. A malicious PR could run `SELECT *` without a partition filter before the guard
   is reviewed.
2. **Security surface:** A GCP SA key in GitHub Secrets can be exfiltrated via a compromised
   Action or dependency.
3. **Not necessary:** The checks that catch real bugs (import errors, SQL syntax errors, dbt
   model structure, transform logic) all work without live data. `dbt parse` validates the
   full DAG structure without executing any queries.

**What CI does catch:** Python import errors, unused imports, line length violations (ruff);
SQL syntax errors and style (sqlfluff); transform field projection and PR language extraction
correctness (pytest); dbt model/ref/source wiring (dbt parse).

**Interview answer:** "CI should catch bugs, not introduce billing risk. Everything worth
catching — import errors, SQL syntax, dbt structure, business logic — doesn't need live data."

---

## 8. dbt for transformations, not ad-hoc SQL scripts

**Chosen:** dbt with a staging → marts two-layer model
**Rejected:** Raw SQL scripts executed directly against BigQuery; Python/pandas transforms

**Why dbt over raw SQL:**
Three things raw SQL scripts don't give you:
1. **`ref()` for dependency tracking:** dbt knows which model depends on which and builds in
   the correct order automatically. Raw scripts need a manually maintained execution order.
2. **Built-in test framework:** `not_null`, `unique`, `accepted_values`, `relationships` run
   alongside every `dbt build`. A data quality failure breaks the build before bad data reaches
   the dashboard.
3. **`dbt build --empty`:** Validates the full model/ref/source DAG structure without touching
   BigQuery — the basis for creds-free CI.

**Why staging → marts, not one flat layer:**
Staging (`stg_github_events`) casts types and dedupes once. Every mart model consumes clean,
typed staging data. If the raw BQ schema changes (e.g. a column is renamed), you fix it in
one place — staging — and all marts are automatically correct. A flat model layer means
hunting down the type cast or dedupe logic in every model that uses raw data.

**Interview answer:** "dbt gives you dependency management, data quality tests, and CI-friendly
structural validation — none of which you get from SQL scripts. The staging/marts split means
schema changes have one fix point, not N."

---

## 9. Walking skeleton before Terraform and Kestra

**Chosen:** Get one full day of data flowing end-to-end manually — download → Parquet → GCS
→ BQ load → one dbt model — before writing any IaC or orchestration code
**Rejected:** Writing Terraform and Kestra first, then doing the first data run through them

**Why:** GCP authentication is the #1 source of lost hours in DE projects. ADC scopes, IAM
role bindings, dataset-level permissions, and service-account impersonation all have subtle
failure modes. Debugging a permissions error inside a `terraform apply` or inside a Kestra
Docker container (where logs are three layers deep) takes 3–5× longer than debugging the same
error in a plain Python script with a direct stack trace.

Getting `gcloud auth application-default login` → GCS write → BQ load working in raw Python
first proves the foundation. Terraform and Kestra then layer on top of a confirmed-working base
rather than introducing two unknown variables simultaneously.

**Interview answer:** "De-risk the hardest part first. GCP auth has subtle failure modes.
Debugging it in a plain Python script takes 10 minutes; debugging it inside Terraform or a
Docker container takes an hour. I proved the foundation works before building anything on top."

---

## 10. Terraform for infrastructure, not manual console setup

**Chosen:** Terraform (`terraform/main.tf`) for GCS bucket, BigQuery datasets, and service account
**Rejected:** Creating resources manually in the GCP console and documenting the steps

**Why Terraform:**
Manual console setup is not reproducible. If someone clones the repo and follows the README,
they'd have to read a list of manual steps, make the right clicks, and hope nothing changed in
the console UI. `terraform apply` is a single command that creates the exact same resources
every time, and `terraform plan` shows a diff before anything is created.

It also directly satisfies the Zoomcamp rubric's "IaC" requirement.

**Least-privilege SA:** The Terraform config creates a service account with the minimum IAM
roles needed — `storage.objectAdmin` on the bucket only (not all GCS), `bigquery.jobUser`
at project level, `bigquery.dataEditor` on the two datasets only (not all BQ). This matters
for interviews: "least-privilege" is a security principle, and scoping IAM to specific
resources rather than project-wide roles demonstrates it.

**Interview answer:** "Terraform makes the infrastructure reproducible — `terraform apply` is
the only step, not a list of console clicks. It also lets me demonstrate least-privilege IAM:
the SA key Kestra uses can only write to this project's specific bucket and two datasets,
nothing else."
