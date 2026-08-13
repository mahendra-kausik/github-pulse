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
MB, and a full day is several million kept events — materializing that as a Python list before
one `pa.Table.from_pylist` call would need multiple GB of RAM. `transform_day` instead streams
line-by-line NDJSON parsing (`gzip` + `json.loads`, GH Archive is newline-delimited so this needs
no dedicated streaming-JSON library) into fixed-size batches (`BATCH_SIZE = 200_000` rows), each
flushed straight to disk through an open `pq.ParquetWriter`. Peak RAM is bounded by one batch, not
one day.

**Why the Parquet writer parses fields as strings, then casts to a typed schema:** `extract_event`
projects raw JSON values (`created_at`, `event_date` are ISO strings straight out of the source
data) with no per-field parsing logic. Each batch is built against a plain string schema, then
`.cast()` to the typed schema (`created_at` → `TIMESTAMP`, `event_date` → `DATE`) right before
writing — Arrow's cast does the ISO8601 parsing in one vectorized pass instead of a Python loop.
This also keeps the written Parquet file's logical types identical to the BigQuery raw table's
column types (`ingestion.load_bq.RAW_TABLE_SCHEMA`), which matters because a Parquet load into
BigQuery validates types against the target table and does not coerce STRING into TIMESTAMP/DATE
the way a CSV or JSON load would — a mismatch here fails every load. `tests/test_transform.py`
locks the two schemas together so they can't drift apart silently again.

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

**Why the table schema lives in Python, not Terraform:** `ingestion/load_bq.py`'s `ensure_table`
creates the raw table (`exists_ok=True`) with `RAW_TABLE_SCHEMA`, partitioning, and clustering,
rather than a `google_bigquery_table` Terraform resource. The loader is the single place that
both defines the schema and writes to it, so the two can never drift out of sync — a Terraform
resource would need its schema hand-kept in lockstep with `ingestion/transform.py`'s Parquet
output. Terraform owns what doesn't change per-run (bucket, datasets, IAM); the loader owns the
table it's responsible for filling.

**Why every generic test on `fct_events` carries an explicit `where`:** `require_partition_filter`
doesn't only guard hand-written queries — dbt's generic tests (`not_null`, `unique`,
`accepted_values`, `relationships`) compile to a `select` against the model with no filter unless
one is configured, so without a `where` on each test `dbt build` would fail with the exact error
the guardrail is designed to raise. Each test in `dbt/models/marts/schema.yml` sets
`config.where: "event_date >= '{{ var(\"start_date\") }}'"`, which is also why `dbt build --empty`
(rewrites refs as `where false`) can never pass here and isn't run in CI — `--empty` and
`require_partition_filter` are structurally incompatible.

**Interview answer:** "Partitioning + `require_partition_filter` is a billing guardrail enforced
in the schema, not in application code. A future teammate can't write a query that blows through
the free-tier budget by accident — the database itself rejects it, including my own dbt tests,
which is why they carry the same date filter a real dashboard query would."

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

**Why the flow's tasks run via Kestra's Process runner, not a Docker task runner:** The flow's
`ingest`/`dbt_build` tasks run as plain `Commands` tasks, executing inside the Kestra container
itself rather than spawning a fresh `python:3.11-slim` container per task. A Docker task runner
would need its own path to the same two things the Kestra container already has configured —
the `/secrets` mount and the GCP env vars — via a `docker.sock` mount plus Kestra's `secret()`
plumbing (base64 `SECRET_*` env vars on the server). Reaching for that machinery to reinvent
access the host process already has is exactly the kind of infra-for-infra's-sake this project
tries to avoid; the Process runner gets both for free from `docker-compose.yml`'s existing
`env_file` and volume mount.

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

**Why sqlfluff uses the Jinja templater, not the dbt templater:** sqlfluff's `dbt` templater
compiles each model by actually invoking dbt, which needs a working BigQuery connection — the
opposite of "creds-free". The `jinja` templater's `apply_dbt_builtins` option stubs `ref()`,
`source()`, `config()`, and `var()` well enough to lint this project's models with zero dbt
process and zero BigQuery connection.

**Why `dbt build --empty` isn't in CI, only `dbt parse`:** `--empty` rewrites every `ref`/`source`
as `select * from <relation> where false limit 0` to validate the DAG without real data — but
`where false` isn't a partition-elimination predicate, so it fails outright against any table with
`require_partition_filter` set (both the raw source and `fct_events` have it). `dbt parse`
validates the same DAG structure (refs resolve, Jinja compiles, no duplicate models) without
compiling to SQL at all, so it validates everything `--empty` would have without hitting the
guardrail `--empty` is structurally incompatible with.

**Interview answer:** "CI should catch bugs, not introduce billing risk. Everything worth
catching — import errors, SQL syntax, dbt structure, business logic — doesn't need live data. And
where I could have reached for a tool's dbt integration (sqlfluff, `dbt build --empty`), I checked
whether it actually needed a live connection first, rather than assuming the credentials-free flag
made it so."

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

This constraint is enforced, not aspirational: `dbt/dbt_project.yml` deliberately has no
`+schema` override on the `staging`/`marts` model config, so both layers land in the profile's
single target dataset (`github_pulse_marts`) rather than dbt's default schema-name concatenation
producing new dataset names on the fly. `dataEditor` lets the SA write rows into an existing
dataset; it does not include `bigquery.datasets.create`. A `+schema` override here would have
forced dbt to try creating a dataset the SA has no permission to create, on every run — the
least-privilege design only holds because the model config was written to fit inside it.

**Interview answer:** "Terraform makes the infrastructure reproducible — `terraform apply` is
the only step, not a list of console clicks. It also lets me demonstrate least-privilege IAM:
the SA key Kestra uses can only write to this project's specific bucket and two datasets,
nothing else — and I had to make sure dbt's own dataset targeting didn't quietly ask for more
than that, since dbt's default schema-naming would otherwise try to create new datasets the SA
can't."

---

## 11. Mart model design: separate agg_repo_trending_daily and agg_repo_momentum

**Chosen:** Two separate mart models for the repo-signal tiles:
- `agg_repo_trending_daily` — WatchEvent (star) count only, grain = (event_date, repo)
- `agg_repo_momentum` — cross-signal burst score (watch + fork + PR), grain = (event_date, repo)

**Rejected:** A single combined repo-signal model that merges all three metrics

**Why separate models:**
The two tiles answer different questions. `agg_repo_trending_daily` answers "what's popular right now?" (pure star velocity — fast to compute, easy to explain). `agg_repo_momentum` answers "did a repo genuinely go viral?" (correlated spike across uncorrelated signals — harder to game). Merging them into one model couples a simple time-series metric with a composite score in a way that makes neither cleaner.

**Why `burst_score = watch + fork + PR` specifically:**
Three signals are deliberately uncorrelated: starring is passive (one click), forking implies intent to use or contribute, opening a PR implies active work. A bot can inflate any single signal cheaply. Spiking all three on the same day strongly implies a real human audience discovered the repo. The score is additive (not a ratio or weighted average) to keep it explainable without domain knowledge.

**Why the `having` clause is load-bearing:**
The additive score alone does *not* deliver the property above, and for a while the model didn't
either — the doc claimed "spiking all three" while the SQL summed three counts without ever
requiring more than one to be non-zero. Against real data that ranked CI-probe repos
(`WolseyBankWitness/rediffusion` at 7,379 automated PRs, 0 stars, 0 forks;
`google-test/signclav2-probe-repo`; `actions-canary/ForkPRCanary`) above genuinely viral ones.
`having watch_count > 0 and fork_count > 0 and pr_count > 0` is what actually enforces the
cross-signal requirement, and it drops 99.6% of rows (4,284,388 → 15,135) because almost every
repo-day touches exactly one signal — which also made this mart 0.32 GB → 1.1 MB.

**Why `countif` instead of a pivot/conditional join:**
Both models read from `fct_events` and use `countif(event_type = '...')` to count each signal in a single scan. A pivot or separate subquery per event type would require three table reads instead of one — three times the bytes billed for the same result.

**Interview answer:** "The two tiles answer different questions — popularity vs. genuine viral momentum — so I kept them as separate models. The burst score uses three uncorrelated signals because any single signal is gameable; spiking all three on the same day is a reliable indicator of real human discovery."

---

## 12. Idempotency and reproducibility hardening

**Chosen:** Download to a `.part` temp file and rename on success; explicitly enable the GCP APIs
Terraform depends on
**Rejected:** Leaving both as implicit assumptions ("the download completed", "the project already
has these APIs enabled")

**Why the `.part` rename:** `download_hour` originally treated any non-empty file at the final
path as "already downloaded" and skipped re-fetching it. An interrupted download (network drop,
Ctrl-C) leaves a truncated `.gz` at that exact path, which is non-empty — so every later run
would skip it as complete, and `transform` would then fail deep inside `gzip.open` on a corrupt
member. Writing to `{name}.gz.part` and only `os.replace()`-ing it to the final name after a full
successful download means a file only ever exists at the final path if it's complete — the
"idempotent, safe to re-run" property the ingestion docstrings already claimed becomes true.

**Why explicit `google_project_service` resources:** A brand-new GCP project doesn't have
`storage.googleapis.com` / `bigquery.googleapis.com` / `iam.googleapis.com` enabled by default.
Without declaring them, `terraform apply` on a fresh project fails with `SERVICE_DISABLED` on
the first resource that needs one, and the fix ("go enable these APIs in the console first") is
exactly the kind of manual, undocumented step Terraform is supposed to eliminate (see §10).
Declaring them as resources with `depends_on` from the bucket/dataset/SA resources makes
`terraform apply` from a genuinely fresh project a single command again.

**Interview answer:** "Two small gaps that only show up on someone else's machine: a download
that gets interrupted shouldn't poison every future run by looking 'done', and infra-as-code
should mean one command works on a brand-new project, not 'terraform apply, then go click enable
in the console when it fails'."

---

## 13. Region: `us-central1`, not `asia-south1`

**Chosen:** `us-central1` for the GCS lake bucket and both BigQuery datasets
**Rejected:** `asia-south1` (Mumbai) — the original choice, reversed here

**Why the reversal:** GCS's always-free tier (5 GB storage) only applies to three US regions
(`us-east1`, `us-west1`, `us-central1`). `asia-south1` isn't one of them, so every byte in the
lake bucket would be billed from day one — a small amount given the 30-day retention rule and
sub-1 GB Parquet footprint, but a direct contradiction of the project's stated free-tier
constraint. BigQuery's free tier is region-agnostic, so this decision is driven by GCS alone.

**Interview answer:** "I'd originally picked the region nearest to me. Re-checking the free-tier
terms, GCS's always-free storage is US-region-only — BigQuery's isn't, but GCS's is. When a
'genuinely free' claim and a regional preference conflict, the constraint wins."

---

## 14. Deployed into an existing project, not a fresh one

**Chosen:** Deploy into `mini-raft-prod`, a pre-existing GCP project with no organization parent
**Rejected:** A newly created dedicated project

**Why:** The linked billing account is capped at 3 projects, all already in use — creating a 4th
project fails to link to billing (`FAILED_PRECONDITION: quota exceeded`), and BigQuery/GCS don't
function without billing. Rather than requesting a quota increase (a support-ticket process with
no guaranteed timeline), the pipeline reuses an existing project with headroom.

**Why not the other existing project:** A second candidate project existed but is parented to a
Google Cloud organization that enforces `constraints/iam.disableServiceAccountKeyCreation`. This
project's Kestra design (§6) requires creating a service-account key file to mount into the
Docker container — that org policy blocks key creation outright, which would only have surfaced
at the orchestration step, not at `terraform apply`. `mini-raft-prod` has no organization parent,
so the policy doesn't apply.

**Interview answer:** "GCP billing accounts and org policies are real constraints, not just infra
noise — I hit a hard project-count cap on the billing account, and separately found an org policy
that would have silently blocked service-account key creation for Kestra on the other available
project. I checked both constraints with `gcloud` before committing to a project, rather than
discovering the second one three steps into deployment."

---

## 15. Kestra image pinned to `v1.3.32`, not `latest`

**Chosen:** `image: kestra/kestra:v1.3.32` in `orchestration/docker-compose.yml`
**Rejected:** `kestra/kestra:latest` (the original), and the older `v0.20.7`

**Why:** `latest` makes the "reproducible from a fresh clone" claim false — two people running
`docker compose up` on different days get different Kestra versions, and a breaking upstream change
turns into a mystery failure in someone else's environment rather than a deliberate upgrade. The
first pin attempt used `v0.20.7`, which no longer exists in the registry; Kestra had since moved to
1.x and pruned the old tag, so the pull failed outright. `v1.3.32` is a real, current tag that was
verified to pull and run.

**Related:** flows in `./flows` are *not* auto-loaded under the Postgres backend — they must be
imported via the UI or `kestra flow namespace update`. The compose comment says so, because the
previous comment claimed auto-load and cost an hour of "why is my flow not there".

**Interview answer:** "Pinning versions is table stakes for reproducibility, but the interesting
part was that my first pin was to a tag that had been pruned upstream. An unpinned `latest` fails
silently and later; a pin to a dead tag fails loudly and immediately. I'd rather have the loud
failure — I fixed it in one step instead of debugging a version drift weeks in."

---

## 16. Kestra script tasks: explicit Process runner, explicit `/usr/bin/python3`

**Chosen:** declare `taskRunner: io.kestra.plugin.core.runner.Process` on both script tasks, and
invoke the interpreter by absolute path with `--break-system-packages`
**Rejected:** the Docker task runner (mounting `/var/run/docker.sock` into the Kestra container)

**Why Process:** the ingest and dbt tasks need GCP config and the service-account key. The Kestra
container already has both — `env_file: ../.env` in compose, and the `./secrets:/secrets:ro` mount.
The Docker runner spawns a *sibling* container that inherits neither, so it would need a
`docker.sock` mount (handing the container control of the host Docker daemon) plus its own copy of
the env and key plumbing. That's a real privilege increase to gain nothing at this scale.

**Two failures this surfaced, both worth knowing:**

1. **The runner default is Docker, not Process.** The flow's comment already *claimed* Process, but
   never declared it, so the first run died on `Docker socket is not accessible`. A comment
   describing intent the YAML doesn't state is worse than no comment.
2. **Bare `python3` is the wrong interpreter.** The Kestra image ships a `uv`-built venv at
   `/app/.venv` that wins on `PATH` and contains no `pip`, so `pip install` failed with
   `No module named pip`. The Debian interpreter at `/usr/bin/python3` does have pip but is
   externally managed (PEP 668), and the image ships no `ensurepip` — so building our own venv
   isn't an option either. Hence `/usr/bin/python3 -m pip install --break-system-packages`, which
   installs to `~/.local` as the non-root `kestra` user. dbt's console script lands in
   `~/.local/bin` (not on `PATH`), so that's prepended inline on the `dbt build` command.

**Interview answer:** "Running the pipeline inside the orchestrator container instead of a sibling
container was a security call as much as a convenience one — the Docker runner would have meant
mounting the host Docker socket, which is effectively root on the host, just to re-plumb
credentials the Kestra container already had. Getting it working meant reading the actual image:
its default task runner isn't what the docs example implies, and it ships a pip-less uv venv that
shadows the system Python. I found both by inspecting the running container rather than guessing at
the error message."

---

## 17. Live daily schedule: GitHub Actions, not Kestra-on-a-laptop or Cloud Run

**Chosen:** a scheduled GitHub Actions workflow (`.github/workflows/daily_ingest.yml`) runs the
pipeline unattended every day; Kestra's own daily trigger ships `disabled: true`
**Rejected:** leaving Kestra-in-Docker as the live schedule; Cloud Run Job + Cloud Scheduler

**Why not Kestra-in-Docker as the live schedule:** it only runs while its Docker container is up,
which on a laptop means "whenever I happen to have it open." A pipeline that's supposed to run
daily but only runs when someone remembers to start Docker isn't actually running daily. This isn't
a defect in the Kestra work from §15–16 — Kestra is still the demonstrated orchestrator and the
on-demand/backfill path — it's a mismatch between "runs in a laptop-hosted container" and "runs
unattended, forever."

**Why GitHub Actions over Cloud Run + Cloud Scheduler:** both are viable and free at this scale.
GitHub Actions won because it's ~15 minutes of work reusing the exact command sequence already
proven in the Kestra flow's `ingest`/`dbt_build` tasks, versus a few hours to containerize
(Dockerfile, Artifact Registry, IAM, new Terraform resources) for materially the same outcome. The
repo is public, so Actions minutes are free with no cap — not a trial-credit-subsidized "free for
now."

**Checked before deciding, not assumed:** `gcloud scheduler jobs list` showed zero existing jobs in
`mini-raft-prod`, so no quota conflict with the project's other resource, a `mini-raft` VM
(`e2-small`, `asia-south1`) unrelated to GitHub Pulse. That VM is also worth knowing about
independent of this decision: it doesn't qualify for GCP's Always Free VM tier (wrong machine
shape and region — that tier only covers `e2-micro` in `us-west1`/`us-central1`/`us-east1`), so
this GCP account wasn't running purely on the always-free tier before this work touched anything.

**Auth reuses what already exists, doesn't mint new credentials:** the workflow authenticates with
the same service-account key already created for Kestra (`orchestration/secrets/sa-key.json`,
pasted into a GitHub encrypted secret, never committed). `google-github-actions/auth` sets
`GOOGLE_APPLICATION_CREDENTIALS`, which both the `google-cloud-*` clients (ADC, zero code change)
and dbt's existing `service_account` profile target (`dbt/profiles.yml`, built for Kestra's mounted
key) already read — one credential, two consumers, no new auth code.

**Why disable Kestra's trigger instead of deleting it:** two schedulers both firing
`ingestion.load_bq`'s `WRITE_TRUNCATE` against the same date is harmless (idempotent, per-partition)
but pointless — double compute for an identical result. `disabled: true` keeps the trigger, and the
flow, as a working demo of "this pipeline can be scheduled by a real orchestrator" without it
competing with the thing actually running live. `recoverMissedSchedules: NONE` travels with it as a
standing property (not a one-time fix) — re-enabling the trigger after any period of being off
would otherwise fire one execution per missed day all at once, the same class of bug fixed on the
GCS Kestra image pin in §15.

**Interview answer:** "The Kestra work stands, but a scheduler that only runs when a laptop's
Docker is open isn't a real daily schedule. Rather than solve that by deploying Kestra somewhere
it'd run 24/7, I asked what the actual requirement was — 'runs unattended, cheaply, reusing proven
commands' — and GitHub Actions cron satisfied that in fifteen minutes versus a few hours to
containerize for Cloud Run. I kept Kestra as the on-demand orchestrator rather than ripping it out,
because the requirement changed, not the value of what I'd built."

---

## 18. 30-day BQ retention, knowingly over the free tier

**Chosen:** `default_partition_expiration_ms` = 30 days on the raw dataset
**Rejected:** 14 days, which would stay inside the 10 GB free tier

**Why this needed a second look:** the original estimate for this window (§ live-data planning)
assumed a 2026 day of GH Archive data costs ~0.19 GB in BigQuery, extrapolated from the *gzip
download size* being ~3.4× smaller than a 2024 hour. That ratio doesn't hold for storage — measured
directly against the live 2026-08-06…12 window, `events` is 1.99 GB for 7 days (~0.285 GB/day), and
`fct_events` mirrors it almost exactly (same row count, same columns), so the two tables together
grow at **~0.57 GB/day**. At 30 days that's **~17 GB steady state**, roughly 70% over the 10 GB
free tier (~$0.15/month in overage once the window fills). A 14-day window would land near 8.2 GB
— comfortable margin.

**Why 30 anyway:** flagged explicitly with both numbers before deciding, not discovered later. The
dashboard is more useful with a month of rolling history than two weeks, and $0.15/month is not a
cost worth trading real dashboard usefulness for — the project's "always-free" framing was a design
default under uncertainty, not a hard constraint once the actual cost of relaxing it is three cents
a week. This is a disclosed, deliberate exception to `CLAUDE.md`'s free-tier rule, not a violation
found after the fact.

**Interview answer:** "My first estimate for this was wrong — I'd extrapolated from download file
size instead of measuring actual BigQuery storage, and the real number came out about 3× higher.
Rather than quietly keep the wrong number or silently swap in a shorter window, I recomputed against
live data, showed both options with real GB and dollar figures, and let the actual tradeoff — a
better dashboard for fifteen cents a month — be a conscious choice instead of an assumption baked
into Terraform."

## 19. Language enrichment via the GitHub REST API, not the event payload

**Chosen:** fetch language from `GET /repos/{owner}/{repo}`, cache it in an unpartitioned
`raw.repo_language` table, join at query time.
**Rejected:** keep reading `fct_events.language`; a one-shot backfill job; `MERGE`-on-write;
partitioning the cache table; stamping language onto event rows at ingest.

**Why this needed fixing at all:** GitHub stopped emitting `language` on the PR event payload
(`payload.pull_request.base.repo.language`) sometime between the 2024 GH Archive data this project
was built against and Aug 2026. Verified directly against raw files on disk
(`data/2026-08-12/2026-08-12-0.json.gz`): `payload.pull_request.base.repo` has only `id`/`name`/
`url` across all 409 PR events in that hour, zero exceptions. This silently zeroed out two models —
`agg_language_daily` (0 rows) and `dim_repo.language` (0/2,039,016 non-null) — with no error
anywhere, because BigQuery doesn't complain about a column that's always NULL. `transform.py` and
the raw table schema still project the field (kept for schema stability, and it starts working
again for free if GitHub ever restores it); the marts just stopped reading it.

**Cache location — unpartitioned `raw.repo_language`, not a dbt seed or a partitioned table:** it's
fetched data, not a transformation, so it's written by Python (`ingestion/enrich_language.py`, same
shape as `load_bq.ensure_table`) and lives beside `raw.events`. The pipeline SA already has
`roles/bigquery.dataEditor` on that dataset (`terraform/main.tf`), so no IAM change was needed.
**It must stay unpartitioned**: the raw dataset carries `default_partition_expiration_ms = 30 days`
(§18), which only applies to partitioned tables. Partitioning this cache would silently wipe it
every 30 days and re-trigger a full ~77k-repo refetch — a trap worth naming explicitly since nothing
would error, it would just quietly get expensive again.

**Append-only + read-side dedup over `MERGE`:** every fetch appends a row; `stg_repo_language`
takes the freshest per `repo_id` via the same `row_number()` idiom already used in
`stg_github_events.sql`. Simpler write path (one `load_table_from_json` call, no `MERGE` DML), and
cheap to keep that way — measured over a 7-day window, 89.3% of the 76,629 repos with PR activity
appeared on only one day, so the table's growth is dominated by first-time-seen repos regardless of
write strategy. Upgrade path if it ever gets unwieldy: swap to `MERGE`, which is a drop-in change
given the dedup already happens at read time.

**Join at query time, not stamped onto events at ingest:** `dim_repo` and `agg_language_daily` join
`fct_events`/`stg_github_events` to the cache on `repo_id` via `stg_repo_language`, rather than
writing language into event rows. Consequence worth having: re-enriching a repo retroactively
corrects every historical row for it still inside the retention window — a misclassification or a
genuine language change self-heals instead of being frozen at ingest time.

**TTL = 7 days, not 30:** only 32 of 76,629 repos were active on all 7 measured days, and within a
7-day sample no repo can cross a 7-day boundary twice — so a 7-day and a 30-day TTL produce
*identical* API cost over that window. Extrapolated to a year the gap only touches the small
highly-recurring sliver (low thousands of extra calls/year against a 5,000/hr authenticated
budget). 7 days buys meaningfully fresher data for effectively nothing, and matches the dashboard's
own rolling window.

**Budget cap + lookback window, not a one-shot backfill job:** each run selects uncached-or-stale
repos across a lookback window (not a single date), newest-active-first, capped at `--max-calls`.
One mechanism covers seeding, daily growth, and catch-up after a budget-capped or spike day — a
per-date design would strand anything missed on its one day forever. This also absorbs data
irregularity gracefully: one measured day (2026-08-07) alone contributed 45,224 new repos versus
4k–14k on every other day, almost certainly a bot/mass-PR burst, and the budget cap turns that into
"picked up over the next couple of runs" instead of a failure.

**404 caches as `language = NULL`, no status column:** deleted/private/renamed-away repos are
indistinguishable from genuinely language-less repos (e.g. docs-only) for dashboard purposes — both
are excluded by the existing `language is not null` filter. Caching the null avoids re-fetching dead
repos on every run. A `status` column can be added later if something ever needs to tell the two
cases apart; nothing does yet.

**Rate limiting:** on 403/429 the run stops cleanly rather than sleeping out the hour — the next
scheduled run resumes automatically via the lookback window, so there's no retry logic to get wrong.
Sequential requests, not concurrent: ~4,500 calls at ~200ms is ~15 minutes, comfortably inside GitHub
Actions' 6-hour job cap, and GitHub's secondary rate limits actively punish concurrent request
bursts more than they reward the time saved.

**Interview answer:** "The language tile went from 'seems fine' to 'zero rows' between when I built
this against 2024 archive data and when I cut it over to live 2026 data — GitHub had quietly
stopped sending that field on PR payloads, and nothing errored because BigQuery doesn't complain
about an always-NULL column. Rather than patch around it, I measured how the repo population
actually behaves — turned out 89% of repos only ever show up once in a week — and used that to
justify a short TTL and a budget-capped incremental fetcher instead of a one-shot backfill, so the
same code path handles seeding, daily growth, and recovering from a bad day without any special
cases."

## 20. `start_date` is a static floor, not a rolling window

**Chosen:** `vars.start_date` pinned to `2024-01-01` — deliberately older than any data that can exist
**Rejected:** keeping it pinned to the live-cutover date and rolling it forward as retention advances

**The bug it caused:** `start_date` was set to `2026-08-06`, the date of the live-data cutover, with a
comment saying it "rolls forward as the raw table's 30-day partition expiration ages out older
partitions." Every staging and mart model filters `event_date >= start_date`, so when 14 days
(`2026-07-23`…`2026-08-05`) were backfilled into `raw.events`, **none of them reached the marts**.
`dbt build` reported `PASS=40 ERROR=0` — nothing failed, the rows simply never matched a `where`
clause. Raw held 75.0M rows while `fct_events` held 23.2M, and the only symptom was a dashboard that
looked exactly as it had before the backfill.

**Why static beats rolling:** the lower bound's only real job is to keep a constant partition filter
present so `require_partition_filter` is satisfied and BigQuery can prune. The *actual* lower bound on
what exists is already enforced in exactly one place — the 30-day `default_partition_expiration_ms` in
Terraform. A second bound that tries to track the first is duplicated logic, and duplicated logic
drifts: the two only have to disagree once, silently, to lose data. A floor older than the retention
horizon can never exclude a real row, and costs nothing, because retention guarantees there is never
anything older to scan.

**Why not `--vars` for the backfill:** a one-off `dbt build --vars '{start_date: 2026-07-23}'` would
have fixed the local run and left the committed default untouched — so the next unattended GitHub
Actions run, which invokes plain `dbt build`, would have rebuilt the marts back down to the 7-day
window and silently undone the backfill overnight. A scheduled job that reverts your work while you
sleep is worse than the original bug.

**Guardrail resized, not removed:** with the filter corrected, the full `fct_events` rebuild scans all
of raw and needed 6.29 GB, tripping `maximum_bytes_billed` at 5 GB — a limit sized when the window was
7 days. Raised to 20 GB in all four places that pin it (`.env`, `.env.example`, `daily_ingest.yml`,
`profiles.yml`). The ceiling on a *legitimate* query here is bounded by retention at ~9 GB, so 20 GB
absorbs event-volume growth while still failing a genuine runaway. Note the failure mode was the
good one: the guardrail refused the query loudly instead of quietly billing for it.

**Interview answer:** "A backfill I ran looked like it worked — the ingestion logs were clean, dbt
reported forty passing tests, zero errors — but the dashboard didn't change. The cause was a date
filter defaulting to the day the project went live, so the older data I'd just loaded was filtered out
of every model. What made it dangerous wasn't the wrong value, it was that it failed silently: an
empty result set isn't an error. The fix I liked wasn't correcting the date, it was removing the
second source of truth entirely — retention already defines the lower bound in Terraform, so the SQL
filter just needs to be a floor that can never be wrong, not a copy of it that has to be maintained."
