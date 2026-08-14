"""Enrichment stage — fetch repo language from the GitHub REST API and cache it in BigQuery.

Why this exists: GitHub stopped emitting `language` on the PR event payload sometime after the
2024 GH Archive data this project was built against, so `fct_events.language` is always NULL on
live data now. This stage restores
language coverage out-of-band by calling `GET /repos/{owner}/{repo}` for repos seen in recent PR
events and caching the result in `{raw}.repo_language`. dbt joins to this cache at read time
(stg_repo_language) instead of reading the (now-dead) column.

Cache table is UNPARTITIONED — deliberately. The raw dataset's
30-day default_partition_expiration_ms only applies to partitioned tables, so leaving this one
unpartitioned is what keeps it from being silently wiped and re-triggering a full refetch.

Append-only: every fetch adds a row; stg_repo_language dedupes to the freshest per repo_id. A
repo is only re-fetched once its cached row is older than --ttl-days.

    python -m ingestion.enrich_language --lookback-days 7 --ttl-days 7 --max-calls 4500
"""

from __future__ import annotations

import argparse
import time

import requests
from google.cloud import bigquery
from requests.adapters import HTTPAdapter
from urllib3.util import Retry

from .config import Settings, load_settings

CACHE_TABLE_SCHEMA = [
    bigquery.SchemaField("repo_id", "INTEGER"),
    bigquery.SchemaField("repo_name", "STRING"),
    bigquery.SchemaField("language", "STRING"),
    bigquery.SchemaField("fetched_at", "TIMESTAMP"),
]

GITHUB_API = "https://api.github.com"
USER_AGENT = "github-pulse-enrichment (github.com/mahendra-kausik/github-pulse)"


def cache_table_fqn(settings: Settings) -> str:
    return f"{settings.project_id}.{settings.dataset_raw}.repo_language"


def ensure_cache_table(client: bigquery.Client, settings: Settings) -> None:
    """Create the language cache table if absent. Deliberately unpartitioned — see module
    docstring."""
    table = bigquery.Table(cache_table_fqn(settings), schema=CACHE_TABLE_SCHEMA)
    client.create_table(table, exists_ok=True)


def repos_needing_language(
    client: bigquery.Client,
    settings: Settings,
    lookback_days: int,
    ttl_days: int,
    limit: int,
) -> list[tuple[int, str]]:
    """Repos with PR activity in the lookback window whose cache entry is missing or stale.

    Newest-active-first, so a budget cap always spends on the freshest backlog first.
    """
    query = f"""
        with latest as (
            select repo_id, max(fetched_at) as fetched_at
            from `{cache_table_fqn(settings)}`
            group by repo_id
        )
        select e.repo_id, any_value(e.repo_name) as repo_name, max(e.event_date) as last_seen
        from `{settings.raw_table_fqn}` e
        left join latest c using (repo_id)
        where e.event_date >= date_sub(current_date(), interval @lookback day)
          and e.event_type = 'PullRequestEvent'
          and e.repo_id is not null
          and (c.fetched_at is null
               or c.fetched_at < timestamp_sub(current_timestamp(), interval @ttl day))
        group by e.repo_id
        order by last_seen desc
        limit @limit
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("lookback", "INT64", lookback_days),
            bigquery.ScalarQueryParameter("ttl", "INT64", ttl_days),
            bigquery.ScalarQueryParameter("limit", "INT64", limit),
        ]
    )
    rows = client.query(query, job_config=job_config).result()
    return [(row.repo_id, row.repo_name) for row in rows]


class RateLimited(Exception):
    """Raised to stop enrichment cleanly when GitHub's rate limit is hit."""


def fetch_language(session: requests.Session, repo_name: str) -> str | None:
    """GET /repos/{owner}/{repo} -> language, or None (missing/deleted/private/language-less repo).

    Raises RateLimited on 403/429 so the caller can stop rather than sleep out the hour — the next
    scheduled run picks up where this one left off via the lookback window.
    """
    resp = session.get(f"{GITHUB_API}/repos/{repo_name}", timeout=10)
    if resp.status_code == 404:
        return None
    if resp.status_code in (403, 429):
        raise RateLimited(f"{resp.status_code} fetching {repo_name}: {resp.text[:200]}")
    resp.raise_for_status()
    return resp.json().get("language")


def enrich(settings: Settings, lookback_days: int, ttl_days: int, max_calls: int) -> int:
    """Fetch + cache language for stale/missing repos. Returns rows written."""
    if not settings.github_token:
        raise RuntimeError("Missing GH_PAT env var — required for GitHub REST API enrichment")

    client = bigquery.Client(project=settings.project_id)
    ensure_cache_table(client, settings)

    targets = repos_needing_language(client, settings, lookback_days, ttl_days, max_calls)
    if not targets:
        print("No repos need enrichment.")
        return 0

    session = requests.Session()
    session.headers.update(
        {
            "Authorization": f"Bearer {settings.github_token}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
            "User-Agent": USER_AGENT,
        }
    )
    # Transient network blips (connection resets, 5xx) are common at this call volume and
    # shouldn't crash the whole batch — retry those in urllib3 rather than hand-rolling it.
    # 403/429 are excluded so RateLimited below still fires immediately, with no wasted retries.
    retry = Retry(total=3, backoff_factor=0.5, status_forcelist=[500, 502, 503, 504])
    session.mount("https://", HTTPAdapter(max_retries=retry))

    rows = []
    try:
        for repo_id, repo_name in targets:
            try:
                language = fetch_language(session, repo_name)
            except RateLimited as exc:
                print(f"Rate limited after {len(rows)} calls, stopping cleanly: {exc}")
                break
            except requests.exceptions.RequestException as exc:
                print(f"Skipping {repo_name} after request error: {exc}")
                continue
            rows.append(
                {
                    "repo_id": repo_id,
                    "repo_name": repo_name,
                    "language": language,
                    "fetched_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                }
            )
    finally:
        # Write whatever succeeded even if the loop above exited early (rate limit or an
        # exception) — otherwise a single bad request loses an entire run's worth of fetches.
        if rows:
            # Load job (like load_bq.py), not a streaming insert: stays inside BQ's free-tier
            # load quota and matches the batch-write style used everywhere else in this pipeline.
            job_config = bigquery.LoadJobConfig(
                schema=CACHE_TABLE_SCHEMA,
                source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
                write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            )
            job = client.load_table_from_json(
                rows, cache_table_fqn(settings), job_config=job_config
            )
            job.result()

    return len(rows)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--lookback-days", type=int, default=7)
    parser.add_argument("--ttl-days", type=int, default=7)
    parser.add_argument("--max-calls", type=int, default=4500)
    args = parser.parse_args()

    settings = load_settings()
    count = enrich(settings, args.lookback_days, args.ttl_days, args.max_calls)
    print(f"Fetched and cached language for {count} repos")


if __name__ == "__main__":
    main()
