"""Central configuration, read from environment (see .env.example).

Keeping every tunable here means the stage modules stay free of magic strings and the whole
pipeline is reconfigured by editing `.env`, never code.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

GHARCHIVE_BASE_URL = "https://data.gharchive.org"  # files look like {date}-{hour}.json.gz
HOURS_PER_DAY = 24

# Event types we keep. Language used to be present on PullRequestEvent, but GitHub stopped
# emitting it on live data (see ingestion/enrich_language.py).
KEEP_EVENT_TYPES = {
    "PushEvent",
    "WatchEvent",
    "IssuesEvent",
    "PullRequestEvent",
    "ForkEvent",
}

# Fields projected out of each raw event -> becomes the Parquet/BQ schema.
# language used to be nested at payload.pull_request.base.repo.language (PR events only); GitHub
# no longer sends it, so this column is always NULL on live data. Kept for schema stability — see
# ingestion/enrich_language.py for the API-based replacement, which marts read instead.
PARQUET_COLUMNS = [
    "id",          # event id (string) -> dedupe key
    "event_type",  # type
    "created_at",  # timestamp
    "event_date",  # DATE partition key (derived from created_at)
    "actor_login",
    "repo_id",
    "repo_name",
    "language",    # always NULL now; superseded by the repo_language cache table
]


@dataclass(frozen=True)
class Settings:
    project_id: str
    region: str
    bucket: str
    dataset_raw: str
    dataset_marts: str
    raw_table: str
    data_dir: Path
    github_token: str | None

    @property
    def raw_table_fqn(self) -> str:
        return f"{self.project_id}.{self.dataset_raw}.{self.raw_table}"


def load_settings() -> Settings:
    """Build Settings from environment variables. Raises if required vars are missing."""
    def required(name: str) -> str:
        val = os.environ.get(name)
        if not val:
            raise RuntimeError(f"Missing required env var: {name} (see .env.example)")
        return val

    return Settings(
        project_id=required("GCP_PROJECT_ID"),
        region=os.environ.get("GCP_REGION", "us-central1"),
        bucket=required("GCS_BUCKET"),
        dataset_raw=os.environ.get("BQ_DATASET_RAW", "github_pulse_raw"),
        dataset_marts=os.environ.get("BQ_DATASET_MARTS", "github_pulse_marts"),
        raw_table=os.environ.get("BQ_RAW_TABLE", "events"),
        data_dir=Path(os.environ.get("DATA_DIR", "./data")),
        github_token=os.environ.get("GH_PAT"),
    )
