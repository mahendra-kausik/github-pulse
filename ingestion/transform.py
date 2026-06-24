"""Stage 2 — stream-parse the day's .json.gz, project needed fields, write slim Parquet.

This is the free-tier linchpin: instead of loading ~6-10 GB of raw JSON/day into BigQuery, we
keep only the columns the marts need (see config.PARQUET_COLUMNS) and write columnar Parquet,
which keeps a 7-day window well under 1 GB.

    python -m ingestion.transform --date 2024-01-01
"""

from __future__ import annotations

import argparse
import gzip
import json
from collections.abc import Iterator
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from .config import KEEP_EVENT_TYPES, load_settings


def extract_language(event: dict) -> str | None:
    """Return repo language, available ONLY on PullRequestEvent.

    Path: payload.pull_request.base.repo.language. All other event types lack it, so this
    returns None for them. This is why agg_language_daily is PR-derived (see CLAUDE.md).
    """
    if event.get("type") != "PullRequestEvent":
        return None
    try:
        return event["payload"]["pull_request"]["base"]["repo"]["language"]
    except (KeyError, TypeError):
        return None


def extract_event(event: dict) -> dict | None:
    """Project a raw GH Archive event down to PARQUET_COLUMNS, or None to drop it.

    Returns None for event types we don't keep. `event_date` is derived from the `created_at`
    timestamp (YYYY-MM-DD) and is the BigQuery partition key.
    """
    event_type = event.get("type")
    if event_type not in KEEP_EVENT_TYPES:
        return None

    created_at = event.get("created_at")  # e.g. "2024-01-01T13:00:00Z"
    event_date = created_at[:10] if created_at else None
    repo = event.get("repo") or {}
    actor = event.get("actor") or {}

    return {
        "id": event.get("id"),
        "event_type": event_type,
        "created_at": created_at,
        "event_date": event_date,
        "actor_login": actor.get("login"),
        "repo_id": repo.get("id"),
        "repo_name": repo.get("name"),
        "language": extract_language(event),
    }


def iter_events(gz_path: Path) -> Iterator[dict]:
    """Yield one parsed JSON event per line from a GH Archive .json.gz (newline-delimited)."""
    with gzip.open(gz_path, "rt", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if line:
                yield json.loads(line)


def transform_day(date: str, day_dir: Path, out_path: Path) -> int:
    """Transform all .gz files in day_dir into a single Parquet at out_path. Returns row count."""
    rows: list[dict] = []
    for gz_path in sorted(day_dir.glob(f"{date}-*.json.gz")):
        for event in iter_events(gz_path):
            projected = extract_event(event)
            if projected is not None:
                rows.append(projected)

    out_path.parent.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pylist(rows, schema=_arrow_schema())
    pq.write_table(table, out_path, compression="snappy")
    return len(rows)


def _arrow_schema() -> pa.Schema:
    """Explicit schema so empty/partial days still produce a correctly-typed Parquet file."""
    return pa.schema(
        [
            ("id", pa.string()),
            ("event_type", pa.string()),
            ("created_at", pa.string()),
            ("event_date", pa.string()),
            ("actor_login", pa.string()),
            ("repo_id", pa.int64()),
            ("repo_name", pa.string()),
            ("language", pa.string()),
        ]
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--date", required=True, help="UTC date, YYYY-MM-DD")
    args = parser.parse_args()

    settings = load_settings()
    day_dir = settings.data_dir / args.date
    out_path = settings.data_dir / args.date / f"{args.date}.parquet"
    count = transform_day(args.date, day_dir, out_path)
    size_mb = out_path.stat().st_size / 1e6
    print(f"Wrote {count} rows -> {out_path} ({size_mb:.1f} MB)")


if __name__ == "__main__":
    main()
