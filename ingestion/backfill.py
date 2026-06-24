"""Run the full ingestion (download -> transform -> upload -> load) over a date window.

    python -m ingestion.backfill --start 2024-01-01 --days 7
"""

from __future__ import annotations

import argparse
from datetime import date, timedelta

from . import download, load_bq, transform, upload_gcs
from .config import load_settings


def daterange(start: str, days: int) -> list[str]:
    """Return `days` consecutive YYYY-MM-DD strings starting at `start`."""
    start_date = date.fromisoformat(start)
    return [(start_date + timedelta(days=i)).isoformat() for i in range(days)]


def ingest_day(date_str: str) -> None:
    """Run all four stages for a single day."""
    settings = load_settings()
    download.download_day(date_str, settings.data_dir)
    day_dir = settings.data_dir / date_str
    transform.transform_day(date_str, day_dir, day_dir / f"{date_str}.parquet")
    upload_gcs.upload_day(settings, date_str)
    rows = load_bq.load_day(settings, date_str)
    print(f"[{date_str}] loaded {rows} rows")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start", required=True, help="window start, YYYY-MM-DD")
    parser.add_argument("--days", type=int, default=7, help="number of days (default 7)")
    args = parser.parse_args()

    for date_str in daterange(args.start, args.days):
        ingest_day(date_str)


if __name__ == "__main__":
    main()
