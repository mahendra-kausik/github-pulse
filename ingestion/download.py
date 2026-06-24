"""Stage 1 — download the 24 hourly GH Archive .json.gz files for a given date.

Files are saved to {DATA_DIR}/{date}/{date}-{hour}.json.gz. Downloads are skipped if the file
already exists, so re-running a day is cheap and idempotent.

    python -m ingestion.download --date 2024-01-01
"""

from __future__ import annotations

import argparse
from pathlib import Path

import requests

from .config import GHARCHIVE_BASE_URL, HOURS_PER_DAY, load_settings


def hourly_url(date: str, hour: int) -> str:
    """GH Archive URL for one hour, e.g. https://data.gharchive.org/2024-01-01-13.json.gz."""
    return f"{GHARCHIVE_BASE_URL}/{date}-{hour}.json.gz"


def download_hour(date: str, hour: int, dest_dir: Path, timeout: int = 60) -> Path:
    """Download a single hour's file to dest_dir, skipping if already present. Returns the path."""
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest = dest_dir / f"{date}-{hour}.json.gz"
    if dest.exists() and dest.stat().st_size > 0:
        return dest
    with requests.get(hourly_url(date, hour), stream=True, timeout=timeout) as resp:
        resp.raise_for_status()
        with dest.open("wb") as fh:
            for chunk in resp.iter_content(chunk_size=1 << 20):
                fh.write(chunk)
    return dest


def download_day(date: str, data_dir: Path) -> list[Path]:
    """Download all 24 hourly files for `date`. Returns the local paths."""
    day_dir = data_dir / date
    return [download_hour(date, hour, day_dir) for hour in range(HOURS_PER_DAY)]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--date", required=True, help="UTC date, YYYY-MM-DD")
    args = parser.parse_args()

    settings = load_settings()
    paths = download_day(args.date, settings.data_dir)
    print(f"Downloaded {len(paths)} files for {args.date} -> {settings.data_dir / args.date}")


if __name__ == "__main__":
    main()
