"""Stage 3 — upload the day's slim Parquet (and optionally raw .gz) to the GCS data lake.

Lake layout:
    gs://{bucket}/raw/{date}/{date}-{hour}.json.gz     (optional, audit trail)
    gs://{bucket}/parquet/{date}/{date}.parquet        (what load_bq reads)

    python -m ingestion.upload_gcs --date 2024-01-01
"""

from __future__ import annotations

import argparse
from pathlib import Path

from google.cloud import storage

from .config import Settings, load_settings


def upload_file(client: storage.Client, bucket: str, local_path: Path, blob_name: str) -> str:
    """Upload one file and return its gs:// URI."""
    blob = client.bucket(bucket).blob(blob_name)
    blob.upload_from_filename(str(local_path))
    return f"gs://{bucket}/{blob_name}"


def upload_day(settings: Settings, date: str, include_raw: bool = False) -> str:
    """Upload the day's parquet (and raw gz if requested). Returns the parquet gs:// URI."""
    client = storage.Client(project=settings.project_id)
    day_dir = settings.data_dir / date

    if include_raw:
        for gz_path in sorted(day_dir.glob(f"{date}-*.json.gz")):
            upload_file(client, settings.bucket, gz_path, f"raw/{date}/{gz_path.name}")

    parquet_path = day_dir / f"{date}.parquet"
    return upload_file(
        client, settings.bucket, parquet_path, f"parquet/{date}/{date}.parquet"
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--date", required=True, help="UTC date, YYYY-MM-DD")
    parser.add_argument("--include-raw", action="store_true", help="also upload raw .gz files")
    args = parser.parse_args()

    settings = load_settings()
    uri = upload_day(settings, args.date, include_raw=args.include_raw)
    print(f"Uploaded parquet -> {uri}")


if __name__ == "__main__":
    main()
