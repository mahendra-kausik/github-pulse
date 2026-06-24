"""Stage 4 — load the day's Parquet from GCS into the partitioned/clustered BQ raw table.

The table is partitioned by `event_date` and clustered by `event_type`. We use WRITE_TRUNCATE
scoped to the day's partition (via the $YYYYMMDD partition decorator) so re-running a day is
idempotent and never duplicates rows.

    python -m ingestion.load_bq --date 2024-01-01
"""

from __future__ import annotations

import argparse

from google.cloud import bigquery

from .config import Settings, load_settings


def ensure_table(client: bigquery.Client, settings: Settings) -> None:
    """Create the raw table if absent: partitioned by event_date, clustered by event_type.

    require_partition_filter=True makes any unfiltered query error out — a free-tier guardrail.
    """
    schema = [
        bigquery.SchemaField("id", "STRING"),
        bigquery.SchemaField("event_type", "STRING"),
        bigquery.SchemaField("created_at", "TIMESTAMP"),
        bigquery.SchemaField("event_date", "DATE"),
        bigquery.SchemaField("actor_login", "STRING"),
        bigquery.SchemaField("repo_id", "INTEGER"),
        bigquery.SchemaField("repo_name", "STRING"),
        bigquery.SchemaField("language", "STRING"),
    ]
    table = bigquery.Table(settings.raw_table_fqn, schema=schema)
    table.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY,
        field="event_date",
        require_partition_filter=True,
    )
    table.clustering_fields = ["event_type"]
    client.create_table(table, exists_ok=True)


def load_day(settings: Settings, date: str) -> int:
    """Load gs://.../parquet/{date}/{date}.parquet into the day's partition. Returns rows loaded."""
    client = bigquery.Client(project=settings.project_id)
    ensure_table(client, settings)

    partition = date.replace("-", "")  # YYYYMMDD decorator
    target = f"{settings.raw_table_fqn}${partition}"
    uri = f"gs://{settings.bucket}/parquet/{date}/{date}.parquet"

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )
    job = client.load_table_from_uri(uri, target, job_config=job_config)
    job.result()  # wait
    return job.output_rows or 0


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--date", required=True, help="UTC date, YYYY-MM-DD")
    args = parser.parse_args()

    settings = load_settings()
    rows = load_day(settings, args.date)
    print(f"Loaded {rows} rows into {settings.raw_table_fqn} partition {args.date}")


if __name__ == "__main__":
    main()
