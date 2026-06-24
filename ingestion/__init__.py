"""GitHub Pulse ingestion package.

Stages, each runnable as `python -m ingestion.<stage> --date YYYY-MM-DD`:
    download   -> fetch 24 hourly GH Archive .json.gz for a date
    transform  -> stream-parse, project needed fields, write slim Parquet
    upload_gcs -> push raw .gz + parquet to the GCS lake
    load_bq    -> load parquet into the partitioned/clustered BQ raw table
"""

__all__ = ["config", "download", "transform", "upload_gcs", "load_bq", "backfill"]
