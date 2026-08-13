"""Unit tests for the transform stage — no GCP/network needed (the CI-safe core).

Focus areas:
  - field projection: only KEEP_EVENT_TYPES survive, with the right columns + derived event_date
  - PR language extraction: the one tricky nested-path correctness fix
  - Parquet schema stays locked to the BigQuery raw table schema (ingestion.load_bq.ensure_table)
  - transform_day writes typed Parquet and drops rows outside the target date
"""

from __future__ import annotations

import gzip
import json

import pyarrow.parquet as pq
from ingestion.config import PARQUET_COLUMNS
from ingestion.load_bq import RAW_TABLE_SCHEMA
from ingestion.transform import _arrow_schema, extract_event, extract_language, transform_day

_BQ_TYPE_TO_ARROW = {
    "STRING": "string",
    "TIMESTAMP": "timestamp[us, tz=UTC]",
    "DATE": "date32[day]",
    "INTEGER": "int64",
}


def _push_event() -> dict:
    return {
        "id": "1",
        "type": "PushEvent",
        "created_at": "2024-01-01T13:00:00Z",
        "actor": {"login": "octocat"},
        "repo": {"id": 100, "name": "octo/repo"},
        # note: no language anywhere on a PushEvent
    }


def _pr_event(language: str | None = "Python") -> dict:
    return {
        "id": "2",
        "type": "PullRequestEvent",
        "created_at": "2024-01-01T14:30:00Z",
        "actor": {"login": "hubot"},
        "repo": {"id": 200, "name": "hub/bot"},
        "payload": {"pull_request": {"base": {"repo": {"language": language}}}},
    }


def test_pr_language_extraction():
    """Language comes only from PullRequestEvent.payload.pull_request.base.repo.language."""
    assert extract_language(_pr_event("Rust")) == "Rust"
    assert extract_language(_pr_event(None)) is None       # PR with null language
    assert extract_language(_push_event()) is None         # non-PR events never have language


def test_pr_language_missing_path_is_safe():
    """A malformed/partial PR payload must not raise — just yields None."""
    broken = {"type": "PullRequestEvent", "payload": {"pull_request": {}}}
    assert extract_language(broken) is None


def test_extract_event_projects_expected_columns():
    row = extract_event(_push_event())
    assert row is not None
    assert set(row.keys()) == set(PARQUET_COLUMNS)
    assert row["event_type"] == "PushEvent"
    assert row["event_date"] == "2024-01-01"   # derived from created_at
    assert row["repo_id"] == 100
    assert row["language"] is None


def test_extract_event_drops_unkept_types():
    unkept = {"id": "9", "type": "MemberEvent", "created_at": "2024-01-01T00:00:00Z"}
    assert extract_event(unkept) is None


def test_extract_event_pr_keeps_language():
    row = extract_event(_pr_event("Go"))
    assert row is not None
    assert row["language"] == "Go"
    assert row["event_type"] == "PullRequestEvent"


def test_parquet_schema_matches_bq_schema():
    """The Parquet schema transform.py writes must match the BQ raw table field-for-field.

    Regression test: transform.py once wrote created_at/event_date as strings while the BQ
    table declared TIMESTAMP/DATE, which BigQuery rejects on load (Parquet loads don't coerce
    STRING -> TIMESTAMP/DATE the way CSV/JSON loads do).
    """
    arrow_fields = {f.name: str(f.type) for f in _arrow_schema()}
    bq_fields = {f.name: _BQ_TYPE_TO_ARROW[f.field_type] for f in RAW_TABLE_SCHEMA}
    assert arrow_fields == bq_fields


def test_transform_day_writes_typed_parquet(tmp_path):
    day_dir = tmp_path / "2024-01-01"
    day_dir.mkdir()
    with gzip.open(day_dir / "2024-01-01-13.json.gz", "wt") as fh:
        fh.write(json.dumps(_push_event()) + "\n")
        fh.write(json.dumps(_pr_event("Go")) + "\n")

    out_path = day_dir / "2024-01-01.parquet"
    count = transform_day("2024-01-01", day_dir, out_path)

    assert count == 2
    written = pq.read_table(out_path)
    assert written.schema.equals(_arrow_schema())
    assert set(written.column("repo_id").to_pylist()) == {100, 200}


def test_transform_day_drops_other_dates(tmp_path):
    """A GH Archive hour file's events don't all share one UTC date — rows outside `date` must
    not survive into that day's Parquet file, or the $YYYYMMDD partitioned BQ load rejects the
    whole file."""
    day_dir = tmp_path / "2024-01-01"
    day_dir.mkdir()
    boundary_event = _push_event() | {"id": "3", "created_at": "2023-12-31T23:59:59Z"}
    with gzip.open(day_dir / "2024-01-01-0.json.gz", "wt") as fh:
        fh.write(json.dumps(_push_event()) + "\n")
        fh.write(json.dumps(boundary_event) + "\n")

    out_path = day_dir / "2024-01-01.parquet"
    count = transform_day("2024-01-01", day_dir, out_path)

    assert count == 1
    assert pq.read_table(out_path).column("id").to_pylist() == ["1"]
