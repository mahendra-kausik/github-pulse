"""Unit tests for the transform stage — no GCP/network needed (the CI-safe core).

Focus areas:
  - field projection: only KEEP_EVENT_TYPES survive, with the right columns + derived event_date
  - PR language extraction: the one tricky nested-path correctness fix
"""

from __future__ import annotations

from ingestion.config import PARQUET_COLUMNS
from ingestion.transform import extract_event, extract_language


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
