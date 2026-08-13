"""Unit tests for the language enrichment stage — no network (the CI-safe core).

Mocks requests responses the same way tests/test_transform.py mocks GH Archive payloads, so
ci.yml stays creds-free.
"""

from __future__ import annotations

from unittest.mock import Mock

import pytest
import requests
from ingestion import enrich_language
from ingestion.config import Settings
from ingestion.enrich_language import RateLimited, enrich, fetch_language


def _response(status_code: int, json_body: dict | None = None) -> Mock:
    resp = Mock()
    resp.status_code = status_code
    resp.json.return_value = json_body or {}
    resp.text = ""
    resp.raise_for_status = Mock()
    return resp


def test_fetch_language_returns_language():
    session = Mock()
    session.get.return_value = _response(200, {"language": "Python"})
    assert fetch_language(session, "octo/repo") == "Python"


def test_fetch_language_null_language():
    """A repo can genuinely have no primary language (e.g. docs-only)."""
    session = Mock()
    session.get.return_value = _response(200, {"language": None})
    assert fetch_language(session, "octo/docs") is None


def test_fetch_language_404_caches_as_none():
    """Deleted/private/renamed-away repos are indistinguishable from language-less ones."""
    session = Mock()
    session.get.return_value = _response(404)
    assert fetch_language(session, "octo/gone") is None


def test_fetch_language_rate_limited_raises():
    session = Mock()
    session.get.return_value = _response(403)
    with pytest.raises(RateLimited):
        fetch_language(session, "octo/repo")


def _settings() -> Settings:
    return Settings(
        project_id="proj",
        region="us-central1",
        bucket="bucket",
        dataset_raw="raw",
        dataset_marts="marts",
        raw_table="events",
        max_bytes_billed=1,
        data_dir=None,
        github_token="fake-token",
    )


def test_enrich_saves_partial_results_past_a_transient_error(monkeypatch):
    """Regression test: a single connection blip on one repo used to crash the whole batch and
    lose every row already fetched, because the BQ write only ran after the loop finished
    uninterrupted. It must now skip the bad repo and still write everything else."""
    monkeypatch.setattr(enrich_language, "ensure_cache_table", lambda client, settings: None)
    monkeypatch.setattr(
        enrich_language,
        "repos_needing_language",
        lambda client, settings, lookback, ttl, limit: [
            (1, "a/one"),
            (2, "a/two"),
            (3, "a/three"),
        ],
    )
    monkeypatch.setattr(enrich_language, "bigquery", Mock(Client=Mock(return_value=Mock())))

    responses = iter(["Python", requests.exceptions.ConnectionError("blip"), "Go"])

    def fake_fetch(session, repo_name):
        result = next(responses)
        if isinstance(result, Exception):
            raise result
        return result

    monkeypatch.setattr(enrich_language, "fetch_language", fake_fetch)

    written = {}

    def fake_load_table_from_json(rows, table, job_config):
        written["rows"] = rows
        return Mock(result=lambda: None)

    enrich_language.bigquery.Client.return_value.load_table_from_json = fake_load_table_from_json

    count = enrich(_settings(), lookback_days=7, ttl_days=7, max_calls=10)

    assert count == 2
    assert [r["repo_name"] for r in written["rows"]] == ["a/one", "a/three"]
