"""Unit tests for the language enrichment stage — no network (the CI-safe core).

Mocks requests responses the same way tests/test_transform.py mocks GH Archive payloads, so
ci.yml stays creds-free.
"""

from __future__ import annotations

from unittest.mock import Mock

import pytest
from ingestion.enrich_language import RateLimited, fetch_language


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
