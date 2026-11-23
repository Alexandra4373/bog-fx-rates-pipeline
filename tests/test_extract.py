import json

import httpx
import pytest

from bog_fx_rates_pipeline import extract


def test_bronze_keeps_the_payload_and_where_it_came_from(tmp_path):
    payload = {"rows": [{"rate_date": "2026-01-01", "pair": "USDGHS", "mid": 11.5}]}

    path = extract.save_bronze(payload, str(tmp_path))

    record = json.loads(path.read_text(encoding="utf-8"))
    assert record["payload"] == payload
    assert record["_source"] == extract.SOURCE_URL
    assert "_ingested_at" in record
    assert "ingest_date=" in str(path)


def fake_answer(status_code):
    """Replace httpx.get with a function that returns a fixed status, without the internet."""

    def fake_get(url, timeout):
        return httpx.Response(status_code, json={}, request=httpx.Request("GET", url))

    return fake_get


def test_a_server_error_is_temporary(monkeypatch):
    monkeypatch.setattr(httpx, "get", fake_answer(503))
    with pytest.raises(extract.TemporarySourceError):
        extract.fetch_rates()


def test_rate_limiting_is_temporary(monkeypatch):
    monkeypatch.setattr(httpx, "get", fake_answer(429))
    with pytest.raises(extract.TemporarySourceError):
        extract.fetch_rates()


def test_a_missing_page_is_permanent(monkeypatch):
    monkeypatch.setattr(httpx, "get", fake_answer(404))
    with pytest.raises(httpx.HTTPStatusError):
        extract.fetch_rates()


def test_network_trouble_is_temporary(monkeypatch):
    def broken_get(url, timeout):
        raise httpx.ConnectError("getaddrinfo failed")

    monkeypatch.setattr(httpx, "get", broken_get)
    with pytest.raises(extract.TemporarySourceError):
        extract.fetch_rates()
