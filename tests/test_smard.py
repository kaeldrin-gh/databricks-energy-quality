"""Unit tests for the SMARD client (no network, no workspace)."""

from __future__ import annotations

import datetime as dt

import pytest

from energy_quality.smard import PricePoint, SmardClient, SmardError, parse_chunk

INDEX_PAYLOAD = {"timestamps": [1700000000000, 1700604800000]}

CHUNK_PAYLOAD = {
    "meta_data": {"version": 1},
    "series": [
        [1788732000000, 155.5],
        [1788735600000, -12.4],
        [1788739200000, None],  # unpublished future hour
        ["not-a-list", 1.0],  # malformed row -> skipped
        [1788746400000, "bad"],  # unparsable price -> skipped
    ],
}


def test_urls_match_documented_endpoint_shape():
    client = SmardClient("https://www.smard.de/app/chart_data", "4169", "DE-LU")
    assert client.index_url() == "https://www.smard.de/app/chart_data/4169/DE-LU/index_hour.json"
    assert (
        client.chunk_url(1788732000000)
        == "https://www.smard.de/app/chart_data/4169/DE-LU/4169_DE-LU_hour_1788732000000.json"
    )


def test_parse_chunk_skips_nulls_and_malformed_rows_and_sorts():
    points = parse_chunk(CHUNK_PAYLOAD, region="DE-LU")

    assert [point.price_eur_mwh for point in points] == [155.5, -12.4]
    assert all(point.delivery_ts_utc.tzinfo == dt.timezone.utc for point in points)
    assert points == sorted(points, key=lambda point: point.delivery_ts_utc)


def test_parse_chunk_rejects_missing_series():
    with pytest.raises(SmardError):
        parse_chunk({"meta_data": {}}, region="DE-LU")


def test_fetch_latest_combines_index_and_chunks():
    client = SmardClient("https://example.invalid", "4169", "DE-LU")
    calls: list[str] = []

    def fake_get_json(url: str) -> dict:
        calls.append(url)
        if url.endswith("index_hour.json"):
            return INDEX_PAYLOAD
        week_start = int(url.rsplit("_", 1)[-1].split(".")[0])
        return {"meta_data": {}, "series": [[week_start, 42.0]]}

    client._get_json = fake_get_json  # type: ignore[method-assign]

    points = client.fetch_latest(weeks=2)

    assert len(points) == 2
    assert calls[0].endswith("index_hour.json")
    assert calls[1].rsplit("/", 1)[-1] == "4169_DE-LU_hour_1700000000000.json"


def test_fetch_latest_errors_on_empty_index():
    client = SmardClient("https://example.invalid", "4169", "DE-LU")
    client._get_json = lambda url: {"timestamps": []}  # type: ignore[method-assign]

    with pytest.raises(SmardError):
        client.fetch_latest(weeks=1)


def test_price_point_keeps_utc_delivery_time():
    point = PricePoint("DE-LU", dt.datetime(2026, 9, 19, 8, tzinfo=dt.timezone.utc), 42.0)

    assert point.delivery_ts_utc.hour == 8
