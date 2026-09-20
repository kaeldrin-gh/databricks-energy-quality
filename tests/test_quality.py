"""Unit tests for the quality checks and the bronze row mapping."""

from __future__ import annotations

import datetime as dt

from energy_quality.ingest import collect_rows, run_ingest
from energy_quality.quality import (
    check_daily_hours,
    check_freshness,
    check_price_bounds,
    check_unique_keys,
    select_complete_days,
)
from energy_quality.smard import PricePoint

NOW = dt.datetime(2026, 9, 20, 6, 0, tzinfo=dt.timezone.utc)


class FakeClient:
    def fetch_latest(self, weeks: int = 3) -> list[PricePoint]:
        return [PricePoint("DE-LU", dt.datetime(2026, 9, 19, 10, tzinfo=dt.timezone.utc), 42.0)]


def test_collect_rows_shapes_bronze_rows_without_spark():
    rows = collect_rows(FakeClient(), now=NOW)

    assert rows == [
        ("DE-LU", dt.datetime(2026, 9, 19, 10), 42.0, "smard", dt.datetime(2026, 9, 20, 6))
    ]


def test_run_ingest_rejects_empty_fetch(monkeypatch):
    class EmptyClient:
        def fetch_latest(self, weeks: int = 3) -> list[PricePoint]:
            return []

    monkeypatch.setattr("energy_quality.ingest.build_client", lambda: EmptyClient())

    class FakeSpark:
        def sql(self, query: str) -> None:
            raise AssertionError("must not touch Spark when there is nothing to write")

    try:
        run_ingest(FakeSpark(), "workspace", "energy_quality")
    except RuntimeError as error:
        assert "no published points" in str(error)
    else:  # pragma: no cover - defensive
        raise AssertionError("expected RuntimeError")


def test_freshness_ok_and_fail():
    assert check_freshness(NOW - dt.timedelta(hours=2), NOW).status == "ok"
    assert check_freshness(NOW - dt.timedelta(hours=30), NOW).status == "fail"
    assert check_freshness(None, NOW).status == "fail"


def test_freshness_treats_naive_timestamps_as_utc():
    naive = (NOW - dt.timedelta(hours=3)).replace(tzinfo=None)

    result = check_freshness(naive, NOW)

    assert result.status == "ok"
    assert result.metric == 3.0


def test_price_bounds_pass_and_fail():
    assert check_price_bounds([12.0, -30.0, 400.0]).status == "ok"
    assert check_price_bounds([12.0, 120000.0]).status == "fail"


def test_unique_keys_detects_duplicates():
    key = ("DE-LU", dt.datetime(2026, 9, 19, 10))

    assert check_unique_keys([key, ("DE-LU", dt.datetime(2026, 9, 19, 11))]).status == "ok"
    assert check_unique_keys([key, key]).status == "fail"


def test_daily_hours_handles_dst_days():
    assert check_daily_hours([24, 24, 24]).status == "ok"
    # 23 or 25 hours happen on DST transitions and only get noted.
    result = check_daily_hours([24, 23, 25])
    assert result.status == "ok"
    assert "DST" in result.detail
    # A short day means missing data.
    assert check_daily_hours([24, 20]).status == "fail"
    # Nothing to judge yet is not a failure.
    assert check_daily_hours([]).status == "ok"


def test_select_complete_days_ignores_window_boundaries():
    day_hours = [
        (dt.date(2026, 9, 1), 2),  # window starts mid-day
        (dt.date(2026, 9, 2), 24),
        (dt.date(2026, 9, 3), 24),
        (dt.date(2026, 9, 4), 2),  # newest published hour, still rolling
    ]

    complete, ignored = select_complete_days(day_hours, latest_day=dt.date(2026, 9, 4))

    assert complete == [24, 24]
    assert ignored == 2


def test_select_complete_days_with_missing_middle_day_fails_check():
    day_hours = [
        (dt.date(2026, 9, 1), 5),
        (dt.date(2026, 9, 2), 24),
        (dt.date(2026, 9, 3), 10),  # missing hours -> must be caught
        (dt.date(2026, 9, 4), 1),
    ]

    complete, ignored = select_complete_days(day_hours, latest_day=dt.date(2026, 9, 4))
    result = check_daily_hours(complete, ignored)

    assert result.status == "fail"
    assert "shortest 10h" in result.detail


def test_checks_report_metric_values():
    assert check_price_bounds([1.0, 2.0]).metric == 0.0
    assert check_unique_keys([("a", NOW), ("a", NOW)]).metric == 1.0
