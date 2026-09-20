"""Pure quality checks for the energy marts.

Each check returns a :class:`CheckResult`; the report task writes them to the
``quality_report`` table and fails the job when any check is a ``fail``. Keeping
the rules here (no Spark) makes them unit-testable without a workspace.
"""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass

# Exchange sanity bounds; a price outside these is a unit or scale bug.
PRICE_LOW = -500.0
PRICE_HIGH = 1000.0
# Freshness SLA: the newest delivery hour may be at most this old.
FRESHNESS_SLA_HOURS = 26.0
# Day-ahead hours are published ahead of delivery; more than this is a glitch.
MAX_AHEAD_HOURS = 36.0


@dataclass(frozen=True)
class CheckResult:
    name: str
    status: str  # ok | warn | fail
    detail: str
    metric: float | None = None


def check_freshness(
    latest_delivery: dt.datetime | None,
    now: dt.datetime,
    sla_hours: float = FRESHNESS_SLA_HOURS,
    max_ahead_hours: float = MAX_AHEAD_HOURS,
) -> CheckResult:
    if latest_delivery is None:
        return CheckResult("freshness", "fail", "no delivery hours in silver_prices")
    if latest_delivery.tzinfo is None:
        actual = latest_delivery.replace(tzinfo=dt.timezone.utc)
    else:
        actual = latest_delivery
    age_hours = (now.astimezone(dt.timezone.utc) - actual).total_seconds() / 3600
    if age_hours < 0:
        # Day-ahead prices are published before delivery, so the newest hour is
        # normally in the future; only an implausible jump is a failure.
        detail = (
            f"latest delivery hour is {-age_hours:.1f}h in the future "
            f"(day-ahead publication, max {max_ahead_hours:.0f}h)"
        )
        if -age_hours > max_ahead_hours:
            return CheckResult("freshness", "fail", detail, round(age_hours, 2))
        return CheckResult("freshness", "ok", detail, round(age_hours, 2))
    detail = f"latest delivery hour is {age_hours:.1f}h old (SLA {sla_hours:.0f}h)"
    if age_hours <= sla_hours:
        return CheckResult("freshness", "ok", detail, round(age_hours, 2))
    return CheckResult("freshness", "fail", detail, round(age_hours, 2))


def check_price_bounds(
    prices: list[float],
    low: float = PRICE_LOW,
    high: float = PRICE_HIGH,
) -> CheckResult:
    if not prices:
        return CheckResult("price_bounds", "fail", "no prices to check")
    outliers = [price for price in prices if price < low or price > high]
    detail = (
        f"{len(outliers)} of {len(prices)} prices outside [{low:.0f}, {high:.0f}] EUR/MWh"
        if outliers
        else f"all {len(prices)} prices within [{low:.0f}, {high:.0f}] EUR/MWh"
    )
    if outliers:
        return CheckResult("price_bounds", "fail", detail, float(len(outliers)))
    return CheckResult("price_bounds", "ok", detail, 0.0)


def check_unique_keys(pairs: list[tuple[str, dt.datetime]]) -> CheckResult:
    total = len(pairs)
    unique = len(set(pairs))
    detail = f"{unique} unique of {total} (region, delivery_ts) keys"
    if unique != total:
        return CheckResult("unique_keys", "fail", detail, float(total - unique))
    return CheckResult("unique_keys", "ok", detail, 0.0)


def select_complete_days(
    day_hours: list[tuple[dt.date, int]],
    latest_day: dt.date,
) -> tuple[list[int], int]:
    """Drop the boundary days of a rolling window.

    ``day_hours`` must be sorted by day. The first day of the ingest window is
    partial because the window starts mid-day, and the day of the newest
    published hour is partial because day-ahead publication rolls forward, so
    neither can be judged for completeness. Returns the hours per complete day
    and how many boundary days were ignored.
    """
    if not day_hours:
        return [], 0
    first_day = day_hours[0][0]
    complete = [hours for day, hours in day_hours if day != first_day and day != latest_day]
    return complete, len(day_hours) - len(complete)


def check_daily_hours(hours_per_day: list[int], ignored_days: int = 0) -> CheckResult:
    """A complete day has 24 hours; DST days have 23 or 25.

    Fewer than 23 hours means missing data and fails the run; 23/25 is expected
    twice a year and only noted in the details.
    """
    if not hours_per_day:
        return CheckResult("daily_hours", "ok", "no complete days to check yet")
    dst_days = [hours for hours in hours_per_day if hours != 24]
    shortest = min(hours_per_day)
    detail = f"{len(hours_per_day)} complete days, shortest {shortest}h"
    if ignored_days:
        detail += f", {ignored_days} boundary day(s) ignored"
    if dst_days:
        detail += f", {len(dst_days)} DST day(s)"
    if shortest < 23:
        return CheckResult("daily_hours", "fail", detail, float(shortest))
    return CheckResult("daily_hours", "ok", detail, float(shortest))
