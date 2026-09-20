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
) -> CheckResult:
    if latest_delivery is None:
        return CheckResult("freshness", "fail", "no delivery hours in silver_prices")
    if latest_delivery.tzinfo is None:
        actual = latest_delivery.replace(tzinfo=dt.timezone.utc)
    else:
        actual = latest_delivery
    age_hours = (now.astimezone(dt.timezone.utc) - actual).total_seconds() / 3600
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


def check_daily_hours(hours_per_day: list[int]) -> CheckResult:
    """A complete day has 24 hours; DST days have 23 or 25.

    Fewer than 23 hours means missing data and fails the run; 23/25 is expected
    twice a year and only noted in the details.
    """
    if not hours_per_day:
        return CheckResult("daily_hours", "fail", "no days in gold_daily")
    dst_days = [hours for hours in hours_per_day if hours != 24]
    shortest = min(hours_per_day)
    detail = f"{len(hours_per_day)} days, shortest {shortest}h" + (
        f", {len(dst_days)} DST day(s)" if dst_days else ""
    )
    if shortest < 23:
        return CheckResult("daily_hours", "fail", detail, float(shortest))
    return CheckResult("daily_hours", "ok", detail, float(shortest))
