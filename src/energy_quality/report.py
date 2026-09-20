"""Run the quality checks against the marts and record them in quality_report.

The job's final task calls this: every check is appended to
``{catalog}.{schema}.quality_report`` (Delta, so the history is queryable), and
the task fails when any check is a ``fail`` so the workspace alert fires.
"""

from __future__ import annotations

import datetime as dt

from energy_quality.quality import (
    CheckResult,
    check_daily_hours,
    check_freshness,
    check_price_bounds,
    check_unique_keys,
)

REPORT_SCHEMA = (
    "check_name string, status string, detail string, metric double, checked_at timestamp"
)


def run_quality_report(
    spark,
    catalog: str,
    schema: str,
    now: dt.datetime | None = None,
) -> list[CheckResult]:
    now = now or dt.datetime.now(dt.timezone.utc)
    fq = f"{catalog}.{schema}"

    latest = spark.sql(f"select max(delivery_ts) as latest from {fq}.silver_prices").collect()[0][
        "latest"
    ]
    price_rows = spark.sql(
        f"select price_eur_mwh from {fq}.silver_prices "
        "where delivery_ts >= current_timestamp() - interval 30 days"
    ).collect()
    key_rows = spark.sql(f"select region, delivery_ts from {fq}.silver_prices").collect()
    day_rows = spark.sql(
        f"select hours from {fq}.gold_daily where day >= current_date() - interval 30 days"
    ).collect()

    results = [
        check_freshness(latest, now),
        check_price_bounds([row["price_eur_mwh"] for row in price_rows]),
        check_unique_keys([(row["region"], row["delivery_ts"]) for row in key_rows]),
        check_daily_hours([row["hours"] for row in day_rows]),
    ]

    checked_at = now.replace(tzinfo=None)
    report_rows = [(r.name, r.status, r.detail, r.metric, checked_at) for r in results]
    (
        spark.createDataFrame(report_rows, REPORT_SCHEMA)
        .write.format("delta")
        .mode("append")
        .saveAsTable(f"{fq}.quality_report")
    )

    failures = [r for r in results if r.status == "fail"]
    if failures:
        raise RuntimeError(
            "quality check failed: " + "; ".join(f"{r.name}: {r.detail}" for r in failures)
        )
    return results
