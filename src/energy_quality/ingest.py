"""Ingest SMARD day-ahead prices into the bronze Delta table (append-only).

Bronze is a landing table: the pipeline's silver view deduplicates to the newest
revision per delivery hour, which keeps the ingest task simple and replay-safe
(re-running the task only appends another revision, never overwrites history).
"""

from __future__ import annotations

import datetime as dt
import os

from energy_quality.smard import SmardClient

BRONZE_SCHEMA = (
    "region string, delivery_ts timestamp, price_eur_mwh double, "
    "source string, fetched_at timestamp"
)

DEFAULT_BASE_URL = "https://www.smard.de/app/chart_data"
DEFAULT_FILTER = "4169"  # day-ahead price Deutschland/Luxemburg
DEFAULT_REGION = "DE-LU"


def build_client() -> SmardClient:
    return SmardClient(
        base_url=os.environ.get("SMARD_BASE_URL", DEFAULT_BASE_URL),
        filter_id=os.environ.get("SMARD_FILTER", DEFAULT_FILTER),
        region=os.environ.get("SMARD_REGION", DEFAULT_REGION),
    )


def collect_rows(
    client: SmardClient,
    weeks: int = 3,
    now: dt.datetime | None = None,
) -> list[tuple]:
    """Fetch recent weekly chunks and shape them for the bronze table."""
    fetched_at = (now or dt.datetime.now(dt.timezone.utc)).replace(tzinfo=None)
    return [
        (
            point.region,
            point.delivery_ts_utc.replace(tzinfo=None),
            point.price_eur_mwh,
            "smard",
            fetched_at,
        )
        for point in client.fetch_latest(weeks=weeks)
    ]


def run_ingest(spark, catalog: str, schema: str, weeks: int = 3) -> int:
    """Append one batch of published prices to ``{catalog}.{schema}.bronze_prices``."""
    rows = collect_rows(build_client(), weeks=weeks)
    if not rows:
        raise RuntimeError("SMARD returned no published points; nothing to ingest")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
    frame = spark.createDataFrame(rows, BRONZE_SCHEMA)
    frame.write.format("delta").mode("append").saveAsTable(f"{catalog}.{schema}.bronze_prices")
    return len(rows)
