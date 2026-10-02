"""Land SMARD day-ahead prices as JSON-lines files in a Unity Catalog Volume.

Each run writes one file, ``smard_<fetched_at>.jsonl``, into
``/Volumes/{catalog}/{schema}/landing/smard/``. The Lakeflow pipeline picks new
files up with Auto Loader into the ``bronze_prices`` streaming table, and AUTO
CDC keeps the newest revision per delivery hour in silver. Landing files are
never rewritten, so re-running the task only adds another revision.
"""

from __future__ import annotations

import datetime as dt
import json
import os

from energy_quality.smard import SmardClient

LANDING_VOLUME = "landing"
LANDING_DIR = "smard"

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
    """Fetch recent weekly chunks as (region, delivery_ts, price, source, fetched_at) in UTC."""
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


def _utc(value: dt.datetime) -> str:
    return value.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def to_json_lines(rows: list[tuple]) -> str:
    """One JSON object per line, timestamps as ISO 8601 UTC, as Auto Loader reads them."""
    return "".join(
        json.dumps(
            {
                "region": region,
                "delivery_ts": _utc(delivery_ts),
                "price_eur_mwh": price,
                "source": source,
                "fetched_at": _utc(fetched_at),
            }
        )
        + "\n"
        for region, delivery_ts, price, source, fetched_at in rows
    )


def landing_path(catalog: str, schema: str) -> str:
    return f"/Volumes/{catalog}/{schema}/{LANDING_VOLUME}/{LANDING_DIR}"


def file_name(fetched_at: dt.datetime) -> str:
    return f"smard_{fetched_at:%Y%m%dT%H%M%S}Z.jsonl"


def run_ingest(catalog: str, schema: str, weeks: int = 3) -> str:
    """Write one batch of published prices to the landing volume; returns the file path."""
    rows = collect_rows(build_client(), weeks=weeks)
    if not rows:
        raise RuntimeError("SMARD returned no published points; nothing to ingest")
    directory = landing_path(catalog, schema)
    os.makedirs(directory, exist_ok=True)
    final = f"{directory}/{file_name(rows[0][4])}"
    # Write under a hidden name, then rename: Auto Loader skips names starting
    # with "_", so it never reads a half-written file.
    partial = f"{directory}/_{file_name(rows[0][4])}.partial"
    with open(partial, "w", encoding="utf-8") as handle:
        handle.write(to_json_lines(rows))
    os.replace(partial, final)
    return final
