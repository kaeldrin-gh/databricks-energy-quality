"""Minimal client for SMARD.de's public chart-data endpoints (no API key).

Endpoint shape (verified against the live service, September 2026):

    index : {base}/{filter}/{region}/index_hour.json
            -> {"timestamps": [week_start_ms, ...]}
    chunk : {base}/{filter}/{region}/{filter}_{region}_hour_{week_start_ms}.json
            -> {"meta_data": {...}, "series": [[delivery_ts_ms, price | null], ...]}

Future hours of the current week are ``null`` until the day-ahead auction
clears; those rows are skipped, not treated as errors.
"""

from __future__ import annotations

import datetime as dt
import logging
import time
from dataclasses import dataclass

import requests

log = logging.getLogger(__name__)

DEFAULT_TIMEOUT = 30


class SmardError(RuntimeError):
    """Raised when SMARD returns an unexpected payload or stays unreachable."""


@dataclass(frozen=True)
class PricePoint:
    region: str
    delivery_ts_utc: dt.datetime
    price_eur_mwh: float


class SmardClient:
    def __init__(
        self,
        base_url: str,
        filter_id: str,
        region: str,
        session: requests.Session | None = None,
        timeout: int = DEFAULT_TIMEOUT,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.filter_id = str(filter_id)
        self.region = region
        self.timeout = timeout
        self._session = session or requests.Session()

    def index_url(self) -> str:
        return f"{self.base_url}/{self.filter_id}/{self.region}/index_hour.json"

    def chunk_url(self, week_start_ms: int) -> str:
        return (
            f"{self.base_url}/{self.filter_id}/{self.region}/"
            f"{self.filter_id}_{self.region}_hour_{week_start_ms}.json"
        )

    def fetch_index(self) -> list[int]:
        payload = self._get_json(self.index_url())
        try:
            return sorted(int(ts) for ts in payload["timestamps"])
        except (KeyError, TypeError, ValueError) as exc:
            raise SmardError(f"unexpected index payload from {self.index_url()}") from exc

    def fetch_chunk(self, week_start_ms: int) -> list[PricePoint]:
        return parse_chunk(self._get_json(self.chunk_url(week_start_ms)), region=self.region)

    def fetch_latest(self, weeks: int = 3) -> list[PricePoint]:
        index = self.fetch_index()
        if not index:
            raise SmardError("SMARD index is empty")
        points: list[PricePoint] = []
        for week_start in index[-weeks:]:
            points.extend(self.fetch_chunk(week_start))
        return points

    def _get_json(self, url: str) -> dict:
        last_error: Exception | None = None
        for attempt in range(1, 5):
            try:
                response = self._session.get(url, timeout=self.timeout)
                response.raise_for_status()
                return response.json()
            except (requests.RequestException, ValueError) as exc:
                last_error = exc
                wait = 2**attempt
                log.warning("GET %s failed (attempt %d/4): %s", url, attempt, exc)
                time.sleep(wait)
        raise SmardError(f"giving up on {url}: {last_error}")


def parse_chunk(payload: dict, region: str) -> list[PricePoint]:
    """Parse a weekly chunk, skipping unpublished (null) hours."""
    try:
        series = payload["series"]
    except (KeyError, TypeError) as exc:
        raise SmardError("chunk payload has no 'series'") from exc

    points: list[PricePoint] = []
    for row in series:
        if not isinstance(row, list) or len(row) != 2:
            continue
        ts_ms, price = row
        if price is None:
            continue
        try:
            delivery_ts = dt.datetime.fromtimestamp(int(ts_ms) / 1000, tz=dt.timezone.utc)
            points.append(
                PricePoint(region=region, delivery_ts_utc=delivery_ts, price_eur_mwh=float(price))
            )
        except (TypeError, ValueError, OSError):
            log.debug("skipping malformed row: %r", row)

    points.sort(key=lambda point: point.delivery_ts_utc)
    return points
