"""ICON ensemble ingestion via Open-Meteo (#3).

Open-Meteo's free ensemble endpoint serves the DWD ICON family with no API key:
  - icon_seamless: 40-member ICON ensemble (D2 → EU → Global by lead range)
  - Hourly 2m temperature returned per member; we reduce to local-day max
    so the schema matches ECMWF/GEFS (one row per member-day).

Notes:
  - Open-Meteo selects the freshest available run server-side; we cannot
    request a specific init cycle. We persist the caller's init_dt in
    `init_datetime` so cache keying stays consistent with the other models;
    for the purposes of BMA the per-day forecast value is what matters.
  - past_days lets the same fetcher backfill a few days during onboarding;
    forecast_days=2 covers our D+1 lock target with a small buffer.
"""
from __future__ import annotations

import logging
from datetime import date, datetime, timedelta, timezone
from typing import Any

import httpx
import polars as pl
import zoneinfo

from weather_edge.config import StationConfig
from weather_edge.exceptions import IngestError

_logger = logging.getLogger(__name__)
_MODEL = "icon"
_ENDPOINT = "https://ensemble-api.open-meteo.com/v1/ensemble"
_OPEN_METEO_MODEL = "icon_seamless"
_HTTP_TIMEOUT = 30.0


def ingest_forecasts(init_dt: datetime, station: StationConfig) -> pl.DataFrame:
    """Fetch ICON ensemble 2m temperature and return a forecast DataFrame.

    Columns: model, member_id, init_datetime, valid_date, station, daily_max_c, lead_hours
    """
    tz = zoneinfo.ZoneInfo(station.timezone)
    params = {
        "latitude": station.lat,
        "longitude": station.lon,
        "hourly": "temperature_2m",
        "models": _OPEN_METEO_MODEL,
        "timezone": "UTC",
        "past_days": 1,
        "forecast_days": 3,
    }

    try:
        resp = httpx.get(_ENDPOINT, params=params, timeout=_HTTP_TIMEOUT)
        resp.raise_for_status()
    except Exception as exc:
        raise IngestError(f"ICON Open-Meteo HTTP failed: {exc}") from exc

    data: dict[str, Any] = resp.json()
    hourly = data.get("hourly")
    if not hourly or "time" not in hourly:
        raise IngestError(f"ICON: malformed response (keys={list(data.keys())})")

    times: list[str] = hourly["time"]
    if not times:
        raise IngestError("ICON: empty hourly time series")

    # Series keys: "temperature_2m" (control / main) plus
    # "temperature_2m_member01", ..., "temperature_2m_memberNN"
    series: dict[int, list[float | None]] = {}
    for key, vals in hourly.items():
        if key == "time" or not key.startswith("temperature_2m"):
            continue
        if key == "temperature_2m":
            member_id = 0
        else:
            suffix = key.removeprefix("temperature_2m_member")
            try:
                member_id = int(suffix)
            except ValueError:
                continue
        series[member_id] = vals

    if not series:
        raise IngestError("ICON: no temperature_2m series in response")

    # Parse hourly UTC timestamps into datetimes for tz-conversion to local day
    # Open-Meteo timestamps look like "2026-05-06T12:00" (no Z) — we requested timezone=UTC
    parsed_times: list[datetime] = [
        datetime.fromisoformat(t).replace(tzinfo=timezone.utc) for t in times
    ]

    # Member → local_day → max temp
    rows: list[dict[str, Any]] = []
    for member_id, values in series.items():
        day_max: dict[date, float] = {}
        for ts_utc, v in zip(parsed_times, values):
            if v is None:
                continue
            local_day = ts_utc.astimezone(tz).date()
            prev = day_max.get(local_day)
            if prev is None or v > prev:
                day_max[local_day] = float(v)

        for vd, tmax in day_max.items():
            rows.append({
                "model": _MODEL,
                "member_id": member_id,
                "init_datetime": init_dt.replace(tzinfo=timezone.utc),
                "valid_date": vd,
                "station": station.icao,
                "daily_max_c": tmax,
                "lead_hours": _lead_hours(init_dt, vd, tz),
            })

    if not rows:
        raise IngestError("ICON: no usable temperature samples after parsing")

    return pl.DataFrame(rows)


def _lead_hours(init_dt: datetime, valid_date: date, tz: zoneinfo.ZoneInfo) -> int:
    noon_local = datetime(valid_date.year, valid_date.month, valid_date.day, 12, tzinfo=tz)
    delta = noon_local.astimezone(timezone.utc) - init_dt.replace(tzinfo=timezone.utc)
    return int(round(delta.total_seconds() / 3600 / 24) * 24)
