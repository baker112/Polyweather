"""Iowa Mesonet ASOS archive — daily max temperature from hourly METAR data.

Endpoint: https://mesonet.agron.iastate.edu/cgi-bin/request/asos.py
Free, no auth required.
"""
from __future__ import annotations

import logging
from datetime import date, datetime, timedelta, timezone

import httpx
import polars as pl
import zoneinfo

from weather_edge.config import StationConfig

_BASE_URL = "https://mesonet.agron.iastate.edu/cgi-bin/request/asos.py"
_TIMEOUT = 60.0
_logger = logging.getLogger(__name__)


async def fetch_observations(
    station: StationConfig,
    start: date,
    end: date,
) -> pl.DataFrame:
    """Fetch hourly METAR temperatures and return daily max in local timezone.

    Columns: station (str), date (date), daily_max_c (f64), source (str), fetched_at (datetime)
    """
    # Iowa Mesonet's day2 is UTC-exclusive (returns data through day2-1T23:59 UTC).
    # Request one extra day so the full local end date is captured after tz conversion.
    api_end = end + timedelta(days=1)

    params = {
        "station": station.icao,
        "data": "tmpf",  # temperature in Fahrenheit
        "year1": str(start.year),
        "month1": str(start.month),
        "day1": str(start.day),
        "year2": str(api_end.year),
        "month2": str(api_end.month),
        "day2": str(api_end.day),
        "tz": "UTC",
        "format": "onlycomma",
        "latlon": "no",
        "missing": "M",
        "trace": "T",
        "direct": "no",
        "report_type": "1,2",
    }

    async with httpx.AsyncClient(timeout=_TIMEOUT) as client:
        resp = await client.get(_BASE_URL, params=params)
        resp.raise_for_status()

    lines = [ln for ln in resp.text.strip().splitlines() if not ln.startswith("#")]
    if len(lines) < 2:
        return pl.DataFrame(schema=_schema())

    # Parse CSV: station, valid(UTC), tmpf
    rows = []
    tz = zoneinfo.ZoneInfo(station.timezone)
    for line in lines[1:]:
        parts = line.split(",")
        if len(parts) < 3:
            continue
        tmpf_str = parts[2].strip()
        if tmpf_str in ("M", "T", ""):
            continue
        try:
            tmpf = float(tmpf_str)
        except ValueError:
            continue
        try:
            valid_utc = datetime.strptime(parts[1].strip(), "%Y-%m-%d %H:%M")
            valid_utc = valid_utc.replace(tzinfo=timezone.utc)
        except ValueError:
            continue
        local_dt = valid_utc.astimezone(tz)
        rows.append((local_dt.date(), (tmpf - 32.0) * 5.0 / 9.0))

    if not rows:
        return pl.DataFrame(schema=_schema())

    df = pl.DataFrame(rows, schema={"date": pl.Date, "tmpc": pl.Float64}, orient="row")
    now = datetime.now(timezone.utc)

    # Polymarket truncates (floor) to the nearest integer, not rounds.
    # Store the floored value so EMOS training labels match Polymarket's resolution.
    resolution_field = getattr(station, "resolution_field", "daily_max_metar_local")
    use_whole_deg = "wholedeg" in resolution_field

    result = (
        df.group_by("date")
        .agg(pl.col("tmpc").max().alias("daily_max_c"))
        .filter(pl.col("date") <= end)  # drop any dates beyond end from the +1-day buffer
        .sort("date")
    )
    if use_whole_deg:
        # Polymarket truncates (floor), not rounds. 15.7°C → 15, not 16.
        result = result.with_columns(
            pl.col("daily_max_c").floor().alias("daily_max_c")
        )

    return (
        result
        .with_columns([
            pl.lit(station.icao).alias("station"),
            pl.lit("iowa_mesonet_asos").alias("source"),
            pl.lit(now).alias("fetched_at"),
        ])
        .select(["station", "date", "daily_max_c", "source", "fetched_at"])
    )


def _schema() -> dict[str, type]:
    return {
        "station": pl.Utf8,
        "date": pl.Date,
        "daily_max_c": pl.Float64,
        "source": pl.Utf8,
        "fetched_at": pl.Datetime,
    }
