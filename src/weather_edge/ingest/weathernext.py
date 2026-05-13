"""WeatherNext 2 ingestion via BigQuery (#7).

Google DeepMind's diffusion-based ensemble model. Targets the public
`weathernext_2_0_0` table linked via Analytics Hub into your own dataset:
  - ~50-member ensemble
  - 0.25° grid (~28 km)
  - 6-hour init cadence (00/06/12/18 UTC)
  - 6-hour lead steps to +15 days
  - Variable name: `2m_temperature` (Kelvin)

Real-time (init_time within last 48h)  → GDM Real-Time Experimental Data ToS.
Historic (init_time older than 48h)    → CC BY 4.0.

Host env vars required:
  WEATHERNEXT_PROJECT             — GCP project ID
  WEATHERNEXT_DATASET             — BQ dataset where the Analytics Hub share is linked
  WEATHERNEXT_TABLE               — table name (default "weathernext_2_0_0")
  GOOGLE_APPLICATION_CREDENTIALS  — path to service-account JSON

Daily-max caveat: WN2's 6h temporal resolution under-samples the true afternoon
peak by 1-2°C versus the hourly ICON / 3-hourly GEFS feeds. The EMOS bias term
absorbs this systematic offset during fitting (per-model EMOS is therefore
required — pooled EMOS would mix the offsets together and degrade calibration).
"""
from __future__ import annotations

import logging
import os
from datetime import date, datetime, timedelta, timezone
from typing import Any

import polars as pl
import zoneinfo

from weather_edge.config import StationConfig
from weather_edge.exceptions import IngestError

_logger = logging.getLogger(__name__)
_MODEL = "weathernext"

# Column-name assumptions. If the BQ schema differs, override these once after
# inspecting the table (see scripts/weathernext_schema.py) — no other code
# changes needed.
_COL_INIT_TIME = "init_time"
_COL_VALID_TIME = "valid_time"
_COL_MEMBER = "ensemble_member"
_COL_GEOG = "geography"
_COL_T2M = "`2m_temperature`"  # backticked because the identifier starts with a digit

# Bounding box (degrees) around station lat/lon: pull all grid cells inside this
# half-width. 0.3° at 50°N covers roughly the nearest 4 cells of a 0.25° grid,
# which is enough for a simple inverse-distance interpolation.
_BBOX_HALFWIDTH_DEG = 0.3


def _bq_client() -> Any:
    try:
        from google.cloud import bigquery  # type: ignore[import-untyped]
    except ImportError as exc:
        raise IngestError(
            "google-cloud-bigquery not installed — add to pyproject.toml and reinstall"
        ) from exc
    project = os.getenv("WEATHERNEXT_PROJECT")
    if not project:
        raise IngestError("WEATHERNEXT_PROJECT env var not set")
    return bigquery.Client(project=project)


def _fq_table() -> str:
    project = os.getenv("WEATHERNEXT_PROJECT")
    dataset = os.getenv("WEATHERNEXT_DATASET")
    table = os.getenv("WEATHERNEXT_TABLE", "weathernext_2_0_0")
    if not (project and dataset):
        raise IngestError(
            "WEATHERNEXT_PROJECT and WEATHERNEXT_DATASET env vars must both be set"
        )
    return f"`{project}.{dataset}.{table}`"


def ingest_forecasts(init_dt: datetime, station: StationConfig) -> pl.DataFrame:
    """Fetch 50-member WN2 2m temperature for a single init and return per-member daily max.

    Columns: model, member_id, init_datetime, valid_date, station, daily_max_c, lead_hours
    """
    init_utc = init_dt.replace(tzinfo=timezone.utc)
    # We need ~4 days of leads to cover D-1..D+3 daily max under any UTC offset.
    end_valid = init_utc + timedelta(hours=96)
    return _query_and_reduce(
        init_start=init_utc,
        init_end=init_utc + timedelta(seconds=1),  # exact-init match
        valid_end=end_valid,
        station=station,
    )


def ingest_historic(
    start_date: date,
    end_date: date,
    station: StationConfig,
    init_hours: tuple[int, ...] = (0, 12),
) -> pl.DataFrame:
    """Fetch WN2 forecasts for every init at `init_hours` UTC in [start_date, end_date].

    Reduces to per-member daily max in station-local timezone. Schema matches
    ingest_forecasts() so the historic rows can be appended via the existing
    parquet writer without further transformation.

    `init_hours` defaults to (0, 12) which covers every station's production
    lock cycle: Asian stations use the 00z run, European/US stations use 12z.
    """
    init_start = datetime(start_date.year, start_date.month, start_date.day, 0, tzinfo=timezone.utc)
    init_end = datetime(end_date.year, end_date.month, end_date.day, 23, 59, 59, tzinfo=timezone.utc)
    valid_end = init_end + timedelta(hours=96)
    return _query_and_reduce(
        init_start=init_start,
        init_end=init_end,
        valid_end=valid_end,
        station=station,
        init_hours=init_hours,
    )


def _query_and_reduce(
    init_start: datetime,
    init_end: datetime,
    valid_end: datetime,
    station: StationConfig,
    init_hours: tuple[int, ...] | None = None,
) -> pl.DataFrame:
    """Run the BQ query, interpolate to station, reduce to per-member daily max."""
    from google.cloud import bigquery  # type: ignore[import-untyped]

    client = _bq_client()
    table = _fq_table()
    lat_lo = station.lat - _BBOX_HALFWIDTH_DEG
    lat_hi = station.lat + _BBOX_HALFWIDTH_DEG
    lon_lo = station.lon - _BBOX_HALFWIDTH_DEG
    lon_hi = station.lon + _BBOX_HALFWIDTH_DEG

    hours_clause = ""
    params = [
        bigquery.ScalarQueryParameter("init_start", "TIMESTAMP", init_start),
        bigquery.ScalarQueryParameter("init_end", "TIMESTAMP", init_end),
        bigquery.ScalarQueryParameter("valid_end", "TIMESTAMP", valid_end),
        bigquery.ScalarQueryParameter("lat_lo", "FLOAT64", lat_lo),
        bigquery.ScalarQueryParameter("lat_hi", "FLOAT64", lat_hi),
        bigquery.ScalarQueryParameter("lon_lo", "FLOAT64", lon_lo),
        bigquery.ScalarQueryParameter("lon_hi", "FLOAT64", lon_hi),
    ]
    if init_hours:
        hours_clause = f"AND EXTRACT(HOUR FROM {_COL_INIT_TIME}) IN UNNEST(@init_hours)"
        params.append(
            bigquery.ArrayQueryParameter("init_hours", "INT64", list(init_hours))
        )

    sql = f"""
        SELECT
          {_COL_INIT_TIME}  AS init_time,
          {_COL_VALID_TIME} AS valid_time,
          {_COL_MEMBER}     AS member,
          ST_Y({_COL_GEOG}) AS lat,
          ST_X({_COL_GEOG}) AS lon,
          {_COL_T2M}        AS t2m_k
        FROM {table}
        WHERE {_COL_INIT_TIME} BETWEEN @init_start AND @init_end
          AND {_COL_VALID_TIME} <= @valid_end
          AND ST_Y({_COL_GEOG}) BETWEEN @lat_lo AND @lat_hi
          AND ST_X({_COL_GEOG}) BETWEEN @lon_lo AND @lon_hi
          {hours_clause}
    """

    cfg = bigquery.QueryJobConfig(query_parameters=params)

    try:
        rows = list(client.query(sql, job_config=cfg).result())
    except Exception as exc:
        raise IngestError(f"WeatherNext BigQuery query failed: {exc}") from exc

    if not rows:
        raise IngestError(
            f"WeatherNext: no rows for {station.icao} init in [{init_start}, {init_end}]"
        )

    return _reduce_to_daily_max(rows, station, init_start)


def _reduce_to_daily_max(
    rows: list[Any],
    station: StationConfig,
    init_start: datetime,
) -> pl.DataFrame:
    """Inverse-distance-weight the per-cell values onto station lat/lon, then
    reduce per (init, member) to local-timezone daily max."""
    import math

    tz = zoneinfo.ZoneInfo(station.timezone)

    # Group rows by (init, member, valid_time) → list of (cell_lat, cell_lon, t2m_k)
    cells: dict[tuple[datetime, Any, datetime], list[tuple[float, float, float]]] = {}
    for r in rows:
        key = (r["init_time"], r["member"], r["valid_time"])
        cells.setdefault(key, []).append((float(r["lat"]), float(r["lon"]), float(r["t2m_k"])))

    # Interpolate each timestep to station lat/lon via inverse-distance weighting.
    # Squared-distance weight matches a 2-D linear approximation closely enough
    # for sub-grid points; falls back to nearest if a cell is exactly on station.
    point_temps: dict[tuple[datetime, Any, datetime], float] = {}
    for (init_time, member, valid_time), cell_vals in cells.items():
        num = 0.0
        den = 0.0
        nearest = None
        for clat, clon, t_k in cell_vals:
            dlat = clat - station.lat
            dlon = clon - station.lon
            d2 = dlat * dlat + dlon * dlon
            if d2 < 1e-12:
                nearest = t_k
                break
            w = 1.0 / d2
            num += w * t_k
            den += w
        t2m_k = nearest if nearest is not None else (num / den if den > 0 else None)
        if t2m_k is None or not math.isfinite(t2m_k):
            continue
        point_temps[(init_time, member, valid_time)] = t2m_k - 273.15

    # Reduce per (init, member, local_date) to max
    day_max: dict[tuple[datetime, Any, date], float] = {}
    for (init_time, member, valid_time), tc in point_temps.items():
        valid_utc = valid_time if valid_time.tzinfo else valid_time.replace(tzinfo=timezone.utc)
        local_day = valid_utc.astimezone(tz).date()
        key = (init_time, member, local_day)
        prev = day_max.get(key, -999.0)
        if tc > prev:
            day_max[key] = tc

    rows_out: list[dict[str, Any]] = []
    for (init_time, member, local_day), tmax in day_max.items():
        init_utc = init_time if init_time.tzinfo else init_time.replace(tzinfo=timezone.utc)
        rows_out.append({
            "model": _MODEL,
            "member_id": _coerce_member_int(member),
            "init_datetime": init_utc,
            "valid_date": local_day,
            "station": station.icao,
            "daily_max_c": tmax,
            "lead_hours": _lead_hours(init_utc, local_day, tz),
        })

    if not rows_out:
        raise IngestError(
            f"WeatherNext: no usable temperature samples after reduction for {station.icao}"
        )

    return pl.DataFrame(rows_out)


def _coerce_member_int(member: Any) -> int:
    """Members may arrive as int or as zero-padded strings ('00', '01', ...)."""
    if isinstance(member, int):
        return member
    try:
        return int(str(member))
    except (TypeError, ValueError):
        return 0  # last-resort sentinel — control member


def _lead_hours(init_dt: datetime, valid_date: date, tz: zoneinfo.ZoneInfo) -> int:
    noon_local = datetime(valid_date.year, valid_date.month, valid_date.day, 12, tzinfo=tz)
    delta = noon_local.astimezone(timezone.utc) - init_dt.astimezone(timezone.utc)
    return int(round(delta.total_seconds() / 3600 / 24) * 24)
