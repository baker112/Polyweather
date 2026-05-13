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

# Schema (verified 2026-05-13 against the linked Analytics Hub share):
#   init_time                TIMESTAMP
#   geography                GEOGRAPHY   (POINT lon/lat of the 0.25° cell centre)
#   geography_polygon        GEOGRAPHY
#   forecast                 RECORD REPEATED
#     time                   TIMESTAMP  (valid time of this lead)
#     hours                  INT64       (lead hours)
#     ensemble               RECORD REPEATED
#       ensemble_member      STRING      ("00".."49")
#       2m_temperature       FLOAT64     (Kelvin)
#       ... other variables (winds, MSLP, etc.) ...
_COL_INIT_TIME = "init_time"
_COL_GEOG = "geography"
_COL_FORECAST = "forecast"
_FCS_VALID_TIME = "time"
_FCS_ENSEMBLE = "ensemble"
_ENS_MEMBER = "ensemble_member"
_ENS_T2M = "`2m_temperature`"  # backticked: identifier starts with a digit

# Bounding box (degrees) around station lat/lon: pull all grid cells inside this
# half-width. 0.3° at 50°N covers roughly the nearest 4 cells of a 0.25° grid,
# which is enough for a simple inverse-distance interpolation.
_BBOX_HALFWIDTH_DEG = 0.3

# Cap forecast lead in BigQuery so we don't scan the full 15-day horizon.
# The pipeline's EMOS/BMA use lead buckets (24, 48, 72); 96h buffers all of
# those. Bumping this would let you experiment with longer leads later, at
# 1× scan-cost growth per ~24h added.
_MAX_LEAD_HOURS = 96


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
    return _query_and_reduce(
        init_start=init_utc,
        init_end=init_utc + timedelta(seconds=1),  # exact-init match
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
    return _query_and_reduce(
        init_start=init_start,
        init_end=init_end,
        station=station,
        init_hours=init_hours,
    )


def _query_and_reduce(
    init_start: datetime,
    init_end: datetime,
    station: StationConfig,
    init_hours: tuple[int, ...] | None = None,
) -> pl.DataFrame:
    """Run the BQ aggregation query and assemble the parquet-shaped DataFrame.

    All heavy work (UNNEST, nearest-cell pick, local-timezone bucketing, daily
    max) runs in BigQuery — Python only sees ~50 members × ~6 valid_dates per
    init in the result set, so monthly chunks return ~20k rows instead of
    ~720k. A pure-Python row loop on that scale was the previous bottleneck.
    """
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
        bigquery.ScalarQueryParameter("max_lead", "INT64", _MAX_LEAD_HOURS),
        bigquery.ScalarQueryParameter("lat_lo", "FLOAT64", lat_lo),
        bigquery.ScalarQueryParameter("lat_hi", "FLOAT64", lat_hi),
        bigquery.ScalarQueryParameter("lon_lo", "FLOAT64", lon_lo),
        bigquery.ScalarQueryParameter("lon_hi", "FLOAT64", lon_hi),
        bigquery.ScalarQueryParameter("station_lat", "FLOAT64", station.lat),
        bigquery.ScalarQueryParameter("station_lon", "FLOAT64", station.lon),
        bigquery.ScalarQueryParameter("tz", "STRING", station.timezone),
    ]
    if init_hours:
        hours_clause = f"AND EXTRACT(HOUR FROM {_COL_INIT_TIME}) IN UNNEST(@init_hours)"
        params.append(
            bigquery.ArrayQueryParameter("init_hours", "INT64", list(init_hours))
        )

    # Strategy (all in SQL):
    #   1. UNNEST the nested forecast/ensemble arrays inside the bbox+time window.
    #   2. For each (init, member, valid_time), pick the single nearest grid
    #      cell — interpolating across 4 cells gives sub-0.1°C gain that EMOS
    #      bias correction absorbs anyway, not worth the row blowup.
    #   3. Bucket valid_time into the station's LOCAL date via DATE(ts, @tz).
    #   4. MAX over the local day → one row per (init, member, local_date).
    sql = f"""
        WITH expanded AS (
          SELECT
            {_COL_INIT_TIME}    AS init_time,
            f.{_FCS_VALID_TIME} AS valid_time,
            e.{_ENS_MEMBER}     AS member,
            e.{_ENS_T2M}        AS t2m_k,
            ST_DISTANCE({_COL_GEOG}, ST_GEOGPOINT(@station_lon, @station_lat)) AS dist_m
          FROM {table},
          UNNEST({_COL_FORECAST}) AS f,
          UNNEST(f.{_FCS_ENSEMBLE}) AS e
          WHERE {_COL_INIT_TIME} BETWEEN @init_start AND @init_end
            AND f.hours <= @max_lead
            AND ST_Y({_COL_GEOG}) BETWEEN @lat_lo AND @lat_hi
            AND ST_X({_COL_GEOG}) BETWEEN @lon_lo AND @lon_hi
            {hours_clause}
        ),
        nearest AS (
          SELECT init_time, valid_time, member, t2m_k
          FROM expanded
          QUALIFY ROW_NUMBER() OVER (
            PARTITION BY init_time, valid_time, member ORDER BY dist_m
          ) = 1
        )
        SELECT
          init_time,
          member,
          DATE(valid_time, @tz) AS local_date,
          MAX(t2m_k) - 273.15   AS daily_max_c
        FROM nearest
        WHERE t2m_k IS NOT NULL
        GROUP BY init_time, member, local_date
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

    tz = zoneinfo.ZoneInfo(station.timezone)
    rows_out: list[dict[str, Any]] = []
    for r in rows:
        init_utc = r["init_time"]
        if init_utc.tzinfo is None:
            init_utc = init_utc.replace(tzinfo=timezone.utc)
        local_day = r["local_date"]  # already a `date` object from BQ DATE()
        rows_out.append({
            "model": _MODEL,
            "member_id": _coerce_member_int(r["member"]),
            "init_datetime": init_utc,
            "valid_date": local_day,
            "station": station.icao,
            "daily_max_c": float(r["daily_max_c"]),
            "lead_hours": _lead_hours(init_utc, local_day, tz),
        })

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
