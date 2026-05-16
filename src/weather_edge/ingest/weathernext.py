"""WeatherNext 2 ingestion — GCS/Zarr only. BigQuery is hard-disabled.

Google DeepMind's diffusion-based ensemble model:
  - 64-member ensemble (`sample` dim)
  - 0.25° grid (721 lat × 1440 lon, longitudes 0..360)
  - 6-hour init cadence (00/06/12/18 UTC)
  - 6-hour lead steps to +15 days (60 leads, `prediction_timedelta` dim)
  - Variable name: `2m_temperature` (Kelvin)

Real-time (init_time within last 48h)  → GDM Real-Time Experimental Data ToS.
Historic (init_time older than 48h)    → CC BY 4.0.

GCS layout (resolver in _resolve_uris):
  gs://weathernext/weathernext_2_0_0/zarr/
    ├── 2022_to_2023/predictions.zarr/      ← consolidated 5D, all 1460 inits for 2022
    ├── 2023_to_2024/predictions.zarr/
    ├── 2024_to_2025/predictions.zarr/
    └── 2025_to_present/
        └── YYYYMMDD_HHhr_01_preds/
            └── predictions.zarr/             ← consolidated 4D, single init

The historic and per-init stores use different dim names (`time` is init in
the former, lead in the latter). _open_and_normalize() renames everything to
canonical (init_time, prediction_timedelta, sample, lat, lon) before concat.

BigQuery backend — HARD-DISABLED:
  The library used to support a BigQuery backend via WEATHERNEXT_BACKEND=bigquery.
  That code path nearly cost £215 (199 TiB scanned) in 2026-05 and has been
  intentionally disabled at the entrypoints (`ingest_forecasts`, `ingest_historic`)
  via `_assert_no_bq_backend()`. The BQ helper functions below remain in the file
  for reference, but no public entrypoint dispatches to them. The associated
  backfill script (`scripts/backfill_weathernext.py`) is also stubbed.

  To re-enable BigQuery (you almost certainly do not want to), you must:
    1. Delete `_assert_no_bq_backend()` calls in `ingest_forecasts` / `ingest_historic`.
    2. Restore the env-var dispatch (`if backend == "bigquery": ...`).
    3. Restore `scripts/backfill_weathernext.py` from git.
  Three locks, by design.

GCS backend (the only live path):
  Reads from gs://weathernext/. Each (init, station) fetch transfers ~4 GiB
  (chunks span the full lat/lon grid; we pull one chunk per (member, lead)
  we want). Egress is covered by Google for this public dataset — empirically
  verified 2026-05-16: 5 GiB test pull from Toronto VPS resulted in £0 Cloud
  Storage charge. Backfill is free.

GCS env vars:
  WEATHERNEXT_GCS_BASE            — Zarr collection root (default
                                    "gs://weathernext/weathernext_2_0_0/zarr/").
                                    Resolver appends year-range or per-init suffixes.
  GOOGLE_APPLICATION_CREDENTIALS  — service-account JSON (optional on a GCE VM
                                    with the default SA already allowlisted)
  WEATHERNEXT_BACKEND             — if set to anything other than "gcs" (case-
                                    insensitive), ingest raises IngestError.
                                    Set to "gcs" or leave unset.

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

# Hard ceiling on bytes-billed per BQ query. Default 2 GB ≈ $0.012 — a single-init
# real-time query scans well under this; anything larger is almost certainly an
# accident. The backfill script raises this per-chunk after a dry-run estimate.
_DEFAULT_MAX_BYTES_BILLED_GB = 2.0
_USD_PER_TIB = 6.25  # BQ on-demand pricing

_DEFAULT_GCS_BASE = "gs://weathernext/weathernext_2_0_0/zarr/"

# Boundary year between historic year-partitioned stores and per-init stores.
# Inits with year < _PER_INIT_FROM_YEAR live in `<Y>_to_<Y+1>/predictions.zarr`.
# Inits with year >= _PER_INIT_FROM_YEAR live in `2025_to_present/<dir>/predictions.zarr`.
_PER_INIT_FROM_YEAR = 2025


def _assert_no_bq_backend() -> None:
    """Block any code path that would route ingest through BigQuery.

    After the 199 TiB / £215 near-miss, BQ is hard-disabled at the entrypoints.
    A stale WEATHERNEXT_BACKEND=bigquery env var on the VPS would have silently
    re-enabled the BQ path; this check turns that into a loud IngestError.
    """
    raw = os.getenv("WEATHERNEXT_BACKEND")
    if raw and raw.lower() != "gcs":
        raise IngestError(
            f"WEATHERNEXT_BACKEND={raw!r} is set, but the BigQuery backend is "
            "hard-disabled. Unset or set to 'gcs'. See weathernext.py docstring "
            "for the reason."
        )


def _max_bytes_billed() -> int:
    gb = float(os.getenv("WEATHERNEXT_MAX_BYTES_BILLED_GB", _DEFAULT_MAX_BYTES_BILLED_GB))
    return int(gb * 1024**3)


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
    """Fetch 64-member WN2 2m temperature for a single init and return per-member daily max.

    Columns: model, member_id, init_datetime, valid_date, station, daily_max_c, lead_hours

    Always uses the GCS Zarr backend. BigQuery dispatch is hard-disabled here
    after the 199 TiB / £215 near-miss in 2026-05. To re-enable BQ you must
    delete the guard below AND remove the disable in `ingest_historic`.
    """
    _assert_no_bq_backend()
    init_utc = init_dt.replace(tzinfo=timezone.utc)
    return _gcs_query_and_reduce(
        init_start=init_utc,
        init_end=init_utc + timedelta(seconds=1),
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
    _assert_no_bq_backend()
    init_start = datetime(start_date.year, start_date.month, start_date.day, 0, tzinfo=timezone.utc)
    init_end = datetime(end_date.year, end_date.month, end_date.day, 23, 59, 59, tzinfo=timezone.utc)
    return _gcs_query_and_reduce(
        init_start=init_start,
        init_end=init_end,
        station=station,
        init_hours=init_hours,
    )


def _bq_query_and_reduce(
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

    cfg = bigquery.QueryJobConfig(
        query_parameters=params,
        use_query_cache=True,             # free re-reads of identical queries within 24h
        maximum_bytes_billed=_max_bytes_billed(),
    )

    try:
        job = client.query(sql, job_config=cfg)
        rows = list(job.result())
    except Exception as exc:
        raise IngestError(f"WeatherNext BigQuery query failed: {exc}") from exc

    billed = getattr(job, "total_bytes_billed", 0) or 0
    cached = getattr(job, "cache_hit", False)
    gib = billed / 1024**3
    usd = (billed / 1024**4) * _USD_PER_TIB
    _logger.info(
        "WN2 query: billed=%.2f GiB ($%.4f)%s init=[%s,%s]",
        gib, usd, " (cache hit)" if cached else "", init_start.isoformat(), init_end.isoformat(),
    )

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


def _resolve_uris(
    init_start: datetime,
    init_end: datetime,
    init_hours: tuple[int, ...] | None,
) -> list[tuple[str, str]]:
    """Return [(uri, kind), ...] covering all inits in [init_start, init_end].

    kind is one of:
      "historic" — one zarr per year (`YYYY_to_YYYY+1/predictions.zarr`).
                   Covers all inits for that year; caller must slice after open.
      "per_init" — one zarr per init time (`2025_to_present/<dir>/predictions.zarr`).
                   URI itself encodes the date+hour, so we only enumerate the
                   specific inits we want (4 per day max).
    """
    base = os.getenv("WEATHERNEXT_GCS_BASE", _DEFAULT_GCS_BASE).rstrip("/")
    hours = init_hours if init_hours else (0, 6, 12, 18)

    uris: list[tuple[str, str]] = []
    seen: set[str] = set()
    year = init_start.year
    end_year = init_end.year
    while year <= end_year:
        if year < _PER_INIT_FROM_YEAR:
            uri = f"{base}/{year}_to_{year + 1}/predictions.zarr/"
            if uri not in seen:
                uris.append((uri, "historic"))
                seen.add(uri)
        else:
            year_start = max(init_start, datetime(year, 1, 1, tzinfo=timezone.utc))
            year_end = min(init_end, datetime(year, 12, 31, 23, 59, 59, tzinfo=timezone.utc))
            day = year_start.date()
            while day <= year_end.date():
                for h in hours:
                    init_dt = datetime(day.year, day.month, day.day, h, tzinfo=timezone.utc)
                    if init_start <= init_dt <= init_end:
                        dirname = f"{init_dt.strftime('%Y%m%d')}_{h:02d}hr_01_preds"
                        uri = f"{base}/2025_to_present/{dirname}/predictions.zarr/"
                        if uri not in seen:
                            uris.append((uri, "per_init"))
                            seen.add(uri)
                day = day + timedelta(days=1)
        year += 1
    return uris


def _open_and_normalize(uri: str, kind: str) -> Any:
    """Open one WN2 Zarr store and rename dims to canonical names.

    Both historic and per-init stores carry a dim called `time`, but it means
    different things:
      historic:  `time` is the init axis (datetime64, size 1460); `prediction_timedelta`
                 is the lead axis.
      per_init:  `time` is the LEAD axis (timedelta64, size 60); `init_time` is a
                 scalar coord; there is no `prediction_timedelta` dim.

    This function returns a Dataset where init_time is always a dim and
    prediction_timedelta is always the lead dim — so caller can concat over a
    mixed list of stores without special-casing.
    """
    import pandas as pd  # type: ignore[import-untyped]
    import xarray as xr  # type: ignore[import-untyped]

    ds = xr.open_zarr(uri, consolidated=True, chunks={})
    if kind == "historic":
        if "time" in ds.dims:
            ds = ds.rename({"time": "init_time"})
    elif kind == "per_init":
        if "time" in ds.dims:
            ds = ds.rename({"time": "prediction_timedelta"})
        if "init_time" in ds.coords and "init_time" not in ds.dims:
            init_val = ds["init_time"].values
            init_ts = pd.Timestamp(init_val)
            ds = ds.drop_vars("init_time").expand_dims({"init_time": [init_ts]})
    else:
        raise IngestError(f"_open_and_normalize: unknown kind {kind!r}")
    return ds


def _open_combined(
    init_start: datetime,
    init_end: datetime,
    init_hours: tuple[int, ...] | None = None,
) -> Any:
    """Open every WN2 Zarr store covering [init_start, init_end] and concat them.

    Returns a single xarray.Dataset with canonical dims (init_time, sample,
    prediction_timedelta, lat, lon). Lazy — heavy fetch happens at .load().

    Per-init stores (2025+) get their scalar init_time promoted to a size-1 dim
    so they concat cleanly with historic-store inits.
    """
    try:
        import xarray as xr  # type: ignore[import-untyped]
    except ImportError as exc:
        raise IngestError("xarray not installed — required for GCS backend") from exc
    try:
        import gcsfs  # type: ignore[import-untyped]  # noqa: F401 (registered by fsspec)
        import zarr  # type: ignore[import-untyped]  # noqa: F401
    except ImportError as exc:
        raise IngestError("gcsfs/zarr not installed — required for GCS backend") from exc

    uris = _resolve_uris(init_start, init_end, init_hours)
    if not uris:
        raise IngestError(
            f"WeatherNext: no zarr stores resolved for [{init_start}, {init_end}]"
        )

    parts = []
    for uri, kind in uris:
        try:
            ds = _open_and_normalize(uri, kind)
        except Exception as exc:
            _logger.warning("WN2 GCS: skipping %s (%s)", uri, exc)
            continue
        if "2m_temperature" in ds.data_vars:
            ds = ds[["2m_temperature"]]
        if kind == "historic":
            ds = ds.sel(init_time=slice(init_start, init_end))
            if ds.sizes.get("init_time", 0) == 0:
                continue
        parts.append(ds)

    if not parts:
        raise IngestError(
            f"WeatherNext: no openable zarr stores for [{init_start}, {init_end}]. "
            "Verify the SA is allowlisted and run scripts/weathernext_gcs_probe.py."
        )

    return xr.concat(parts, dim="init_time", combine_attrs="override")


def _gcs_query_and_reduce(
    init_start: datetime,
    init_end: datetime,
    station: StationConfig,
    init_hours: tuple[int, ...] | None = None,
) -> pl.DataFrame:
    """GCS/Zarr equivalent of _bq_query_and_reduce.

    Opens all stores covering [init_start, init_end] via `_open_combined` —
    handles the historic-vs-per-init layout difference and returns one Dataset
    with canonical dims. Then: nearest-cell pick, lead cap, local-tz bucketing,
    daily max per (init, member, local_date).

    Per-(init, station) fetch transfers ~4 GiB of chunks (each chunk spans the
    full lat/lon grid). Egress is free for this public dataset — see module
    docstring.
    """
    import numpy as np
    import pandas as pd

    ds = _open_combined(init_start, init_end, init_hours)

    init_dim = "init_time"
    lead_dim = "prediction_timedelta"
    lat_dim = _pick_dim(ds, ("latitude", "lat"))
    lon_dim = _pick_dim(ds, ("longitude", "lon"))
    member_dim = _pick_dim(ds, ("sample", "number", "ensemble_member", "member", "realization"))
    t2m_var = _pick_var(ds, ("2m_temperature", "t2m", "2t"))

    # Bucket uses 0..360 longitudes; -180..180 stations need wrapping.
    lon_vals = ds[lon_dim].values
    lon_max = float(np.nanmax(lon_vals))
    station_lon = station.lon % 360 if lon_max > 180 else station.lon

    # init_hours filtering for historic stores (per-init URIs are pre-filtered
    # by _resolve_uris). Cheap — operates on a small datetime index.
    if init_hours is not None:
        init_index = pd.DatetimeIndex(ds[init_dim].values)
        keep = np.isin(init_index.hour, list(init_hours))
        if not keep.any():
            raise IngestError(
                f"WeatherNext: no inits in [{init_start}, {init_end}] at hours {init_hours}"
            )
        ds = ds.isel({init_dim: np.where(keep)[0]})

    # Nearest single cell — interpolating across 4 cells is a <0.1°C gain that
    # EMOS bias absorbs anyway (same rationale as the BQ path).
    cell = ds[t2m_var].sel(
        {lat_dim: station.lat, lon_dim: station_lon},
        method="nearest",
    )

    # Cap lead to keep the chunk fetch small. Coord may be timedelta64 or int hours.
    lead_vals = cell[lead_dim].values
    if np.issubdtype(lead_vals.dtype, np.timedelta64):
        lead_hours_arr = (lead_vals / np.timedelta64(1, "h")).astype("int64")
    else:
        lead_hours_arr = np.asarray(lead_vals, dtype="int64")
    cell = cell.isel({lead_dim: np.where(lead_hours_arr <= _MAX_LEAD_HOURS)[0]})

    # Realize the chunk(s) to memory — only the requested (init × member × lead) cells.
    cell = cell.load()
    df = cell.to_dataframe().reset_index()
    if df.empty:
        raise IngestError(
            f"WeatherNext (GCS): no rows for {station.icao} init in [{init_start}, {init_end}]"
        )

    tz = zoneinfo.ZoneInfo(station.timezone)
    valid_utc = pd.to_datetime(df[init_dim], utc=True)
    lead_td = df[lead_dim]
    if not np.issubdtype(lead_td.dtype, np.timedelta64):
        lead_td = pd.to_timedelta(lead_td.astype("int64"), unit="h")
    valid_utc = valid_utc + lead_td
    df["_valid_utc"] = valid_utc
    df["_local_date"] = valid_utc.dt.tz_convert(tz).dt.date

    df["_t2m_c"] = df[t2m_var] - 273.15
    grouped = (
        df.dropna(subset=["_t2m_c"])
          .groupby([init_dim, member_dim, "_local_date"], as_index=False)["_t2m_c"]
          .max()
    )

    rows_out: list[dict[str, Any]] = []
    for _, r in grouped.iterrows():
        init_utc = pd.Timestamp(r[init_dim]).to_pydatetime()
        if init_utc.tzinfo is None:
            init_utc = init_utc.replace(tzinfo=timezone.utc)
        local_day = r["_local_date"]
        rows_out.append({
            "model": _MODEL,
            "member_id": _coerce_member_int(r[member_dim]),
            "init_datetime": init_utc,
            "valid_date": local_day,
            "station": station.icao,
            "daily_max_c": float(r["_t2m_c"]),
            "lead_hours": _lead_hours(init_utc, local_day, tz),
        })

    _logger.info(
        "WN2 GCS read: %d rows  station=%s  init=[%s,%s]",
        len(rows_out), station.icao, init_start.isoformat(), init_end.isoformat(),
    )
    return pl.DataFrame(rows_out)


def _pick_dim(ds: Any, candidates: tuple[str, ...]) -> str:
    for name in candidates:
        if name in ds.dims:
            return name
    raise IngestError(
        f"WeatherNext Zarr missing expected dim — tried {candidates}, "
        f"found {list(ds.dims)}. Run scripts/weathernext_gcs_probe.py to inspect."
    )


def _pick_var(ds: Any, candidates: tuple[str, ...]) -> str:
    for name in candidates:
        if name in ds.data_vars:
            return name
    raise IngestError(
        f"WeatherNext Zarr missing expected variable — tried {candidates}, "
        f"found {list(ds.data_vars)}. Run scripts/weathernext_gcs_probe.py to inspect."
    )


def estimate_historic_bytes(
    start_date: date,
    end_date: date,
    station: StationConfig,
    init_hours: tuple[int, ...] = (0, 12),
) -> int:
    """Dry-run the historic query and return bytes BigQuery would bill.

    Charges nothing — uses `dry_run=True`. Use this before kicking off a
    multi-year backfill so you can put a USD number on it first.
    """
    from google.cloud import bigquery  # type: ignore[import-untyped]

    init_start = datetime(start_date.year, start_date.month, start_date.day, 0, tzinfo=timezone.utc)
    init_end = datetime(end_date.year, end_date.month, end_date.day, 23, 59, 59, tzinfo=timezone.utc)

    client = _bq_client()
    table = _fq_table()
    lat_lo = station.lat - _BBOX_HALFWIDTH_DEG
    lat_hi = station.lat + _BBOX_HALFWIDTH_DEG
    lon_lo = station.lon - _BBOX_HALFWIDTH_DEG
    lon_hi = station.lon + _BBOX_HALFWIDTH_DEG

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
        bigquery.ArrayQueryParameter("init_hours", "INT64", list(init_hours)),
    ]
    sql = f"""
        SELECT 1 FROM {table},
        UNNEST({_COL_FORECAST}) AS f,
        UNNEST(f.{_FCS_ENSEMBLE}) AS e
        WHERE {_COL_INIT_TIME} BETWEEN @init_start AND @init_end
          AND f.hours <= @max_lead
          AND ST_Y({_COL_GEOG}) BETWEEN @lat_lo AND @lat_hi
          AND ST_X({_COL_GEOG}) BETWEEN @lon_lo AND @lon_hi
          AND EXTRACT(HOUR FROM {_COL_INIT_TIME}) IN UNNEST(@init_hours)
    """
    cfg = bigquery.QueryJobConfig(query_parameters=params, dry_run=True, use_query_cache=False)
    job = client.query(sql, job_config=cfg)
    return int(getattr(job, "total_bytes_processed", 0) or 0)


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
