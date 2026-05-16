"""WeatherNext 2 ingestion — GCS/Zarr only.

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

BigQuery — REMOVED:
  The library used to support a BigQuery backend that nearly cost £215
  (199 TiB scanned) in 2026-05. After the empirical confirmation that GCS
  egress from gs://weathernext is sponsor-paid (5 GiB test → £0 charge on
  2026-05-16), BQ was nuked: google-cloud-bigquery removed from
  pyproject.toml, BQ helpers deleted from this file, BQ scripts stubbed.
  `_assert_no_bq_backend()` remains as a guard so a stale
  WEATHERNEXT_BACKEND env var fails loudly rather than silently.

GCS backend (the only path):
  Reads from gs://weathernext/. Each (init, station) fetch transfers ~4 GiB
  (chunks span the full lat/lon grid; we pull one chunk per (member, lead)
  we want). Egress is sponsor-paid for this public dataset — empirically
  verified 2026-05-16: 5 GiB test pull from Toronto VPS resulted in £0
  Cloud Storage charge.

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

# Cap forecast lead so we don't load every chunk to 15-day horizon. The pipeline's
# EMOS/BMA use lead buckets (24, 48, 72); 96h buffers all of those. Bumping this
# lets you experiment with longer leads later (small extra GCS transfer per added
# 24h since each chunk is one lead step).
_MAX_LEAD_HOURS = 96

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


def ingest_forecasts(init_dt: datetime, station: StationConfig) -> pl.DataFrame:
    """Fetch 64-member WN2 2m temperature for a single init and return per-member daily max.

    Columns: model, member_id, init_datetime, valid_date, station, daily_max_c, lead_hours

    GCS/Zarr only; BigQuery has been removed. See module docstring.
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
    """Open WN2 Zarr stores for [init_start, init_end] and reduce to daily max per member.

    Opens all stores covering the window via `_open_combined` — handles the
    historic-vs-per-init layout difference and returns one Dataset with
    canonical dims. Then: nearest-cell pick, lead cap, local-tz bucketing,
    daily max per (init, member, local_date).

    Per-(init, station) fetch transfers ~4 GiB of chunks (each chunk spans the
    full lat/lon grid). Egress is sponsor-paid for this public dataset — see
    module docstring.
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
