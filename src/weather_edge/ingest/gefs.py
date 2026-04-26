"""GEFS ingestion from AWS S3 Open Data (anonymous access).

Bucket: noaa-gefs-pds
Keys:   gefs.{YYYYMMDD}/{HH}/atmos/pgrb2ap25/
        gec00.t{HH}z.pgrb2a.0p25.f{FFF}   (control)
        gep{NN}.t{HH}z.pgrb2a.0p25.f{FFF}  (perturbed, NN=01-30)

Variable: TMP / 2 m above ground (shortName "2t" in GRIB, or "TMP:2 m above ground")
Members: 31 total (c00 + p01-p30)
"""
from __future__ import annotations

import logging
import tempfile
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import numpy as np
import polars as pl
import xarray as xr
import zoneinfo

from weather_edge.config import StationConfig
from weather_edge.exceptions import IngestError

_logger = logging.getLogger(__name__)
_BUCKET = "noaa-gefs-pds"
_MODEL = "gefs"

# Lead steps in hours; GEFS runs to 384h but we only need D+1 through D+3
_STEPS = list(range(0, 121, 6))


def ingest_forecasts(init_dt: datetime, station: StationConfig) -> pl.DataFrame:
    """Fetch 31-member GEFS ensemble and return a forecast DataFrame.

    Columns: model, member_id, init_datetime, valid_date, station, daily_max_c, lead_hours
    """
    try:
        import s3fs  # type: ignore[import-untyped]
    except ImportError as exc:
        raise IngestError("s3fs not installed") from exc

    fs = s3fs.S3FileSystem(anon=True)
    tz = zoneinfo.ZoneInfo(station.timezone)
    date_str = init_dt.strftime("%Y%m%d")
    hour_str = f"{init_dt.hour:02d}"
    prefix = f"{_BUCKET}/gefs.{date_str}/{hour_str}/atmos/pgrb2ap25"

    # Build list of (member_id, s3_key, lead_step) tuples
    member_keys: list[tuple[int, str, int]] = []
    for step in _STEPS:
        step_str = f"{step:03d}"
        # Control member
        key_c = f"{prefix}/gec00.t{hour_str}z.pgrb2a.0p25.f{step_str}"
        member_keys.append((0, key_c, step))
        # Perturbed members
        for m in range(1, 31):
            key_p = f"{prefix}/gep{m:02d}.t{hour_str}z.pgrb2a.0p25.f{step_str}"
            member_keys.append((m, key_p, step))

    # Track per-member, per-day max temperature
    # Structure: {member_id: {valid_date: max_temp_c}}
    day_max: dict[int, dict[date, float]] = {m: {} for m in range(31)}

    rows: list[dict[str, Any]] = []

    with tempfile.TemporaryDirectory() as tmpdir:
        # Download in step-level batches to minimise S3 open calls
        for step in _STEPS:
            valid_utc = init_dt.replace(tzinfo=timezone.utc) + timedelta(hours=step)
            local_day = valid_utc.astimezone(tz).date()

            for member_id in range(31):
                if member_id == 0:
                    key = f"{prefix}/gec00.t{hour_str}z.pgrb2a.0p25.f{step:03d}"
                else:
                    key = f"{prefix}/gep{member_id:02d}.t{hour_str}z.pgrb2a.0p25.f{step:03d}"

                tmp_path = Path(tmpdir) / f"m{member_id:02d}_f{step:03d}.grib2"
                try:
                    fs.get(key, str(tmp_path))
                    temp_c = _extract_temp(tmp_path, station.lat, station.lon)
                    if temp_c is not None:
                        prev = day_max[member_id].get(local_day, -999.0)
                        day_max[member_id][local_day] = max(prev, temp_c)
                except Exception as exc:
                    _logger.debug("GEFS skip %s: %s", key, exc)

    now = datetime.now(timezone.utc)
    for member_id, date_temps in day_max.items():
        for vd, tmax in date_temps.items():
            lead = _lead_hours(init_dt, vd, tz)
            rows.append({
                "model": _MODEL,
                "member_id": member_id,
                "init_datetime": init_dt.replace(tzinfo=timezone.utc),
                "valid_date": vd,
                "station": station.icao,
                "daily_max_c": tmax,
                "lead_hours": lead,
            })

    if not rows:
        raise IngestError("GEFS: no data retrieved")

    return pl.DataFrame(rows)


def _extract_temp(grib_path: Path, lat: float, lon: float) -> float | None:
    """Bilinear-interpolate 2m temperature from a single GRIB2 file to station lat/lon."""
    try:
        ds = xr.open_dataset(
            grib_path,
            engine="cfgrib",
            backend_kwargs={
                "filter_by_keys": {
                    "typeOfLevel": "heightAboveGround",
                    "level": 2,
                    "shortName": "2t",
                }
            },
        )
    except Exception:
        try:
            # Fallback: try without filter (some files use different encoding)
            ds = xr.open_dataset(grib_path, engine="cfgrib")
        except Exception:
            return None

    var_name = "t2m" if "t2m" in ds else next(iter(ds.data_vars), None)
    if var_name is None:
        return None

    # GEFS uses 0-360 longitude; normalise lon
    lon_360 = lon % 360.0
    try:
        val = ds[var_name].interp(
            latitude=lat,
            longitude=lon_360,
            method="linear",
        )
        temp_c = float(val.values) - 273.15
        return temp_c if np.isfinite(temp_c) else None
    except Exception:
        return None


def _lead_hours(init_dt: datetime, valid_date: date, tz: zoneinfo.ZoneInfo) -> int:
    noon_local = datetime(valid_date.year, valid_date.month, valid_date.day, 12, tzinfo=tz)
    delta = noon_local.astimezone(timezone.utc) - init_dt.replace(tzinfo=timezone.utc)
    return int(round(delta.total_seconds() / 3600 / 24) * 24)
