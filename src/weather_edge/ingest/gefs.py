"""GEFS ingestion from AWS S3 Open Data (anonymous access).

Bucket:  noaa-gefs-pds
Path:    gefs.{YYYYMMDD}/{HH}/atmos/pgrb2ap5/
Files:   gec00.t{HH}z.pgrb2a.0p50.f{FFF}   (control)
         gep{NN}.t{HH}z.pgrb2a.0p50.f{FFF}  (perturbed, NN=01-30)

GEFS GEFSv12 notes (post-2024 NOAA reorg):
  - pgrb2ap5 = primary product at 0.5° resolution, 3-hourly to f240, then 6-hourly to f384
  - pgrb2sp25 (0.25°) now contains ONLY ensemble stats (geavg/gespr), no per-member
  - Each file ~5 MB; we use byte-range reads on the .idx sidecar to download only
    the 2m TMP field (~100 KB/file)
  - Members: c00 (control) + p01-p30 (perturbed) = 31 total
"""
from __future__ import annotations

import logging
import os
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

# Lead steps in hours; 3-hourly, limited to cover D+1 through D+3 from a 12z init
# D+1 noon ≈ f24; D+3 noon ≈ f72; pad a day each side for timezone buffering
_STEPS = list(range(6, 97, 3))  # f006 to f096, 3-hourly


def ingest_forecasts(
    init_dt: datetime,
    station: StationConfig,
    steps: list[int] | None = None,
) -> pl.DataFrame:
    """Fetch 31-member GEFS 2m temperature and return a forecast DataFrame.

    Uses byte-range reads (via .idx sidecars) to download only the TMP field
    from each GRIB2 file rather than the full ~17 MB per file.

    Args:
        steps: Forecast lead hours to download. Defaults to _STEPS (f006–f096, 3-hourly).
               Pass [24] for a lightweight single-step backfill.

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
    base = f"{_BUCKET}/gefs.{date_str}/{hour_str}/atmos"

    # GEFSv12 product hierarchy (per-member files only):
    #   pgrb2ap5 — subset A, 0.5°: select variables (pressure levels, precip, MSLP)
    #   pgrb2bp5 — subset B, 0.5°: extended variables including 2m TMP and 10m winds
    # pgrb2sp25 (0.25°) carries only ensemble stats (geavg/gespr) — skip per-member.
    # Try both A and B; 2m TMP may be in B only depending on the init cycle.
    # Tuple: (subdir, filename suffix, resolution token)
    _PRODUCTS = [
        ("pgrb2ap5", "pgrb2a", "0p50"),
        ("pgrb2bp5", "pgrb2b", "0p50"),
    ]

    steps_to_fetch = steps if steps is not None else _STEPS

    # {member_id: {local_date: max_temp_c}}
    day_max: dict[int, dict[date, float]] = {m: {} for m in range(31)}

    with tempfile.TemporaryDirectory() as tmpdir:
        for step in steps_to_fetch:
            valid_utc = init_dt.replace(tzinfo=timezone.utc) + timedelta(hours=step)
            local_day = valid_utc.astimezone(tz).date()
            step_str = f"{step:03d}"

            for member_id in range(31):
                mem_pfx = "gec00" if member_id == 0 else f"gep{member_id:02d}"
                tmp_path = Path(tmpdir) / f"m{member_id:02d}_f{step_str}.grib2"

                temp_c = None
                for prod_dir, prod_sfx, prod_res in _PRODUCTS:
                    fname = f"{mem_pfx}.t{hour_str}z.{prod_sfx}.{prod_res}.f{step_str}"
                    key = f"{base}/{prod_dir}/{fname}"
                    temp_c = _fetch_tmp_field(fs, key, tmp_path, station.lat, station.lon)
                    if temp_c is not None:
                        break  # found data in this product, no need to try fallback

                if temp_c is not None:
                    prev = day_max[member_id].get(local_day, -999.0)
                    day_max[member_id][local_day] = max(prev, temp_c)

    now = datetime.now(timezone.utc)
    rows: list[dict[str, Any]] = []
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
        raise IngestError(
            f"GEFS: no data retrieved for {init_dt:%Y-%m-%dT%Hz} "
            f"(tried products: {[p[0] for p in _PRODUCTS]})"
        )

    return pl.DataFrame(rows)


# ─── Byte-range field extraction ──────────────────────────────────────────────

def _fetch_tmp_field(
    fs: Any,
    key: str,
    tmp_path: Path,
    lat: float,
    lon: float,
) -> float | None:
    """Download only the TMP 2m field from a GEFS GRIB2 file via byte-range read.

    Uses the .idx sidecar to locate the byte range, then reads just that GRIB
    message. Falls back to full-file download if the index is unavailable.
    """
    byte_start, byte_end = _find_tmp_bytes(fs, key)

    if byte_start is None:
        # Fall back: try downloading the full file
        try:
            fs.get(key, str(tmp_path))
        except Exception as exc:
            _logger.debug("GEFS skip %s: %s", key, exc)
            return None
    else:
        try:
            with fs.open(key, "rb") as remote:
                remote.seek(byte_start)
                chunk = remote.read(byte_end - byte_start if byte_end else -1)
            tmp_path.write_bytes(chunk)
        except Exception as exc:
            _logger.debug("GEFS byte-range read failed %s: %s", key, exc)
            return None

    return _extract_temp(tmp_path, lat, lon)


def _find_tmp_bytes(fs: Any, key: str) -> tuple[int | None, int | None]:
    """Parse .idx sidecar to find byte start/end of the TMP 2m field.

    Returns (None, None) if the index cannot be read or TMP not found.
    """
    idx_key = key + ".idx"
    try:
        with fs.open(idx_key, "r") as f:
            lines = f.read().strip().split("\n")
    except Exception:
        return None, None

    entries: list[tuple[int, str]] = []
    for line in lines:
        parts = line.split(":", 2)
        if len(parts) >= 2:
            try:
                entries.append((int(parts[1]), line))
            except ValueError:
                pass

    for i, (offset, desc) in enumerate(entries):
        if "TMP" in desc and "2 m above ground" in desc:
            byte_start = offset
            byte_end = entries[i + 1][0] if i + 1 < len(entries) else None
            return byte_start, byte_end

    return None, None


# ─── GRIB2 interpolation ──────────────────────────────────────────────────────

def _extract_temp(grib_path: Path, lat: float, lon: float) -> float | None:
    """Bilinear-interpolate 2m temperature from a GRIB2 file to station lat/lon.

    Tries a type+level filter first (works for both ECMWF and NCEP/GEFS files).
    Falls back to unfiltered open if that fails (e.g. multi-message byte-range read).
    Does NOT filter on shortName — "2t" is ECMWF notation and silently rejects GEFS.
    """
    if not grib_path.exists() or grib_path.stat().st_size == 0:
        return None

    ds = None
    # Filter by level type+value only — catches both "2t" (ECMWF) and "TMP" (NCEP)
    for filt in [
        {"typeOfLevel": "heightAboveGround", "level": 2},
        {},  # last resort: take first variable in file
    ]:
        try:
            ds = xr.open_dataset(
                grib_path,
                engine="cfgrib",
                backend_kwargs={"filter_by_keys": filt} if filt else {},
            )
            if ds.data_vars:
                break
            ds.close()
            ds = None
        except Exception as exc:
            _logger.debug("cfgrib open failed (filter=%s): %s", filt, exc)
            ds = None

    if ds is None:
        _logger.debug("cfgrib could not open %s", grib_path.name)
        return None

    # Variable name: "t2m" (CF standard) or first available
    var_name = "t2m" if "t2m" in ds else next(iter(ds.data_vars), None)
    if var_name is None:
        ds.close()
        return None

    # GEFS uses 0–360 longitude
    lon_360 = lon % 360.0
    try:
        val = ds[var_name].interp(latitude=lat, longitude=lon_360, method="linear")
        temp_c = float(val.values) - 273.15
        return temp_c if np.isfinite(temp_c) else None
    except Exception as exc:
        _logger.debug("GEFS interpolation failed at (%.3f, %.3f): %s", lat, lon_360, exc)
        return None
    finally:
        ds.close()


# ─── Helpers ──────────────────────────────────────────────────────────────────

def _lead_hours(init_dt: datetime, valid_date: date, tz: zoneinfo.ZoneInfo) -> int:
    noon_local = datetime(valid_date.year, valid_date.month, valid_date.day, 12, tzinfo=tz)
    delta = noon_local.astimezone(timezone.utc) - init_dt.replace(tzinfo=timezone.utc)
    return int(round(delta.total_seconds() / 3600 / 24) * 24)
