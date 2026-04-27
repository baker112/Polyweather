"""ECMWF Open Data ingestion — HRES + 51-member ENS at 0.25°.

Uses the ecmwf-opendata Python client to download GRIB2 files, then extracts
2m temperature at the station lat/lon using bilinear interpolation.

Member schema:
  - HRES: member_id = 0 (deterministic)
  - ENS: member_id = 0 (control) + 1-50 (perturbed)
"""
from __future__ import annotations

import logging
import tempfile
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import polars as pl
import xarray as xr
import zoneinfo

from weather_edge.config import StationConfig
from weather_edge.exceptions import IngestError

_logger = logging.getLogger(__name__)
_MODEL = "ecmwf"

# Lead steps to request; covers D+1 from either 00z (~36h) or 12z (~24h) run
_STEPS_HRES = list(range(0, 61, 3))
_STEPS_ENS = list(range(0, 61, 6))


def ingest_forecasts(init_dt: datetime, station: StationConfig) -> pl.DataFrame:
    """Fetch ECMWF HRES + ENS and return a forecast DataFrame.

    Columns: model, member_id, init_datetime, valid_date, station, daily_max_c, lead_hours
    """
    try:
        from ecmwf.opendata import Client  # type: ignore[import-untyped]
    except ImportError as exc:
        raise IngestError("ecmwf-opendata not installed") from exc

    client = Client(source="ecmwf")
    tz = zoneinfo.ZoneInfo(station.timezone)
    rows: list[dict[str, Any]] = []

    with tempfile.TemporaryDirectory() as tmpdir:
        # ── HRES ──────────────────────────────────────────────────────────────
        hres_path = Path(tmpdir) / "hres.grib2"
        try:
            client.retrieve(
                date=init_dt.strftime("%Y%m%d"),
                time=init_dt.hour,
                step=_STEPS_HRES,
                stream="oper",
                type="fc",
                param="2t",
                target=str(hres_path),
            )
            hres_rows = _extract_rows(hres_path, station, tz, init_dt, member_offset=0)
            rows.extend(hres_rows)
            _logger.info("ECMWF HRES: %d member-day rows", len(hres_rows))
        except Exception as exc:
            _logger.warning("ECMWF HRES failed: %s", exc)

        # ── ENS perturbed ─────────────────────────────────────────────────────
        ens_path = Path(tmpdir) / "ens_pf.grib2"
        try:
            client.retrieve(
                date=init_dt.strftime("%Y%m%d"),
                time=init_dt.hour,
                step=_STEPS_ENS,
                stream="enfo",
                type="pf",
                number=list(range(1, 51)),
                param="2t",
                target=str(ens_path),
            )
            ens_rows = _extract_rows(ens_path, station, tz, init_dt, member_offset=None)
            rows.extend(ens_rows)
            _logger.info("ECMWF ENS: %d member-day rows", len(ens_rows))
        except Exception as exc:
            _logger.warning("ECMWF ENS failed: %s", exc)

        # ── ENS control ───────────────────────────────────────────────────────
        cf_path = Path(tmpdir) / "ens_cf.grib2"
        try:
            client.retrieve(
                date=init_dt.strftime("%Y%m%d"),
                time=init_dt.hour,
                step=_STEPS_ENS,
                stream="enfo",
                type="cf",
                param="2t",
                target=str(cf_path),
            )
            cf_rows = _extract_rows(cf_path, station, tz, init_dt, member_offset=0)
            rows.extend(cf_rows)
        except Exception as exc:
            _logger.warning("ECMWF ENS control failed: %s", exc)

    if not rows:
        raise IngestError("ECMWF: no data retrieved for any stream")

    return _to_dataframe(rows, _MODEL)


def _extract_rows(
    grib_path: Path,
    station: StationConfig,
    tz: zoneinfo.ZoneInfo,
    init_dt: datetime,
    member_offset: int | None,
) -> list[dict[str, Any]]:
    """Open a GRIB2 file, bilinear-interpolate to station, compute daily max."""
    try:
        ds = xr.open_dataset(
            grib_path,
            engine="cfgrib",
            backend_kwargs={"filter_by_keys": {"shortName": "2t", "typeOfLevel": "heightAboveGround"}},
        )
    except Exception as exc:
        _logger.warning("cfgrib open failed for %s: %s", grib_path, exc)
        return []

    # Bilinear interpolation to exact station lat/lon
    # xarray uses nearest by default for 0.25° grids; linear is fine for ~28 km cells
    t2m = ds["t2m"].interp(
        latitude=station.lat,
        longitude=station.lon % 360,  # ECMWF uses 0-360
        method="linear",
    ) - 273.15  # K → °C

    rows: list[dict[str, Any]] = []

    has_number = "number" in t2m.dims
    members = t2m.number.values if has_number else [member_offset if member_offset is not None else 0]

    for member_id in members:
        member_data = t2m.sel(number=member_id) if has_number else t2m

        # Group by local calendar day
        day_max: dict[date, float] = {}
        for step_val in member_data.step.values:
            valid_utc = init_dt + _np_td_to_timedelta(step_val)
            local_day = valid_utc.replace(tzinfo=timezone.utc).astimezone(tz).date()
            temp_c = float(member_data.sel(step=step_val).values)
            if not np.isfinite(temp_c):
                continue
            if local_day not in day_max or temp_c > day_max[local_day]:
                day_max[local_day] = temp_c

        for vd, tmax in day_max.items():
            lead = _lead_hours(init_dt, vd, tz)
            rows.append({
                "member_id": int(member_id),
                "valid_date": vd,
                "daily_max_c": tmax,
                "lead_hours": lead,
            })

    return rows


def _np_td_to_timedelta(ns: Any) -> "import datetime.timedelta":  # type: ignore[return]
    from datetime import timedelta
    return timedelta(seconds=int(ns) // 10**9)


def _lead_hours(init_dt: datetime, valid_date: date, tz: zoneinfo.ZoneInfo) -> int:
    from datetime import timedelta
    noon_local = datetime(valid_date.year, valid_date.month, valid_date.day, 12, tzinfo=tz)
    delta = noon_local.astimezone(timezone.utc) - init_dt.replace(tzinfo=timezone.utc)
    return int(round(delta.total_seconds() / 3600 / 24) * 24)


def _to_dataframe(rows: list[dict[str, Any]], model: str) -> pl.DataFrame:
    now = datetime.now(timezone.utc)
    records = [
        {
            "model": model,
            "member_id": r["member_id"],
            "init_datetime": now,  # set below
            "valid_date": r["valid_date"],
            "station": "",  # set below
            "daily_max_c": r["daily_max_c"],
            "lead_hours": r["lead_hours"],
        }
        for r in rows
    ]
    return pl.DataFrame(records)
