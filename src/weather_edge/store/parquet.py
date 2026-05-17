from __future__ import annotations

import json
import logging
import math
import os
import pickle
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from typing import Any

import duckdb
import polars as pl

_DATA_DIR = Path(__file__).parent.parent.parent.parent / "data"
_logger = logging.getLogger(__name__)


def _ensure(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    return path


def _json_default(o: Any) -> Any:
    """JSON encoder for datetimes — emit strict ISO 8601 with `T` separator."""
    if isinstance(o, datetime):
        return o.isoformat()
    if isinstance(o, (date, time, timedelta)):
        return o.isoformat() if not isinstance(o, timedelta) else str(o)
    return str(o)


def _scrub_nonfinite(obj: Any) -> Any:
    """Replace NaN/±Inf floats with None recursively so JSON output is RFC 8259 valid."""
    if isinstance(obj, float):
        return obj if math.isfinite(obj) else None
    if isinstance(obj, dict):
        return {k: _scrub_nonfinite(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_scrub_nonfinite(x) for x in obj]
    return obj


def _dump_json(obj: Any, path: Path) -> None:
    """Atomically write JSON with NaN-scrubbing, strict spec, and ISO datetimes.

    Writes to a sibling .tmp file then os.replaces it onto the target so a
    crash mid-write can't leave a 0-byte or truncated file for the next
    reader. (Same-filesystem rename is atomic on Linux/Windows.)
    """
    tmp = path.with_suffix(path.suffix + ".tmp")
    with open(tmp, "w") as f:
        json.dump(_scrub_nonfinite(obj), f, default=_json_default, indent=2, allow_nan=False)
        f.flush()
        try:
            os.fsync(f.fileno())
        except OSError:
            pass
    os.replace(tmp, path)


def _safe_load_json(path: Path) -> Any | None:
    """Load JSON, returning None for missing / empty / corrupt files.

    Also quarantines a corrupt file by renaming it to `<name>.corrupt-<ts>`
    so the next write is unblocked and the bad bytes are kept for inspection.
    """
    if not path.exists():
        return None
    try:
        if path.stat().st_size == 0:
            raise json.JSONDecodeError("empty file", "", 0)
        with open(path) as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError) as exc:
        ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
        quarantine = path.with_name(f"{path.name}.corrupt-{ts}")
        try:
            os.replace(path, quarantine)
            _logger.warning(
                "Corrupt JSON at %s (%s) — quarantined to %s",
                path, exc, quarantine.name,
            )
        except OSError:
            _logger.warning("Corrupt JSON at %s (%s) — could not quarantine", path, exc)
        return None


# ─── Forecasts ────────────────────────────────────────────────────────────────

def write_forecasts(df: pl.DataFrame, model: str, init_dt: datetime, station: str) -> Path:
    date_str = init_dt.strftime("%Y-%m-%d")
    hour_str = init_dt.strftime("%H")
    path = _ensure(
        _DATA_DIR / "forecasts"
        / f"model={model}"
        / f"init_date={date_str}"
        / f"init_hour={hour_str}"
        / f"station={station}"
    ) / "data.parquet"
    if path.exists():
        existing = pl.read_parquet(path)
        df = pl.concat([existing, df]).unique(subset=["member_id", "valid_date"], keep="last").sort("valid_date")
    df.write_parquet(path)
    return path


def read_forecasts(model: str, init_dt: datetime, station: str) -> pl.DataFrame | None:
    date_str = init_dt.strftime("%Y-%m-%d")
    hour_str = init_dt.strftime("%H")
    path = (
        _DATA_DIR / "forecasts"
        / f"model={model}"
        / f"init_date={date_str}"
        / f"init_hour={hour_str}"
        / f"station={station}"
        / "data.parquet"
    )
    return pl.read_parquet(path) if path.exists() else None


# ─── Observations ─────────────────────────────────────────────────────────────

def write_observations(df: pl.DataFrame, station: str) -> Path:
    path = _ensure(_DATA_DIR / "observations" / f"station={station}") / "data.parquet"
    if path.exists():
        existing = pl.read_parquet(path)
        df = pl.concat([existing, df]).unique(subset=["station", "date"], keep="last").sort("date")
    df.write_parquet(path)
    return path


def read_observations(station: str, start: date, end: date) -> pl.DataFrame:
    path = _DATA_DIR / "observations" / f"station={station}" / "data.parquet"
    if not path.exists():
        return pl.DataFrame()
    return (
        pl.read_parquet(path)
        .filter(pl.col("date").is_between(start, end))
    )


# ─── EMOS params ──────────────────────────────────────────────────────────────
# Phase 2: optional `model` parameter partitions params by source model.
# model=None writes to the pooled path (Phase 1 behaviour).

def write_emos_params(
    record: dict[str, Any],
    station: str,
    lead_hours: int,
    valid_from: datetime,
    model: str | None = None,
) -> Path:
    ts = valid_from.strftime("%Y%m%dT%H%M%S")
    sub = f"model={model}" if model else "pooled"
    path = _ensure(
        _DATA_DIR / "emos_params"
        / f"station={station}"
        / f"lead_hours={lead_hours}"
        / sub
    ) / f"{ts}.json"
    _dump_json(record, path)
    return path


def read_emos_params(
    station: str,
    lead_hours: int,
    as_of: datetime,
    model: str | None = None,
) -> dict[str, Any] | None:
    """Return the most recent EMOS params with valid_from ≤ as_of (critical backtest invariant)."""
    sub = f"model={model}" if model else "pooled"
    base = _DATA_DIR / "emos_params" / f"station={station}" / f"lead_hours={lead_hours}" / sub
    if not base.exists():
        # Fallback: try old path layout (pre-Phase-2 files have no model subdir)
        if model is None:
            base = _DATA_DIR / "emos_params" / f"station={station}" / f"lead_hours={lead_hours}"
            if not base.exists():
                return None
        else:
            return None
    as_of_str = as_of.strftime("%Y%m%dT%H%M%S")
    # Filter out tmp/quarantine sidecars so corrupt files don't shadow good ones.
    candidates = sorted(
        (f for f in base.glob("*.json")
         if not f.name.endswith(".tmp") and ".corrupt-" not in f.name
         and f.stem <= as_of_str),
        key=lambda f: f.stem,
        reverse=True,
    )
    for latest in candidates:
        result = _safe_load_json(latest)
        if isinstance(result, dict):
            return result
    return None


# ─── Predictions ──────────────────────────────────────────────────────────────

def write_prediction(record: dict[str, Any], station: str, valid_date: date) -> Path:
    path = _ensure(
        _DATA_DIR / "predictions" / f"station={station}" / f"date={valid_date}"
    ) / "prediction.json"
    _dump_json(record, path)
    return path


# ─── Market snapshots ─────────────────────────────────────────────────────────

def write_market_snapshot(record: dict[str, Any], station: str, target_date: date) -> Path:
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    path = _ensure(
        _DATA_DIR / "market_snapshots" / f"station={station}" / f"date={target_date}"
    ) / f"{ts}.json"
    _dump_json(record, path)
    return path


def read_market_snapshot(station: str, target_date: date) -> dict[str, Any] | None:
    """Return the most recent cached market snapshot for (station, date), or None."""
    snap_dir = _DATA_DIR / "market_snapshots" / f"station={station}" / f"date={target_date}"
    if not snap_dir.exists():
        return None
    files = sorted(
        f for f in snap_dir.glob("*.json")
        if not f.name.endswith(".tmp") and ".corrupt-" not in f.name
    )
    # Walk newest-first; quarantine + skip any corrupt files instead of crashing.
    for f in reversed(files):
        result = _safe_load_json(f)
        if isinstance(result, dict):
            return result
    return None


# ─── Picks ────────────────────────────────────────────────────────────────────
# `mode` is the lock-strategy identifier ("bma" | "intraday" | "peak"). The
# default "bma" preserves the pre-three-mode-split filename (picks.json) so
# historic picks remain readable and BMA's evening lock doesn't need a
# migration. Non-bma strategies write to picks_<mode>.json alongside it so all
# three modes can co-exist for the same (station, date) without clobbering.

def _picks_filename(mode: str = "bma") -> str:
    return "picks.json" if mode == "bma" else f"picks_{mode}.json"


def picks_exist(station: str, target_date: date, mode: str = "bma") -> bool:
    return (
        _DATA_DIR / "picks" / f"date={target_date}" / f"station={station}" / _picks_filename(mode)
    ).exists()


def write_picks(
    record: dict[str, Any], station: str, target_date: date, mode: str = "bma"
) -> Path:
    from weather_edge.exceptions import AlreadyLockedError
    path_dir = _ensure(_DATA_DIR / "picks" / f"date={target_date}" / f"station={station}")
    path = path_dir / _picks_filename(mode)
    if path.exists():
        raise AlreadyLockedError(
            f"Picks already locked for {station} on {target_date} (mode={mode})"
        )
    _dump_json(record, path)
    return path


def read_picks(
    station: str, target_date: date, mode: str = "bma"
) -> dict[str, Any] | None:
    path = _DATA_DIR / "picks" / f"date={target_date}" / f"station={station}" / _picks_filename(mode)
    result = _safe_load_json(path)
    return result if isinstance(result, dict) else None


def read_all_picks(station: str, mode: str = "bma") -> list[dict[str, Any]]:
    base = _DATA_DIR / "picks"
    records = []
    for p in sorted(base.glob(f"date=*/station={station}/{_picks_filename(mode)}")):
        result = _safe_load_json(p)
        if isinstance(result, dict):
            records.append(result)
    return records


# ─── Resolutions ──────────────────────────────────────────────────────────────

def write_resolution(record: dict[str, Any], station: str, target_date: date) -> Path:
    path = _ensure(_DATA_DIR / "resolutions" / f"station={station}" / f"date={target_date}") / "resolution.json"
    _dump_json(record, path)
    return path


def read_resolution(station: str, target_date: date) -> dict[str, Any] | None:
    path = _DATA_DIR / "resolutions" / f"station={station}" / f"date={target_date}" / "resolution.json"
    result = _safe_load_json(path)
    return result if isinstance(result, dict) else None


def read_all_resolutions(station: str) -> list[dict[str, Any]]:
    base = _DATA_DIR / "resolutions" / f"station={station}"
    if not base.exists():
        return []
    records = []
    for p in sorted(base.glob("date=*/resolution.json")):
        result = _safe_load_json(p)
        if isinstance(result, dict):
            records.append(result)
    return records


# ─── Backtest results ─────────────────────────────────────────────────────────

def write_backtest_result(df: pl.DataFrame, station: str) -> Path:
    path = _ensure(_DATA_DIR / "backtest_results" / f"station={station}") / "results.parquet"
    if path.exists():
        existing = pl.read_parquet(path)
        df = pl.concat([existing, df]).unique(subset=["date", "station"]).sort("date")
    df.write_parquet(path)
    return path


def read_backtest_results(station: str, start: date, end: date) -> pl.DataFrame:
    path = _DATA_DIR / "backtest_results" / f"station={station}" / "results.parquet"
    if not path.exists():
        return pl.DataFrame()
    return (
        pl.read_parquet(path)
        .filter(pl.col("date").is_between(start, end))
    )


# ─── QRF params ───────────────────────────────────────────────────────────────

def write_qrf_params(
    forest: Any,
    X_train: Any,
    y_train: Any,
    meta: dict[str, Any],
    station: str,
    lead_hours: int,
    valid_from: datetime,
) -> Path:
    """Persist QRF forest + training arrays as a pickle alongside a JSON metadata file."""
    ts = valid_from.strftime("%Y%m%dT%H%M%S")
    base = _ensure(_DATA_DIR / "qrf_params" / f"station={station}" / f"lead_hours={lead_hours}")

    payload = {"forest": forest, "X_train": X_train, "y_train": y_train, "meta": meta}
    pkl_path = base / f"{ts}.pkl"
    with open(pkl_path, "wb") as f:
        pickle.dump(payload, f)

    json_path = base / f"{ts}.json"
    _dump_json(meta, json_path)

    return pkl_path


def read_qrf_params(
    station: str,
    lead_hours: int,
    as_of: datetime,
) -> dict[str, Any] | None:
    """Return {'forest', 'X_train', 'y_train', 'meta'} or None (backtest-safe: valid_from ≤ as_of)."""
    base = _DATA_DIR / "qrf_params" / f"station={station}" / f"lead_hours={lead_hours}"
    if not base.exists():
        return None
    as_of_str = as_of.strftime("%Y%m%dT%H%M%S")
    candidates = [f for f in base.glob("*.pkl") if f.stem <= as_of_str]
    if not candidates:
        return None
    latest = max(candidates, key=lambda f: f.stem)
    with open(latest, "rb") as f:
        result: dict[str, Any] = pickle.load(f)
    return result


# ─── DuckDB analytics ─────────────────────────────────────────────────────────

def query(sql: str) -> pl.DataFrame:
    con = duckdb.connect()
    return pl.from_pandas(con.execute(sql).df())
