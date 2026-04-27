from __future__ import annotations

import json
import pickle
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import duckdb
import polars as pl

_DATA_DIR = Path(__file__).parent.parent.parent.parent / "data"


def _ensure(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    return path


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
    with open(path, "w") as f:
        json.dump(record, f, default=str, indent=2)
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
    candidates = [f for f in base.glob("*.json") if f.stem <= as_of_str]
    if not candidates:
        return None
    latest = max(candidates, key=lambda f: f.stem)
    with open(latest) as f:
        result: dict[str, Any] = json.load(f)
    return result


# ─── Predictions ──────────────────────────────────────────────────────────────

def write_prediction(record: dict[str, Any], station: str, valid_date: date) -> Path:
    path = _ensure(
        _DATA_DIR / "predictions" / f"station={station}" / f"date={valid_date}"
    ) / "prediction.json"
    with open(path, "w") as f:
        json.dump(record, f, default=str, indent=2)
    return path


# ─── Market snapshots ─────────────────────────────────────────────────────────

def write_market_snapshot(record: dict[str, Any], station: str, target_date: date) -> Path:
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    path = _ensure(
        _DATA_DIR / "market_snapshots" / f"station={station}" / f"date={target_date}"
    ) / f"{ts}.json"
    with open(path, "w") as f:
        json.dump(record, f, default=str, indent=2)
    return path


# ─── Picks ────────────────────────────────────────────────────────────────────

def picks_exist(station: str, target_date: date) -> bool:
    return (
        _DATA_DIR / "picks" / f"date={target_date}" / f"station={station}" / "picks.json"
    ).exists()


def write_picks(record: dict[str, Any], station: str, target_date: date) -> Path:
    from weather_edge.exceptions import AlreadyLockedError
    path_dir = _ensure(_DATA_DIR / "picks" / f"date={target_date}" / f"station={station}")
    path = path_dir / "picks.json"
    if path.exists():
        raise AlreadyLockedError(f"Picks already locked for {station} on {target_date}")
    with open(path, "w") as f:
        json.dump(record, f, default=str, indent=2)
    return path


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
    with open(json_path, "w") as f:
        json.dump(meta, f, indent=2)

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
