"""Build a single JSON snapshot of pipeline state for external analysis.

The dump bundles config, runtime mode, bankroll, per-station performance,
recent picks/resolutions/executions, EMOS/QRF calibration state, a small
forecast-cache inventory and a tail of the JSONL event log. It deliberately
omits raw forecast/observation parquet (too big and redundant) and never
captures secrets (POLYMARKET_PK, CLOB_*, TELEGRAM_*).

Use:
    from weather_edge.pipeline.dump import build_state_dump
    path = build_state_dump(["EGLC", ...], days=30)
"""
from __future__ import annotations

import json
import logging
import os
import subprocess
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from weather_edge.store.parquet import _dump_json

_DATA_DIR = Path(__file__).parents[3] / "data"
_LOGS_DIR = Path(__file__).parents[3] / "logs"
_logger = logging.getLogger(__name__)

# Forecast models in use by the ingest pipeline (matches data/forecasts/model=*).
_MODELS = ("GEFS", "ECMWF", "ICON")


def _git_sha() -> str | None:
    try:
        out = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=Path(__file__).parents[3],
            capture_output=True,
            text=True,
            timeout=2,
        )
        if out.returncode == 0:
            return out.stdout.strip()
    except Exception:
        pass
    return None


def _package_version() -> str | None:
    try:
        from importlib.metadata import version
        return version("weather-edge")
    except Exception:
        return None


def _mode() -> str:
    if os.getenv("TRADING_ENABLED", "false").lower() != "true":
        return "off"
    live_stations = [s for s in os.getenv("LIVE_STATIONS", "").split(",") if s.strip()]
    if live_stations:
        return "partial"
    if os.getenv("LIVE_TRADING", "false").lower() == "true":
        return "live"
    return "dryrun"


def _runtime_block() -> dict[str, Any]:
    raw_whitelist = [s.strip().upper() for s in os.getenv("LIVE_STATIONS", "").split(",") if s.strip()]
    return {
        "mode": _mode(),
        "live_whitelist": raw_whitelist,
        "trading_enabled": os.getenv("TRADING_ENABLED", "false").lower() == "true",
        "live_trading": os.getenv("LIVE_TRADING", "false").lower() == "true",
    }


def _bankroll_block() -> dict[str, Any]:
    from weather_edge.execution import bankroll as br
    out: dict[str, Any] = {}
    try:
        out["live"] = br.load()
    except Exception as exc:
        out["live"] = {"error": str(exc)}
    # Per-mode dry bankrolls. Check existence first because load_dry() would
    # auto-init to $100 on first call, and a dump should never mutate state.
    dry: dict[str, Any] = {}
    for mode in br.DRY_MODES:
        path = br._dry_path(mode)
        if not path.exists():
            dry[mode] = None
            continue
        try:
            dry[mode] = br.load_dry(mode=mode)
        except Exception as exc:
            dry[mode] = {"error": str(exc)}
    # Pre-split snapshot if the migration was run — useful for historic compare.
    archive = _DATA_DIR / "dry_bankroll.pre_split.bak.json"
    if archive.exists():
        try:
            dry["pre_split_archive"] = json.loads(archive.read_text())
        except Exception as exc:
            dry["pre_split_archive"] = {"error": str(exc)}
    out["dry"] = dry
    return out


def _config_block(stations: list[str]) -> dict[str, Any]:
    from weather_edge.config import get_station, load_thresholds
    cfg_stations = []
    for sid in stations:
        try:
            cfg_stations.append(get_station(sid).model_dump())
        except Exception as exc:
            cfg_stations.append({"icao": sid, "error": str(exc)})
    try:
        thresholds = load_thresholds().model_dump()
    except Exception as exc:
        thresholds = {"error": str(exc)}
    return {"stations": cfg_stations, "thresholds": thresholds}


def _filter_by_date(records: list[dict[str, Any]], start: date, end: date, key: str) -> list[dict[str, Any]]:
    """Keep records whose `key` (date string or date object) falls in [start, end]."""
    out = []
    for r in records:
        v = r.get(key)
        if v is None:
            continue
        if isinstance(v, str):
            try:
                d = date.fromisoformat(v[:10])
            except ValueError:
                continue
        elif isinstance(v, date):
            d = v
        else:
            continue
        if start <= d <= end:
            out.append(r)
    return out


def _summarise_snapshot(snap: dict[str, Any]) -> dict[str, Any]:
    """Trim a market snapshot to the analysable bits."""
    outs = []
    for o in snap.get("outcomes", []) or []:
        outs.append({
            "label": o.get("label"),
            "low": o.get("low"),
            "high": o.get("high"),
            "best_bid": o.get("best_bid"),
            "best_ask": o.get("best_ask"),
            "mid": o.get("mid"),
            "spread": o.get("spread"),
            "liquidity": o.get("liquidity"),
        })
    return {
        "captured_at": snap.get("captured_at") or snap.get("timestamp"),
        "outcomes": outs,
    }


def _history_block(stations: list[str], start: date, end: date) -> dict[str, Any]:
    """Per-station history bundle keyed by lock strategy (bma/intraday/peak).

    Resolutions and market snapshots are shared across modes (the resolved
    bracket and the market state don't depend on which strategy bet on it),
    so those live at the station root. Picks, executions, and settlement
    markers split per-mode so offline analysis can attribute P&L by strategy.
    """
    from weather_edge.execution import bankroll as br
    from weather_edge.execution.polymarket_exec import load_executions
    from weather_edge.store import parquet as store

    out: dict[str, Any] = {}
    for sid in stations:
        try:
            resolutions = store.read_all_resolutions(sid)
        except Exception as exc:
            resolutions = []
            _logger.warning("read_all_resolutions(%s) failed: %s", sid, exc)
        resolutions_w = _filter_by_date(resolutions, start, end, "date")

        # Picks per mode. Walk read_all_picks(mode=...) and date-filter.
        picks_by_mode: dict[str, list[dict[str, Any]]] = {}
        for mode in br.DRY_MODES:
            try:
                picks = store.read_all_picks(sid, mode=mode)
            except Exception as exc:
                picks = []
                _logger.warning("read_all_picks(%s, %s) failed: %s", sid, mode, exc)
            # Tag each pick with its mode for downstream analysis convenience.
            for p in picks:
                p.setdefault("_mode", mode)
            picks_by_mode[mode] = (
                _filter_by_date(picks, start, end, "target_date")
                or _filter_by_date(picks, start, end, "date")
            )

        executions_by_mode: dict[str, list[dict[str, Any]]] = {m: [] for m in br.DRY_MODES}
        settlements_by_mode: dict[str, list[dict[str, Any]]] = {m: [] for m in br.DRY_MODES}
        snapshots: list[dict[str, Any]] = []
        n_days = (end - start).days + 1
        for offset in range(n_days):
            d = start + timedelta(days=offset)
            for mode in br.DRY_MODES:
                try:
                    for e in load_executions(sid, d, mode=mode):
                        executions_by_mode[mode].append({**e, "_date": d.isoformat(), "_mode": mode})
                except Exception as exc:
                    _logger.warning("load_executions(%s, %s, %s) failed: %s", sid, d, mode, exc)
                marker = (
                    _DATA_DIR / "executions" / f"station={sid}"
                    / f"date={d}" / f"_settled_{mode}.json"
                )
                if marker.exists():
                    try:
                        settlements_by_mode[mode].append({
                            "date": d.isoformat(),
                            **json.loads(marker.read_text()),
                        })
                    except Exception as exc:
                        _logger.warning("settled marker read failed (%s %s %s): %s",
                                        sid, d, mode, exc)
            try:
                snap = store.read_market_snapshot(sid, d)
            except Exception:
                snap = None
            if isinstance(snap, dict):
                trimmed = _summarise_snapshot(snap)
                trimmed["date"] = d.isoformat()
                snapshots.append(trimmed)

        out[sid] = {
            "picks": picks_by_mode,
            "resolutions": resolutions_w,
            "executions": executions_by_mode,
            "settlements": settlements_by_mode,
            "market_snapshots_sample": snapshots,
        }
    return out


def _calibration_block(stations: list[str]) -> dict[str, Any]:
    from weather_edge.store import parquet as store

    now = datetime.now(timezone.utc)
    out: dict[str, Any] = {}
    for sid in stations:
        # Latest pooled EMOS at lead 24h.
        try:
            emos_latest = store.read_emos_params(sid, lead_hours=24, as_of=now)
        except Exception as exc:
            emos_latest = {"error": str(exc)}

        # History: walk the pooled directory and read up to last 30 timestamped files.
        history: list[dict[str, Any]] = []
        emos_dir = _DATA_DIR / "emos_params" / f"station={sid}" / "lead_hours=24" / "pooled"
        if emos_dir.exists():
            files = sorted(
                f for f in emos_dir.glob("*.json")
                if not f.name.endswith(".tmp") and ".corrupt-" not in f.name
            )
            for f in files[-30:]:
                try:
                    data = json.loads(f.read_text())
                except Exception:
                    continue
                history.append({"file": f.name, "params": data})

        # QRF metadata only (forest is a pickle, too big and not analysable as JSON).
        qrf_meta: dict[str, Any] | None = None
        qrf_dir = _DATA_DIR / "qrf_params" / f"station={sid}" / "lead_hours=24"
        if qrf_dir.exists():
            metas = sorted(
                f for f in qrf_dir.glob("*.json")
                if not f.name.endswith(".tmp") and ".corrupt-" not in f.name
            )
            if metas:
                try:
                    qrf_meta = json.loads(metas[-1].read_text())
                    qrf_meta["_file"] = metas[-1].name
                except Exception:
                    qrf_meta = None

        out[sid] = {
            "emos_pooled_latest": emos_latest,
            "emos_pooled_history": history,
            "qrf_meta": qrf_meta,
        }
    return out


def _forecast_cache_inventory(stations: list[str], start: date, end: date) -> dict[str, Any]:
    """Count cached forecast init_dts per (model, station) within window. No raw data."""
    out: dict[str, Any] = {}
    for model in _MODELS:
        model_dir = _DATA_DIR / "forecasts" / f"model={model}"
        per_station: dict[str, Any] = {}
        if not model_dir.exists():
            out[model] = per_station
            continue
        for sid in stations:
            init_dts: list[str] = []
            for date_dir in sorted(model_dir.glob("init_date=*")):
                date_str = date_dir.name.split("=", 1)[1]
                try:
                    d = date.fromisoformat(date_str)
                except ValueError:
                    continue
                if not (start <= d <= end):
                    continue
                for hour_dir in sorted(date_dir.glob("init_hour=*")):
                    parquet = hour_dir / f"station={sid}" / "data.parquet"
                    if parquet.exists():
                        init_dts.append(f"{date_str}T{hour_dir.name.split('=', 1)[1]}:00")
            per_station[sid] = {
                "n_init_dts_in_window": len(init_dts),
                "latest_init_dt": init_dts[-1] if init_dts else None,
            }
        out[model] = per_station
    return out


def _logs_tail(days: int = 7, max_events: int = 2000) -> list[dict[str, Any]]:
    if not _LOGS_DIR.exists():
        return []
    today = datetime.now(timezone.utc).date()
    events: list[dict[str, Any]] = []
    for offset in range(days, -1, -1):  # oldest → newest
        d = today - timedelta(days=offset)
        path = _LOGS_DIR / f"{d}.jsonl"
        if not path.exists():
            continue
        try:
            with open(path) as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        events.append(json.loads(line))
                    except json.JSONDecodeError:
                        continue
        except OSError:
            continue
    return events[-max_events:]


def _backtest_latest(stations: list[str]) -> dict[str, Any]:
    """Per-station summary of the on-disk backtest_results parquet (full history, not windowed)."""
    import polars as pl
    out: dict[str, Any] = {}
    for sid in stations:
        path = _DATA_DIR / "backtest_results" / f"station={sid}" / "results.parquet"
        if not path.exists():
            out[sid] = None
            continue
        try:
            df = pl.read_parquet(path)
        except Exception as exc:
            out[sid] = {"error": str(exc)}
            continue
        if df.is_empty():
            out[sid] = {"n_rows": 0}
            continue
        bets = df.filter(pl.col("pnl").is_not_null()) if "pnl" in df.columns else df
        n = bets.height
        if n == 0 or "pnl" not in bets.columns:
            out[sid] = {"n_rows": int(df.height)}
            continue
        pnl_total = float(bets["pnl"].sum() or 0.0)
        staked = float(bets["entry_mid"].sum() or 0.0) if "entry_mid" in bets.columns else 0.0
        wins = int((bets["pnl"] > 0).sum())
        date_min = df["date"].min() if "date" in df.columns else None
        date_max = df["date"].max() if "date" in df.columns else None
        out[sid] = {
            "n_rows": int(df.height),
            "n_bets": n,
            "wins": wins,
            "win_rate": wins / n if n else None,
            "pnl": pnl_total,
            "staked": staked,
            "roi": (pnl_total / staked) if staked > 0 else None,
            "date_min": str(date_min) if date_min is not None else None,
            "date_max": str(date_max) if date_max is not None else None,
        }
    return out


def _decorate_bucket(b: dict[str, float | int]) -> dict[str, Any]:
    n = int(b["n"])
    staked = float(b["staked"])
    return {
        "n": n,
        "wins": int(b["wins"]),
        "staked": staked,
        "pnl": float(b["pnl"]),
        "win_rate": (int(b["wins"]) / n) if n else None,
        "roi": (float(b["pnl"]) / staked) if staked > 0 else None,
        "mean_clv": (float(b["clv_sum"]) / int(b["clv_n"])) if int(b["clv_n"]) > 0 else None,
        "clv_n": int(b["clv_n"]),
    }


def _sum_buckets(*buckets: dict) -> dict:
    keys = ("n", "wins", "staked", "pnl", "clv_sum", "clv_n")
    out = {k: 0 for k in keys}
    for b in buckets:
        for k in keys:
            out[k] += b.get(k, 0)
    return out


def _performance_block(stations: list[str], days: int) -> dict[str, Any]:
    """All-modes combined per-station/totals view (back-compat with old dumps).

    Pairs with `_performance_by_mode_block` below — they read the same
    underlying data, just aggregated differently. Keep both so an analyst can
    diff total bot performance against per-strategy attribution in one pass.
    """
    from weather_edge.pipeline.reporting import station_breakdown
    raw = station_breakdown(stations, days)

    per_station: dict[str, Any] = {}
    totals_live = _sum_buckets()
    totals_dry = _sum_buckets()
    for sid, d in raw.items():
        live = d["live"]
        dry = d["dry"]
        combined = _sum_buckets(live, dry)
        per_station[sid] = {
            "live": _decorate_bucket(live),
            "dry": _decorate_bucket(dry),
            "combined": _decorate_bucket(combined),
        }
        totals_live = _sum_buckets(totals_live, live)
        totals_dry = _sum_buckets(totals_dry, dry)

    return {
        "per_station": per_station,
        "totals": {
            "live": _decorate_bucket(totals_live),
            "dry": _decorate_bucket(totals_dry),
            "combined": _decorate_bucket(_sum_buckets(totals_live, totals_dry)),
        },
    }


def _performance_by_mode_block(stations: list[str], days: int) -> dict[str, Any]:
    """Per-mode performance attribution: the heart of the three-mode comparison.

    Returns {per_station: {sid: {mode: {live, dry, combined}}},
             totals_by_mode: {mode: {live, dry, combined}}}.
    """
    from weather_edge.execution import bankroll as br
    from weather_edge.pipeline.reporting import station_breakdown_by_mode

    raw = station_breakdown_by_mode(stations, days)

    per_station: dict[str, Any] = {}
    totals_by_mode: dict[str, dict] = {
        m: {"live": _sum_buckets(), "dry": _sum_buckets()} for m in br.DRY_MODES
    }
    for sid, modes in raw.items():
        per_station[sid] = {}
        for mode in br.DRY_MODES:
            live = modes[mode]["live"]
            dry = modes[mode]["dry"]
            per_station[sid][mode] = {
                "live": _decorate_bucket(live),
                "dry": _decorate_bucket(dry),
                "combined": _decorate_bucket(_sum_buckets(live, dry)),
            }
            totals_by_mode[mode]["live"] = _sum_buckets(totals_by_mode[mode]["live"], live)
            totals_by_mode[mode]["dry"] = _sum_buckets(totals_by_mode[mode]["dry"], dry)

    decorated_totals: dict[str, Any] = {}
    for mode in br.DRY_MODES:
        live = totals_by_mode[mode]["live"]
        dry = totals_by_mode[mode]["dry"]
        decorated_totals[mode] = {
            "live": _decorate_bucket(live),
            "dry": _decorate_bucket(dry),
            "combined": _decorate_bucket(_sum_buckets(live, dry)),
        }

    return {"per_station": per_station, "totals_by_mode": decorated_totals}


def build_state_dump(
    stations: list[str],
    days: int = 30,
    out_path: Path | None = None,
) -> Path:
    """Walk pipeline state and write a single JSON dump. Returns path."""
    now = datetime.now(timezone.utc)
    end = now.date() - timedelta(days=1)  # last fully resolved day
    start = end - timedelta(days=days - 1)

    if out_path is None:
        dump_dir = _DATA_DIR / "dumps"
        dump_dir.mkdir(parents=True, exist_ok=True)
        out_path = dump_dir / f"dump_{now.strftime('%Y-%m-%d_%H%M%S')}.json"

    payload: dict[str, Any] = {
        "meta": {
            "generated_at": now.isoformat(),
            "weather_edge_version": _package_version(),
            "git_sha": _git_sha(),
            "window_days": days,
            "window_start": start.isoformat(),
            "window_end": end.isoformat(),
            "stations": list(stations),
        },
        "config": _config_block(stations),
        "runtime": _runtime_block(),
        "bankroll": _bankroll_block(),
        "performance": _performance_block(stations, days),
        "performance_by_mode": _performance_by_mode_block(stations, days),
        "history": _history_block(stations, start, end),
        "calibration": _calibration_block(stations),
        "forecast_cache_inventory": _forecast_cache_inventory(stations, start, end),
        "logs_tail": _logs_tail(),
        "backtest_latest": _backtest_latest(stations),
    }

    _dump_json(payload, out_path)
    return out_path
