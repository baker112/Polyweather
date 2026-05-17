"""Aggregation helpers for resolved-bet performance metrics.

Used by the scheduler's /pnl /topstations /losers commands and by the
/dump command's performance section.
"""
from __future__ import annotations

import json
from datetime import date as _date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from weather_edge.execution.polymarket_exec import load_executions
from weather_edge.store import parquet as _store

_DATA_DIR = Path(__file__).parents[3] / "data"


def pnl_for(execution: dict[str, Any], resolved_label: str) -> float:
    """P&L of a single execution given the bracket that resolved YES."""
    entry = float(execution.get("price", 0) or 0)
    stake = float(execution.get("usdc_stake", 0) or 0)
    if entry <= 0:
        return 0.0
    br_l = execution.get("bracket_label", "?")
    sd = execution.get("side", "?")
    win = (br_l == resolved_label and sd == "YES") or (br_l != resolved_label and sd == "NO")
    return (1.0 / entry - 1) * stake if win else -stake


def _clv_map(station: str, target_date: _date) -> dict[str, float]:
    p = _DATA_DIR / "clv_snapshots" / f"station={station}" / f"{target_date}.json"
    if not p.exists():
        return {}
    try:
        data = json.loads(p.read_text())
    except Exception:
        return {}
    return {o["label"]: float(o["mid"]) for o in data.get("outcomes", [])}


def _empty_bucket() -> dict[str, float | int]:
    return {"n": 0, "wins": 0, "staked": 0.0, "pnl": 0.0, "clv_sum": 0.0, "clv_n": 0}


def _accumulate(bucket: dict, e: dict, label: str, clv_close: dict[str, float]) -> None:
    """Fold one execution into a stats bucket in place. Shared by both
    station_breakdown variants so the per-bet math stays one place."""
    p = pnl_for(e, label)
    bucket["n"] += 1
    if p > 0:
        bucket["wins"] += 1
    bucket["staked"] += float(e.get("usdc_stake", 0) or 0)
    bucket["pnl"] += p
    br_l = e.get("bracket_label", "?")
    sd = e.get("side", "?")
    entry = float(e.get("price", 0) or 0)
    if entry > 0 and br_l in clv_close:
        closing = clv_close[br_l]
        clv = (closing - entry) if sd == "YES" else (entry - closing)
        bucket["clv_sum"] += clv
        bucket["clv_n"] += 1


def station_breakdown(stations: list[str], days: int) -> dict[str, dict]:
    """Aggregate resolved-bet P&L per station over the last `days` days.

    Returns {station: {"live": bucket, "dry": bucket}} where bucket holds
    {n, wins, staked, pnl, clv_sum, clv_n}. CLV is mean per-bet closing-line
    value — set clv_n=0 for stations with no closing snapshots yet.

    Sums across all three lock strategies (bma, intraday, peak). Use
    station_breakdown_by_mode for per-strategy attribution.
    """
    from weather_edge.execution import bankroll as br

    now = datetime.now(timezone.utc)
    out: dict[str, dict] = {sid: {"live": _empty_bucket(), "dry": _empty_bucket()} for sid in stations}
    for days_ago in range(1, days + 1):
        d = (now - timedelta(days=days_ago)).date()
        for sid in stations:
            try:
                rec = _store.read_resolution(sid, d)
            except Exception:
                rec = None
            if not (isinstance(rec, dict) and rec.get("resolved")):
                continue
            label = rec.get("resolved_label", "")
            clv_close = _clv_map(sid, d)
            for mode in br.DRY_MODES:
                try:
                    execs = load_executions(sid, d, mode=mode)
                except Exception:
                    continue
                for e in execs:
                    if not isinstance(e, dict):
                        continue
                    bucket = out[sid]["dry"] if e.get("dry_run") else out[sid]["live"]
                    _accumulate(bucket, e, label, clv_close)
    return out


def station_breakdown_by_mode(stations: list[str], days: int) -> dict[str, dict]:
    """Per-station, per-mode resolved-bet breakdown.

    Returns {station: {mode: {"live": bucket, "dry": bucket}}}. Use this when
    you need to attribute P&L to the specific strategy that placed the bet
    (dump.py, future per-mode reporting).
    """
    from weather_edge.execution import bankroll as br

    now = datetime.now(timezone.utc)
    out: dict[str, dict] = {
        sid: {m: {"live": _empty_bucket(), "dry": _empty_bucket()} for m in br.DRY_MODES}
        for sid in stations
    }
    for days_ago in range(1, days + 1):
        d = (now - timedelta(days=days_ago)).date()
        for sid in stations:
            try:
                rec = _store.read_resolution(sid, d)
            except Exception:
                rec = None
            if not (isinstance(rec, dict) and rec.get("resolved")):
                continue
            label = rec.get("resolved_label", "")
            clv_close = _clv_map(sid, d)
            for mode in br.DRY_MODES:
                try:
                    execs = load_executions(sid, d, mode=mode)
                except Exception:
                    continue
                for e in execs:
                    if not isinstance(e, dict):
                        continue
                    bucket = out[sid][mode]["dry"] if e.get("dry_run") else out[sid][mode]["live"]
                    _accumulate(bucket, e, label, clv_close)
    return out
