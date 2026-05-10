"""Per-station edge gate: auto-pause live trading on bleeding stations.

Pass conditions over the last 30 days:
  1. mean per-bet CLV > 0
  2. EMOS-corrected ECMWF CRPS < raw ECMWF ensemble CRPS

Bootstrap: stations with too-few resolved bets or too-few CRPS days pass through
(don't gate young stations).
"""
from __future__ import annotations

import json
import math
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import numpy as np

from weather_edge.models import EmosParams
from weather_edge.postprocess.bma import _init_dt_for
from weather_edge.postprocess.crps import crps_gaussian

MIN_BETS = 10
MIN_CRPS_DAYS = 14
WINDOW_DAYS = 30
LEAD_HOURS = 24
RAW_SIGMA_FLOOR = 2.0  # K — climo floor when ECMWF ensemble var is tiny

# Auto-promotion threshold for per-station Kelly multiplier (#6).
# Lowered from 50 → 25 on 2026-05-10: at the bot's current ~19% lock rate,
# the 50-bet bar wasn't reachable inside a season. Stations with +CLV across
# 25 resolved bets are statistically distinguishable from random.
PROMOTE_MIN_BETS = 25
PROMOTED_MULTIPLIER = 1.0

_DATA_ROOT = Path(__file__).parents[3] / "data"


def _clv_for_bet(entry: float, side: str, closing_mid: float) -> float:
    return (closing_mid - entry) if side == "YES" else (entry - closing_mid)


def rolling_station_clv(
    station_id: str,
    as_of: date,
    window_days: int = WINDOW_DAYS,
) -> tuple[float, int]:
    """Mean per-bet CLV across settled bets in [as_of - window_days, as_of - 1].

    Returns (mean_clv, n_bets). Re-derives CLV from clv_snapshots so this works
    for old settled records that didn't persist `clv` in their dicts.
    """
    end = as_of - timedelta(days=1)
    start = end - timedelta(days=window_days - 1)
    clvs: list[float] = []

    cur = start
    while cur <= end:
        settled = (
            _DATA_ROOT / "executions" / f"station={station_id}" / f"date={cur}" / "_settled.json"
        )
        clv_snap = _DATA_ROOT / "clv_snapshots" / f"station={station_id}" / f"{cur}.json"
        if not settled.exists() or not clv_snap.exists():
            cur += timedelta(days=1)
            continue
        try:
            srec = json.loads(settled.read_text())
            cdat = json.loads(clv_snap.read_text())
        except Exception:
            cur += timedelta(days=1)
            continue
        closing = {o["label"]: float(o["mid"]) for o in cdat.get("outcomes", [])}
        for r in srec.get("records", []):
            label = r.get("bracket_label")
            entry = float(r.get("entry", 0) or 0)
            side = r.get("side", "")
            if entry <= 0 or label not in closing:
                continue
            clvs.append(_clv_for_bet(entry, side, closing[label]))
        cur += timedelta(days=1)

    if not clvs:
        return (0.0, 0)
    return (float(np.mean(clvs)), len(clvs))


def rolling_crps_advantage(
    station_id: str,
    as_of: date,
    lead_hours: int = LEAD_HOURS,
    window_days: int = WINDOW_DAYS,
) -> tuple[float, float, int] | None:
    """Compute (mean_model_crps, mean_raw_crps, n_days) for ECMWF over the window.

    model_crps: EMOS-corrected ECMWF Gaussian.
    raw_crps:   raw ECMWF ensemble mean with ensemble std (floored).
    Returns None if no overlap days are available.
    """
    from weather_edge.store import parquet as store
    import polars as pl

    end_date = as_of - timedelta(days=1)
    start_date = end_date - timedelta(days=window_days - 1)

    obs_df = store.read_observations(station_id, start_date, end_date)
    if obs_df.is_empty():
        return None

    obs_map: dict[date, float] = {
        row["date"]: row["daily_max_c"]
        for row in obs_df.iter_rows(named=True)
    }

    model_scores: list[float] = []
    raw_scores: list[float] = []

    cur = start_date
    while cur <= end_date:
        if cur not in obs_map:
            cur += timedelta(days=1)
            continue
        y = obs_map[cur]

        raw_params = store.read_emos_params(
            station_id,
            lead_hours,
            as_of=datetime(cur.year, cur.month, cur.day, 18, tzinfo=timezone.utc),
            model="ecmwf",
        )
        if raw_params is None:
            cur += timedelta(days=1)
            continue
        params = EmosParams(**raw_params)

        init_dt = _init_dt_for(cur, lead_hours)
        df = store.read_forecasts("ecmwf", init_dt, station_id)
        if df is None:
            cur += timedelta(days=1)
            continue
        subset = df.filter(
            (pl.col("valid_date") == cur) & (pl.col("lead_hours") == lead_hours)
        )
        if subset.is_empty():
            cur += timedelta(days=1)
            continue

        vals = np.array(subset["daily_max_c"].to_list(), dtype=np.float64)
        ens_mean = float(np.mean(vals))
        ens_var = float(np.var(vals, ddof=1)) if len(vals) > 1 else 0.0

        mu_m = params.a + params.b * ens_mean
        sigma_m = math.sqrt(max(params.c + params.d * ens_var, 1e-8))
        sigma_raw = math.sqrt(max(ens_var, RAW_SIGMA_FLOOR**2))

        try:
            model_scores.append(crps_gaussian(mu_m, sigma_m, y))
            raw_scores.append(crps_gaussian(ens_mean, sigma_raw, y))
        except ValueError:
            pass

        cur += timedelta(days=1)

    if not model_scores:
        return None
    return (float(np.mean(model_scores)), float(np.mean(raw_scores)), len(model_scores))


def all_time_station_clv(station_id: str) -> tuple[float, int]:
    """Mean per-bet CLV across ALL settled bets for the station, ever.

    Used by the auto-promotion check (#6): once a station has accumulated
    PROMOTE_MIN_BETS resolved bets with positive mean CLV, it earns the
    full Kelly multiplier of 1.0.
    """
    base = _DATA_ROOT / "executions" / f"station={station_id}"
    if not base.exists():
        return (0.0, 0)
    clvs: list[float] = []
    for date_dir in sorted(base.iterdir()):
        if not date_dir.is_dir() or not date_dir.name.startswith("date="):
            continue
        cur_str = date_dir.name.removeprefix("date=")
        settled = date_dir / "_settled.json"
        clv_snap = _DATA_ROOT / "clv_snapshots" / f"station={station_id}" / f"{cur_str}.json"
        if not settled.exists() or not clv_snap.exists():
            continue
        try:
            srec = json.loads(settled.read_text())
            cdat = json.loads(clv_snap.read_text())
        except Exception:
            continue
        closing = {o["label"]: float(o["mid"]) for o in cdat.get("outcomes", [])}
        for r in srec.get("records", []):
            label = r.get("bracket_label")
            entry = float(r.get("entry", 0) or 0)
            side = r.get("side", "")
            if entry <= 0 or label not in closing:
                continue
            clvs.append(_clv_for_bet(entry, side, closing[label]))
    if not clvs:
        return (0.0, 0)
    return (float(np.mean(clvs)), len(clvs))


def effective_kelly_multiplier(station_id: str) -> tuple[float, str]:
    """Return (multiplier, reason) for live sizing.

    Promotes the configured stations.yaml value to PROMOTED_MULTIPLIER once a
    station has PROMOTE_MIN_BETS+ resolved bets with positive mean CLV.
    Otherwise returns the configured value verbatim.
    """
    from weather_edge.config import get_station

    cfg = get_station(station_id).kelly_multiplier
    mean_clv, n_bets = all_time_station_clv(station_id)
    if n_bets >= PROMOTE_MIN_BETS and mean_clv > 0 and PROMOTED_MULTIPLIER > cfg:
        return (PROMOTED_MULTIPLIER, f"promoted: n={n_bets}, clv={mean_clv:+.3f}")
    return (cfg, f"config: n={n_bets}, clv={mean_clv:+.3f}")


def station_passes_gate(station_id: str, as_of: date) -> tuple[bool, str]:
    """Return (passed, reason). Passed=True means live trading is allowed."""
    mean_clv, n_bets = rolling_station_clv(station_id, as_of)
    crps = rolling_crps_advantage(station_id, as_of)

    if n_bets < MIN_BETS or crps is None or crps[2] < MIN_CRPS_DAYS:
        n_days = 0 if crps is None else crps[2]
        return (True, f"warmup: n_bets={n_bets}, n_crps_days={n_days}")

    model_crps, raw_crps, n_days = crps
    clv_ok = mean_clv > 0
    crps_ok = model_crps < raw_crps

    summary = (
        f"clv={mean_clv:+.3f} ({n_bets} bets), "
        f"model_crps={model_crps:.2f} vs raw={raw_crps:.2f} ({n_days}d)"
    )
    if clv_ok and crps_ok:
        return (True, f"live: {summary}")
    return (False, f"gated: {summary}")
