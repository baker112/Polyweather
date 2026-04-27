"""Backtest replays historical dates through the same lock_picks code path.

Critical invariant: EMOS params used for date T must have valid_from < T.
The same is enforced by read_emos_params(as_of=lock_time_for_T).

Produces a deterministic parquet result if run twice over the same date range.
"""
from __future__ import annotations

import logging
from datetime import date, datetime, timedelta, timezone
from typing import Any

import polars as pl

from weather_edge.config import get_station, load_thresholds
from weather_edge.exceptions import AlreadyLockedError, IngestError
from weather_edge.pipeline.lock import lock_picks
from weather_edge.store import parquet as store

_logger = logging.getLogger(__name__)


def backtest(
    station_id: str,
    start: date,
    end: date,
    lock_hour_utc: int = 18,
) -> pl.DataFrame:
    """Walk every date in [start, end] and score each pick.

    Returns a DataFrame with one row per date:
    date, station, mu, sigma, pick_label, pick_side, entry_mid,
    observed_daily_max, bracket_hit, pnl, crps
    """
    station = get_station(station_id)
    results: list[dict[str, Any]] = []

    current = start
    while current <= end:
        # Operationally the pipeline runs the evening BEFORE target_date to
        # predict D+1. So init_dt = (current - 1 day) at 12z, giving lead=24h.
        # Setting lock_time one day earlier achieves this via _most_recent_12z.
        lock_time = datetime(
            current.year, current.month, current.day,
            lock_hour_utc, 0, 0, tzinfo=timezone.utc,
        ) - timedelta(days=1)
        _logger.info("Backtest %s %s", station_id, current)

        try:
            picks_result = lock_picks(current, station_id, now_utc=lock_time)
        except AlreadyLockedError:
            # Already ran — reload and continue to scoring
            picks_result = _load_existing_picks(station_id, current)
        except (IngestError, Exception) as exc:
            _logger.warning("lock_picks failed for %s %s: %s", station_id, current, exc)
            current += timedelta(days=1)
            continue

        # Score against observed daily max
        obs_df = store.read_observations(station_id, current, current)
        if obs_df.is_empty():
            current += timedelta(days=1)
            continue

        observed = float(obs_df["daily_max_c"][0])
        from weather_edge.postprocess.crps import crps_gaussian
        score = crps_gaussian(picks_result.mu, picks_result.sigma, observed)

        for pick in picks_result.picks:
            hit = _bracket_hit(pick.low, pick.high, observed)
            # P&L: cost = entry_mid (as fraction of $1), payout = 1 if hit else 0
            pnl = (1.0 if hit else 0.0) - pick.market_prob
            results.append({
                "date": current,
                "station": station_id,
                "mu": picks_result.mu,
                "sigma": picks_result.sigma,
                "pick_label": pick.bracket_label,
                "pick_side": pick.side,
                "entry_mid": pick.market_prob,
                "edge_at_entry": pick.edge,
                "observed_daily_max": observed,
                "bracket_hit": int(hit),
                "pnl": pnl,
                "crps": score,
            })

        if not picks_result.picks:
            # No pick — record the model output anyway for calibration
            results.append({
                "date": current,
                "station": station_id,
                "mu": picks_result.mu,
                "sigma": picks_result.sigma,
                "pick_label": None,
                "pick_side": None,
                "entry_mid": None,
                "edge_at_entry": None,
                "observed_daily_max": observed,
                "bracket_hit": None,
                "pnl": None,
                "crps": score,
            })

        current += timedelta(days=1)

    if not results:
        return pl.DataFrame()

    df = pl.DataFrame(results)
    store.write_backtest_result(df, station_id)
    return df


def _bracket_hit(low: float | None, high: float | None, observed: float) -> bool:
    above_low = (low is None) or (observed >= low)
    below_high = (high is None) or (observed < high)
    return above_low and below_high


def _load_existing_picks(station_id: str, target_date: date) -> Any:
    import json
    from pathlib import Path
    from weather_edge.models import LockedPicks

    picks_path = (
        Path(__file__).parent.parent.parent.parent
        / "data" / "picks"
        / f"date={target_date}" / f"station={station_id}" / "picks.json"
    )
    with open(picks_path) as f:
        data = json.load(f)
    return LockedPicks(**data)
