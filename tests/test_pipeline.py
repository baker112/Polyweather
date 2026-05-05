"""End-to-end pipeline tests using frozen fixtures.

These tests bypass real HTTP calls by patching the ingest and market layers.
All data is deterministic: the same inputs always produce the same output.
"""
from __future__ import annotations

import json
import math
import tempfile
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import polars as pl
import pytest

from weather_edge.exceptions import AlreadyLockedError
from weather_edge.models import (
    BracketSpec,
    Candidate,
    EmosParams,
    MarketOutcome,
    MarketSnapshot,
    PredictedDistribution,
)
from weather_edge.postprocess.emos import compute_brackets, predict_pdf

# ─── Fixtures ────────────────────────────────────────────────────────────────

TARGET_DATE = date(2025, 7, 15)
STATION_ID = "EGLL"
NOW_UTC = datetime(2025, 7, 14, 18, 0, 0, tzinfo=timezone.utc)

EMOS_PARAMS = EmosParams(
    a=0.5,
    b=0.95,
    c=0.8,
    d=0.7,
    station=STATION_ID,
    lead_hours=24,
    fitted_at=datetime(2025, 7, 14, 12, 0, tzinfo=timezone.utc),
    training_window_days=60,
    n_samples=58,
    train_crps=1.23,
    valid_from=datetime(2025, 7, 14, 12, 0, tzinfo=timezone.utc),
)

FORECAST_VALUES = [22.0, 23.0, 21.5, 24.0, 22.5, 23.5] * 14  # ~84 members

BRACKETS = [
    BracketSpec(label="Below 19°C", low=None, high=19.0),
    BracketSpec(label="19°C to 22°C", low=19.0, high=22.0),
    BracketSpec(label="22°C to 25°C", low=22.0, high=25.0),
    BracketSpec(label="Above 25°C", low=25.0, high=None),
]


def _make_snapshot(probs: list[float]) -> MarketSnapshot:
    outcomes = [
        MarketOutcome(
            label=b.label,
            low=b.low,
            high=b.high,
            best_bid=p - 0.01,
            best_ask=p + 0.01,
            mid=p,
            spread=0.02,
            liquidity=1500.0,
            top_ask_size=500.0,
            top_bid_size=500.0,
            token_id=f"tok_{i}",
        )
        for i, (b, p) in enumerate(zip(BRACKETS, probs))
    ]
    return MarketSnapshot(
        market_id="mkt_001",
        slug="highest-temperature-in-london-on-2025-07-15",
        station=STATION_ID,
        target_date=TARGET_DATE,
        fetched_at=NOW_UTC,
        outcomes=outcomes,
        implied_sum=sum(probs),
    )


# ─── Unit tests ───────────────────────────────────────────────────────────────

def test_predict_pdf_gives_sensible_distribution() -> None:
    dist = predict_pdf(FORECAST_VALUES, EMOS_PARAMS, TARGET_DATE)
    assert 15.0 < dist.mu < 35.0, f"mu={dist.mu} out of plausible range"
    assert dist.sigma > 0


def test_compute_brackets_sums_to_one() -> None:
    dist = predict_pdf(FORECAST_VALUES, EMOS_PARAMS, TARGET_DATE)
    probs = compute_brackets(dist, BRACKETS)
    total = sum(p.model_prob for p in probs)
    assert abs(total - 1.0) < 1e-9


def test_edge_detection_with_large_edge() -> None:
    """A model prob of 0.65 vs market 0.35 should be a clear YES pick."""
    from weather_edge.pipeline.lock import compute_edges
    from weather_edge.models import BracketProb

    bracket_probs = [
        BracketProb(label="22°C to 25°C", low=22.0, high=25.0, model_prob=0.65),
    ]
    # Market shows 0.35 → edge = 0.30 >> 0.04 threshold
    snapshot = _make_snapshot([0.05, 0.15, 0.35, 0.45])

    candidates = compute_edges(bracket_probs, snapshot, NOW_UTC)
    assert len(candidates) == 1
    assert candidates[0].side == "YES"
    assert candidates[0].edge == pytest.approx(0.30, abs=1e-6)


def test_edge_detection_no_pick_when_spread_too_high() -> None:
    """Wide spread should block the pick even when edge is large."""
    from weather_edge.pipeline.lock import compute_edges
    from weather_edge.models import BracketProb

    bracket_probs = [
        BracketProb(label="22°C to 25°C", low=22.0, high=25.0, model_prob=0.70),
    ]
    snapshot = _make_snapshot([0.05, 0.15, 0.35, 0.45])

    # Override spread to 0.10 (> MAX_SPREAD=0.04)
    snapshot.outcomes[2] = MarketOutcome(
        **{**snapshot.outcomes[2].model_dump(), "spread": 0.10, "best_bid": 0.30, "best_ask": 0.40}
    )

    candidates = compute_edges(bracket_probs, snapshot, NOW_UTC)
    assert len(candidates) == 0


def test_edge_detection_no_pick_when_stale_market() -> None:
    """Market fetched > 10 minutes ago should be rejected."""
    from datetime import timedelta
    from weather_edge.pipeline.lock import compute_edges
    from weather_edge.models import BracketProb

    bracket_probs = [
        BracketProb(label="22°C to 25°C", low=22.0, high=25.0, model_prob=0.70),
    ]
    stale_time = NOW_UTC - timedelta(minutes=20)
    snapshot = _make_snapshot([0.05, 0.15, 0.35, 0.45])
    snapshot = MarketSnapshot(**{**snapshot.model_dump(), "fetched_at": stale_time})

    candidates = compute_edges(bracket_probs, snapshot, NOW_UTC)
    assert len(candidates) == 0


def test_lock_picks_idempotency() -> None:
    """Calling lock_picks twice raises AlreadyLockedError on the second call."""
    import importlib
    import weather_edge.store.parquet as store_mod

    with tempfile.TemporaryDirectory() as tmpdir:
        # Patch the DATA_DIR inside the store module
        tmp_path = Path(tmpdir) / "data"
        tmp_path.mkdir()

        frozen_dist = predict_pdf(FORECAST_VALUES, EMOS_PARAMS, TARGET_DATE)
        with (
            patch.object(store_mod, "_DATA_DIR", tmp_path),
            patch("weather_edge.pipeline.lock._load_or_fetch_forecasts") as mock_fetch,
            # Patch _stage3_4 directly (Phase 2 replaced _get_emos_params with this)
            patch("weather_edge.pipeline.lock._stage3_4", return_value=frozen_dist),
            patch("weather_edge.pipeline.lock._fetch_and_persist_market", new_callable=AsyncMock) as mock_market,
            patch("weather_edge.pipeline.lock._most_recent_12z",
                  return_value=datetime(2025, 7, 13, 12, 0, tzinfo=timezone.utc)),
        ):
            # init=2025-07-13T12z, valid=2025-07-15 → lead bucket = 48h
            mock_fetch.return_value = pl.DataFrame({
                "model": ["ecmwf"] * len(FORECAST_VALUES),
                "member_id": list(range(len(FORECAST_VALUES))),
                "init_datetime": [NOW_UTC] * len(FORECAST_VALUES),
                "valid_date": [TARGET_DATE] * len(FORECAST_VALUES),
                "station": [STATION_ID] * len(FORECAST_VALUES),
                "daily_max_c": FORECAST_VALUES,
                "lead_hours": [48] * len(FORECAST_VALUES),
            })
            mock_market.return_value = _make_snapshot([0.05, 0.20, 0.55, 0.20])

            from weather_edge.pipeline.lock import lock_picks

            # First call should succeed
            result1 = lock_picks(TARGET_DATE, STATION_ID, NOW_UTC)
            assert result1.date == TARGET_DATE

            # Second call must raise
            with pytest.raises(AlreadyLockedError):
                lock_picks(TARGET_DATE, STATION_ID, NOW_UTC)
