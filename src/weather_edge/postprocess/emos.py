"""EMOS (Non-homogeneous Gaussian Regression) — Gneiting et al. 2005.

Model:
    Y | m̄, s²  ~  N(μ, σ²)
    μ  = a + b·m̄
    σ² = c + d·s²

Constraints: b > 0, c ≥ 0, d ≥ 0.
Loss: minimise mean CRPS over a rolling training window.
"""
from __future__ import annotations

import math
from datetime import date, datetime, timedelta, timezone
from typing import NamedTuple

import numpy as np
from numpy.typing import NDArray
from scipy.optimize import minimize  # type: ignore[import-untyped]

from weather_edge.models import BracketProb, BracketSpec, EmosParams, PredictedDistribution
from weather_edge.postprocess.crps import mean_crps

_PROB_FLOOR = 1e-4
_LEAD_BUCKETS = (24, 48, 72)


class TrainingPair(NamedTuple):
    ens_mean: float
    ens_var: float
    obs: float


# ─── Fitting ──────────────────────────────────────────────────────────────────

def _emos_loss(
    params: NDArray[np.float64],
    ens_mean: NDArray[np.float64],
    ens_var: NDArray[np.float64],
    obs: NDArray[np.float64],
) -> float:
    a, b, c, d = params
    mu = a + b * ens_mean
    sigma_sq = c + d * ens_var
    sigma = np.sqrt(np.maximum(sigma_sq, 1e-8))
    return mean_crps(mu, sigma, obs)


def fit_emos(training_pairs: list[TrainingPair], station: str, lead_hours: int) -> EmosParams:
    if not training_pairs:
        from weather_edge.exceptions import EmosError
        raise EmosError("No training pairs provided")

    ens_mean = np.array([p.ens_mean for p in training_pairs], dtype=np.float64)
    ens_var = np.array([p.ens_var for p in training_pairs], dtype=np.float64)
    obs = np.array([p.obs for p in training_pairs], dtype=np.float64)

    x0 = np.array([0.0, 1.0, 1.0, 1.0])
    bounds = [(None, None), (1e-6, None), (0.0, None), (0.0, None)]

    result = minimize(
        _emos_loss,
        x0,
        args=(ens_mean, ens_var, obs),
        method="L-BFGS-B",
        bounds=bounds,
        options={"maxiter": 2000, "ftol": 1e-12},
    )

    a, b, c, d = result.x
    mu_fit = a + b * ens_mean
    sigma_fit = np.sqrt(np.maximum(c + d * ens_var, 1e-8))
    train_crps = mean_crps(mu_fit, sigma_fit, obs)

    now = datetime.now(timezone.utc)
    return EmosParams(
        a=float(a),
        b=float(b),
        c=float(c),
        d=float(d),
        station=station,
        lead_hours=lead_hours,
        fitted_at=now,
        training_window_days=60,
        n_samples=len(training_pairs),
        train_crps=float(train_crps),
        valid_from=now,
    )


# ─── Prediction ───────────────────────────────────────────────────────────────

def predict_pdf(
    ensemble_values: list[float],
    emos_params: EmosParams,
    valid_date: date,
) -> PredictedDistribution:
    """Pool ensemble → compute m̄ and s² → apply EMOS → return calibrated N(μ, σ)."""
    arr = np.array(ensemble_values, dtype=np.float64)
    ens_mean = float(np.mean(arr))
    ens_var = float(np.var(arr, ddof=1)) if len(arr) > 1 else 0.0

    mu = emos_params.a + emos_params.b * ens_mean
    sigma_sq = emos_params.c + emos_params.d * ens_var
    sigma = math.sqrt(max(sigma_sq, 1e-8))

    return PredictedDistribution(
        mu=mu,
        sigma=sigma,
        station=emos_params.station,
        valid_date=valid_date,
        lead_hours=emos_params.lead_hours,
    )


# ─── Bracket probabilities ────────────────────────────────────────────────────

def compute_brackets(
    distribution: PredictedDistribution,
    brackets: list[BracketSpec],
) -> list[BracketProb]:
    """Compute bracket probabilities with floor and renormalisation."""
    raw = [distribution.bracket_prob(b.low, b.high) for b in brackets]

    # Floor before renormalisation
    floored = [max(p, _PROB_FLOOR) for p in raw]
    total = sum(floored)
    normalised = [p / total for p in floored]

    return [
        BracketProb(label=b.label, low=b.low, high=b.high, model_prob=p)
        for b, p in zip(brackets, normalised)
    ]


# ─── Training-pair assembly (used by CLI and backtest) ───────────────────────

def assemble_training_pairs(
    station: str,
    lead_hours: int,
    as_of: date,
    window_days: int = 60,
) -> list[TrainingPair]:
    """Load forecast + observation pairs for the rolling training window.

    Uses only data strictly before as_of to prevent future leakage.
    """
    from weather_edge.store import parquet as store

    end_date = as_of - timedelta(days=1)
    start_date = end_date - timedelta(days=window_days - 1)

    obs_df = store.read_observations(station, start_date, end_date)
    if obs_df.is_empty():
        return []

    obs_map: dict[date, float] = {
        row["date"]: row["daily_max_c"]
        for row in obs_df.iter_rows(named=True)
    }

    pairs: list[TrainingPair] = []

    current = start_date
    while current <= end_date:
        if current not in obs_map:
            current += timedelta(days=1)
            continue

        # Determine which init cycle would have been used for this date
        init_dt = _init_datetime_for(current, lead_hours)

        all_values: list[float] = []
        for model in ("ecmwf", "gefs"):
            df = store.read_forecasts(model, init_dt, station)
            if df is None:
                continue
            import polars as pl
            subset = df.filter(
                (pl.col("valid_date") == current) & (pl.col("lead_hours") == lead_hours)
            )
            all_values.extend(subset["daily_max_c"].to_list())

        if len(all_values) < 5:
            current += timedelta(days=1)
            continue

        arr = np.array(all_values, dtype=np.float64)
        pairs.append(TrainingPair(
            ens_mean=float(np.mean(arr)),
            ens_var=float(np.var(arr, ddof=1)),
            obs=obs_map[current],
        ))
        current += timedelta(days=1)

    return pairs


def _init_datetime_for(valid_date: date, lead_hours: int) -> datetime:
    """Estimate the 12z init cycle that produces the given lead_hours for valid_date."""
    # For a 12z init, lead_hours=24 covers ~12z+24h = 12z next day
    # Round to nearest 12z run before the valid_date
    valid_dt = datetime(valid_date.year, valid_date.month, valid_date.day, 12, 0, 0, tzinfo=timezone.utc)
    init_dt = valid_dt - timedelta(hours=lead_hours)
    # Snap to 00z or 12z
    if init_dt.hour >= 12:
        return init_dt.replace(hour=12, minute=0, second=0, microsecond=0)
    return init_dt.replace(hour=0, minute=0, second=0, microsecond=0)


def bucket_lead_hours(lead_hours: int) -> int:
    """Snap raw lead hours to the nearest supported bucket (24, 48, 72)."""
    return min(_LEAD_BUCKETS, key=lambda b: abs(b - lead_hours))


# ─── Phase 2: per-model training pairs ───────────────────────────────────────

def assemble_training_pairs_per_model(
    station: str,
    lead_hours: int,
    as_of: date,
    model: str,
    window_days: int = 60,
) -> list[TrainingPair]:
    """Assemble training pairs using only a single model's ensemble members.

    Used in Phase 2 to fit separate EMOS for each source model.
    Uses only data strictly before as_of to prevent leakage.
    """
    from weather_edge.store import parquet as store
    import polars as pl

    end_date = as_of - timedelta(days=1)
    start_date = end_date - timedelta(days=window_days - 1)

    obs_df = store.read_observations(station, start_date, end_date)
    if obs_df.is_empty():
        return []

    obs_map: dict[date, float] = {
        row["date"]: row["daily_max_c"]
        for row in obs_df.iter_rows(named=True)
    }

    pairs: list[TrainingPair] = []
    current = start_date

    while current <= end_date:
        if current not in obs_map:
            current += timedelta(days=1)
            continue

        init_dt = _init_datetime_for(current, lead_hours)
        df = store.read_forecasts(model, init_dt, station)
        if df is None:
            current += timedelta(days=1)
            continue

        subset = df.filter(
            (pl.col("valid_date") == current) & (pl.col("lead_hours") == lead_hours)
        )
        if subset.height < 3:
            current += timedelta(days=1)
            continue

        vals = np.array(subset["daily_max_c"].to_list(), dtype=np.float64)
        pairs.append(TrainingPair(
            ens_mean=float(np.mean(vals)),
            ens_var=float(np.var(vals, ddof=1)),
            obs=obs_map[current],
        ))
        current += timedelta(days=1)

    return pairs


def fit_emos_per_model(
    station: str,
    lead_hours: int,
    as_of: date,
    models: tuple[str, ...] = ("ecmwf", "gefs"),
    window_days: int = 60,
) -> dict[str, EmosParams]:
    """Fit one EmosParams per model. Returns only models with sufficient data.

    Phase 2 entry point. Falls back gracefully if a model has insufficient data.
    """
    results: dict[str, EmosParams] = {}
    for model in models:
        pairs = assemble_training_pairs_per_model(station, lead_hours, as_of, model, window_days)
        if len(pairs) < 10:
            continue
        params = fit_emos(pairs, station=station, lead_hours=lead_hours)
        results[model] = params
    return results
