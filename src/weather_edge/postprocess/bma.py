"""Bayesian Model Averaging (BMA) mixture for Phase 2.

Replaces the pooled-ensemble approach in Stage 3.

Each model m has its own fitted EMOS params producing N(μ_m, σ_m²).
The BMA predictive distribution is a Gaussian mixture:

    p(y) = Σ_m  w_m · N(y | μ_m, σ_m²)

Bracket probabilities close-form under the mixture:

    P(a ≤ Y < b) = Σ_m  w_m · [Φ((b−μ_m)/σ_m) − Φ((a−μ_m)/σ_m)]

Weights are derived from each model's rolling mean CRPS — better models get
higher weight (inverse-CRPS with a smoothing floor so no model is zeroed out).
"""
from __future__ import annotations

import math
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from typing import NamedTuple

import numpy as np
from scipy.stats import norm  # type: ignore[import-untyped]

from weather_edge.models import EmosParams
from weather_edge.postprocess.crps import crps_gaussian

_SMOOTHING = 0.05  # floor added to CRPS before inversion; prevents zero weights
_WEIGHT_WINDOW_DAYS = 30  # rolling window for CRPS-based weight estimation


class ModelComponent(NamedTuple):
    model: str
    weight: float
    mu: float
    sigma: float


@dataclass
class BMAMixture:
    """Gaussian mixture predictive distribution from BMA.

    Exposes the same `.bracket_prob(low, high)` interface as PredictedDistribution
    so it can be dropped in as a replacement throughout the pipeline.
    """
    components: list[ModelComponent]
    station: str
    valid_date: date
    lead_hours: int

    def bracket_prob(self, low: float | None, high: float | None) -> float:
        """P(low ≤ Y < high) under the weighted Gaussian mixture."""
        total = 0.0
        for comp in self.components:
            p_low = 0.0 if low is None else float(norm.cdf(low, comp.mu, comp.sigma))
            p_high = 1.0 if high is None else float(norm.cdf(high, comp.mu, comp.sigma))
            total += comp.weight * (p_high - p_low)
        return total

    @property
    def mu(self) -> float:
        """Mean of the mixture (weighted average of component means)."""
        return sum(c.weight * c.mu for c in self.components)

    @property
    def sigma(self) -> float:
        """Std dev of the mixture via law of total variance."""
        mixture_mean = self.mu
        total_var = sum(
            c.weight * (c.sigma**2 + (c.mu - mixture_mean) ** 2)
            for c in self.components
        )
        return math.sqrt(max(total_var, 1e-8))


# ─── Weight computation ───────────────────────────────────────────────────────

def compute_bma_weights(
    model_crps: dict[str, float],
    smoothing: float = _SMOOTHING,
) -> dict[str, float]:
    """Inverse-CRPS weights with smoothing floor.

    Args:
        model_crps: rolling mean CRPS for each model over the weight window.
        smoothing:  added to each CRPS before inversion; prevents zero weights.
    """
    if not model_crps:
        return {}
    raw = {m: 1.0 / (c + smoothing) for m, c in model_crps.items()}
    total = sum(raw.values())
    return {m: w / total for m, w in raw.items()}


def rolling_model_crps(
    station: str,
    lead_hours: int,
    as_of: date,
    window_days: int = _WEIGHT_WINDOW_DAYS,
) -> dict[str, float]:
    """Compute rolling mean CRPS for each model using historical predictions vs observations.

    Uses data strictly before as_of to prevent leakage.
    """
    from weather_edge.store import parquet as store
    import polars as pl

    end_date = as_of - timedelta(days=1)
    start_date = end_date - timedelta(days=window_days - 1)

    obs_df = store.read_observations(station, start_date, end_date)
    if obs_df.is_empty():
        return {}

    obs_map: dict[date, float] = {
        row["date"]: row["daily_max_c"]
        for row in obs_df.iter_rows(named=True)
    }

    model_scores: dict[str, list[float]] = {}

    current = start_date
    while current <= end_date:
        if current not in obs_map:
            current += timedelta(days=1)
            continue

        y = obs_map[current]

        for model in ("ecmwf", "gefs", "icon"):
            raw = store.read_emos_params(station, lead_hours, as_of=datetime(
                current.year, current.month, current.day, 18, tzinfo=timezone.utc
            ), model=model)
            if raw is None:
                current += timedelta(days=1)
                continue

            params = EmosParams(**raw)

            # Reconstruct ensemble mean and var from stored forecast
            init_dt = _init_dt_for(current, lead_hours)
            df = store.read_forecasts(model, init_dt, station)
            if df is None:
                current += timedelta(days=1)
                continue

            subset = df.filter(
                (pl.col("valid_date") == current) & (pl.col("lead_hours") == lead_hours)
            )
            if subset.is_empty():
                current += timedelta(days=1)
                continue

            vals = np.array(subset["daily_max_c"].to_list())
            ens_mean = float(np.mean(vals))
            ens_var = float(np.var(vals, ddof=1)) if len(vals) > 1 else 0.0

            mu = params.a + params.b * ens_mean
            sigma_sq = params.c + params.d * ens_var
            sigma = math.sqrt(max(sigma_sq, 1e-8))

            score = crps_gaussian(mu, sigma, y)
            model_scores.setdefault(model, []).append(score)

        current += timedelta(days=1)

    return {m: float(np.mean(scores)) for m, scores in model_scores.items() if scores}


def _init_dt_for(valid_date: date, lead_hours: int) -> datetime:
    valid_dt = datetime(valid_date.year, valid_date.month, valid_date.day, 12, tzinfo=timezone.utc)
    init_dt = valid_dt - timedelta(hours=lead_hours)
    if init_dt.hour >= 12:
        return init_dt.replace(hour=12, minute=0, second=0, microsecond=0)
    return init_dt.replace(hour=0, minute=0, second=0, microsecond=0)


# ─── BMA prediction ──────────────────────────────────────────────────────────

def predict_pdf_bma(
    model_data: list[tuple[str, list[float], EmosParams]],
    weights: dict[str, float],
    valid_date: date,
    station: str,
    lead_hours: int,
) -> BMAMixture:
    """Produce a BMA Gaussian mixture from per-model EMOS predictions.

    Args:
        model_data: list of (model_name, ensemble_values, emos_params) tuples.
        weights:    per-model weights from compute_bma_weights().
        valid_date: the date being forecast.
    """
    components: list[ModelComponent] = []

    for model, values, params in model_data:
        w = weights.get(model, 0.0)
        if w <= 0 or not values:
            continue

        arr = np.array(values, dtype=np.float64)
        ens_mean = float(np.mean(arr))
        ens_var = float(np.var(arr, ddof=1)) if len(arr) > 1 else 0.0

        mu = params.a + params.b * ens_mean
        sigma_sq = params.c + params.d * ens_var
        sigma = math.sqrt(max(sigma_sq, 1e-8))

        components.append(ModelComponent(model=model, weight=w, mu=mu, sigma=sigma))

    if not components:
        from weather_edge.exceptions import EmosError
        raise EmosError("No model components available for BMA mixture")

    # Renormalize in case some models had w=0
    total_w = sum(c.weight for c in components)
    components = [ModelComponent(c.model, c.weight / total_w, c.mu, c.sigma) for c in components]

    return BMAMixture(
        components=components,
        station=station,
        valid_date=valid_date,
        lead_hours=lead_hours,
    )
