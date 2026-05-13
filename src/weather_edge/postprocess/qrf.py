"""Quantile Regression Forest (QRF) — Phase 3 alternative to Gaussian EMOS.

Meinshausen (2006): train a random forest on (ensemble_features → observed_max);
at prediction time, weight training observations by how often they share leaf nodes
with the query point, yielding an empirical predictive CDF.

This avoids the Gaussian shape assumption baked into EMOS/BMA.

Features per training pair (8 total):
    ens_mean, ens_var, ens_p10, ens_p25, ens_p50, ens_p75, ens_p90, n_members
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from typing import NamedTuple

import numpy as np
from numpy.typing import NDArray
from sklearn.ensemble import RandomForestRegressor  # type: ignore[import-untyped]

_N_ESTIMATORS = 200
_MIN_SAMPLES_LEAF = 5
_RANDOM_STATE = 42

_FEATURE_NAMES = [
    "ens_mean", "ens_var",
    "ens_p10", "ens_p25", "ens_p50", "ens_p75", "ens_p90",
    "n_members",
]


class QRFTrainingPair(NamedTuple):
    features: list[float]  # length 8, same order as _FEATURE_NAMES
    obs: float


@dataclass
class QRFDistribution:
    """Empirical predictive distribution from QRF leaf-node matching.

    Exposes the same .bracket_prob(low, high) interface as PredictedDistribution
    and BMAMixture, so it can be dropped in throughout the pipeline.
    """
    station: str
    valid_date: date
    lead_hours: int
    leaf_values: NDArray[np.float64] = field(repr=False)   # sorted training obs
    leaf_weights: NDArray[np.float64] = field(repr=False)  # normalized weights, sum=1

    def bracket_prob(self, low: float | None, high: float | None) -> float:
        """Weighted empirical P(low ≤ Y < high)."""
        w = self.leaf_weights
        y = self.leaf_values
        mask_low = np.zeros(len(y), dtype=bool) if low is None else (y < low)
        mask_high = np.ones(len(y), dtype=bool) if high is None else (y < high)
        return float(max(0.0, np.sum(w[mask_high]) - np.sum(w[mask_low])))

    @property
    def mu(self) -> float:
        """Mean of the empirical leaf distribution."""
        return float(np.average(self.leaf_values, weights=self.leaf_weights))

    @property
    def sigma(self) -> float:
        """IQR-based sigma estimate (robust to non-Gaussian tails).

        Floored at 0.5°C: a daily-max temperature is never knowable to better
        than half a degree once you account for observation rounding +
        microclimate noise. Without this floor, leaf-collapse (when all
        matching training obs are near-identical) drove sigma to 1e-4°C and
        produced delta-like predictive distributions that broke Kelly sizing.
        """
        q75 = _weighted_quantile(self.leaf_values, 0.75, self.leaf_weights)
        q25 = _weighted_quantile(self.leaf_values, 0.25, self.leaf_weights)
        return float(max((q75 - q25) / 1.3490, 0.5))


# ─── Feature extraction ───────────────────────────────────────────────────────

def _extract_features(ensemble_values: list[float]) -> list[float]:
    """Convert raw ensemble member values into QRF feature vector (length 8)."""
    arr = np.array(ensemble_values, dtype=np.float64)
    return [
        float(np.mean(arr)),
        float(np.var(arr, ddof=1)) if len(arr) > 1 else 0.0,
        float(np.percentile(arr, 10)),
        float(np.percentile(arr, 25)),
        float(np.percentile(arr, 50)),
        float(np.percentile(arr, 75)),
        float(np.percentile(arr, 90)),
        float(len(arr)),
    ]


# ─── Fitting ──────────────────────────────────────────────────────────────────

def fit_qrf(
    training_pairs: list[QRFTrainingPair],
    station: str,
    lead_hours: int,
    n_estimators: int = _N_ESTIMATORS,
    min_samples_leaf: int = _MIN_SAMPLES_LEAF,
) -> tuple[RandomForestRegressor, NDArray[np.float64], NDArray[np.float64], dict]:
    """Fit a QRF. Returns (forest, X_train, y_train, metadata).

    Raises EmosError if fewer than 10 training pairs are available.
    """
    if len(training_pairs) < 10:
        from weather_edge.exceptions import EmosError
        raise EmosError(
            f"Insufficient QRF training data: {len(training_pairs)} pairs (need ≥10)"
        )

    X = np.array([p.features for p in training_pairs], dtype=np.float64)
    y = np.array([p.obs for p in training_pairs], dtype=np.float64)

    forest = RandomForestRegressor(
        n_estimators=n_estimators,
        min_samples_leaf=min_samples_leaf,
        random_state=_RANDOM_STATE,
    )
    forest.fit(X, y)

    now = datetime.now(timezone.utc)
    meta: dict = {
        "station": station,
        "lead_hours": lead_hours,
        "fitted_at": now.isoformat(),
        "n_samples": len(training_pairs),
        "n_estimators": n_estimators,
        "min_samples_leaf": min_samples_leaf,
        "valid_from": now.isoformat(),
    }
    return forest, X, y, meta


# ─── Prediction ───────────────────────────────────────────────────────────────

def predict_qrf(
    forest: RandomForestRegressor,
    X_train: NDArray[np.float64],
    y_train: NDArray[np.float64],
    ensemble_values: list[float],
    valid_date: date,
    station: str,
    lead_hours: int,
) -> QRFDistribution:
    """Produce an empirical predictive distribution via leaf-node matching.

    For each tree, training observations that fall in the same leaf as the query
    are upweighted (inverse proportional to leaf size). The final weights are
    averaged across trees, then normalized.
    """
    x_new = np.array([_extract_features(ensemble_values)], dtype=np.float64)

    train_leaves = forest.apply(X_train)  # (n_train, n_trees)
    new_leaves = forest.apply(x_new)      # (1, n_trees)

    n_train, n_trees = train_leaves.shape
    weights = np.zeros(n_train, dtype=np.float64)

    for t in range(n_trees):
        new_leaf_t = new_leaves[0, t]
        same_leaf = train_leaves[:, t] == new_leaf_t
        count = int(same_leaf.sum())
        if count > 0:
            weights[same_leaf] += 1.0 / count

    weights /= n_trees

    total = weights.sum()
    if total <= 0:
        weights = np.ones(n_train, dtype=np.float64) / n_train
    else:
        weights /= total

    # Leaf-collapse guard: if the effective sample size is tiny (most weight
    # piled on one or two training obs), blend in uniform mass so bracket_prob
    # doesn't become a delta function. ESS = 1 / Σwᵢ² ; threshold of 5 keeps
    # high-confidence days sharp while broadening the pathological ones.
    ess = 1.0 / max(float(np.sum(weights ** 2)), 1e-12)
    if ess < 5.0:
        weights = 0.7 * weights + 0.3 * (np.ones(n_train, dtype=np.float64) / n_train)
        weights /= weights.sum()

    sorter = np.argsort(y_train)
    return QRFDistribution(
        station=station,
        valid_date=valid_date,
        lead_hours=lead_hours,
        leaf_values=y_train[sorter],
        leaf_weights=weights[sorter],
    )


# ─── Training-pair assembly ───────────────────────────────────────────────────

def assemble_qrf_training_pairs(
    station: str,
    lead_hours: int,
    as_of: date,
    window_days: int = 60,
) -> list[QRFTrainingPair]:
    """Assemble richer (8-feature) training pairs for QRF.

    Pools ECMWF + GEFS members for each date in the rolling window.
    Uses only data strictly before as_of to prevent leakage.
    """
    import polars as pl

    from weather_edge.postprocess.emos import _init_datetime_for
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

    pairs: list[QRFTrainingPair] = []
    current = start_date

    while current <= end_date:
        if current not in obs_map:
            current += timedelta(days=1)
            continue

        init_dt = _init_datetime_for(current, lead_hours)
        all_values: list[float] = []

        for model in ("ecmwf", "gefs", "icon", "weathernext"):
            df = store.read_forecasts(model, init_dt, station)
            if df is None:
                continue
            subset = df.filter(
                (pl.col("valid_date") == current) & (pl.col("lead_hours") == lead_hours)
            )
            all_values.extend(subset["daily_max_c"].to_list())

        if len(all_values) < 5:
            current += timedelta(days=1)
            continue

        pairs.append(QRFTrainingPair(
            features=_extract_features(all_values),
            obs=obs_map[current],
        ))
        current += timedelta(days=1)

    return pairs


# ─── Utility ──────────────────────────────────────────────────────────────────

def _weighted_quantile(
    values: NDArray[np.float64],
    q: float,
    weights: NDArray[np.float64],
) -> float:
    """Weighted quantile via linear interpolation on the cumulative weight CDF."""
    sorter = np.argsort(values)
    sorted_vals = values[sorter]
    sorted_w = weights[sorter]
    cumw = np.cumsum(sorted_w)
    cumw /= cumw[-1]
    return float(np.interp(q, cumw, sorted_vals))
