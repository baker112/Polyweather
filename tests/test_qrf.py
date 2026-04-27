"""Tests for Quantile Regression Forest (Phase 3)."""
from __future__ import annotations

from datetime import date

import numpy as np
import pytest

from weather_edge.models import BracketSpec
from weather_edge.postprocess.emos import compute_brackets
from weather_edge.postprocess.qrf import (
    QRFDistribution,
    QRFTrainingPair,
    _extract_features,
    _weighted_quantile,
    fit_qrf,
    predict_qrf,
)


# ─── Fixtures ─────────────────────────────────────────────────────────────────

def _synthetic_pairs(n: int = 30, seed: int = 42) -> list[QRFTrainingPair]:
    rng = np.random.default_rng(seed)
    pairs = []
    for _ in range(n):
        true_temp = float(rng.normal(18.0, 3.0))
        ens = rng.normal(true_temp - 0.5, 1.5, size=30).tolist()
        obs = float(rng.normal(true_temp, 0.5))
        pairs.append(QRFTrainingPair(features=_extract_features(ens), obs=obs))
    return pairs


def _fit_and_predict(n_train: int = 30) -> QRFDistribution:
    pairs = _synthetic_pairs(n=n_train)
    forest, X, y, _ = fit_qrf(pairs, "EGLC", 24)
    ens = [16.0, 17.0, 18.0, 19.0, 20.0] * 6
    return predict_qrf(forest, X, y, ens, date(2026, 4, 27), "EGLC", 24)


# ─── Feature extraction ───────────────────────────────────────────────────────

def test_extract_features_length():
    feats = _extract_features([15.0, 16.0, 17.0, 18.0, 19.0])
    assert len(feats) == 8


def test_extract_features_mean():
    feats = _extract_features([10.0, 20.0])
    assert feats[0] == pytest.approx(15.0)


def test_extract_features_n_members():
    ens = list(range(31))
    feats = _extract_features(ens)
    assert feats[7] == 31.0


# ─── Fitting ──────────────────────────────────────────────────────────────────

def test_fit_qrf_returns_correct_shapes():
    pairs = _synthetic_pairs(n=20)
    forest, X, y, meta = fit_qrf(pairs, "EGLC", 24)
    assert X.shape == (20, 8)
    assert y.shape == (20,)
    assert meta["n_samples"] == 20
    assert meta["station"] == "EGLC"
    assert meta["lead_hours"] == 24


def test_fit_qrf_insufficient_data_raises():
    from weather_edge.exceptions import EmosError
    pairs = _synthetic_pairs(n=5)
    with pytest.raises(EmosError, match="Insufficient QRF training data"):
        fit_qrf(pairs, "EGLC", 24)


def test_fit_qrf_exactly_10_succeeds():
    pairs = _synthetic_pairs(n=10)
    forest, X, y, _ = fit_qrf(pairs, "EGLC", 24)
    assert len(y) == 10


# ─── Prediction / leaf weights ────────────────────────────────────────────────

def test_leaf_weights_sum_to_one():
    dist = _fit_and_predict()
    assert abs(dist.leaf_weights.sum() - 1.0) < 1e-9


def test_leaf_values_sorted():
    dist = _fit_and_predict()
    assert np.all(np.diff(dist.leaf_values) >= 0)


def test_mu_in_reasonable_range():
    dist = _fit_and_predict()
    assert 0.0 < dist.mu < 40.0


def test_sigma_positive():
    dist = _fit_and_predict()
    assert dist.sigma > 0.0


# ─── Bracket probabilities ────────────────────────────────────────────────────

def test_bracket_prob_full_range_is_one():
    dist = _fit_and_predict()
    assert dist.bracket_prob(None, None) == pytest.approx(1.0)


def test_bracket_prob_lower_tail_is_one():
    dist = _fit_and_predict()
    # P(Y < 1000°C) = 1 for any realistic distribution
    assert dist.bracket_prob(None, 1000.0) == pytest.approx(1.0)


def test_bracket_prob_upper_tail_is_zero():
    dist = _fit_and_predict()
    # P(Y ≥ 1000°C) ≈ 0
    assert dist.bracket_prob(1000.0, None) == pytest.approx(0.0)


def test_bracket_prob_monotone():
    dist = _fit_and_predict()
    thresholds = [5.0, 12.0, 18.0, 24.0, 35.0]
    cdfs = [dist.bracket_prob(None, t) for t in thresholds]
    for i in range(len(cdfs) - 1):
        assert cdfs[i] <= cdfs[i + 1]


def test_bracket_prob_nonnegative():
    dist = _fit_and_predict()
    brackets = [
        (None, 10.0), (10.0, 15.0), (15.0, 20.0), (20.0, None),
    ]
    for low, high in brackets:
        assert dist.bracket_prob(low, high) >= 0.0


# ─── Integration with compute_brackets ───────────────────────────────────────

def test_compute_brackets_with_qrf_sums_to_one():
    dist = _fit_and_predict()
    brackets = [
        BracketSpec(label="Below 15°C", low=None, high=15.0),
        BracketSpec(label="15–20°C", low=15.0, high=20.0),
        BracketSpec(label="20–25°C", low=20.0, high=25.0),
        BracketSpec(label="Above 25°C", low=25.0, high=None),
    ]
    result = compute_brackets(dist, brackets)
    total = sum(bp.model_prob for bp in result)
    assert total == pytest.approx(1.0, abs=1e-9)


def test_compute_brackets_floor_applied():
    """Very extreme bracket should not be exactly zero after floor+renorm."""
    dist = _fit_and_predict()
    brackets = [
        BracketSpec(label="Very cold", low=None, high=-50.0),
        BracketSpec(label="Normal", low=-50.0, high=None),
    ]
    result = compute_brackets(dist, brackets)
    assert result[0].model_prob > 0.0  # floor applied


# ─── Weighted quantile utility ────────────────────────────────────────────────

def test_weighted_quantile_uniform_weights():
    vals = np.array([1.0, 2.0, 3.0, 4.0, 5.0])
    weights = np.ones(5) / 5.0
    q50 = _weighted_quantile(vals, 0.5, weights)
    # cumulative weights: 0.2, 0.4, 0.6, 0.8, 1.0 → q50 interpolates between vals[1] and vals[2]
    assert 1.0 < q50 < 5.0


def test_weighted_quantile_extreme_weight():
    vals = np.array([1.0, 2.0, 100.0])
    # Almost all weight on the last element
    weights = np.array([0.01, 0.01, 0.98])
    q90 = _weighted_quantile(vals, 0.9, weights)
    assert q90 > 50.0
