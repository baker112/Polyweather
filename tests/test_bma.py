"""BMA mixture tests: weights, mixture probabilities, mixture moments."""
from __future__ import annotations

import math
from datetime import date

import numpy as np
import pytest

from weather_edge.models import EmosParams
from weather_edge.postprocess.bma import (
    BMAMixture,
    ModelComponent,
    compute_bma_weights,
    predict_pdf_bma,
)

_VALID_DATE = date(2026, 4, 27)
_STATION = "EGLL"


def _params(a: float, b: float, c: float, d: float) -> EmosParams:
    from datetime import datetime, timezone
    now = datetime.now(timezone.utc)
    return EmosParams(
        a=a, b=b, c=c, d=d,
        station=_STATION, lead_hours=24,
        fitted_at=now, training_window_days=60,
        n_samples=50, train_crps=1.5, valid_from=now,
    )


# ─── Weight tests ─────────────────────────────────────────────────────────────

def test_weights_sum_to_one() -> None:
    model_crps = {"ecmwf": 1.2, "gefs": 1.5}
    weights = compute_bma_weights(model_crps)
    assert abs(sum(weights.values()) - 1.0) < 1e-9


def test_lower_crps_gets_higher_weight() -> None:
    weights = compute_bma_weights({"ecmwf": 1.0, "gefs": 2.0})
    assert weights["ecmwf"] > weights["gefs"]


def test_equal_crps_gives_equal_weights() -> None:
    weights = compute_bma_weights({"ecmwf": 1.5, "gefs": 1.5})
    assert abs(weights["ecmwf"] - weights["gefs"]) < 1e-9


def test_weights_with_smoothing_floor() -> None:
    """A model with CRPS=0 should not get infinite weight due to smoothing."""
    weights = compute_bma_weights({"ecmwf": 0.0, "gefs": 1.5}, smoothing=0.05)
    assert all(w > 0 for w in weights.values())
    assert abs(sum(weights.values()) - 1.0) < 1e-9


def test_empty_crps_returns_empty_weights() -> None:
    assert compute_bma_weights({}) == {}


# ─── BMAMixture tests ─────────────────────────────────────────────────────────

def _two_component_mixture(
    mu1: float, sigma1: float, w1: float,
    mu2: float, sigma2: float,
) -> BMAMixture:
    w2 = 1.0 - w1
    return BMAMixture(
        components=[
            ModelComponent("ecmwf", w1, mu1, sigma1),
            ModelComponent("gefs", w2, mu2, sigma2),
        ],
        station=_STATION,
        valid_date=_VALID_DATE,
        lead_hours=24,
    )


def test_mixture_bracket_prob_sums_to_one() -> None:
    mix = _two_component_mixture(20.0, 2.0, 0.6, 22.0, 1.5)
    total = (
        mix.bracket_prob(None, 15.0)
        + mix.bracket_prob(15.0, 20.0)
        + mix.bracket_prob(20.0, 25.0)
        + mix.bracket_prob(25.0, None)
    )
    assert abs(total - 1.0) < 1e-9


def test_mixture_mu_is_weighted_average() -> None:
    mix = _two_component_mixture(20.0, 2.0, 0.6, 22.0, 2.0)
    expected_mu = 0.6 * 20.0 + 0.4 * 22.0
    assert abs(mix.mu - expected_mu) < 1e-10


def test_mixture_sigma_law_of_total_variance() -> None:
    w1, mu1, sigma1 = 0.6, 20.0, 2.0
    w2, mu2, sigma2 = 0.4, 22.0, 2.0
    mix = _two_component_mixture(mu1, sigma1, w1, mu2, sigma2)
    mixture_mean = w1 * mu1 + w2 * mu2
    expected_var = (
        w1 * (sigma1**2 + (mu1 - mixture_mean)**2)
        + w2 * (sigma2**2 + (mu2 - mixture_mean)**2)
    )
    assert abs(mix.sigma**2 - expected_var) < 1e-9


def test_single_component_mixture_matches_gaussian() -> None:
    """A mixture with one component should match a plain Gaussian exactly."""
    from scipy.stats import norm
    mix = BMAMixture(
        components=[ModelComponent("ecmwf", 1.0, 20.0, 2.0)],
        station=_STATION, valid_date=_VALID_DATE, lead_hours=24,
    )
    expected = float(norm.cdf(22.0, 20.0, 2.0) - norm.cdf(18.0, 20.0, 2.0))
    actual = mix.bracket_prob(18.0, 22.0)
    assert abs(actual - expected) < 1e-12


def test_mixture_tail_brackets_handle_infinity() -> None:
    mix = _two_component_mixture(20.0, 2.0, 0.5, 22.0, 2.0)
    # 0°C is ~10 sigma below the mixture — still representable in float64
    p_below_0 = mix.bracket_prob(None, 0.0)
    # 35°C is ~6-7 sigma above — clearly tiny but representable
    p_above_35 = mix.bracket_prob(35.0, None)
    assert 0 < p_below_0 < 0.01
    assert 0 < p_above_35 < 0.01


# ─── predict_pdf_bma tests ────────────────────────────────────────────────────

def test_predict_pdf_bma_produces_mixture() -> None:
    params_e = _params(a=0.5, b=0.95, c=0.8, d=0.7)
    params_g = _params(a=0.0, b=1.0, c=1.0, d=0.8)

    model_data = [
        ("ecmwf", [22.0, 23.0, 21.5, 24.0] * 12, params_e),
        ("gefs", [21.0, 22.5, 23.5, 22.0] * 8, params_g),
    ]
    weights = {"ecmwf": 0.6, "gefs": 0.4}

    mix = predict_pdf_bma(model_data, weights, _VALID_DATE, _STATION, 24)
    assert isinstance(mix, BMAMixture)
    assert len(mix.components) == 2
    assert abs(sum(c.weight for c in mix.components) - 1.0) < 1e-9


def test_predict_pdf_bma_renormalizes_when_model_missing() -> None:
    """If a model has zero weight, remaining weights should still sum to 1."""
    params_e = _params(a=0.5, b=0.95, c=0.8, d=0.7)
    model_data = [
        ("ecmwf", [22.0, 23.0, 21.0] * 10, params_e),
        # gefs has no values → skipped
    ]
    weights = {"ecmwf": 0.6, "gefs": 0.4}

    mix = predict_pdf_bma(model_data, weights, _VALID_DATE, _STATION, 24)
    assert len(mix.components) == 1
    assert abs(mix.components[0].weight - 1.0) < 1e-9


def test_predict_pdf_bma_raises_with_no_models() -> None:
    from weather_edge.exceptions import EmosError
    with pytest.raises(EmosError):
        predict_pdf_bma([], {}, _VALID_DATE, _STATION, 24)
