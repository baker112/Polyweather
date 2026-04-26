"""Test crps_gaussian against the properscoring reference implementation."""
from __future__ import annotations

import numpy as np
import pytest

from weather_edge.postprocess.crps import crps_gaussian, crps_gaussian_vec, mean_crps

try:
    import properscoring  # type: ignore[import-untyped]
    HAS_PROPERSCORING = True
except ImportError:
    HAS_PROPERSCORING = False


@pytest.mark.skipif(not HAS_PROPERSCORING, reason="properscoring not installed")
def test_crps_matches_properscoring_scalar() -> None:
    rng = np.random.default_rng(42)
    for _ in range(100):
        mu = rng.uniform(-5, 35)
        sigma = rng.uniform(0.5, 5.0)
        y = rng.uniform(-5, 40)
        ours = crps_gaussian(mu, sigma, y)
        reference = float(properscoring.crps_gaussian(y, mu, sigma))
        assert abs(ours - reference) < 1e-9, (
            f"CRPS mismatch: ours={ours} ref={reference} (mu={mu}, sigma={sigma}, y={y})"
        )


@pytest.mark.skipif(not HAS_PROPERSCORING, reason="properscoring not installed")
def test_crps_matches_properscoring_vectorised() -> None:
    rng = np.random.default_rng(0)
    mu = rng.uniform(-5, 35, size=200)
    sigma = rng.uniform(0.5, 5.0, size=200)
    y = rng.uniform(-5, 40, size=200)

    ours = crps_gaussian_vec(mu, sigma, y)
    reference = properscoring.crps_gaussian(y, mu, sigma)
    np.testing.assert_allclose(ours, reference, atol=1e-9)


def test_crps_non_negative() -> None:
    """CRPS is always non-negative."""
    rng = np.random.default_rng(7)
    for _ in range(50):
        mu = float(rng.uniform(0, 30))
        sigma = float(rng.uniform(0.1, 10.0))
        y = float(rng.uniform(-10, 45))
        assert crps_gaussian(mu, sigma, y) >= 0.0


def test_crps_perfect_forecast() -> None:
    """As sigma → 0 and mu == y, CRPS → 0."""
    assert crps_gaussian(20.0, 1e-6, 20.0) < 1e-4


def test_crps_raises_on_non_positive_sigma() -> None:
    with pytest.raises(ValueError):
        crps_gaussian(0.0, 0.0, 5.0)
    with pytest.raises(ValueError):
        crps_gaussian(0.0, -1.0, 5.0)


def test_mean_crps_scalar() -> None:
    mu = np.array([15.0, 20.0, 25.0])
    sigma = np.array([2.0, 2.0, 2.0])
    y = np.array([16.0, 19.5, 26.0])
    result = mean_crps(mu, sigma, y)
    assert result > 0
    # Each individual CRPS
    expected = np.mean([crps_gaussian(m, s, o) for m, s, o in zip(mu, sigma, y)])
    assert abs(result - expected) < 1e-12
