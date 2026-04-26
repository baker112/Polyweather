"""Closed-form CRPS for a Gaussian predictive distribution.

Reference: Gneiting & Raftery (2007), eq. 21.

    CRPS(N(μ, σ²), y) = σ · [z·(2Φ(z) − 1) + 2φ(z) − 1/√π]
    where z = (y − μ) / σ
"""
from __future__ import annotations

import math

import numpy as np
from numpy.typing import NDArray
from scipy.stats import norm  # type: ignore[import-untyped]


def crps_gaussian(mu: float, sigma: float, y: float) -> float:
    """CRPS for a single Gaussian forecast vs. a scalar observation."""
    if sigma <= 0:
        raise ValueError(f"sigma must be positive, got {sigma}")
    z = (y - mu) / sigma
    return float(sigma * (z * (2.0 * norm.cdf(z) - 1.0) + 2.0 * norm.pdf(z) - 1.0 / math.sqrt(math.pi)))


def crps_gaussian_vec(
    mu: NDArray[np.float64],
    sigma: NDArray[np.float64],
    y: NDArray[np.float64],
) -> NDArray[np.float64]:
    """Vectorised CRPS over arrays of forecasts and observations."""
    if np.any(sigma <= 0):
        raise ValueError("All sigma values must be positive")
    z = (y - mu) / sigma
    result: NDArray[np.float64] = sigma * (
        z * (2.0 * norm.cdf(z) - 1.0) + 2.0 * norm.pdf(z) - 1.0 / math.sqrt(math.pi)
    )
    return result


def mean_crps(
    mu: NDArray[np.float64],
    sigma: NDArray[np.float64],
    y: NDArray[np.float64],
) -> float:
    return float(np.mean(crps_gaussian_vec(mu, sigma, y)))
