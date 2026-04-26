"""Calibration diagnostics: reliability diagram and PIT histogram."""
from __future__ import annotations

from datetime import date

import matplotlib.pyplot as plt
import numpy as np
from numpy.typing import NDArray
from scipy.stats import norm  # type: ignore[import-untyped]

from weather_edge.store import parquet as store


def pit_values(
    mu: NDArray[np.float64],
    sigma: NDArray[np.float64],
    observed: NDArray[np.float64],
) -> NDArray[np.float64]:
    """Probability Integral Transform: Φ((y−μ)/σ). Should be U[0,1] if calibrated."""
    return np.array(norm.cdf((observed - mu) / sigma), dtype=np.float64)


def plot_pit_histogram(
    station_id: str,
    start: date,
    end: date,
    n_bins: int = 10,
    output_path: str | None = None,
) -> None:
    df = store.read_backtest_results(station_id, start, end).drop_nulls(subset=["mu", "sigma", "observed_daily_max"])
    if df.is_empty():
        print("No data for PIT histogram")
        return

    mu = df["mu"].to_numpy()
    sigma = df["sigma"].to_numpy()
    obs = df["observed_daily_max"].to_numpy()

    pits = pit_values(mu, sigma, obs)

    fig, ax = plt.subplots(figsize=(7, 4))
    ax.hist(pits, bins=n_bins, range=(0, 1), density=True, edgecolor="white", color="steelblue")
    ax.axhline(1.0, color="red", linestyle="--", label="Uniform (ideal)")
    ax.set_xlabel("PIT value")
    ax.set_ylabel("Density")
    ax.set_title(f"PIT Histogram — {station_id} ({start} to {end})")
    ax.legend()
    fig.tight_layout()
    if output_path:
        fig.savefig(output_path, dpi=150)
    else:
        plt.show()
    plt.close(fig)


def plot_reliability_diagram(
    station_id: str,
    start: date,
    end: date,
    n_bins: int = 10,
    output_path: str | None = None,
) -> None:
    """Bin model probabilities into deciles and plot predicted vs. empirical hit rate."""
    df = store.read_backtest_results(station_id, start, end).drop_nulls(
        subset=["entry_mid", "bracket_hit"]
    )
    if df.is_empty():
        print("No pick data for reliability diagram")
        return

    pred_probs = df["entry_mid"].to_numpy()
    outcomes = df["bracket_hit"].to_numpy().astype(float)

    bin_edges = np.linspace(0, 1, n_bins + 1)
    bin_centres: list[float] = []
    empirical: list[float] = []

    for lo, hi in zip(bin_edges[:-1], bin_edges[1:]):
        mask = (pred_probs >= lo) & (pred_probs < hi)
        if mask.sum() == 0:
            continue
        bin_centres.append(float(pred_probs[mask].mean()))
        empirical.append(float(outcomes[mask].mean()))

    fig, ax = plt.subplots(figsize=(6, 6))
    ax.plot([0, 1], [0, 1], "k--", label="Perfect calibration")
    ax.scatter(bin_centres, empirical, s=60, color="steelblue", zorder=5)
    ax.plot(bin_centres, empirical, color="steelblue", label="Model")
    ax.set_xlim(0, 1)
    ax.set_ylim(0, 1)
    ax.set_xlabel("Predicted probability")
    ax.set_ylabel("Empirical frequency")
    ax.set_title(f"Reliability Diagram — {station_id} ({start} to {end})")
    ax.legend()
    fig.tight_layout()
    if output_path:
        fig.savefig(output_path, dpi=150)
    else:
        plt.show()
    plt.close(fig)
