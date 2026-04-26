"""Brier score, closing-line value, and P&L curve."""
from __future__ import annotations

from datetime import date

import matplotlib.pyplot as plt
import numpy as np
import polars as pl

from weather_edge.store import parquet as store


def brier_score(outcomes: np.ndarray, probs: np.ndarray) -> float:
    """Mean squared error between predicted probs and binary outcomes."""
    return float(np.mean((probs - outcomes) ** 2))


def compute_report(
    station_id: str,
    start: date,
    end: date,
) -> dict[str, object]:
    df = store.read_backtest_results(station_id, start, end)

    if df.is_empty():
        return {"error": "No backtest data"}

    df_scored = df.drop_nulls(subset=["crps"])
    mean_crps = float(df_scored["crps"].mean()) if not df_scored.is_empty() else float("nan")

    df_picks = df.drop_nulls(subset=["entry_mid", "bracket_hit", "pnl"])
    n_picks = len(df_picks)

    if n_picks == 0:
        return {
            "n_days": len(df),
            "n_picks": 0,
            "mean_crps": mean_crps,
            "brier_score": float("nan"),
            "total_pnl": 0.0,
            "hit_rate": float("nan"),
        }

    outcomes = df_picks["bracket_hit"].to_numpy().astype(float)
    probs = df_picks["entry_mid"].to_numpy()
    pnl = df_picks["pnl"].to_numpy()

    return {
        "n_days": len(df),
        "n_picks": n_picks,
        "mean_crps": mean_crps,
        "brier_score": brier_score(outcomes, probs),
        "total_pnl": float(pnl.sum()),
        "hit_rate": float(outcomes.mean()),
        "mean_edge_at_entry": float(df_picks["edge_at_entry"].mean()),
    }


def plot_pnl_curve(
    station_id: str,
    start: date,
    end: date,
    output_path: str | None = None,
) -> None:
    df = store.read_backtest_results(station_id, start, end).drop_nulls(subset=["pnl"])
    if df.is_empty():
        print("No P&L data")
        return

    pnl = df.sort("date")["pnl"].to_numpy()
    cumulative = np.cumsum(pnl)

    fig, ax = plt.subplots(figsize=(10, 4))
    ax.plot(cumulative, color="steelblue")
    ax.axhline(0, color="black", linewidth=0.8, linestyle="--")
    ax.fill_between(range(len(cumulative)), cumulative, where=(cumulative >= 0),
                    alpha=0.3, color="green")
    ax.fill_between(range(len(cumulative)), cumulative, where=(cumulative < 0),
                    alpha=0.3, color="red")
    ax.set_xlabel("Pick number")
    ax.set_ylabel("Cumulative P&L (units)")
    ax.set_title(f"P&L Curve — {station_id} ({start} to {end})")
    fig.tight_layout()
    if output_path:
        fig.savefig(output_path, dpi=150)
    else:
        plt.show()
    plt.close(fig)


def plot_clv_distribution(
    station_id: str,
    start: date,
    end: date,
    output_path: str | None = None,
) -> None:
    """Closing-line value distribution: (closing_mid - entry_mid) on our side.

    Requires a 'closing_mid' column in backtest results (populated when available).
    Positive mean = we beat the close = real edge.
    """
    df = store.read_backtest_results(station_id, start, end)
    if "closing_mid" not in df.columns or df.drop_nulls(subset=["closing_mid"]).is_empty():
        print("No closing_mid data for CLV distribution (run after market resolution)")
        return

    df_clv = df.drop_nulls(subset=["entry_mid", "closing_mid", "pick_side"])
    entry = df_clv["entry_mid"].to_numpy()
    closing = df_clv["closing_mid"].to_numpy()
    # On a YES buy, CLV = closing_mid - entry_mid; on a NO buy, CLV = entry_mid - closing_mid
    sides = df_clv["pick_side"].to_list()
    clv = np.array([
        closing[i] - entry[i] if sides[i] == "YES" else entry[i] - closing[i]
        for i in range(len(entry))
    ])

    fig, ax = plt.subplots(figsize=(7, 4))
    ax.hist(clv, bins=20, edgecolor="white", color="steelblue")
    ax.axvline(0, color="black", linestyle="--")
    ax.axvline(float(clv.mean()), color="red", linestyle="-", label=f"Mean CLV = {clv.mean():+.4f}")
    ax.set_xlabel("CLV (closing_mid − entry_mid)")
    ax.set_ylabel("Count")
    ax.set_title(f"CLV Distribution — {station_id} ({start} to {end})")
    ax.legend()
    fig.tight_layout()
    if output_path:
        fig.savefig(output_path, dpi=150)
    else:
        plt.show()
    plt.close(fig)
