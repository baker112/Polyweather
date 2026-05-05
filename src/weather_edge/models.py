from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import Any

from pydantic import BaseModel
from scipy.stats import norm  # type: ignore[import-untyped]


class EmosParams(BaseModel):
    a: float
    b: float
    c: float
    d: float
    station: str
    lead_hours: int
    fitted_at: datetime
    training_window_days: int
    n_samples: int
    train_crps: float
    valid_from: datetime


@dataclass
class PredictedDistribution:
    mu: float
    sigma: float
    station: str
    valid_date: date
    lead_hours: int

    def bracket_prob(self, low: float | None, high: float | None) -> float:
        """P(low ≤ Y < high); None means −∞ / +∞ respectively."""
        p_low = 0.0 if low is None else float(norm.cdf(low, self.mu, self.sigma))
        p_high = 1.0 if high is None else float(norm.cdf(high, self.mu, self.sigma))
        return p_high - p_low


class BracketSpec(BaseModel):
    label: str
    low: float | None  # None = −∞
    high: float | None  # None = +∞


class BracketProb(BaseModel):
    label: str
    low: float | None
    high: float | None
    model_prob: float


class MarketOutcome(BaseModel):
    label: str
    low: float | None
    high: float | None
    best_bid: float
    best_ask: float
    mid: float
    spread: float
    liquidity: float
    top_ask_size: float = 0.0  # contracts available at best_ask (for buying YES)
    top_bid_size: float = 0.0  # contracts available at best_bid (for selling YES / buying NO via inverse)
    token_id: str       # YES token
    no_token_id: str = ""  # NO token (clobTokenIds[1])


class MarketSnapshot(BaseModel):
    market_id: str
    slug: str
    station: str
    target_date: date
    fetched_at: datetime
    outcomes: list[MarketOutcome]
    implied_sum: float


class Candidate(BaseModel):
    bracket_label: str
    low: float | None
    high: float | None
    model_prob: float
    market_prob: float
    edge: float  # model_prob − market_prob
    side: str  # "YES" or "NO"
    spread: float
    liquidity: float
    kelly_fraction: float = 0.0  # full Kelly stake fraction (cap externally)
    max_stake_usdc: float | None = None  # depth-implied stake cap; None = no depth cap
    gates: dict[str, bool]
    raw_values: dict[str, float]


class LockedPicks(BaseModel):
    date: date
    station: str
    locked_at: datetime
    mu: float
    sigma: float
    picks: list[Candidate]
    no_edge_reason: str | None
    provenance: dict[str, Any]
