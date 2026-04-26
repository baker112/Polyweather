"""Polymarket CLOB API client.

Market discovery: GET https://gamma-api.polymarket.com/markets?slug={slug}
Order book:       GET https://clob.polymarket.com/book?token_id={token_id}

No authentication required for public read operations.
"""
from __future__ import annotations

import logging
import re
from datetime import date, datetime, timezone
from typing import Any

import httpx

from weather_edge.exceptions import MarketError
from weather_edge.models import MarketOutcome, MarketSnapshot

_GAMMA_URL = "https://gamma-api.polymarket.com"
_CLOB_URL = "https://clob.polymarket.com"
_TIMEOUT = 30.0
_logger = logging.getLogger(__name__)

# Polymarket overround sanity bounds
_OVERROUND_MIN = 0.9
_OVERROUND_MAX = 1.2

# Pattern to extract bracket bounds from outcome labels, e.g. "21°C to 23°C", "Above 28°C", "Below 15°C"
_BRACKET_RE = re.compile(
    r"(?:above\s*([\d.]+)|below\s*([\d.]+)|([\d.]+)\s*(?:°c|c|to|-)\s*([\d.]+))",
    re.IGNORECASE,
)


async def fetch_market(slug: str, station: str, target_date: date) -> MarketSnapshot:
    """Fetch a Polymarket market by slug and return a fully hydrated MarketSnapshot."""
    async with httpx.AsyncClient(timeout=_TIMEOUT, follow_redirects=True) as client:
        market_data = await _get_market(client, slug)
        outcomes = await _get_outcomes(client, market_data)

    implied_sum = sum(o.mid for o in outcomes)
    if not (_OVERROUND_MIN <= implied_sum <= _OVERROUND_MAX):
        raise MarketError(
            f"Market {slug!r} implied sum {implied_sum:.3f} outside [{_OVERROUND_MIN}, {_OVERROUND_MAX}]"
        )

    return MarketSnapshot(
        market_id=str(market_data.get("id", "")),
        slug=slug,
        station=station,
        target_date=target_date,
        fetched_at=datetime.now(timezone.utc),
        outcomes=outcomes,
        implied_sum=implied_sum,
    )


async def _get_market(client: httpx.AsyncClient, slug: str) -> dict[str, Any]:
    resp = await client.get(f"{_GAMMA_URL}/markets", params={"slug": slug})
    resp.raise_for_status()
    data: list[dict[str, Any]] = resp.json()
    if not data:
        raise MarketError(f"No market found for slug {slug!r}")
    return data[0]


async def _get_outcomes(client: httpx.AsyncClient, market: dict[str, Any]) -> list[MarketOutcome]:
    """Fetch order books for all tokens and build MarketOutcome objects."""
    tokens: list[dict[str, Any]] = market.get("tokens", [])
    if not tokens:
        # Fallback: try clob_token_ids / outcomes fields
        token_ids: list[str] = market.get("clob_token_ids", [])
        outcome_labels: list[str] = market.get("outcomes", [])
        tokens = [
            {"token_id": tid, "outcome": label}
            for tid, label in zip(token_ids, outcome_labels)
        ]

    outcomes: list[MarketOutcome] = []
    for token in tokens:
        token_id: str = str(token.get("token_id", ""))
        label: str = str(token.get("outcome", ""))
        if not token_id:
            continue
        book = await _get_book(client, token_id)
        low, high = _parse_bracket(label)
        outcomes.append(MarketOutcome(
            label=label,
            low=low,
            high=high,
            best_bid=book["best_bid"],
            best_ask=book["best_ask"],
            mid=book["mid"],
            spread=book["spread"],
            liquidity=book["liquidity"],
            token_id=token_id,
        ))

    if not outcomes:
        raise MarketError("No outcomes found in market")

    return sorted(outcomes, key=lambda o: (o.low is None, o.low or 0.0))


async def _get_book(client: httpx.AsyncClient, token_id: str) -> dict[str, float]:
    resp = await client.get(f"{_CLOB_URL}/book", params={"token_id": token_id})
    resp.raise_for_status()
    data: dict[str, Any] = resp.json()

    bids: list[dict[str, str]] = data.get("bids", [])
    asks: list[dict[str, str]] = data.get("asks", [])

    best_bid = max((float(b["price"]) for b in bids), default=0.0)
    best_ask = min((float(a["price"]) for a in asks), default=1.0)

    mid = (best_bid + best_ask) / 2.0
    spread = best_ask - best_bid

    # Liquidity = total resting bid value (a simple proxy for depth)
    liquidity = sum(float(b["price"]) * float(b["size"]) for b in bids)

    return {
        "best_bid": best_bid,
        "best_ask": best_ask,
        "mid": mid,
        "spread": spread,
        "liquidity": liquidity,
    }


def _parse_bracket(label: str) -> tuple[float | None, float | None]:
    """Extract (low, high) temperature bounds from a bracket label string.

    Examples:
      "Below 15°C"       → (None, 15.0)
      "15°C to 17°C"     → (15.0, 17.0)
      "Above 28°C"       → (28.0, None)
    """
    label_lower = label.lower().strip()

    if "below" in label_lower or "under" in label_lower:
        nums = re.findall(r"[\d.]+", label_lower)
        if nums:
            return None, float(nums[0])

    if "above" in label_lower or "over" in label_lower:
        nums = re.findall(r"[\d.]+", label_lower)
        if nums:
            return float(nums[0]), None

    nums = re.findall(r"[\d.]+", label_lower)
    if len(nums) >= 2:
        return float(nums[0]), float(nums[1])

    _logger.warning("Could not parse bracket from label: %r", label)
    return None, None
