"""Polymarket CLOB API client.

Discovery:  GET https://gamma-api.polymarket.com/events?slug={event_slug}
            Each temperature bracket is a binary YES/NO market inside the event.
            The YES token price = market probability for that bracket.

Order book: GET https://clob.polymarket.com/book?token_id={yes_token_id}

No authentication required for public read operations.

Bracket parsing notes:
  Polymarket London markets resolve against Wunderground integer °C values.
  A Wunderground-reported N°C corresponds to actual ∈ [N−0.5, N+0.5), so we
  shift bracket boundaries by ±0.5°C when building bracket low/high for the
  continuous EMOS/BMA/QRF distributions. See docs/RESOLUTION.md for details.
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

_OVERROUND_MIN = 0.9
_OVERROUND_MAX = 1.2


async def fetch_market(slug: str, station: str, target_date: date) -> MarketSnapshot:
    """Fetch a Polymarket temperature event by slug and return a MarketSnapshot.

    The slug is the *event* slug (e.g. 'highest-temperature-in-london-on-april-27-2026').
    All bracket sub-markets are fetched and their CLOB books queried.
    """
    async with httpx.AsyncClient(timeout=_TIMEOUT, follow_redirects=True) as client:
        event = await _get_event(client, slug)
        outcomes = await _get_outcomes(client, event)

    implied_sum = sum(o.mid for o in outcomes)
    if not (_OVERROUND_MIN <= implied_sum <= _OVERROUND_MAX):
        raise MarketError(
            f"Event {slug!r} implied sum {implied_sum:.3f} outside "
            f"[{_OVERROUND_MIN}, {_OVERROUND_MAX}]"
        )

    return MarketSnapshot(
        market_id=str(event.get("id", "")),
        slug=slug,
        station=station,
        target_date=target_date,
        fetched_at=datetime.now(timezone.utc),
        outcomes=outcomes,
        implied_sum=implied_sum,
    )


# ─── Event + market discovery ─────────────────────────────────────────────────

async def _get_event(client: httpx.AsyncClient, slug: str) -> dict[str, Any]:
    """Fetch a Polymarket event by slug from the Gamma API events endpoint."""
    resp = await client.get(f"{_GAMMA_URL}/events", params={"slug": slug})
    resp.raise_for_status()
    data: list[dict[str, Any]] = resp.json()
    if not data:
        raise MarketError(f"No event found for slug {slug!r}")
    event = data[0]
    if not event.get("markets"):
        raise MarketError(f"Event {slug!r} has no bracket markets")
    return event


async def _get_outcomes(
    client: httpx.AsyncClient,
    event: dict[str, Any],
) -> list[MarketOutcome]:
    """Build MarketOutcome objects for each bracket market in the event.

    Each bracket market in event['markets'] is a binary YES/NO market.
    YES token id = clobTokenIds[0]; YES price = market probability for the bracket.
    """
    markets: list[dict[str, Any]] = event.get("markets", [])
    outcomes: list[MarketOutcome] = []

    for mkt in markets:
        if not mkt.get("active", True) or mkt.get("closed", False):
            continue
        if not mkt.get("enableOrderBook", True):
            # AMM-only market — use outcomePrices as fallback
            outcome = _outcome_from_prices(mkt)
            if outcome is not None:
                outcomes.append(outcome)
            continue

        token_ids: list[str] = _parse_json_list(mkt.get("clobTokenIds", "[]"))
        if not token_ids:
            continue

        yes_token_id = token_ids[0]
        no_token_id = token_ids[1] if len(token_ids) > 1 else ""
        label = mkt.get("groupItemTitle", "") or mkt.get("question", "")

        try:
            book = await _get_book(client, yes_token_id)
        except Exception as exc:
            _logger.warning("CLOB book failed for token %s: %s", yes_token_id[:16], exc)
            outcome = _outcome_from_prices(mkt)
            if outcome is not None:
                outcomes.append(outcome)
            continue

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
            token_id=yes_token_id,
            no_token_id=no_token_id,
        ))

    if not outcomes:
        raise MarketError("No active outcomes found in event")

    return sorted(outcomes, key=lambda o: (o.low is None, o.low or 0.0))


def _outcome_from_prices(mkt: dict[str, Any]) -> MarketOutcome | None:
    """Build a MarketOutcome from outcomePrices when CLOB is unavailable."""
    prices = _parse_json_list(mkt.get("outcomePrices", "[]"))
    token_ids = _parse_json_list(mkt.get("clobTokenIds", "[]"))
    label = mkt.get("groupItemTitle", "") or mkt.get("question", "")
    if not prices or not label:
        return None
    yes_price = float(prices[0])
    low, high = _parse_bracket(label)
    return MarketOutcome(
        label=label,
        low=low,
        high=high,
        best_bid=yes_price,
        best_ask=yes_price,
        mid=yes_price,
        spread=0.0,
        liquidity=float(mkt.get("liquidityNum", 0.0)),
        token_id=str(token_ids[0]) if token_ids else "",
    )


# ─── CLOB order book ──────────────────────────────────────────────────────────

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
    liquidity = sum(float(b["price"]) * float(b["size"]) for b in bids)

    return {
        "best_bid": best_bid,
        "best_ask": best_ask,
        "mid": mid,
        "spread": spread,
        "liquidity": liquidity,
    }


# ─── Bracket parsing ──────────────────────────────────────────────────────────

def _parse_bracket(label: str) -> tuple[float | None, float | None]:
    """Extract (low, high) bracket bounds in Celsius from a Polymarket temperature outcome label.

    Supports both Celsius (European markets) and Fahrenheit (US markets, auto-detected from label).
    Applies ±0.5 unit boundary adjustment to account for integer rounding in resolution sources.

    Celsius examples:
      "15°C or below"  → (None, 15.5)
      "16°C"           → (15.5, 16.5)
      "25°C or higher" → (24.5, None)

    Fahrenheit examples (converted to Celsius in output):
      "85°F or higher" → ((84.5-32)/1.8, None) = (29.17, None)
      "90°F"           → ((89.5-32)/1.8, (90.5-32)/1.8) = (31.94, 32.50)
    """
    s = label.lower().strip()
    is_fahrenheit = "°f" in s or ("\xb0f" in s) or (
        re.search(r"\d+\s*f\b", s) is not None and "°c" not in s and "\xb0c" not in s
    )
    nums = [float(n) for n in re.findall(r"\d+(?:\.\d+)?", s)]

    is_lower_tail = any(kw in s for kw in ("below", "under", "or below", "or lower", "or less"))
    is_upper_tail = any(kw in s for kw in ("above", "over", "or higher", "or above", "or more"))

    if is_lower_tail and nums:
        return None, _to_celsius(nums[0] + 0.5, is_fahrenheit)

    if is_upper_tail and nums:
        return _to_celsius(nums[0] - 0.5, is_fahrenheit), None

    if len(nums) == 1:
        return _to_celsius(nums[0] - 0.5, is_fahrenheit), _to_celsius(nums[0] + 0.5, is_fahrenheit)

    if len(nums) >= 2:
        # Explicit range — no rounding adjustment, just unit conversion
        return _to_celsius(nums[0], is_fahrenheit), _to_celsius(nums[1], is_fahrenheit)

    _logger.warning("Could not parse bracket bounds from label: %r", label)
    return None, None


def _to_celsius(value: float, is_fahrenheit: bool) -> float:
    if is_fahrenheit:
        return (value - 32.0) / 1.8
    return value


# ─── Utility ──────────────────────────────────────────────────────────────────

def _parse_json_list(value: Any) -> list[str]:
    """Parse a JSON string or list into a Python list of strings."""
    import json
    if isinstance(value, list):
        return [str(v) for v in value]
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
            return [str(v) for v in parsed] if isinstance(parsed, list) else []
        except json.JSONDecodeError:
            return []
    return []
