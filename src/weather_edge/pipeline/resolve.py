"""Resolution tracking — match locked picks against resolved Polymarket outcomes."""
from __future__ import annotations

import asyncio
import logging
from datetime import date
from typing import Any

from weather_edge.config import get_station
from weather_edge.store import parquet as store

_logger = logging.getLogger(__name__)


async def resolve_date(
    station_id: str,
    target_date: date,
    force: bool = False,
) -> dict[str, Any]:
    """Fetch resolved market for target_date and compute P&L against locked picks.

    Returns a resolution record. If the market hasn't resolved yet, returns
    {"resolved": False}. Persists result to data/resolutions/.
    """
    existing = store.read_resolution(station_id, target_date)
    if existing and existing.get("resolved") and not force:
        return existing

    station = get_station(station_id)
    slug = station.market_slug_pattern.format(
        month_lower=target_date.strftime("%B").lower(),
        day=target_date.day,
        year=target_date.year,
    )

    from weather_edge.market.polymarket import fetch_market
    snapshot = await fetch_market(slug, station_id, target_date)

    # Resolved markets: winning outcome has mid ~1.0, others ~0.0
    resolved_label: str | None = None
    for outcome in snapshot.outcomes:
        if outcome.mid >= 0.98:
            resolved_label = outcome.label
            break

    record: dict[str, Any] = {
        "station": station_id,
        "date": str(target_date),
        "resolved": resolved_label is not None,
        "resolved_label": resolved_label,
        "picks_pnl": [],
        "total_pnl_per_unit": 0.0,
    }

    if resolved_label is None:
        _logger.info("Market for %s %s not yet resolved", station_id, target_date)
        store.write_resolution(record, station_id, target_date)
        return record

    # Load picks and compute P&L
    picks_data = store.read_picks(station_id, target_date)
    picks = (picks_data or {}).get("picks", [])

    pnl_records: list[dict[str, Any]] = []
    for pick in picks:
        label = pick["bracket_label"]
        side = pick["side"]
        market_prob = pick["market_prob"]  # YES token mid-price at lock time

        if side == "YES":
            entry_price = market_prob
            correct = (label == resolved_label)
        else:
            entry_price = 1.0 - market_prob  # NO token price
            correct = (label != resolved_label)

        pnl_per_unit = (1.0 - entry_price) if correct else -entry_price

        pnl_records.append({
            "bracket_label": label,
            "side": side,
            "entry_price": round(entry_price, 4),
            "resolved_label": resolved_label,
            "correct": correct,
            "pnl_per_unit": round(pnl_per_unit, 4),
        })

    record["picks_pnl"] = pnl_records
    record["total_pnl_per_unit"] = round(sum(r["pnl_per_unit"] for r in pnl_records), 4)

    path = store.write_resolution(record, station_id, target_date)
    _logger.info(
        "Resolved %s %s: %s  P&L=%.4f  -> %s",
        station_id, target_date, resolved_label, record["total_pnl_per_unit"], path,
    )
    return record


def resolve_range(
    station_id: str,
    start: date,
    end: date,
    force: bool = False,
) -> list[dict[str, Any]]:
    """Resolve all dates in range. Skips already-resolved unless force=True."""
    from datetime import timedelta
    results = []
    current = start
    while current <= end:
        try:
            rec = asyncio.run(resolve_date(station_id, current, force=force))
            results.append(rec)
        except Exception as exc:
            _logger.warning("Resolution failed for %s %s: %s", station_id, current, exc)
        current += timedelta(days=1)
    return results
