"""Live order execution via Polymarket CLOB API.

Required env vars:
  POLYMARKET_PK          Ethereum private key (0x-prefixed hex)
  CLOB_API_KEY           } from `we setup-clob` or Polymarket dashboard
  CLOB_SECRET            }
  CLOB_PASS_PHRASE       }

Minimum order size enforced: MIN_ORDER_USDC (default $1).
"""
from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from weather_edge.models import Candidate, MarketOutcome
from weather_edge import telegram as _tg

_logger = logging.getLogger(__name__)
_CLOB_HOST = "https://clob.polymarket.com"
_EXECUTIONS_DIR = Path(__file__).parents[4] / "data" / "executions"
MIN_ORDER_USDC = 1.0  # Polymarket minimum


def _client() -> Any:
    try:
        from py_clob_client.client import ClobClient  # type: ignore[import-untyped]
        from py_clob_client.clob_types import ApiCreds  # type: ignore[import-untyped]
        from py_clob_client.constants import POLYGON  # type: ignore[import-untyped]
    except ImportError as exc:
        raise RuntimeError(
            "py-clob-client not installed. Run: pip install py-clob-client"
        ) from exc

    pk = os.getenv("POLYMARKET_PK")
    api_key = os.getenv("CLOB_API_KEY")
    api_secret = os.getenv("CLOB_SECRET")
    api_pass = os.getenv("CLOB_PASS_PHRASE")

    if not pk:
        raise RuntimeError(
            "POLYMARKET_PK environment variable not set.\n"
            "Export your Ethereum private key: export POLYMARKET_PK=0x..."
        )

    creds = None
    if api_key and api_secret and api_pass:
        creds = ApiCreds(
            api_key=api_key,
            api_secret=api_secret,
            api_passphrase=api_pass,
        )

    return ClobClient(
        host=_CLOB_HOST,
        key=pk,
        chain_id=POLYGON,
        creds=creds,
    )


def derive_api_creds() -> dict[str, str]:
    """Derive L2 API credentials from the private key. Call once during setup."""
    client = _client()
    creds = client.create_or_derive_api_creds()
    return {
        "CLOB_API_KEY": creds.api_key,
        "CLOB_SECRET": creds.api_secret,
        "CLOB_PASS_PHRASE": creds.api_passphrase,
    }


def place_order(
    pick: Candidate,
    outcome: MarketOutcome,
    usdc_stake: float,
    dry_run: bool = True,
) -> dict[str, Any]:
    """Place a single limit order for a pick. Returns execution record.

    Args:
        pick: The Candidate to trade.
        outcome: The MarketOutcome with token IDs and prices.
        usdc_stake: Amount of USDC to risk (Kelly × bankroll).
        dry_run: If True, skip actual order submission.
    """
    if usdc_stake < MIN_ORDER_USDC:
        raise ValueError(
            f"Order size ${usdc_stake:.2f} below minimum ${MIN_ORDER_USDC:.2f}. "
            "Increase bankroll or wait for higher Kelly signal."
        )

    if pick.side == "YES":
        token_id = outcome.token_id
        price = round(outcome.mid, 2)
    else:
        token_id = outcome.no_token_id or ""
        if not token_id:
            raise ValueError(f"No NO token_id for outcome {outcome.label}")
        price = round(1.0 - outcome.mid, 2)

    # CLOB size = number of shares; 1 share = $price USDC
    shares = round(usdc_stake / price, 2)

    record: dict[str, Any] = {
        "bracket_label": pick.bracket_label,
        "side": pick.side,
        "token_id": token_id,
        "price": price,
        "shares": shares,
        "usdc_stake": usdc_stake,
        "edge": pick.edge,
        "kelly_fraction": pick.kelly_fraction,
        "dry_run": dry_run,
        "submitted_at": datetime.now(timezone.utc).isoformat(),
        "order_id": None,
        "status": "dry_run" if dry_run else "pending",
    }

    if not dry_run:
        try:
            from py_clob_client.clob_types import OrderArgs, OrderType  # type: ignore[import-untyped]
            from py_clob_client.order_builder.constants import BUY  # type: ignore[import-untyped]
        except ImportError as exc:
            raise RuntimeError("py-clob-client not installed") from exc

        client = _client()

        # One-time USDC + CTF approval (no-op if already approved)
        try:
            client.set_allowances()
        except Exception as exc:
            _logger.warning("set_allowances failed (may already be set): %s", exc)

        # Neg-risk markets require a flag in the signed order
        try:
            neg_risk: bool = client.get_neg_risk(token_id)
        except Exception:
            neg_risk = False

        try:
            signed = client.create_order(OrderArgs(
                token_id=token_id,
                price=price,
                size=shares,
                side=BUY,
                neg_risk=neg_risk,
            ))
            resp = client.post_order(signed, OrderType.GTC)
        except Exception as exc:
            # Surface the raw API error body for diagnosis
            body = getattr(exc, "body", None) or getattr(exc, "message", None) or str(exc)
            _logger.error("CLOB order rejected (token=%s price=%.2f size=%.2f neg_risk=%s): %s",
                          token_id, price, shares, neg_risk, body)
            raise
        order_id = resp.get("orderID") or resp.get("id", "unknown")
        record["order_id"] = order_id
        record["status"] = "submitted"
        record["raw_response"] = resp
        _logger.info(
            "Order placed: %s %s %.2f shares @ %.2f → order_id=%s",
            pick.side, pick.bracket_label, shares, price, order_id
        )
        _tg.send(
            f"Bet placed: {pick.side} {pick.bracket_label}\n"
            f"{shares:.2f} shares @ {price:.2f}  (${usdc_stake:.2f} stake)\n"
            f"order_id: {order_id}"
        )
    else:
        _logger.info(
            "DRY RUN: would buy %s %.2f shares @ %.2f (stake $%.2f)",
            pick.side, shares, price, usdc_stake
        )
        _tg.send(
            f"[DRY RUN] Would bet: {pick.side} {pick.bracket_label}\n"
            f"{shares:.2f} shares @ {price:.2f}  (${usdc_stake:.2f} stake)"
        )

    return record


def save_execution(
    station: str,
    target_date: Any,
    records: list[dict[str, Any]],
) -> Path:
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    path = (
        _EXECUTIONS_DIR
        / f"station={station}"
        / f"date={target_date}"
        / f"{ts}.json"
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        json.dump(records, f, indent=2, default=str)
    return path


def load_executions(station: str, target_date: Any) -> list[dict[str, Any]]:
    base = _EXECUTIONS_DIR / f"station={station}" / f"date={target_date}"
    if not base.exists():
        return []
    records: list[dict[str, Any]] = []
    for p in sorted(base.glob("*.json")):
        with open(p) as f:
            records.extend(json.load(f))
    return records
