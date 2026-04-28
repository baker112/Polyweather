"""USDC bankroll tracker — persisted as data/bankroll.json."""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

_PATH = Path(__file__).parents[3] / "data" / "bankroll.json"


def load() -> dict[str, Any]:
    if not _PATH.exists():
        raise FileNotFoundError(
            "Bankroll not initialised. Run: we init-bankroll --usdc <amount>"
        )
    with open(_PATH) as f:
        return json.load(f)


def save(b: dict[str, Any]) -> None:
    b["last_updated"] = datetime.now(timezone.utc).isoformat()
    _PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(_PATH, "w") as f:
        json.dump(b, f, indent=2, default=str)


def init(initial_usdc: float) -> dict[str, Any]:
    b: dict[str, Any] = {
        "initial_usdc": initial_usdc,
        "current_usdc": initial_usdc,
        "reserved_usdc": 0.0,
        "total_pnl": 0.0,
        "n_trades": 0,
    }
    save(b)
    return b


def available(b: dict[str, Any]) -> float:
    return float(b["current_usdc"]) - float(b.get("reserved_usdc", 0.0))


def reserve(b: dict[str, Any], amount: float) -> None:
    b["reserved_usdc"] = float(b.get("reserved_usdc", 0.0)) + amount
    save(b)


def settle(b: dict[str, Any], stake: float, pnl: float) -> None:
    """Called on resolution: release reservation, apply P&L."""
    b["reserved_usdc"] = max(0.0, float(b.get("reserved_usdc", 0.0)) - stake)
    b["current_usdc"] = float(b["current_usdc"]) + pnl
    b["total_pnl"] = float(b.get("total_pnl", 0.0)) + pnl
    b["n_trades"] = int(b.get("n_trades", 0)) + 1
    save(b)
