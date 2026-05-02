"""USDC bankroll tracker — persisted as data/bankroll.json."""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

_PATH = Path(__file__).parents[3] / "data" / "bankroll.json"
_DRY_PATH = Path(__file__).parents[3] / "data" / "dry_bankroll.json"
DRY_INITIAL_USDC = 100.0  # paper-trading bankroll seed


def load() -> dict[str, Any]:
    if not _PATH.exists():
        raise FileNotFoundError(
            "Bankroll not initialised. Run: we init-bankroll --usdc <amount>"
        )
    try:
        with open(_PATH) as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError) as exc:
        # Quarantine the corrupt file so its bytes are preserved and the next
        # writer isn't blocked, then surface a clear error. Don't silently
        # re-init: this holds real $$$, so the user must confirm recovery.
        try:
            ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
            quarantine = _PATH.with_name(f"{_PATH.name}.corrupt-{ts}")
            _PATH.rename(quarantine)
            hint = f" (corrupt file moved to {quarantine.name})"
        except OSError:
            hint = ""
        raise RuntimeError(
            f"Bankroll file at {_PATH} is corrupt ({exc}){hint}. "
            "Inspect the quarantined file, then re-run: we init-bankroll --usdc <amount>"
        ) from exc


def save(b: dict[str, Any]) -> None:
    b["last_updated"] = datetime.now(timezone.utc).isoformat()
    _PATH.parent.mkdir(parents=True, exist_ok=True)
    _dump(b, _PATH)


def _dump(b: dict[str, Any], path: Path) -> None:
    import math as _math
    def _scrub(v: Any) -> Any:
        if isinstance(v, float) and not _math.isfinite(v):
            return None
        if isinstance(v, dict):
            return {k: _scrub(x) for k, x in v.items()}
        if isinstance(v, (list, tuple)):
            return [_scrub(x) for x in v]
        return v
    def _default(o: Any) -> str:
        if isinstance(o, datetime):
            return o.isoformat()
        return str(o)
    with open(path, "w") as f:
        json.dump(_scrub(b), f, indent=2, default=_default, allow_nan=False)


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


# ─── Dry-run (paper) bankroll ──────────────────────────────────────────────────
# Independent of the live bankroll. Auto-initialises at $DRY_INITIAL_USDC the
# first time it's loaded so paper-trading P&L can accrue without ceremony.

def _save_dry(b: dict[str, Any]) -> None:
    b["last_updated"] = datetime.now(timezone.utc).isoformat()
    _DRY_PATH.parent.mkdir(parents=True, exist_ok=True)
    _dump(b, _DRY_PATH)


def load_dry() -> dict[str, Any]:
    if _DRY_PATH.exists():
        try:
            with open(_DRY_PATH) as f:
                return json.load(f)
        except (json.JSONDecodeError, OSError):
            # Dry bankroll is paper-trading only — silently re-seed if corrupt.
            pass
    b: dict[str, Any] = {
        "initial_usdc": DRY_INITIAL_USDC,
        "current_usdc": DRY_INITIAL_USDC,
        "reserved_usdc": 0.0,
        "total_pnl": 0.0,
        "n_trades": 0,
    }
    _save_dry(b)
    return b


def reserve_dry(b: dict[str, Any], amount: float) -> None:
    b["reserved_usdc"] = float(b.get("reserved_usdc", 0.0)) + amount
    _save_dry(b)


def settle_dry(b: dict[str, Any], stake: float, pnl: float) -> None:
    b["reserved_usdc"] = max(0.0, float(b.get("reserved_usdc", 0.0)) - stake)
    b["current_usdc"] = float(b["current_usdc"]) + pnl
    b["total_pnl"] = float(b.get("total_pnl", 0.0)) + pnl
    b["n_trades"] = int(b.get("n_trades", 0)) + 1
    _save_dry(b)
