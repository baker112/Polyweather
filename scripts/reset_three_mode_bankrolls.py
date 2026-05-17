"""One-time migration: split the single dry bankroll into three per-mode files.

What it does (idempotent):
  1. Reads data/dry_bankroll.json if present.
  2. Snapshots it to data/dry_bankroll.pre_split.bak.json so the old state
     is preserved (in case the operator wants to inspect or restore).
  3. Creates fresh data/bankroll_dry_{bma,intraday,peak}.json at $100 each.
  4. Sends a one-time Telegram heads-up announcing the reset (skipped if
     the Telegram env vars aren't set or the lib isn't importable).

Re-running the script does NOT re-archive or re-seed — the snapshot has
a sibling `.completed` marker so subsequent runs are a no-op. Remove the
marker if a fresh wipe is required.

Run:
    python -m scripts.reset_three_mode_bankrolls
or:
    python scripts/reset_three_mode_bankrolls.py
"""
from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / "src"))

from weather_edge.execution import bankroll as br  # noqa: E402

DATA = REPO / "data"
LEGACY = DATA / "dry_bankroll.json"
BACKUP = DATA / "dry_bankroll.pre_split.bak.json"
MARKER = DATA / "dry_bankroll.pre_split.completed"
SEED_USDC = 100.0


def _print(msg: str) -> None:
    print(msg)


def main() -> int:
    if MARKER.exists():
        _print(f"Migration already completed (marker {MARKER.name} exists). No-op.")
        return 0

    DATA.mkdir(parents=True, exist_ok=True)

    # Step 1: archive the existing singleton (if any)
    archived = None
    if LEGACY.exists():
        try:
            existing = json.loads(LEGACY.read_text())
        except json.JSONDecodeError as exc:
            _print(f"Refusing to migrate: legacy file at {LEGACY} is corrupt ({exc})")
            return 1
        BACKUP.write_text(json.dumps(existing, indent=2))
        archived = existing
        _print(f"Archived {LEGACY.name} -> {BACKUP.name}")
    else:
        _print(f"No legacy {LEGACY.name} to archive — proceeding to fresh seed.")

    # Step 2: seed each mode's bankroll at SEED_USDC. We overwrite any partial
    # state because this is the canonical one-time reset.
    for mode in br.DRY_MODES:
        path = br._dry_path(mode)
        b = {
            "initial_usdc": SEED_USDC,
            "current_usdc": SEED_USDC,
            "reserved_usdc": 0.0,
            "total_pnl": 0.0,
            "n_trades": 0,
        }
        br._save_dry(b, mode=mode)
        _print(f"Seeded {path.name} at ${SEED_USDC:.0f}")

    # Step 3: drop the marker
    MARKER.write_text(datetime.now(timezone.utc).isoformat() + "\n")

    # Step 4: best-effort Telegram heads-up (only if env vars are wired)
    try:
        from weather_edge import telegram as _tg  # type: ignore
        body = [
            "🔄 <b>Three-mode bankroll split applied</b>",
            f"Archived old paper bankroll -> {BACKUP.name}",
            "",
            f"  📊 bma:       ${SEED_USDC:.2f}",
            f"  ⚡ intraday:  ${SEED_USDC:.2f}",
            f"  🎯 peak:      ${SEED_USDC:.2f}",
        ]
        if archived is not None:
            body.append("")
            body.append(
                f"Previous singleton state: ${archived.get('current_usdc', 0):.2f}, "
                f"P&amp;L=${archived.get('total_pnl', 0):+.2f}, "
                f"{archived.get('n_trades', 0)} trades."
            )
        _tg.send("\n".join(body))
        _print("Telegram heads-up sent.")
    except Exception as exc:
        _print(f"Skipped Telegram notification: {exc}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
