"""One-off: record a manually placed bet into the executions store."""
import json
from pathlib import Path

record = {
    "bracket_label": "16°C",
    "side": "NO",
    "token_id": "",
    "price": 0.55,
    "shares": round(1.21 / 0.55, 2),
    "usdc_stake": 1.21,
    "edge": None,
    "kelly_fraction": None,
    "dry_run": False,
    "submitted_at": "2026-04-28T18:05:00+00:00",
    "order_id": "manual",
    "status": "submitted",
}

path = Path(__file__).parents[1] / "data" / "executions" / "station=EGLC" / "date=2026-04-28"
path.mkdir(parents=True, exist_ok=True)
out = path / "manual.json"
out.write_text(json.dumps([record], indent=2))
print(f"Saved: {out}")
