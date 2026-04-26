from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

_LOGS_DIR = Path(__file__).parent.parent.parent / "logs"


def log_event(
    stage: str,
    station: str,
    status: str,
    duration_ms: float,
    **extra: Any,
) -> None:
    _LOGS_DIR.mkdir(exist_ok=True)
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    path = _LOGS_DIR / f"{today}.jsonl"
    event: dict[str, Any] = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "stage": stage,
        "station": station,
        "status": status,
        "duration_ms": round(duration_ms, 1),
        **extra,
    }
    with open(path, "a") as f:
        f.write(json.dumps(event, default=str) + "\n")
