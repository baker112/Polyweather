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

    with open(path, "a") as f:
        f.write(json.dumps(_scrub(event), default=_default, allow_nan=False) + "\n")
