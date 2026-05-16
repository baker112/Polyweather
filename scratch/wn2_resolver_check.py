"""Sanity check the WN2 URI resolver — no network reads.

Calls _resolve_uris with various init windows and prints the URIs it
generates so we can eyeball that the year-boundary logic and per-init
filename pattern are right.

Run: python scratch/wn2_resolver_check.py
"""
from datetime import datetime, timezone

from weather_edge.ingest.weathernext import _resolve_uris


def show(label: str, start: datetime, end: datetime, init_hours=None) -> None:
    print(f"\n=== {label} ===")
    print(f"  window: {start.isoformat()} → {end.isoformat()}")
    if init_hours:
        print(f"  hours: {init_hours}")
    uris = _resolve_uris(start, end, init_hours)
    print(f"  -> {len(uris)} store(s)")
    for uri, kind in uris[:6]:
        print(f"     [{kind:9s}] {uri}")
    if len(uris) > 6:
        print(f"     ... and {len(uris) - 6} more")


# Single historic init (2023)
show(
    "single historic init",
    datetime(2023, 6, 15, 12, tzinfo=timezone.utc),
    datetime(2023, 6, 15, 12, tzinfo=timezone.utc),
)

# Multi-year historic backfill (2022-2024)
show(
    "3-year historic backfill",
    datetime(2022, 1, 1, tzinfo=timezone.utc),
    datetime(2024, 12, 31, 23, tzinfo=timezone.utc),
)

# Single per-init in 2025
show(
    "single per-init (2025)",
    datetime(2025, 1, 1, 0, tzinfo=timezone.utc),
    datetime(2025, 1, 1, 0, tzinfo=timezone.utc),
)

# 2025 first week, all 4 daily inits
show(
    "first week 2025, all hours",
    datetime(2025, 1, 1, 0, tzinfo=timezone.utc),
    datetime(2025, 1, 7, 23, tzinfo=timezone.utc),
)

# Boundary spanning 2024 → 2025
show(
    "spans 2024-2025 boundary",
    datetime(2024, 12, 30, 0, tzinfo=timezone.utc),
    datetime(2025, 1, 2, 23, tzinfo=timezone.utc),
    init_hours=(0, 12),
)

# Spans 2023-2025 (multi-year + per-init)
show(
    "2023-2025 mixed",
    datetime(2023, 6, 1, tzinfo=timezone.utc),
    datetime(2025, 6, 1, tzinfo=timezone.utc),
    init_hours=(0, 12),
)

print("\nDone. No network reads — pure URI generation check.")
