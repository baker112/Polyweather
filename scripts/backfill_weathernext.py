"""One-shot backfill of WeatherNext 2 historic forecasts (#7).

Usage:
    python scripts/backfill_weathernext.py --start 2022-01-01 --end 2026-05-13
    python scripts/backfill_weathernext.py --start 2024-01-01 --end 2026-05-13 --stations EGLC KLGA
    python scripts/backfill_weathernext.py --start 2022-01-01 --end 2022-01-31 --dry-run

Iterates every active station in monthly windows, queries BigQuery, splits the
returned rows by init_datetime, and writes one parquet per (station, init) into
the existing layout: data/forecasts/model=weathernext/...

Idempotent: skips combinations whose parquet already exists on disk. Re-runs
after partial failure pick up where they left off. The CC BY 4.0 historic
licence applies to anything older than 48 hours; younger inits are real-time
data and fall under the GDM Real-Time Experimental Data ToS.

Required env vars (see src/weather_edge/ingest/weathernext.py for details):
    WEATHERNEXT_PROJECT, WEATHERNEXT_DATASET, GOOGLE_APPLICATION_CREDENTIALS
"""
from __future__ import annotations

import argparse
import logging
import sys
import time
from datetime import date, datetime, timedelta
from pathlib import Path

import polars as pl

# Make 'weather_edge' importable when running as a script.
sys.path.insert(0, str(Path(__file__).parents[1] / "src"))

from weather_edge.config import StationConfig, load_stations  # noqa: E402
from weather_edge.exceptions import IngestError  # noqa: E402
from weather_edge.ingest import weathernext  # noqa: E402
from weather_edge.store import parquet as store  # noqa: E402

_logger = logging.getLogger("backfill_weathernext")
_WINDOW_DAYS = 30  # monthly query chunks
_USD_PER_TIB = 6.25  # BQ on-demand pricing


def _station_init_hours(station: StationConfig) -> tuple[int, ...]:
    """Match the production lock cycle (see lock.py:_most_recent_12z).

    Asian stations lock ~08z and consume the same-day 00z run; European/US
    stations lock 19:30z–21:15z and consume the same-day 12z run. We pull
    both anyway in case lock_time_utc shifts later, but per-station filter
    keeps the BQ scan minimal during backfill.
    """
    return (0, 12)


def _already_have(station_id: str, init_dt: datetime) -> bool:
    """Skip if parquet already exists for this (station, init)."""
    return store.read_forecasts("weathernext", init_dt, station_id) is not None


def estimate_total_cost(targets: list[StationConfig], start: date, end: date) -> tuple[int, float]:
    """Dry-run every monthly chunk for every station and return (bytes, USD).

    Charges nothing — dry_run jobs are free. Use this to decide whether the
    real backfill is within budget before kicking it off.
    """
    total_bytes = 0
    for station in targets:
        cursor = start
        init_hours = _station_init_hours(station)
        while cursor <= end:
            window_end = min(cursor + timedelta(days=_WINDOW_DAYS - 1), end)
            b = weathernext.estimate_historic_bytes(cursor, window_end, station, init_hours)
            total_bytes += b
            _logger.info(
                "[%s] %s → %s  est %.2f GiB ($%.4f)",
                station.icao, cursor, window_end,
                b / 1024**3, (b / 1024**4) * _USD_PER_TIB,
            )
            cursor = window_end + timedelta(days=1)
    usd = (total_bytes / 1024**4) * _USD_PER_TIB
    return total_bytes, usd


def backfill_station(station: StationConfig, start: date, end: date, dry_run: bool) -> dict:
    """Backfill one station in monthly chunks. Returns per-station stats."""
    stats = {"queries": 0, "rows_written": 0, "inits_written": 0, "skipped": 0, "errors": 0}
    cursor = start
    init_hours = _station_init_hours(station)

    while cursor <= end:
        window_end = min(cursor + timedelta(days=_WINDOW_DAYS - 1), end)
        _logger.info("[%s] window %s → %s", station.icao, cursor, window_end)
        t0 = time.monotonic()
        try:
            df = weathernext.ingest_historic(cursor, window_end, station, init_hours=init_hours)
        except IngestError as exc:
            _logger.warning("[%s] %s → %s: %s", station.icao, cursor, window_end, exc)
            stats["errors"] += 1
            cursor = window_end + timedelta(days=1)
            continue
        stats["queries"] += 1

        # Split returned rows by init_datetime and write one parquet per init.
        for init_dt, group in df.group_by("init_datetime"):
            init_value = init_dt[0] if isinstance(init_dt, tuple) else init_dt
            if hasattr(init_value, "to_pydatetime"):
                init_value = init_value.to_pydatetime()
            assert isinstance(init_value, datetime)
            if _already_have(station.icao, init_value):
                stats["skipped"] += 1
                continue
            if not dry_run:
                store.write_forecasts(group, "weathernext", init_value, station.icao)
            stats["inits_written"] += 1
            stats["rows_written"] += len(group)

        _logger.info(
            "[%s] wrote %d inits / %d rows (%.1fs)",
            station.icao,
            stats["inits_written"],
            stats["rows_written"],
            time.monotonic() - t0,
        )
        cursor = window_end + timedelta(days=1)

    return stats


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--start", type=date.fromisoformat, required=True, help="YYYY-MM-DD inclusive")
    parser.add_argument("--end", type=date.fromisoformat, required=True, help="YYYY-MM-DD inclusive")
    parser.add_argument(
        "--stations",
        nargs="*",
        default=None,
        help="Specific ICAOs to backfill (default: all active in config/stations.yaml)",
    )
    parser.add_argument("--dry-run", action="store_true", help="Query but don't write parquet")
    parser.add_argument(
        "--estimate-cost",
        action="store_true",
        help="Dry-run every chunk and print total bytes + USD, then exit. Charges nothing.",
    )
    parser.add_argument(
        "--max-cost-usd",
        type=float,
        default=None,
        help="Abort if estimated total exceeds this USD budget (runs --estimate-cost first).",
    )
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args()

    logging.basicConfig(
        level=args.log_level,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )

    all_stations = load_stations()
    targets = (
        [all_stations[s.upper()] for s in args.stations if s.upper() in all_stations]
        if args.stations
        else list(all_stations.values())
    )
    if not targets:
        raise SystemExit("No matching stations.")

    _logger.info(
        "Backfill: %d station(s) × %s → %s (%s)",
        len(targets), args.start, args.end, "DRY RUN" if args.dry_run else "live writes",
    )

    if args.estimate_cost or args.max_cost_usd is not None:
        bytes_, usd = estimate_total_cost(targets, args.start, args.end)
        _logger.info(
            "ESTIMATE  total %.2f GiB  $%.4f at $%.2f/TiB",
            bytes_ / 1024**3, usd, _USD_PER_TIB,
        )
        if args.estimate_cost:
            return
        if args.max_cost_usd is not None and usd > args.max_cost_usd:
            raise SystemExit(
                f"Estimated cost ${usd:.4f} exceeds budget ${args.max_cost_usd:.4f}; aborting."
            )

    grand = {"queries": 0, "rows_written": 0, "inits_written": 0, "skipped": 0, "errors": 0}
    for station in targets:
        s = backfill_station(station, args.start, args.end, args.dry_run)
        for k in grand:
            grand[k] += s[k]
        _logger.info("[%s] DONE  %s", station.icao, s)

    _logger.info("ALL DONE  %s", grand)


if __name__ == "__main__":
    main()
