"""Daily pipeline scheduler using APScheduler.

Jobs (all UTC):
  17:30z — ingest ECMWF + GEFS forecasts (12z run usually published by ~17z)
  18:00z — lock picks for D+1
  02:00z — ingest yesterday's observations + resolve yesterday's market
  Sunday 03:00z — re-fit EMOS and QRF models
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone

from weather_edge import telegram as _tg

_logger = logging.getLogger(__name__)


def _ingest_job(station_id: str) -> None:
    from datetime import timedelta

    from weather_edge.config import get_station
    from weather_edge.ingest import ecmwf, gefs
    from weather_edge.pipeline.lock import _most_recent_12z
    from weather_edge.store import parquet as store

    now_utc = datetime.now(timezone.utc)
    init_dt = _most_recent_12z(now_utc)
    cfg = get_station(station_id)

    results: list[str] = []
    for model_name, fetch_fn in [("ecmwf", ecmwf.ingest_forecasts), ("gefs", gefs.ingest_forecasts)]:
        try:
            df = fetch_fn(init_dt, cfg)
            store.write_forecasts(df, model_name, init_dt, station_id)
            _logger.info("Ingested %s %s: %d rows", model_name, init_dt, len(df))
            results.append(f"{model_name}: {len(df)} rows")
        except Exception as exc:
            _logger.error("Ingest %s failed: %s", model_name, exc)
            results.append(f"{model_name}: FAILED ({exc})")
    _tg.send(f"Ingest {station_id} {init_dt.date()}\n" + "\n".join(results))


def _lock_job(station_id: str) -> None:
    import asyncio
    from datetime import timedelta

    from weather_edge.pipeline.lock import lock_picks
    from weather_edge.exceptions import AlreadyLockedError

    now_utc = datetime.now(timezone.utc)
    target_date = (now_utc + timedelta(days=1)).date()
    try:
        result = lock_picks(target_date, station_id, now_utc)
        _logger.info(
            "Locked %s %s: mu=%.2f sigma=%.2f picks=%d",
            station_id, target_date, result.mu, result.sigma, len(result.picks),
        )
        if result.picks:
            lines = [f"Pick locked: {station_id} {target_date}  (mu={result.mu:.1f}C, sigma={result.sigma:.2f})"]
            for p in result.picks:
                lines.append(
                    f"  {p.side} {p.bracket_label}  model={p.model_prob:.2f}  mkt={p.market_prob:.2f}  edge={p.edge:+.3f}"
                )
            _tg.send("\n".join(lines))
        else:
            _tg.send(f"No edge: {station_id} {target_date}\n{result.no_edge_reason or 'all edges below threshold'}")
    except AlreadyLockedError:
        _logger.info("Already locked %s %s", station_id, target_date)
    except Exception as exc:
        _logger.error("Lock failed %s %s: %s", station_id, target_date, exc)
        _tg.send(f"Lock FAILED: {station_id} {target_date}\n{exc}")


def _resolve_and_observe_job(station_id: str) -> None:
    import asyncio
    from datetime import timedelta

    from weather_edge.config import get_station
    from weather_edge.ingest.metar import fetch_observations
    from weather_edge.pipeline.resolve import resolve_date
    from weather_edge.store import parquet as store

    now_utc = datetime.now(timezone.utc)
    yesterday = (now_utc - timedelta(days=1)).date()
    cfg = get_station(station_id)

    try:
        import asyncio as _asyncio
        df = _asyncio.run(fetch_observations(cfg, yesterday, yesterday))
        if not df.is_empty():
            store.write_observations(df, station_id)
            _logger.info("Ingested observations for %s %s", station_id, yesterday)
    except Exception as exc:
        _logger.error("Observation ingest failed: %s", exc)

    try:
        rec = asyncio.run(resolve_date(station_id, yesterday))
        resolved_label = rec.get("resolved_label", "unknown")
        _logger.info("Resolved %s %s: %s", station_id, yesterday, resolved_label)

        from weather_edge.execution.polymarket_exec import load_executions
        import json
        from pathlib import Path

        execs = load_executions(station_id, yesterday)
        if execs:
            clv_path = Path(__file__).parents[4] / "data" / "clv_snapshots" / f"station={station_id}" / f"{yesterday}.json"
            clv_outcomes: dict[str, float] = {}
            if clv_path.exists():
                try:
                    clv_data = json.loads(clv_path.read_text())
                    clv_outcomes = {o["label"]: o["mid"] for o in clv_data.get("outcomes", [])}
                except Exception:
                    pass

            lines = [f"Result: {station_id} {yesterday}  resolved={resolved_label}"]
            for e in execs:
                if e.get("dry_run"):
                    continue
                bracket = e.get("bracket_label", "?")
                side = e.get("side", "?")
                entry = float(e.get("price", 0))
                stake = float(e.get("usdc_stake", 0))
                win = (bracket == resolved_label and side == "YES") or (bracket != resolved_label and side == "NO")
                pnl = (1.0 / entry - 1) * stake if win else -stake
                line = f"  {side} {bracket}: {'WIN' if win else 'LOSS'}  entry={entry:.2f}  P&L=${pnl:+.2f}"
                if bracket in clv_outcomes:
                    closing = clv_outcomes[bracket]
                    clv = (closing - entry) if side == "YES" else (entry - closing)
                    line += f"  CLV={clv:+.3f} (close={closing:.2f})"
                lines.append(line)
            if len(lines) > 1:
                _tg.send("\n".join(lines))
    except Exception as exc:
        _logger.error("Resolution failed %s %s: %s", station_id, yesterday, exc)
        _tg.send(f"Resolution FAILED: {station_id} {yesterday}\n{exc}")


def _closing_snapshot_job(station_id: str) -> None:
    """Snapshot the market price at ~01:00z for closing-line value tracking."""
    import asyncio
    import json
    from datetime import timedelta
    from pathlib import Path

    from weather_edge.config import get_station
    from weather_edge.market.polymarket import fetch_market

    now_utc = datetime.now(timezone.utc)
    yesterday = (now_utc - timedelta(days=1)).date()
    cfg = get_station(station_id)
    slug = cfg.market_slug_pattern.format(
        date=yesterday.strftime("%Y-%m-%d"),
        month_lower=yesterday.strftime("%B").lower(),
        day=yesterday.day,
        year=yesterday.year,
    )

    try:
        snapshot = asyncio.run(fetch_market(slug, station_id, yesterday))
        clv_dir = Path(__file__).parents[4] / "data" / "clv_snapshots" / f"station={station_id}"
        clv_dir.mkdir(parents=True, exist_ok=True)
        clv_path = clv_dir / f"{yesterday}.json"
        with open(clv_path, "w") as f:
            json.dump(snapshot.model_dump(), f, default=str)
        _logger.info("CLV snapshot saved for %s %s", station_id, yesterday)
    except Exception as exc:
        _logger.warning("CLV snapshot failed %s %s: %s", station_id, yesterday, exc)


def _refit_job(station_id: str) -> None:
    from datetime import date

    from weather_edge.postprocess.emos import assemble_training_pairs, fit_emos
    from weather_edge.postprocess.qrf import assemble_qrf_training_pairs, fit_qrf
    from weather_edge.store import parquet as store

    now_utc = datetime.now(timezone.utc)
    as_of = now_utc.date()

    for lead in (24, 48, 72):
        pairs = assemble_training_pairs(station_id, lead, as_of)
        if len(pairs) >= 10:
            params = fit_emos(pairs, station_id, lead)
            store.write_emos_params(params.model_dump(), station_id, lead, params.valid_from)
            _logger.info("Re-fitted EMOS %s lead=%dh n=%d CRPS=%.4f", station_id, lead, params.n_samples, params.train_crps)

    pairs_qrf = assemble_qrf_training_pairs(station_id, 24, as_of, window_days=90)
    if len(pairs_qrf) >= 10:
        forest, X, y, meta = fit_qrf(pairs_qrf, station_id, 24)
        store.write_qrf_params(forest, X, y, meta, station_id, 24, now_utc)
        _logger.info("Re-fitted QRF %s n=%d", station_id, len(pairs_qrf))


def start(stations: list[str]) -> None:
    """Start the blocking daily scheduler."""
    try:
        from apscheduler.schedulers.blocking import BlockingScheduler
        from apscheduler.triggers.cron import CronTrigger
    except ImportError:
        raise ImportError("apscheduler not installed — run: pip install apscheduler")

    sched = BlockingScheduler(timezone="UTC")

    for station_id in stations:
        sched.add_job(
            _ingest_job, CronTrigger(hour=17, minute=30),
            args=[station_id], id=f"ingest_{station_id}", name=f"Ingest {station_id}",
            max_instances=1,
        )
        sched.add_job(
            _lock_job, CronTrigger(hour=18, minute=0),
            args=[station_id], id=f"lock_{station_id}", name=f"Lock {station_id}",
            max_instances=1,
        )
        sched.add_job(
            _closing_snapshot_job, CronTrigger(hour=1, minute=0),
            args=[station_id], id=f"clv_{station_id}", name=f"CLV snapshot {station_id}",
            max_instances=1,
        )
        sched.add_job(
            _resolve_and_observe_job, CronTrigger(hour=2, minute=0),
            args=[station_id], id=f"resolve_{station_id}", name=f"Resolve {station_id}",
            max_instances=1,
        )
        sched.add_job(
            _refit_job, CronTrigger(day_of_week="sun", hour=3, minute=0),
            args=[station_id], id=f"refit_{station_id}", name=f"Refit {station_id}",
            max_instances=1,
        )

    _logger.info("Scheduler started for stations: %s", stations)
    _logger.info("Jobs: ingest@17:30z, lock@18:00z, resolve+obs@02:00z, refit@Sun03:00z")

    def _status() -> str:
        now = datetime.now(timezone.utc)
        lines = [f"Scheduler running — {now.strftime('%Y-%m-%d %H:%M')} UTC", ""]
        for job in sched.get_jobs():
            next_run = job.next_run_time
            if next_run:
                delta = next_run - now
                h, m = divmod(int(delta.total_seconds()) // 60, 60)
                lines.append(f"{job.name}: next in {h}h {m}m ({next_run.strftime('%H:%M')}z)")
            else:
                lines.append(f"{job.name}: not scheduled")
        return "\n".join(lines)

    _tg.start_command_listener(_status)
    _tg.send(f"Scheduler started — stations: {', '.join(stations)}\nSend /status to check next job times.")
    sched.start()
