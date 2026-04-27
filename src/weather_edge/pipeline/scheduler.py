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

    for model_name, fetch_fn in [("ecmwf", ecmwf.ingest_forecasts), ("gefs", gefs.ingest_forecasts)]:
        try:
            df = fetch_fn(init_dt, cfg)
            store.write_forecasts(df, model_name, init_dt, station_id)
            _logger.info("Ingested %s %s: %d rows", model_name, init_dt, len(df))
        except Exception as exc:
            _logger.error("Ingest %s failed: %s", model_name, exc)


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
    except AlreadyLockedError:
        _logger.info("Already locked %s %s", station_id, target_date)
    except Exception as exc:
        _logger.error("Lock failed %s %s: %s", station_id, target_date, exc)


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
        _logger.info("Resolved %s %s: %s", station_id, yesterday, rec.get("resolved_label"))
    except Exception as exc:
        _logger.error("Resolution failed %s %s: %s", station_id, yesterday, exc)


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
        )
        sched.add_job(
            _lock_job, CronTrigger(hour=18, minute=0),
            args=[station_id], id=f"lock_{station_id}", name=f"Lock {station_id}",
        )
        sched.add_job(
            _resolve_and_observe_job, CronTrigger(hour=2, minute=0),
            args=[station_id], id=f"resolve_{station_id}", name=f"Resolve {station_id}",
        )
        sched.add_job(
            _refit_job, CronTrigger(day_of_week="sun", hour=3, minute=0),
            args=[station_id], id=f"refit_{station_id}", name=f"Refit {station_id}",
        )

    _logger.info("Scheduler started for stations: %s", stations)
    _logger.info("Jobs: ingest@17:30z, lock@18:00z, resolve+obs@02:00z, refit@Sun03:00z")
    sched.start()
