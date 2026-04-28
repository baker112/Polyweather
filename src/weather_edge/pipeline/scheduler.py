"""Daily pipeline scheduler using APScheduler.

Jobs (all UTC, staggered 2-3 min per station):
  ~17:30z — ingest ECMWF + GEFS forecasts (12z run usually published by ~17z)
  lock_time_utc — lock picks for D+1 (per-station, set in config/stations.yaml)
  lock_time_utc+5m — execute orders
  ~01:00z — CLV closing snapshot
  ~02:00z — ingest yesterday's observations + resolve yesterday's market
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

        from weather_edge.execution import bankroll as br
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

            try:
                bankroll_data = br.load()
            except FileNotFoundError:
                bankroll_data = None

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
                if bankroll_data is not None:
                    try:
                        br.settle(bankroll_data, stake, pnl)
                    except Exception as exc:
                        _logger.warning("Bankroll settle failed: %s", exc)
            if len(lines) > 1:
                _tg.send("\n".join(lines))
    except Exception as exc:
        _logger.error("Resolution failed %s %s: %s", station_id, yesterday, exc)
        _tg.send(f"Resolution FAILED: {station_id} {yesterday}\n{exc}")


def _execute_job(station_id: str) -> None:
    """Place orders for tomorrow's locked picks. Dry-run unless LIVE_TRADING=true."""
    import asyncio
    import os
    from datetime import timedelta

    from weather_edge.config import get_station
    from weather_edge.execution import bankroll as br
    from weather_edge.execution.polymarket_exec import MIN_ORDER_USDC, place_order, save_execution
    from weather_edge.market.polymarket import fetch_market
    from weather_edge.models import LockedPicks, MarketSnapshot
    from weather_edge.store import parquet as store

    dry_run = os.getenv("LIVE_TRADING", "false").lower() != "true"
    now_utc = datetime.now(timezone.utc)
    target_date = (now_utc + timedelta(days=1)).date()

    picks_data = store.read_picks(station_id, target_date)
    if not picks_data or not picks_data.get("picks"):
        _logger.info("No picks to execute for %s %s", station_id, target_date)
        return

    locked = LockedPicks(**picks_data)

    try:
        bankroll = br.load()
    except FileNotFoundError:
        _tg.send(f"Execute FAILED {station_id}: bankroll not initialised. Run: we init-bankroll --usdc <amount>")
        return

    avail = br.available(bankroll)
    cfg = get_station(station_id)
    slug = cfg.market_slug_pattern.format(
        date=target_date.strftime("%Y-%m-%d"),
        month_lower=target_date.strftime("%B").lower(),
        day=target_date.day,
        year=target_date.year,
    )

    try:
        snapshot = asyncio.run(fetch_market(slug, station_id, target_date))
        store.write_market_snapshot(snapshot.model_dump(), station_id, target_date)
    except Exception as exc:
        _tg.send(f"Execute FAILED {station_id}: market fetch error\n{exc}")
        return

    outcome_map = {o.label: o for o in snapshot.outcomes}
    records = []
    total_staked = 0.0

    for pick in locked.picks:
        outcome = outcome_map.get(pick.bracket_label)
        if outcome is None:
            continue
        usdc_stake = round(pick.kelly_fraction * avail, 2)
        if usdc_stake < MIN_ORDER_USDC:
            _logger.info("Skipping %s: stake $%.2f below minimum", pick.bracket_label, usdc_stake)
            continue
        try:
            rec = place_order(pick, outcome, usdc_stake, dry_run=dry_run)
            records.append(rec)
            total_staked += usdc_stake
        except Exception as exc:
            _logger.error("Order failed %s %s: %s", station_id, pick.bracket_label, exc)
            _tg.send(f"Order FAILED {station_id} {pick.bracket_label}: {exc}")

    if records and not dry_run:
        br.reserve(bankroll, total_staked)
    if records:
        save_execution(station_id, target_date, records)


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

    from weather_edge.config import get_station as _get_station

    for i, station_id in enumerate(stations):
        cfg = _get_station(station_id)
        lock_h, lock_m = map(int, cfg.lock_time_utc.split(":"))

        # Ingest runs 30 min before lock so the forecast is cached when lock fires.
        # CLV/resolve are staggered 2 min apart by index (low-traffic cleanup jobs).
        ingest_total_m = lock_h * 60 + lock_m - 30
        ingest_h, ingest_m = divmod(ingest_total_m, 60)

        exec_total_m = lock_h * 60 + lock_m + 5
        exec_h, exec_m = divmod(exec_total_m, 60)

        clv_total_m = 1 * 60 + 0 + i * 2
        clv_h, clv_m = divmod(clv_total_m, 60)

        resolve_total_m = 2 * 60 + 0 + i * 2
        resolve_h, resolve_m = divmod(resolve_total_m, 60)

        sched.add_job(
            _ingest_job, CronTrigger(hour=ingest_h, minute=ingest_m),
            args=[station_id], id=f"ingest_{station_id}", name=f"Ingest {station_id}",
        )
        sched.add_job(
            _lock_job, CronTrigger(hour=lock_h, minute=lock_m),
            args=[station_id], id=f"lock_{station_id}", name=f"Lock {station_id}",
        )
        sched.add_job(
            _execute_job, CronTrigger(hour=exec_h, minute=exec_m),
            args=[station_id], id=f"execute_{station_id}", name=f"Execute {station_id}",
        )
        sched.add_job(
            _closing_snapshot_job, CronTrigger(hour=clv_h, minute=clv_m),
            args=[station_id], id=f"clv_{station_id}", name=f"CLV snapshot {station_id}",
        )
        sched.add_job(
            _resolve_and_observe_job, CronTrigger(hour=resolve_h, minute=resolve_m),
            args=[station_id], id=f"resolve_{station_id}", name=f"Resolve {station_id}",
        )
        sched.add_job(
            _refit_job, CronTrigger(day_of_week="sun", hour=3, minute=0),
            args=[station_id], id=f"refit_{station_id}", name=f"Refit {station_id}",
        )
        _logger.info(
            "Scheduled %s: ingest@%02d:%02dz lock@%02d:%02dz execute@%02d:%02dz",
            station_id, ingest_h, ingest_m, lock_h, lock_m, exec_h, exec_m,
        )

    _logger.info("Scheduler started for stations: %s", stations)

    import os as _os

    def _status() -> str:
        now = datetime.now(timezone.utc)
        live = _os.getenv("LIVE_TRADING", "false").lower() == "true"
        lines = [f"Scheduler running — {now.strftime('%Y-%m-%d %H:%M')} UTC", f"Mode: {'LIVE' if live else 'dry-run'}", ""]
        for job in sched.get_jobs():
            next_run = job.next_run_time
            if next_run:
                delta = next_run - now
                h, m = divmod(int(delta.total_seconds()) // 60, 60)
                lines.append(f"{job.name}: next in {h}h {m}m ({next_run.strftime('%H:%M')}z)")
            else:
                lines.append(f"{job.name}: not scheduled")
        return "\n".join(lines)

    def _picks() -> str:
        from datetime import timedelta
        from weather_edge.models import LockedPicks
        from weather_edge.store import parquet as store
        now = datetime.now(timezone.utc)
        target_date = (now + timedelta(days=1)).date()
        lines = [f"Picks for {target_date}:"]
        found = False
        for sid in stations:
            data = store.read_picks(sid, target_date)
            if data is None:
                lines.append(f"  {sid}: not locked yet")
                continue
            locked = LockedPicks(**data)
            if locked.picks:
                for p in locked.picks:
                    lines.append(f"  {sid} {p.side} {p.bracket_label}  edge={p.edge:+.3f}  kelly={p.kelly_fraction*100:.1f}%")
                found = True
            else:
                lines.append(f"  {sid}: no edge ({locked.no_edge_reason or 'all below threshold'})")
        return "\n".join(lines)

    def _bankroll() -> str:
        from weather_edge.execution import bankroll as br
        try:
            b = br.load()
            avail = br.available(b)
            return (
                f"Bankroll\n"
                f"  Current: ${b['current_usdc']:.2f}\n"
                f"  Reserved: ${b.get('reserved_usdc', 0):.2f}\n"
                f"  Available: ${avail:.2f}\n"
                f"  Total P&L: ${b.get('total_pnl', 0):+.2f}\n"
                f"  Trades: {b.get('n_trades', 0)}"
            )
        except FileNotFoundError:
            return "Bankroll not initialised. Run: we init-bankroll --usdc <amount>"

    def _pnl() -> str:
        from datetime import timedelta
        from weather_edge.execution.polymarket_exec import load_executions
        now = datetime.now(timezone.utc)
        lines = ["P&L last 7 days:"]
        total_pnl = 0.0
        total_staked = 0.0
        n = 0
        for days_ago in range(1, 8):
            d = (now - timedelta(days=days_ago)).date()
            for sid in stations:
                for e in load_executions(sid, d):
                    if e.get("dry_run"):
                        continue
                    stake = float(e.get("usdc_stake", 0))
                    pnl = float(e.get("pnl", 0))
                    total_staked += stake
                    total_pnl += pnl
                    n += 1
        lines.append(f"  Trades: {n}")
        lines.append(f"  Staked: ${total_staked:.2f}")
        lines.append(f"  P&L: ${total_pnl:+.2f}")
        if total_staked > 0:
            lines.append(f"  ROI: {total_pnl/total_staked*100:+.1f}%")
        lines.append("\n(Use bankroll for settled totals)")
        return "\n".join(lines)

    _tg.start_command_listener({
        "/status": _status,
        "/picks": _picks,
        "/bankroll": _bankroll,
        "/pnl": _pnl,
    })
    live_mode = _os.getenv("LIVE_TRADING", "false").lower() == "true"
    _tg.send(
        f"Scheduler started — stations: {', '.join(stations)}\n"
        f"Mode: {'LIVE' if live_mode else 'dry-run'}\n"
        f"Commands: /status /picks /bankroll /pnl"
    )
    sched.start()
