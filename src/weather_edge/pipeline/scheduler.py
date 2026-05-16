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
    import time as _time
    from datetime import timedelta

    from weather_edge.config import get_station
    from weather_edge.ingest import ecmwf, gefs, icon, weathernext
    from weather_edge.logging import log_event
    from weather_edge.pipeline.lock import _most_recent_12z
    from weather_edge.store import parquet as store

    now_utc = datetime.now(timezone.utc)
    init_dt = _most_recent_12z(now_utc)
    cfg = get_station(station_id)

    lines: list[str] = [f"📡 <b>Ingest {station_id}</b> · {init_dt.strftime('%Y-%m-%d %HZ')}"]
    any_fail = False
    for model_name, fetch_fn in [
        ("ecmwf", ecmwf.ingest_forecasts),
        ("gefs", gefs.ingest_forecasts),
        ("icon", icon.ingest_forecasts),
        ("weathernext", weathernext.ingest_forecasts),
    ]:
        # Per-call timer — prevents cumulative-since-job-start drift.
        t0 = _time.monotonic()
        try:
            # GEFS: download only step=24 (matches backfill mode + lock.py _DEFAULT_LEAD_HOURS).
            # ~30s per station instead of ~15 min for all steps.
            if model_name == "gefs":
                # steps 24+30+36 covers D+1 noon for both 12z and 00z inits across UTC±12.
                # Still much faster than all steps (~30s vs ~15 min).
                df = fetch_fn(init_dt, cfg, steps=[24, 30, 36])
            else:
                df = fetch_fn(init_dt, cfg)
            store.write_forecasts(df, model_name, init_dt, station_id)
            _logger.info("Ingested %s %s: %d rows", model_name, init_dt, len(df))
            lines.append(f"  ✅ {model_name}: {len(df)} rows")
            log_event(f"ingest_{model_name}", station_id, "ok",
                      (_time.monotonic() - t0) * 1000, rows=len(df))
        except Exception as exc:
            _logger.error("Ingest %s failed: %s", model_name, exc)
            lines.append(f"  ❌ {model_name}: {exc}")
            any_fail = True
            log_event(f"ingest_{model_name}", station_id, "error",
                      (_time.monotonic() - t0) * 1000, error=str(exc))
    if any_fail:
        lines[0] = lines[0].replace("📡", "⚠️")
    _tg.send("\n".join(lines))


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
            lines = [f"🎯 <b>Pick locked: {station_id}</b> · {target_date}",
                     f"   μ={result.mu:.1f}°C  σ={result.sigma:.2f}"]
            for p in result.picks:
                cap_str = f"  cap=${p.max_stake_usdc:.0f}" if p.max_stake_usdc is not None else ""
                lines.append(
                    f"  {'🟢' if p.side == 'YES' else '🔴'} {p.side} {p.bracket_label}"
                    f"  model={p.model_prob:.0%}  mkt={p.market_prob:.0%}  edge={p.edge:+.1%}"
                    f"  kelly={p.kelly_fraction:.1%}{cap_str}"
                )
            _tg.send("\n".join(lines))
        else:
            _tg.send(f"ℹ️ <b>No edge: {station_id}</b> · {target_date}\n{result.no_edge_reason or 'all edges below threshold'}")
    except AlreadyLockedError:
        _logger.info("Already locked %s %s", station_id, target_date)
    except Exception as exc:
        _logger.error("Lock failed %s %s: %s", station_id, target_date, exc)
        _tg.send(f"❌ <b>Lock FAILED: {station_id}</b> · {target_date}\n{exc}")


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
        if execs and rec.get("resolved"):
            clv_path = Path(__file__).parents[3] / "data" / "clv_snapshots" / f"station={station_id}" / f"{yesterday}.json"
            clv_outcomes: dict[str, float] = {}
            if clv_path.exists():
                try:
                    clv_data = json.loads(clv_path.read_text())
                    clv_outcomes = {o["label"]: o["mid"] for o in clv_data.get("outcomes", [])}
                except Exception:
                    pass

            # Per-date settlement marker — prevents double-settlement if the
            # resolve job re-runs (manual /resolve, restart catch-up, etc.).
            settled_path = (
                Path(__file__).parents[3] / "data" / "executions"
                / f"station={station_id}" / f"date={yesterday}" / "_settled.json"
            )
            already_settled = settled_path.exists()

            try:
                bankroll_data = br.load()
            except FileNotFoundError:
                bankroll_data = None
            dry_bankroll = br.load_dry()

            lines = [f"📋 <b>Result: {station_id}</b> · {yesterday}  →  {resolved_label}"]
            settled_records: list[dict] = []
            for e in execs:
                bracket = e.get("bracket_label", "?")
                side = e.get("side", "?")
                entry = float(e.get("price", 0))
                stake = float(e.get("usdc_stake", 0))
                if entry <= 0:
                    continue
                win = (bracket == resolved_label and side == "YES") or (bracket != resolved_label and side == "NO")
                pnl = (1.0 / entry - 1) * stake if win else -stake
                is_dry = bool(e.get("dry_run"))
                tag = " [dry]" if is_dry else ""
                icon = "🏆" if win else "💸"
                line = f"  {icon} {side} {bracket}{tag}  entry={entry:.2f}  P&amp;L=${pnl:+.2f}"
                clv_value: float | None = None
                if bracket in clv_outcomes:
                    closing = clv_outcomes[bracket]
                    clv_value = (closing - entry) if side == "YES" else (entry - closing)
                    line += f"  CLV={clv_value:+.3f}"
                lines.append(line)

                rec_out = {
                    "bracket_label": bracket,
                    "side": side,
                    "dry_run": is_dry,
                    "entry": entry,
                    "stake": stake,
                    "win": win,
                    "pnl": round(pnl, 4),
                }
                if clv_value is not None:
                    rec_out["clv"] = round(clv_value, 4)
                settled_records.append(rec_out)

                if not already_settled:
                    try:
                        if is_dry:
                            br.settle_dry(dry_bankroll, stake, pnl)
                        elif bankroll_data is not None:
                            br.settle(bankroll_data, stake, pnl)
                    except Exception as exc:
                        _logger.warning("Bankroll settle failed: %s", exc)

            if not already_settled and settled_records:
                settled_path.parent.mkdir(parents=True, exist_ok=True)
                from weather_edge.store.parquet import _dump_json as _dj
                _dj({
                    "resolved_label": resolved_label,
                    "settled_at": now_utc.isoformat(),
                    "records": settled_records,
                }, settled_path)

            # Append running dry-bankroll line so paper-trading P&L is always visible
            if any(e.get("dry_run") for e in execs):
                lines.append(
                    f"  📒 dry bankroll: ${dry_bankroll['current_usdc']:.2f}  "
                    f"(P&amp;L=${dry_bankroll.get('total_pnl', 0):+.2f}  "
                    f"{dry_bankroll.get('n_trades', 0)} trades)"
                )
            if len(lines) > 1:
                _tg.send("\n".join(lines))
    except Exception as exc:
        _logger.error("Resolution failed %s %s: %s", station_id, yesterday, exc)
        _tg.send(f"❌ <b>Resolution FAILED: {station_id}</b> · {yesterday}\n{exc}")


def _execute_job(station_id: str) -> None:
    """Place orders for tomorrow's locked picks.

    Gated by TRADING_ENABLED (master switch, set via /mode). When disabled, the
    job no-ops so picks are still locked but no orders are placed or simulated.
    Dry-run vs live is controlled by LIVE_TRADING.
    """
    import asyncio
    import os
    from datetime import timedelta

    from weather_edge.config import get_station
    from weather_edge.execution import bankroll as br
    from weather_edge.execution.polymarket_exec import MIN_ORDER_USDC, place_order, save_execution
    from weather_edge.market.polymarket import fetch_market
    from weather_edge.models import LockedPicks, MarketSnapshot
    from weather_edge.store import parquet as store

    if os.getenv("TRADING_ENABLED", "false").lower() != "true":
        _logger.info("Trading disabled (TRADING_ENABLED!=true) — skipping execute for %s", station_id)
        return

    dry_run = not _is_live_for(station_id)
    now_utc = datetime.now(timezone.utc)
    target_date = (now_utc + timedelta(days=1)).date()

    if not dry_run:
        from weather_edge.pipeline.edge_gate import station_passes_gate
        try:
            passed, reason = station_passes_gate(station_id, target_date)
        except Exception as exc:
            _logger.warning("Edge gate errored for %s (%s) — allowing live", station_id, exc)
            passed, reason = True, f"gate-error: {exc}"
        if not passed:
            dry_run = True
            _logger.warning("Edge gate forced dry-run for %s: %s", station_id, reason)
            _tg.send(f"⏸ <b>{station_id}</b> auto-paused (live→dry): {reason}")

    picks_data = store.read_picks(station_id, target_date)
    if not picks_data or not picks_data.get("picks"):
        _logger.info("No picks to execute for %s %s", station_id, target_date)
        return

    locked = LockedPicks(**picks_data)

    # Dry runs size against a separate paper bankroll (auto-init at $100)
    # so simulated P&L doesn't pollute the live tracker.
    if dry_run:
        bankroll = br.load_dry()
    else:
        try:
            bankroll = br.load()
        except FileNotFoundError:
            _tg.send(f"❌ <b>Execute FAILED {station_id}:</b> bankroll not initialised\nRun: we init-bankroll --usdc &lt;amount&gt;")
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
        _tg.send(f"❌ <b>Execute FAILED {station_id}:</b> market fetch error\n{exc}")
        return

    outcome_map = {o.label: o for o in snapshot.outcomes}
    records = []
    total_staked = 0.0

    for pick in locked.picks:
        outcome = outcome_map.get(pick.bracket_label)
        if outcome is None:
            continue
        usdc_stake = round(pick.kelly_fraction * avail, 2)
        if pick.max_stake_usdc is not None and usdc_stake > pick.max_stake_usdc:
            _logger.info(
                "Downsizing %s: kelly stake $%.2f → depth cap $%.2f",
                pick.bracket_label, usdc_stake, pick.max_stake_usdc,
            )
            usdc_stake = round(pick.max_stake_usdc, 2)
        if usdc_stake < MIN_ORDER_USDC:
            _logger.info("Skipping %s: stake $%.2f below minimum", pick.bracket_label, usdc_stake)
            continue
        try:
            rec = place_order(pick, outcome, usdc_stake, dry_run=dry_run)
            records.append(rec)
            total_staked += usdc_stake
        except Exception as exc:
            _logger.error("Order failed %s %s: %s", station_id, pick.bracket_label, exc)
            _tg.send(f"❌ <b>Order FAILED {station_id} {pick.bracket_label}:</b> {exc}")

    if records:
        if dry_run:
            br.reserve_dry(bankroll, total_staked)
        else:
            br.reserve(bankroll, total_staked)
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
        clv_dir = Path(__file__).parents[3] / "data" / "clv_snapshots" / f"station={station_id}"
        clv_dir.mkdir(parents=True, exist_ok=True)
        clv_path = clv_dir / f"{yesterday}.json"
        from weather_edge.store.parquet import _dump_json as _dj
        _dj(snapshot.model_dump(), clv_path)
        _logger.info("CLV snapshot saved for %s %s", station_id, yesterday)
    except Exception as exc:
        _logger.warning("CLV snapshot failed %s %s: %s", station_id, yesterday, exc)


def _refit_job(station_id: str) -> None:
    from datetime import date

    from weather_edge.postprocess.emos import assemble_training_pairs, fit_emos, fit_emos_per_model
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

        # Per-model EMOS for BMA. Without this the BMA path can't pick up newer
        # ensembles (e.g. WeatherNext after onboarding) — only the pooled fit
        # above would refresh, leaving per-model params stale.
        per_model = fit_emos_per_model(station_id, lead, as_of, now_utc=now_utc)
        for model_name, params in per_model.items():
            store.write_emos_params(
                params.model_dump(), station_id, lead, params.valid_from, model=model_name
            )
            _logger.info(
                "Re-fitted EMOS %s lead=%dh model=%s n=%d CRPS=%.4f",
                station_id, lead, model_name, params.n_samples, params.train_crps,
            )

    pairs_qrf = assemble_qrf_training_pairs(station_id, 24, as_of, window_days=90)
    if len(pairs_qrf) >= 10:
        forest, X, y, meta = fit_qrf(pairs_qrf, station_id, 24)
        store.write_qrf_params(forest, X, y, meta, station_id, 24, now_utc)
        _logger.info("Re-fitted QRF %s n=%d", station_id, len(pairs_qrf))


# ─── /mode helpers ────────────────────────────────────────────────────────────

def _env_path() -> "Any":
    from pathlib import Path
    return Path(__file__).parents[3] / ".env"


def _write_env(updates: dict[str, str]) -> None:
    """Persist env vars to .env (project root) and update os.environ for the running process."""
    import os
    env_path = _env_path()
    lines = env_path.read_text().splitlines() if env_path.exists() else []
    keys = set(updates.keys())
    lines = [l for l in lines if not any(l.startswith(f"{k}=") for k in keys)]
    for k, v in updates.items():
        lines.append(f"{k}={v}")
        os.environ[k] = v
    env_path.write_text("\n".join(lines) + "\n")


def _live_stations() -> set[str]:
    """Whitelist of station IDs allowed to submit live orders.

    When non-empty, this overrides LIVE_TRADING on a per-station basis: only
    listed stations submit real orders, the rest fall back to dry-run.
    """
    import os
    raw = os.getenv("LIVE_STATIONS", "")
    return {s.strip().upper() for s in raw.split(",") if s.strip()}


def _is_live_for(station_id: str) -> bool:
    """Whether `station_id` should submit live orders right now."""
    import os
    if os.getenv("TRADING_ENABLED", "false").lower() != "true":
        return False
    whitelist = _live_stations()
    if whitelist:
        return station_id.upper() in whitelist
    return os.getenv("LIVE_TRADING", "false").lower() == "true"


def _current_mode() -> str:
    """Return 'off', 'dryrun', 'live', or 'partial' based on TRADING_ENABLED, LIVE_TRADING, LIVE_STATIONS."""
    import os
    if os.getenv("TRADING_ENABLED", "false").lower() != "true":
        return "off"
    if _live_stations():
        return "partial"
    if os.getenv("LIVE_TRADING", "false").lower() == "true":
        return "live"
    return "dryrun"


# ─── Daily summary ────────────────────────────────────────────────────────────

def _daily_summary_job(stations: list[str]) -> None:
    """Send a consolidated Telegram digest: yesterday P&L + bankroll + tomorrow's picks."""
    from datetime import timedelta

    from weather_edge.execution import bankroll as br
    from weather_edge.execution.polymarket_exec import load_executions
    from weather_edge.models import LockedPicks
    from weather_edge.store import parquet as store

    now = datetime.now(timezone.utc)
    yesterday = (now - timedelta(days=1)).date()
    tomorrow = (now + timedelta(days=1)).date()

    mode = _current_mode()
    mode_icon = {"off": "⏸", "dryrun": "🔵", "live": "🟢", "partial": "🟡"}.get(mode, "❓")
    lines = [f"📊 <b>Daily Summary</b> · {now.strftime('%Y-%m-%d %H:%M')}z  {mode_icon} {mode}"]

    # Yesterday P&L per station (show dry-run results when no live bets exist)
    def _pnl_for(e: dict, resolved_label: str) -> float:
        bracket = e.get("bracket_label", "?")
        side = e.get("side", "?")
        entry = float(e.get("price", 0) or 0)
        stake = float(e.get("usdc_stake", 0) or 0)
        if entry <= 0:
            return 0.0
        win = (bracket == resolved_label and side == "YES") or (bracket != resolved_label and side == "NO")
        return (1.0 / entry - 1) * stake if win else -stake

    lines.append("\n<b>Yesterday P&amp;L</b>")
    total_live_pnl = 0.0
    total_dry_pnl = 0.0
    any_bets = False
    any_dry = False
    for sid in stations:
        try:
            rec = store.read_resolution(sid, yesterday)
        except Exception:
            rec = None
        try:
            execs = [e for e in load_executions(sid, yesterday) if isinstance(e, dict)]
        except Exception:
            execs = []
        live_execs = [e for e in execs if not e.get("dry_run")]
        dry_execs = [e for e in execs if e.get("dry_run")]
        if not (live_execs or dry_execs):
            continue
        any_bets = True
        resolved = bool(rec and rec.get("resolved"))
        resolved_label = (rec or {}).get("resolved_label", "")

        if live_execs:
            if resolved:
                pnl = sum(_pnl_for(e, resolved_label) for e in live_execs)
                total_live_pnl += pnl
                icon = "🏆" if pnl >= 0 else "💸"
                lines.append(f"  {icon} {sid}  →  {resolved_label}  P&amp;L=${pnl:+.2f}")
            else:
                lines.append(f"  ⏳ {sid}: pending")
        if dry_execs:
            any_dry = True
            if resolved:
                pnl = sum(_pnl_for(e, resolved_label) for e in dry_execs)
                total_dry_pnl += pnl
                icon = "🏆" if pnl >= 0 else "💸"
                lines.append(f"  {icon} {sid} [dry]  →  {resolved_label}  P&amp;L=${pnl:+.2f}")
            else:
                lines.append(f"  ⏳ {sid} [dry]: pending")
    if not any_bets:
        lines.append("  No bets yesterday")
    else:
        if mode == "live":
            lines.append(f"  <b>Live total: ${total_live_pnl:+.2f}</b>")
        if any_dry:
            lines.append(f"  <b>Dry total: ${total_dry_pnl:+.2f}</b>")

    # Bankroll
    lines.append("\n<b>Bankroll</b>")
    try:
        b = br.load()
        avail = b['current_usdc'] - b.get('reserved_usdc', 0)
        lines.append(
            f"  💰 live: ${b['current_usdc']:.2f}  "
            f"(avail=${avail:.2f}  P&amp;L=${b.get('total_pnl', 0):+.2f}  {b.get('n_trades', 0)} trades)"
        )
    except FileNotFoundError:
        lines.append("  💰 live: not initialised")
    db = br.load_dry()
    db_avail = db['current_usdc'] - db.get('reserved_usdc', 0)
    lines.append(
        f"  📒 dry:  ${db['current_usdc']:.2f}  "
        f"(avail=${db_avail:.2f}  P&amp;L=${db.get('total_pnl', 0):+.2f}  {db.get('n_trades', 0)} trades)"
    )

    # Tomorrow's picks
    lines.append(f"\n<b>Picks for {tomorrow}</b>")
    any_picks = False
    for sid in stations:
        data = store.read_picks(sid, tomorrow)
        if data is None:
            continue
        try:
            locked = LockedPicks(**data)
        except Exception:
            continue
        if locked.picks:
            any_picks = True
            for p in locked.picks:
                lines.append(
                    f"  🎯 {sid}  {'🟢' if p.side == 'YES' else '🔴'} {p.side} {p.bracket_label}"
                    f"  edge={p.edge:+.1%}  kelly={p.kelly_fraction:.1%}"
                )
    if not any_picks:
        lines.append("  No picks yet (locks fire later today)")

    # Edge-gate status (auto-pause flag per station)
    from weather_edge.pipeline.edge_gate import station_passes_gate
    lines.append("\n<b>Edge gate</b>")
    for sid in stations:
        try:
            passed, reason = station_passes_gate(sid, tomorrow)
        except Exception as exc:
            lines.append(f"  ❓ {sid}: gate error ({exc})")
            continue
        icon = "✅" if passed else "⏸"
        lines.append(f"  {icon} {sid}: {reason}")

    _tg.send("\n".join(lines))


# ─── Catch-up ──────────────────────────────────────────────────────────────────

def _catchup(stations: list[str]) -> None:
    """At scheduler startup, run any lock/execute jobs that should already have fired today.

    For each station: if now > lock_time + 15min and tomorrow's picks don't exist,
    run lock immediately. If now > exec_time + 5min and picks exist but no executions
    are recorded, run execute. Idempotent — safe across multiple restarts.
    """
    from datetime import timedelta

    from weather_edge.config import get_station
    from weather_edge.execution.polymarket_exec import load_executions
    from weather_edge.store import parquet as store

    now = datetime.now(timezone.utc)
    tomorrow = (now + timedelta(days=1)).date()

    for station_id in stations:
        try:
            cfg = get_station(station_id)
        except Exception as exc:
            _logger.warning("Catch-up: cannot load %s: %s", station_id, exc)
            continue

        lock_h, lock_m = map(int, cfg.lock_time_utc.split(":"))
        lock_dt = now.replace(hour=lock_h, minute=lock_m, second=0, microsecond=0)
        exec_dt = lock_dt + timedelta(minutes=5)

        if now > lock_dt + timedelta(minutes=15):
            if store.read_picks(station_id, tomorrow) is None:
                _logger.info("Catch-up: running missed lock for %s %s", station_id, tomorrow)
                _tg.send(f"⏰ <b>Catch-up:</b> running missed lock for {station_id} · {tomorrow}")
                try:
                    _lock_job(station_id)
                except Exception as exc:
                    _logger.error("Catch-up lock failed %s: %s", station_id, exc)

        if now > exec_dt + timedelta(minutes=5):
            picks_data = store.read_picks(station_id, tomorrow)
            if picks_data and picks_data.get("picks"):
                if not load_executions(station_id, tomorrow):
                    _logger.info("Catch-up: running missed execute for %s %s", station_id, tomorrow)
                    try:
                        _execute_job(station_id)
                    except Exception as exc:
                        _logger.error("Catch-up execute failed %s: %s", station_id, exc)


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

        exec_total_m = lock_h * 60 + lock_m + 1
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

    # Daily consolidated digest: yesterday P&L + bankroll + tomorrow's picks
    sched.add_job(
        _daily_summary_job, CronTrigger(hour=8, minute=30),
        args=[stations], id="daily_summary", name="Daily summary",
    )

    _logger.info("Scheduler started for stations: %s", stations)

    import threading as _threading

    def _status(args: str = "") -> str:
        now = datetime.now(timezone.utc)
        lines = [
            f"Scheduler running — {now.strftime('%Y-%m-%d %H:%M')} UTC",
            f"Mode: {_current_mode()}",
            "",
        ]
        for job in sched.get_jobs():
            # APScheduler 3.x exposes next_run_time as an attribute; 4.x removed
            # it and moved scheduling state onto the trigger. Try both so this
            # works regardless of which version pip resolved.
            next_run = getattr(job, "next_run_time", None)
            if next_run is None:
                try:
                    next_run = job.trigger.get_next_fire_time(None, now)
                except Exception:
                    next_run = None
            if next_run:
                delta = next_run - now
                h, m = divmod(int(delta.total_seconds()) // 60, 60)
                lines.append(f"{job.name}: next in {h}h {m}m ({next_run.strftime('%H:%M')}z)")
            else:
                lines.append(f"{job.name}: scheduled")
        return "\n".join(lines)

    def _picks(args: str = "") -> str:
        from datetime import timedelta
        from weather_edge.config import get_station as _gs
        from weather_edge.models import LockedPicks
        from weather_edge.store import parquet as store
        now = datetime.now(timezone.utc)
        target_date = (now + timedelta(days=1)).date()
        lines = [f"Picks for {target_date} (now {now.strftime('%H:%M')}z):"]
        for sid in stations:
            cfg = _gs(sid)
            lock_h, lock_m = map(int, cfg.lock_time_utc.split(":"))
            lock_dt = now.replace(hour=lock_h, minute=lock_m, second=0, microsecond=0)
            data = store.read_picks(sid, target_date)
            if data is None:
                mins_until = int((lock_dt - now).total_seconds() / 60)
                if mins_until > 0:
                    lines.append(f"  {sid}: not locked yet (lock at {cfg.lock_time_utc}z, {mins_until}m away)")
                else:
                    lines.append(f"  {sid}: lock overdue — try /lock {sid}")
                continue
            locked = LockedPicks(**data)
            locked_at = locked.locked_at.strftime("%H:%M") if locked.locked_at else "?"
            if locked.picks:
                lines.append(f"  {sid} (locked {locked_at}z, mu={locked.mu:.1f}C):")
                for p in locked.picks:
                    lines.append(f"    {p.side} {p.bracket_label}  edge={p.edge:+.3f}  kelly={p.kelly_fraction*100:.1f}%")
            else:
                lines.append(f"  {sid} (locked {locked_at}z): no edge — {locked.no_edge_reason or 'all below threshold'}")
        return "\n".join(lines)

    def _bankroll(args: str = "") -> str:
        from weather_edge.execution import bankroll as br
        lines: list[str] = ["Bankroll"]
        try:
            b = br.load()
            avail = br.available(b)
            lines += [
                f"  Live:",
                f"    Current: ${b['current_usdc']:.2f}",
                f"    Reserved: ${b.get('reserved_usdc', 0):.2f}",
                f"    Available: ${avail:.2f}",
                f"    Total P&L: ${b.get('total_pnl', 0):+.2f}",
                f"    Trades: {b.get('n_trades', 0)}",
            ]
        except FileNotFoundError:
            lines.append("  Live: not initialised (we init-bankroll --usdc <amount>)")
        except Exception as exc:
            # Corrupt JSON, permission errors, etc. — show in-band, don't
            # crash the dispatcher (otherwise the whole reply is an error).
            lines.append(f"  Live: ⚠️ unreadable — {exc}")
        try:
            db = br.load_dry()
            lines += [
                f"  Dry (paper, ${br.DRY_INITIAL_USDC:.0f} seed):",
                f"    Current: ${db['current_usdc']:.2f}",
                f"    Reserved: ${db.get('reserved_usdc', 0):.2f}",
                f"    Available: ${br.available(db):.2f}",
                f"    Total P&L: ${db.get('total_pnl', 0):+.2f}",
                f"    Trades: {db.get('n_trades', 0)}",
            ]
        except Exception as exc:
            lines.append(f"  Dry: ⚠️ unreadable — {exc}")
        return "\n".join(lines)

    def _pnl(args: str = "") -> str:
        from datetime import timedelta
        from weather_edge.execution.polymarket_exec import load_executions
        from weather_edge.store import parquet as _store
        now = datetime.now(timezone.utc)

        def _pnl_for(e: dict, label: str) -> float:
            entry = float(e.get("price", 0) or 0)
            stake = float(e.get("usdc_stake", 0) or 0)
            if entry <= 0:
                return 0.0
            br_l = e.get("bracket_label", "?")
            sd = e.get("side", "?")
            win = (br_l == label and sd == "YES") or (br_l != label and sd == "NO")
            return (1.0 / entry - 1) * stake if win else -stake

        live = {"n": 0, "staked": 0.0, "pnl": 0.0}
        dry = {"n": 0, "staked": 0.0, "pnl": 0.0}
        for days_ago in range(1, 8):
            d = (now - timedelta(days=days_ago)).date()
            for sid in stations:
                try:
                    rec = _store.read_resolution(sid, d)
                except Exception:
                    rec = None
                if not (isinstance(rec, dict) and rec.get("resolved")):
                    continue
                label = rec.get("resolved_label", "")
                try:
                    execs = load_executions(sid, d)
                except Exception:
                    continue
                for e in execs:
                    # Defensive: load_executions used to extend with dict keys
                    # when a file held a single-record dict, yielding str items.
                    if not isinstance(e, dict):
                        continue
                    bucket = dry if e.get("dry_run") else live
                    bucket["n"] += 1
                    bucket["staked"] += float(e.get("usdc_stake", 0) or 0)
                    bucket["pnl"] += _pnl_for(e, label)

        def _block(name: str, b: dict) -> list[str]:
            out = [f"{name}: trades={b['n']} staked=${b['staked']:.2f} P&L=${b['pnl']:+.2f}"]
            if b["staked"] > 0:
                out.append(f"  ROI: {b['pnl']/b['staked']*100:+.1f}%")
            return out

        lines = ["P&L last 7 days (resolved bets):"]
        lines += _block("  Live", live)
        lines += _block("  Dry ", dry)
        lines.append("\n(Use /bankroll for settled totals)")
        return "\n".join(lines)

    def _station_breakdown(days: int) -> dict[str, dict]:
        from weather_edge.pipeline.reporting import station_breakdown
        return station_breakdown(list(stations), days)

    def _parse_days(args: str, default: int = 7) -> int:
        for tok in args.strip().split():
            try:
                n = int(tok)
                return max(1, min(n, 90))
            except ValueError:
                continue
        return default

    def _parse_sort_by(args: str) -> str:
        for tok in args.strip().lower().split():
            if tok in {"clv", "pnl"}:
                return tok
        return "pnl"

    def _format_ranking(args: str, reverse: bool, title: str) -> str:
        days = _parse_days(args)
        sort_by = _parse_sort_by(args)
        data = _station_breakdown(days)

        rows: list[tuple[str, float, float, int, int, float | None]] = []
        for sid, d in data.items():
            n = d["live"]["n"] + d["dry"]["n"]
            if n == 0:
                continue
            pnl = d["live"]["pnl"] + d["dry"]["pnl"]
            staked = d["live"]["staked"] + d["dry"]["staked"]
            wins = d["live"]["wins"] + d["dry"]["wins"]
            clv_n = d["live"]["clv_n"] + d["dry"]["clv_n"]
            clv_sum = d["live"]["clv_sum"] + d["dry"]["clv_sum"]
            mean_clv = (clv_sum / clv_n) if clv_n > 0 else None
            rows.append((sid, pnl, staked, n, wins, mean_clv))

        if not rows:
            return f"{title} (last {days}d): no resolved bets yet."

        if sort_by == "clv":
            # None CLV pushed to the unfavoured end so they don't dominate either ranking.
            unfav = float("-inf") if reverse else float("inf")
            rows.sort(key=lambda r: (r[5] if r[5] is not None else unfav), reverse=reverse)
        else:
            rows.sort(key=lambda r: r[1], reverse=reverse)
        lines = [f"{title} (last {days}d, sort={sort_by}, live+dry combined):"]
        for sid, pnl, staked, n, wins, mean_clv in rows:
            roi = (pnl / staked * 100) if staked > 0 else 0.0
            clv_str = f"  CLV={mean_clv:+.3f}" if mean_clv is not None else "  CLV=n/a"
            lines.append(
                f"  {sid}: P&L=${pnl:+.2f}  ROI={roi:+.1f}%{clv_str}  "
                f"trades={n}  wins={wins}/{n} ({wins/n*100:.0f}%)  staked=${staked:.2f}"
            )
        lines.append("\nCLV = mean per-bet closing-line value (less noisy than P&L). Try `/topstations clv 14`.")
        return "\n".join(lines)

    def _cmd_top(args: str = "") -> str:
        return _format_ranking(args, reverse=True, title="Most profitable stations")

    def _cmd_losers(args: str = "") -> str:
        return _format_ranking(args, reverse=False, title="Biggest losing stations")

    def _cmd_live(args: str = "") -> str:
        """Per-station live-trading whitelist.

        /live                       — show current setting
        /live STA1 STA2 ...         — only these stations go live; rest stay dry
        /live all                   — every station goes live
        /live none | off            — clear whitelist; everything goes dry (mode stays on)
        """
        import os
        raw = args.strip()
        whitelist = _live_stations()

        if not raw:
            if _current_mode() == "off":
                return "Mode is OFF. Use /mode dryrun or /mode live first."
            if whitelist:
                bad = [s for s in whitelist if s not in stations]
                tail = f"  (unknown: {', '.join(bad)})" if bad else ""
                return (
                    f"Live whitelist: {', '.join(sorted(whitelist))}{tail}\n"
                    f"Other stations run dry-run.\n"
                    f"Usage: /live STA1 STA2 | /live all | /live none"
                )
            mode = _current_mode()
            return (
                f"Live whitelist: (empty)\n"
                f"All stations follow global mode: <b>{mode}</b>\n"
                f"Usage: /live STA1 STA2 | /live all | /live none"
            )

        tokens = [t.upper() for t in raw.replace(",", " ").split()]

        if tokens == ["ALL"]:
            _write_env({"TRADING_ENABLED": "true", "LIVE_TRADING": "true", "LIVE_STATIONS": ""})
            return f"All {len(stations)} station(s) now LIVE."

        if tokens in (["NONE"], ["OFF"]):
            _write_env({"LIVE_TRADING": "false", "LIVE_STATIONS": ""})
            return "Live whitelist cleared. All stations now dry-run (mode unchanged otherwise)."

        unknown = [t for t in tokens if t not in stations]
        if unknown:
            return f"Unknown station(s): {', '.join(unknown)}\nKnown: {', '.join(stations)}"

        wanted = sorted(set(tokens))
        _write_env({
            "TRADING_ENABLED": "true",
            "LIVE_TRADING": "false",
            "LIVE_STATIONS": ",".join(wanted),
        })
        dry = [s for s in stations if s not in wanted]
        return (
            f"Live: {', '.join(wanted)}\n"
            f"Dry:  {', '.join(dry) if dry else '(none)'}\n"
            f"Mode: <b>{_current_mode()}</b>"
        )

    # ── Resolve target stations from command args ────────────────────────────
    def _resolve_targets(args: str) -> list[str]:
        if not args:
            return list(stations)
        wanted = [s.upper() for s in args.replace(",", " ").split()]
        return [s for s in wanted if s in stations] or list(stations)

    # ── Telegram write commands (dispatched to background threads) ───────────
    def _spawn(name: str, target: "Any", *fn_args: "Any") -> None:
        _threading.Thread(target=target, args=fn_args, daemon=True, name=name).start()

    def _cmd_backtest(args: str = "") -> str:
        """Walk-forward backtest on cached forecasts/markets.

        /backtest                   — last 30d, all stations
        /backtest 14                — last 14d, all stations
        /backtest 30 EGLC KLGA      — last 30d, just those stations
        """
        # Parse: optional integer days + optional station list
        tokens = args.replace(",", " ").split()
        days = 30
        targets: list[str] = []
        for tok in tokens:
            if tok.isdigit():
                days = max(1, min(int(tok), 90))
            elif tok.upper() in stations:
                targets.append(tok.upper())
        if not targets:
            targets = list(stations)

        from datetime import date as _date, timedelta as _td
        end = _date.today() - _td(days=1)
        start = end - _td(days=days - 1)

        def _run() -> None:
            import polars as pl
            from weather_edge.pipeline.backtest import backtest as _bt
            _tg.send(
                f"📊 <b>Backtest started</b>\n"
                f"Window: {start} → {end} ({days}d)\n"
                f"Stations: {', '.join(targets)}\n"
                f"This can take a while; results will follow."
            )
            rows: list[tuple[str, float, float, int, int]] = []
            errors: list[str] = []
            for sid in targets:
                try:
                    df = _bt(sid, start, end)
                except Exception as exc:
                    errors.append(f"{sid}: {exc}")
                    continue
                if df.is_empty():
                    continue
                bets = df.filter(pl.col("pnl").is_not_null())
                if bets.is_empty():
                    rows.append((sid, 0.0, 0.0, 0, 0))
                    continue
                pnl_col = bets["pnl"].to_list()
                entry_col = bets["entry_mid"].to_list()
                pnl_total = float(sum(pnl_col))
                staked = float(sum(entry_col))
                wins = sum(1 for p in pnl_col if p > 0)
                rows.append((sid, pnl_total, staked, len(pnl_col), wins))

            if not rows and not errors:
                _tg.send("Backtest: no resolved bets in window.")
                return

            rows.sort(key=lambda r: r[1], reverse=True)
            lines = [f"📊 <b>Backtest {start} → {end}</b>"]
            grand_pnl = grand_stake = 0.0
            for sid, pnl_total, staked, n, wins in rows:
                roi = (pnl_total / staked * 100) if staked > 0 else 0.0
                wr = (wins / n * 100) if n else 0.0
                lines.append(
                    f"  {sid}: P&L=${pnl_total:+.2f}  ROI={roi:+.1f}%  "
                    f"trades={n}  wins={wins}/{n} ({wr:.0f}%)"
                )
                grand_pnl += pnl_total
                grand_stake += staked
            grand_roi = (grand_pnl / grand_stake * 100) if grand_stake > 0 else 0.0
            lines.append(f"<b>TOTAL: P&L=${grand_pnl:+.2f}  ROI={grand_roi:+.1f}%</b>")
            if errors:
                lines.append("\nErrors:")
                lines.extend(f"  {e}" for e in errors)
            _tg.send("\n".join(lines))

        _spawn("manual-backtest", _run)
        return (
            f"Backtest dispatched: {start} → {end} ({days}d), "
            f"{len(targets)} station(s). Results will be posted when done."
        )

    def _cmd_dump(args: str = "") -> str:
        """Export a JSON snapshot of pipeline state for external analysis.

        /dump          — last 30 days, all stations
        /dump 60       — last 60 days
        """
        days = 30
        for tok in args.replace(",", " ").split():
            if tok.isdigit():
                days = max(1, min(int(tok), 90))

        def _run() -> None:
            from weather_edge.pipeline.dump import build_state_dump
            try:
                path = build_state_dump(list(stations), days=days)
                size_kb = path.stat().st_size / 1024
                _tg.send_document(
                    path,
                    caption=f"📦 <b>State dump</b> · {days}d · {size_kb:.0f} KB",
                )
            except Exception as exc:
                _tg.send(f"Dump failed: {exc}")

        _spawn("manual-dump", _run)
        return f"Dump dispatched ({days}d). File will be uploaded when ready."

    def _cmd_lock(args: str = "") -> str:
        targets = _resolve_targets(args)
        for sid in targets:
            _spawn(f"manual-lock-{sid}", _lock_job, sid)
        return f"Lock dispatched for {len(targets)} station(s): {', '.join(targets)}"

    def _cmd_execute(args: str = "") -> str:
        if _current_mode() == "off":
            return "Trading is OFF. Enable with /mode dryrun or /mode live first."
        targets = _resolve_targets(args)
        for sid in targets:
            _spawn(f"manual-exec-{sid}", _execute_job, sid)
        return f"Execute dispatched ({_current_mode()}) for {len(targets)} station(s): {', '.join(targets)}"

    def _cmd_ingest(args: str = "") -> str:
        targets = _resolve_targets(args)
        for sid in targets:
            _spawn(f"manual-ingest-{sid}", _ingest_job, sid)
        return f"Ingest dispatched for {len(targets)} station(s): {', '.join(targets)}"

    def _cmd_resolve(args: str = "") -> str:
        targets = _resolve_targets(args)
        for sid in targets:
            _spawn(f"manual-resolve-{sid}", _resolve_and_observe_job, sid)
        return f"Resolve dispatched for {len(targets)} station(s): {', '.join(targets)}"

    def _cmd_summary(args: str = "") -> str:
        _spawn("manual-summary", _daily_summary_job, list(stations))
        return "Daily summary dispatched."

    def _cmd_mode(args: str = "") -> str:
        target = args.strip().lower()
        if not target:
            return (
                f"Mode: <b>{_current_mode()}</b>\n"
                "Usage: /mode off | /mode dryrun | /mode live\n"
                "  off     — locks fire, no orders placed\n"
                "  dryrun  — orders simulated, no real money\n"
                "  live    — real orders submitted to Polymarket"
            )
        if target == "off":
            _write_env({"TRADING_ENABLED": "false"})
        elif target == "dryrun":
            _write_env({"TRADING_ENABLED": "true", "LIVE_TRADING": "false"})
        elif target == "live":
            _write_env({"TRADING_ENABLED": "true", "LIVE_TRADING": "true"})
        else:
            return f"Unknown mode '{target}'. Use: off | dryrun | live"
        return f"Mode set to <b>{_current_mode()}</b>. Effective immediately; persisted to .env."

    _tg.start_command_listener({
        "/status": _status,
        "/picks": _picks,
        "/bankroll": _bankroll,
        "/pnl": _pnl,
        "/topstations": _cmd_top,
        "/losers": _cmd_losers,
        "/lock": _cmd_lock,
        "/ingest": _cmd_ingest,
        "/execute": _cmd_execute,
        "/resolve": _cmd_resolve,
        "/summary": _cmd_summary,
        "/mode": _cmd_mode,
        "/live": _cmd_live,
        "/backtest": _cmd_backtest,
        "/dump": _cmd_dump,
    })
    whitelist = _live_stations()
    live_line = f"Live stations: {', '.join(sorted(whitelist))}\n" if whitelist else ""
    _tg.send(
        f"🚀 <b>Scheduler started</b> · {len(stations)} stations\n"
        f"Mode: {_current_mode()}\n"
        f"{live_line}"
        f"Stations: {', '.join(stations)}\n"
        f"Commands: /status /picks /bankroll /pnl /topstations /losers /summary /lock /ingest /execute /resolve /mode /live /backtest"
    )

    # Run any missed jobs from earlier today (e.g. after VPS reboot)
    _catchup(list(stations))

    sched.start()
