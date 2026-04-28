"""CLI entry point: `we <command>`

All commands are deterministic given their inputs.
The single datetime.now() injection point is here; never call it in pipeline code.
"""
from __future__ import annotations

import asyncio
import logging
import sys
from datetime import date, datetime, timezone
from typing import Annotated, Optional

import typer
from rich.console import Console
from rich.table import Table

app = typer.Typer(name="we", help="Weather Edge — ensemble forecast -> Polymarket edge CLI")
ingest_app = typer.Typer(help="Ingest forecast or observation data")
app.add_typer(ingest_app, name="ingest")

_console = Console()
logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")


def _notify(title: str, body: str) -> None:
    try:
        import plyer  # type: ignore[import-untyped]
        plyer.notification.notify(title=title, message=body, timeout=10)
    except Exception:
        pass


# ─── Ingest commands ──────────────────────────────────────────────────────────

@ingest_app.command("forecasts")
def ingest_forecasts(
    station: Annotated[str, typer.Option("--station", "-s", help="ICAO station code")] = "EGLL",
    init: Annotated[
        Optional[str], typer.Option("--init", help="Init datetime ISO (e.g. 2026-04-25T12:00Z)")
    ] = None,
    step: Annotated[
        Optional[int], typer.Option("--step", help="Single GEFS step to download (hours). Omit for all steps.")
    ] = None,
) -> None:
    """Fetch ECMWF HRES+ENS and GEFS forecasts for a given init cycle.

    Use --step 24 for a fast single-step GEFS download (~30s vs ~15min for all steps).
    """
    from weather_edge.config import get_station
    from weather_edge.ingest import ecmwf, gefs
    from weather_edge.store import parquet as store

    now_utc = datetime.now(timezone.utc)
    if init:
        init_dt = datetime.fromisoformat(init.replace("Z", "+00:00"))
    else:
        from weather_edge.pipeline.lock import _most_recent_12z
        init_dt = _most_recent_12z(now_utc)

    cfg = get_station(station)
    gefs_steps = [step] if step is not None else None
    _console.print(f"Ingesting forecasts for [bold]{station}[/bold] init=[bold]{init_dt}[/bold]")

    for model_name, fetch_fn in [("ecmwf", ecmwf.ingest_forecasts), ("gefs", gefs.ingest_forecasts)]:
        try:
            if model_name == "gefs" and gefs_steps is not None:
                df = fetch_fn(init_dt, cfg, steps=gefs_steps)
            else:
                df = fetch_fn(init_dt, cfg)
            df = df.with_columns([
                __import__("polars").lit(cfg.icao).alias("station"),
                __import__("polars").lit(init_dt.replace(tzinfo=timezone.utc)).alias("init_datetime"),
            ])
            path = store.write_forecasts(df, model_name, init_dt, station)
            _console.print(f"  [green]{model_name}[/green]: {len(df)} rows -> {path}")
        except Exception as exc:
            _console.print(f"  [red]{model_name} failed[/red]: {exc}")


@ingest_app.command("observations")
def ingest_observations(
    station: Annotated[str, typer.Option("--station", "-s")] = "EGLL",
    start: Annotated[str, typer.Option("--start", help="Start date YYYY-MM-DD")] = "",
    end: Annotated[str, typer.Option("--end", help="End date YYYY-MM-DD")] = "",
) -> None:
    """Fetch METAR observations from Iowa Mesonet ASOS."""
    from weather_edge.config import get_station
    from weather_edge.ingest.metar import fetch_observations
    from weather_edge.store import parquet as store

    if not start or not end:
        _console.print("[red]--start and --end are required[/red]")
        raise typer.Exit(1)

    start_d = date.fromisoformat(start)
    end_d = date.fromisoformat(end)
    cfg = get_station(station)

    _console.print(f"Fetching observations for [bold]{station}[/bold] {start_d} to {end_d}")
    df = asyncio.run(fetch_observations(cfg, start_d, end_d))
    if df.is_empty():
        _console.print("[yellow]No data returned[/yellow]")
        return
    path = store.write_observations(df, station)
    _console.print(f"[green]{len(df)} rows[/green] -> {path}")


# ─── Backfill ─────────────────────────────────────────────────────────────────

@app.command("backfill")
def backfill_cmd(
    station: Annotated[str, typer.Option("--station", "-s")] = "EGLC",
    start: Annotated[str, typer.Option("--start", help="Start date YYYY-MM-DD")] = "",
    end: Annotated[str, typer.Option("--end", help="End date YYYY-MM-DD")] = "",
    lead: Annotated[int, typer.Option("--lead", help="Lead hours to backfill")] = 24,
    step: Annotated[int, typer.Option("--step", help="Single forecast step to download (hours)")] = 24,
) -> None:
    """Backfill GEFS forecasts for a date range using a single forecast step.

    For each valid date D, downloads the init cycle at D-{lead}h with steps=[{step}].
    Skips dates already cached. Use --step 24 for a lightweight ~15 MB/day backfill.
    """
    import datetime as _dt

    from weather_edge.config import get_station
    from weather_edge.ingest import gefs as gefs_ingest
    from weather_edge.postprocess.emos import _init_datetime_for
    from weather_edge.store import parquet as store

    if not start or not end:
        _console.print("[red]--start and --end are required[/red]")
        raise typer.Exit(1)

    start_d = date.fromisoformat(start)
    end_d = date.fromisoformat(end)
    cfg = get_station(station)

    _console.print(
        f"Backfilling GEFS for [bold]{station}[/bold] "
        f"{start_d} -> {end_d}  lead={lead}h  step=f{step:03d}"
    )

    n_fetched = n_skipped = n_failed = 0
    current = start_d
    while current <= end_d:
        init_dt = _init_datetime_for(current, lead)

        # Skip if data for this specific valid_date already exists in the parquet
        existing = store.read_forecasts("gefs", init_dt, station)
        if existing is not None:
            import polars as _pl
            has_date = existing.filter(_pl.col("valid_date") == current).height > 0
            if has_date:
                _console.print(f"  [dim]{current}  init={init_dt.strftime('%Y-%m-%dT%HZ')}  (cached)[/dim]")
                n_skipped += 1
                current += _dt.timedelta(days=1)
                continue

        _console.print(f"  {current}  init={init_dt.strftime('%Y-%m-%dT%HZ')} ... ", end="")
        try:
            df = gefs_ingest.ingest_forecasts(init_dt, cfg, steps=[step])
            store.write_forecasts(df, "gefs", init_dt, station)
            _console.print(f"[green]{len(df)} rows[/green]")
            n_fetched += 1
        except Exception as exc:
            _console.print(f"[red]FAILED: {exc}[/red]")
            n_failed += 1

        current += _dt.timedelta(days=1)

    _console.print(
        f"\nDone: [green]{n_fetched} fetched[/green]  "
        f"[dim]{n_skipped} skipped[/dim]  "
        f"[red]{n_failed} failed[/red]"
    )


# ─── Fit EMOS ─────────────────────────────────────────────────────────────────

@app.command("fit-emos")
def fit_emos_cmd(
    station: Annotated[str, typer.Option("--station", "-s")] = "EGLL",
    lead: Annotated[int, typer.Option("--lead", help="Lead hours (24, 48, 72)")] = 24,
    as_of: Annotated[
        Optional[str], typer.Option("--as-of", help="Date YYYY-MM-DD (default: today)")
    ] = None,
) -> None:
    """Fit EMOS parameters on the rolling 60-day training window."""
    from weather_edge.postprocess.emos import assemble_training_pairs, fit_emos
    from weather_edge.store import parquet as store

    as_of_d = date.fromisoformat(as_of) if as_of else date.today()
    _console.print(f"Fitting EMOS for [bold]{station}[/bold] lead=[bold]{lead}h[/bold] as_of={as_of_d}")

    pairs = assemble_training_pairs(station, lead, as_of_d)
    if not pairs:
        _console.print("[red]No training pairs found — ingest observations first[/red]")
        raise typer.Exit(1)

    params = fit_emos(pairs, station, lead)
    store.write_emos_params(params.model_dump(), station, lead, params.valid_from, model=None)

    _console.print(f"[green]Fitted on {params.n_samples} samples[/green]")
    _console.print(f"  a={params.a:.4f}  b={params.b:.4f}  c={params.c:.4f}  d={params.d:.4f}")
    _console.print(f"  train CRPS: {params.train_crps:.4f}")


@app.command("fit-qrf")
def fit_qrf_cmd(
    station: Annotated[str, typer.Option("--station", "-s")] = "EGLL",
    lead: Annotated[int, typer.Option("--lead", help="Lead hours (24, 48, 72)")] = 24,
    as_of: Annotated[
        Optional[str], typer.Option("--as-of", help="Date YYYY-MM-DD (default: today)")
    ] = None,
    window: Annotated[int, typer.Option("--window", help="Training window days")] = 60,
) -> None:
    """Fit a Quantile Regression Forest (Phase 3) on the rolling training window."""
    from datetime import datetime, timezone

    from weather_edge.postprocess.qrf import assemble_qrf_training_pairs, fit_qrf
    from weather_edge.store import parquet as store

    as_of_d = date.fromisoformat(as_of) if as_of else date.today()
    _console.print(
        f"Fitting QRF for [bold]{station}[/bold] lead=[bold]{lead}h[/bold] "
        f"as_of={as_of_d} window={window}d"
    )

    pairs = assemble_qrf_training_pairs(station, lead, as_of_d, window_days=window)
    if len(pairs) < 10:
        _console.print(
            f"[red]Insufficient training pairs: {len(pairs)} (need ≥10) — "
            "ingest more forecasts and observations first[/red]"
        )
        raise typer.Exit(1)

    forest, X, y, meta = fit_qrf(pairs, station, lead)
    valid_from = datetime.now(timezone.utc)
    path = store.write_qrf_params(forest, X, y, meta, station, lead, valid_from)

    _console.print(f"[green]QRF fitted on {len(pairs)} samples[/green] -> {path}")
    _console.print(f"  n_estimators={meta['n_estimators']}  min_samples_leaf={meta['min_samples_leaf']}")


@app.command("fit-emos-bma")
def fit_emos_bma_cmd(
    station: Annotated[str, typer.Option("--station", "-s")] = "EGLL",
    lead: Annotated[int, typer.Option("--lead", help="Lead hours (24, 48, 72)")] = 24,
    as_of: Annotated[
        Optional[str], typer.Option("--as-of", help="Date YYYY-MM-DD (default: today)")
    ] = None,
) -> None:
    """Fit per-model EMOS for BMA (Phase 2). Stores separate params for ecmwf and gefs."""
    from weather_edge.postprocess.emos import fit_emos_per_model
    from weather_edge.store import parquet as store

    as_of_d = date.fromisoformat(as_of) if as_of else date.today()
    _console.print(
        f"Fitting per-model EMOS (BMA) for [bold]{station}[/bold] lead=[bold]{lead}h[/bold] as_of={as_of_d}"
    )

    model_params = fit_emos_per_model(station, lead, as_of_d)
    if not model_params:
        _console.print("[red]Insufficient per-model data — ingest more forecasts first[/red]")
        raise typer.Exit(1)

    for model_name, params in model_params.items():
        store.write_emos_params(params.model_dump(), station, lead, params.valid_from, model=model_name)
        _console.print(f"  [green]{model_name}[/green]: n={params.n_samples} CRPS={params.train_crps:.4f}  "
                       f"a={params.a:.3f} b={params.b:.3f} c={params.c:.3f} d={params.d:.3f}")


# ─── Onboard station ─────────────────────────────────────────────────────────

@app.command("onboard-station")
def onboard_station_cmd(
    station: Annotated[str, typer.Option("--station", "-s", help="ICAO code from stations.yaml")] = "",
    backfill_days: Annotated[int, typer.Option("--backfill-days")] = 90,
) -> None:
    """One-shot setup for a new station: GEFS backfill (step=24), METAR observations, fit EMOS + QRF.

    Run once before adding the station's ICAO to the scheduler --stations list
    (or to the systemd ExecStart). Idempotent — re-runs skip already-cached data.
    """
    import datetime as _dt

    import polars as _pl

    from weather_edge.config import get_station
    from weather_edge.ingest import gefs as gefs_ingest
    from weather_edge.ingest.metar import fetch_observations
    from weather_edge.postprocess.emos import (
        _init_datetime_for,
        assemble_training_pairs,
        fit_emos,
    )
    from weather_edge.postprocess.qrf import assemble_qrf_training_pairs, fit_qrf
    from weather_edge.store import parquet as store

    if not station:
        _console.print("[red]--station is required[/red]")
        raise typer.Exit(1)

    try:
        cfg = get_station(station)
    except Exception as exc:
        _console.print(f"[red]Unknown station '{station}': {exc}[/red]")
        raise typer.Exit(1)

    end_d = date.today() - _dt.timedelta(days=1)
    start_d = end_d - _dt.timedelta(days=backfill_days)
    _console.print(f"Onboarding [bold]{station}[/bold] ({cfg.name})")
    _console.print(f"  Backfill range: {start_d} -> {end_d} ({backfill_days} days)")

    # ── Step 1: GEFS backfill (step=24) ───────────────────────────────────────
    _console.print("\n[bold][1/4] GEFS backfill (step=24)...[/bold]")
    n_fetched = n_skipped = n_failed = 0
    current = start_d
    while current <= end_d:
        init_dt = _init_datetime_for(current, 24)
        existing = store.read_forecasts("gefs", init_dt, station)
        if existing is not None and existing.filter(_pl.col("valid_date") == current).height > 0:
            n_skipped += 1
            current += _dt.timedelta(days=1)
            continue
        try:
            df = gefs_ingest.ingest_forecasts(init_dt, cfg, steps=[24])
            store.write_forecasts(df, "gefs", init_dt, station)
            n_fetched += 1
        except Exception as exc:
            _console.print(f"  [red]{current}: {exc}[/red]")
            n_failed += 1
        current += _dt.timedelta(days=1)
    _console.print(f"  [green]{n_fetched} fetched[/green], {n_skipped} skipped, [red]{n_failed} failed[/red]")

    # ── Step 2: METAR observations ────────────────────────────────────────────
    _console.print("\n[bold][2/4] METAR observations...[/bold]")
    try:
        df_obs = asyncio.run(fetch_observations(cfg, start_d, end_d))
    except Exception as exc:
        _console.print(f"  [red]Observation fetch failed: {exc}[/red]")
        df_obs = None

    if df_obs is not None and not df_obs.is_empty():
        store.write_observations(df_obs, station)
        _console.print(f"  [green]{len(df_obs)} rows saved[/green]")
    elif df_obs is not None:
        _console.print("  [yellow]No observations returned[/yellow]")

    # ── Step 3: Fit pooled EMOS for leads 24, 48, 72 ─────────────────────────
    _console.print("\n[bold][3/4] Fitting EMOS...[/bold]")
    for lead in (24, 48, 72):
        pairs = assemble_training_pairs(station, lead, end_d)
        if len(pairs) >= 10:
            params = fit_emos(pairs, station, lead)
            store.write_emos_params(params.model_dump(), station, lead, params.valid_from, model=None)
            _console.print(
                f"  lead={lead}h: [green]n={params.n_samples} CRPS={params.train_crps:.4f}[/green]"
            )
        else:
            _console.print(f"  lead={lead}h: [yellow]only {len(pairs)} pairs — skipped[/yellow]")

    # ── Step 4: Fit QRF for lead=24 ───────────────────────────────────────────
    _console.print("\n[bold][4/4] Fitting QRF (lead=24h)...[/bold]")
    pairs_qrf = assemble_qrf_training_pairs(station, 24, end_d, window_days=backfill_days)
    if len(pairs_qrf) >= 10:
        forest, X, y, meta = fit_qrf(pairs_qrf, station, 24)
        store.write_qrf_params(forest, X, y, meta, station, 24, datetime.now(timezone.utc))
        _console.print(f"  [green]n={len(pairs_qrf)} samples[/green]")
    else:
        _console.print(f"  [yellow]Only {len(pairs_qrf)} pairs — QRF skipped[/yellow]")

    _console.print(
        f"\n[green]{station} onboarded.[/green] Add it to the scheduler --stations list."
    )


# ─── Lock picks ───────────────────────────────────────────────────────────────

@app.command("lock")
def lock_cmd(
    station: Annotated[str, typer.Option("--station", "-s")] = "EGLL",
    date_str: Annotated[
        Optional[str], typer.Option("--date", help="Target date YYYY-MM-DD (default: tomorrow)")
    ] = None,
    all_stations: Annotated[
        bool, typer.Option("--all-stations", help="Run for all configured stations")
    ] = False,
    force: Annotated[
        bool, typer.Option("--force", "-f", help="Overwrite existing picks with fresh market data")
    ] = False,
) -> None:
    """Run the full pipeline and lock picks for the target date."""
    from weather_edge.config import load_stations
    from weather_edge.exceptions import AlreadyLockedError, IngestError
    from weather_edge.pipeline.lock import lock_picks
    import datetime as _dt

    now_utc = datetime.now(timezone.utc)
    target_date = (
        date.fromisoformat(date_str) if date_str
        else (now_utc + _dt.timedelta(days=1)).date()
    )

    stations_to_run = list(load_stations().keys()) if all_stations else [station]

    for station_id in stations_to_run:
        _console.print(f"Locking picks for [bold]{station_id}[/bold] date=[bold]{target_date}[/bold]")
        try:
            result = lock_picks(target_date, station_id, now_utc, force=force)
        except AlreadyLockedError as exc:
            _console.print(f"  [yellow]{exc}[/yellow]")
            continue
        except (IngestError, Exception) as exc:
            _console.print(f"  [red]Pipeline failed: {exc}[/red]")
            continue

        mode = result.provenance.get("mode", "pooled")
        _console.print(f"  mu={result.mu:.2f}C  sigma={result.sigma:.2f}C  mode=[cyan]{mode}[/cyan]")

        if result.picks:
            table = Table(title=f"Locked Picks -- {station_id}", show_header=True)
            table.add_column("Bracket")
            table.add_column("Side")
            table.add_column("Model %", justify="right")
            table.add_column("Market %", justify="right")
            table.add_column("Edge", justify="right")
            table.add_column("Kelly %", justify="right")
            for pick in result.picks:
                label = pick.bracket_label.replace("°", "deg")
                table.add_row(
                    label,
                    pick.side,
                    f"{pick.model_prob*100:.1f}",
                    f"{pick.market_prob*100:.1f}",
                    f"{pick.edge*100:+.1f}",
                    f"{pick.kelly_fraction*100:.1f}",
                )
            _console.print(table)
            _notify(
                "Weather Edge: Edge Found!",
                f"{station_id} {target_date}: {len(result.picks)} pick(s)",
            )
        else:
            _console.print(f"  [yellow]No edge -- {result.no_edge_reason}[/yellow]")


# ─── Backtest ─────────────────────────────────────────────────────────────────

@app.command("backtest")
def backtest_cmd(
    station: Annotated[str, typer.Option("--station", "-s")] = "EGLL",
    start: Annotated[str, typer.Option("--start")] = "",
    end: Annotated[str, typer.Option("--end")] = "",
) -> None:
    """Replay historical dates through the full pipeline and score results."""
    from weather_edge.pipeline.backtest import backtest

    if not start or not end:
        _console.print("[red]--start and --end are required[/red]")
        raise typer.Exit(1)

    start_d = date.fromisoformat(start)
    end_d = date.fromisoformat(end)

    _console.print(f"Backtesting [bold]{station}[/bold] {start_d} -> {end_d}")
    df = backtest(station, start_d, end_d)

    if df.is_empty():
        _console.print("[yellow]No results[/yellow]")
        return

    n_days = len(df)
    n_picks = int(df.filter(__import__("polars").col("pick_label").is_not_null()).height)
    mean_crps_val = df.drop_nulls(subset=["crps"])["crps"].mean()
    _console.print(f"\n  Days: {n_days}  |  Picks: {n_picks}  |  Mean CRPS: {mean_crps_val:.4f}")


# ─── Report ───────────────────────────────────────────────────────────────────

@app.command("report")
def report_cmd(
    station: Annotated[str, typer.Option("--station", "-s")] = "EGLL",
    start: Annotated[str, typer.Option("--start")] = "",
    end: Annotated[Optional[str], typer.Option("--end")] = None,
    output_dir: Annotated[Optional[str], typer.Option("--output-dir")] = None,
) -> None:
    """Generate calibration report: reliability diagram, PIT, Brier, P&L."""
    from weather_edge.eval.brier import compute_report, plot_clv_distribution, plot_pnl_curve
    from weather_edge.eval.calibration import plot_pit_histogram, plot_reliability_diagram

    if not start:
        _console.print("[red]--start is required[/red]")
        raise typer.Exit(1)

    start_d = date.fromisoformat(start)
    end_d = date.fromisoformat(end) if end else date.today()

    import os
    odir = output_dir or "."

    _console.print(f"Generating report for [bold]{station}[/bold] {start_d} -> {end_d}")

    summary = compute_report(station, start_d, end_d)
    _console.print(summary)

    plot_reliability_diagram(station, start_d, end_d,
                             output_path=os.path.join(odir, f"{station}_reliability.png"))
    plot_pit_histogram(station, start_d, end_d,
                       output_path=os.path.join(odir, f"{station}_pit.png"))
    plot_pnl_curve(station, start_d, end_d,
                   output_path=os.path.join(odir, f"{station}_pnl.png"))
    plot_clv_distribution(station, start_d, end_d,
                          output_path=os.path.join(odir, f"{station}_clv.png"))

    _console.print(f"[green]Plots saved to {odir}[/green]")


# ─── Resolve ──────────────────────────────────────────────────────────────────

@app.command("resolve")
def resolve_cmd(
    station: Annotated[str, typer.Option("--station", "-s")] = "EGLC",
    date_str: Annotated[Optional[str], typer.Option("--date", help="Date YYYY-MM-DD")] = None,
    start: Annotated[Optional[str], typer.Option("--start")] = None,
    end: Annotated[Optional[str], typer.Option("--end")] = None,
    all_dates: Annotated[bool, typer.Option("--all", help="Resolve all dates with locked picks")] = False,
    force: Annotated[bool, typer.Option("--force", help="Re-fetch even if already resolved")] = False,
) -> None:
    """Fetch resolved market outcome and compute P&L against locked picks."""
    from weather_edge.pipeline.resolve import resolve_date, resolve_range
    from weather_edge.store import parquet as store
    import datetime as _dt

    if date_str:
        dates = [date.fromisoformat(date_str)]
    elif all_dates:
        all_picks = store.read_all_picks(station)
        dates = sorted(date.fromisoformat(p["date"]) for p in all_picks)
        if not dates:
            _console.print("[yellow]No locked picks found[/yellow]")
            return
        _console.print(f"Resolving {len(dates)} locked dates for [bold]{station}[/bold]")
    elif start:
        start_d = date.fromisoformat(start)
        end_d = date.fromisoformat(end) if end else date.today()
        from datetime import timedelta
        dates = []
        cur = start_d
        while cur <= end_d:
            dates.append(cur)
            cur += _dt.timedelta(days=1)
    else:
        import datetime as _dt2
        dates = [(_dt2.datetime.now(timezone.utc) - _dt.timedelta(days=1)).date()]

    for d in dates:
        import asyncio as _asyncio
        rec = _asyncio.run(resolve_date(station, d, force=force))
        if not rec["resolved"]:
            _console.print(f"  {d}: [yellow]not resolved yet[/yellow]")
            continue
        pnl = rec["total_pnl_per_unit"]
        color = "green" if pnl >= 0 else "red"
        _console.print(f"  {d}: resolved=[bold]{rec['resolved_label']}[/bold]  P&L=[{color}]{pnl:+.4f}[/{color}] per unit")
        for p in rec.get("picks_pnl", []):
            icon = "+" if p["correct"] else "-"
            _console.print(f"    {icon} {p['bracket_label']} {p['side']} @ {p['entry_price']:.3f} -> pnl={p['pnl_per_unit']:+.4f}")


# ─── Execution ────────────────────────────────────────────────────────────────

@app.command("init-bankroll")
def init_bankroll_cmd(
    usdc: Annotated[float, typer.Option("--usdc", help="Starting USDC amount")] = 10.0,
) -> None:
    """Initialise the bankroll tracker with a starting USDC balance."""
    from weather_edge.execution import bankroll
    b = bankroll.init(usdc)
    _console.print(f"[green]Bankroll initialised: ${b['initial_usdc']:.2f} USDC[/green]")
    _console.print(f"  File: data/bankroll.json")


@app.command("setup-clob")
def setup_clob_cmd() -> None:
    """Derive Polymarket CLOB API credentials from POLYMARKET_PK and print env vars.

    Set POLYMARKET_PK first, then run this once to get your API key/secret/passphrase.
    """
    from weather_edge.execution.polymarket_exec import derive_api_creds
    _console.print("Deriving CLOB API credentials from POLYMARKET_PK...")
    try:
        creds = derive_api_creds()
    except Exception as exc:
        _console.print(f"[red]Failed: {exc}[/red]")
        raise typer.Exit(1)
    _console.print("[green]Add these to your shell profile (.bashrc / .env):[/green]")
    for k, v in creds.items():
        _console.print(f"  export {k}={v}")


@app.command("execute")
def execute_cmd(
    station: Annotated[str, typer.Option("--station", "-s")] = "EGLC",
    date_str: Annotated[str, typer.Option("--date", help="Date YYYY-MM-DD")] = "",
    dry_run: Annotated[bool, typer.Option("--dry-run/--live", help="Simulate only (default: dry-run)")] = True,
    yes: Annotated[bool, typer.Option("--yes", "-y", help="Skip confirmation prompt")] = False,
) -> None:
    """Place live orders for locked picks. Reads picks from data/picks/.

    Always runs in --dry-run mode by default. Pass --live to submit real orders.
    Requires POLYMARKET_PK (and CLOB_API_KEY/CLOB_SECRET/CLOB_PASS_PHRASE for live).
    """
    import json as _json
    from pathlib import Path
    from weather_edge.execution import bankroll as br
    from weather_edge.execution.polymarket_exec import (
        MIN_ORDER_USDC, place_order, save_execution,
    )
    from weather_edge.models import LockedPicks, MarketOutcome
    from weather_edge.store import parquet as store

    if not date_str:
        _console.print("[red]--date is required[/red]")
        raise typer.Exit(1)

    target_date = date.fromisoformat(date_str)

    # Load picks
    picks_data = store.read_picks(station, target_date)
    if picks_data is None:
        _console.print(f"[red]No picks found for {station} {target_date}. Run `we lock` first.[/red]")
        raise typer.Exit(1)

    locked = LockedPicks(**picks_data)
    if not locked.picks:
        _console.print(f"[yellow]No qualifying picks for {station} {target_date}: {locked.no_edge_reason}[/yellow]")
        raise typer.Exit(0)

    # Load bankroll
    try:
        bankroll = br.load()
    except FileNotFoundError:
        _console.print("[red]Bankroll not initialised. Run: we init-bankroll --usdc 10.0[/red]")
        raise typer.Exit(1)

    avail = br.available(bankroll)
    _console.print(f"Bankroll: ${bankroll['current_usdc']:.2f} USDC  (available: ${avail:.2f})")

    # Load market snapshot for token IDs — fetch fresh if missing or stale (no NO tokens)
    from weather_edge.models import MarketSnapshot
    from weather_edge.config import get_station as _get_station
    snap_raw = store.read_market_snapshot(station, target_date)
    _need_fresh = snap_raw is None or not any(
        o.get("no_token_id") for o in snap_raw.get("outcomes", [])
    )
    if _need_fresh:
        _console.print("[dim]Fetching fresh market snapshot...[/dim]")
        _cfg = _get_station(station)
        _slug = _cfg.market_slug_pattern.format(
            date=target_date.strftime("%Y-%m-%d"),
            month_lower=target_date.strftime("%B").lower(),
            day=target_date.day,
            year=target_date.year,
        )
        try:
            import asyncio as _asyncio
            from weather_edge.market.polymarket import fetch_market
            _snap = _asyncio.run(fetch_market(_slug, station, target_date))
            store.write_market_snapshot(_snap.model_dump(), station, target_date)
            snapshot = _snap
        except Exception as _exc:
            _console.print(f"[red]Market fetch failed: {_exc}[/red]")
            raise typer.Exit(1)
    else:
        snapshot = MarketSnapshot(**snap_raw)
    outcome_map = {o.label: o for o in snapshot.outcomes}

    # Build order table
    table = Table(title=f"Orders — {station} {target_date}", show_header=True)
    table.add_column("Bracket")
    table.add_column("Side")
    table.add_column("Edge")
    table.add_column("Kelly")
    table.add_column("Price")
    table.add_column("Stake (USDC)")
    table.add_column("OK?")

    order_rows = []
    for pick in locked.picks:
        outcome = outcome_map.get(pick.bracket_label)
        if outcome is None:
            _console.print(f"[yellow]Skipping {pick.bracket_label}: not in current snapshot[/yellow]")
            continue

        usdc_stake = round(pick.kelly_fraction * avail, 2)
        price = outcome.mid if pick.side == "YES" else (1.0 - outcome.mid)
        ok = usdc_stake >= MIN_ORDER_USDC

        table.add_row(
            pick.bracket_label,
            f"[green]{pick.side}[/green]" if pick.side == "YES" else f"[red]{pick.side}[/red]",
            f"{pick.edge:+.3f}",
            f"{pick.kelly_fraction*100:.1f}%",
            f"{price:.3f}",
            f"${usdc_stake:.2f}",
            "[green]YES[/green]" if ok else f"[red]NO (min ${MIN_ORDER_USDC:.0f})[/red]",
        )
        if ok:
            order_rows.append((pick, outcome, usdc_stake))

    _console.print(table)

    if not order_rows:
        _console.print(f"[yellow]All orders below minimum ${MIN_ORDER_USDC:.2f} USDC. Increase bankroll or wait for larger Kelly signal.[/yellow]")
        raise typer.Exit(0)

    mode_tag = "[yellow]DRY RUN[/yellow]" if dry_run else "[bold red]LIVE — REAL MONEY[/bold red]"
    _console.print(f"\nMode: {mode_tag}")

    if not yes:
        confirm = typer.prompt(
            f"Submit {len(order_rows)} order(s)? [yes/no]",
            default="no",
        )
        if confirm.strip().lower() not in ("yes", "y"):
            _console.print("[yellow]Aborted.[/yellow]")
            raise typer.Exit(0)

    records = []
    total_staked = 0.0
    for pick, outcome, usdc_stake in order_rows:
        try:
            rec = place_order(pick, outcome, usdc_stake, dry_run=dry_run)
            records.append(rec)
            total_staked += usdc_stake
            status = rec["status"]
            _console.print(
                f"  [green]{'[DRY]' if dry_run else '[LIVE]'}[/green] "
                f"{pick.side} {pick.bracket_label} ${usdc_stake:.2f} @ {rec['price']:.3f} — {status}"
            )
        except Exception as exc:
            _console.print(f"  [red]FAILED {pick.bracket_label}: {exc}[/red]")

    if records and not dry_run:
        br.reserve(bankroll, total_staked)

    path = save_execution(station, target_date, records)
    _console.print(f"\n[green]Saved execution record: {path}[/green]")
    if dry_run:
        _console.print("[dim]Re-run with --live to submit real orders.[/dim]")


# ─── Scheduler ────────────────────────────────────────────────────────────────

@app.command("scheduler")
def scheduler_cmd(
    stations: Annotated[Optional[str], typer.Option("--stations", help="Comma-separated station list")] = "EGLC",
) -> None:
    """Start the daily pipeline scheduler (blocking). Runs ingest/lock/resolve automatically."""
    from weather_edge.pipeline.scheduler import start as start_scheduler

    station_list = [s.strip() for s in (stations or "EGLC").split(",")]
    _console.print(f"Starting scheduler for stations: [bold]{', '.join(station_list)}[/bold]")
    _console.print("  17:30z - ingest forecasts")
    _console.print("  18:00z - lock picks for D+1")
    _console.print("  02:00z - ingest observations + resolve yesterday")
    _console.print("  Sun 03:00z - re-fit EMOS + QRF")
    _console.print("[yellow]Press Ctrl+C to stop[/yellow]")
    start_scheduler(station_list)


if __name__ == "__main__":
    app()
