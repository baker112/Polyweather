"""Main pipeline orchestrator.

lock_picks(date, station, now_utc) → LockedPicks

Stages:
  1. ingest_forecasts  (ECMWF + GEFS)
  2. ingest_observations  (Iowa Mesonet, used for EMOS training)
  3. fit_emos / load cached EmosParams
  4. predict_pdf
  5. fetch_market
  6. compute_edges → lock_picks
"""
from __future__ import annotations

import asyncio
import logging
import time
from datetime import date, datetime, timedelta, timezone
from typing import Any

import polars as pl

from weather_edge.config import StationConfig, get_station, load_thresholds
from weather_edge.exceptions import AlreadyLockedError, EmosError, IngestError, MarketError
from weather_edge.models import (
    BracketSpec,
    Candidate,
    EmosParams,
    LockedPicks,
    MarketSnapshot,
    PredictedDistribution,
)
from weather_edge.postprocess.emos import (
    assemble_training_pairs,
    bucket_lead_hours,
    compute_brackets,
    fit_emos,
    fit_emos_per_model,
    predict_pdf,
)
from weather_edge.store import parquet as store

_logger = logging.getLogger(__name__)

_DEFAULT_LEAD_HOURS = 24  # primary lead bucket for the 12z run targeting D+1


def lock_picks(
    target_date: date,
    station_id: str,
    now_utc: datetime,
    force: bool = False,
) -> LockedPicks:
    """Orchestrate stages 1-6 and write immutable picks file.

    Raises AlreadyLockedError if picks already exist for this (date, station).
    Pass force=True to overwrite existing picks with fresh market data.
    """
    from weather_edge.logging import log_event

    if store.picks_exist(station_id, target_date):
        if not force:
            raise AlreadyLockedError(f"Picks already locked for {station_id} on {target_date}")
        _logger.info("Force re-lock: deleting existing picks for %s %s", station_id, target_date)
        picks_path = store._DATA_DIR / "picks" / f"date={target_date}" / f"station={station_id}" / "picks.json"
        picks_path.unlink(missing_ok=True)

    station = get_station(station_id)

    provenance: dict[str, Any] = {}
    pipeline_start = time.monotonic()

    # ── Stage 1: Ingest forecasts ─────────────────────────────────────────────
    # Per-stage timer anchored immediately before the work; the prior
    # version reused the pipeline-start timer, conflating later stages'
    # latency with the ingest figure on long lock_picks runs.
    t0 = time.monotonic()
    init_dt = _most_recent_12z(now_utc)
    provenance["init_dt"] = init_dt.isoformat()
    forecast_dfs: list[pl.DataFrame] = []

    try:
        from weather_edge.ingest import ecmwf
        df_ecmwf = _load_or_fetch_forecasts("ecmwf", init_dt, station, ecmwf.ingest_forecasts)
        forecast_dfs.append(df_ecmwf)
        provenance["ecmwf_members"] = int(df_ecmwf.filter(pl.col("valid_date") == target_date).height)
    except (IngestError, Exception) as exc:
        _logger.warning("ECMWF ingest failed: %s", exc)
        provenance["ecmwf_error"] = str(exc)

    try:
        from weather_edge.ingest import gefs
        df_gefs = _load_or_fetch_forecasts("gefs", init_dt, station, gefs.ingest_forecasts)
        forecast_dfs.append(df_gefs)
        provenance["gefs_members"] = int(df_gefs.filter(pl.col("valid_date") == target_date).height)
    except (IngestError, Exception) as exc:
        _logger.warning("GEFS ingest failed: %s", exc)
        provenance["gefs_error"] = str(exc)

    if not forecast_dfs:
        raise IngestError("All forecast sources failed")

    all_forecasts = pl.concat(forecast_dfs)
    lead_hours = bucket_lead_hours(
        int((datetime(target_date.year, target_date.month, target_date.day, 12, tzinfo=timezone.utc)
             - init_dt.replace(tzinfo=timezone.utc)).total_seconds() // 3600)
    )

    target_fcs = all_forecasts.filter(
        (pl.col("valid_date") == target_date) & (pl.col("lead_hours") == lead_hours)
    )
    ensemble_values = target_fcs["daily_max_c"].to_list()
    if not ensemble_values:
        # Fallback: relax lead_hours constraint — take any rows for the target date
        # and snap to the nearest available bucket. Handles cases where the cached
        # forecast was written with a slightly different lead (e.g. timezone offset
        # shifts noon-local vs noon-UTC by a few hours).
        date_fcs = all_forecasts.filter(pl.col("valid_date") == target_date)
        if date_fcs.is_empty():
            available = all_forecasts["valid_date"].unique().to_list() if not all_forecasts.is_empty() else []
            _logger.warning(
                "No forecast rows for %s at all — available dates: %s", target_date, sorted(available)
            )
            raise IngestError(f"No forecast values for {target_date} at lead={lead_hours}h")
        # Pick the lead bucket closest to expected
        available_leads = date_fcs["lead_hours"].unique().to_list()
        best_lead = min(available_leads, key=lambda h: abs(h - lead_hours))
        _logger.warning(
            "No forecasts for %s at lead=%dh; falling back to lead=%dh (available: %s)",
            target_date, lead_hours, best_lead, sorted(available_leads),
        )
        target_fcs = date_fcs.filter(pl.col("lead_hours") == best_lead)
        ensemble_values = target_fcs["daily_max_c"].to_list()
        lead_hours = best_lead

    provenance["ensemble_size"] = len(ensemble_values)
    log_event("ingest_forecasts", station_id, "ok",
              (time.monotonic() - t0) * 1000, ensemble_size=len(ensemble_values))

    # ── Stage 3+4: EMOS fit + predict PDF (BMA mixture when per-model params exist) ──
    t1 = time.monotonic()

    # Separate per-model forecast values for BMA path
    model_values: dict[str, list[float]] = {}
    for _model in ("ecmwf", "gefs"):
        _mdf = all_forecasts.filter(
            (pl.col("model") == _model)
            & (pl.col("valid_date") == target_date)
            & (pl.col("lead_hours") == lead_hours)
        )
        if not _mdf.is_empty():
            model_values[_model] = _mdf["daily_max_c"].to_list()

    dist = _stage3_4(
        station_id, lead_hours, now_utc, target_date, ensemble_values, model_values, provenance
    )
    log_event("fit_emos", station_id, "ok",
              (time.monotonic() - t1) * 1000,
              mode=provenance.get("mode", "pooled"),
              **{k: v for k, v in provenance.items() if k.startswith("emos")})

    store.write_prediction(
        {"mu": dist.mu, "sigma": dist.sigma, "station": station_id, "date": str(target_date)},
        station_id, target_date,
    )
    provenance["mu"] = dist.mu
    provenance["sigma"] = dist.sigma

    # ── Stage 5: Fetch market ─────────────────────────────────────────────────
    t2 = time.monotonic()
    slug = station.market_slug_pattern.format(
        date=target_date.strftime("%Y-%m-%d"),
        month_lower=target_date.strftime("%B").lower(),
        day=target_date.day,
        year=target_date.year,
    )

    snapshot: MarketSnapshot | None = None

    # For historical dates (backtest), load cached snapshot before live fetch.
    # Live lock always fetches fresh to respect the market_freshness gate.
    is_historical = target_date < now_utc.date()
    if is_historical:
        cached_raw = store.read_market_snapshot(station_id, target_date)
        if cached_raw is not None:
            try:
                snapshot = MarketSnapshot(**cached_raw)
                _logger.info("Market: loaded from cache for %s %s", station_id, target_date)
            except Exception:
                snapshot = None

    if snapshot is None:
        try:
            snapshot = asyncio.run(_fetch_and_persist_market(slug, station_id, target_date))
        except (MarketError, Exception) as exc:
            _logger.warning("Market fetch failed: %s", exc)
            log_event("fetch_market", station_id, "error",
                      (time.monotonic() - t2) * 1000, error=str(exc))
            return _no_pick(target_date, station_id, now_utc, dist, f"market_error: {exc}", provenance)

    log_event("fetch_market", station_id, "ok",
              (time.monotonic() - t2) * 1000, implied_sum=snapshot.implied_sum)

    # ── Stage 6: Compute edges ────────────────────────────────────────────────
    brackets = [BracketSpec(label=o.label, low=o.low, high=o.high) for o in snapshot.outcomes]
    bracket_probs = compute_brackets(dist, brackets)
    candidates = compute_edges(bracket_probs, snapshot, now_utc)

    candidates.sort(key=lambda c: abs(c.edge), reverse=True)
    picks = candidates  # all qualifying brackets

    no_edge_reason: str | None = None
    if not picks:
        failing = _summarise_failures(candidates if candidates else [], bracket_probs, snapshot)
        no_edge_reason = f"no_edge — gates: {failing}"
        _logger.info("%s %s: %s", station_id, target_date, no_edge_reason)

    result = LockedPicks(
        date=target_date,
        station=station_id,
        locked_at=now_utc,
        mu=dist.mu,
        sigma=dist.sigma,
        picks=picks,
        no_edge_reason=no_edge_reason,
        provenance=provenance,
    )

    store.write_picks(result.model_dump(), station_id, target_date)
    log_event("lock_picks", station_id, "ok",
              (time.monotonic() - pipeline_start) * 1000, n_picks=len(picks))
    return result


# ─── Edge detection ───────────────────────────────────────────────────────────

def compute_edges(
    bracket_probs: list[Any],  # list[BracketProb]
    snapshot: MarketSnapshot,
    now_utc: datetime,
) -> list[Candidate]:
    thresholds = load_thresholds()
    freshness_cutoff = now_utc - timedelta(minutes=thresholds.market_freshness_minutes)

    outcome_map = {o.label: o for o in snapshot.outcomes}
    candidates: list[Candidate] = []

    for bp in bracket_probs:
        if bp.label not in outcome_map:
            continue
        outcome = outcome_map[bp.label]

        edge = bp.model_prob - outcome.mid
        side = "YES" if edge > 0 else "NO"

        # Side-relevant top-of-book depth:
        #   YES bet → buy YES at best_ask, depth = top_ask_size (shares) * best_ask (price/share)
        #   NO bet  → buy NO  ≈ sell YES at best_bid (or buy NO at its 1-best_bid ask),
        #             depth in USDC of NO contracts ≈ top_bid_size * (1 - best_bid)
        if side == "YES":
            top_size_shares = outcome.top_ask_size
            fill_price = outcome.best_ask if outcome.best_ask > 0 else outcome.mid
        else:
            top_size_shares = outcome.top_bid_size
            fill_price = (1.0 - outcome.best_bid) if outcome.best_bid > 0 else (1.0 - outcome.mid)
        top_size_usdc = top_size_shares * fill_price
        # Pre-feature snapshots have both sizes = 0; treat as "unknown" rather
        # than zero-depth so backtest replay against legacy snapshots still works.
        has_book_data = outcome.top_ask_size > 0 or outcome.top_bid_size > 0
        net_edge = abs(edge) - outcome.spread

        gates = {
            "min_edge": abs(edge) >= thresholds.min_edge,
            "max_spread": outcome.spread <= thresholds.max_spread,
            "min_liquidity": outcome.liquidity >= thresholds.min_liquidity,
            "max_raw_prob": bp.model_prob <= thresholds.max_raw_prob,
            "market_fresh": snapshot.fetched_at >= freshness_cutoff,
            "min_top_size": (not has_book_data) or top_size_usdc >= thresholds.min_top_size_usdc,
            "min_net_edge": net_edge >= thresholds.min_net_edge,
        }

        # Full Kelly fraction: f* = |edge| / price_of_losing_side, capped at max_kelly_fraction
        if edge > 0:  # YES bet
            kelly = edge / (1.0 - outcome.mid) if outcome.mid < 1.0 else 0.0
        else:  # NO bet
            kelly = abs(edge) / outcome.mid if outcome.mid > 0.0 else 0.0
        kelly = min(kelly, thresholds.max_kelly_fraction) * thresholds.kelly_multiplier

        # Depth-implied stake cap: only consume `depth_safety_factor` of top-of-book.
        # Skip the cap when book data is unavailable (legacy snapshots).
        max_stake_usdc = (
            top_size_usdc * thresholds.depth_safety_factor
            if has_book_data and top_size_usdc > 0 else None
        )

        candidates.append(Candidate(
            bracket_label=bp.label,
            low=bp.low,
            high=bp.high,
            model_prob=bp.model_prob,
            market_prob=outcome.mid,
            edge=edge,
            side=side,
            spread=outcome.spread,
            liquidity=outcome.liquidity,
            kelly_fraction=round(kelly, 4),
            max_stake_usdc=round(max_stake_usdc, 2) if max_stake_usdc is not None else None,
            gates=gates,
            raw_values={
                "edge": edge,
                "spread": outcome.spread,
                "liquidity": outcome.liquidity,
                "model_prob": bp.model_prob,
                "top_size_usdc": top_size_usdc,
                "net_edge": net_edge,
            },
        ))

    return [c for c in candidates if all(c.gates.values())]


# ─── Stage 3+4 helper — pooled EMOS or BMA mixture ───────────────────────────

def _stage3_4(
    station_id: str,
    lead_hours: int,
    now_utc: datetime,
    target_date: date,
    ensemble_values: list[float],
    model_values: dict[str, list[float]],
    provenance: dict[str, Any],
) -> Any:
    """Return a PredictedDistribution, BMAMixture, or QRFDistribution.

    Priority order:
      Phase 3 — QRF (if fitted params exist on disk)
      Phase 2 — BMA mixture (if per-model EMOS params exist for ≥2 models)
      Phase 1 — Pooled EMOS fallback
    """
    # ── Phase 3: QRF ─────────────────────────────────────────────────────────
    qrf_data = store.read_qrf_params(station_id, lead_hours, now_utc)
    if qrf_data is not None:
        from weather_edge.postprocess.qrf import predict_qrf
        forest = qrf_data["forest"]
        X_train = qrf_data["X_train"]
        y_train = qrf_data["y_train"]
        provenance["mode"] = "qrf"
        provenance["qrf_n_train"] = int(qrf_data["meta"].get("n_samples", len(y_train)))
        _logger.info("Stage 3+4: QRF (n=%d)", provenance["qrf_n_train"])
        return predict_qrf(forest, X_train, y_train, ensemble_values,
                           target_date, station_id, lead_hours)

    # ── Phase 2: BMA ─────────────────────────────────────────────────────────
    # Attempt to load per-model EMOS params (Phase 2 path)
    per_model: dict[str, EmosParams] = {}
    for model in ("ecmwf", "gefs"):
        raw = store.read_emos_params(station_id, lead_hours, now_utc, model=model)
        if raw is not None and model in model_values and len(model_values[model]) >= 3:
            per_model[model] = EmosParams(**raw)

    if len(per_model) >= 2:
        from weather_edge.postprocess.bma import (
            compute_bma_weights,
            predict_pdf_bma,
            rolling_model_crps,
        )
        model_crps = rolling_model_crps(station_id, lead_hours, now_utc.date())
        # Equal weights if CRPS history is unavailable (first few days after setup)
        weights = (
            compute_bma_weights(model_crps)
            if model_crps
            else {m: 1.0 / len(per_model) for m in per_model}
        )
        model_data = [
            (model, model_values[model], params)
            for model, params in per_model.items()
        ]
        provenance["mode"] = "bma"
        provenance["bma_weights"] = {m: round(w, 4) for m, w in weights.items()}
        _logger.info("Stage 3+4: BMA mixture (%d models, weights=%s)", len(per_model), weights)
        return predict_pdf_bma(model_data, weights, target_date, station_id, lead_hours)

    # Pooled fallback (Phase 1 path)
    emos_params = _get_pooled_emos_params(station_id, lead_hours, now_utc)
    provenance["mode"] = "pooled"
    provenance["emos_n_samples"] = emos_params.n_samples
    provenance["emos_train_crps"] = emos_params.train_crps
    _logger.info("Stage 3+4: pooled EMOS (n=%d)", emos_params.n_samples)
    return predict_pdf(ensemble_values, emos_params, target_date)


# ─── Helpers ──────────────────────────────────────────────────────────────────

def _most_recent_12z(now_utc: datetime) -> datetime:
    """Return the most recent ECMWF run that should be published (~7h lag for 12z, ~7h lag for 00z).

    Priority: today 12z → today 00z → yesterday 12z.
    """
    now_utc = now_utc.replace(tzinfo=timezone.utc)
    today_12z = now_utc.replace(hour=12, minute=0, second=0, microsecond=0)
    today_00z = now_utc.replace(hour=0, minute=0, second=0, microsecond=0)
    if now_utc >= today_12z + timedelta(hours=7):
        return today_12z
    if now_utc >= today_00z + timedelta(hours=7):
        return today_00z
    yesterday = now_utc - timedelta(days=1)
    return yesterday.replace(hour=12, minute=0, second=0, microsecond=0)


def _load_or_fetch_forecasts(
    model: str,
    init_dt: datetime,
    station: StationConfig,
    fetch_fn: Any,
) -> pl.DataFrame:
    cached = store.read_forecasts(model, init_dt, station.icao)
    if cached is not None:
        _logger.info("%s forecasts: loaded from cache", model)
        return cached
    df = fetch_fn(init_dt, station)
    df = df.with_columns([
        pl.lit(station.icao).alias("station"),
        pl.lit(init_dt.replace(tzinfo=timezone.utc)).alias("init_datetime"),
    ])
    store.write_forecasts(df, model, init_dt, station.icao)
    return df


def _get_pooled_emos_params(station_id: str, lead_hours: int, now_utc: datetime) -> EmosParams:
    """Load or fit pooled (all-model) EMOS params.

    If no historical forecast+obs pairs exist yet, bootstraps with the identity
    EMOS transform (a=0, b=1, c=0.5, d=1) so the pipeline can still run and
    fetch the market. Replace once ≥30 training pairs accumulate.
    """
    raw = store.read_emos_params(station_id, lead_hours, now_utc, model=None)
    if raw is not None:
        return EmosParams(**raw)

    pairs = assemble_training_pairs(station_id, lead_hours, now_utc.date())
    if pairs:
        params = fit_emos(pairs, station_id, lead_hours, now_utc=now_utc)
        # Defensive backtest invariant: never persist a fit whose valid_from
        # would let a same-or-earlier lock time read it. params.valid_from is
        # set to now_utc inside fit_emos; the assertion guards future drift.
        if params.valid_from > now_utc:
            raise EmosError(
                f"EMOS valid_from {params.valid_from} must be <= lock time {now_utc}"
            )
        store.write_emos_params(params.model_dump(), station_id, lead_hours, params.valid_from, model=None)
    else:
        _logger.warning(
            "No training pairs for %s lead=%dh — using identity EMOS bootstrap",
            station_id, lead_hours,
        )
        params = EmosParams(
            a=0.0, b=1.0, c=0.5, d=1.0,
            station=station_id, lead_hours=lead_hours,
            fitted_at=now_utc, training_window_days=0,
            n_samples=0, train_crps=float("nan"),
            valid_from=now_utc,
        )
    return params


async def _fetch_and_persist_market(
    slug: str, station_id: str, target_date: date
) -> MarketSnapshot:
    from weather_edge.market.polymarket import fetch_market
    snapshot = await fetch_market(slug, station_id, target_date)
    store.write_market_snapshot(snapshot.model_dump(), station_id, target_date)
    return snapshot


def _no_pick(
    target_date: date,
    station_id: str,
    now_utc: datetime,
    dist: PredictedDistribution,
    reason: str,
    provenance: dict[str, Any],
) -> LockedPicks:
    result = LockedPicks(
        date=target_date,
        station=station_id,
        locked_at=now_utc,
        mu=dist.mu,
        sigma=dist.sigma,
        picks=[],
        no_edge_reason=reason,
        provenance=provenance,
    )
    store.write_picks(result.model_dump(), station_id, target_date)
    return result


def _summarise_failures(
    all_candidates: list[Candidate],
    bracket_probs: list[Any],
    snapshot: MarketSnapshot,
) -> str:
    thresholds = load_thresholds()
    outcome_map = {o.label: o for o in snapshot.outcomes}
    failures: list[str] = []

    for bp in bracket_probs:
        if bp.label not in outcome_map:
            continue
        outcome = outcome_map[bp.label]
        edge = bp.model_prob - outcome.mid
        if abs(edge) < thresholds.min_edge:
            failures.append(f"{bp.label}: edge={edge:+.3f}<{thresholds.min_edge}")
        if outcome.spread > thresholds.max_spread:
            failures.append(f"{bp.label}: spread={outcome.spread:.3f}>{thresholds.max_spread}")
        if outcome.liquidity < thresholds.min_liquidity:
            failures.append(f"{bp.label}: liq=${outcome.liquidity:.0f}<${thresholds.min_liquidity:.0f}")
        side = "YES" if edge > 0 else "NO"
        has_book_data = outcome.top_ask_size > 0 or outcome.top_bid_size > 0
        if has_book_data:
            if side == "YES":
                top_usdc = outcome.top_ask_size * (outcome.best_ask or outcome.mid)
            else:
                top_usdc = outcome.top_bid_size * (1.0 - (outcome.best_bid or outcome.mid))
            if top_usdc < thresholds.min_top_size_usdc:
                failures.append(f"{bp.label}: top=${top_usdc:.1f}<${thresholds.min_top_size_usdc:.0f}")
        net = abs(edge) - outcome.spread
        if net < thresholds.min_net_edge:
            failures.append(f"{bp.label}: net_edge={net:+.3f}<{thresholds.min_net_edge}")

    return "; ".join(failures) if failures else "all edges below threshold"
