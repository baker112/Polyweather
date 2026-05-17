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


def _lock_strategy_for(bma_mode_override: str | None) -> str:
    """Map the forecast-aggregation override to the lock-strategy identifier
    used for storage routing (picks file, executions subdir, dry bankroll).

    None         → "bma"       (D-1 evening BMA blend)
    "wn2_only"   → "intraday"  (same-day WN2-only short-lead)
    "wn2_peak"   → "peak"      (same-day WN2 p75 point-forecast)
    """
    if bma_mode_override is None:
        return "bma"
    if bma_mode_override == "wn2_only":
        return "intraday"
    if bma_mode_override == "wn2_peak":
        return "peak"
    return "bma"

# Floor on the predictive σ before computing bracket probabilities. The pre-2026-05-13
# /dump analysis showed bot betting NO on the exact resolved bracket ~30% of picks
# (vs ~17% expected for well-calibrated 6-bracket markets) — symptom of over-narrow
# distributions where σ collapsed to the inner classes' 0.5°C floor. Lifting to 1.5°C
# spreads bracket probability away from the central mass and stops the catastrophic
# "NO on the exact answer" failure mode.
_MIN_SIGMA = 1.5


class _SigmaFloored:
    """Wrap a predictive distribution so bracket_prob uses max(σ, _MIN_SIGMA).

    Falls back to a single Gaussian when the floor activates — strictly less
    flexible than the underlying BMA mixture / QRF kernel, but only kicks in
    when the distribution was so concentrated the mixture structure had
    collapsed anyway.
    """

    def __init__(self, inner: Any, min_sigma: float) -> None:
        self._inner = inner
        self.mu = float(inner.mu)
        self._raw_sigma = float(inner.sigma)
        self._sigma_eff = max(self._raw_sigma, min_sigma)
        self._floored = self._sigma_eff > self._raw_sigma

    @property
    def sigma(self) -> float:
        return self._sigma_eff

    def bracket_prob(self, low: float | None, high: float | None) -> float:
        if not self._floored:
            return self._inner.bracket_prob(low, high)
        from scipy.stats import norm  # type: ignore[import-untyped]
        p_low = 0.0 if low is None else float(norm.cdf(low, self.mu, self._sigma_eff))
        p_high = 1.0 if high is None else float(norm.cdf(high, self.mu, self._sigma_eff))
        return p_high - p_low


def lock_picks(
    target_date: date,
    station_id: str,
    now_utc: datetime,
    force: bool = False,
    bma_mode_override: str | None = None,
    init_dt_override: datetime | None = None,
    kelly_multiplier_override: float | None = None,
) -> LockedPicks:
    """Orchestrate stages 1-6 and write immutable picks file.

    Raises AlreadyLockedError if picks already exist for this (date, station).
    Pass force=True to overwrite existing picks with fresh market data.

    bma_mode_override: if provided, replaces station.bma_mode for this call.
        Used by the intraday lock job to force WN2-only without touching yaml.
    init_dt_override: if provided, skips the default `_most_recent_12z` choice
        and uses this init datetime. Used by the intraday lock job to pull the
        freshest available init (e.g. today's 06z) rather than yesterday's 12z.
    kelly_multiplier_override: if provided, replaces thresholds.kelly_multiplier
        in stake sizing. Intraday uses 0.25 (quarter Kelly) because the mode is
        new and untested; regular D-1 locks use the configured 0.5 (half Kelly).
    """
    from weather_edge.logging import log_event

    lock_strategy = _lock_strategy_for(bma_mode_override)

    if store.picks_exist(station_id, target_date, mode=lock_strategy):
        if not force:
            raise AlreadyLockedError(
                f"Picks already locked for {station_id} on {target_date} "
                f"(mode={lock_strategy})"
            )
        _logger.info(
            "Force re-lock: deleting existing picks for %s %s (mode=%s)",
            station_id, target_date, lock_strategy,
        )
        picks_path = (
            store._DATA_DIR / "picks" / f"date={target_date}"
            / f"station={station_id}" / store._picks_filename(lock_strategy)
        )
        picks_path.unlink(missing_ok=True)

    station = get_station(station_id)

    provenance: dict[str, Any] = {"lock_strategy": lock_strategy}
    pipeline_start = time.monotonic()

    # ── Stage 1: Ingest forecasts ─────────────────────────────────────────────
    # Per-stage timer anchored immediately before the work; the prior
    # version reused the pipeline-start timer, conflating later stages'
    # latency with the ingest figure on long lock_picks runs.
    t0 = time.monotonic()
    init_dt = init_dt_override if init_dt_override is not None else _most_recent_12z(now_utc)
    provenance["init_dt"] = init_dt.isoformat()
    if bma_mode_override:
        provenance["bma_mode_override"] = bma_mode_override

    # In wn2_only / wn2_peak modes the other models aren't used for the pick.
    # Skip their ingests entirely — they pull from rate-limited (multiurl)
    # sources and can serially block the WN2 ingest for minutes if e.g. GEFS
    # is 429'd.
    effective_bma_mode = bma_mode_override or station.bma_mode
    skip_non_wn2 = effective_bma_mode in ("wn2_only", "wn2_peak")
    if skip_non_wn2:
        provenance["skipped_non_wn2_ingests"] = True

    forecast_dfs: list[pl.DataFrame] = []

    if not skip_non_wn2:
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

        try:
            from weather_edge.ingest import icon
            df_icon = _load_or_fetch_forecasts("icon", init_dt, station, icon.ingest_forecasts)
            forecast_dfs.append(df_icon)
            provenance["icon_members"] = int(df_icon.filter(pl.col("valid_date") == target_date).height)
        except (IngestError, Exception) as exc:
            _logger.warning("ICON ingest failed: %s", exc)
            provenance["icon_error"] = str(exc)

    try:
        from weather_edge.ingest import weathernext
        df_wn = _load_or_fetch_forecasts("weathernext", init_dt, station, weathernext.ingest_forecasts)
        forecast_dfs.append(df_wn)
        provenance["weathernext_members"] = int(df_wn.filter(pl.col("valid_date") == target_date).height)
    except (IngestError, Exception) as exc:
        _logger.warning("WeatherNext ingest failed: %s", exc)
        provenance["weathernext_error"] = str(exc)

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

    # Strip blown-up ensemble members before fitting. KLGA hit max-sigma 8.9°C
    # in the May-10 dump because a single outlier inflated ens_var. We use
    # MAD-based filtering (robust to small-N ensembles) and drop anything more
    # than 4 MADs from the median — keeps real distribution width, kills bugs.
    pre_n = len(ensemble_values)
    ensemble_values = _strip_outliers(ensemble_values)
    if pre_n - len(ensemble_values):
        provenance["ensemble_outliers_dropped"] = pre_n - len(ensemble_values)

    provenance["ensemble_size"] = len(ensemble_values)
    log_event("ingest_forecasts", station_id, "ok",
              (time.monotonic() - t0) * 1000, ensemble_size=len(ensemble_values))

    # ── Stage 3+4: EMOS fit + predict PDF (BMA mixture when per-model params exist) ──
    t1 = time.monotonic()

    # Separate per-model forecast values for BMA path
    model_values: dict[str, list[float]] = {}
    for _model in ("ecmwf", "gefs", "icon", "weathernext"):
        _mdf = all_forecasts.filter(
            (pl.col("model") == _model)
            & (pl.col("valid_date") == target_date)
            & (pl.col("lead_hours") == lead_hours)
        )
        if not _mdf.is_empty():
            model_values[_model] = _strip_outliers(_mdf["daily_max_c"].to_list())

    dist = _stage3_4(
        station_id, lead_hours, now_utc, target_date, ensemble_values, model_values, provenance,
        bma_mode_override=bma_mode_override,
    )
    raw_sigma = float(dist.sigma)
    dist = _SigmaFloored(dist, _MIN_SIGMA)
    if dist._floored:
        _logger.info(
            "Stage 3+4: σ floored %.2f → %.2f for %s %s (mode=%s)",
            raw_sigma, dist.sigma, station_id, target_date, provenance.get("mode"),
        )
        provenance["sigma_raw"] = raw_sigma
        provenance["sigma_floored"] = True
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
            return _no_pick(
                target_date, station_id, now_utc, dist, f"market_error: {exc}",
                provenance, lock_strategy=lock_strategy,
            )

    log_event("fetch_market", station_id, "ok",
              (time.monotonic() - t2) * 1000, implied_sum=snapshot.implied_sum)

    # ── Stage 6: Compute edges ────────────────────────────────────────────────
    brackets = [BracketSpec(label=o.label, low=o.low, high=o.high) for o in snapshot.outcomes]

    # Per-station Kelly multiplier (#6) — auto-promotes once station has 50+
    # resolved bets with positive CLV; otherwise uses stations.yaml config value.
    from weather_edge.pipeline.edge_gate import effective_kelly_multiplier
    station_kelly_mult, kelly_reason = effective_kelly_multiplier(station_id)
    provenance["station_kelly_multiplier"] = station_kelly_mult
    provenance["station_kelly_reason"] = kelly_reason

    if effective_bma_mode == "wn2_peak":
        # Point-forecast strategy: trust μ; bet YES on the bracket containing
        # it; quarter-Kelly sizing; skip the probabilistic edge gates.
        candidates = _compute_peak_bet(
            dist.mu, brackets, snapshot, now_utc, station.min_liquidity,
            station_kelly_multiplier=station_kelly_mult,
            kelly_multiplier_override=kelly_multiplier_override,
            provenance=provenance,
        )
    else:
        bracket_probs = compute_brackets(dist, brackets)
        candidates = compute_edges(
            bracket_probs, snapshot, now_utc, station_kelly_mult,
            station_min_liquidity=station.min_liquidity,
            predictive_mu=dist.mu,
            kelly_multiplier_override=kelly_multiplier_override,
        )

    candidates.sort(key=lambda c: abs(c.edge), reverse=True)
    picks = candidates  # all qualifying brackets

    no_edge_reason: str | None = None
    if not picks:
        if effective_bma_mode == "wn2_peak":
            # _summarise_failures uses bracket_probs which we don't have in peak mode.
            no_edge_reason = (
                f"wn2_peak skipped: {provenance.get('wn2_peak_skip_reason', 'gates failed')}"
            )
        else:
            failing = _summarise_failures(
                candidates if candidates else [], bracket_probs, snapshot,
                station_min_liquidity=station.min_liquidity,
            )
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

    store.write_picks(result.model_dump(), station_id, target_date, mode=lock_strategy)
    log_event("lock_picks", station_id, "ok",
              (time.monotonic() - pipeline_start) * 1000, n_picks=len(picks))
    return result


# ─── Peak-bet (wn2_peak mode) ─────────────────────────────────────────────────


def _compute_peak_bet(
    mu: float,
    brackets: list[Any],   # list[BracketSpec]
    snapshot: MarketSnapshot,
    now_utc: datetime,
    station_min_liquidity: float | None,
    station_kelly_multiplier: float,
    kelly_multiplier_override: float | None,
    provenance: dict[str, Any],
) -> list[Candidate]:
    """wn2_peak strategy: bet YES on the single bracket containing μ.

    No probability computation, no edge math, no σ. Just: take the model's
    predicted peak, find which Polymarket bracket it lands in, bet quarter
    Kelly on YES. Sizing matches intraday — the model asserts 100% confidence
    by construction (raw Kelly = 1.0) so the cap chain is what actually does
    the work: max_kelly_fraction × kelly_multiplier_override (0.25 from the
    caller) × station_kelly_multiplier. Skips entirely if the bracket isn't
    in the market or its liquidity is too thin to fill.
    """
    thresholds = load_thresholds()
    kelly_mult = (
        kelly_multiplier_override
        if kelly_multiplier_override is not None
        else thresholds.kelly_multiplier
    )
    freshness_cutoff = now_utc - timedelta(minutes=thresholds.market_freshness_minutes)
    min_liquidity = (
        station_min_liquidity if station_min_liquidity is not None
        else thresholds.min_liquidity
    )

    # Find the bracket containing μ. Brackets may have open endpoints (None means ±∞).
    # Both μ and bracket bounds are in Celsius — market/polymarket.py:_to_celsius()
    # converts Fahrenheit bracket labels for US stations before they reach here.
    target_bracket = None
    for b in brackets:
        low_ok = b.low is None or mu >= b.low
        high_ok = b.high is None or mu < b.high
        if low_ok and high_ok:
            target_bracket = b
            break

    if target_bracket is None:
        bracket_summary = [
            f"{b.label}=[{b.low if b.low is not None else '−∞'}, {b.high if b.high is not None else '+∞'})"
            for b in brackets
        ]
        provenance["wn2_peak_target_bracket"] = None
        provenance["wn2_peak_available_brackets"] = bracket_summary
        provenance["wn2_peak_skip_reason"] = (
            f"μ={mu:.2f}°C outside bracket range — available: {bracket_summary}"
        )
        return []

    provenance["wn2_peak_target_bracket"] = target_bracket.label
    provenance["wn2_peak_predicted_mu"] = round(mu, 2)

    outcome_map = {o.label: o for o in snapshot.outcomes}
    outcome = outcome_map.get(target_bracket.label)
    if outcome is None:
        provenance["wn2_peak_skip_reason"] = f"bracket {target_bracket.label} not in market"
        return []

    # Side-relevant depth (YES bet → top_ask × best_ask).
    top_size_usdc = outcome.top_ask_size * (outcome.best_ask if outcome.best_ask > 0 else outcome.mid)
    has_book_data = outcome.top_ask_size > 0 or outcome.top_bid_size > 0

    gates = {
        "market_fresh": snapshot.fetched_at >= freshness_cutoff,
        "min_liquidity": outcome.liquidity >= min_liquidity,
        "min_top_size": (not has_book_data) or top_size_usdc >= thresholds.min_top_size_usdc,
        # Intentionally NO min_edge / max_spread / max_raw_prob / min_net_edge.
        # wn2_peak is an experimental "trust the model" strategy.
    }

    max_stake_usdc = (
        top_size_usdc * thresholds.depth_safety_factor
        if has_book_data and top_size_usdc > 0 else None
    )

    # Quarter-Kelly sizing. Model claims certainty (model_prob=1.0) so the raw
    # Kelly numerator (1.0 - market_prob) and denominator (1.0 - market_prob)
    # cancel to 1.0. The cap × kelly_mult × station_kelly chain bounds it.
    kelly = (
        min(1.0, thresholds.max_kelly_fraction)
        * kelly_mult
        * station_kelly_multiplier
    )

    candidate = Candidate(
        bracket_label=target_bracket.label,
        low=target_bracket.low,
        high=target_bracket.high,
        model_prob=1.0,  # by construction (we're betting on it as if it's the answer)
        market_prob=outcome.mid,
        edge=1.0 - outcome.mid,
        side="YES",
        spread=outcome.spread,
        liquidity=outcome.liquidity,
        kelly_fraction=round(kelly, 4),
        max_stake_usdc=round(max_stake_usdc, 2) if max_stake_usdc is not None else None,
        gates=gates,
        raw_values={
            "wn2_peak_mu": mu,
            "market_mid": outcome.mid,
            "top_size_usdc": top_size_usdc,
            "kelly_raw": 1.0,
            "kelly_mult": kelly_mult,
        },
    )
    return [candidate] if all(gates.values()) else []


# ─── Edge detection ───────────────────────────────────────────────────────────

def compute_edges(
    bracket_probs: list[Any],  # list[BracketProb]
    snapshot: MarketSnapshot,
    now_utc: datetime,
    station_kelly_multiplier: float = 1.0,
    station_min_liquidity: float | None = None,
    predictive_mu: float | None = None,
    kelly_multiplier_override: float | None = None,
) -> list[Candidate]:
    thresholds = load_thresholds()
    kelly_mult = (
        kelly_multiplier_override
        if kelly_multiplier_override is not None
        else thresholds.kelly_multiplier
    )
    freshness_cutoff = now_utc - timedelta(minutes=thresholds.market_freshness_minutes)
    min_liquidity = (
        station_min_liquidity if station_min_liquidity is not None
        else thresholds.min_liquidity
    )

    outcome_map = {o.label: o for o in snapshot.outcomes}
    candidates: list[Candidate] = []

    for bp in bracket_probs:
        if bp.label not in outcome_map:
            continue
        outcome = outcome_map[bp.label]

        edge = bp.model_prob - outcome.mid
        side = "YES" if edge > 0 else "NO"

        # Guard against betting NO on the bracket that contains the predictive
        # mean. The /dump 2026-05-13 analysis showed this was the dominant
        # losing-mode: when the bot's μ landed inside bracket X, it would
        # *still* compute NO edge on X (because market priced X higher than
        # model's narrow-distribution prob) and then lose hard when truth
        # actually landed in X. Skip silently — no candidate, no gate failure.
        if (
            side == "NO"
            and predictive_mu is not None
            and bp.low is not None and bp.high is not None
            and bp.low <= predictive_mu < bp.high
        ):
            continue

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

        # max_raw_prob must apply to whichever side we're betting. A NO bet
        # when model_prob=0.01 means we're betting at 99% NO confidence —
        # equally as extreme as a YES bet at 99% YES confidence and just as
        # likely to be model bias rather than real edge. Previously this gate
        # only checked the YES probability; that let through "model says 0%,
        # market says 100%, bet NO at -99% edge" picks where WN2 had simply
        # under-sampled the afternoon peak.
        confidence_on_bet_side = bp.model_prob if side == "YES" else (1.0 - bp.model_prob)
        gates = {
            "min_edge": abs(edge) >= thresholds.min_edge,
            "max_spread": outcome.spread <= thresholds.max_spread,
            "min_liquidity": outcome.liquidity >= min_liquidity,
            "max_raw_prob": confidence_on_bet_side <= thresholds.max_raw_prob,
            "market_fresh": snapshot.fetched_at >= freshness_cutoff,
            "min_top_size": (not has_book_data) or top_size_usdc >= thresholds.min_top_size_usdc,
            "min_net_edge": net_edge >= thresholds.min_net_edge,
        }

        # Full Kelly fraction: f* = |edge| / price_of_losing_side, capped at max_kelly_fraction
        if edge > 0:  # YES bet
            kelly = edge / (1.0 - outcome.mid) if outcome.mid < 1.0 else 0.0
        else:  # NO bet
            kelly = abs(edge) / outcome.mid if outcome.mid > 0.0 else 0.0
        kelly = (
            min(kelly, thresholds.max_kelly_fraction)
            * kelly_mult
            * station_kelly_multiplier
        )

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
    bma_mode_override: str | None = None,
) -> Any:
    """Return a PredictedDistribution, BMAMixture, or QRFDistribution.

    Priority order:
      Phase 0 — WN2-only short-circuit when station.bma_mode == "wn2_only"
                or when bma_mode_override == "wn2_only" (intraday job).
      Phase 3 — QRF (if fitted params exist on disk)
      Phase 2 — BMA mixture (if per-model EMOS params exist for ≥2 models)
      Phase 1 — Pooled EMOS fallback
    """
    # ── Phase 0: WN2-only short-circuit (also covers wn2_peak; same forecast) ──
    station_cfg = get_station(station_id)
    effective_mode = bma_mode_override or station_cfg.bma_mode
    if effective_mode in ("wn2_only", "wn2_peak"):
        # wn2_peak uses the 75th percentile of per-member daily max as the
        # "predicted peak" instead of the ensemble mean. WN2's 6h cadence
        # systematically under-samples the actual afternoon peak hour (the
        # observed daily max often lies between two of WN2's 6h-stepped
        # lead samples). Using a higher quantile biases the prediction
        # toward the realised peak by ~1°C, partially compensating until
        # WN2-specific EMOS calibration provides a learned correction.
        quantile = 0.75 if effective_mode == "wn2_peak" else None
        return _wn2_only_distribution(
            station_id, lead_hours, now_utc, target_date, model_values, provenance,
            quantile=quantile,
        )

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
    for model in ("ecmwf", "gefs", "icon", "weathernext"):
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


def _wn2_only_distribution(
    station_id: str,
    lead_hours: int,
    now_utc: datetime,
    target_date: date,
    model_values: dict[str, list[float]],
    provenance: dict[str, Any],
    quantile: float | None = None,
) -> PredictedDistribution:
    """WN2-only path: μ/σ come from the 64-member WN2 ensemble alone.

    quantile=None (default): μ = ensemble mean. EMOS-aware. Used by wn2_only.
    quantile=q (e.g. 0.75):  μ = ensemble q-th percentile. EMOS is skipped
                             (no calibrated quantile fit available yet).
                             Used by wn2_peak to compensate for the 6h-cadence
                             peak-undershoot bias.

    The σ floor in lock_picks still applies downstream for wn2_only — wn2_peak
    bypasses bracket-probability math entirely so σ is informational only.
    """
    import numpy as np

    wn2_values = model_values.get("weathernext", [])
    if len(wn2_values) < 3:
        raise IngestError(
            f"wn2_only mode but only {len(wn2_values)} WeatherNext members "
            f"for {station_id} {target_date} lead={lead_hours}h — needs ≥3"
        )

    if quantile is not None:
        mu = float(np.quantile(wn2_values, quantile))
        sigma = float(np.std(wn2_values, ddof=1)) if len(wn2_values) > 1 else 0.5
        provenance["mode"] = f"wn2_peak_q{int(quantile*100)}"
        provenance["wn2_n_members"] = len(wn2_values)
        provenance["wn2_quantile"] = quantile
        _logger.info(
            "Stage 3+4: WN2-peak q%d (members=%d, μ=%.2f, σ=%.2f)",
            int(quantile * 100), len(wn2_values), mu, sigma,
        )
        return PredictedDistribution(
            mu=mu,
            sigma=sigma,
            station=station_id,
            valid_date=target_date,
            lead_hours=lead_hours,
        )

    raw_emos = store.read_emos_params(station_id, lead_hours, now_utc, model="weathernext")
    if raw_emos is not None:
        emos_params = EmosParams(**raw_emos)
        dist = predict_pdf(wn2_values, emos_params, target_date)
        provenance["mode"] = "wn2_only_emos"
        provenance["wn2_emos_n_samples"] = emos_params.n_samples
        provenance["wn2_emos_train_crps"] = emos_params.train_crps
        provenance["wn2_n_members"] = len(wn2_values)
        _logger.info(
            "Stage 3+4: WN2-only EMOS (n_train=%d, members=%d)",
            emos_params.n_samples, len(wn2_values),
        )
        return dist

    mu = float(np.mean(wn2_values))
    sigma = float(np.std(wn2_values, ddof=1)) if len(wn2_values) > 1 else 0.5
    provenance["mode"] = "wn2_only_raw"
    provenance["wn2_n_members"] = len(wn2_values)
    _logger.info(
        "Stage 3+4: WN2-only raw (members=%d, μ=%.2f, σ=%.2f) — no WN2 EMOS yet",
        len(wn2_values), mu, sigma,
    )
    return PredictedDistribution(
        mu=mu,
        sigma=sigma,
        station=station_id,
        valid_date=target_date,
        lead_hours=lead_hours,
    )


# ─── Helpers ──────────────────────────────────────────────────────────────────

def _strip_outliers(values: list[float], k: float = 4.0) -> list[float]:
    """Drop ensemble members more than `k` MADs from the median.

    Robust outlier filter (MAD is unaffected by the very outliers we're trying
    to remove, unlike σ). Keeps everything if the ensemble has <5 members or
    if MAD is 0 (all members identical → nothing to filter).
    """
    import numpy as np
    if len(values) < 5:
        return values
    arr = np.asarray(values, dtype=np.float64)
    med = float(np.median(arr))
    mad = float(np.median(np.abs(arr - med)))
    if mad <= 0:
        return values
    # 1.4826 scales MAD to match σ for a Gaussian distribution.
    threshold = k * 1.4826 * mad
    kept = arr[np.abs(arr - med) <= threshold]
    # Safety: never strip more than half the ensemble (would mean MAD itself is broken).
    if len(kept) < len(arr) // 2:
        return values
    return kept.tolist()


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


def _most_recent_init(now_utc: datetime, min_lag_hours: int = 6) -> datetime:
    """Return the most recent WN2 init (00/06/12/18 UTC) published at least `min_lag_hours` ago.

    Default `min_lag_hours=6`: empirical WN2 publication latency on GCS exceeds 5
    hours sometimes (observed 2026-05-16: 12z init not present at 17:00 UTC).
    6 is the conservative-but-still-fresh choice. Callers can pass a larger
    value (e.g. 12) for the fallback retry path when the chosen init has not
    yet published.

    Examples (with default min_lag=6):
      - 12:30 UTC → cutoff 06:30 → 06z init (6.5h old). Lead to 14z peak = 8h.
      - 17:30 UTC → cutoff 11:30 → 06z init (11.5h old). Lead to 19z peak = 13h.
      - 18:30 UTC → cutoff 12:30 → 12z init (6.5h old). Lead to 20z peak = 8h.
    """
    now_utc = now_utc.replace(tzinfo=timezone.utc)
    cutoff = now_utc - timedelta(hours=min_lag_hours)
    # Walk back hour-by-hour to find the most recent 00/06/12/18 at or before cutoff.
    candidate = cutoff.replace(minute=0, second=0, microsecond=0)
    while candidate.hour not in (0, 6, 12, 18):
        candidate -= timedelta(hours=1)
    return candidate


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
    lock_strategy: str = "bma",
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
    store.write_picks(result.model_dump(), station_id, target_date, mode=lock_strategy)
    return result


def _summarise_failures(
    all_candidates: list[Candidate],
    bracket_probs: list[Any],
    snapshot: MarketSnapshot,
    station_min_liquidity: float | None = None,
) -> str:
    thresholds = load_thresholds()
    min_liq = (
        station_min_liquidity if station_min_liquidity is not None
        else thresholds.min_liquidity
    )
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
        if outcome.liquidity < min_liq:
            failures.append(f"{bp.label}: liq=${outcome.liquidity:.0f}<${min_liq:.0f}")
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
