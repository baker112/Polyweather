# Weather Edge Model — Build Spec

A daily pipeline that ingests ensemble weather forecasts, post-processes them against historical observations, computes calibrated probability distributions over Polymarket temperature brackets, and surfaces picks where the model disagrees with the market.

**Status:** greenfield, no existing code. **Phase 1 only** — single city (London / EGLC), paper-trade only, no live trading.

---

## Goal

By the end of Phase 1 we should have a deterministic CLI that, given a target date, produces a calibrated probability distribution over tomorrow's London Heathrow daily-max temperature, compares it against Polymarket's market prices for the corresponding bracket market, and logs the picks (or "no edge") with full provenance for backtesting.

The same code path must work for live runs and historical backtests. No separate backtest harness.

---

## Non-goals (Phase 1)

- Real money trading. Paper-trade only.
- Multi-city. London / EGLC only until validated.
- ML / gradient boosting. Plain EMOS (Non-homogeneous Gaussian Regression) is the baseline; nothing fancier until it's beaten.
- Web UI. CLI + parquet logs are sufficient.
- Auto-deploy / cron. Manual invocation is fine; we'll cron later.

---

## Architecture

Six-stage pipeline backed by a single parquet store. Each stage is a pure function from `(inputs, params)` to `outputs`. Live and backtest are the same code; only the date and source-of-truth for "now" change.

```
ingest_forecasts → ingest_observations → fit_emos → predict_pdf → fetch_market → compute_edges → lock_picks
```

Persistence happens in two phases:

1. **Pre-compute persistence.** Forecasts and observations are written to parquet immediately on fetch. If anything downstream crashes, the irreplaceable data survives.
2. **Post-compute persistence.** EMOS params, predictions, market snapshots, and final picks are written after each stage.

### Directory layout

```
weather_edge/
├── pyproject.toml
├── README.md
├── config/
│   └── stations.yaml          # ICAO, lat/lon, timezone, market resolution rules
├── src/weather_edge/
│   ├── __init__.py
│   ├── cli.py                 # entry point: `we <command>`
│   ├── ingest/
│   │   ├── ecmwf.py           # ECMWF Open Data
│   │   ├── gefs.py            # GEFS via AWS Open Data
│   │   └── metar.py           # Iowa Mesonet ASOS archive
│   ├── postprocess/
│   │   ├── emos.py            # NGR fitting + prediction
│   │   └── crps.py            # CRPS scoring
│   ├── market/
│   │   └── polymarket.py      # CLOB API client
│   ├── pipeline/
│   │   ├── lock.py            # main lock_picks(date, station) function
│   │   └── backtest.py        # replays historical dates through same code
│   ├── store/
│   │   └── parquet.py         # DuckDB + parquet IO
│   └── eval/
│       ├── calibration.py     # reliability diagrams, PIT histograms
│       └── brier.py
├── data/
│   ├── forecasts/             # partitioned by (model, init_date, station)
│   ├── observations/          # partitioned by (station, date)
│   ├── emos_params/           # versioned with valid_from timestamp
│   ├── predictions/
│   ├── market_snapshots/
│   └── picks/                 # write-once log
└── tests/
    ├── test_emos.py           # EMOS fits known synthetic distributions correctly
    ├── test_brackets.py       # bracket probabilities sum to 1, tails handled
    ├── test_pipeline.py       # end-to-end on a frozen day
    └── fixtures/              # cached forecast/obs slices for deterministic tests
```

### Tech stack

- **Python 3.11+** (match `astral` and modern `xarray`)
- `xarray` + `cfgrib` — GRIB2 ingestion
- `polars` — transforms (faster than pandas for this workload)
- `duckdb` — analytical queries over parquet
- `scipy.optimize` — EMOS fitting (L-BFGS-B)
- `httpx` — async HTTP for Polymarket and Mesonet
- `typer` — CLI
- `pydantic` — config and data model validation
- `pytest` — tests
- `ruff` + `mypy --strict` — lint and type-check

No pandas (use polars). No requests (use httpx). No setup.py (use pyproject.toml with hatch or uv).

---

## Phase 0 — Resolution forensics (do this first, before any code)

Create `docs/RESOLUTION.md` documenting, for London:

- ICAO station Polymarket resolves against (likely **EGLL** Heathrow, but verify — could be EGLC City)
- Exact data field used (NWS-equivalent daily climate report TMAX? Highest hourly METAR? Highest 6-hourly synop?)
- Calendar day timezone (local? UTC?)
- Tiebreak rule for missing data
- Source URL for the resolution criteria

Verify by pulling **30 historical resolution outcomes** from Polymarket's resolved-markets API and comparing each against your reconstructed truth from METAR. **All 30 must match exactly.** If they don't, stop and figure out why before writing any model code.

---

## Stage 1 — Ingest forecasts

### `ingest_forecasts(init_datetime, station) -> ForecastBundle`

Inputs:
- `init_datetime`: UTC datetime of the model init cycle (e.g. `2026-04-25T12:00Z`)
- `station`: station ID from `config/stations.yaml`

Behaviour:
- Pulls **ECMWF Open Data**: HRES + 51-member ENS at 0.25°, daily max temperature at the station's grid cell, native timesteps only (no temporal interpolation).
- Pulls **GEFS** from AWS S3 Open Data: 31 members, 0.25°, same constraints.
- Bilinear interpolation to the station lat/lon is acceptable; document this in code comments.
- Writes to `data/forecasts/model={model}/init_date={YYYY-MM-DD}/init_hour={HH}/station={icao}/data.parquet`
- Each row: `(model, member_id, init_datetime, valid_date, station, daily_max_c, lead_hours)`

Failure modes:
- If a model is unavailable, log warning and continue with remaining models. Do not abort.
- If both models fail, raise `IngestError`. Caller decides retry policy.

Tests:
- Mock S3/HTTP responses with cached GRIB fixtures. Verify member counts, lead times, station extraction.

---

## Stage 2 — Ingest observations

### `ingest_observations(date_range, station) -> ObservationFrame`

Source: **Iowa Mesonet ASOS archive** (`https://mesonet.agron.iastate.edu/cgi-bin/request/asos.py`). Free, requires no auth, good historical depth.

Behaviour:
- Pulls hourly METAR temperature observations for the station over the date range.
- Computes daily max in the station's local timezone (per `stations.yaml`).
- **Handles the resolution-rule field** identified in Phase 0. If Polymarket uses something other than computed daily max from hourly METAR, this function must produce that exact field.
- Writes to `data/observations/station={icao}/data.parquet`, columns: `(station, date, daily_max_c, source, fetched_at)`.

Tests:
- Reproduce 5 known historical daily maxes from a public source to within 0.1°C.

---

## Stage 3 — EMOS post-processing

This is the model. Get this right.

### `fit_emos(training_pairs) -> EmosParams`

Implements **Non-homogeneous Gaussian Regression** (Gneiting et al. 2005).

Model:
```
Y | ensemble_mean=m̄, ensemble_var=s² ~ N(μ, σ²)
where μ = a + b·m̄
      σ² = c + d·s²
```

Constraints: `b > 0`, `c >= 0`, `d >= 0`. Use bounded L-BFGS-B.

Loss: minimise mean **CRPS** over the training window. Closed-form CRPS for a Gaussian forecast:

```
CRPS(N(μ,σ²), y) = σ · [ z·(2Φ(z) - 1) + 2φ(z) - 1/√π ]
where z = (y - μ) / σ
```

Implement `crps_gaussian(mu, sigma, y)` as a separate, pure, well-tested function.

Training set: rolling **60-day window** of `(ensemble_mean, ensemble_var, observed_daily_max)` triples for a fixed `(station, lead_time)`. Fit one parameter set per `(station, lead_time)`. Lead time is bucketed to 24h intervals (24h, 48h, 72h).

Output: `EmosParams(a, b, c, d, station, lead_time, fitted_at, training_window, n_samples, train_crps)`. Persist to `data/emos_params/` with a `valid_from` timestamp. **Backtests must use the params that were live at forecast time, not today's params** — this is the single most important correctness invariant.

Tests:
- Synthetic data: generate ensembles from a known Gaussian, verify EMOS recovers `(a, b, c, d)` close to ground truth.
- Verify CRPS implementation against a reference (`properscoring` library — only as a test dependency, not a runtime one).
- Verify rolling window doesn't leak future data into training.

### `predict_pdf(forecast_bundle, emos_params) -> PredictedDistribution`

Pool ensemble members from all available models into a single ensemble (Phase 1 keeps it simple — no per-model weighting). Compute `m̄` and `s²` across the pooled ensemble. Apply EMOS:

```
μ = a + b·m̄
σ = sqrt(c + d·s²)
```

Return a `PredictedDistribution` object that knows its `(μ, σ)` and exposes `.bracket_prob(low, high)` using the Gaussian CDF.

---

## Stage 4 — Bracket probabilities

### `compute_brackets(distribution, market_brackets) -> BracketProbs`

Inputs:
- `distribution`: PredictedDistribution from Stage 3
- `market_brackets`: list of `(label, low, high)` from the Polymarket market

Behaviour:
- For each bracket, compute `P(low <= Y < high) = Φ((high-μ)/σ) - Φ((low-μ)/σ)`.
- Lowest bracket should be `(-∞, low)`; highest `(high, +∞)`.
- **Floor every bracket probability at 1e-4** so nothing is rated impossible (guardrail against tail blow-ups).
- **Re-normalise** so probabilities sum to 1 after flooring.

Tests:
- Probabilities sum to 1.0 within 1e-9.
- Tail brackets correctly handle infinity.
- Flooring is applied before normalisation.

---

## Stage 5 — Market snapshot

### `fetch_market(market_slug) -> MarketSnapshot`

Polymarket exposes a CLOB REST API. Find the market for "London high temperature, [date]" — the slug pattern needs to be discovered (Phase 0 task). For each bracket outcome:

- Mid price (best_bid + best_ask) / 2
- Spread (best_ask - best_bid)
- Volume / liquidity proxy
- Timestamp of fetch

Persist every fetch to `data/market_snapshots/` — never overwrite. We need historical snapshots for closing-line value analysis.

Implied probability per bracket = mid price. Sanity check: market-implied probabilities across all brackets should sum to roughly 1.0–1.1 (the overround is the house edge / spread cost). If sum < 0.9 or > 1.2, flag the market as malformed and skip.

---

## Stage 6 — Edge detection and gating

### `compute_edges(model_probs, market_snapshot) -> list[Candidate]`

For each bracket present in both:
```
edge = model_prob - market_implied_prob
```

Candidate is a pick if **all** of these hold:
- `abs(edge) >= MIN_EDGE` (config, default 0.04 = 4 percentage points)
- `market_snapshot.spread <= MAX_SPREAD` (config, default 0.04)
- `market_snapshot.liquidity >= MIN_LIQUIDITY` (config, default $500 of resting orders)
- `model_prob <= MAX_RAW_PROB` (config, default 0.85 — guard against overconfident model)
- `market_snapshot.fetched_at` within last 10 minutes
- Side: BUY YES if `edge > 0`, BUY NO if `edge < 0`

Return all surviving candidates with full provenance (which gates passed, raw values).

### `lock_picks(date, station) -> LockedPicks`

The single entry point that orchestrates everything.

Behaviour:
1. Determine target init datetime: most recent 12z run available before `LOCK_TIME_UTC` (default 18:00 UTC).
2. Run stages 1–6.
3. Sort surviving candidates by `abs(edge)` desc, take top 1 (Phase 1 = max one pick per city per day).
4. Write to `data/picks/date={date}/station={station}/picks.parquet` with `mode='error_if_exists'` — locked picks are immutable.
5. Return the `LockedPicks` object.

Idempotency: calling `lock_picks` twice for the same `(date, station)` after the lock file exists raises `AlreadyLockedError`. There is no "update" path. The discipline matters more than the convenience.

---

## CLI

Use `typer`. Commands:

```bash
we ingest forecasts --station EGLL --init 2026-04-25T12:00Z
we ingest observations --station EGLL --start 2024-01-01 --end 2026-04-25
we fit-emos --station EGLL --lead 24 --as-of 2026-04-25
we lock --station EGLL --date 2026-04-26              # the daily entry point
we backtest --station EGLL --start 2026-01-01 --end 2026-04-25
we report --station EGLL --start 2026-01-01           # calibration + Brier + P&L
```

Every command must be **deterministic given its inputs** — no hidden global state, no `datetime.now()` outside a single injection point in `cli.py`.

---

## Backtest

`backtest(station, start, end)` walks every date in `[start, end]`:

1. For each date, identify what data *would have been available* at lock time (no peeking — the EMOS params used must have been fitted on data strictly before that date).
2. Run the same `lock_picks` function used live.
3. Once the resolution date arrives in the loop, look up the actual observation and score:
   - Bracket hit: 1 if observed daily max landed in the picked bracket, else 0
   - P&L: `payout - cost` where cost = entry mid price, payout = 1 if hit else 0
   - CRPS of the model's full distribution against the observation
4. Append to `data/backtest_results/`.

Critical invariant: a backtest run on the same date range twice produces byte-identical output. If it doesn't, there's a leak somewhere.

---

## Evaluation outputs

`we report` produces:

- **Reliability diagram** — bin model probabilities into deciles, plot predicted vs empirical hit rate. Should track the 45° line.
- **PIT histogram** — for each prediction, compute `Φ((y-μ)/σ)`. Should be uniform on [0,1]. U-shape = underdispersive (overconfident).
- **CRPS comparison** — model CRPS vs (a) raw ensemble mean as Gaussian with sample variance, (b) climatology baseline (sample mean and variance of observations over training window). Model must beat both.
- **Brier score** — vs market closing line. Whether we beat the market is the trading question.
- **Closing-line value** — distribution of `(closing_mid - entry_mid)` on the side we took. Positive mean = beating the close = real edge.
- **P&L curve** — cumulative paper-trade P&L assuming 1 unit per pick.

---

## Config

`config/stations.yaml`:

```yaml
EGLL:
  name: London Heathrow
  lat: 51.4775
  lon: -0.4614
  timezone: Europe/London
  unit: celsius
  market_slug_pattern: "highest-temperature-in-london-on-{date}"
  resolution_field: "daily_max_metar_local"     # set after Phase 0
  lock_time_utc: "18:00"
```

`config/thresholds.yaml`:

```yaml
min_edge: 0.04
max_spread: 0.04
min_liquidity: 500
max_raw_prob: 0.85
market_freshness_minutes: 10
```

All thresholds must be readable from config — never hardcoded. We will tune these.

---

## Logging

Every stage emits structured JSON logs (one event per stage transition) to `logs/{date}.jsonl`. Required fields: `timestamp`, `stage`, `station`, `status`, `duration_ms`, plus stage-specific provenance. This is the audit trail when something goes wrong at 18:00 UTC.

---

## What "done" looks like for Phase 1

A demo where, on a given day:

1. `we lock --station EGLL --date $TOMORROW` runs end-to-end in under 5 minutes.
2. Output shows: model μ/σ, top 3 model brackets, market mid for those brackets, computed edges, and either a single locked pick with full provenance or "no edge — gates: [list which gates rejected what]".
3. `we backtest --station EGLL --start 2025-01-01 --end 2025-12-31` produces a year of paper-trade results with calibration plot, Brier vs market, and CLV distribution.
4. The model's CRPS beats both raw-ensemble and climatology baselines.
5. All tests pass with `pytest`. `ruff check` and `mypy --strict` clean.

If Brier vs market is also positive over a meaningful sample (say 200+ picks), Phase 2 (more cities, ECMWF weighting, BMA mixture) is justified. If not, we know exactly where to dig — calibration plot will tell us whether the issue is location, scale, or shape of the predictive distribution.

---

## Out of scope, but plan for it

- Multi-model BMA mixture (replaces pooled ensemble in Stage 3)
- ECMWF / GEFS weighting by recent CRPS
- Per-model EMOS, then mixture
- Quantile regression forests as alternative to Gaussian
- Cron / scheduler / alerting
- Live trading via Polymarket's authenticated API (Phase 5+, only after CLV is positive)

The architecture should leave clean seams for these — particularly Stage 3's `predict_pdf` should accept a list of model-specific `(EmosParams, ensemble)` pairs in the BMA version, with Phase 1 being the special case of a single pooled ensemble.
