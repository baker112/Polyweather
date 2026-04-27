# polyweather

Daily pipeline: ensemble weather forecasts → EMOS calibration → Polymarket edge detection → automated betting.

Ingests ECMWF and GEFS forecasts, fits a Non-homogeneous Gaussian Regression (EMOS) model against historical METAR observations, computes calibrated bracket probabilities, and places bets where the model disagrees with Polymarket's prices.

---

## Setup

**Requirements:** Python 3.11+, `uv` or `pip`

```bash
pip install -e ".[dev]"
```

**Environment variables** — add to `.env` or your shell profile:

```bash
# Polymarket CLOB (run `we setup-clob` to derive API creds from the private key)
export POLYMARKET_PK=0x...
export CLOB_API_KEY=...
export CLOB_SECRET=...
export CLOB_PASS_PHRASE=...

# Telegram notifications (optional — see Telegram section below)
export TELEGRAM_BOT_TOKEN=...
export TELEGRAM_CHAT_ID=...
```

---

## Daily workflow

The scheduler handles everything automatically. These are the manual equivalents:

```bash
# Ingest ECMWF + GEFS forecasts (runs automatically at 17:30 UTC)
we ingest forecasts --station EGLC

# Lock picks for tomorrow (runs automatically at 18:00 UTC)
we lock --station EGLC --date 2026-04-28

# Execute locked picks (dry-run by default; add --live for real orders)
we execute --station EGLC --date 2026-04-28

# Ingest yesterday's observations and resolve the market (runs at 02:00 UTC)
we ingest observations --station EGLC --start 2026-04-27 --end 2026-04-27
we resolve --station EGLC --date 2026-04-27
```

### Backtest

```bash
we backtest --station EGLC --start 2025-01-01 --end 2025-12-31
we report   --station EGLC --start 2025-01-01
```

---

## Scheduler (VPS)

Starts a blocking APScheduler process that runs all jobs on their UTC schedule:

```bash
we scheduler --stations EGLC
```

| Job | Time (UTC) | What it does |
|-----|-----------|--------------|
| Ingest | 17:30 | Fetch ECMWF + GEFS forecasts |
| Lock | 18:00 | Compute edges, lock picks |
| Resolve + observe | 02:00 | Fetch yesterday's METAR, resolve market |
| Refit | Sunday 03:00 | Re-fit EMOS + QRF models |

Run it as a persistent systemd service or inside `tmux`/`screen`.

---

## Telegram notifications

The pipeline sends Telegram messages when bets are placed and after each scheduled job. To set it up:

1. Open Telegram and message **@BotFather** → `/newbot` → follow prompts → copy the token.
2. Send your new bot a message (e.g. `/start`).
3. Fetch your chat ID:
   ```
   https://api.telegram.org/bot<TOKEN>/getUpdates
   ```
   Look for `"chat": {"id": <number>}` in the response.
4. Set the environment variables:
   ```bash
   export TELEGRAM_BOT_TOKEN=<token>
   export TELEGRAM_CHAT_ID=<chat_id>
   ```

Once running, send `/status` to the bot at any time to get the scheduler's next job times.

---

## Supported stations

| ICAO | City |
|------|------|
| EGLC | London City Airport |
| EGLL | London Heathrow |
| EHAM | Amsterdam Schiphol |
| EDDF | Frankfurt Airport |
| LFPB | Paris Le Bourget |
| KJFK | New York JFK |
| KLAX | Los Angeles |
| KORD | Chicago O'Hare |
| KMIA | Miami |

Add new stations to `config/stations.yaml`.

---

## Project layout

```
src/weather_edge/
├── cli.py                  # `we` CLI entry point
├── config.py               # station config loader
├── models.py               # Candidate, MarketOutcome, etc.
├── telegram.py             # Telegram notifications
├── ingest/
│   ├── ecmwf.py            # ECMWF Open Data
│   ├── gefs.py             # GEFS via AWS Open Data
│   └── metar.py            # Iowa Mesonet ASOS observations
├── postprocess/
│   ├── emos.py             # NGR (EMOS) fitting + prediction
│   ├── qrf.py              # Quantile regression forest
│   └── crps.py             # CRPS scoring
├── pipeline/
│   ├── lock.py             # lock_picks() — main daily entry point
│   ├── backtest.py         # historical replay
│   ├── resolve.py          # market resolution
│   └── scheduler.py        # APScheduler daily jobs
├── market/
│   └── polymarket.py       # Polymarket CLOB API client
├── execution/
│   ├── polymarket_exec.py  # order placement
│   └── bankroll.py         # Kelly sizing
├── store/
│   └── parquet.py          # DuckDB + parquet IO
└── eval/
    ├── calibration.py      # reliability diagrams, PIT histograms
    └── brier.py            # Brier score
config/
├── stations.yaml           # ICAO codes, lat/lon, market slug patterns
└── thresholds.yaml         # min_edge, max_spread, min_liquidity, etc.
data/
├── forecasts/              # partitioned by model / init_date / station
├── observations/           # partitioned by station / date
├── picks/                  # write-once lock files
├── executions/             # order records
└── backtest_results/
```

---

## Tests

```bash
pytest
ruff check src/
mypy src/
```
