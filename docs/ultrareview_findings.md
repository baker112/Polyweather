# Ultrareview Findings — 2026-04-30

Reviewed: `main` vs empty `review-base` branch (full codebase, 189 files, 2260 insertions)

> Status (2026-05-02): bug_001, bug_007, bug_009, bug_011 fixed in code.
> bug_003 already addressed (`.gitignore` covers `__pycache__/` and `*.py[cod]`; no bytecode tracked).
> bug_010 is data-only — old picks files lacking `kelly_fraction` will roll off; the writer already emits it.

---

## Normal (fix these)

### bug_001 — Bare `NaN` literals in JSON output

**Files:** `data/picks/date=*/station=EGLC/picks.json` (~30 files), `data/emos_params/**/*.json` (2 files), `logs/2026-04-26.jsonl`, `logs/2026-04-27.jsonl`

Python's `json.dump` defaults to `allow_nan=True`, silently emitting the token `NaN` which is invalid per RFC 8259. Every non-Python consumer (JavaScript `JSON.parse`, Go `encoding/json`, Rust `serde_json`, jq, DuckDB `read_json`, BigQuery/Snowflake ingest, `pandas.read_json(engine='pyarrow')`) rejects these files.

**Fix:** Scrub floats before writing and pass `allow_nan=False`:

```python
import math, json

def _scrub(obj):
    if isinstance(obj, float) and not math.isfinite(obj):
        return None
    if isinstance(obj, dict):
        return {k: _scrub(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_scrub(x) for x in obj]
    return obj

json.dump(_scrub(obj), fh, allow_nan=False)
```

---

### bug_007 — Look-ahead bias: 2026-04-25 picks use EMOS fit from 2.6 days in the future

**File:** `data/picks/date=2026-04-25/station=EGLC/picks.json` lines 14–18

The picks file claims `locked_at: 2026-04-24 18:00:00+00:00` but its provenance carries `emos_n_samples: 28` and `emos_train_crps: 0.7041159919778978` — values that exactly match `data/emos_params/station=EGLC/lead_hours=24/pooled/20260427T115029.json`, an EMOS fit produced ~2.6 days **after** the claimed lock time.

The param selector is using "most recent file on disk" rather than `max(valid_from) where valid_from <= locked_at`. Any backtest or PnL aggregation built on these picks will overstate predictive power.

**Fix:** In the pick-locker and backtest harness, select params by:
```python
max(p for p in params if p.valid_from <= locked_at)
```
Add an explicit "no fit available" code path so backfills before the first real fit (`2026-04-26T22:20:58`) emit a structured warning rather than silently grabbing a future-dated fit. Consider adding an assertion that fails the pipeline if any selected param file has `valid_from > locked_at`.

---

### bug_003 — Committed `.pyc` bytecode with no `.py` source files tracked

**Files:** `src/weather_edge/**/__pycache__/` (35 files), `tests/__pycache__/`

35 `.pyc` bytecode files are committed but zero `.py` source files are tracked. The bytecode is locked to `cpython-311` (and `pytest-9.0.3` for test caches), making the project unbuildable from source on any other Python version.

**Fix:**
```bash
git rm -r --cached '**/__pycache__'
# Add to .gitignore:
__pycache__/
*.pyc
*.pyo
```
Then add the actual `.py` source tree and commit.

---

## Nit (low priority)

### bug_009 — Inconsistent timestamp formats across writers

**Files:** `data/emos_params/**/*.json` (space separator), `data/qrf_params/**/*.json` (T separator), `data/picks/**/*.json` (`locked_at` uses space, `provenance.init_dt` uses T)

EMOS writer uses `str(datetime)` which emits a space separator (`2026-04-27 00:05:37`), not strict ISO 8601. QRF writer correctly uses `datetime.isoformat()` which emits `T`. String comparisons across these two formats can flip lexicographic ordering for same-second timestamps (`space (0x20) < T (0x54)`).

**Fix:** One line in the EMOS writer and picks `locked_at` field — replace `str(dt)` / `json.dumps(default=str)` with `dt.isoformat()`.

---

### bug_010 — Schema drift: `kelly_fraction` missing from 2026-04-28 picks

**File:** `data/picks/date=2026-04-28/station=EGLC/picks.json` lines 17–18

`data/picks/date=2026-04-29/picks.json` (locked `2026-04-27T11:01`) includes `kelly_fraction: 0.5503` on its pick, but `data/picks/date=2026-04-28/picks.json` (locked `2026-04-27T00:29`, ~10.5h earlier) lacks the field entirely. Both picks pass all five gates. Writer changed mid-day without backfilling the earlier record.

**Fix:** Either backfill `2026-04-28` with the recomputed `kelly_fraction` (inputs are still in the file), or formally make the field optional and handle its absence explicitly in the consumer.

---

## Pre-existing

### bug_011 — `ingest_forecasts` `duration_ms` off by ~25x

**File:** `logs/2026-04-26.jsonl` lines 36, 39, 42

Three consecutive `ingest_forecasts` events for EGLC report 25–26 minute durations (`duration_ms` ~1,500,000–1,600,000), yet the same operation completed in 860ms on the same station just 14 minutes earlier (line 32). The implied start times precede the prior pipeline stage completion by 7–13 minutes — physically impossible without concurrent workers.

The pattern continues in `logs/2026-04-27.jsonl`. Likely cause: the timer is anchored to a module-level reference that isn't reset per-call, so `duration_ms` reports cumulative rather than per-call elapsed time.

**Fix:** Capture `t0 = time.monotonic()` at the start of each stage and emit `(time.monotonic() - t0) * 1000` at the end. Source is in `src/weather_edge/logging.py`.
