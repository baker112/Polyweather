# Three-mode P&L comparison + Telegram overhaul

**Status:** Spec/handoff doc. Open in a fresh session; everything you need is in here.

**Target outcome:** Three independent trading modes (BMA, Intraday, Peak)
each running on all 12 stations daily with a $100 dry bankroll each, plus
a Telegram interface that's actually readable.

---

## 1. Where the project is right now

- Live on GCP VPS (`northamerica-northeast2-b`, Toronto), Ubuntu 24.04, user
  `ohbaker1`, dir `~/Polyweather`, venv `.venv`, systemd unit `polyweather`.
- BigQuery hard-disabled (see `weathernext.py` docstring). WN2 reads
  exclusively from GCS (`gs://weathernext/`), sponsor-paid, £0 cost.
- 12 active stations (`config/stations.yaml`). Each samples its own 0.25°
  WN2 grid cell via `xarray.sel(method="nearest")`.
- Three modes already implemented at the **forecast computation** level:
  - **BMA** (`bma_mode: bma`) — D-1 evening lock, 4-model BMA (ECMWF/GEFS/ICON/WN2),
    per-model EMOS where available, Phase 3 QRF if fitted, σ floor 1.5°C,
    quarter-Kelly via `kelly_multiplier=0.5 × station_kelly_multiplier=0.5`.
  - **Intraday** (`bma_mode_override="wn2_only"` via `_intraday_lock_job`) —
    fires at `intraday_lock_time_utc`, WN2-only Gaussian, σ floor, **explicit**
    quarter-Kelly override (0.25). Targets TODAY (not D+1).
  - **Peak** (`bma_mode_override="wn2_peak"` via `_peak_lock_job`) — fires at
    same `intraday_lock_time_utc`, p75 of WN2 ensemble daily-max, single YES
    bet on bracket containing μ, flat 1% bankroll sizing.
- All three are currently **clobbering each other's picks files** — they
  all write to `data/picks/date=YYYY-MM-DD/station=XXX/picks.json` and the
  last one wins. That's the core thing to fix.
- One shared dry bankroll at `data/bankroll_dry.json` accumulates all
  three modes' simulated trades indistinguishably.

**Goals of this work:**

1. **Separate picks, executions, bankrolls per mode** so P&L is attributable.
2. **Reseed all 3 bankrolls to $100** for clean head-to-head comparison.
3. **Overhaul Telegram** to be readable at a glance — fewer, denser messages.

---

## 2. Three-mode separation — concrete spec

### 2.1 Mode identifiers

Use these strings literally throughout the codebase:
- `"bma"` — D-1 evening BMA blend (currently the default `bma_mode` for all stations)
- `"intraday"` — same-day WN2-only short-lead
- `"peak"` — same-day WN2 p75 point-forecast

Note the naming distinction: `bma_mode` in `StationConfig` already has a
value `"wn2_only"` and `"wn2_peak"` — those are the **forecast aggregation
modes** that drive `_stage3_4`. The new `"intraday"` / `"peak"` /
`"bma"` identifiers are the **lock/strategy modes** for P&L attribution.
These two layers are related but distinct: the lock job's strategy name
determines which bankroll/picks file is used; the underlying forecast
aggregation might be the same (`wn2_only` is shared by intraday and peak).

Map:

| Lock strategy ID | Forecast aggregation | Lock job | Picks file | Bankroll |
|---|---|---|---|---|
| `bma` | `station.bma_mode` (usually `bma`) | `_lock_job` | `picks_bma.json` | `bankroll_dry_bma.json` |
| `intraday` | `wn2_only` (forced) | `_intraday_lock_job` | `picks_intraday.json` | `bankroll_dry_intraday.json` |
| `peak` | `wn2_peak` (forced) | `_peak_lock_job` | `picks_peak.json` | `bankroll_dry_peak.json` |

### 2.2 File layout changes

**Picks:**
```
data/picks/date=YYYY-MM-DD/station=XXX/picks_bma.json
                                       picks_intraday.json
                                       picks_peak.json
```

**Executions:**
```
data/executions/station=XXX/date=YYYY-MM-DD/
    bma/<execution-id>.json
    intraday/<execution-id>.json
    peak/<execution-id>.json
    _settled_bma.json
    _settled_intraday.json
    _settled_peak.json
```

**Bankrolls (dry):**
```
data/bankroll_dry_bma.json       # seed $100
data/bankroll_dry_intraday.json  # seed $100
data/bankroll_dry_peak.json      # seed $100
data/bankroll_dry.json           # archive existing one as .pre_split.bak before reseeding
```

Keep live bankroll (`data/bankroll.json`) **as is** — it's the real one and
not split. Live trading still uses it (only one mode can go live at a
time; configured via env var, e.g. `LIVE_MODE=bma|intraday|peak`).

### 2.3 Files to modify

| File | What changes |
|---|---|
| `src/weather_edge/store/parquet.py` | `write_picks(data, station_id, target_date, mode="bma")` and `read_picks(station_id, target_date, mode="bma")` |
| `src/weather_edge/execution/bankroll.py` | `load_dry(mode="bma")`, `save_dry(data, mode)`, `settle_dry(data, stake, pnl, mode)`, `reserve_dry(data, stake, mode)`. Loader auto-seeds to $100 if file missing |
| `src/weather_edge/execution/polymarket_exec.py` | `save_execution(station_id, date, records, mode)` writes under `mode/` subdir; `load_executions(station_id, date, mode="bma")` reads same |
| `src/weather_edge/pipeline/lock.py` | `lock_picks(...)` derives `lock_strategy` from `bma_mode_override` (or "bma" default) and passes it to `store.write_picks` |
| `src/weather_edge/pipeline/scheduler.py` | `_lock_job`, `_intraday_lock_job`, `_peak_lock_job`, `_execute_job`, `_resolve_and_observe_job` all aware of their strategy. `picks_exist()`/`read_picks()`/`load_executions()`/`save_execution()`/`load_dry()`/`settle_dry()` calls all parameterised by mode |
| `src/weather_edge/pipeline/resolve.py` | Loops over the three modes when settling; per-mode `_settled_<mode>.json` marker so each mode is idempotent independently |

### 2.4 Acceptance criteria (must all pass before commit)

1. `we init-bankroll-dry --mode bma --usdc 100` (new CLI subcommand) — also
   `--mode intraday`, `--mode peak`. Each writes the corresponding file.
2. After a full day of dry runs, `data/bankroll_dry_*.json` files exist
   and each contains exactly the trades attributed to that mode.
3. `/bankroll` Telegram command shows three bankrolls in one message.
4. `/pnl 7` shows last-7-days P&L per mode side by side.
5. `_resolve_and_observe_job` correctly settles each (station, date, mode)
   triple exactly once. Re-running it doesn't double-count.
6. Backtest (`/backtest`) still works — it can pick which mode to backtest via
   an optional `--mode` argument.
7. All existing single-mode tests still pass (if any). New tests for the
   per-mode separation are bonus.

### 2.5 Edge cases / gotchas

- **First-day seeding**: when `bankroll_dry_<mode>.json` doesn't exist yet,
  `load_dry(mode)` should auto-create it with `current_usdc=100, reserved_usdc=0,
  total_pnl=0, n_trades=0`. Same auto-init behaviour as the existing
  `load_dry()`.
- **Migration of existing data**: before reseeding, snapshot the current
  `data/bankroll_dry.json` to `data/bankroll_dry.pre_split.bak.json` and
  print a one-time Telegram message noting the reset.
- **Picks-file backwards compat**: `read_picks` with default `mode="bma"`
  should still find the **old** non-mode-suffixed `picks.json` if it exists,
  so resolve-missed for historic dates still works. New picks always go
  to the mode-suffixed path.
- **Execute job parallelism**: today `_execute_job` runs once per station
  ~5 min after each lock. With three modes, three separate
  `_execute_<mode>` cron entries fire 5 min after each respective lock.
- **Edge gate (`station_kelly_multiplier`)**: the auto-promotion logic is
  shared today (it reads from `data/executions/...`). After the split,
  it should auto-promote *per mode* (a station might be promoted on BMA
  but not on Peak yet). Add a `mode` param to `effective_kelly_multiplier`.
- **CLV snapshots, refit jobs**: shared. They're forecast/market-side, not
  strategy-side. Leave them as is.

---

## 3. Telegram overhaul

### 3.1 What's wrong now

Current Telegram is a mess of independent messages per (station, mode,
event). For a full day across 12 stations × 3 modes × ~5 event types
that's 100+ messages. Easy to miss things, hard to spot patterns. Also
the message bodies have too many numbers — model%/mkt%/edge%/kelly%/cap=$
on every pick.

### 3.2 New format — principles

- **Digest > per-station messages.** Where a job touches all 12 stations,
  send ONE consolidated message at end-of-job rather than 12 individual.
- **One line per station.** Strip detail to: station, key value, action.
- **Verbose only on `/pick STATION`** — a user can drill into one station
  via a query command rather than always seeing every number.
- **Use mode icons consistently**: 📊 BMA, ⚡ Intraday, 🎯 Peak.
- **Status icons**: 🟢 YES bet, 🔴 NO bet, ⏸ no edge, ❌ failed.
- **Numbers**: trim to 1 decimal place for temperatures, 0 decimals
  for percentages, dollar amounts to nearest cent.

### 3.3 Message redesigns (template by event type)

**Startup** (today: 4-line banner)
```
🚀 Polyweather online · 12 stations · mode=dryrun
Commands: /status /picks /bankroll /pnl /summary /lock /intraday /peak /resolve /mode /live /backtest
```

**Lock digest** (one message at end of each lock window, per mode)
```
⚡ Intraday locks · 12:00z 2026-05-16
   EGLC  17.5°C   🟢 YES 17    $2.50
   LFPB  18.0°C   ⏸  no edge
   KLGA  21.0°C   🟢 YES 21    $3.10
   …
   Bankroll: $103.45 ($5.60 reserved)
```
- Implementation: spawn one `_intraday_digest_job` that waits for all
  per-station `_intraday_lock_job`s to finish, collects results, sends.
  OR: simpler — accumulate picks in a list, send digest after the last
  station's lock finishes (track via per-day "intraday_completed_count"
  counter, send digest when it hits len(stations)). Or simplest: a daily
  scheduled digest job at the latest intraday_lock_time + 5 min that reads
  all today's picks files for that mode and assembles the message.

**Individual lock errors** (only on failure, not success):
```
❌ Intraday FAILED: KORD · 2026-05-16
   all WN2 init attempts failed (last: ...)
```

**Pick details on demand** (`/pick EGLC intraday`):
```
⚡ Intraday EGLC · 2026-05-16
   μ=17.4°C  σ=1.50  init=06z  lead≈6h
   🟢 YES 17°C
     model 38% vs mkt 24%  edge +14%
     kelly 6.25% of bankroll, cap $5
     bet placed (dry): $5.00 @ 0.24
```

**Resolution digest** (one message per day after all stations resolve):
```
📋 Resolution · 2026-05-15
   EGLC  resolved 17°C
     📊 BMA       no bet
     ⚡ Intraday  🟢 YES 17    +$2.18
     🎯 Peak      🟢 YES 18    -$1.00
   LFPB  resolved 19°C
     📊 BMA       🟢 YES 18    -$3.00
     ⚡ Intraday  🟢 YES 19    +$4.45
     🎯 Peak      no bet
   …
   Day totals:  📊 -$3.00  ⚡ +$6.63  🎯 -$1.00
```

**Bankroll** (`/bankroll`):
```
💰 Bankrolls
   📊 BMA       $103.45  ($5.60 reserved, +$3.45 PnL, 8 trades)
   ⚡ Intraday  $107.18  ($2.10 reserved, +$7.18 PnL, 4 trades)
   🎯 Peak      $98.50   ($1.00 reserved, -$1.50 PnL, 2 trades)
   💼 Live      $0.00    (not initialised)
```

**P&L last N days** (`/pnl 7`):
```
📈 P&L last 7 days
   📊 BMA       trades=15  win=8  ROI=+4.2%
   ⚡ Intraday  trades=12  win=7  ROI=+8.1%
   🎯 Peak      trades=8   win=2  ROI=-12.3%
```

**Daily summary** (`/summary`, cron at 08:30z):
```
📊 Polyweather digest · 2026-05-16 09:00z

Yesterday (2026-05-15)
  📊 BMA      6/12 stations resolved   +$2.20   ROI +1.8%
  ⚡ Intraday 8/12 stations resolved   +$6.50   ROI +5.1%
  🎯 Peak     4/12 stations resolved   -$0.80   ROI -0.8%

Bankrolls
  📊 $103.45  ⚡ $107.18  🎯 $98.50

Tomorrow (2026-05-17) picks (BMA only; intraday/peak fire same-day)
  EGLC  μ=18.3°C  🟢 YES 18
  LFPB  μ=19.0°C  ⏸  no edge
  KLGA  μ=22.0°C  🟢 YES 22
  …
```

### 3.4 Implementation notes for Telegram refactor

- All Telegram emit goes through `weather_edge.telegram._tg.send(...)`. Add
  a `_tg.send_digest(message, dedup_key=None)` variant that allows batching
  if multiple jobs try to send within a short window — but probably not
  worth it; a per-mode digest job that runs once is simpler.
- New module `weather_edge/telegram_format.py` to keep the message
  templating out of `scheduler.py`. Functions like
  `format_lock_digest(mode, picks_per_station)`,
  `format_bankroll_message(bankrolls)`, etc.
- Strip `result.no_edge_reason` from Telegram entirely — it's in the picks
  JSON for debugging.
- Hide the `init=12z lead≈6h · μ=X σ=Y` provenance from default messages,
  show only in `/pick <station> <mode>` drill-down.

### 3.5 Acceptance criteria for Telegram

1. A full normal day produces ≤ 15 Telegram messages total (3 lock
   digests, 1 resolution digest, 1 daily summary, plus any failures).
   Today the same day produces 60+ messages.
2. Each message stands alone — readable without context from prior messages.
3. `/pick EGLC intraday` shows the full verbose detail.
4. The startup banner is two lines max.

---

## 4. Implementation order

Build in this sequence so you have something working after each phase
(rollback-safe):

### Phase A — Plumbing (no behavioural change)
1. Add `mode` parameter (default `"bma"`) to `store.read_picks` /
   `store.write_picks`. Backwards-compatible — default reads old
   non-suffixed path if mode-suffixed doesn't exist.
2. Add `mode` parameter (default `"bma"`) to `bankroll.load_dry` /
   `save_dry` / `settle_dry` / `reserve_dry`. Auto-seed to $100 when
   creating a new mode's file.
3. Add `mode` parameter to `polymarket_exec.save_execution` /
   `load_executions`.

Commit. Run `we scheduler` and confirm everything still works
(default `mode="bma"` everywhere = current behaviour).

### Phase B — Mode-aware lock & execute
4. `lock_picks` derives `lock_strategy` from `bma_mode_override`:
   - `bma_mode_override == None` → strategy = "bma"
   - `bma_mode_override == "wn2_only"` → strategy = "intraday"
   - `bma_mode_override == "wn2_peak"` → strategy = "peak"
   Threads strategy into `store.write_picks(..., mode=strategy)`.
5. `_execute_job` becomes `_execute_job(station_id, mode="bma")`. Reads
   the mode-specific picks file, uses the mode-specific bankroll.
6. Scheduler registers three execute jobs per station (one per mode),
   each 5 min after its corresponding lock.

Commit. Run a /lock EGLC, /intraday EGLC, /peak EGLC in sequence. Verify
three separate picks files written, three separate executions (if any
edge), three separate bankrolls updated.

### Phase C — Mode-aware resolve
7. `_resolve_and_observe_job` loops `for mode in ("bma","intraday","peak")`
   and settles each independently. Per-mode `_settled_<mode>.json` markers.
8. `/resolve_missed` extended similarly.

Commit. Run `/resolve_missed 4`. Verify all three modes get settled for
each of the last 4 days, with each settling exactly once even on re-run.

### Phase D — Bankroll initialisation
9. New CLI subcommand: `we init-bankroll-dry --mode bma --usdc 100` (and
   `intraday`, `peak`). Or just hardcode auto-init to $100 in load_dry.
10. One-time migration script `scripts/reset_three_mode_bankrolls.py` that:
    - Snapshots `data/bankroll_dry.json` to `.pre_split.bak.json`
    - Creates fresh `data/bankroll_dry_{bma,intraday,peak}.json` at $100 each
    - Prints a Telegram message documenting the reset

Commit. Run the migration script. Confirm three $100 files.

### Phase E — Telegram overhaul
11. Add `weather_edge/telegram_format.py` with templating functions.
12. Add `_intraday_digest_job(stations)` that fires 5 min after the last
    station's intraday lock for the day; reads all today's intraday picks
    files; sends one digest message.
13. Same for `_peak_digest_job` and `_bma_digest_job`.
14. Suppress per-station `_tg.send` calls in `_lock_job` /
    `_intraday_lock_job` / `_peak_lock_job` — only digests speak. Failures
    still send their own message.
15. Add `_resolve_digest_job(stations)` that fires after the last
    resolve completes; sends consolidated resolution message.
16. Rewrite `/bankroll`, `/pnl`, `/summary`, `/picks` commands per the
    templates in §3.3.
17. New `/pick STATION MODE` drill-down command.

Commit. Run a full daily cycle (might want to wait until tomorrow's
auto-runs to validate end-to-end).

---

## 5. Operational notes for the implementer

### VPS deploy

```bash
ssh polyweather   # or however the user normally SSHs
cd ~/Polyweather
source .venv/bin/activate
git pull
sudo systemctl restart polyweather
sudo journalctl -u polyweather -n 30 --no-pager | grep -vE "blob data"
```

### Test commands

Once Phase B lands, trial:
```
/lock EGLC      # writes picks_bma.json
/intraday EGLC  # writes picks_intraday.json
/peak EGLC      # writes picks_peak.json
```

Verify on the VPS:
```bash
ls -la data/picks/date=$(date -u +%Y-%m-%d)/station=EGLC/
# Should show: picks_bma.json, picks_intraday.json, picks_peak.json
```

After Phase D, run the migration script and verify:
```bash
ls -la data/bankroll_dry*.json
# Should show: bankroll_dry_bma.json, bankroll_dry_intraday.json,
#              bankroll_dry_peak.json, bankroll_dry.pre_split.bak.json
cat data/bankroll_dry_bma.json   # should show current_usdc=100
```

### Rollback

Each commit is independent. If any phase breaks something:
```bash
git revert <commit-sha>
sudo systemctl restart polyweather
```

The phase boundaries are chosen so each is independently revertable.

### Costs

This refactor adds **zero** cost — no new external calls, no new
GCS reads, no BQ usage. Just internal data segregation. Daily WN2
ingest cost stays £0 (sponsor-paid). Daily backup cost stays ~£0.03.

---

## 6. Open questions for the implementing session

- **Live trading**: currently `LIVE_TRADING=true` makes the BMA execute
  job place real orders. With three modes, do we want `LIVE_MODE=bma|intraday|peak`
  to pick which mode goes live, or allow multiple live at once? The user
  is in dry-run for now, so this can be deferred — but worth flagging.
- **Mode selection per station**: currently every station does all 3 modes.
  Future option: per-station opt-in via yaml field like `modes: [bma, intraday]`.
  Not for this work — but design the code so it's easy to add later.
- **Refit jobs**: WN2-specific EMOS will eventually be fitted. Should the
  refit be per-mode (different EMOS for intraday-WN2 vs peak-WN2)? Probably
  not — the underlying WN2 forecast is identical; only the aggregation
  (mean vs p75) differs. One WN2 EMOS, used by both. Worth noting in the
  refit code that the calibration applies to the raw ensemble, not to a
  particular aggregation.

---

## 7. Context the implementing session will need

These memory files cover the rest of the project state:

- `memory/MEMORY.md` — index
- `memory/weathernext_gcs_layout.md` — WN2 GCS layout (zarr paths,
  schema, chunk shapes, sponsor-paid egress)
- `memory/gcp_cost_state.md` — VPS cost reality (~£21/mo, free trial gone)
- `memory/backup_bucket.md` — daily backup explains the only ongoing
  Cloud Storage line item
- `memory/feedback_bq_cost.md` — BQ near-miss; never re-enable BQ
- `memory/vps_setup.md` — VPS layout
- `scratch/SCHEDULER_RESTART.md` — VPS deploy runbook (the systemd
  approach, not the nohup one)

Recent commits (most recent first):
```
8e3397f  intraday/peak: lock 2h before peak start so 6h forecast window covers it
b1ceb53  intraday/peak: all-station timings, 75th-percentile peak, /lock acknowledgement
f11dfac  peak/intraday: cleaner Telegram + bracket-out-of-range diagnostic
c4ddda1  wn2_peak: point-forecast mode + /peak Telegram command
b4556e5  lock: symmetric max_raw_prob gate (block extreme confidence on either side)
b652e4a  intraday + resolve: WN2 init fallback and /resolve_missed backfill
c9224bd  lock: skip non-WN2 ingests in wn2_only mode
57c60c6  intraday: quarter Kelly + /intraday Telegram command + RKSI
8590732  intraday: short-lead WN2 lock targeting same-day daily max
b5a8efe  WN2-only mode: per-station bma_mode toggle
```

Read `src/weather_edge/pipeline/lock.py` (entrypoint),
`src/weather_edge/pipeline/scheduler.py` (jobs + Telegram commands),
`src/weather_edge/execution/bankroll.py`, and
`src/weather_edge/execution/polymarket_exec.py` first. Those are where
most changes happen.

Good luck.
