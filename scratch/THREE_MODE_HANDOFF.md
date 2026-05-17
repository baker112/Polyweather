# Three-mode refactor — handoff after implementation

**Status:** All six phases shipped to `main` and pushed. The bot is set up
to run autonomously across three competing strategies for a multi-week
head-to-head P&L comparison. The previous handoff doc
(`scratch/THREE_MODE_REFACTOR.md`) is the spec; this doc is what actually
landed, what's left, and the conversation context the next session will
need.

---

## 1. What shipped

Six commits implement the spec, one per phase, plus a follow-up fix:

```
995c556  three-mode F: per-mode dump + fix /topstations missing intraday/peak P&L
cac53a3  three-mode E: Telegram overhaul — digests, /pick drill-down, two-line banner
6030705  three-mode D: per-mode bankroll init CLI + migration script
99858eb  three-mode C: per-mode resolve and idempotent settlement
4bb32e9  three-mode B: route lock_strategy through storage; peak quarter-Kelly
a8b5fd6  three-mode A: thread mode= through picks/bankroll/executions storage
```

### On-disk layout (post-migration)

```
data/
  picks/date=YYYY-MM-DD/station=XXX/
    picks.json              # bma  (legacy path preserved; mode="bma" still hits here)
    picks_intraday.json
    picks_peak.json
  executions/station=XXX/date=YYYY-MM-DD/
    20260517T123000.json    # bma  (flat dir; mode="bma" stays here)
    intraday/<ts>.json
    peak/<ts>.json
    _settled_bma.json       # per-mode settlement marker
    _settled_intraday.json
    _settled_peak.json
  dry_bankroll.json                # bma  (legacy path)
  bankroll_dry_intraday.json
  bankroll_dry_peak.json
  dry_bankroll.pre_split.bak.json  # archive from migration
  dry_bankroll.pre_split.completed # idempotency marker
  bankroll.json                    # live (singleton across modes — unchanged)
```

The deliberate asymmetry: `mode="bma"` keeps the legacy file paths because
that path was already in use, so no migration of historic BMA data is
required. The two new strategies get mode-suffixed paths from day one.

### Strategy ⇌ on-disk mapping

| Lock strategy | `bma_mode_override` | Picks file | Exec dir | Dry bankroll |
|---|---|---|---|---|
| `bma` | `None` | `picks.json` | flat | `dry_bankroll.json` |
| `intraday` | `wn2_only` | `picks_intraday.json` | `<date>/intraday/` | `bankroll_dry_intraday.json` |
| `peak` | `wn2_peak` | `picks_peak.json` | `<date>/peak/` | `bankroll_dry_peak.json` |

The mapping is computed by `lock._lock_strategy_for(bma_mode_override)` and
threaded through `store.write_picks(..., mode=...)`, `br.load_dry(mode=...)`,
`pe.save_execution(..., mode=...)`, etc. Every storage call defaults to
`mode="bma"` so passing nothing preserves the legacy behaviour.

### Sizing

- BMA: half-Kelly × half station-Kelly = ~quarter effective (unchanged)
- Intraday: explicit 0.25 override × half station-Kelly = ~eighth effective
- Peak: same 0.25 override (was flat 1%; user explicitly asked for quarter Kelly).
  Because peak's `model_prob=1.0` by construction, raw Kelly = 1.0 → cap chain
  is what bounds it: `min(1.0, max_kelly_fraction=0.25) × 0.25 × station_kelly = 0.0312`
  (3.1% of bankroll per bet at default settings; was 1%).

### Telegram (`src/weather_edge/telegram_format.py`)

All per-station success messages are silenced. Five scheduled digests
speak per day:

- `bma_lock_digest`        @ `max(lock_time_utc) + 10m`
- `intraday_lock_digest`   @ `max(intraday_lock_time_utc) + 10m`
- `peak_lock_digest`       @ `max(intraday_lock_time_utc) + 10m`
- `resolve_digest`         @ `max(resolve_time) + 20m`
- `daily_summary`          @ `08:30z`

Plus failure messages (lock failed, resolution failed) and acknowledgement
messages for manual commands (`/lock`, `/intraday`, etc.). A typical
12-station day went from ~60+ messages to ~5 successes + failures.

New command: `/pick STATION [bma|intraday|peak]` for the verbose
single-mode drill-down. Existing `/bankroll`, `/pnl N`, `/summary`,
`/picks` were rewritten to use `telegram_format`.

### Resolve

`_resolve_and_observe_job` now loops `for mode in br.DRY_MODES` per
station. Each mode has its own `_settled_<mode>.json` marker, so re-runs
(manual `/resolve`, restart catch-up, etc.) don't double-settle a mode
that already settled, but a brand-new mode added later will still get
its first settlement.

### Dump

`build_state_dump` now emits per-mode bundles:

- `bankroll.dry`: `{bma, intraday, peak, pre_split_archive?}` (None for
  missing files — does **not** auto-init)
- `history[sid]`: `picks`, `executions`, `settlements` all keyed by mode;
  resolutions and market snapshots stay at the station root (shared)
- `performance`: legacy schema, but now correctly sums across all three
  modes (fixed the `/topstations` regression introduced by the storage
  split)
- `performance_by_mode`: per-station per-mode buckets + `totals_by_mode`

`/topstations` and `/losers` were silently underreporting between
commits `a8b5fd6` and `995c556` — they read default-mode-only execs. Fix
landed in `995c556`. Worth re-running them once you have post-fix data.

---

## 2. Deploying on the VPS

Already pushed to `origin/main`. The user has SSH access; the runbook is:

```bash
ssh polyweather   # or however ohbaker1@polyweather is reached
cd ~/Polyweather
source .venv/bin/activate
git pull
python scripts/reset_three_mode_bankrolls.py   # one-time; idempotent
sudo systemctl restart polyweather
sudo journalctl -u polyweather -n 50 --no-pager | grep -vE "blob data"
```

Migration script behaviour:

1. Archives `data/dry_bankroll.json` → `data/dry_bankroll.pre_split.bak.json`
2. Seeds `bma`/`intraday`/`peak` dry bankrolls at $100 each
3. Posts a Telegram heads-up
4. Drops `data/dry_bankroll.pre_split.completed` so re-runs are a no-op

The systemd restart picks up the new code; the catchup logic still
covers the **BMA** evening lock only (see §3 gaps). Intraday/peak missed
locks aren't retried by `_catchup`.

---

## 3. Known gaps (deferred, not regressions)

1. **`_catchup` is BMA-only.** If the VPS reboots between
   `intraday_lock_time_utc` and end-of-day, missed intraday/peak locks
   are skipped. Acceptable for short reboots; would bite during a
   multi-hour outage. Fix is straightforward — mirror the BMA branch
   for each mode that has an intraday lock time.

2. **Live bankroll is still singleton.** The spec deferred the
   `LIVE_MODE=bma|intraday|peak` env var because the user is in dry-run.
   When you flip a mode live, you'll need:
   - A new `_is_live_for(station_id, mode)` overload that consults
     `LIVE_MODE` so only the chosen mode submits real orders
   - `_execute_job(station_id, mode=...)` already loads the right
     bankroll via `br.load_dry(mode=mode)` when dry, and the singleton
     `br.load()` when live. That stays correct under `LIVE_MODE`.

3. **`app.py` (Streamlit dashboard) scans flat exec dir.** Won't show
   intraday/peak P&L. Not fixed (user said "fuck the dashboard").

4. **Backtest is BMA-only.** `pipeline/backtest.py` and the `/backtest`
   command don't take a mode. The backtest replays D-1 BMA logic; doing
   a fair intraday/peak backtest requires historic WN2 inits at
   short-lead times that we may or may not have cached. Defer until
   live data accumulates.

5. **Refit jobs are mode-agnostic.** WN2 EMOS is fitted once and shared
   across `wn2_only` and `wn2_peak` — this is intentional (same
   ensemble, only the aggregation differs). Worth a comment in the
   refit code so a future agent doesn't try to "fix" it.

---

## 4. Strategic context from the implementation session

The user asked three forward-looking questions worth recording:

### "Can I leave this running for a couple weeks then switch live if promising?"

Yes. All four cron families (lock/execute per mode + digests + resolve)
fire autonomously for every station with `intraday_lock_time_utc` set
(all 12 currently). Resolve is idempotent. The only autonomy gap is
`_catchup` not handling intraday/peak (§3 #1).

To go live with one mode, the user needs the `LIVE_MODE` env var wiring
described in §3 #2. ~10 lines of code in `_is_live_for` plus an
env-var-aware `/mode` Telegram command extension.

### "Can the three modes be rolled into a mega model later?"

Three reasonable paths, ranked by data efficiency:

1. **Strategy BMA (recommended).** Treat each mode as a model and feed
   its predictive distribution into the existing
   `postprocess/bma.predict_pdf_bma` mixture. Quarter-Kelly on the
   blended distribution. Needs ~6-8 weeks of resolved bets per mode for
   stable CRPS weights. Slots into existing infrastructure cleanly.

2. **Vote-gated single bet.** Bet only when ≥2 modes agree on
   direction; size by the highest-confidence mode. Crude variance
   reduction, works with ~2 weeks of data.

3. **Meta-learner / stacking.** xgboost on `(mode picks + gates +
   market state) → win prob`. Needs ~500+ resolved bets per mode.
   Powerful but overfitting risk; defer until ~2 months of data.

(1) aligns with the timeline you'd want to evaluate going live anyway.

### "I worry about locking 2h early and missing the run-up"

The intuition is backwards. Locking YES at 30¢ and watching the market
drift to 70¢ before resolution is the alpha we're trying to capture —
we already own the shares at 30¢, and the move-up just unrealised-P&Ls
us. The eventual 100¢ payout is the same regardless of intermediate
prices.

What we *do* lose by locking early is model confidence — at T-2h
we're using a WN2 init that's 5-13h old, while observations of the
morning's actual weather aren't being incorporated. Three reasonable
optimizations if it becomes a real problem after a few weeks of data:

- A second confirmation lock at T-30min that doubles down (or skips
  entry) based on whether edge persists
- Take-profit exit at, say, 80¢ pre-resolution to capture most of the
  run-up without holding through final weather variance
- A `intraday_late` mode at T-30min that competes head-to-head with
  current intraday timing

None urgent. Decide after seeing the data.

---

## 5. What to do next session

In priority order:

1. **Validate on the VPS.** If the user has already run the migration
   + restart, check that:
   - `/bankroll` shows three at $100 (plus live)
   - Next intraday/peak lock digest lands ~10 min after the last
     station's lock time
   - `/pick EGLC intraday` shows verbose detail
   - `/pnl 7` shows three modes
   - Failures (if any) Telegram themselves

2. **Catch-up for intraday/peak (§3 #1)** if you're going to be
   away from the VPS for stretches. Mirror the BMA branch in
   `_catchup` for each `cfg.intraday_lock_time_utc`-having station;
   target_date is today, not tomorrow.

3. **Strategy comparison reporting.** Once a week of data accumulates,
   add a `/compare` Telegram command (or just enrich `/pnl`) that
   shows per-mode CLV, ROI, win-rate side by side. The `dump`'s
   `performance_by_mode.totals_by_mode` block is already structured
   for this.

4. **Open Spec §6 questions** (still deferred):
   - `LIVE_MODE` env var implementation
   - Per-station mode opt-in via `stations.yaml` (e.g., `modes: [bma, intraday]`)
   - Refit-per-mode (probably no — see §3 #5)

---

## 6. File map for orientation

| Concern | File |
|---|---|
| Storage primitives (picks/execs/bankroll) | `src/weather_edge/store/parquet.py`, `src/weather_edge/execution/{bankroll,polymarket_exec}.py` |
| Lock pipeline + strategy mapping | `src/weather_edge/pipeline/lock.py` (`lock_picks`, `_lock_strategy_for`, `_compute_peak_bet`) |
| Scheduler + per-mode crons + digests | `src/weather_edge/pipeline/scheduler.py` |
| Resolution + per-mode settle markers | `src/weather_edge/pipeline/scheduler.py` (`_resolve_and_observe_job`) + `src/weather_edge/pipeline/resolve.py` |
| Telegram templates | `src/weather_edge/telegram_format.py` |
| Reporting helpers (used by /pnl /topstations /dump) | `src/weather_edge/pipeline/reporting.py` |
| Dump | `src/weather_edge/pipeline/dump.py` |
| Migration | `scripts/reset_three_mode_bankrolls.py` |
| Spec (frozen) | `scratch/THREE_MODE_REFACTOR.md` |
| This handoff | `scratch/THREE_MODE_HANDOFF.md` |

`memory/three_mode_split.md` and `memory/telegram_overhaul.md` summarise
the new state for future Claude sessions; check them first before
reading code.

Good luck.
