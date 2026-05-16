# Scheduler restart — VPS runbook

After landing the `bma_mode: wn2_only` change, you need to restart the
scheduler on the VPS for it to pick up the new code and the (optionally
edited) `stations.yaml`.

Everything below is verified £0 to run — no GCS backfill, no BQ scans.

---

## 1. SSH in and update

```bash
source .venv/bin/activate
cd ~/Polyweather
git pull
```

You should see the new commit(s) touching `config.py`, `lock.py`, and
`stations.yaml`.

### 1a. Refresh dependencies (one-time after the BQ nuke)

google-cloud-bigquery was removed from pyproject.toml as part of the BQ
hard-disable. Refresh the venv so the package is gone (defence in depth —
even unreachable code can't import it now):

```bash
pip install -e . --upgrade
pip uninstall -y google-cloud-bigquery   # in case pip leaves the unused dep behind
pip show google-cloud-bigquery 2>&1 | grep -E "Name:" || echo "✓ google-cloud-bigquery uninstalled"
```

The final line should print the green-check confirmation.

---

## 2. Optional: flip one station to WN2-only

Pick a station with enough WN2 daily ingests to make a meaningful test
(EGLC is your most-used). Edit `config/stations.yaml`:

```yaml
EGLC:
  name: London City Airport
  lat: 51.5053
  lon: 0.0553
  timezone: Europe/London
  unit: celsius
  market_slug_pattern: "highest-temperature-in-london-on-{month_lower}-{day}-{year}"
  resolution_field: "daily_max_metar_local_wholedeg"
  lock_time_utc: "19:30"
  bma_mode: wn2_only   # ← add this line
```

Save. The scheduler reads this once at startup, so you'll need to restart
(see step 4). Leave everything else on `bma` (the default) so you have a
direct apples-to-apples comparison.

---

## 3. Find and stop the existing scheduler (if any)

```bash
ps aux | grep "we scheduler" | grep -v grep
```

If it's running, kill it:

```bash
# Replace <PID> with the process ID from the ps output
kill <PID>
```

Or, more aggressively if you've lost the PID:

```bash
pkill -f "we scheduler"
```

Wait ~2s, run `ps` again to confirm it's gone.

---

## 4. Start the scheduler in the background

```bash
cd ~/Polyweather
source .venv/bin/activate
nohup we scheduler > logs/scheduler.log 2>&1 &
disown
```

(Make sure `logs/` exists: `mkdir -p logs`.)

Verify it's up:

```bash
ps aux | grep "we scheduler" | grep -v grep
tail -f logs/scheduler.log
```

Expect a startup banner like:
```
🚀 Scheduler started · 12 stations
Mode: dryrun (or live/off depending on your .env)
Stations: RKSI, ZSPD, RCSS, ..., EGLC, ..., KBKF
```

You'll also get a Telegram message confirming startup.

---

## 5. Sanity-check via Telegram

In the bot, type:

```
/status         → confirm all jobs scheduled
/picks          → tomorrow's picks (will be empty until first lock fires)
/mode           → confirm trading mode (off/dryrun/live)
```

Once tonight's lock fires (19:30z for EGLC), check the lock message — the
provenance field should show `mode: "wn2_only_raw"` (or `wn2_only_emos` if
per-model EMOS has been fit). That confirms the new path is active.

---

## 6. Watch the first picks

After 19:30z, the bot will post the EGLC lock. Compare it side-by-side
with the other European-group station LFPB (which is still on `bma`):
- Are EGLC's σ values plausibly tighter or wider than LFPB's?
- Does EGLC's μ make sense for the forecast horizon?
- Are the picks (if any) on plausible brackets?

If anything looks off, flip EGLC back to `bma_mode: bma`, restart, and
ping me with the lock-message screenshot.

---

## If something goes sideways

| Symptom | Likely cause | Fix |
|---|---|---|
| `IngestError: wn2_only mode but only 0 WeatherNext members` | WN2 ingest failed for this init | Check `tail logs/scheduler.log` for the WN ingest error; fall back to `bma_mode: bma` |
| Scheduler won't start: `ValidationError: bma_mode` | Typo in stations.yaml (e.g. `wn2only` not `wn2_only`) | Fix YAML, restart |
| Lock fires but mode shows `pooled` not `wn2_only_*` | Scheduler is running old code | `git pull` and restart |
| No Telegram messages on startup | Bot token not set or scheduler crashed | Check `logs/scheduler.log` tail for tracebacks |

---

## Optional: per-station logs

You can verify which mode each station is using by inspecting the picks
file directly:

```bash
cat data/picks/date=$(date -u -d tomorrow +%Y-%m-%d)/station=EGLC/picks.json | jq .provenance.mode
```

Should print `"wn2_only_raw"` or `"wn2_only_emos"` for EGLC (if flipped),
and `"bma"` / `"pooled"` for the others.
