# WN2 resolver — wake-up runbook

Cost summary: **everything below is verified £0** to run. The 5 GiB egress
test on 2026-05-16 produced no Cloud Storage charge. Real ingests of WN2
from Toronto are free (Google covers public-dataset egress).

---

## 1. Pull the latest code on the VPS

```bash
source .venv/bin/activate
cd ~/Polyweather
git pull
```

You should see `ae9651b` (WeatherNext resolver) plus the earlier probe
commits.

---

## 2. Sanity-check the URI resolver (NO network reads)

```bash
python scratch/wn2_resolver_check.py
```

Eyeball the output. Expect to see 6 sections:

1. **single historic init** (2023-06-15 12:00) → 1 `historic` URI:
   `gs://weathernext/weathernext_2_0_0/zarr/2023_to_2024/predictions.zarr/`
2. **3-year historic backfill** (2022→2024) → 3 `historic` URIs, one per year
3. **single per-init (2025)** → 1 `per_init` URI like:
   `gs://weathernext/weathernext_2_0_0/zarr/2025_to_present/20250101_00hr_01_preds/predictions.zarr/`
4. **first week 2025, all hours** → 28 `per_init` URIs (7 days × 4 inits)
5. **spans 2024-2025 boundary** with hours=(0,12) → 1 historic + 4 per-init
6. **2023-2025 mixed** with hours=(0,12) → 2 historic + many per-init

**If any URI looks wrong** (wrong filename pattern, missing year, etc.),
paste the output and stop — don't run step 3.

---

## 3. Real one-init ingest test (~4 GiB pull, £0)

This validates the full GCS path end-to-end. Two inits to try — pick either
or both:

### Historic path (uses `2023_to_2024/predictions.zarr`)

```bash
python -c "
from datetime import datetime, timezone
from weather_edge.ingest import weathernext
from weather_edge.config import load_stations
s = load_stations()['EGLC']
df = weathernext.ingest_forecasts(datetime(2023,6,15,12,tzinfo=timezone.utc), s)
print(df)
print(f'Rows: {len(df)}, members: {df[\"member_id\"].n_unique()}')
"
```

### Per-init path (uses `2025_to_present/20250114_12hr_01_preds/predictions.zarr`)

```bash
python -c "
from datetime import datetime, timezone
from weather_edge.ingest import weathernext
from weather_edge.config import load_stations
s = load_stations()['EGLC']
df = weathernext.ingest_forecasts(datetime(2025,1,14,12,tzinfo=timezone.utc), s)
print(df)
print(f'Rows: {len(df)}, members: {df[\"member_id\"].n_unique()}')
"
```

**Expected**: each prints a polars DataFrame with columns
`model, member_id, init_datetime, valid_date, station, daily_max_c, lead_hours`.
Should have ~64 members × ~4 forecast days = ~256 rows. Run time ~30-60s.

**If `load_stations` isn't the right import name**, check
`src/weather_edge/config.py` for the actual loader signature. Alternative:

```python
from weather_edge.config import StationConfig
s = StationConfig(icao='EGLC', lat=51.5053, lon=0.0553, timezone='Europe/London')
```

---

## 4. What to paste back

When you message me, include:

1. Output of step 2 (the URIs)
2. Output of step 3 (the DataFrame, or the traceback if it failed)
3. A note on how long each ingest took (rough seconds is fine)

I'll then either fix any issues, or — if all green — wire the per-model
EMOS refit / backfill orchestration. The backfill itself is also free, so
no surprises there.

---

## If something goes sideways

| Symptom | Likely cause | Fix |
|---|---|---|
| `IngestError: no zarr stores resolved` | Window outside 2022-present | Pick an init in range |
| `IngestError: no openable zarr stores` | URI typo or 2025 date with no data yet | Try a date you saw in the probe output (Jan-Feb 2025 is safe) |
| `KeyError: 'init_time'` after concat | Dim normalization didn't fire | Paste traceback — likely an xarray version diff |
| Hangs >5 min on a single init | Network issue or dask deadlock | Ctrl-C, check `gcloud compute instances describe …` for status |
| Cost question | Check Cloud Console → Billing → Cloud Storage filter | Should still be £0 |

---

## Optional: stop the VPS before sleeping to save ~£0.40/night

```bash
# From the VPS (will disconnect your SSH):
sudo shutdown -h now

# Or from your laptop:
gcloud compute instances stop polyweather --zone=northamerica-northeast2-b
```

Start again in the morning with:
```powershell
gcloud compute instances start polyweather --zone=northamerica-northeast2-b
```

(Wait ~30s after start before SSHing in.)
