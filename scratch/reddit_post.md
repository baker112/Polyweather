# Reddit post — verify GCS egress is really free

**Subreddit:** `r/googlecloud` (best fit). Secondary: `r/datasets`.

**Title** (pick one):

- *Is GCS egress from a public Google Research bucket (gs://weathernext) actually free across regions? 5 GiB pulled, £0 billed — what am I missing?*
- *Pulled 5 GiB from gs://weathernext to a Toronto VM. Bucket is in US multi-region. £0 in billing after 24h. Sanity check before I do a 20 TiB backfill?*

---

## Body

I'm doing a weather forecasting side project and need to read Google's
WeatherNext 2 dataset from GCS. Before kicking off a multi-year backfill
I wanted to verify the egress wouldn't quietly bankrupt me. Tested with a
single ~5 GiB pull and got £0 charged, which seems too good to be true.
Asking here before I do something dumb.

### The setup

- **Bucket:** `gs://weathernext/` — Google's public WeatherNext 2 dataset
  (zarr stores under `weathernext_2_0_0/zarr/`). Bucket location:
  **US multi-region**.
- **VM:** GCE `e2-medium` in **`northamerica-northeast2-b`** (Toronto).
  Same continent, different region from the bucket.
- **Auth:** default Compute Engine service account, allowlisted on the
  bucket. Reading via `xarray.open_zarr` + `gcsfs`.

### The test

Pulled a single ~5 GiB slice of `2m_temperature` from `2022_to_2023/predictions.zarr`:

```python
import xarray as xr
ds = xr.open_zarr("gs://weathernext/weathernext_2_0_0/zarr/2022_to_2023/predictions.zarr/",
                  consolidated=True, chunks=None)
val = ds["2m_temperature"].isel(time=0, sample=slice(0, 64),
                                 prediction_timedelta=slice(0, 20)).values
# Transferred 4.95 GiB in 35.9s (141 MiB/s)
```

### The result, 24 hours later

In Cloud Console → Billing → Reports:

- **Cloud Storage doesn't appear in the Services filter at all.** That filter
  only lists services with non-zero charges, so this implies **zero Cloud
  Storage charges**.
- Compute Engine line items show normal compute/RAM/disk for the VM (~£0.50
  over 14h) — nothing related to the 5 GiB pull.
- "Network Internet Data Transfer Out from Toronto" line items show 0 GiB.
- No "Multi-region Network Egress" line item anywhere.

My back-of-envelope said cross-region GCS egress should be ~$0.02/GiB
(≈ $0.10 for this test). Instead it's $0.

### My questions

1. **Is egress from `gs://weathernext` actually free** because it's a public
   Google Research dataset (and Google covers the egress)? Or am I seeing
   newer "Premium Tier" same-continent egress, which is also free for some
   traffic classes?
2. **If I do a full backfill** — roughly 20 TiB total across 5000 init times,
   pulled chunk-by-chunk to the same Toronto VM — should I still expect £0?
   Or does this fall over at some scale (e.g. a 1 PB read suddenly gets
   classified differently)?
3. **Is there a SKU I should be watching that wouldn't appear until later?**
   Some egress charges have multi-day lag, but 24h should be plenty.
4. **Is there a way to verify in advance** (a dry-run / pricing-calculator
   query) before I commit to the backfill, so I'm not pleading my case to
   GCP support if it ends up costing $400?

I've already set a $10 monthly budget alert as a safety net, but I'd rather
understand the pricing model than rely on the alert.

### What I'm trying to avoid

I had a near-miss earlier: a BigQuery query against the same dataset scanned
199 TiB at ~£215 list price (covered by free trial credits). I'm not trying
to repeat that with egress. Hence the cautious test before the real pull.

Any insight from people who've used public Google Research datasets at scale
would be very welcome. Will update the post with the actual outcome of the
backfill once it's done in case it helps the next person.
