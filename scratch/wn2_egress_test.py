"""Egress test for WN2 GCS — pulls ~5 GiB so the cost is visible in billing.

Fetches 1 init × 64 members × 20 leads × full lat/lon of 2m_temperature from
the historic 2022_to_2023 store. ~320 chunks at 16 MiB each.

Expected cost at common GCP egress rates:
  Same-region / premium tier:    $0     (no line item)
  Cross-region same continent:   ~$0.11 ($0.02/GiB)
  Cross-continent:               ~$0.27 ($0.05/GiB)
  US-multi to non-NA / non-EU:   ~$0.64 ($0.12/GiB)

Run once, wait ~5-15 min for billing to update, then check:
  Cloud Console -> Billing -> Reports -> Service: Cloud Storage -> Today
"""
import time

import xarray as xr

URI = "gs://weathernext/weathernext_2_0_0/zarr/2022_to_2023/predictions.zarr/"

print(f"Opening: {URI}")
ds = xr.open_zarr(URI, consolidated=True, chunks=None)
t2m = ds["2m_temperature"]
print(f"  full shape: {t2m.shape}")
print(f"  chunks:     {t2m.encoding.get('chunks')}")

# 1 init × 64 members × 20 leads × 721 lat × 1440 lon × 4 bytes (float32)
n_init, n_member, n_lead = 1, 64, 20
n_lat, n_lon = 721, 1440
est_bytes = n_init * n_member * n_lead * n_lat * n_lon * 4
est_gib = est_bytes / 1024**3
print()
print(f"Will fetch: {n_init} inits × {n_member} members × {n_lead} leads × {n_lat}×{n_lon}")
print(f"Estimated transfer: ~{est_gib:.2f} GiB")
print(f"Expected cost @ $0.02/GiB: ${est_gib * 0.02:.2f}")
print(f"Worst case  @ $0.12/GiB: ${est_gib * 0.12:.2f}")
print()
print("Starting fetch in 5 seconds — Ctrl-C to abort…")
time.sleep(5)

print("Fetching…")
t0 = time.time()
val = t2m.isel(
    time=0,
    sample=slice(0, n_member),
    prediction_timedelta=slice(0, n_lead),
).values
elapsed = time.time() - t0

mib = val.nbytes / 1024**2
gib = mib / 1024
print()
print(f"Got shape {val.shape}, dtype {val.dtype}")
print(f"Transferred {gib:.2f} GiB ({mib:.0f} MiB) in {elapsed:.1f}s")
print(f"Effective throughput: {mib / elapsed:.0f} MiB/s")
print()
print("Now wait ~5-15 minutes and check:")
print("  Cloud Console -> Billing -> Reports")
print("  Filters: Service = Cloud Storage, Time range = Today")
print()
print(f"Look for an egress line item near ~${gib * 0.02:.2f}")
print("  - If you see ~$0.11:    cross-region NA pricing ($0.02/GiB) applies")
print("  - If you see ~$0.27:    cross-continent pricing ($0.05/GiB)")
print("  - If you see $0:        same-region under premium tier — backfill is free")
