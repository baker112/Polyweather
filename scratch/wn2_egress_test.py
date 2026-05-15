"""One-chunk egress test for WN2 GCS.

Fetches a single full chunk of 2m_temperature from the historic 2022_to_2023
store. Prints elapsed time and bytes transferred so we can correlate with the
Cloud Billing report afterwards.

Cost: ~$0.08 cross-region or $0 same-region (newer pricing).
Run once, then check Console -> Billing -> Reports filtered to today.
"""
import time

import xarray as xr

URI = "gs://weathernext/weathernext_2_0_0/zarr/2022_to_2023/predictions.zarr/"

print(f"Opening: {URI}")
ds = xr.open_zarr(URI, consolidated=True, chunks=None)
print(f"  2m_temperature shape: {ds['2m_temperature'].shape}")
print(f"  2m_temperature chunks: {ds['2m_temperature'].encoding.get('chunks')}")

print()
print("Fetching one chunk's worth (time=0, sample=0..3, lead=0, all lat/lon)…")
t0 = time.time()
val = ds["2m_temperature"].isel(
    time=0,
    sample=slice(0, 4),
    prediction_timedelta=0,
).values
elapsed = time.time() - t0

mib = val.nbytes / 1024**2
print()
print(f"Got shape {val.shape}, dtype {val.dtype}")
print(f"Transferred {mib:.2f} MiB in {elapsed:.1f}s")
print()
print("Now check Cloud Console -> Billing -> Reports -> filter to today")
print("Look for 'Multi-region Network Egress' or 'GCS Internet Egress' line items.")
print(f"  - If you see ~${mib / 1024 * 0.02:.4f} charged: cross-region ($0.02/GiB)")
print("  - If you see $0:   you're effectively same-region, all reads free")
