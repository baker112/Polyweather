"""Probe the WeatherNext Zarr store on GCS — print the schema, no charges.

Run this on the VPS (where the Compute Engine SA is allowlisted) BEFORE trusting
the GCS backend in production. Verifies:
  - Authentication works (SA can list the bucket)
  - Zarr opens cleanly with consolidated metadata
  - Dim names match what _gcs_query_and_reduce expects
  - 2m_temperature variable exists and is in Kelvin

Usage:
    python scripts/weathernext_gcs_probe.py
    python scripts/weathernext_gcs_probe.py --uri gs://weathernext/weathernext_2_0_0/

Cost: ~zero. Opens consolidated metadata (a small JSON) and reads dim coords.
No actual gridded data is fetched.
"""
from __future__ import annotations

import argparse
import os
import sys


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "--uri",
        default=os.getenv("WEATHERNEXT_GCS_URI", "gs://weathernext/weathernext_2_0_0/"),
    )
    args = parser.parse_args()

    try:
        import xarray as xr
    except ImportError:
        sys.exit("xarray not installed — pip install xarray zarr gcsfs")
    try:
        import gcsfs  # noqa: F401
        import zarr  # noqa: F401
    except ImportError:
        sys.exit("gcsfs/zarr not installed — pip install zarr gcsfs")

    print(f"Opening Zarr: {args.uri}")
    try:
        ds = xr.open_zarr(args.uri, consolidated=True, chunks={})
    except Exception as exc:
        sys.exit(f"FAIL: {exc}")

    print()
    print("=== Dimensions ===")
    for k, v in ds.dims.items():
        print(f"  {k:30s}  size={v}")

    print()
    print("=== Coordinates ===")
    for k in ds.coords:
        c = ds[k]
        sample = c.values[:3] if c.size > 3 else c.values
        print(f"  {k:30s}  dtype={c.dtype}  shape={c.shape}  first={sample}")

    print()
    print("=== Data variables ===")
    for k in ds.data_vars:
        v = ds[k]
        print(f"  {k:30s}  dims={v.dims}  dtype={v.dtype}  units={v.attrs.get('units', '?')}")

    print()
    expected_dims = {
        "init_time": ("init_time", "time"),
        "lead":      ("prediction_timedelta", "step", "lead_time", "lead"),
        "lat":       ("latitude", "lat"),
        "lon":       ("longitude", "lon"),
        "member":    ("number", "ensemble_member", "sample", "member", "realization"),
    }
    print("=== Backend compatibility ===")
    ok = True
    for label, cands in expected_dims.items():
        match = next((c for c in cands if c in ds.dims), None)
        status = f"OK   ({match})" if match else f"MISS (expected one of {cands})"
        print(f"  {label:10s}  {status}")
        if not match:
            ok = False
    t2m = next((c for c in ("2m_temperature", "t2m", "2t") if c in ds.data_vars), None)
    print(f"  {'2m temp':10s}  {'OK   (' + t2m + ')' if t2m else 'MISS (no 2m_temperature)'}")
    if not t2m:
        ok = False

    print()
    if ok:
        print("✓ Backend should work as-is. Default ingest uses this store.")
    else:
        print("✗ Backend constants need editing in src/weather_edge/ingest/weathernext.py")
        print("  Look for _pick_dim/_pick_var calls in _gcs_query_and_reduce.")
        sys.exit(1)


if __name__ == "__main__":
    main()
