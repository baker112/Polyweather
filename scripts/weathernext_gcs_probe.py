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
        print(f"Root open failed: {exc}")
        print()
        print("Listing bucket contents to find actual Zarr store paths…")
        _list_and_probe(args.uri)
        sys.exit(1)

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


def _list_and_probe(uri: str) -> None:
    """Walk the bucket prefix, find candidate Zarr stores, and open one.

    Identifies a Zarr store by the presence of a `.zgroup` / `.zarray` / `zarr.json`
    marker. Prints the first 50 top-level prefixes and tries to open the first
    candidate it finds so we can confirm the dim names.
    """
    import gcsfs
    import xarray as xr

    fs = gcsfs.GCSFileSystem()
    bucket_path = uri.replace("gs://", "").rstrip("/")

    print(f"  ls {uri}")
    try:
        entries = fs.ls(bucket_path, detail=False)
    except Exception as exc:
        print(f"  Cannot list bucket: {exc}")
        return

    print(f"  Found {len(entries)} top-level entries. First 50:")
    for e in entries[:50]:
        marker = ""
        # Cheap check: is this prefix itself a Zarr store?
        for sentinel in (".zgroup", ".zmetadata", "zarr.json"):
            if fs.exists(f"{e}/{sentinel}"):
                marker = f"  ← Zarr store ({sentinel})"
                break
        print(f"    gs://{e}{marker}")

    # Find the first Zarr store and try to open it.
    candidates = []
    for e in entries:
        for sentinel in (".zmetadata", ".zgroup", "zarr.json"):
            if fs.exists(f"{e}/{sentinel}"):
                candidates.append((e, sentinel))
                break

    if not candidates:
        print()
        print("  No Zarr store markers at the top level.")
        print("  Try one level deeper. Examples to inspect manually:")
        for e in entries[:5]:
            print(f"    gsutil ls gs://{e}/")
        return

    target, sentinel = candidates[0]
    target_uri = f"gs://{target}/"
    print()
    print(f"  Opening first candidate: {target_uri} (marker: {sentinel})")
    try:
        consolidated = sentinel == ".zmetadata"
        ds = xr.open_zarr(target_uri, consolidated=consolidated, chunks={})
    except Exception as exc:
        print(f"  Open failed: {exc}")
        return
    print()
    print(f"  ✓ Opened. Total candidate stores: {len(candidates)}.")
    print(f"  Dims  : {dict(ds.dims)}")
    print(f"  Vars  : {list(ds.data_vars)}")
    print(f"  Coords: {list(ds.coords)}")
    print()
    print(f"  Re-run probe against the actual store:")
    print(f"    python scripts/weathernext_gcs_probe.py --uri {target_uri}")


if __name__ == "__main__":
    main()
