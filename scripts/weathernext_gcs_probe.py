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
from typing import Any


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "--uri",
        default=os.getenv("WEATHERNEXT_GCS_URI", "gs://weathernext/weathernext_2_0_0/zarr/"),
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
        # chunks=None avoids the dask requirement — fine for metadata inspection.
        ds = xr.open_zarr(args.uri, consolidated=True, chunks=None)
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


_SENTINELS = (".zmetadata", ".zgroup", "zarr.json")
# Subdir names we never want to recurse into — they aren't Zarr stores and can
# hold huge trees (e.g. Earth Engine asset listings) that hang `ls`.
_SKIP_NAMES = ("ee_assets",)
# Hard cap on how many entries to enumerate under any one prefix. Keeps the
# probe responsive on directories that contain thousands of per-init zarrs.
_MAX_ENTRIES_PER_PREFIX = 200


def _basename(path: str) -> str:
    return path.rstrip("/").rsplit("/", 1)[-1]


def _find_zarr_marker(fs: Any, prefix: str) -> str | None:
    for s in _SENTINELS:
        if fs.exists(f"{prefix}/{s}"):
            return s
    return None


def _list_and_probe(uri: str) -> None:
    """Walk the bucket prefix to depth 2, find Zarr stores, and open one.

    Identifies a Zarr store by the presence of a `.zgroup` / `.zmetadata` /
    `zarr.json` marker. Prints what it finds and tries to open the first
    candidate it sees so we can confirm dim/var names.
    """
    import gcsfs

    fs = gcsfs.GCSFileSystem()
    bucket_path = uri.replace("gs://", "").rstrip("/")

    print(f"  ls {uri}")
    try:
        entries = fs.ls(bucket_path, detail=False)
    except Exception as exc:
        print(f"  Cannot list bucket: {exc}")
        return

    candidates: list[tuple[str, str]] = []  # (prefix, sentinel)
    skipped_top: list[str] = []
    for top in entries:
        if top.rstrip("/") == bucket_path:
            continue  # the bucket path itself shows up; skip
        name = _basename(top)
        if name in _SKIP_NAMES:
            skipped_top.append(top)
            print(f"    gs://{top}  (skipping — known non-Zarr prefix)")
            continue
        marker = _find_zarr_marker(fs, top)
        suffix = f"  ← Zarr store ({marker})" if marker else ""
        print(f"    gs://{top}{suffix}")
        if marker:
            candidates.append((top, marker))

    # Drill one level deeper into any non-Zarr top-level prefixes (excluding skips).
    deeper = [
        t for t in entries
        if t.rstrip("/") != bucket_path
        and _basename(t) not in _SKIP_NAMES
        and not _find_zarr_marker(fs, t)
    ]
    # Subdirs whose children had no zarr sentinel — we'll still try opening one
    # at the end (handles unconsolidated zarr stores like 2025_to_present/).
    markerless_first_children: list[str] = []
    for sub in deeper:
        print(f"  ls gs://{sub}/")
        try:
            sub_entries = fs.ls(sub, detail=False)
        except Exception as exc:
            print(f"    (skip — {exc})")
            continue
        if len(sub_entries) > _MAX_ENTRIES_PER_PREFIX:
            print(f"    (large prefix — {len(sub_entries)} entries; showing first {_MAX_ENTRIES_PER_PREFIX})")
        found_marker_in_sub = False
        for child in sub_entries[:_MAX_ENTRIES_PER_PREFIX]:
            if child.rstrip("/") == sub.rstrip("/"):
                continue
            marker = _find_zarr_marker(fs, child)
            suffix = f"  ← Zarr store ({marker})" if marker else ""
            print(f"    gs://{child}{suffix}")
            if marker:
                candidates.append((child, marker))
                found_marker_in_sub = True
        if len(sub_entries) > _MAX_ENTRIES_PER_PREFIX:
            print(f"    … and {len(sub_entries) - _MAX_ENTRIES_PER_PREFIX} more")
        if not found_marker_in_sub:
            first_real = next(
                (c for c in sub_entries if c.rstrip("/") != sub.rstrip("/")),
                None,
            )
            if first_real:
                markerless_first_children.append(first_real)

    if candidates:
        target, sentinel = candidates[0]
        target_uri = f"gs://{target}/"
        print()
        print(f"  Found {len(candidates)} Zarr store(s). Opening first: {target_uri}")
        consolidated = sentinel == ".zmetadata"
        _print_store(target_uri, consolidated=consolidated)
        print()
        print("  Re-run probe against the actual store:")
        print(f"    python scripts/weathernext_gcs_probe.py --uri {target_uri}")
    else:
        print()
        print("  No Zarr store markers found within depth 2.")

    if markerless_first_children:
        print()
        print("  Subdirs with no sentinel — trying to open first child of each as zarr:")
        for path in markerless_first_children:
            test_uri = f"gs://{path}/"
            print(f"    {test_uri}")
            opened = False
            for cons in (True, False):
                try:
                    _print_store(test_uri, consolidated=cons, indent="      ")
                    print(f"      (consolidated={cons})")
                    opened = True
                    break
                except Exception as exc:
                    print(f"      consolidated={cons} → {type(exc).__name__}: {exc}")
            if not opened:
                print("      Could not open as zarr.")


def _print_store(uri: str, *, consolidated: bool, indent: str = "  ") -> None:
    """Open one Zarr store with `chunks=None` (no dask needed) and print dims/vars."""
    import xarray as xr  # type: ignore[import-untyped]

    ds = xr.open_zarr(uri, consolidated=consolidated, chunks=None)
    print(f"{indent}Dims  : {dict(ds.dims)}")
    print(f"{indent}Vars  : {list(ds.data_vars)}")
    print(f"{indent}Coords: {list(ds.coords)}")
    if "2m_temperature" in ds.data_vars or "t2m" in ds.data_vars:
        t2m = "2m_temperature" if "2m_temperature" in ds.data_vars else "t2m"
        print(f"{indent}{t2m} units: {ds[t2m].attrs.get('units', '?')}  shape: {ds[t2m].shape}")
    # Print first few values of each coord — tiny, doesn't fetch much.
    for cname in ds.coords:
        try:
            c = ds[cname]
            sample = c.values[:3] if c.size > 3 else c.values
            print(f"{indent}coord {cname}: dtype={c.dtype} shape={c.shape} first={sample}")
        except Exception as exc:
            print(f"{indent}coord {cname}: <unreadable: {exc}>")


if __name__ == "__main__":
    main()
