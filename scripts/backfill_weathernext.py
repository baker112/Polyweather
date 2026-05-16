"""DISABLED — old BigQuery-based WeatherNext backfill.

This script routed every backfill chunk through BigQuery and was responsible
for the 199 TiB / £215 near-miss in 2026-05. It has been intentionally stubbed
to refuse to run.

The replacement — a GCS/Zarr backfill — has not been written yet. When it is,
it will live in `scripts/backfill_weathernext_gcs.py` and pull directly from
`gs://weathernext/weathernext_2_0_0/zarr/` (egress is sponsor-paid for that
public dataset; verified £0 at 5 GiB on 2026-05-16).

If you genuinely need to re-enable BigQuery here (you almost certainly don't),
restore this file from git and ALSO remove the `_assert_no_bq_backend()` guard
in `src/weather_edge/ingest/weathernext.py`. Two locks, by design.
"""
from __future__ import annotations

import sys


def main() -> None:
    print(
        "ERROR: scripts/backfill_weathernext.py is disabled — it used BigQuery, "
        "which caused the 199 TiB / £215 near-miss in 2026-05.\n"
        "Use the (yet-to-be-written) GCS backfill at "
        "scripts/backfill_weathernext_gcs.py instead.",
        file=sys.stderr,
    )
    sys.exit(2)


if __name__ == "__main__":
    main()
