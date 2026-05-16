"""DISABLED — used to inspect the WeatherNext 2 BigQuery table schema.

The BigQuery Python client was removed from the project's dependencies
(google-cloud-bigquery is no longer in pyproject.toml). The WN2 ingest path
no longer uses BQ at all — we read directly from the gs://weathernext/ Zarr
stores. Use scripts/weathernext_gcs_probe.py to inspect the GCS schema
instead.
"""
from __future__ import annotations

import sys


def main() -> None:
    print(
        "ERROR: scripts/weathernext_schema.py is disabled — WN2 ingest no "
        "longer uses BigQuery. Use scripts/weathernext_gcs_probe.py to inspect "
        "the GCS Zarr schema.",
        file=sys.stderr,
    )
    sys.exit(2)


if __name__ == "__main__":
    main()
