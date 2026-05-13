"""Inspect the WeatherNext 2 BigQuery table schema.

Run this once before backfill to confirm the column names in
src/weather_edge/ingest/weathernext.py match your linked dataset. If any
identifier differs (e.g. "valid_time" vs "time"), update the _COL_* constants
at the top of that module.

Usage:
    python scripts/weathernext_schema.py
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parents[1] / "src"))


def main() -> None:
    from google.cloud import bigquery  # type: ignore[import-untyped]

    project = os.getenv("WEATHERNEXT_PROJECT")
    dataset = os.getenv("WEATHERNEXT_DATASET")
    table = os.getenv("WEATHERNEXT_TABLE", "weathernext_2_0_0")
    if not (project and dataset):
        raise SystemExit("WEATHERNEXT_PROJECT and WEATHERNEXT_DATASET env vars must be set.")

    client = bigquery.Client(project=project)
    ref = client.get_table(f"{project}.{dataset}.{table}")
    print(f"\nTable: {ref.full_table_id}")
    print(f"Rows : {ref.num_rows:,}   Size: {ref.num_bytes / 1e9:.1f} GB\n")
    print(f"{'NAME':<30} {'TYPE':<12} MODE")
    print("-" * 60)
    for f in ref.schema:
        print(f"{f.name:<30} {f.field_type:<12} {f.mode}")


if __name__ == "__main__":
    main()
