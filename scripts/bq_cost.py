"""Show BigQuery spend for recent jobs in this project.

Sums `total_bytes_billed` across the project's BQ jobs in a time window and
prints GiB scanned + USD at on-demand pricing ($6.25/TiB).

Usage:
    python scripts/bq_cost.py                    # last 24h, default project
    python scripts/bq_cost.py --hours 48
    python scripts/bq_cost.py --project my-proj  # override project
"""
from __future__ import annotations

import argparse
import os
from datetime import datetime, timedelta, timezone

USD_PER_TIB = 6.25


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--hours", type=int, default=24)
    parser.add_argument(
        "--project",
        default=os.getenv("WEATHERNEXT_PROJECT") or os.getenv("GOOGLE_CLOUD_PROJECT"),
    )
    args = parser.parse_args()

    if not args.project:
        raise SystemExit(
            "No project. Pass --project, or set WEATHERNEXT_PROJECT / GOOGLE_CLOUD_PROJECT."
        )

    from google.cloud import bigquery  # type: ignore[import-untyped]

    client = bigquery.Client(project=args.project)
    since = datetime.now(timezone.utc) - timedelta(hours=args.hours)

    n = 0
    total_bytes = 0
    for job in client.list_jobs(min_creation_time=since, all_users=True):
        if job.job_type != "query":
            continue
        billed = getattr(job, "total_bytes_billed", None) or 0
        if billed:
            n += 1
            total_bytes += billed

    gib = total_bytes / 1024**3
    tib = total_bytes / 1024**4
    usd = tib * USD_PER_TIB

    print(f"Project        : {args.project}")
    print(f"Window         : last {args.hours}h")
    print(f"Billed queries : {n}")
    print(f"Bytes billed   : {gib:,.2f} GiB ({tib:.4f} TiB)")
    print(f"Cost (on-demand): ${usd:,.4f}")


if __name__ == "__main__":
    main()
