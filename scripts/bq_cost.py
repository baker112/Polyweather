"""DISABLED — used to report recent BigQuery spend via list_jobs().

The BigQuery Python client was removed from the project's dependencies
(google-cloud-bigquery is no longer in pyproject.toml). Even though this
script only read free metadata, it would now ImportError on first run.

If you genuinely need BQ cost reporting, query the billing-export table
instead (see scratch/billing_check.sql).
"""
from __future__ import annotations

import sys


def main() -> None:
    print(
        "ERROR: scripts/bq_cost.py is disabled — google-cloud-bigquery is no "
        "longer a dependency. Query the billing-export table via "
        "scratch/billing_check.sql instead.",
        file=sys.stderr,
    )
    sys.exit(2)


if __name__ == "__main__":
    main()
