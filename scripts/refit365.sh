#!/usr/bin/env bash
set -euo pipefail
WE=/home/ohbaker1/Polyweather/.venv/bin/we
for s in RKSI ZSPD RCSS VHHH VILK LFPB KLGA KDAL KHOU KORD KBKF MMMX EGLC; do
    echo "=== $s ==="
    $WE onboard-station --station "$s" --backfill-days 365
done
