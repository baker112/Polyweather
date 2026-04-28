#!/usr/bin/env bash
# Onboard all stations except EGLC (already live).
# Run in a tmux session so you can detach and come back:
#   tmux new -s onboard
#   bash scripts/onboard_all.sh 2>&1 | tee ~/onboard.log
#   Ctrl-B D  (detach)

set -euo pipefail

WE=/home/ohbaker1/Polyweather/.venv/bin/we
STATIONS=(RKSI ZSPD RCSS VHHH VILK LFPB KLGA KDAL KHOU KORD KBKF MMMX)
TOTAL=${#STATIONS[@]}

echo "=== Polyweather bulk onboard — $(date -u '+%Y-%m-%d %H:%M')z ==="
echo "Stations to onboard: ${STATIONS[*]}"
echo "Estimated time: ~45 min/station × $TOTAL stations = ~$((TOTAL * 45 / 60))h"
echo ""

FAILED=()

for i in "${!STATIONS[@]}"; do
    SID="${STATIONS[$i]}"
    echo "──────────────────────────────────────────────────────────"
    echo "[$((i+1))/$TOTAL] Onboarding $SID — $(date -u '+%H:%M')z"
    echo "──────────────────────────────────────────────────────────"

    if $WE onboard-station --station "$SID" --backfill-days 90; then
        echo "[OK] $SID done — $(date -u '+%H:%M')z"
    else
        echo "[FAIL] $SID failed — $(date -u '+%H:%M')z"
        FAILED+=("$SID")
    fi

    # Brief pause between stations to be polite to GEFS/METAR APIs
    if [[ $((i+1)) -lt $TOTAL ]]; then
        echo "(pausing 10s before next station...)"
        sleep 10
    fi
done

echo ""
echo "=== Onboard complete — $(date -u '+%Y-%m-%d %H:%M')z ==="
if [[ ${#FAILED[@]} -gt 0 ]]; then
    echo "FAILED stations: ${FAILED[*]}"
    echo "Re-run the script — onboard-station is idempotent and will skip cached data."
else
    echo "All stations onboarded successfully."
fi

echo ""
echo "Next step — restart the scheduler service to pick up all stations:"
echo "  sudo systemctl restart polyweather"
echo "  sudo systemctl status polyweather"
