#!/usr/bin/env bash
# Onboard all stations except EGLC (already live).
# Run in a tmux session so you can detach and come back:
#   tmux new -s onboard
#   bash scripts/onboard_all.sh 2>&1 | tee ~/onboard.log
#   Ctrl-B D  (detach)
#
# Sends Telegram notifications on each station complete/fail and a final summary.

set -euo pipefail

WE=/home/ohbaker1/Polyweather/.venv/bin/we
ENV_FILE=/home/ohbaker1/Polyweather/.env
STATIONS=(RKSI ZSPD RCSS VHHH VILK LFPB KLGA KDAL KHOU KORD KBKF MMMX)
TOTAL=${#STATIONS[@]}

# Load TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID from .env
if [[ -f "$ENV_FILE" ]]; then
    set -a; source "$ENV_FILE"; set +a
fi

tg() {
    local msg="$1"
    if [[ -n "${TELEGRAM_BOT_TOKEN:-}" && -n "${TELEGRAM_CHAT_ID:-}" ]]; then
        curl -s -X POST "https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendMessage" \
            -d "chat_id=${TELEGRAM_CHAT_ID}" \
            -d "text=${msg}" \
            -d "parse_mode=HTML" > /dev/null
    fi
}

echo "=== Polyweather bulk onboard — $(date -u '+%Y-%m-%d %H:%M')z ==="
echo "Stations to onboard: ${STATIONS[*]}"
echo "Estimated time: ~45 min/station × $TOTAL stations = ~$((TOTAL * 45 / 60))h"
echo ""

tg "Onboard started: ${STATIONS[*]} (~$((TOTAL * 45 / 60))h)"

# ── Step 0: Fix EGLC observations with correct floor truncation ───────────────
echo "──────────────────────────────────────────────────────────"
echo "[0] Re-ingesting EGLC observations (truncation fix) — $(date -u '+%H:%M')z"
echo "──────────────────────────────────────────────────────────"
if $WE ingest observations --station EGLC --start 2025-11-01 --end 2026-04-28; then
    echo "[OK] EGLC observations re-ingested"
    $WE fit-emos --station EGLC && echo "[OK] EGLC EMOS refitted"
    tg "[0] EGLC obs + EMOS fixed (truncation)"
else
    echo "[WARN] EGLC observation re-ingest failed — continuing"
    tg "[0] EGLC obs re-ingest FAILED — check log"
fi
echo ""

FAILED=()
DONE=0

for i in "${!STATIONS[@]}"; do
    SID="${STATIONS[$i]}"
    echo "──────────────────────────────────────────────────────────"
    echo "[$((i+1))/$TOTAL] Onboarding $SID — $(date -u '+%H:%M')z"
    echo "──────────────────────────────────────────────────────────"

    START_T=$(date +%s)
    if $WE onboard-station --station "$SID" --backfill-days 90; then
        ELAPSED=$(( ($(date +%s) - START_T) / 60 ))
        echo "[OK] $SID done — $(date -u '+%H:%M')z (${ELAPSED}m)"
        DONE=$((DONE + 1))
        tg "[$((i+1))/$TOTAL] $SID onboarded OK (${ELAPSED}m) — $((TOTAL - i - 1)) remaining"
    else
        ELAPSED=$(( ($(date +%s) - START_T) / 60 ))
        echo "[FAIL] $SID failed — $(date -u '+%H:%M')z (${ELAPSED}m)"
        FAILED+=("$SID")
        tg "[$((i+1))/$TOTAL] $SID FAILED after ${ELAPSED}m"
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
    tg "Onboard done: $DONE/$TOTAL OK. FAILED: ${FAILED[*]}. Re-run to retry."
else
    echo "All $DONE stations onboarded successfully."
    tg "Onboard done: all $DONE stations OK. Run: sudo systemctl restart polyweather"
fi

echo ""
echo "Next step — restart the scheduler service to pick up all stations:"
echo "  sudo systemctl restart polyweather"
echo "  sudo systemctl status polyweather"
