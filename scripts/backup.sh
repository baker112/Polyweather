#!/bin/bash
# Daily backup of data/ to GCS.
# Run once manually to test: bash ~/Polyweather/scripts/backup.sh
# Add to crontab: 30 4 * * * bash /home/ohbaker1/Polyweather/scripts/backup.sh >> /home/ohbaker1/Polyweather/logs/backup.log 2>&1

set -euo pipefail

BUCKET="${POLYWEATHER_BACKUP_BUCKET:-gs://polyweather-backup}"
SRC="/home/ohbaker1/Polyweather/data/"

echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] Starting backup to $BUCKET"
gsutil -m rsync -r -d "$SRC" "$BUCKET/data/"
echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] Backup complete"
