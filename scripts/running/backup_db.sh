#!/bin/bash
set -e

BACKUP_DIR='/var/barberini-analytics/db-backups'
mkdir -p "$BACKUP_DIR"
case $1 in
    monthly)
        INTERVAL='monthly'
        ;;
    weekly)
        INTERVAL='weekly'
        ;;
    daily)
        INTERVAL='daily'
        ;;
    *)
        echo "Please specify monthly, weekly or daily"
        exit 1
        ;;
esac
BASE_NAME="db_dump_$INTERVAL"

rm -f "$BACKUP_DIR/$BASE_NAME"*
docker exec barberini_analytics_db pg_dump -U postgres barberini > "$BACKUP_DIR/$BASE_NAME-$(date +%Y-%m-%d).sql"
