#!/bin/bash
BACKUP_DIR='/var/db-backups'

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

rm "$BACKUP_DIR/$BASE_NAME"*
docker exec db pg_dump -U postgres barberini > "$BACKUP_DIR/$BASE_NAME-`date +%Y-%m-%d`.sql"
