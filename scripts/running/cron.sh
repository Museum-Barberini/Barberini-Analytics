#!/bin/bash
set -e
set -o pipefail

export USER=$1-run
BASEDIR=$(dirname "$0")/../..
PATH="/usr/local/bin:$PATH"
LOGPATH="/var/log/barberini-analytics"
LOGFILE="$LOGPATH/$1-$(date +%Y-%m-%d).log"
TMPFILE=$(mktemp --suffix=.log)
EMAIL_STR="INFO: Sending email to \[.+\]"
export BARBERINI_ANALYTICS_CONTEXT=PRODUCTION

# Delete 2 weeks old log
rm -f "$LOGPATH/$1-$(date -d '2 weeks ago' +%Y-%m-%d).log"


{
    echo "===================================================================="
    echo "Starting $1 run at [$(date +"%Y-%m-%d %H:%M")]"

    {
        make -C "$BASEDIR" startup

        docker-compose -p "$USER" -f "$BASEDIR/docker/docker-compose.yml" \
            exec -T barberini_analytics_luigi \
            /app/scripts/running/fill_db.sh "$1"
    } |& tee "$TMPFILE" \
        || grep -Eq "$EMAIL_STR" "$TMPFILE" \
        || docker-compose -p "$USER" -f "$BASEDIR/docker/docker-compose.yml" \
            exec -T barberini_analytics_luigi \
            /app/scripts/running/notify_external_error.py "$1"

    rm -f "$TMPFILE"
    make -C "$BASEDIR" shutdown

    echo "Ending $1 run at [$(date +"%Y-%m-%d %H:%M")]"
    echo "======================================================================="
} > "$LOGFILE" 2>&1
