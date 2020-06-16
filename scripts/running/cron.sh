#!/bin/bash
set -e

USER=$1-run
BASEDIR=$(dirname "$0")/../..
PATH="/usr/local/bin:$PATH"

EMAIL_STR="INFO: Sending email to \[.+\]"

export BARBERINI_ANALYTICS_CONTEXT=PRODUCTION

LOGPATH="/var/log/barberini-analytics"
LOGFILE=$LOGPATH/"$1-$(date +%Y-%m-%d).log"
TMPFILE=$(mktemp)

# Delete 2 weeks old log
rm $LOGPATH/"$1-$(date -d '2 weeks ago' +%Y-%m-%d).log" || true

{
echo "======================================================================="
echo "Starting $1 run at [$(date +"%Y-%m-%d %H:%M")]"
make -C $BASEDIR startup USER=$USER
docker-compose -p $USER -f $BASEDIR/docker/docker-compose.yml exec -T \
        barberini_analytics_luigi /app/scripts/running/fill_db.sh $1 \
    || grep -Eq $EMAIL_STR $TMPFILE \
    || docker-compose -p $USER -f $BASEDIR/docker/docker-compose.yml exec -T \
        barberini_analytics_luigi /app/scripts/running/external_error.py $1
make -C $BASEDIR shutdown USER=$USER
echo "Ending $1 run at [$(date +"%Y-%m-%d %H:%M")]"
echo "======================================================================="
} > $TMPFILE 2>&1

cat $TMPFILE >> $LOGFILE
