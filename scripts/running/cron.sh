#!/bin/bash
USER=$1-run
BASEDIR=$(dirname "$0")/../..
PATH=/usr/local/bin:$PATH

LOGPATH=/var/log/bp-logs
LOGFILE=$LOGPATH/"$1-`date +%Y-%m-%d`.log"

# Delete 1 week old log
rm $LOGPATH/"$1-`date -d '1 week ago' +%Y-%m-%d`.log" || true

{
echo "================================================================================================"
echo "Starting $1 run at [$(date +"%Y-%m-%d %H:%M")]"
make -C $BASEDIR startup USER=$USER
docker-compose -p $USER -f $BASEDIR/docker-compose.yml exec -T luigi /app/scripts/running/fill_db.sh $1
make -C $BASEDIR shutdown USER=$USER
echo "Ending $1 run at [$(date +"%Y-%m-%d %H:%M")]"
echo "================================================================================================"
} >> $LOGFILE 2>&1
