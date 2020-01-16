#!/bin/bash
USER=hourly-run
LOG=/var/log/bp-logs/hourly.log
BASEDIR=$(dirname "$0")/..
PATH=/usr/local/bin:$PATH

{
echo "================================================================================================"
echo [$(date +"%Y-%m-%d %H:%M")]
make -C $BASEDIR startup USER=$USER
docker-compose -p $USER -f $BASEDIR/docker-compose.yml exec -T luigi /bin/bash -c 'cd /app \
    && make luigi-ui \
    && sleep 5 \
    && make luigi-task LMODULE=fill_db LTASK=FillDBHourly \
    && make luigi-clean'
make -C $BASEDIR shutdown USER=$USER
echo "================================================================================================"
} >> $LOG 2>&1

