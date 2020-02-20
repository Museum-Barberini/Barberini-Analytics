#!/bin/bash
USER=$1-run
LOG=/var/log/bp-logs/$1.log
BASEDIR=$(dirname "$0")/..
PATH=/usr/local/bin:$PATH

export SSL_DIR=/etc/letsencrypt/live/barberini-analytics.westeurope.cloudapp.azure.com
if [[ -e $SSL_DIR/privkey.pem ]]
    then export SSLON=1;
fi

{
echo "================================================================================================"
echo "Starting $1 run at [$(date +"%Y-%m-%d %H:%M")]"
make -C $BASEDIR startup USER=$USER
docker-compose -p $USER -f $BASEDIR/docker-compose.yml exec -T luigi /app/scripts/fill_db.sh $1
make -C $BASEDIR shutdown USER=$USER
if [ $1 == "daily" ]
    then make -C $BASEDIR db-backup
fi
echo "Ending $1 run at [$(date +"%Y-%m-%d %H:%M")]"
echo "================================================================================================"
} >> $LOG 2>&1

