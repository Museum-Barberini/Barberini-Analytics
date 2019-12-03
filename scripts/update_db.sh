#!/bin/bash
BASEDIR=$(dirname "$0")/..
make -C $BASEDIR startup
docker-compose -f $BASEDIR/docker-compose.yml exec luigi /bin/sh -c 'cd /app && make luigi-ui && sleep 5 && make luigi-task && make luigi-clean'
make -C $BASEDIR shutdown
