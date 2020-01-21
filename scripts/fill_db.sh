#!/bin/bash
case $1 in
    daily)
        TASK=FillDBDaily
        ;;
    hourly)
        TASK=FillDBHourly
        ;;
    *)
        TASK=UnknownTask
        ;;
esac

cd /app
make luigi-ui
sleep 3
make luigi-task LMODULE=fill_db LTASK=$TASK
make luigi-clean