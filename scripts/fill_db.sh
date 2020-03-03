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
make luigi-task LMODULE=fill_db LTASK=$TASK
if [ $? -ne 0 ]
    then cp -r ./output ./output-$1-run-$(date +"%Y-%m-%d_%H-%M")
fi
make luigi-clean