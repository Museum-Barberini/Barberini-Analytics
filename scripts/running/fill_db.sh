#!/bin/bash
case $1 in
    daily)
        TASK=FillDBDaily
        export OUTPUT_DIR="output_daily"
        ;;
    hourly)
        TASK=FillDBHourly
        export OUTPUT_DIR="output_hourly"
        ;;
    *)
        TASK=FillDB
        ;;
esac


cd /app
make luigi-task LMODULE=fill_db LTASK=$TASK
if [ $? -ne 0 ]
    then cp -r ./output ./output-$1-run-$(date +"%Y-%m-%d_%H-%M")
fi

make luigi-clean
