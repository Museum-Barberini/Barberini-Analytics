#!/bin/bash

case $1 in
    daily)
        TASK=FillDbDaily
        export OUTPUT_DIR="output_daily"
        ;;
    hourly)
        TASK=FillDbHourly
        export OUTPUT_DIR="output_hourly"
        ;;
    *)
        TASK=FillDb
        ;;
esac

cd /app
make apply-pending-migrations luigi-task LMODULE=fill_db LTASK=$TASK
EXIT_VAL=$?

if [ $EXIT_VAL -ne 0 ]
    then cp -r $OUTPUT_DIR ./output-$1-run-$(date +"%Y-%m-%d_%H-%M")
fi

make luigi-clean

exit $EXIT_VAL
