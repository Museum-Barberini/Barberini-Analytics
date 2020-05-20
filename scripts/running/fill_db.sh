#!/bin/bash
set -e

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
{
    make apply-pending-migrations luigi-task LMODULE=fill_db LTASK=$TASK
} || {
    cp -r $OUTPUT_DIR ./output-$1-run-$(date +"%Y-%m-%d_%H-%M")
}

make luigi-clean
