#!/bin/bash
# usage:
# fill_db.sh [daily | hourly]
# fill_db.sh  # all tasks
# TASK=SomeTask fill_db.sh

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
        TASK=${TASK:-FillDb}
        ;;
esac

# output directories are unique per run
OUTPUT_DIR="$OUTPUT_DIR-$1-run-$(date +"%Y-%m-%d_%H-%M")"
export OUTPUT_DIR

cd /app || exit
make apply-pending-migrations luigi-task LMODULE=_fill_db LTASK="$TASK"
EXIT_VAL=$?

# preserve output directory if task failed (for debugging or manual re-run)
# in particular, if the script was interrupted, the output directory will not be deleted
if [ $EXIT_VAL -eq 0 ]
    then make luigi-clean
fi

exit $EXIT_VAL
