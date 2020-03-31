#!/bin/bash
case $1 in
    daily)
        TASK=FillDBDaily
        OTHER_CONTAINER=hourly-run-luigi
        ;;
    hourly)
        TASK=FillDBHourly
        OTHER_CONTAINER=daily-run-luigi
        ;;
    *)
        TASK=FillDB
        OTHER_CONTAINER=ly-run-luigi  # matches both hourly and daily run
        ;;
esac

cd /app
make luigi-task LMODULE=fill_db LTASK=$TASK
if [ $? -ne 0 ]
    then cp -r ./output ./output-$1-run-$(date +"%Y-%m-%d_%H-%M")
fi

# don't delete the output if a daily/hourly run is still in progress
if [ ! "$( docker ps -q --filter name=$OTHER_CONTAINER )" ]
    then make luigi-clean
fi
