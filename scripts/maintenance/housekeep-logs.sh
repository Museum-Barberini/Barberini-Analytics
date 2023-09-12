#!/usr/bin/env bash

# given a directory of many folders named like this:
#    11.7 MiB [          ] /output-daily-run-2021-09-07_04-32
#    10.5 MiB [          ] /output-daily-run-2020-09-03_04-02
#     7.9 MiB [          ] /output-hourly-run-2020-04-01_08-05
#     6.8 MiB [          ] /output-daily-run-2020-07-21_03-35
#     6.1 MiB [          ] /output-hourly-run-2020-04-02_11-13
#     4.9 MiB [          ] /output-daily-run-2020-07-18_03-34
#     3.6 MiB [          ] /output-hourly-run-2022-01-01_16-03
#     3.6 MiB [          ] /output-hourly-run-2022-01-01_21-02
# we want to delete all of these folders that are older than 90 days

APPPATH="/root/barberini-analytics"
LOGPATH="/var/log/barberini-analytics"
DAYS_TO_KEEP=${DAYS_TO_KEEP:-90}
TYPES=${TYPES:-"log output"}
# split into array
IFS=' ' read -r -a TYPES <<< "$TYPES"
DRY=${DRY:-false}

DATE_REGEX='[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]'

NOW=$(date +%s)
shopt -s lastpipe  # nonlocal variables
COUNT=0
SIZE=0

{
    ([[ " ${TYPES[*]} " =~ "log" ]] && find "$LOGPATH" -type f -iname "*ly*.log" -print0) || true \
    & ([[ " ${TYPES[*]} " =~ "output" ]] && find "$APPPATH" -type d -iname "output-*-run-*" -print0) || true;
} |
    while IFS= read -r -d '' FOLDER; do
        # get the date from the folder name
        FOLDER_DATE=$(echo "$FOLDER" | grep -o "$DATE_REGEX")
        # convert the date to seconds
        FOLDER_DATE=$(date --date="$FOLDER_DATE" +%s)
        # calculate the difference between the current date and the folder date
        DIFF=$(( (NOW - FOLDER_DATE) / 86400 ))
        # delete the folder if it is older than max days
        if [ $DIFF -gt "$DAYS_TO_KEEP" ]; then
            COUNT=$((COUNT + 1))
            SIZE=$((SIZE + $(du -s "$FOLDER" | cut -f1)))
            echo "rm -rf $FOLDER"
            if [ "$DRY" = false ]; then
                rm -rf "$FOLDER"
            fi
        fi
    done

echo "$([ "$DRY" = true ] && echo "[WOULD HAVE] ")Deleted $COUNT items, freeing $SIZE kB"
