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

# powered by GitHub Copilot

APPPATH="/root/bp-barberini"
LOGPATH="/var/log/barberini-analytics"
DAYS_TO_KEEP=90

# the date format is not always the same, so we have to use a regex
# to get the date out of the folder name
DATE_REGEX='[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]'

# get the current date
NOW=$(date +%s)

# find all output folders
{ find "$APPPATH" -type d -iname "output-*-run-*" -print0 & find "$LOGPATH" -type f -iname "*ly*.log" -print0; } |
    while IFS= read -r -d '' FOLDER; do
        # get the date from the folder name
        FOLDER_DATE=$(echo "$FOLDER" | grep -o "$DATE_REGEX")
        # convert the date to seconds
        FOLDER_DATE=$(date --date="$FOLDER_DATE" +%s)
        # calculate the difference between the current date and the folder date
        DIFF=$(( (NOW - FOLDER_DATE) / 86400 ))
        # delete the folder if it is older than max days
        if [ $DIFF -gt $DAYS_TO_KEEP ]; then
            echo "deleting $FOLDER"
            rm -rf "$FOLDER"
        fi
    done
