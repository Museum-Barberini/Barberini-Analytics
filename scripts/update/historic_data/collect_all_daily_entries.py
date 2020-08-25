#!/usr/bin/env python3
"""Script to collect all historic daily entries from Gomus."""

import datetime as dt

from historic_data_helper import prepare_task, run_luigi_task, rename_output


prepare_task()

cur_day = dt.date.today()
first_date = dt.date(2016, 1, 1)

# the number of days to export daily entry reports
delta = cur_day - first_date
print(delta.days)

for day_offset in range(delta.days):

    print(day_offset)

    # daily entries

    run_luigi_task('daily_entries',
                   'DailyEntries',
                   'today',
                   cur_day)

    rename_output('daily_entries.csv', day_offset)

    for i in range(2):

        rename_output(f'entries_1day.{i}.csv', day_offset)
        rename_output(f'entries_unique_1day.{i}.csv', day_offset)

    # expected daily entries

    run_luigi_task('daily_entries',
                   'ExpectedDailyEntries',
                   'today',
                   cur_day)

    rename_output('expected_daily_entries.csv', day_offset)

    for j in range(2, 4):

        rename_output(f'entries_1day.{j}.csv', day_offset)
        rename_output(f'entries_unique_1day.{j}.csv', day_offset)

    cur_day = cur_day - dt.timedelta(days=1)
