#!/usr/bin/env python3

import datetime as dt

from historic_data_helper import prepare_task, run_luigi_task, rename_output

# -Daily Entries-

# run 'make connect' first

prepare_task()

cur_day = dt.date.today()

# 250 weeks should be sufficient for collecting all the data
for day_offset in range(250 * 7):

    print(day_offset)

    # daily entries

    run_luigi_task('daily_entries',
                   'DailyEntries',
                   'today',
                   cur_day)

    rename_output('daily_entries.csv', day_offset)

    for i in range(2):

        rename_output(f'entries_1day.{i}.csv', day_offset)

    # expected daily entries

    run_luigi_task('daily_entries',
                   'ExpectedDailyEntries',
                   'today',
                   cur_day)

    rename_output('expected_daily_entries.csv', day_offset)

    for j in range(2, 4):

        rename_output(f'entries_1day.{j}.csv', day_offset)

    cur_day = cur_day - dt.timedelta(days=1)
