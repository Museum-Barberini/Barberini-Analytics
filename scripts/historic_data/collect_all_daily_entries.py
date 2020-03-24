#!/usr/bin/env python3

import datetime as dt

from historic_data_helper import HistoricData

# -Daily Entries-

# run 'make connect' first

HistoricData.prepare_task()

cur_day = dt.date.today()

for day_offset in range(7 * 7):

    print(day_offset)

    # daily entries

    HistoricData.run_luigi_task('daily_entries',
                                'DailyEntries',
                                'today',
                                cur_day)

    HistoricData.rename_output('daily_entries.csv', day_offset)

    for i in range(2):

        HistoricData.rename_output(f'entries_1day.{i}.csv', day_offset)

    # expected daily entries

    HistoricData.run_luigi_task('daily_entries',
                                'ExpectedDailyEntries',
                                'today',
                                cur_day)

    HistoricData.rename_output('expected_daily_entries.csv', day_offset)

    for j in range(2, 4):

        HistoricData.rename_output(f'entries_1day.{j}.csv', day_offset)

    cur_day = cur_day - dt.timedelta(days=1)
