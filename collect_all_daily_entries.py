#!usr/bin/env python3

import datetime as dt
import subprocess as sp

# before start: run 'make connect'

# -Daily Entries-

sp.run(
    "make luigi-scheduler".split()
)

for day_offset in range(250 * 7):

    print(day_offset)

    cur_day = dt.date.today() - dt.timedelta(days=day_offset)

    # daily entries

    sp.run(
        f"luigi --module gomus.daily_entries DailyEntriesToDB "
        f"--today {cur_day}".split()
    )
    sp.run(
        f"mv output/gomus/daily_entries.csv output/gomus/daily_entries"
        f"_{cur_day}.csv".split()
    )
    for i in range(2):
        sp.run(
            f"mv output/gomus/entries_1day.{i}.csv "
            f"output/gomus/entries_1day_{cur_day}.{i}.csv".split()
        )

    # expected daily entries

    sp.run(
        f"luigi --module gomus.daily_entries ExpectedDailyEntriesToDB "
        f"--today {cur_day}".split()
    )
    sp.run(
        f"mv output/gomus/expected_daily_entries.csv "
        f"output/gomus/expected_daily_entries_{cur_day}.csv".split()
    )
    for i in range(2):
        j = i+2
        sp.run(
            f"mv output/gomus/entries_1day.{j}.csv "
            f"output/gomus/entries_1day_{cur_day}.{j}"
            ".csv".split()
        )
