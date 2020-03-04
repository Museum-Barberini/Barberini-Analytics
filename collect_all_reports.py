#!usr/bin/env python3

# orders / customers
import datetime as dt
import subprocess as sp
import sys

for week_offset in range(250):

    report_type = sys.argv[1]
    cap_type = report_type.capitalize()
    print(report_type, week_offset)

    today = dt.date.today() - dt.timedelta(weeks=week_offset)
    sp.run(
        f"mv output/gomus/{report_type}.csv output/gomus/{report_type}"
        f"_{week_offset}.csv".split()
    )
    sp.run(
        f"mv output/gomus/{report_type}.0.csv output/gomus/{report_type}"
        f"_{week_offset}.0.csv".split()
    )
    sp.run(
        f"luigi --module gomus.{report_type} {cap_type}ToDB "
        f"--today {today}".split()
    )

print("\n------------\n completed successfully \n -----------")
