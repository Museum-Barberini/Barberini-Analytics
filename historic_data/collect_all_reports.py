#!usr/bin/env python3

import datetime as dt
import subprocess as sp
import sys

# before start: run 'make connect'

# -Customers-
#   some reports need to be adjusted manually (misplaced columns)
#   -> make sure you got all reports that require fixing
# -Orders-
#   run customers before orders
#   comment out: _required CustomersToDB-Task in ExtractOrderData

sp.run(
    "make luigi-scheduler".split()
)

report_type = sys.argv[1]
cap_type = report_type.capitalize()

today = dt.date.today()
if report_type == 'orders':
    today = today - dt.timedelta(days=1)

for week_offset in range(250):

    print(report_type, week_offset)

    today = today - dt.timedelta(weeks=week_offset)

    sp.run(
        f"mv output/gomus/{report_type}.csv output/gomus/{report_type}"
        f"_{week_offset}.csv".split()
    )
    sp.run(
        f"mv output/gomus/{report_type}_7days.0.csv output/gomus/{report_type}"
        f"_{week_offset}_7days.0.csv".split()
    )
    sp.run(
        f"luigi --module gomus.{report_type} {cap_type}ToDB "
        f"--today {today}".split()
    )
