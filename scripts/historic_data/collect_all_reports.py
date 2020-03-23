#!/usr/bin/env python3

import datetime as dt
import subprocess as sp
import sys

# before start: run 'make connect'

# -Customers-
#   some reports need to be adjusted manually (misplaced columns)
#   -> make sure you got all reports that require fixing
# -Orders-
#   run customers before orders
#   comment out: _required Customer-Tasks in ExtractOrderData

sp.run(
    "make luigi-scheduler".split()
)

report_type = sys.argv[1]
cap_type = report_type.capitalize()

today = dt.date.today()

for week_offset in range(0, 250):

    print(report_type, week_offset, today)

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

    if report_type == 'customers':
        sp.run(
            f"mv output/gomus/gomus_to_customers_mapping.csv output/gomus/"
            f"gomus_to_customers_mapping_{week_offset}.csv".split()
        )
        sp.run(
            f"luigi --module gomus.{report_type} GomusToCustomerMappingToDB "
            f"--today {today}".split()
        )

    today = today - dt.timedelta(weeks=1)
