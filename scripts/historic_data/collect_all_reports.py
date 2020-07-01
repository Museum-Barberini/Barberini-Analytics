#!/usr/bin/env python3

import datetime as dt
import math
import sys

from historic_data_helper import prepare_task, run_luigi_task, rename_output

# -Customers-
#   some reports need to be adjusted manually (misplaced columns)
#   -> make sure you got all reports that require fixing
# -Orders-
#   run customers before orders
#   comment out: _required Customer-Tasks in ExtractOrderData

# run 'make connect' first

prepare_task()

report_type = sys.argv[1]
cap_type = report_type.capitalize()

today = dt.date.today()
first_date = dt.date(2016, 1, 1)

delta = today - first_date
weeks = math.ceil(delta.days/7)
print(weeks)

# weeks contains the number of weeks that have passed until today
for week_offset in range(0, weeks):

    print(report_type, week_offset, today)

    if report_type == 'orders':
        run_luigi_task(report_type,
                       cap_type,
                       'today',
                       today)
    else:
        run_luigi_task(report_type,
                       'GomusToCustomerMapping',
                       'today',
                       today)

    rename_output(f'{report_type}.csv', week_offset)
    rename_output(f'{report_type}_7days.0.csv', week_offset)

    if report_type == 'customers':

        rename_output(f'cleansed_{report_type}.csv', week_offset)
        rename_output('gomus_to_customers_mapping.csv', week_offset)

    today = today - dt.timedelta(weeks=1)
