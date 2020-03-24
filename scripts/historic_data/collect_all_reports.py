#!/usr/bin/env python3

import datetime as dt
import sys

from historic_data_helper import HistoricData

# -Customers-
#   some reports need to be adjusted manually (misplaced columns)
#   -> make sure you got all reports that require fixing
# -Orders-
#   run customers before orders
#   comment out: _required Customer-Tasks in ExtractOrderData

# run 'make connect' first

HistoricData.prepare_task()

report_type = sys.argv[1]
cap_type = report_type.capitalize()

today = dt.date.today()

for week_offset in range(0, 250):

    print(report_type, week_offset, today)

    HistoricData.run_luigi_task(report_type,
                                cap_type,
                                'today',
                                today)

    HistoricData.rename_output(f'{report_type}.csv', week_offset)

    HistoricData.rename_output(f'{report_type}_7days.0.csv', week_offset)

    if report_type == 'customers':

        HistoricData.run_luigi_task(report_type,
                                    'GomusToCustomerMapping',
                                    'today',
                                    today)

        HistoricData.rename_output('gomus_to_customers_mapping.csv',
                                   week_offset)

    today = today - dt.timedelta(weeks=1)
