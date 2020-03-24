#!/usr/bin/env python3

from historic_data_helper import HistoricData

# -Bookings-

# run 'make connect' first

HistoricData.prepare_task()

HistoricData.run_luigi_task('bookings',
                            'Bookings',
                            'timespan',
                            '_all')
