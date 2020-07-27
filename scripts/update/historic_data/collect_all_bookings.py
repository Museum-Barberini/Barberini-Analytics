#!/usr/bin/env python3

from historic_data_helper import prepare_task, run_luigi_task

# -Bookings-

# run 'make connect' first

prepare_task()

run_luigi_task('bookings',
               'Bookings',
               'timespan',
               '_all')
