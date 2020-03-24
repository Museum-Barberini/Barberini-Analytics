#!/usr/bin/env python3

from historic_data_helper import prepare_task, run_luigi_task

# -Events-
#   run bookings before events
#   remove conditional in EnsureBookingsIsRun (collect all event_ids)

# run 'make connect' first

prepare_task()

run_luigi_task('events', 'Events')
