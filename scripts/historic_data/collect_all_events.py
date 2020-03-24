#!/usr/bin/env python3

from historic_data_helper import HistoricData

# -Events-
#   run bookings before events
#   remove conditional in EnsureBookingsIsRun (collect all event_ids)

# run 'make connect' first

HistoricData.prepare_task()

HistoricData.run_luigi_task('events', 'Events')
