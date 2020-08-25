#!/usr/bin/env python3
"""
Script to collect all historic events from Gomus.

# -Events-
#   run bookings before events
#   remove conditional in FetchCategoryReservations (collect all event_ids)
"""

from historic_data_helper import prepare_task, run_luigi_task


prepare_task()

run_luigi_task('events', 'Events')
