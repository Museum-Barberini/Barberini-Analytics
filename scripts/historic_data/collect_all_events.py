#!/usr/bin/env python3

import subprocess as sp

# -Events-
#   run bookings before events
#   remove conditional in EnsureBookingsIsRun (collect all event_ids)

sp.run(
    "make luigi-scheduler".split()
)
sp.run(
    "luigi --module gomus.events EventsToDB".split()
)
