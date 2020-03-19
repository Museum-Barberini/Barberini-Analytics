#!/usr/bin/env python3

# before start run 'make connect'

import subprocess as sp

sp.run(
    "make luigi-scheduler".split()
)
sp.run(
    "luigi --module gomus.bookings BookingsToDB "
    "--timespan _all".split()
)
