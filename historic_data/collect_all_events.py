#!usr/bin/env python3

import subprocess as sp

# -Events-
#   run bookings before events

sp.run(
    "make luigi-scheduler".split()
)
sp.run(
    "luigi --module gomus.events EventsToDB".split()
)
