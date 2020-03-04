#!usr/bin/env python3
import subprocess as sp

print('-----now collecting all bookings-----')

sp.run(
    "make luigi-scheduler".split()
)
sp.run(
    "luigi --module gomus.bookings BookingsToDB "
    "--timespan _all".split()
)

print('-----done-----')
