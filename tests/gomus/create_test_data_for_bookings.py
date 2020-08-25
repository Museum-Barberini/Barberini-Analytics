#!/usr/bin/env python3
"""Generate test cases for scrape_bookings_data.csv."""
# link to a specific booking:
# https://barberini.gomus.de/admin/bookings/BOOKING-ID

import mmh3

hash_seed = 666

booker_id = input("Enter booking_id\n").strip()
mail_address = input(
    "\nEnter mail address and then immediately forget it\n\
or simply hit ENTER if there is none:\n").strip()
other_data = input(
    "\nEnter other data that you expect to be scraped in this format:\n\
order_date,language\n\
2019-12-31 23:59:00,Deutsch\n").strip()

if mail_address:
    customer_id = mmh3.hash(mail_address, seed=hash_seed)
else:
    customer_id = 0
to_be_hashed = f"{customer_id},{other_data}"

hashed = mmh3.hash(to_be_hashed, seed=hash_seed)
print("\nAdd this to scrape_bookings_data.csv in test_data:")
print(f"{booker_id},{hashed}")
