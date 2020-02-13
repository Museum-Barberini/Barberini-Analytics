#!/usr/bin/env python3
import luigi

from csv_to_db import CsvToDb

from ._utils.scrape_gomus import EnhanceBookingsWithScraper


class BookingsToDB(CsvToDb):

    table = 'gomus_booking'

    columns = [
        ('booking_id', 'INT'),
        ('customer_id', 'INT'),
        ('category', 'TEXT'),
        ('participants', 'INT'),
        ('guide_id', 'INT'),
        ('date', 'DATE'),
        ('daytime', 'TIME'),
        ('duration', 'INT'), # in minutes
        ('exhibition', 'TEXT'),
        ('title', 'TEXT'),
        ('status', 'TEXT'),
        ('order_date', 'DATE'),
        ('language', 'TEXT')
    ]
    
    primary_key = 'booking_id'

    foreign_keys = [
            {
                "origin_column": "customer_id",
                "target_table": "gomus_customer",
                "target_column": "customer_id"
            }
        ]

    def requires(self):
        return EnhanceBookingsWithScraper()
