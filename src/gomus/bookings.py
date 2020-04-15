import luigi
import os

from csv_to_db import CsvToDb
from gomus._utils.scrape_gomus import EnhanceBookingsWithScraper


class BookingsToDB(CsvToDb):

    timespan = luigi.parameter.Parameter(default='_nextYear')
    table = 'gomus_booking'

    columns = [
        ('booking_id', 'INT'),
        ('customer_id', 'INT'),
        ('category', 'TEXT'),
        ('participants', 'INT'),
        ('guide_id', 'INT'),
        ('duration', 'INT'),  # in minutes
        ('exhibition', 'TEXT'),
        ('title', 'TEXT'),
        ('status', 'TEXT'),
        ('start_datetime', 'TIMESTAMP'),
        ('order_date', 'DATE'),
        ('language', 'TEXT')
    ]

    primary_key = 'booking_id'

    foreign_keys = [
        {
            'origin_column': 'customer_id',
            'target_table': 'gomus_customer',
            'target_column': 'customer_id'
        }
    ]

    def requires(self):
        timespan = self.timespan
        if os.environ['MINIMAL'] == 'True':
            timespan = '_7days'
        return EnhanceBookingsWithScraper(
            columns=[col[0] for col in self.columns],
            foreign_keys=self.foreign_keys,
            timespan=timespan)
