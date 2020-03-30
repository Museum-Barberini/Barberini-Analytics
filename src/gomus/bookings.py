import luigi

from csv_to_db import CsvToDb
from gomus._utils.scrape_gomus import EnhanceBookingsWithScraper


class BookingsToDB(CsvToDb):
    minimal = luigi.parameter.BoolParameter(default=False)

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
        if self.minimal:
            self.timespan = '_7days'

        return EnhanceBookingsWithScraper(
            columns=[col[0] for col in self.columns],
            foreign_keys=self.foreign_keys,
            timespan=self.timespan,
            minimal=self.minimal)
