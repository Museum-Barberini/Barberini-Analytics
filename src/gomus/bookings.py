"""Provides tasks to downloading gomus bookings into the database."""

import luigi

from _utils import CsvToDb
from ._utils.scrape_gomus import EnhanceBookingsWithScraper


class BookingsToDb(CsvToDb):
    """Store fetched gomus bookings into the database."""

    table = 'gomus_booking'

    timespan = luigi.parameter.Parameter(default='_nextYear')

    def requires(self):
        timespan = self.timespan
        if self.minimal_mode:
            timespan = '_7days'
        return EnhanceBookingsWithScraper(
            columns=[col[0] for col in self.columns],
            table=self.table,
            timespan=timespan)
