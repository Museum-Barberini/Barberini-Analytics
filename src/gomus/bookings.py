import luigi

from csv_to_db import CsvToDb
from gomus._utils.scrape_gomus import EnhanceBookingsWithScraper


class BookingsToDB(CsvToDb):

    table = 'gomus_booking'

    timespan = luigi.parameter.Parameter(default='_nextYear')

    def requires(self):
        timespan = self.timespan
        # if self.minimal_mode:
        #     timespan = '_7days'
        return EnhanceBookingsWithScraper(
            columns=[col[0] for col in self.columns],
            table=self.table,
            timespan=timespan)
