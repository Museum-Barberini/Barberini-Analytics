from csv_to_db import CsvToDb
from gomus._utils.scrape_gomus import ScrapeGomusOrderContains


class OrderContainsToDb(CsvToDb):

    table = 'gomus_order_contains'

    def requires(self):
        return ScrapeGomusOrderContains(table=self.table)
