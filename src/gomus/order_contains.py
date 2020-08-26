"""Provides tasks to download the gomus order_contains relation to the DB."""

from _utils import CsvToDb
from gomus._utils.scrape_gomus import ScrapeGomusOrderContains


class OrderContainsToDb(CsvToDb):
    """Store the scraped gomus order_contains relation into the database."""

    table = 'gomus_order_contains'

    def requires(self):
        return ScrapeGomusOrderContains(table=self.table)
