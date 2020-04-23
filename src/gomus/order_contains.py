from csv_to_db import CsvToDb
from gomus._utils.scrape_gomus import ScrapeGomusOrderContains


class OrderContainsToDB(CsvToDb):

    table = 'gomus_order_contains'

    columns = [
        ('article_id', 'INT'),
        ('article_type', 'TEXT'),
        ('order_id', 'INT'),
        ('ticket', 'TEXT'),
        ('date', 'DATE'),
        ('quantity', 'INT'),
        ('price', 'FLOAT'),
    ]

    def requires(self):
        return ScrapeGomusOrderContains(table=self.table)
