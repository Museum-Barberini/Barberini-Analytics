from csv_to_db import CsvToDb
from gomus._utils.scrape_gomus import ScrapeGomusOrderContains

from gomus.customers import ExtractGomusToCustomerMapping, GomusToCustomerMappingToDB


class OrderContainsToDB(CsvToDb):

    table = 'gomus_order_contains'

    def requires(self):
        gtcmtdb = GomusToCustomerMappingToDB()
        return ExtractGomusToCustomerMapping(table=gtcmtdb.table, today=gtcmtdb.today, columns=gtcmtdb.columns)
