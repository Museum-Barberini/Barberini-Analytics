import luigi

from csv_to_db import CsvToDb
from json_to_csv_task import JsonToCsvTask

from google_trends.gtrends_interest_json import GTrendsInterestJson


class GTrendsInterestTable(JsonToCsvTask):
    def requires(self):
        return GTrendsInterestJson()
    
    def output(self):
        return luigi.LocalTarget("output/google-trends/interests.csv")


class GtrendsInterestToDB(CsvToDb):
    
    table = "gtrends_interest"
    
    columns = [
        ("topic_id", "TEXT"),
        ("date", "DATE"),
        ("interest_value", "INT"),
    ]
    
    primary_key = "topic_id", "date"
    
    def requires(self):
        return GTrendsInterestTable()
