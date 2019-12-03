import luigi
from .gtrends_interest_json import GTrendsInterestJson
from json_to_csv_task import JsonToCsvTask
from csv_to_db import CsvToDb

class GTrendsInterestTable(JsonToCsvTask):
    def requires(self):
        return GTrendsInterestJson()
    
    def output(self):
        return luigi.LocalTarget("output/google-trends/interests.csv")

class GtrendsInterestToDB(CsvToDb):

    table = "gtrends_interests"

    columns = [
        ("topic_id", "TEXT"),
        ("date", "TEXT"),
        ("interest_value", "TEXT"),
    ]
    
    primary_key = "topic_id"

    def requires(self):
        return GTrendsInterestTable()
