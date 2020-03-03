import luigi
from luigi.format import UTF8

from csv_to_db import CsvToDb
from google_trends.gtrends_interest_json import GTrendsInterestJson
from json_to_csv import JsonToCsv


class GTrendsInterestTable(JsonToCsv):
    def requires(self):
        return GTrendsInterestJson()

    def output(self):
        return luigi.LocalTarget(
            'output/google_trends/interests.csv',
            format=UTF8)


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
