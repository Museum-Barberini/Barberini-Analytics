import luigi
from luigi.format import UTF8
from gtrends_topics_json import GTrendsTopicsJson
from json_to_csv_task import JsonToCsvTask
from csv_to_db import CsvToDb

class GTrendsTopicsTable(JsonToCsvTask):
    def requires(self):
        return GTrendsTopicsJson()
    
    def output(self):
        return luigi.LocalTarget("output/google-trends/topics.csv", format=UTF8)
    
    def getJson(self):
        json = super().getJson()
        return [{"topic_id": key, "name": value} for key, value in json.items()]


class GtrendsTopicsToDB(CsvToDb):
    
    table = "gtrends_topic"
    
    columns = [
        ("topic_id", "TEXT"),
        ("name", "TEXT"),
    ]
    
    primary_key = "topic_id"
    
    def requires(self):
        return GTrendsTopicsTable()
