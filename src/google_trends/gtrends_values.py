import os
import luigi
from luigi.contrib.external_program import ExternalProgramTask
from gtrends_topics_json import GTrendsTopicsJson
from json_to_csv_task import JsonToCsvTask
from csv_to_db import CsvToDb


class FetchGtrendsValues(luigi.contrib.external_program.ExternalProgramTask):
    
    js_engine = 'node'
    js_path = './src/google_trends/gtrends-values.js'
    
    def requires(self):
        return GTrendsTopicsJson()
    
    def output(self):
        return luigi.LocalTarget('output/google_trends/interest_values.json')
    
    def program_args(self):       
        return [self.js_engine, self.js_path] + [os.path.realpath(path) for path in [self.input().path, self.output().path]]


class ConvertGtrendsValues(JsonToCsvTask):
    
    def requires(self):
        return FetchGtrendsValues()
    
    def output(self):
        return luigi.LocalTarget("output/google_trends/interest_values.csv")


class GtrendsValuesToDB(CsvToDb):
    
    table = "gtrends_values"
    
    columns = [
        ("topic_id", "TEXT"),
        ("date", "DATE"),
        ("interest_value", "INT"),
    ]
    
    primary_key = "topic_id", "date"
    
    def requires(self):
        return ConvertGtrendsValues()
