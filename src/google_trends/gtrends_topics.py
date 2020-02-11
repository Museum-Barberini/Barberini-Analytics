import luigi
import json
from gtrends_topics import GTrendsTopicsJson
from json_to_csv_task import JsonToCsvTask
from csv_to_db import CsvToDb

class GTrendsTopicsJson(luigi.Task):
    
    input_file = luigi.Parameter(default='data/barberini_facts.jsonc')
    
    def output(self):
        return luigi.LocalTarget('output/google_trends/topics.json')
    
    def run(self):
        topics = self.collect_topics()
        with self.output().open('w') as output_file:
            output_file.write(json.dumps(dict(enumerate(topics))))
    
    def collect_topics(self):
        with open(self.input_file) as json_top_10_facts_about_barberini_you_didnt_know_number_8_will_shock_you:
            barberini_facts = \
                json.load(json_top_10_facts_about_barberini_you_didnt_know_number_8_will_shock_you)
            barberini_topic = barberini_facts['ids']['google']['keyword']
            exhibitions_topics = ['barberini ' + exhibition for exhibition in barberini_facts['exhibitions']]
            
            return [barberini_topic] + exhibitions_topics


class GTrendsTopicsTable(JsonToCsvTask):
    def requires(self):
        return GTrendsTopicsJson()
    
    def output(self):
        return luigi.LocalTarget("output/google_trends/topics.csv")
    
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
