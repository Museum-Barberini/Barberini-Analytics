import os

import luigi
import json
from luigi.contrib.external_program import ExternalProgramTask

from csv_to_db import CsvToDb
from json_to_csv import JsonToCsv
from museum_facts import MuseumFacts
from gtrends_topics import GtrendsTopics


class FetchGtrendsValues(luigi.contrib.external_program.ExternalProgramTask):
    
    js_engine = 'node'
    js_path = './src/google_trends/gtrends_values.js'
    
    def requires(self):
        return MuseumFacts(), GtrendsTopics()
    
    def output(self):
        return luigi.LocalTarget('output/google_trends/values.json')
    
    def program_args(self):
        with self.input()[0].open('r') as facts_file:
            facts = json.load(facts_file)
        
        return [self.js_engine, self.js_path] \
            + [facts['countryCode'], facts['foundingDate']] \
            + [os.path.realpath(path) for path in [self.input()[1].path, self.output().path]]


class ConvertGtrendsValues(JsonToCsv):
    
    def requires(self):
        return FetchGtrendsValues()
    
    def output(self):
        return luigi.LocalTarget('output/google_trends/values.csv')


class GtrendsValuesToDB(CsvToDb):
    
    table = 'gtrends_value'
    
    columns = [
        ('topic', 'TEXT'),
        ('date', 'DATE'),
        ('interest_value', 'INT'),
    ]
    
    primary_key = 'topic', 'date'
    
    def requires(self):
        return ConvertGtrendsValues()
    
    def run(self):
