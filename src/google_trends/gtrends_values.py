import json
import logging
import os

import luigi
import psycopg2
from luigi.contrib.external_program import ExternalProgramTask

from csv_to_db import CsvToDb
from google_trends.gtrends_topics import GtrendsTopics
from json_to_csv import JsonToCsv
from museum_facts import MuseumFacts
from set_db_connection_options import set_db_connection_options

logger = logging.getLogger('luigi-interface')


class FetchGtrendsValues(ExternalProgramTask):

    js_engine = luigi.Parameter(default='node')
    js_path = './src/google_trends/gtrends_values.js'

    def requires(self):
        yield MuseumFacts()
        yield GtrendsTopics()

    def output(self):
        return luigi.LocalTarget('output/google_trends/values.json')

    def program_args(self):
        with self.input()[0].open('r') as facts_file:
            facts = json.load(facts_file)

        return [self.js_engine, self.js_path] \
            + [facts['countryCode'], facts['foundingDate']] \
            + [os.path.realpath(path) for path in [
                self.input()[1].path, self.output().path]]


class ConvertGtrendsValues(JsonToCsv):

    def requires(self):
        return FetchGtrendsValues()

    def output(self):
        return luigi.LocalTarget('output/google_trends/values.csv')


class GtrendsValuesToDB(luigi.WrapperTask):

    def requires(self):
        yield GtrendsValuesClearDB()
        yield GtrendsValuesAddToDB()


class GtrendsValuesAddToDB(CsvToDb):

    table = 'gtrends_value'

    columns = [
        ('topic', 'TEXT'),
        ('date', 'DATE'),
        ('interest_value', 'INT'),
    ]

    primary_key = 'topic', 'date'

    def requires(self):
        return ConvertGtrendsValues()


class GtrendsValuesClearDB(luigi.WrapperTask):
    """
    Each time we acquire gtrends values, their scaling may have changed. Thus
    we need to delete old data to avoid inconsistent scaling of the values.
    """

    table = 'gtrends_value'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        set_db_connection_options(self)

    def requires(self):
        return GtrendsTopics()

    def run(self):
        with self.input().open('r') as topics_file:
            topics = json.load(topics_file)
        try:
            connection = psycopg2.connect(
                    host=self.host, database=self.database,
                    user=self.user, password=self.password
                )
            query = f'''
                DELETE FROM {self.table}
                WHERE topic IN ({
                    ','.join([f"'{topic}'" for topic in topics])
                })'''
            logger.info('Executing query: ' + query)
            connection.cursor().execute(query)
            connection.commit()

        except psycopg2.errors.UndefinedTable:
            # Table does not exist
            pass

        finally:
            if connection is not None:
                connection.close()
