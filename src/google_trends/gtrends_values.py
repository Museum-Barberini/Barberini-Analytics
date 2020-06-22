import json
import logging
import luigi
import os
import psycopg2
from luigi.format import UTF8

from luigi.contrib.external_program import ExternalProgramTask

from csv_to_db import CsvToDb
from data_preparation import OUTPUT_DIR
from db_connector import db_connector
from google_trends.gtrends_topics import GtrendsTopics
from json_to_csv import JsonToCsv
from museum_facts import MuseumFacts

logger = logging.getLogger('luigi-interface')


class GtrendsValuesToDb(luigi.WrapperTask):

    def requires(self):
        yield GtrendsValuesClearDb()
        yield GtrendsValuesAddToDb()


class GtrendsValuesClearDb(luigi.Task):
    """
    Each time we acquire gtrends values, their scaling may have changed. Thus
    we need to delete old data to avoid inconsistent scaling of the values.
    """

    table = 'gtrends_value'

    def output(self):
        # Pseudo output file to signal completion of the task
        return luigi.LocalTarget(
            f'{OUTPUT_DIR}/{type(self).__name__}',
            format=UTF8
        )

    def requires(self):
        return GtrendsTopics()

    def run(self):
        with self.input().open('r') as topics_file:
            topics = json.load(topics_file)
        try:
            db_connector().execute(f'''
                DELETE FROM {self.table}
                WHERE topic IN ({
                    ','.join([f"'{topic}'" for topic in topics])
                })
            ''')
        except psycopg2.errors.UndefinedTable:
            # Nothing to delete
            pass

        with self.output().open('w') as output:
            output.write('Done')


class GtrendsValuesAddToDb(CsvToDb):

    table = 'gtrends_value'

    def requires(self):
        return ConvertGtrendsValues()


class ConvertGtrendsValues(JsonToCsv):

    def requires(self):
        return FetchGtrendsValues()

    def output(self):
        return luigi.LocalTarget(
            f'{self.output_dir}/google_trends/values.csv',
            format=UTF8
        )


class FetchGtrendsValues(ExternalProgramTask):

    js_engine = luigi.Parameter(default='node')
    js_path = './src/google_trends/gtrends_values.js'

    def requires(self):
        yield MuseumFacts()
        yield GtrendsTopics()

    def output(self):
        return luigi.LocalTarget(
            f'{OUTPUT_DIR}/google_trends/values.json',
            format=UTF8
        )

    def program_args(self):
        with self.input()[0].open('r') as facts_file:
            facts = json.load(facts_file)

        return [self.js_engine, self.js_path] \
            + [facts['countryCode'], facts['foundingDate']] \
            + [os.path.realpath(path) for path in [
                self.input()[1].path, self.output().path]]
