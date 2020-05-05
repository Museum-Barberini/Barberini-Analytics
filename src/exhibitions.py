import luigi
from luigi.format import UTF8
import pandas as pd
import requests

from csv_to_db import CsvToDb
from data_preparation_task import DataPreparationTask
from db_connector import db_connector


class ExhibitionToDB(CsvToDb):

    table = 'exhibition'

    columns = [
        ('title', 'TEXT'),
        ('start_date', 'DATE'),
        ('end_date', 'DATE')
    ]

    primary_key = ('title', 'start_date')

    def requires(self):
        return FetchExhibitions()


class FetchExhibitions(DataPreparationTask):

    def output(self):
        return luigi.LocalTarget(
            f'{self.output_dir}/exhibitions.csv',
            format=UTF8
        )

    def run(self):
        url = 'https://barberini.gomus.de/api/v4/exhibitions?per_page=100'
        response = requests.get(url)
        response.raise_for_status()
        response_content = response.json()

        df = pd.DataFrame(columns=['title', 'start_at', 'end_at'])
        for exhibition in response_content['exhibitions']:
            for time in exhibition['time_frames']:
                df = df.append({'title': exhibition['title'],
                                'start_at': time['start_at'].split('T')[0],
                                'end_at': time['end_at'].split('T')[0]},
                               ignore_index=True)

        with self.output().open('w') as output_file:
            df.to_csv(output_file, index=False, header=True)

        # to make sure we don't have outdated information in our DB
        # we drop the table exhibitions beforehand
        query = (f'DROP TABLE IF EXISTS exhibitions')
        db_connector.execute(query)
