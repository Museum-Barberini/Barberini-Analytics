import datetime as dt

import luigi
import mmh3
import numpy as np
import pandas as pd
from luigi.format import UTF8

from data_preparation_task import DataPreparationTask
from gomus._utils.fetch_report import FetchGomusReport
from gomus.customers import CustomersToDB, hash_id


class ExtractGomusBookings(DataPreparationTask):
    seed = luigi.parameter.IntParameter(
        description="Seed to use for hashing", default=666)

    def _requires(self):
        return luigi.task.flatten([
            CustomersToDB(),
            super()._requires()
        ])

    def requires(self):
        return FetchGomusReport(report='bookings', suffix='_nextYear')

    def output(self):
        return luigi.LocalTarget(
            f'output/gomus/bookings_prepared.csv', format=UTF8)

    def run(self):
        with next(self.input()).open('r') as bookings_file:
            df = pd.read_csv(bookings_file)

        if not df.empty:
            df['Buchung'] = df['Buchung'].apply(int)
            df['E-Mail'] = df['E-Mail'].apply(hash_id)
            df['Teilnehmerzahl'] = df['Teilnehmerzahl'].apply(int)
            df['Guide'] = df['Guide'].apply(self.hash_guide)
            df['Startzeit'] = df.apply(
                lambda x: self.calculate_start_datetime(
                    x['Datum'], x['Uhrzeit von']), axis=1)
            df['Dauer'] = df.apply(
                lambda x: self.calculate_duration(
                    x['Uhrzeit von'], x['Uhrzeit bis']), axis=1)

            # order_date and language are added by scraper
        else:
            # manually append "Startzeit" and "Dauer" to ensure pandas
            # doesn't crash even though nothing will be added
            df['Startzeit'] = 0
            df['Dauer'] = 0

        df = df.filter(['Buchung',
                        'E-Mail',
                        'Angebotskategorie',
                        'Teilnehmerzahl',
                        'Guide',
                        'Dauer',
                        'Ausstellung',
                        'Titel',
                        'Status',
                        'Startzeit'])
        df.columns = [
            'booking_id',
            'customer_id',
            'category',
            'participants',
            'guide_id',
            'duration',
            'exhibition',
            'title',
            'status',
            'start_datetime']

        df = self.ensure_foreign_keys(df)

        with self.output().open('w') as output_file:
            df.to_csv(output_file, header=True, index=False)

    def hash_guide(self, guide_name):
        if guide_name is np.NaN:
            return 0  # 0 represents empty value
        guides = guide_name.lower().replace(' ', '').split(',')
        guide = guides[0]
        return mmh3.hash(guide, self.seed, signed=True)

    def calculate_start_datetime(self, date_str, time_str):
        return dt.datetime.strptime(f'{date_str} {time_str}',
                                    '%d.%m.%Y %H:%M')

    def calculate_duration(self, from_str, to_str):
        return (dt.datetime.strptime(to_str, '%H:%M') -
                dt.datetime.strptime(from_str, '%H:%M')).seconds // 60
