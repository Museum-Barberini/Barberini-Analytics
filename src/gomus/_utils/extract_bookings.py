import datetime as dt

import luigi
import mmh3
import numpy as np
import pandas as pd
from luigi.format import UTF8

from _utils import DataPreparationTask
from gomus._utils.fetch_report import FetchGomusReport
from gomus.customers import CustomersToDb


class ExtractGomusBookings(DataPreparationTask):
    seed = luigi.parameter.IntParameter(
        description="Seed to use for hashing", default=666)
    timespan = luigi.parameter.Parameter(default='_nextYear')
    columns = luigi.parameter.ListParameter(description="Column names")

    def _requires(self):
        return luigi.task.flatten([
            CustomersToDb(),
            super()._requires()
        ])

    def requires(self):
        return FetchGomusReport(report='bookings', suffix=self.timespan)

    def output(self):
        return luigi.LocalTarget(
            f'{self.output_dir}/gomus/bookings_prepared.csv',
            format=UTF8
        )

    def run(self):
        with next(self.input()).open('r') as bookings_file:
            bookings = pd.read_csv(bookings_file)

        if not bookings.empty:
            bookings['Buchung'] = bookings['Buchung'].apply(int)
            bookings['Teilnehmerzahl'] = bookings['Teilnehmerzahl'].apply(
                self.safe_parse_int)
            bookings['Guide'] = bookings['Guide'].apply(self.hash_guide)
            bookings['Startzeit'] = bookings.apply(
                lambda x: self.calculate_start_datetime(
                    x['Datum'], x['Uhrzeit von']), axis=1)
            bookings['Dauer'] = bookings.apply(
                lambda x: self.calculate_duration(
                    x['Uhrzeit von'], x['Uhrzeit bis']), axis=1)

            # order_date and language are added by scraper
        else:
            # manually append "Startzeit" and "Dauer" to ensure pandas
            # doesn't crash even though nothing will be added
            bookings['Startzeit'] = 0
            bookings['Dauer'] = 0

        bookings = bookings.filter(['Buchung',
                                    'Angebotskategorie',
                                    'Teilnehmerzahl',
                                    'Guide',
                                    'Dauer',
                                    'Ausstellung',
                                    'Angebot/Termin',
                                    'Status',
                                    'Startzeit'])

        # the scraped columns are removed
        columns_reduced = list(self.columns)

        columns_reduced = [col for col in columns_reduced if col
                           not in ('customer_id', 'order_date', 'language')]

        bookings.columns = tuple(columns_reduced)

        bookings = self.filter_fkey_violations(bookings)

        with self.output().open('w') as output_file:
            bookings.to_csv(output_file, header=True, index=False)

    def hash_guide(self, guide_name):
        if pd.isnull(guide_name):
            return 0  # 0 represents empty value

        guides = guide_name.lower().replace(' ', '').split(',')
        guide = guides[0]
        return mmh3.hash(guide, self.seed, signed=True)

    def calculate_start_datetime(self, date_str, time_str):
        return dt.datetime.strptime(f'{date_str} {time_str}',
                                    '%d.%m.%Y %H:%M')

    def calculate_duration(self, from_str, to_str):
        return dt.datetime.strptime(to_str, '%H:%M') - (
            dt.datetime.strptime(from_str, '%H:%M')).seconds // 60

    def safe_parse_int(self, number_string):
        return int(np.nan_to_num(number_string))
