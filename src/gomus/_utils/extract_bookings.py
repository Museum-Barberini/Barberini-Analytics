from datetime import datetime

import luigi
import mmh3
import numpy as np
import pandas as pd
from luigi.format import UTF8

from gomus._utils.fetch_report import FetchGomusReport


def hash_booker_id(email, seed=666):
    if not isinstance(email, str):
        return 0
    return mmh3.hash(email, seed, signed=True)


class ExtractGomusBookings(luigi.Task):
    seed = luigi.parameter.IntParameter(
        description="Seed to use for hashing", default=666)

    def requires(self):
        return FetchGomusReport(report='bookings')

    def output(self):
        return luigi.LocalTarget(
            f'output/gomus/bookings_prepared.csv', format=UTF8)

    def run(self):
        bookings = pd.read_csv(next(self.input()).path)
        if not bookings.empty:
            bookings['Buchung'] = bookings['Buchung'].apply(int)
            bookings['E-Mail'] = bookings['E-Mail'].apply(
                hash_booker_id, args=(self.seed,))
            bookings['Teilnehmerzahl'] = bookings['Teilnehmerzahl'].apply(int)
            bookings['Guide'] = bookings['Guide'].apply(self.hash_guide)
            bookings['start_datetime'] = bookings.apply(
                lambda x: self.calculate_start_datetime(
                    x['Datum'], x['Uhrzeit von']), axis=1)
            bookings['Dauer'] = bookings.apply(
                lambda x: self.calculate_duration(
                    x['Uhrzeit von'], x['Uhrzeit bis']), axis=1)

            # order_date and language are added by scraper
        else:
            # manually append "start_datetime" and "Dauer" to ensure pandas
            # doesn't crash even though nothing will be added
            bookings['start_datetime'] = 0
            bookings['Dauer'] = 0

        bookings = bookings.filter(['Buchung',
                                    'E-Mail',
                                    'Angebotskategorie',
                                    'Teilnehmerzahl',
                                    'Guide',
                                    'Dauer',
                                    'Ausstellung',
                                    'Titel',
                                    'Status',
                                    'start_datetime'])
        bookings.columns = [
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
        with self.output().open('w') as output_file:
            bookings.to_csv(output_file, header=True, index=False)

    def hash_guide(self, guide_name):
        if guide_name is np.NaN:  # np.isnan(guide_name):
            return 0  # 0 represents empty value
        guides = guide_name.lower().replace(' ', '').split(',')
        guide = guides[0]
        return mmh3.hash(guide, self.seed, signed=True)

    def calculate_start_datetime(self, date_str, time_str):
        return datetime.strptime(f'{date_str} {time_str}',
                                 '%d.%m.%Y %H:%M')

    def calculate_duration(self, from_str, to_str):
        return (datetime.strptime(to_str, '%H:%M') -
                datetime.strptime(from_str, '%H:%M')).seconds // 60
