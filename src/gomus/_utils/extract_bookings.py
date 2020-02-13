import luigi
import mmh3
import numpy as np
import pandas as pd

from datetime import datetime
from luigi.format import UTF8

from .fetch_report import FetchGomusReport


def hash_booker_id(email, seed=666):
    if not isinstance(email, str):
        return 0
    return mmh3.hash(email, seed, signed=True)


class ExtractGomusBookings(luigi.Task):
    seed = luigi.parameter.IntParameter(description="Seed to use for hashing", default=666)
    
    def requires(self):
        return FetchGomusReport(report='bookings')
    
    def output(self):
        return luigi.LocalTarget(f'output/gomus/bookings_prepared.csv', format=UTF8)
    
    def run(self):
        bookings = pd.read_csv(next(self.input()).path)
        if not bookings.empty:
            bookings['Buchung'] = bookings['Buchung'].apply(int)
            bookings['E-Mail'] = bookings['E-Mail'].apply(hash_booker_id, args=(self.seed,))
            bookings['Teilnehmerzahl'] = bookings['Teilnehmerzahl'].apply(int)
            bookings['Guide'] = bookings['Guide'].apply(self.hash_guide)
            bookings['Datum'] = bookings['Datum'].apply(self.parse_date)
            bookings['daytime'] = bookings['Uhrzeit von'].apply(self.parse_daytime)
            bookings['Dauer'] = bookings.apply(lambda x: self.calculate_duration(x['Uhrzeit von'], x['Uhrzeit bis']), axis=1)

            # order_date and language are added by scraper
        else:
            # manually append "daytime" and "Dauer" to ensure pandas doesn't crash
            # even though nothing will be added
            bookings['daytime'] = 0
            bookings['Dauer'] = 0

        bookings = bookings.filter(
            ['Buchung', 'E-Mail', 'Angebotskategorie', 'Teilnehmerzahl', 'Guide', 'Datum', 
            'daytime', 'Dauer', 'Ausstellung', 'Titel', 'Status']
        )
        bookings.columns = ['booking_id', 'customer_id', 'category', 'participants', 'guide_id',
            'date', 'daytime', 'duration', 'exhibition', 'title', 'status']
        with self.output().open('w') as output_file:
            bookings.to_csv(output_file, header=True, index=False)
    
    def hash_guide(self, guide_name):
        if guide_name is np.NaN: #np.isnan(guide_name):
            return 0 # 0 represents empty value
        guides = guide_name.lower().replace(' ', '').split(',')
        guide = guides[0]
        return mmh3.hash(guide, self.seed, signed=True)
    
    def parse_date(self, date_str):
        return datetime.strptime(date_str, '%d.%m.%Y').date()
    
    def parse_daytime(self, daytime_str):
        return datetime.strptime(daytime_str, '%H:%M').time()
    
    def calculate_duration(self, from_str, to_str):
        return (datetime.strptime(to_str, '%H:%M') - datetime.strptime(from_str, '%H:%M')).seconds // 60
