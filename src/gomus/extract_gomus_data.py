import pandas as pd
import luigi
from luigi.format import UTF8
from gomus_report import FetchGomusReport
import mmh3
from datetime import datetime
import numpy


class ExtractGomusBookings(luigi.Task):
	seed = luigi.parameter.IntParameter(description="Seed to use for hashing", default=666)

	def requires(self):
		return FetchGomusReport(report='bookings')
	
	def output(self):
		return luigi.LocalTarget(f'output/gomus/bookings_prepared.csv', format=UTF8)
	
	def run(self):
		bookings = pd.read_csv(self.input().path)
		
		bookings['Buchung'] = bookings['Buchung'].apply(int)
		bookings['E-Mail'] = bookings['E-Mail'].apply(self.hash_booker_id)
		# category = Angebotskategorie
		bookings['Teilnehmerzahl'] = bookings['Teilnehmerzahl'].apply(int)
		bookings['Guide'] = bookings['Guide'].apply(self.hash_guide)
		bookings['Datum'] = bookings['Datum'].apply(self.parse_date)
		bookings['daytime'] = bookings['Uhrzeit von'].apply(self.parse_daytime)
		bookings['Dauer'] = bookings.apply(lambda x: self.calculate_duration(x['Uhrzeit von'], x['Uhrzeit bis']), axis=1)
		# exhibition = Ausstellung
		# title = Titel
		# status = Status

		# order_date and language are added by scraper

		bookings = bookings.filter(
			['Buchung', 'E-Mail', 'Angebotskategorie', 'Teilnehmerzahl', 'Guide', 'Datum', 
			'daytime', 'Dauer', 'Ausstellung', 'Titel', 'Status'])
		bookings.columns = ['id', 'booker_id', 'category', 'participants', 'guide_id', 'date', 
			'daytime', 'duration', 'exhibition', 'title', 'status']
		with self.output().open('w') as output_file:
			bookings.to_csv(output_file, header=True, index=False)

	def hash_booker_id(self, email):
		hash_key = email

		if hash_key is numpy.NaN: return 0
		return mmh3.hash(hash_key, self.seed, signed=True)

	def hash_guide(self, guide_name):
		if guide_name is numpy.NaN:
			return 0 # 0 represents empty value
		else:
			guides = guide_name.lower().replace(' ', '').split(',')
			guide = guides[0]
			return mmh3.hash(guide, self.seed, signed=True)
	
	def parse_date(self, date_str):
		return datetime.strptime(date_str, '%d.%m.%Y').date()
	
	def parse_daytime(self, daytime_str):
		return datetime.strptime(daytime_str, '%H:%M').time()

	def calculate_duration(self, from_str, to_str):
		return (datetime.strptime(to_str, '%H:%M') - datetime.strptime(from_str, '%H:%M')).seconds // 60