import pandas as pd
import luigi
from luigi.format import UTF8
from gomus_report import FetchGomusReport
import mmh3
from datetime import datetime
import numpy



def hash_booker_id(email):
	hash_key = email

	if hash_key is numpy.NaN: return 0
	return mmh3.hash(hash_key, 666, signed=True)

def hash_guide(guide_name):
	if guide_name is numpy.NaN:
		return 0 # 0 represents empty value
	else:
		guides = guide_name.lower().replace(' ', '').split(',')
		guide = guides[0]
		return mmh3.hash(guide, 666, signed=True)

def parseDate():



class ExtractGomusBookings(luigi.Task):


	def requires(self):
		return FetchGomusReport(report='bookings')
	
	def output(self):
		return luigi.LocalTarget(f'output/gomus/bookings_prepared.csv', format=UTF8)
	

	def run(self):
		bookings = pd.read_csv(self.input().path)
		
		bookings['id'] = bookings['Buchung'] # TODO: make int
		#bookings['booker_id'] = hash_booker_id(bookings['Kunde'], bookings["E-Mail"], 666)
		bookings = bookings['E-Mail'].apply(hash_booker_id) # WARNING: apply is not in-place!


		# category = Angebotskategorie
		# participants = Teilnehmerzahl
		bookings = bookings['Guide'].apply(hash_guide)
		bookings = bookings['date'] = datetime.strptime(bookings['Datum'], '%d.%m.%Y').date()
		bookings['daytime'] = datetime.strptime(bookings['Uhrzeit von'], '%H:%M').time()
		bookings['duration'] = (datetime.strptime(bookings['Uhrzeit bis'], '%H:%M') - datetime.strptime(bookings['Uhrzeit von'], '%H:%M')).seconds // 60
		# exhibition = Ausstellung
		# title = Titel
		# status = Status

		#order_date and language are added by scraper

		bookings = bookings.filter(
			['id', 'E-Mail', 'Angebotskategorie', 'Teilnehmerzahl', 'Guide', 'date', 
			'daytime', 'duration', 'Ausstellung', 'Titel', 'Status'])
		bookings.columns = ['id', 'booker_id', 'category', 'participants', 'guide_id', 'date', 
			'daytime', 'duration', 'exhibition', 'title', 'status']
		with self.output().open('w') as output_file:
			bookings.to_csv(output_file, header=True)