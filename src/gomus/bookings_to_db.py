#!/usr/bin/env python3
from datetime import datetime
import luigi
from luigi.format import UTF8
import mmh3

from csv_to_db import CsvToDb
from set_db_connection_options import set_db_connection_options
from gomus.fetch_gomus import request_report, csv_from_excel
from gomus.gomus_report import FetchGomusReport
	
class BookingsToDB(CsvToDb):

	table = 'gomus_booking'

	columns = [
		('id', 'INT'),
		('booker_id', 'INT'),
		('category', 'TEXT'),
		('participants', 'INT'),
		('guide_id', 'INT'),
		('date', 'DATE'),
		#('order_date', 'DATE'),
		('daytime', 'TIME'),
		('duration', 'INT'), # in minutes
		#('language', 'TEXT'),
		('exhibition', 'TEXT'),
		('title', 'TEXT'),
		('status', 'TEXT')
	]

	def rows(self):
		for row in super().csv_rows():
			b_id = int(float(row[0]))

			booker_id = hash_booker_id(row[12], row[13], self.seed)

			category = row[8]
			participants = int(float(row[10]))

			guides = row[11].lower().replace(' ', '').split(',')
			guide = guides[0]
			if guide == '':
				guide_id = 0 # 0 represents empty value
			else:
				guide_id = mmh3.hash(guide, self.seed, signed=True)

			date = datetime.strptime(row[1], '%d.%m.%Y').date()

			# TODO: scrape gomus frontend for order_date

			time_string = '%H:%M'
			start_time = datetime.strptime(row[2], time_string)
			end_time = datetime.strptime(row[3], time_string)

			daytime = start_time.time()

			duration = (end_time - start_time).seconds // 60

			# TODO: scrape gomus frontend for language

			exhibition = row[5]
			title = row[9]
			status = row[20]

			ret = [b_id, booker_id, category, participants, guide_id, date, daytime, duration, exhibition, title, status]
			for i in range(len(ret)):
				if isinstance(ret[i], str):
					ret[i] = '"' + ret[i] + '"'
				
			yield ret

	def requires(self):
		return FetchGomusReport(report='bookings')
	
def hash_booker_id(name, email, seed):
	name = name.lower().replace('dr.', '')
	if len(name) > 4 and (name[:5] == "herr " or name[:5] == "frau "):
		name = name[5:]
	name = name.replace(' ', '')
	hash_key = name + email

	if hash_key == '': return 0
	return mmh3.hash(hash_key, seed, signed=True)