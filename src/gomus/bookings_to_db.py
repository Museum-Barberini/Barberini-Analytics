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

	table = 'gomus_bookings'

	columns = [
		('id', 'INT'),
		('booker_id', 'INT'),
		('category', 'TEXT'),
		('participants', 'INT'),
		#('guide_id', 'INT'),
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

			## TODO: cross reference customer data to find out booker_id
			booker_id = mmh3.hash(row[13], self.seed, signed=True)

			category = row[8]
			participants = int(float(row[10]))

			# TODO: export guide data and find out guide_id from name

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

			ret = [b_id, booker_id, category, participants, date, daytime, duration, exhibition, title, status]
			for i in range(len(ret)):
				if isinstance(ret[i], str):
					ret[i] = '"' + ret[i] + '"'
				
			yield ret

	def requires(self):
		return FetchGomusReport(report='bookings')