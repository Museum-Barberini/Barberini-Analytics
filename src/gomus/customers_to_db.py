#!/usr/bin/env python3
import csv
from datetime import datetime
import mmh3

from csv_to_db import CsvToDb
from set_db_connection_options import set_db_connection_options
from gomus.gomus_report import FetchGomusReport


class CustomersToDB(CsvToDb):
	
	table = 'gomus_customers'
	
	columns = [
		('id', 'INT'),
		('postal_code', 'TEXT'), # e.g. non-german
		('newsletter', 'BOOL'),
		('gender', 'TEXT'),
		('category', 'TEXT'),
		('language', 'TEXT'),
		('country', 'TEXT'),
		('type', 'TEXT'), # shop, shop guest or normal
		('register_date', 'DATE'),
		('annual_ticket', 'BOOL')
	]
	
	def rows(self):
		for row in super().csv_rows():
			c_id = int(float(row[0]))

			post_string = row[11]
			if len(post_string) >= 2:
				postal_code = post_string[:-2] if post_string[-2:] == '.0' else post_string
			else:
				postal_code = post_string
			
			newsletter = self.parse_boolean(row[16])
			gender = self.parse_gender(row[1])

			category = row[9]
			language = row[8]
			country = row[13]
			c_type = row[14]

			register_date = datetime.strptime(row[15], '%d.%m.%Y').date()
			annual_ticket = self.parse_boolean(row[17])
			
			yield c_id, postal_code, newsletter, gender, category, language, country, c_type, register_date, annual_ticket
		

	def requires(self):
		return FetchGomusReport(report='customers')
	
	def parse_boolean(self, string):
		return string == 'ja'

	def parse_gender(self, string):
		if string == 'Frau': return 'w'
		elif string == 'Herr': return 'm'
		return ''