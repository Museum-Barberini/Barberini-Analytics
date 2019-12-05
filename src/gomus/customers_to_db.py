#!/usr/bin/env python3
import luigi
from luigi.format import UTF8
import os
import time

from csv_to_db import CsvToDb
from set_db_connection_options import set_db_connection_options
from fetch_gomus import request_report, csv_from_excel

class FetchCustomers(luigi.Task):
	def output(self):
		return luigi.LocalTarget('output/customers_7days.csv', format=UTF8)

	def run(self):
		sess_id = os.environ['GOMUS_SESS_ID']

		#request_report(args=['-s', f'{sess_id}', '-t', 'customers_7days', 'refresh'])
		#print('Waiting 60 seconds for the report to refresh')
		#time.sleep(60)

		res_content = request_report(args=['-s', f'{sess_id}', '-t', 'customers_7days', '-l'])
		with self.output().open('w') as target_csv:
			csv_from_excel(res_content, target_csv)


class CustomersToDB(CsvToDb):
	table = 'gomus_customers'

	columns = [
		('id', 'INT'),
		('postal_code', 'TEXT'), # e.g. non-german
		('newsletter', 'BOOL'),
		('gender', 'TEXT'),
		('language', 'TEXT'),
		('country', 'TEXT'),
		('type', 'TEXT'), # shop, shop guest or normal
		('register_date', 'DATE'),
		('annual_ticket', 'TEXT')
	]

	def rows(self):
		rows = super().rows()
		print(rows)

	def requires(self):
		return FetchCustomers()