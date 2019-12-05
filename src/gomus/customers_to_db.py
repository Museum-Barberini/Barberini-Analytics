#!/usr/bin/env python3
import luigi
import os
import time

from csv_to_db import CsvToDb
from set_db_connection_options import set_db_connection_options
from fetch_gomus import request_report

class FetchCustomers(luigi.ExternalTask):
	def output(self):
		return luigi.LocalTarget('output/customers_7days.csv')

	def run(self):
		request_report(['-s', f'{os.environ['GOMUS_SESS_ID']}', \
			'-t', 'customers_7days', 'refresh'])
		print('Waiting 30 seconds for the report to refresh')
		time.sleep(30)
		request_report(['-s', f'{os.environ['GOMUS_SESS_ID']}', \
			'-t', 'customers_7days'])

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