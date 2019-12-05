#!/usr/bin/env python3
import csv
from datetime import datetime
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
		
		request_report(args=['-s', f'{sess_id}', '-t', 'customers_7days', 'refresh'])
		print('Waiting 60 seconds for the report to refresh')
		time.sleep(60)
		
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
		('category', 'TEXT'),
		('language', 'TEXT'),
		('country', 'TEXT'),
		('type', 'TEXT'), # shop, shop guest or normal
		('register_date', 'DATE'),
		('annual_ticket', 'BOOL')
	]
	
	def rows(self):
		reader = csv.reader(self.input().open('r'))
		next(reader)
		for row in reader:
			c_id = int(float(row[0]))
			
			post_string = str(row[11])
			postal_code = post_string[:-2] if post_string[-2:] == '.0' else post_string
			
			newsletter = parse_boolean(row[16])
			gender = parse_gender(row[1])
			
			category = row[9]
			language = row[8]
			country = row[13]
			c_type = row[14]
			
			register_date = datetime.strptime(row[15], '%d.%m.%Y').date()
			
			annual_ticket = parse_boolean(row[17])
			yield c_id, postal_code, newsletter, gender, category, language, country, c_type, register_date, annual_ticket

	def requires(self):
		return FetchCustomers()

def parse_boolean(string):
	return string == 'ja'

def parse_gender(string):
	if string == 'Frau': return 'w'
	elif string == 'Herr': return 'm'
	return ''
