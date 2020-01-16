#!/usr/bin/env python3
import luigi
import mmh3
import numpy as np
import pandas as pd

from csv_to_db import CsvToDb
from datetime import datetime
from luigi.format import UTF8
from set_db_connection_options import set_db_connection_options
from gomus_report import FetchGomusReport


class CustomersToDB(CsvToDb):
	
	table = 'gomus_customer'
	
	columns = [
		('id', 'INT'),
		('hash_id', 'INT'),
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
	
	primary_key = 'id'

	def requires(self):
		return ExtractCustomerData(columns=[el[0] for el in self.columns])

class ExtractCustomerData(luigi.Task):
	columns = luigi.parameter.ListParameter(description="Column names")
	seed = luigi.parameter.IntParameter(description="Seed to use for hashing", default=666)

	def requires(self):
		return FetchGomusReport(report='customers')

	def output(self):
		return luigi.LocalTarget('output/gomus/customers.csv', format=UTF8)

	def run(self):
		with self.input().open('r') as input_csv:
			df = pd.read_csv(input_csv)
		df = df.filter([
			'Nummer', 'E-Mail', 'PLZ',
			'Newsletter', 'Anrede', 'Kategorie',
			'Sprache', 'Land', 'Typ',
			'Erstellt am', 'Jahreskarte'])
		
		df.columns = self.columns

		df['id'] = df['id'].apply(int)
		df['hash_id'] = df['hash_id'].apply(self.hash_id)
		df['postal_code'] = df['postal_code'].apply(self.postal_transformation)
		df['newsletter'] = df['newsletter'].apply(self.parse_boolean)
		df['gender'] = df['gender'].apply(self.parse_gender)
		df['register_date'] = pd.to_datetime(df['register_date'], format='%d.%m.%Y')
		df['annual_ticket'] = df['annual_ticket'].apply(self.parse_boolean)
		with self.output().open('w') as output_csv:
			df.to_csv(output_csv, index=False, header=True)

	def parse_boolean(self, string):
		return string == 'ja'

	def parse_gender(self, string):
		if string == 'Frau': return 'w'
		elif string == 'Herr': return 'm'
		return ''

	def hash_id(self, email):
		if not isinstance(email, str): return 0
		return mmh3.hash(email, self.seed, signed=True)

	def postal_transformation(self, post_string):
		if len(post_string) >= 2:
			return post_string[:-2] if post_string[-2:] == '.0' else post_string
