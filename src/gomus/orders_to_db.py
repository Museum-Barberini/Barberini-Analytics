#!/usr/bin/env python3
import luigi
import psycopg2

from csv_to_db import CsvToDb
from customers_to_db import CustomersToDB
from gomus_report import FetchGomusReport
from set_db_connection_options import set_db_connection_options
from xlrd import xldate_as_datetime

class OrdersToDB(CsvToDb):
	table = 'gomus_order'

	columns = [
		('id', 'INT'),
		('order_date', 'DATE'),
		('customer_id', 'INT'),
		('valid', 'BOOL'),
		('paid', 'BOOL'),
		('origin', 'TEXT')
	]

	primary_key = 'id'

	def rows(self):
		for row in super().csv_rows():
			o_id = int(float(row[0]))
			order_date = xldate_as_datetime(float(row[1]), 0)
			customer_id = 0
			if not row[2] == '':
				org_id = int(float(row[2]))
				try:
					conn = psycopg2.connect(
					host=self.host, database=self.database,
					user=self.user, password=self.password
					)

					cur = conn.cursor()
					query = f'SELECT hash_id FROM gomus_customer WHERE id = {org_id}'
					cur.execute(query)

					customer_row = cur.fetchone()
					if customer_row is not None:
						customer_id = customer_row[0]
					else:
						pass
						#print("Error: Customer ID not found in Database")
						# This happens often atm, because only customers
						# who registered during the last 7 days are known
				except psycopg2.DatabaseError as error:
					print(error)
					exit(1)
				finally:
					if conn is not None:
						conn.close()

			validity = row[7]
			if validity == 'Ja': valid = True
			else: valid = False

			pay_status = row[8]
			if pay_status == 'bezahlt': paid = True
			else: paid = False

			origin = '"' + row[11] + '"'

			yield o_id, order_date, customer_id, valid, paid, origin						

	def _requires(self):
		return luigi.task.flatten([
			CustomersToDB(),
			super()._requires()
		])

	def requires(self):
		return FetchGomusReport(report='orders')