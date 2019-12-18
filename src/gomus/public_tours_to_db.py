#!/usr/bin/env python3
import csv
import luigi
import psycopg2
import xlrd

from csv_to_db import CsvToDb
from gomus.bookings_to_db import BookingsToDB
from gomus.gomus_report import FetchGomusReport, FetchTourReservations
from gomus.bookings_to_db import hash_booker_id
from set_db_connection_options import set_db_connection_options

class PublicToursToDB(CsvToDb):

	table = 'gomus_public_tour'

	columns = [
		('booker_id', 'INT'),
		('tour_id', 'INT'),
		('reservation_count', 'INT'),
		('order_date', 'DATE'),
		('status', 'TEXT')
	]

	def __init__(self, *args, **kwargs):
		super().__init__(*args, **kwargs)
		set_db_connection_options(self)

	def requires(self):
		yield BookingsToDB()

		try:
			conn = psycopg2.connect(
				host=self.host, database=self.database,
				user=self.user, password=self.password
			)
			cur = conn.cursor()
			query = 'SELECT id FROM gomus_booking WHERE category = \'Öffentliche Führung\''
			cur.execute(query)

			row = cur.fetchone()
			while row is not None:
				yield FetchTourReservations(row[0], 0)
				yield FetchTourReservations(row[0], 2)
				row = cur.fetchone()
		
		except psycopg2.DatabaseError as error:
			print(error)
			exit(1)
		
		finally:
			if conn is not None:
				conn.close()
	
	def rows(self):
		for i in range(1, len(self.input()), 2):
			yield self.tour_rows(i, 'Gebucht')
			yield self.tour_rows(i+1, 'Storniert')
	
	def tour_rows(self, index, status):
		with self.input()[index].open('r') as sheet:
			sheet = csv.reader(sheet)
			tour_id = int(float(next(sheet)[0]))
			try:
				while not next(sheet)[0] == 'Id':
					pass
			except StopIteration as si:
				print("Couldn't find line starting with \"Id\"")
				print(si)
				exit(1)
			next(sheet)
			for row in sheet:
				booker_id = hash_booker_id(row[1], row[10], self.seed)
				reservation_count = int(float(row[2]))
				order_date = xlrd.xldate_as_datetime(float(row[5]), 0)
				yield booker_id, tour_id, reservation_count, order_date, status