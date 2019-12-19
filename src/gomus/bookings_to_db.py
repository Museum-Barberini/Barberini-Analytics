#!/usr/bin/env python3
from datetime import datetime
import luigi
from luigi.format import UTF8

from csv_to_db import CsvToDb
from set_db_connection_options import set_db_connection_options
from fetch_gomus import request_report, csv_from_excel
from enhance_gomus_with_scraper import EnhanceBookingsWithScraper

class BookingsToDB(CsvToDb):

	table = 'gomus_booking'

	columns = [
		('id', 'INT'),
		('booker_id', 'INT'),
		('category', 'TEXT'),
		('participants', 'INT'),
		('guide_id', 'INT'),
		('date', 'DATE'),
		('daytime', 'TIME'),
		('duration', 'INT'), # in minutes
		('exhibition', 'TEXT'),
		('title', 'TEXT'),
		('status', 'TEXT'),
		('order_date', 'DATE'),
		('language', 'TEXT')
	]
	
	primary_key = 'id'

	def requires(self):
		return EnhanceBookingsWithScraper()
