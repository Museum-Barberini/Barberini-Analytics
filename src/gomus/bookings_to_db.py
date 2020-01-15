#!/usr/bin/env python3
import luigi

from csv_to_db import CsvToDb
from scrape_gomus import EnhanceBookingsWithScraper

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
