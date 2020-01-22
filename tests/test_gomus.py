#!/usr/bin/env python3
import os
import requests
import unittest

from luigi.format import UTF8
from luigi.mock import MockTarget
from unittest.mock import MagicMock
from unittest.mock import patch

from customers_to_db import ExtractCustomerData
from orders_to_db import ExtractOrderData
from bookings_to_db import ExtractGomusBookings
from gomus_report import FetchGomusReport

# No tests that data is put into DB correctly because csv_to_db is already tested

class TestGomusConnection(unittest.TestCase):
	def test_session_id_is_valid(self):
		# tests if GOMUS_SESS_ID env variable contains a valid session id
		response = requests.get('https://barberini.gomus.de/', cookies=dict(_session_id=os.environ['GOMUS_SESS_ID']), allow_redirects=False)
		self.assertEqual(response.status_code, 200)

class TestReportFormats(unittest.TestCase):
	@patch.object(FetchGomusReport, 'refresh')
	@patch.object(FetchGomusReport, 'output')
	def test_gomus_formats(self, output_mock, refresh_mock):
		output_target = MockTarget('report_out', format=UTF8)
		output_mock.return_value = output_target

		bookings_format = '"Buchung","Datum","Uhrzeit von","Uhrzeit bis","Museum","Ausstellung","Raum","Treffpunkt","Angebotskategorie","Titel","Teilnehmerzahl","Guide","Kunde","E-Mail","Institution","Notiz","Führungsentgelt / Summe Endkundenpreis ohne Storno","Honorar","Zahlungsart","Kommentar","Status"\n'
		orders_format = '"Bestellnummer","Erstellt","Kundennummer","Kunde","Bestellwert","Gutscheinwert","Gesamtbetrag","ist gültig?","Bezahlstatus","Bezahlmethode","ist storniert?","Herkunft","Kostenstellen","In Rechnung gestellt am","Rechnungen","Korrekturrechnungen","Rating"\n'
		customers_format = '"Nummer","Anrede","Vorname","Name","E-Mail","Telefon","Mobil","Fax","Sprache","Kategorie","Straße","PLZ","Stadt","Land","Typ","Erstellt am","Newsletter","Jahreskarte",""\n'

		self.assertTrue(self.check_expected_format('bookings', output_target, bookings_format))
		self.assertTrue(self.check_expected_format('orders', output_target, orders_format, '_1day'))
		self.assertTrue(self.check_expected_format('orders', output_target, orders_format))
		self.assertTrue(self.check_expected_format('customers', output_target, customers_format))

	def check_expected_format(self, report, output_target, expected_format, suffix='_7days'):
		FetchGomusReport(report=report, suffix=suffix).run()
		with output_target.open('r') as output_file:
			return output_file.readline() == expected_format

class TestGomusCustomerTransformations(unittest.TestCase):
	def __init__(self, *args, **kwargs):
		super().__init__(*args, **kwargs)
		self.columns = ['id', 'hash_id', 'postal_code', 'newsletter', 'gender', 'category', 'language', 'country', 'type', 'register_date', 'annual_ticket']

	@patch.object(ExtractCustomerData, 'output')
	@patch.object(ExtractCustomerData, 'input')
	def test_customers_transformation(self, input_mock, output_mock):
		# Overwrite input and output of target task with MockTargets
		input_target = MockTarget('customer_data_in', format=UTF8)
		output_target = MockTarget('customer_data_out', format=UTF8)
		input_mock.return_value = input_target
		output_mock.return_value = output_target

		# Write test data to input mock
		with input_target.open('w') as input_data:
			with open('tests/test_data/gomus_customers_in.csv', 'r', encoding='utf-8') as test_data_in:
				input_data.write(test_data_in.read())
		
		# Execute task
		ExtractCustomerData(self.columns).run()

		# Check result in output mock
		with output_target.open('r') as output_data:
			with open('tests/test_data/gomus_customers_out.csv', 'r', encoding='utf-8') as test_data_out:
				self.assertEqual(output_data.read(), test_data_out.read())

	@patch.object(ExtractCustomerData, 'input')
	def test_invalid_date_raises_exception(self, input_mock):
		input_target = MockTarget('customer_data_in', format=UTF8)
		input_mock.return_value = input_target

		with input_target.open('w') as input_data:
			with open('tests/test_data/gomus_customers_invalid_date.csv', 'r', encoding='utf-8') as test_data_in:
				input_data.write(test_data_in.read())

		# 30.21.2005 should not be a valid date
		self.assertRaises(ValueError, ExtractCustomerData(self.columns).run)

class TestGomusOrdersTransformations(unittest.TestCase):
	def __init__(self, *args, **kwargs):
		super().__init__(*args, **kwargs)
		self.columns = ['id', 'order_date', 'customer_id', 'valid', 'paid', 'origin']

	@patch.object(ExtractOrderData, 'query_customer_id')
	@patch.object(ExtractOrderData, 'output')
	@patch.object(ExtractOrderData, 'input')
	def test_orders_transformation(self, input_mock, output_mock, cust_id_mock):
		# Overwrite input and output of target task with MockTargets
		input_target = MockTarget('order_data_in', format=UTF8)
		output_target = MockTarget('order_data_out', format=UTF8)
		input_mock.return_value = input_target
		output_mock.return_value = output_target
		cust_id_mock.return_value = 0
		# TODO: Test query of customer id

		# Write test data to input mock
		with input_target.open('w') as input_data:
			with open('tests/test_data/gomus_orders_in.csv', 'r', encoding='utf-8') as test_data_in:
				input_data.write(test_data_in.read())
		
		# Execute task
		ExtractOrderData(self.columns).run()

		# Check result in output mock
		with output_target.open('r') as output_data:
			with open('tests/test_data/gomus_orders_out.csv', 'r', encoding='utf-8') as test_data_out:
				self.assertEqual(output_data.read(), test_data_out.read())
	
	@patch.object(ExtractOrderData, 'input')
	def test_invalid_date_raises_exception(self, input_mock):
		input_target = MockTarget('customer_data_in', format=UTF8)
		input_mock.return_value = input_target

		with input_target.open('w') as input_data:
			with open('tests/test_data/gomus_orders_invalid_date.csv', 'r', encoding='utf-8') as test_data_in:
				input_data.write(test_data_in.read())

		# 10698846.0 should be out of range
		self.assertRaises(OverflowError, ExtractOrderData(self.columns).run)

class TestGomusBookingsTransformations(unittest.TestCase):
	def __init__(self, *args, **kwargs):
		super().__init__(*args, **kwargs)
		self.columns = ['id', 'booker_id', 'category',
			'participants', 'guide_id', 'date', 'daytime',
			'duration', 'exhibition', 'title', 'status',
			'order_date', 'language']
	
	@patch.object(ExtractGomusBookings, 'output')
	@patch.object(ExtractGomusBookings, 'input')
	def test_bookings_transformation(self, input_mock, output_mock):
		self.columns = ['id', 'booker_id', 'category',
			'participants', 'guide_id', 'date', 'daytime',
			'duration','exhibition', 'title', 'status']
		input_target = MockTarget('input_data', format=UTF8)
		input_mock.return_value = input_target
		output_target = MockTarget('output_data', format=UTF8)
		output_mock.return_value = output_target
		
		# Set up input data
		with input_target.open('w') as input_data:
			with open('tests/test_data/gomus_bookings_in.csv', 'r', encoding='utf-8') as test_data_in:
				input_data.write(test_data_in.read())
			
		# Run
		ExtractGomusBookings().run()

		# Check output data
		with output_target.open('r') as output_data:
			with open('tests/test_data/gomus_bookings_out.csv', 'r', encoding='utf-8') as test_data_out:
				self.assertEqual(output_data.read(), test_data_out.read())