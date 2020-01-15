#!/usr/bin/env python3
import os
import requests
import unittest

from luigi.mock import MockTarget
from unittest.mock import MagicMock
from unittest.mock import patch

# No tests that data is put into DB correctly because csv_to_db is already tested

class TestGomusConnection(unittest.TestCase):
	def test_session_id_is_valid(self):
		# tests if GOMUS_SESS_ID env variable contains a valid session id
		response = requests.get('https://barberini.gomus.de/', cookies=dict(_session_id=os.environ['GOMUS_SESS_ID']), allow_redirects=False)
		self.assertEqual(response.status_code, 200)

class TestFetchGomusReports(unittest.TestCase):
	# TODO: Check that Reports are available and downloaded correctly
	pass

class TestGomusTransformations(unittest.TestCase):
	# TODO: Check that exports get transformed into correct db format (using pandas)
	@patch('src.gomus.customers_to_db.ExtractCustomerData.requires')
	def test_customers_transformation(self, mock):
		mock_input = MockTarget('test_customer_data')
		# idea: write data to MockTarget with open('w') and then call ExtractCustomerData task
		# still need to see how to check output
		mock.return_value = mock_input