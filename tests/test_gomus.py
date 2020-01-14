#!/usr/bin/env python3
import os
import requests
import unittest

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
	pass