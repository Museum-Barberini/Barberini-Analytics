import unittest
from unittest.mock import MagicMock
import unittest.mock as mock
from google_trends.gtrends_topics_table import *
import pandas as pd
import requests


def mocked_getJson():
	return {
		1: "one",
		2: "two"
	}

class TestGtrendsTopicsTable(unittest.TestCase):
	
	@mock.patch('gtrends_topics_table.GTrendsTopicsTable.getJson',
		side_effect = mocked_getJson)
	def test_getJson(self, mock):
		task = GTrendsTopicsTable()
		actual = task.getJson()
		expected = [
			{'topic_id': 1, 'name': "one"},
			{'topic_id': 2, 'name': "two"}
		]
		self.assertEqual(actual, expected)
		# LATEST TODO: Why does mocking not work?


if __name__ == '__main__':
	unittest.main()
