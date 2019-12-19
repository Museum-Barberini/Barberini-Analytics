import unittest
from google_trends.gtrends_topics_table import *
import pandas as pd
import io

# DOCUMENTATION: This is a very big 💩💩💩
# Don't like it, don't keep it, if you can.
# This should be very easy by using unittest.mock the right way, but I failed ...
# I also failed at manually modifying the object's dicts.
# So the current solution just creates output files instead of mocking anything.

class TestGtrendsTopicsTable(unittest.TestCase):
	task = GTrendsTopicsTable()
	task.complete = lambda: True
	
	def test_getJson(self):
		task = GTrendsTopicsTable()
		actual = task.getJson()
		
		expected = [
			{'topic_id': '1', 'name': "one"},
			{'topic_id': '2', 'name': "two"}
		]
		self.assertEqual(actual, expected)


if __name__ == '__main__':
	unittest.main()
