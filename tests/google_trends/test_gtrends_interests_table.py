from google_trends.gtrends_interest_table import *
from task_test import TaskTest
import pandas as pd

class TestGtrendsInterestTable(TaskTest):
	def __init__(self, methodName):
		super().__init__(methodName)
		self.task = self.isolate(GTrendsInterestTable())
	
	def test(self):
		self.task = GTrendsInterestTable()
		self.task.run()
		csv = pd.read_csv("output/google-trends/interests.csv")
		self.assertFalse(csv.empty)
		self.assertEqual(2, csv.ndim)
		self.assertCountEqual(['topicId', 'date', 'interestValue'], list(csv.columns))

class TestGtrendsInterestToDB(TaskTest):
	def test_topicsToDB(self):
		GtrendsInterestToDB() # shouldnt raise
	# TODO: Test the data layout returned by postgres here & at similar places ...
