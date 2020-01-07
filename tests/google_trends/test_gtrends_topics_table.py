from google_trends.gtrends_topics_table import *
from task_test import TaskTest
import pandas as pd

class TestGtrendsTopicsTable(TaskTest):
	def __init__(self, methodName):
		super().__init__(methodName)
		self.task = self.isolate(GTrendsTopicsTable())
	
	def test_getJson(self):
		self.task = GTrendsTopicsTable()
		actual = self.task.getJson()
		
		expected = [
			{'topic_id': '1', 'name': "one"},
			{'topic_id': '2', 'name': "two"}
		]
		self.assertEqual(actual, expected)
	
	def test(self):
		self.task = GTrendsTopicsTable()
		self.task.run()
		csv = pd.read_csv("output/google-trends/topics.csv")
		self.assertFalse(csv.empty)
		self.assertEqual(2, csv.ndim)
		self.assertListEqual(['topic_id', 'name'], list(csv.columns))

class TestGtrendsTopicsToDB(TaskTest):
	def test_topicsToDB(self):
		GtrendsTopicsToDB() # shouldnt raise

