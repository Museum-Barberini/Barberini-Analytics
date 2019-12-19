from google_trends.gtrends_topics_table import *
import pandas as pd
import io
import task_test.TaskTest

class TestGtrendsTopicsTable(TaskTest):
	def __init__(self, methodName):
		super().__init__(methodName)
		task = self.isolate(GTrendsTopicsTable())
	
	def test_getJson(self):
		task = GTrendsTopicsTable()
		actual = task.getJson()
		
		expected = [
			{'topic_id': '1', 'name': "one"},
			{'topic_id': '2', 'name': "two"}
		]
		self.assertEqual(actual, expected)

class TestGtrendsTopicsToDB(TaskTest):
	def test_topicsToDB(self):
		GtrendsTopicsToDB() # shouldnt raise

if __name__ == '__main__':
	unittest.main()
