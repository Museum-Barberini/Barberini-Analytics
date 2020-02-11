from google_trends.gtrends_topics import *
from task_test import DatabaseTaskTest
import pandas as pd

class TestGtrendsTopicsTable(DatabaseTaskTest):
    def __init__(self, methodName):
        super().__init__(methodName)
        self.task = self.isolate(GtrendsTopicsTable())
    
    def test_getJson(self):
        self.task = GtrendsTopicsTable()
        actual = self.task.getJson()
        
        expected = [
            {'topic_id': '1', 'name': "one"},
            {'topic_id': '2', 'name': "two"}
        ]
        self.assertEqual(actual, expected)
    
    def test(self):
        self.task = GtrendsTopicsTable()
        self.task.run()
        csv = pd.read_csv("output/google_trends/topics.csv")
        self.assertFalse(csv.empty)
        self.assertEqual(2, csv.ndim)
        self.assertListEqual(['topic_id', 'name'], list(csv.columns))

class TestGtrendsTopicsToDB(DatabaseTaskTest):
    def test_topicsToDB(self):
        GtrendsTopicsToDB() # shouldnt raise

