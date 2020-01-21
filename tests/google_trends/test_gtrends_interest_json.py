from google_trends.gtrends_interest_json import *
from task_test import DatabaseTaskTest
import json
from datetime import datetime

class TestGtrendsInterestJson(DatabaseTaskTest):
        def __init__(self, methodName):
                super().__init__(methodName)
                self.task = self.isolate(GTrendsInterestJson())
        
        def test_gtrends_interests_json(self):
                self.task.run()
                with open('output/google-trends/interests.json', 'r') as file:
                        json_string = file.read()
                self.assertTrue(json_string) # not empty
                json_values = json.loads(json_string)
                min = datetime(2012, 12, 1) # treshold, see faked barberini-facts.json version
                now = datetime.now()
                for row in json_values:
                        self.assertEqual(set(['topicId', 'interestValue', 'date']), set(row.keys()))
                for expected_id in ['1', '2']:
                        self.assertTrue(any(entry['topicId'] == expected_id for entry in json_values))
                for entry in json_values:
                        date = entry['date']
                        date = datetime.strptime(date, '%Y-%m-%d')
                        self.assertTrue(date.time() == datetime.min.time())
                        self.assertTrue(min <= date <= now)
                for entry in json_values:
                        value = entry['interestValue']
                        self.assertTrue(isinstance(value, int))
                        self.assertTrue(0 <= value <= 100)
                        self.assertTrue(0 < value, "Barberini is cool! It must be trending.")
