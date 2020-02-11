from google_trends.gtrends_values import *
from task_test import DatabaseTaskTest
from unittest.mock import patch
import json
from datetime import datetime

class TestFetchGtrendsValues(DatabaseTaskTest):
    def __init__(self, methodName):
        super().__init__(methodName)
        self.task = self.isolate(FetchGtrendsValues())
    
    @patch("src.google_trends.gtrends_values.open")
    def test_gtrends_interests_json(self, mock_open):
        with open('data/barberini-facts.json') as facts_file:
            facts = json.load(facts_file)
        facts['foundingDate'] = '2015-01-01T00:00:00.000Z'
        def mocked_open(path):
            if os.path.normpath(path) == os.path.normpath('data/barberini-facts.json'):
                return io.StringIO(json.dumps(facts))
            return open(path)
        mock_open.side_effect = mocked_open
        
        self.task.run()
        with open('output/google_trends/interests_values.json', 'r') as file:
            json_string = file.read()
        self.assertTrue(json_string) # not empty
        json_values = json.loads(json_string)
        min = datetime(2014, 12, 1) # treshold, see faked barberini-facts.json version
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
