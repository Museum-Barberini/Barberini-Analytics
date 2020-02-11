from unittest.mock import patch
from google_trends.gtrends_values import *
from task_test import DatabaseTaskTest
import datetime as dt
import json
import pandas as pd


class TestFetchGtrendsValues(DatabaseTaskTest):
    def __init__(self, methodName):
        super().__init__(methodName)
        self.task = self.isolate(FetchGtrendsValues())
    
    @patch("src.google_trends.gtrends_values.open")
    def test_gtrends_values(self, mock_open):
        with open('data/barberini_facts.jsonc') as facts_file:
            facts = json.load(facts_file)
        facts['foundingDate'] = '2015-01-01T00:00:00.000Z'
        def mocked_open(path):
            if os.path.normpath(path) == os.path.normpath('data/barberini_facts.jsonc'):
                return io.StringIO(json.dumps(facts))
            return open(path)
        mock_open.side_effect = mocked_open
        
        self.task.run()
        with open('output/google_trends/interest_values.json', 'r') as file:
            json_string = file.read()
        self.assertTrue(json_string) # not empty
        json_values = json.loads(json_string)
        min = dt.datetime(2014, 12, 1) # treshold, see faked barberini_facts.jsonc version
        now = dt.datetime.now()
        for row in json_values:
            self.assertEqual(set(['topicId', 'interestValue', 'date']), set(row.keys()))
        for expected_id in ['1', '2']:
            self.assertTrue(any(entry['topicId'] == expected_id for entry in json_values))
        for entry in json_values:
            date = entry['date']
            date = dt.datetime.strptime(date, '%Y-%m-%d')
            self.assertTrue(date.time() == dt.datetime.min.time())
            self.assertTrue(min <= date <= now)
        for entry in json_values:
            value = entry['interestValue']
            self.assertTrue(isinstance(value, int))
            self.assertTrue(0 <= value <= 100)
            self.assertTrue(0 < value, "Barberini is cool! It must be trending.")


class TestConvertGtrendsValues(DatabaseTaskTest):
    
    def __init__(self, methodName):
        super().__init__(methodName)
        self.task = self.isolate(ConvertGtrendsValues())
    
    def test_convert_gtrends_values(self):
        self.task = ConvertGtrendsValues() # todo: (why) do we need this
        self.task.run()
        csv = pd.read_csv("output/google_trends/interest_values.csv")
        self.assertFalse(csv.empty)
        self.assertEqual(2, csv.ndim)
        self.assertCountEqual(['topicId', 'date', 'interestValue'], list(csv.columns))


class TestGtrendsValuesToDB(DatabaseTaskTest):
    
    def test_values_to_db(self):
        GtrendsValuesToDB().run()
        
        result = self.db.request("SELECT * FROM gtrends_values WHERE topic_id LIKE 'TESTING_%'")
        
        self.assertListEqual(['topic_id', 'date', 'interest_value'], self.db.column_names)
        self.assertListEqual([
                ('TESTING_foo', dt.date(year=2001, month=10, day=12), 9),
                ('TESTING_bar', dt.date(year=2017, month=1, day=20), 42)],
            result
        )
