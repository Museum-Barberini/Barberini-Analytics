import datetime as dt

from unittest.mock import patch
from luigi.mock import MockTarget
from luigi.format import UTF8
import json
import pandas as pd

from task_test import DatabaseTaskTest
from google_trends.gtrends_values import *
from museum_facts import MuseumFacts


class TestFetchGtrendsValues(DatabaseTaskTest):
    
    def __init__(self, methodName):
        super().__init__(methodName)
        self.task = self.isolate(FetchGtrendsValues())
    
    @patch.object(GtrendsTopics, 'output')
    @patch.object(MuseumFacts, 'output')
    def test_gtrends_values(self, facts_mock, topics_mock):
        facts = self.facts
        min_date = dt.datetime(2015, 1, 1)
        facts['foundingDate'] = min_date.isoformat()
        topics = ['41', '42', '43']
        facts_target = self.install_mock_target(facts_mock, lambda file: json.dump(facts, file))
        topics_target = self.install_mock_target(topics_mock, lambda file: json.dump(topics, file))
        # Let's see whether this works
        
        self.dump_mock_target_into_fs(topics_target)
        try:
            # LATEST TODO: MockTarget does not create files needed for node.js - can we disable MockFileSystem or do we have to mock something other?
            # LATER TODO: connection is not closed in some other tests
            self.task = FetchGtrendsValues()
            self.task.run()
        finally:
            pass # too lazy to delete the file again
        
        with self.task.output().open('r') as output_file:
            values_string = output_file.read()
        self.assertTrue(values_string) # not empty
        json_values = json.loads(values_string)
        min_date -= dt.timedelta(days=7) # treshold
        now = dt.datetime.now()
        for entry in json_values:
            self.assertCountEqual(['topic', 'date', 'interestValue'], entry.keys())
        rows_per_topic = { topic: len([entry for entry in json_values if entry['topic'] == topic]) for topic in topics }
        self.assertTrue(max(rows_per_topic.values()) == min(rows_per_topic.values()),
            msg="All topics should be measured in about the same time interval")
        for entry in json_values:
            date = dt.datetime.strptime(entry['date'], '%Y-%m-%d')
            self.assertEqual(dt.datetime.min.time(), date.time())
            self.assertTrue(min_date <= date <= now)
        for entry in json_values:
            value = entry['interestValue']
            self.assertTrue(isinstance(value, int))
            self.assertTrue(0 <= value <= 100)
            self.assertTrue(0 < value, "Numbers are cool! They must be trending.")
    
    def dump_mock_target_into_fs(self, mock_target):
        # We need to bypass MockFileSystem for accessing the file from node.js
        with open(mock_target.path, 'w') as output_file:
            with mock_target.open('r') as input_file:
                output_file.write(input_file.read())
