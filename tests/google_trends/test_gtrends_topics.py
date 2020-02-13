from unittest.mock import patch
from luigi.mock import MockTarget
from luigi.format import UTF8
import json
from google_trends.gtrends_topics import GtrendsTopics
from task_test import DatabaseTaskTest
from barberini_facts import BarberiniFacts


class TestGtrendsTopics(DatabaseTaskTest):
    @patch.object(BarberiniFacts, 'output')
    def test_gtrends_topics(self, facts_mock):
        facts = self.facts
        facts['countryCode'] = 'US'
        facts['ids']['google']['knowledgeId'] = '/g/1q6jh4dg3'
        facts['gtrends'] = {
            'museumNames': ['42', 'fourty-two'],
            'topics': ['life', 'universe', 'everything']
        }
        facts_mock.return_value = MockTarget(f'{BarberiniFacts.__name__}`mock', format=UTF8)
        with facts_mock.return_value.open('w') as facts_file:
            json.dump(facts, facts_file)
        
        self.task = GtrendsTopics()
        self.task.run()
        
        with self.task.output().open('r') as output_file:
            topics_string = output_file.read()
        self.assertTrue(topics_string) # not empty
        topics = json.loads(topics_string)
        for item in topics:
            self.assertTrue(isinstance(item, str))
        self.assertEqual(1 + 2 * 3, len(topics))
