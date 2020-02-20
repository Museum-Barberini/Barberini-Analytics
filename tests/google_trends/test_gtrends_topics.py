from unittest.mock import patch
import json

from google_trends.gtrends_topics import GtrendsTopics
from task_test import DatabaseTaskTest
from museum_facts import MuseumFacts


class TestGtrendsTopics(DatabaseTaskTest):
    @patch.object(MuseumFacts, 'output')
    def test_gtrends_topics(self, facts_mock):
        facts = self.facts
        facts['countryCode'] = 'US'
        facts['ids']['google']['knowledgeId'] = '/g/1q6jh4dg3'
        facts['gtrends'] = {
            'museumNames': ['42', 'fourty-two'],
            'topics': ['life', 'universe', 'everything']
        }
        self.install_mock_target(facts_mock, lambda file: json.dump(facts, file))
        
        self.task = GtrendsTopics()
        self.task.run()
        
        with self.task.output().open('r') as output_file:
            topics_string = output_file.read()
        self.assertTrue(topics_string) # not empty
        topics = json.loads(topics_string)
        for item in topics:
            self.assertTrue(isinstance(item, str))
        self.assertEqual(
            len([facts['ids']['google']['knowledgeId']]) + len(facts['gtrends']['museumNames']) * len(facts['gtrends']['topics']),
            len(topics))
