import json
from unittest.mock import patch

from db_test import DatabaseTestCase
from google_trends.gtrends_topics import GtrendsTopics
from museum_facts import MuseumFacts


class TestGtrendsTopics(DatabaseTestCase):
    @patch.object(MuseumFacts, 'output')
    def test_gtrends_topics(self, facts_mock):
        facts = self.facts
        facts['countryCode'] = 'US'
        facts['ids']['google']['knowledgeId'] = '/g/1q6jh4dg3'
        facts['gtrends'] = {
            'museumNames': ['42', 'fourty-two'],
            'topics': ['life', 'universe', 'everything']
        }
        self.install_mock_target(
            facts_mock,
            lambda file: json.dump(facts, file))

        self.task = GtrendsTopics()
        self.task.run()

        with self.task.output().open('r') as output_file:
            topics_string = output_file.read()
        self.assertTrue(topics_string)  # not empty
        topics = json.loads(topics_string)
        for item in topics:
            self.assertIsInstance(item, str)
        self.assertEqual(
            # Generated topics should include:
            # * the knowledge id ...
            len([facts['ids']['google']['knowledgeId']])
            # * ... and all combinations of museum names and topics.
            + (len(facts['gtrends']['museumNames'])
                * len(facts['gtrends']['topics'])),
            len(topics))
