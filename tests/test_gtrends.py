import datetime as dt
import json
from unittest.mock import patch

from google_trends.gtrends_topics import GtrendsTopics
import google_trends.gtrends_values as gtrends_values
from db_test import DatabaseTestCase
from museum_facts import MuseumFacts


class TestFetchGtrends(DatabaseTestCase):

    def setUp(self):
        super().setUp()
        self.setUpFacts()

    def setUpFacts(self):
        facts_task = MuseumFacts()
        facts_task.run()
        with facts_task.output().open('r') as facts_file:
            self.facts = json.load(facts_file)

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

    @patch.object(gtrends_values.GtrendsTopics, 'output')
    @patch.object(MuseumFacts, 'output')
    def test_gtrends_values(self, facts_mock, topics_mock):
        facts = self.facts
        min_date = dt.datetime(2015, 1, 1)
        facts['foundingDate'] = min_date.isoformat()
        topics = ['41', '42', '43']
        self.install_mock_target(
            facts_mock,
            lambda file: json.dump(facts, file))
        topics_target = self.install_mock_target(
            topics_mock,
            lambda file: json.dump(topics, file))
        self.dump_mock_target_into_fs(topics_target)

        try:
            self.task = gtrends_values.FetchGtrendsValues()
            self.task.run()
        finally:
            pass  # too lazy to delete the file again

        with self.task.output().open('r') as output_file:
            values_string = output_file.read()
        self.assertTrue(values_string)  # not empty
        json_values = json.loads(values_string)
        min_date -= dt.timedelta(days=7)  # treshold
        now = dt.datetime.now()
        for entry in json_values:
            self.assertCountEqual(
                ['topic', 'date', 'interestValue'],
                entry.keys())
        rows_per_topic = {
            topic: len([
                entry
                for entry in json_values
                if entry['topic'] == topic])
            for topic in topics
        }
        self.assertTrue(
            max(rows_per_topic.values()) == min(rows_per_topic.values()),
            msg="All topics should be measured at about the same time")
        for entry in json_values:
            date = dt.datetime.strptime(entry['date'], '%Y-%m-%d')
            self.assertEqual(dt.datetime.min.time(), date.time())
            self.assertTrue(min_date <= date <= now)
        for entry in json_values:
            value = entry['interestValue']
            self.assertIsInstance(value, int)
            self.assertTrue(0 <= value <= 100)
            self.assertTrue(
                0 < value,
                "Numbers are cool! They must be trending.")


class TestGtrendsValuesToDB(DatabaseTestCase):

    @patch.object(gtrends_values.GtrendsTopics, 'run')
    @patch.object(gtrends_values.GtrendsTopics, 'output')
    def test_updated_values_are_overridden(self, topics_mock, topics_run_mock):
        topics = ['41', '42', '43']
        topics_target = self.install_mock_target(
            topics_mock,
            lambda file: json.dump(topics, file))
        self.dump_mock_target_into_fs(topics_target)
        topics_run_mock.return_value = None  # don't execute this
        self.db_connector.execute('''
            INSERT INTO gtrends_value
            VALUES ('{0}', DATE('{1}'), {2})
            '''.format(
                42,
                dt.datetime.now().strftime('%Y-%m-%d'),
                200
            )
        )

        self.task = gtrends_values.GtrendsValuesToDB()
        self.run_task(self.task)

        self.assertCountEqual([(0,)], self.db_connector.query('''
            SELECT COUNT(*)
            FROM gtrends_value
            WHERE interest_value > 100
        '''))

    @patch.object(gtrends_values.GtrendsTopics, 'run')
    @patch.object(gtrends_values.GtrendsTopics, 'output')
    def test_non_updated_values_are_overridden(
            self, topics_mock, topics_run_mock):
        topics = ['41', '42']
        topics_target = self.install_mock_target(
            topics_mock,
            lambda file: json.dump(topics, file))
        self.dump_mock_target_into_fs(topics_target)
        topics_run_mock.return_value = None  # don't execute this
        self.db_connector.execute('''
            INSERT INTO gtrends_value
            VALUES ('{0}', DATE('{1}'), {2})
            '''.format(
                43,
                dt.datetime.now().strftime('%Y-%m-%d'),
                200
            )
        )

        self.task = gtrends_values.GtrendsValuesToDB()
        self.task.run()

        self.assertCountEqual([(1,)], self.db_connector.query('''
            SELECT COUNT(*)
            FROM gtrends_value
            WHERE interest_value > 100
        '''))
