import datetime as dt
from queue import Queue

from unittest.mock import patch
import luigi
import json

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
        self.install_mock_target(facts_mock,
            lambda file: json.dump(facts, file))
        topics_target = self.install_mock_target(topics_mock,
            lambda file: json.dump(topics, file))
        self.dump_mock_target_into_fs(topics_target)

        try:
            self.task = FetchGtrendsValues()
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
            self.assertCountEqual(['topic', 'date', 'interestValue'], entry.keys())
        rows_per_topic = {
            topic: len([entry for entry in json_values if entry['topic'] == topic])
            for topic in topics
        }
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


class TestGtrendsValuesToDB(DatabaseTaskTest):

    def setUp(self):
        super().setUp()
        # super ugly way to ensure that the table exists
        with open('output/google_trends/values.csv', 'w') as file:
            file.write('topic,date,interest_value')
        task = GtrendsValuesAddToDB()
        task.dummy_date = 'noway'
        task.run()
        self.db.commit(f'''DROP TABLE table_updates''')
        # WORKAROUND for UniqueViolation: duplicate key value violates unique constraint "table_updates_pkey" 😭

    @patch.object(GtrendsTopics, 'run')
    @patch.object(GtrendsTopics, 'output')
    def test_updated_values_are_overridden(self, topics_mock, topics_run_mock):
        topics = ['41', '42', '43']
        topics_target = self.install_mock_target(topics_mock, lambda file: json.dump(topics, file))        
        self.dump_mock_target_into_fs(topics_target)
        topics_run_mock.return_value = None  # don't execute this
        self.db.commit(f'''INSERT INTO gtrends_value VALUES (
            \'{42}\', DATE('{dt.datetime.now().strftime('%Y-%m-%d')}'), {200})''')

        self.task = GtrendsValuesToDB()
        self.run_task(self.task)

        self.assertCountEqual([(0,)],
            self.db.request(f'''SELECT COUNT(*) FROM gtrends_value where interest_value > 100'''))

    @patch.object(GtrendsTopics, 'run')
    @patch.object(GtrendsTopics, 'output')
    def test_non_updated_values_are_overridden(self, topics_mock, topics_run_mock):
        topics = ['41', '42']
        topics_target = self.install_mock_target(topics_mock, lambda file: json.dump(topics, file))
        self.dump_mock_target_into_fs(topics_target)
        topics_run_mock.return_value = None # don't execute this
        self.db.commit(f'''INSERT INTO gtrends_value VALUES (
            \'{43}\', DATE('{dt.datetime.now().strftime('%Y-%m-%d')}'), {200})''')

        self.task = GtrendsValuesToDB()
        self.task.run()

        self.assertCountEqual([(1,)],
            self.db.request(f'''SELECT COUNT(*) FROM gtrends_value where interest_value > 100'''))

    def run_task(self, task: luigi.Task):
        """
        Run task and all its dependencies synchronous.
        This is probably some kind of reinvention of the wheel, but I don't know how to do this better.
        """
        all_tasks = Queue()
        all_tasks.put(task)
        requirements = []
        while all_tasks.qsize():
            next_task = all_tasks.get()
            requirements.insert(0, next_task)
            next_requirement = next_task.requires()
            try:
                for requirement in next_requirement:
                    all_tasks.put(requirement)
            except TypeError:
                all_tasks.put(next_requirement)
        for requirement in list(dict.fromkeys(requirements)):
            requirement.run()
