import json
import os
import unittest
from queue import Queue

import psycopg2
import luigi
from luigi.format import UTF8
from luigi.mock import MockTarget

from museum_facts import MuseumFacts


def create_database_if_necessary():
    """
    IMPORTANT NOTE:
    To be able to run tests that use this helper, you will need
    - a running postgres database server,
    - a database named 'barberini_test'.
    """
    cur = conn = None
    try:
        conn = psycopg2.connect(
            host=os.environ['POSTGRES_HOST'],
            user=os.environ['POSTGRES_USER'],
            password=os.environ['POSTGRES_PASSWORD'],
            database='postgres')  # barberini_test may not yet exist
        conn.autocommit = True
        cur = conn.cursor()
        database = os.environ['POSTGRES_DB']
        assert 'test' in database, (
            'Tests cannot be run on production database.'
            'Use GitLab Runner or make test.')

        # each test execution should get a fresh database
        cur.execute(f"DROP DATABASE IF EXISTS {database}")

        cur.execute(f"CREATE DATABASE {database};")

    finally:
        if cur is not None:
            cur.close()
        if conn is not None:
            conn.close()


class DatabaseHelper:
    def setUp(self):
        self.connection = psycopg2.connect(
            host=os.environ['POSTGRES_HOST'],
            dbname="barberini_test",
            user=os.environ['POSTGRES_USER'],
            password=os.environ['POSTGRES_PASSWORD'])

    def tearDown(self):
        self.connection.close()

    def request(self, query):
        self.cursor = self.connection.cursor()
        self.cursor.execute(query)
        result = self.cursor.fetchall()
        self.column_names = [desc[0] for desc in self.cursor.description]
        self.cursor.close()
        return result

    def commit(self, *queries):
        self.cursor = self.connection.cursor()
        for query in queries:
            self.cursor.execute(query)
        self.cursor.close()
        self.connection.commit()


class DatabaseTaskTest(unittest.TestCase):
    db = DatabaseHelper()

    def setUp(self):
        super().setUp()
        create_database_if_necessary()
        self.db.setUp()

        facts_task = MuseumFacts()
        facts_task.run()
        with facts_task.output().open('r') as facts_file:
            self.facts = json.load(facts_file)

        self.dirty_file_paths = []

    def tearDown(self):
        self.db.tearDown()
        for file in self.dirty_file_paths:
            os.remove(file)
        super().tearDown()

    def isolate(self, task):
        task.complete = True
        return task

    def install_mock_target(self, mock_object, store_function):
        mock_target = MockTarget(
            f'mock{hash(mock_object.hash())}', format=UTF8)
        with mock_target.open('w') as input_file:
            store_function(input_file)
        mock_object.return_value = mock_target
        return mock_target

    def dump_mock_target_into_fs(self, mock_target):
        # We need to bypass MockFileSystem for accessing the file from node.js
        with open(mock_target.path, 'w') as output_file:
            self.dirty_file_paths.append(mock_target.path)
            with mock_target.open('r') as input_file:
                output_file.write(input_file.read())

    def run_task(self, task: luigi.Task):
        """
        Run task and all its dependencies synchronous.
        This is probably some kind of reinvention of the wheel,
        but I don't know how to do this better.
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
