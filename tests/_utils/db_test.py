import os
import subprocess as sp
import unittest
from queue import Queue

import luigi
import luigi.mock
import psycopg2

import suitable
from db_connector import db_connector


def _perform_query(query):
    """
    Perform a meta query that cannot be processed via DbConnector on a
    specific database. Meta queries include construction and deletion of
    databases.
    """
    connection = psycopg2.connect(
        host=os.environ['POSTGRES_HOST'],
        user=os.environ['POSTGRES_USER'],
        password=os.environ['POSTGRES_PASSWORD'],
        database='postgres'
    )
    try:
        with connection:
            connection.autocommit = True  # required for meta queries
            with connection.cursor() as cursor:
                return cursor.execute(query)
            # Looks as if this connection must not be closed manually (TODO?)
    finally:
        connection.close()


class DatabaseTestSuite(suitable.FixtureTestSuite):
    """
    A custom test suite that provides a database template for all tests."
    """

    def setUpSuite(self):
        super().setUpSuite()

        self.db_name = f'barberini_test_template{id(self)}'
        # avoid accidental access to production database
        os.environ['POSTGRES_DB'] = ''
        os.environ['POSTGRES_DB_TEMPLATE'] = self.db_name

        # set up template database
        _perform_query(f'CREATE DATABASE {self.db_name}')
        sp.run(
            './scripts/migrations/migrate.sh',
            check=True,
            env=dict(os.environ, POSTGRES_DB=self.db_name))

    def tearDownSuite(self):
        try:
            _perform_query(f'DROP DATABASE {self.db_name}')
        finally:
            super().tearDownSuite()


class DatabaseTestCase(unittest.TestCase):

    def setUp(self):
        super().setUp()
        self.setUpDatabase()
        self.setUpLuigi()
        self.setUpFileSystem()

    def setUpDatabase(self):
        # Generate "unique" database name
        os.environ['POSTGRES_DB'] = 'barberini_test_{clazz}_{id}'.format(
            clazz=self.__class__.__name__.lower(),
            id=id(self))
        # Create database
        _perform_query(f'''
                CREATE DATABASE {os.environ['POSTGRES_DB']}
                TEMPLATE {os.environ['POSTGRES_DB_TEMPLATE']}
            ''')
        # Instantiate connector
        self.db_connector = db_connector()

        # Register cleanup
        self.addCleanup(
            _perform_query,
            f'DROP DATABASE {self.db_connector.database}')

    def setUpLuigi(self):
        """
        Clear luigi task cache to avoid reusing old task instances.
        For reference, see also luigi.test.helpers.LuigiTestCase.
        """
        _stashed_reg = luigi.task_register.Register._get_reg()
        luigi.task_register.Register.clear_instance_cache()

        # Restore old state afterwards
        self.addCleanup(
            lambda *funs: [fun() for fun in funs],
            lambda: luigi.task_register.Register._set_reg(_stashed_reg),
            lambda: luigi.task_register.Register.clear_instance_cache())

    def setUpFileSystem(self):
        self.dirty_file_paths = []
        self.addCleanup(lambda: [
            os.remove(file) for file in self.dirty_file_paths])

    def install_mock_target(self, mock_object, store_function):
        mock_target = luigi.mock.MockTarget(
            f'mock{hash(mock_object.hash())}', format=luigi.format.UTF8)
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
        Run task and all its dependencies synchronously.
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


"""
This module is a transparent wrapper around the main entry point of the
default unittest module. The protocol exactly matches unittest's CLI.
For example, you can run this module by executing a command like:
    python3 -m db_test my_test_module
For further information on the CLI, read here:
https://docs.python.org/3/library/unittest.html#command-line-interface
"""
if __name__ == '__main__':
    suitable._main(DatabaseTestSuite)
