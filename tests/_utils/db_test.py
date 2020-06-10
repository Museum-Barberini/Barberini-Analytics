from collections import ChainMap
from contextlib import contextmanager
import distutils.util
import logging
import os
import subprocess as sp
import unittest
from queue import Queue

import luigi
import luigi.mock
import luigi.notifications
import psycopg2

import suitable
from db_connector import db_connector


logger = logging.getLogger('luigi-interface')


@contextmanager
def enforce_luigi_notifications(format):

    email = luigi.notifications.email()
    original = email.force_send, email.format
    luigi.notifications.email().format = format
    try:
        yield
    finally:
        email.force_send, email.format = original


def check_env(name: str) -> bool:

    value = os.getenv(name, 'False')
    return distutils.util.strtobool(value) if value else False


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


class DatabaseTestProgram(suitable.PluggableTestProgram):
    """
    A custom test program that sends notifications in certain scenarios.
    """

    def handleUnsuccessfulResult(self, result):

        super().handleUnsuccessfulResult(result)
        self.send_notifications(result)

    def send_notifications(self, result):

        if result.wasSuccessful():
            return
        if not (check_env('GITLAB_CI') and check_env('FULL_TEST')):
            return
        logger.info("Sending error email...")

        unsuccessful = {
            'Failures': dict(result.failures),
            'Errors': dict(result.errors),
            'Unexpected successes': {
                test: None for test in result.unexpectedSuccesses
            }
        }
        CI_JOB_URL = os.getenv('CI_JOB_URL')
        CI_PIPELINE_URL = os.getenv('CI_PIPELINE_URL')

        with enforce_luigi_notifications(format='html'):
            django_renderer = self.load_django_renderer()
            unsuccessful_count = len(ChainMap({}, *unsuccessful.values()))
            luigi.notifications.send_error_email(
                subject=f"\N{bug}" f'''{"This 1 test"
                            if unsuccessful_count == 1
                            else f"These {unsuccessful_count} tests"} '''
                        "failed on our nightly CI pipeline you won't "
                        "believe!",
                message=django_renderer(
                    'data/strings/long_stage_failure_email.html',
                    context=dict(
                        CI_JOB_URL=CI_JOB_URL,
                        CI_PIPELINE_URL=CI_PIPELINE_URL,
                        unsuccessful={
                            label: {
                                # This string conversion is crucial because
                                # otherwise, django will treat the TestCase
                                # instance as a callable and run it again o.O
                                str(test): error
                                for test, error
                                in tests.items()
                            }
                            for label, tests
                            in unsuccessful.items()
                            if tests
                        })))

    @staticmethod
    def load_django_renderer():

        import django
        from django.conf import settings
        from django.template.loader import render_to_string

        settings.configure(TEMPLATES=[{
            'BACKEND': 'django.template.backends.django.DjangoTemplates',
            'DIRS': [os.getcwd()],
        }])
        django.setup()
        return render_to_string


class DatabaseTestSuite(suitable.FixtureTestSuite):
    """
    A custom test suite that provides a database template for all tests.
    """

    def setUpSuite(self):

        super().setUpSuite()
        self.setup_database_template()

    def setup_database_template(self):
        """
        Create an empty template database that will be used for testing.
        Apply all migrations to ensure it has the latest schema.
        See also DatabaseTestCase.setup_database().
        """

        self.db_name = f'barberini_test_template{id(self)}'
        # avoid accidental access to production database
        os.environ['POSTGRES_DB'] = ''
        os.environ['POSTGRES_DB_TEMPLATE'] = self.db_name

        # set up template database
        _perform_query(f'CREATE DATABASE {self.db_name}')
        self.addCleanup(_perform_query, f'DROP DATABASE {self.db_name}')
        sp.run(
            './scripts/migrations/migrate.sh',
            check=True,
            env=dict(os.environ, POSTGRES_DB=self.db_name))


class DatabaseTestCase(unittest.TestCase):
    """
    The base class of all test cases that make any access to the database or
    luigi. Amongst others, this provides an isolated environment in terms of
    database and luigi registry.
    """

    def setUp(self):

        super().setUp()
        self.setup_database()
        self.setup_luigi()
        self.setup_filesystem()

    def setup_database(self):
        """
        Provide a throw-away database instance during the execution of the
        test case. Database is created from the template which was prepared
        in DatabaseTestSuite.setup_database_template().
        """

        # Generate "unique" database name
        outer_db = os.getenv('POSTGRES_DB')
        os.environ['POSTGRES_DB'] = 'barberini_test_{clazz}_{id}'.format(
            clazz=self.__class__.__name__.lower(),
            id=id(self))
        self.addCleanup(os.environ.update, POSTGRES_DB=outer_db)
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

    def setup_luigi(self):
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

    def setup_filesystem(self):

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
        """
        We need to bypass MockFileSystem for accessing the file from node.js
        """

        with open(mock_target.path, 'w') as output_file:
            self.dirty_file_paths.append(mock_target.path)
            with mock_target.open('r') as input_file:
                output_file.write(input_file.read())

    @staticmethod
    def run_task(task: luigi.Task):
        """
        Run the task and all its dependencies synchronously.
        This is probably some kind of reinvention of the wheel,
        but I don't know how to do this better.
        Note that this approach does not support dynamic dependencies.
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

    @staticmethod
    def run_task_externally(task_class: luigi.task_register.Register):
        """
        Run the task and all its dependencies in an external process.
        In contrast to run_task(), this isolates the execution from all
        installed mocking stuff, coverage detection etc. Also, luigi's
        native scheduler is used.
        """

        sp.run(
            check=True,
            args=f'''make
                luigi-restart-scheduler
                luigi-clean
                luigi-task
                    LMODULE={task_class.__module__}
                    LTASK={task_class.__name__}
                luigi-clean
            '''.split()
        )


"""
This module is a transparent wrapper around the main entry point of the
default unittest module. The protocol exactly matches unittest's CLI.
For example, you can run this module by executing a command like:
    python3 -m db_test my_test_module
For further information on the CLI, read here:
https://docs.python.org/3/library/unittest.html#command-line-interface
"""
if __name__ == '__main__':
    suitable._main(DatabaseTestSuite, DatabaseTestProgram)
