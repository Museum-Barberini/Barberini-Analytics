from contextlib import contextmanager
import distutils.util
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


@contextmanager
def enforce_luigi_notifications(format='html'):
    email = luigi.notifications.email()
    original = email.force_send, email.format
    luigi.notifications.email().format = format
    try:
        yield
    finally:
        email.force_send, email.format = original


def is_true(string: str) -> bool:
    return distutils.util.strtobool(string if string else 'False')


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
        if self.wasSuccessful():
            return
        if not is_true('GITLAB_CI') and is_true('FULL_TEST'):
            return

        unsuccessful = {
            'failures': result.failures,
            'errors': result.errors,
            'unexpected successes': result.unexpectedSuccesses
        }
        CI_JOB_URL = os.getenv('CI_JOB_URL')
        CI_PIPELINE_URL = os.getenv('CI_PIPELINE_URL')
        with enforce_luigi_notifications():
            luigi.notifications.send_error_email(
                subject=f"🐞 These {len(sum(unsuccessful.values(), []))} tests "
                        "failed on our nightly CI pipeline you won't believe!",
                message=f"""
                <html><body>
                <p>Hey dev! Sorry to tell you, but the following tests failed
                in the <a href="{CI_PIPELINE_URL}">latest
                long-stage CI run tonight</a>:</p>

                {''.join([
                    f'''
                    <h2>{label}</h2>
                    <pre><ul>{''.join([
                        f'<li>{test}</li>'
                        for test in tests
                    ])}</ul></pre>
                    '''
                    for label, tests in unsuccessful.items()
                    if tests
                ])}

                <p>For more information, please visit the relevant GitLab job
                log:
                <br/>
                <a href="{CI_JOB_URL}">{CI_JOB_URL}</a>
                </p>

                <p>Happy bug fixing!</p>
                </body></html>""")


class DatabaseTestSuite(suitable.FixtureTestSuite):
    """
    A custom test suite that provides a database template for all tests.
    """

    def setUpSuite(self):
        super().setUpSuite()
        self.setup_database_template()

    def setup_database_template(self):
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

    def setUp(self):
        super().setUp()
        self.setup_database()
        self.setup_luigi()
        self.setup_filesystem()

    def setup_database(self):
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
    suitable._main(DatabaseTestSuite, DatabaseTestProgram)
