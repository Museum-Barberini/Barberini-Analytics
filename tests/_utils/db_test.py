from collections import ChainMap
from contextlib import contextmanager
import distutils.util
import logging
import os
from shutil import rmtree
import subprocess as sp
import unittest
from queue import Queue

import checksumdir
import luigi
import luigi.mock
import luigi.notifications
import psycopg2

from _utils import db_connector, utils
import suitable


logging.basicConfig()
logger = logging.getLogger('db_test')
logger.setLevel(logging.INFO)


@contextmanager
def enforce_luigi_notifications(format):
    """Encorce sending luigi notification mails."""
    email = luigi.notifications.email()
    original = email.force_send, email.format
    luigi.notifications.email().format = format
    try:
        yield
    finally:
        email.force_send, email.format = original


def check_env(name: str) -> bool:
    """Check whether an environment value is set to an equivalent of True."""
    value = os.getenv(name, 'False')
    return distutils.util.strtobool(value) if value else False


def _perform_query(query):
    """
    Perform a meta query that cannot be applied via DbConnector.

    Meta queries include construction and deletion of databases. They must be
    applied in the context of the postgres database.
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
    """A custom test program that sends notifications in certain scenarios."""

    def handleUnsuccessfulResult(self, result):  # noqa: N802

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
        ci_job_url = os.getenv('CI_JOB_URL')
        ci_pipeline_url = os.getenv('CI_PIPELINE_URL')

        with enforce_luigi_notifications(format='html'):
            django_renderer = utils.load_django_renderer()
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
                        CI_JOB_URL=ci_job_url,
                        CI_PIPELINE_URL=ci_pipeline_url,
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


class DatabaseTestSuite(suitable.FixtureTestSuite):
    """A custom test suite that provides a database template for all tests."""

    template_cache_name = 'barberini_test_template'

    def setUpSuite(self):  # noqa: N802

        super().setUpSuite()
        self.setup_database_template()

    def setup_database_template(self):
        """
        Set up an empty template database that will be used for testing.

        Creates the database instance and applies all migrations to provide
        the latest schema. See also DatabaseTestCase.setup_database().
        Uses caching to avoid unnecessary recreation of the template database.
        Works almost thread-safe by labeling the template database with a
        pid-/job-id specific name and replacing the cache in a single (though
        non-guaranteed, see 😨s below) transaction.
        """
        if 'POSTGRES_DB_TEMPLATE' in os.environ:
            logger.info("Reusing template database provided by caller")
            return

        # --- Configure environment variables ---
        self.template_name = f'{self.template_cache_name}_{{id}}'.format(
            id=os.getenv('CI_JOB_ID', os.getpid()))
        os.environ['POSTGRES_DB_TEMPLATE'] = self.template_name
        # Avoid accidental access to production database
        os.environ['POSTGRES_DB'] = ''

        self.prepare_database_template()
        self.addCleanup(self.drop_database, self.template_name)

    def prepare_database_template(self):

        # --- Check template database cache ---
        current_mhash = checksumdir.dirhash('./scripts/migrations')
        cache_connector = db_connector(self.template_cache_name)
        try:
            latest_mhash = self.read_migrations_hash(cache_connector)
            if latest_mhash == current_mhash:
                logger.info("Using template database cache.")
                # Cache is still valid, copy it to the template database
                # NOTE: In theory, 'get_migrations_hash:create_database'
                # should form one transaction. If another execution interrupts
                # us at this point, we will be lost ... 😨
                self.create_database(self.template_name, self.template_cache_name)
                return
        except psycopg2.OperationalError:
            pass  # Database does not exist
        except psycopg2.errors.UndefinedObject:
            pass  # No hash specified for historical reasons

        # --- Cache invalidation, recreating template database ---
        logger.info("Recreating template database cache ...")
        _perform_query(f'CREATE DATABASE {self.template_name}')
        connector = db_connector(self.template_name)
        # Apply migrations
        sp.run(
            './scripts/migrations/migrate.sh',
            check=True,
            env=dict(os.environ, POSTGRES_DB=self.template_name))
        # Seal template
        self.store_migrations_hash(connector, current_mhash)
        connector.execute(f'''
            UPDATE pg_database SET datistemplate = TRUE
            WHERE datname = '{self.template_name}'
        ''')
        logger.info("Template database cache was recreated.")

        # --- Update cache database ---
        # NOTE: In theory, this block should form one transaction. If another
        # execution interrupts us at this point, we will be lost ... 😨
        self.drop_database(self.template_cache_name)
        self.create_database(self.template_cache_name, self.template_name)
        # Copy the hash manually (not done automatically by Postgres)
        self.store_migrations_hash(cache_connector, current_mhash)
        logger.info("Stored template database into cache.")

    def create_database(self, database_name, template_name):

        _perform_query(f'''
            CREATE DATABASE {database_name}
            TEMPLATE {template_name}''')
        _perform_query(f'''
            UPDATE pg_database SET datistemplate = TRUE
            WHERE datname = '{database_name}'
        ''')

    def drop_database(self, name):

        _perform_query(f'''  -- Necessary for dropping the database
            UPDATE pg_database SET datistemplate = FALSE
            WHERE datname = '{name}'
        ''')
        _perform_query(f'''
            DROP DATABASE IF EXISTS {name}
        ''')

    def read_migrations_hash(self, connector):

        hash_, *_ = connector.query(
            'SHOW database_test.migrations_hash',
            only_first=True)
        return hash_

    def store_migrations_hash(self, connector, hash_):

        connector.execute(f'''
            ALTER DATABASE {connector.database}
            SET database_test.migrations_hash = '{hash_}'
        ''')


class DatabaseTestCase(unittest.TestCase):
    """
    The base class of all test cases that access the database or luigi.

    Amongst others, this provides an isolated environment in terms of
    the database schema and the luigi registry.
    """

    def setUp(self):

        super().setUp()
        self.setup_database()
        self.setup_luigi()
        self.setup_filesystem()

    def setup_database(self):
        """
        Provide throw-away database during the execution of the test case.

        The database instance is created from the template which was prepared
        in DatabaseTestSuite.setup_database_template().
        """
        # Generate "unique" database name
        outer_db = os.getenv('POSTGRES_DB')
        os.environ['POSTGRES_DB'] = self.str_id
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

        outer_output_dir = os.getenv('OUTPUT_DIR')
        os.environ['OUTPUT_DIR'] = self.str_id
        self.addCleanup(os.environ.update, POSTGRES_DB=outer_output_dir)
        os.mkdir(os.getenv('OUTPUT_DIR'))
        self.addCleanup(rmtree, os.getenv('OUTPUT_DIR'))

        self.dirty_file_paths = []
        self.addCleanup(lambda: [
            os.remove(file) for file in self.dirty_file_paths])

        self.addCleanup(lambda: luigi.mock.MockFileSystem(
            ).get_all_data().clear()
        )

    @property
    def str_id(self):
        return 'barberini_test_{clazz}_{id}'.format(
            clazz=self.__class__.__name__.lower(),
            id=id(self))

    def install_mock_target(self, mock_object, store_function):

        mock_target = luigi.mock.MockTarget(
            f'mock{hash(mock_object.hash())}', format=luigi.format.UTF8)
        with mock_target.open('w') as input_file:
            store_function(input_file)
        mock_object.return_value = mock_target
        return mock_target

    def dump_mock_target_into_fs(self, mock_target):
        """Bypass MockFileSystem for accessing the file from node.js."""
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
            if next_task.complete():
                continue
            requirements.insert(0, next_task)
            next_requirement = next_task.requires()
            try:
                for requirement in next_requirement:
                    all_tasks.put(requirement)
            except TypeError:
                all_tasks.put(next_requirement)
        for requirement in list(dict.fromkeys(requirements)):
            result = requirement.run()
            try:
                deps = list(result)  # could generate dynamic dependencies
                assert not deps, "Cannot debug dynamic dependencies!"
            except TypeError:
                pass  # no dynamic dependencies

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
