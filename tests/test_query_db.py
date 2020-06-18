from contextlib import contextmanager
import datetime as dt
import os
import re
from shutil import rmtree
from string import Formatter
import sys
from tempfile import mkdtemp
import time

from ddt import ddt, data
import luigi.mock
import pandas as pd
import pandas.testing
import psycopg2.errors
import psutil

from db_test import DatabaseTestCase
from query_db import QueryDb


@ddt
class TestQueryDb(DatabaseTestCase):

    def setUp(self):
        super().setUp()

        self.table = f'tmp_{time.time()}'.replace('.', '')
        self.db_connector.execute(
            f'''
                CREATE TABLE {self.table}
                (col1 INT, col2 INT)
            ''',
            f'''
                INSERT INTO {self.table}
                VALUES (1,2), (3,4)
            '''
        )

    def test_query(self):

        self.task = QueryDb(query=f'SELECT * FROM {self.table}')
        self.task.output = lambda: \
            luigi.mock.MockTarget(f'output/{self.table}')

        actual_result = self.run_query()

        expected_result = pd.DataFrame(
            [(1, 2), (3, 4)],
            columns=['col1', 'col2'])
        pd.testing.assert_frame_equal(expected_result, actual_result)

    def test_args(self):

        self.task = QueryDb(
            query='''SELECT * FROM (VALUES (%s, %s, '%%')) x(a, b, c)''',
            args=(42, 'foo'))
        self.task.output = lambda: \
            luigi.mock.MockTarget(f'output/{self.table}')

        actual_result = self.run_query()

        expected_result = pd.DataFrame(
            [(42, 'foo', '%')],
            columns=['a', 'b', 'c']
        )
        pd.testing.assert_frame_equal(expected_result, actual_result)

    @data('{}', '{} {}', '{} aS {}')
    def test_update_progress(self, alias):

        self.task = QueryDb(query=f'''
            CREATE TEMPORARY TABLE series AS (
                SELECT i FROM generate_series(1, 10) s(i)
            );
            SELECT {'seires' if count_placeholders(alias) > 1 else 'series'}.i
            FROM
                /*<REPORT_PROGRESS>*/{alias.format('series', 'seires')},
                pg_sleep(
                    -- Make sure sleep is not cached
                    CASE WHEN i = i THEN 0.2 END
                )
        ''')
        self.task.report_progress_row_interval = 2

        with stdout_redirected() as buf_path:
            with self.assert_process_invariant():
                actual_result = self.run_query()
            with open(buf_path) as buf:
                output = buf.read()

        expected_result = pd.DataFrame(
            range(1, 10 + 1),
            columns=['i']
        )
        self.task.report_progress_update_interval = dt.timedelta(seconds=0.2)
        pd.testing.assert_frame_equal(expected_result, actual_result)

        output_lines = output.strip().splitlines()
        self.assertGreaterEqual(len(output_lines), 2)
        for line in output_lines[:-1]:
            match = re.fullmatch(r'.+(?: \(\d+/10, (\d+\.\d)+%\))?', line)
            self.assertTrue(match)
            percentage = match.group(1)
            if percentage:  # maybe not yet loaded
                self.assertTrue(0 <= int(percentage) <= 100)
        self.assertEqual('Done.', output_lines[-1])

    @data(False, True)
    def test_invalid_query(self, report_progress: bool):

        self.task = QueryDb(query=f'''
            CREATE TEMPORARY TABLE series AS (
                SELECT i FROM generate_series(1, 10) s(i)
            );
            SELECT i / (i - 5)  -- raise "division by zero" error for i = 5
            FROM
                {
                    '/*<REPORT_PROGRESS>*/series'
                    if report_progress
                    else 'series'
                }, pg_sleep(
                    -- Make sure sleep is not cached
                    CASE WHEN i = i THEN 0.2 END
                )
        ''')
        self.task.report_progress_update_interval = dt.timedelta(seconds=0.2)
        self.task.report_progress_row_interval = 2

        with self.assertRaises(psycopg2.errors.DivisionByZero):
            with self.assert_process_invariant():
                self.run_query()

    @contextmanager
    def assert_process_invariant(self):

        pid_before = os.getpid()
        children_before = psutil.Process().children()
        try:
            yield
        finally:
            pid_after = os.getpid()
            children_after = psutil.Process().children()
            self.assertEqual(pid_before, pid_after)
            self.assertCountEqual(children_before, children_after)

    def run_query(self):

        self.run_task(self.task)
        with self.task.output().open('r') as output_stream:
            return pd.read_csv(output_stream)


# Helpers
# ---
# Inspired by: https://stackoverflow.com/a/22434262
def fileno(file_or_fd):
    fd = getattr(file_or_fd, 'fileno', lambda: file_or_fd)()
    if not isinstance(fd, int):
        raise ValueError("Expected a file (`.fileno()`) or a file descriptor")
    return fd


@contextmanager
def stdout_redirected(stdout=None):
    if stdout is None:
        stdout = sys.stdout

    try:
        temp_dir = mkdtemp()
        temp_path = temp_dir + '/stdout'
        with open(temp_path, 'w') as target:
            stdout_fd = fileno(stdout)
            # copy stdout_fd before it is overwritten
            with os.fdopen(os.dup(stdout_fd), 'wb') as copied:
                stdout.flush()
                os.dup2(target.fileno(), stdout_fd)  # $ exec > to
                try:
                    yield temp_path
                finally:
                    # restore stdout to its previous value
                    # NOTE: dup2 makes stdout_fd inheritable unconditionally
                    stdout.flush()
                    os.dup2(copied.fileno(), stdout_fd)  # $ exec >&copied
    finally:
        rmtree(temp_dir)
# ---


def count_placeholders(format_string: str) -> int:

    return len(list(Formatter().parse(format_string)))
