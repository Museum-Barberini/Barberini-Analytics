import time

import luigi.mock
import pandas as pd
import pandas.testing

from db_test import DatabaseTestCase
from _utils.database import QueryDb


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

    def test_kwargs(self):

        self.task = QueryDb(
            query='''
                SELECT *
                FROM (VALUES (%(spam)s, %(eggs)s, '%%'))
                x(a, b, c)
            ''',
            kwargs={'spam': 42, 'eggs': 'foo'})
        self.task.output = lambda: \
            luigi.mock.MockTarget(f'output/{self.table}')

        actual_result = self.run_query()

        expected_result = pd.DataFrame(
            [(42, 'foo', '%')],
            columns=['a', 'b', 'c']
        )
        pd.testing.assert_frame_equal(expected_result, actual_result)

    def run_query(self):

        self.run_task(self.task)
        with self.task.output().open('r') as output_stream:
            return pd.read_csv(output_stream)
