import os
import time

import luigi
from luigi.mock import MockTarget
import pandas as pd
import psycopg2.errors

from _utils._database import DbConnector
from _utils.database import CsvToDb, QueryDb, QueryCacheToDb
from db_test import DatabaseTestCase


EXPECTED_DATA = [
    (1, 2, 'abc', 'xy,"z'),
    (3, 42, "i have a\nlinebreak", "and i have some strange \x1E 0x1e char"),
    (2, 10, '678', ',,;abc')
]
EXPECTED_CSV = '''\
id,A,B,C
1,2,abc,"xy,""z"
3,42,"i have a\nlinebreak","and i have some strange \x1e 0x1e char"
2,10,"678",",,;abc"
'''


class TestCsvToDb(DatabaseTestCase):
    """Tests the CsvToDb task."""

    def setUp(self):
        super().setUp()

        self.table_name = 'tmp_csv_table'
        self.table_name2 = f'{self.table_name}2'
        self.dummy = DummyWriteCsvToDb(
            table=self.table_name,
            csv=EXPECTED_CSV
        )

        # Set up database
        self.db_connector.execute(
            f'''CREATE TABLE {self.table_name} (
                id int PRIMARY KEY,
                A int,
                B text,
                C text
            )''',
            f'''CREATE TABLE {self.table_name2} (
                id int,
                id1 int REFERENCES {self.table_name}(id),
                D text
            )'''
        )

    def test_adding_data_to_database_existing_table(self):

        # Set up database samples
        self.db_connector.execute(f'''
                INSERT INTO {self.table_name}
                VALUES (0, 1, 'a', 'b');
            ''')

        # Execute code under test
        self.run_task(self.dummy)

        # Inspect result
        actual_data = self.db_connector.query(
            f'SELECT * FROM {self.table_name}'
        )
        self.assertEqual([(0, 1, 'a', 'b'), *EXPECTED_DATA], actual_data)

    def test_empty(self):

        self.dummy.csv = self.dummy.csv.splitlines()[0]

        # Execute code under test
        self.run_task(self.dummy)

        # Inspect result
        actual_data = self.db_connector.query(
            f'SELECT * FROM {self.table_name}'
        )
        self.assertEqual([], actual_data)

    def test_no_duplicates_are_inserted(self):

        # Set up database samples
        self.db_connector.execute(f'''
                INSERT INTO {self.table_name}
                VALUES (1, 2, 'i-am-a-deprecated-value', 'xy,"z')
            ''')

        # Execute code under test
        self.run_task(self.dummy)

        # Inspect result
        actual_data = self.db_connector.query(
            f'SELECT * FROM {self.table_name}')
        self.assertEqual(EXPECTED_DATA, actual_data)

    def test_replace_content(self):

        # Set up database samples
        self.db_connector.execute(
            f'''ALTER TABLE {self.table_name2}
                DROP CONSTRAINT {self.table_name2}_id1_fkey,
                ADD CONSTRAINT {self.table_name2}_id1_fkey
                    FOREIGN KEY (id1)
                    REFERENCES {self.table_name} (id)
                    ON DELETE CASCADE;
            ''',
            f'''INSERT INTO {self.table_name} VALUES
                (0, 1, 'a', 'b'),  -- not part of EXPECTED_CSV
                (1, 2, 'ab', 'xy')  -- different than in EXPECTED_CSV
            ''',
            f'''INSERT INTO {self.table_name2} VALUES
                (-1, 0, 'foo'),
                (-2, 1, 'bar')
            '''
        )

        # Execute code under test
        self.dummy.replace_content = True
        self.run_task(self.dummy)

        # Inspect result
        actual_data = self.db_connector.query(
            f'SELECT * FROM {self.table_name}'
        )
        self.assertEqual(EXPECTED_DATA, actual_data)
        actual_data2 = self.db_connector.query(
            f'SELECT * FROM {self.table_name2}'
        )
        self.assertEqual([(-2, 1, 'bar')], actual_data2)

    def test_columns(self):

        self.run_task(self.dummy)

        expected_columns = [
            ('id', 'integer'),
            ('a', 'integer'),
            ('b', 'text'),
            ('c', 'text')
        ]
        actual_columns = list(self.dummy.columns)
        self.assertListEqual(expected_columns, actual_columns)

    def test_primary_key(self):

        # Set up database samples
        constraint_name = 'custom_primary_name'
        self.db_connector.execute(
            f'''
                INSERT INTO {self.table_name}
                VALUES (1, 2, 'i-am-a-deprecated-value', 'xy,"z')
            ''',
            f'''
                ALTER INDEX {self.table_name}_pkey
                RENAME TO {constraint_name}
            '''
        )

        self.assertEqual(constraint_name, self.dummy.primary_constraint_name)

        # Execute code under test
        self.run_task(self.dummy)

        # Inspect result
        actual_data = self.db_connector.query(
            f'SELECT * FROM {self.table_name}')
        self.assertEqual(EXPECTED_DATA, actual_data)

    def test_array_columns(self):

        complex_data = [
            ((1, 2, 3), ['a', 'b', 'c'], "'quoted str'"),
            ((), [], '[bracketed str]')
        ]
        complex_csv = '''\
tuple,array,str
"(1,2,3)","['a','b','c']",'quoted str'
(),[],[bracketed str]
'''

        # Set up database
        self.db_connector.execute(
            f'DROP TABLE {self.table_name} CASCADE',
            f'''CREATE TABLE {self.table_name} (
                ints int[],
                texts text[],
                text text PRIMARY KEY
            )''')

        self.dummy.csv = complex_csv

        # Execute code under test
        self.run_task(self.dummy)

        # Inspect result
        actual_data = self.db_connector.query(
            f'SELECT * FROM {self.table_name}'
        )
        self.assertEqual(
            [(list(row[0]), *row[1:]) for row in complex_data],
            actual_data
        )


class TestDbConnector(DatabaseTestCase):
    """Tests the DbConnector class."""

    def setUp(self):
        super().setUp()

        # Set up object under test
        self.connector = DbConnector(
            host=os.environ['POSTGRES_HOST'],
            database=os.environ['POSTGRES_DB'],
            user=os.environ['POSTGRES_USER'],
            password=os.environ['POSTGRES_PASSWORD'])

        # Set up connection manually (don't use the module under test!)
        self.connection = psycopg2.connect(
            host=self.connector.host,
            database=self.connector.database,
            user=self.connector.user,
            password=self.connector.password)

        # Give each test a temporary table to work with
        self.temp_table = f'tmp_{time.time()}'.replace('.', '')
        with self.connection as conn:
            with conn.cursor() as cur:
                cur.execute(f'''
                    CREATE TABLE {self.temp_table}
                    (col1 INT, col2 INT)
                ''')
                cur.execute(f'''
                    INSERT INTO {self.temp_table}
                    VALUES (1,2),(3,4)
                ''')

    def tearDown(self):
        try:
            self.connection.close()
        finally:
            super().tearDown()

    def test_query(self):

        rows = self.connector.query(f'SELECT * FROM {self.temp_table}')
        self.assertEqual([(1, 2), (3, 4)], rows)

    def test_query_args(self):

        rows = self.connector.query(
            'SELECT * FROM (VALUES (%s, %s, %s)) x',
            42, 'foo', [1, 2, 3]
        )
        self.assertEqual([(42, 'foo', [1, 2, 3])], rows)

    def test_query_kwargs(self):

        rows = self.connector.query(
            'SELECT * FROM (VALUES (%(spam)s, %(eggs)s)) x',
            spam=42, eggs='foo'
        )
        self.assertEqual([(42, 'foo')], rows)

    def test_query_with_header(self):

        rows, columns = self.connector.query_with_header(
            f'SELECT * FROM {self.temp_table}')
        self.assertEqual([(1, 2), (3, 4)], rows)
        self.assertSequenceEqual(['col1', 'col2'], columns)

    def test_query_with_header_args(self):

        rows, columns = self.connector.query_with_header(
            'SELECT * FROM (VALUES (%s, %s, %s)) x(a, b, c)',
            42, 'foo', [1, 2, 3]
        )
        self.assertEqual([(42, 'foo', [1, 2, 3])], rows)
        self.assertSequenceEqual(['a', 'b', 'c'], columns)

    def test_execute(self):

        self.connector.execute(f'''
            DELETE FROM {self.temp_table}
            WHERE col1 = 1
        ''')

        with self.connection as conn:
            with conn.cursor() as cur:
                cur.execute(f'SELECT * FROM {self.temp_table}')
                table_content = cur.fetchall()

        self.assertEqual([(3, 4)], table_content)

    def test_execute_with_arg(self):

        self.connector.execute((
            f'''
                DELETE FROM {self.temp_table}
                WHERE col1 = %s
            ''',
            (1,)
        ))

        with self.connection as conn:
            with conn.cursor() as cur:
                cur.execute(f'SELECT * FROM {self.temp_table}')
                table_content = cur.fetchall()

        self.assertEqual([(3, 4)], table_content)

    def test_execute_multiple(self):

        self.connector.execute(
            f'''
                DELETE FROM {self.temp_table}
                WHERE col1 = 1
            ''',
            f'''
                DELETE FROM {self.temp_table}
                WHERE col1 = 3
            ''')

        with self.connection as conn:
            with conn.cursor() as cur:
                cur.execute(f'SELECT * FROM {self.temp_table}')
                table_content = cur.fetchall()

        self.assertFalse(table_content)  # empty

    def test_execute_multiple_with_args(self):

        self.connector.execute(
            (f'''
                DELETE FROM {self.temp_table}
                WHERE col1 = %(foo)s - %(bar)s
            ''', {'foo': 7, 'bar': 6}),
            f'''
                DELETE FROM {self.temp_table}
                WHERE col1 = 3
            ''')

        with self.connection as conn:
            with conn.cursor() as cur:
                cur.execute(f'SELECT * FROM {self.temp_table}')
                table_content = cur.fetchall()

        self.assertFalse(table_content)  # empty

    def test_exists_case_not_empty(self):

        exists = self.connector.exists(f'''
            SELECT * FROM {self.temp_table}
        ''')

        self.assertTrue(exists)

    def test_exists_case_empty(self):

        exists = self.connector.exists(f'''
            SELECT * FROM {self.temp_table}
            WHERE col1 = 10
        ''')

        self.assertFalse(exists)

    def test_exists_table_case_true(self):

        exists = self.connector.exists_table(self.temp_table)
        self.assertTrue(exists)

    def test_exists_table_case_false(self):

        with self.connection as conn:
            with conn.cursor() as cur:
                cur.execute(f'DROP TABLE {self.temp_table}')

        exists = self.connector.exists_table(self.temp_table)
        self.assertFalse(exists)

    def test_error_is_raised_on_bad_table_name(self):

        with self.assertRaises(psycopg2.Error):
            self.connector.query('''
                SELECT * FROM table_that_does_not_exist
            ''')


class TestQueryDb(DatabaseTestCase):
    """Tests the QueryDb task."""

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
            MockTarget(f'output/{self.table}')

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
            MockTarget(f'output/{self.table}')

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
            MockTarget(f'output/{self.table}')

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


class TestQueryCacheToDb(DatabaseTestCase):
    """Tests the QueryCacheToDb task."""

    table = 'my_cool_cache'

    def setUp(self):

        super().setUp()

        self.db_connector.execute(f'''
            CREATE TABLE {self.table} (
                spam INT,
                eggs TEXT
            )
        ''')

    def test_simple(self):

        self.task = QueryCacheToDb(
            table=self.table,
            query='''
            SELECT * FROM (VALUES
                (1, 'foo'),
                (2, 'bar')
            ) v(x, y)
            '''
        )

        self.assertFalse(
            self.db_connector.query(f'SELECT * FROM {self.table}'),
            msg="Cache table should be empty before running the task"
        )

        self.run_task(self.task)

        self.assertSequenceEqual(
            [(1, 'foo'), (2, 'bar')],
            self.db_connector.query(f'SELECT * FROM {self.table}'),
            msg="Cache table should have been filled after running the task"
        )

        self.run_task(self.task)

        self.assertSequenceEqual(
            [(1, 'foo'), (2, 'bar')],
            self.db_connector.query(f'SELECT * FROM {self.table}'),
            msg="Cache table should have been replaced after running the task"
        )

    def test_errorneous_query(self):

        self.task = QueryCacheToDb(
            table=self.table,
            query='''
                SELECT x / x * x, y FROM (VALUES
                    (0, 'foo'),
                    (2, 'bar')
                ) v(x, y)
            '''
        )
        self.db_connector.execute((
            f'INSERT INTO {self.table} VALUES (%s, %s)',
            (42, "🥳")
        ))

        self.assertEqual(
            [(42, "🥳")],
            self.db_connector.query(f'SELECT * FROM {self.table}'),
            msg="Cache table should contain old values before running the "
                "task"
        )

        with self.assertRaises(psycopg2.errors.DivisionByZero):
            self.run_task(self.task)

        self.assertEqual(
            [(42, "🥳")],
            self.db_connector.query(f'SELECT * FROM {self.table}'),
            msg="Cache table should still contain old values after the task "
                "failed"
        )

    def test_args(self):

        self.task = QueryCacheToDb(
            table=self.table,
            query='''
                SELECT * FROM (VALUES
                    (%s, %s),
                    (%s, '%%')
                ) v(x, y)
            ''',
            args=(1, 'foo', 2)
        )
        self.task.output = lambda: \
            MockTarget(f'output/{self.table}')

        self.run_task(self.task)

        self.assertSequenceEqual(
            [(1, 'foo'), (2, '%')],
            self.db_connector.query(f'SELECT * FROM {self.table}')
        )

    def test_kwargs(self):

        self.task = QueryCacheToDb(
            table=self.table,
            query='''
                SELECT * FROM (VALUES
                    (%(spam)s, %(eggs)s)
                ) v(x, y)
            ''',
            kwargs={'spam': 42, 'eggs': 'foo'}
        )
        self.task.output = lambda: \
            MockTarget(f'output/{self.table}')

        self.run_task(self.task)

        self.assertSequenceEqual(
            [(42, 'foo')],
            self.db_connector.query(f'SELECT * FROM {self.table}')
        )


class DummyFileWrapper(luigi.Task):
    """Dummy task to write an output file."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.mock_target = MockTarget(
            f'DummyFileWrapperMock{hash(self)}',
            format=luigi.format.UTF8)

    csv = luigi.Parameter()

    def run(self):
        with self.mock_target.open('w') as input_file:
            input_file.write(self.csv)

    def output(self):
        return self.mock_target


class DummyWriteCsvToDb(CsvToDb):
    """Dummy subclass of CsvToDb."""

    table = luigi.Parameter()

    csv = luigi.Parameter()

    def requires(self):
        return DummyFileWrapper(csv=self.csv)
