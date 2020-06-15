import os
import time

import psycopg2

from db_connector import DbConnector
from db_test import DatabaseTestCase


class TestDbConnector(DatabaseTestCase):

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

    def test_query_with_header(self):

        rows, columns = self.connector.query_with_header(
            f'SELECT * FROM {self.temp_table}')
        self.assertEqual([(1, 2), (3, 4)], rows)
        self.assertSequenceEqual(['col1', 'col2'], columns)

    def test_query_with_header_args(self):

        rows, columns = self.connector.query_with_header(
            f'SELECT * FROM (VALUES (%s, %s, %s)) x(a, b, c)',
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
