import os
import psycopg2
import time

from db_connector import DbConnector
from task_test import DatabaseTaskTest


class TestDbConnector(DatabaseTaskTest):

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
        self.connection.set_isolation_level(0)
        with self.connection as conn:
            with conn.cursor() as cur:
                cur.execute(f'DROP TABLE {self.temp_table}')

        super().tearDown()

    def test_query(self):

        res = self.connector.query(f'SELECT * FROM {self.temp_table}')
        self.assertEqual(res, [(1, 2), (3, 4)])

    def test_query_multiple(self):

        res = self.connector.query(
            f'SELECT * FROM {self.temp_table}',
            f'SELECT COUNT(*) from {self.temp_table}')
        self.assertCountEqual(res, [
            [(1, 2), (3, 4)],
            [(2,)]
        ])

    def test_execute(self):

        self.connector.execute(f'''
            DELETE FROM {self.temp_table}
            WHERE col1 = 1
        ''')

        with self.connection as conn:
            with conn.cursor() as cur:
                cur.execute(f'SELECT * FROM {self.temp_table}')
                table_content = cur.fetchall()

        self.assertEqual(table_content, [(3, 4)])

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

    def test_error_is_raised_on_bad_table_name(self):

        with self.assertRaises(psycopg2.Error):
            self.connector.query('''
                SELECT * FROM table_that_does_not_exist
            ''')
