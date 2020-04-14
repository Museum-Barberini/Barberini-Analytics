import psycopg2
import time

from db_connector import DbConnector
from task_test import DatabaseTaskTest


class TestDbConnector(DatabaseTaskTest):

    def setUp(self):
        super().setUp()
        # give each test a temporary table to work with
        self.temp_table = f'tmp_{time.time()}'.replace('.', '')
        with self.db.connection as conn:
            with conn.cursor() as cur:
                cur.execute(f'CREATE TABLE {self.temp_table}'
                            '(col1 INT, col2 INT)')
                cur.execute(f'INSERT INTO {self.temp_table} '
                            'VALUES (1,2),(3,4)')

    def tearDown(self):
        self.db.connection.set_isolation_level(0)
        self.db.commit(f'DROP TABLE {self.temp_table}')
        super().tearDown()

    def test_query(self):

        res = DbConnector.query(f'SELECT * FROM {self.temp_table}')
        self.assertEqual(res, [(1, 2), (3, 4)])

    def test_execute(self):

        DbConnector.execute(f'DELETE FROM {self.temp_table} WHERE col1=1')

        with self.db.connection as conn:
            with conn.cursor() as cur:
                cur.execute(f'SELECT * from {self.temp_table}')
                table_content = cur.fetchall()

        self.assertEqual(table_content, [(3, 4)])

    def test_exists_case_not_empty(self):

        exists = DbConnector.exists(f'SELECT * from {self.temp_table}')

        self.assertTrue(exists)

    def test_exists_case_empty(self):

        exists = DbConnector.exists(
                f'SELECT * from {self.temp_table} WHERE col1=10')

        self.assertFalse(exists)

    def test_error_is_raised_on_bad_table_name(self):

        with self.assertRaises(psycopg2.Error):
            DbConnector.query('SELECT * FROM table_that_does_not_exist')
