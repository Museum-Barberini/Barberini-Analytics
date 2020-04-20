import datetime as dt
import tempfile
import time

import luigi
import mmh3

from csv_to_db import CsvToDb
from task_test import DatabaseTaskTest

# Initialize test and write it to a csv file
expected_data = [(1, 2, 'abc', 'xy,"z'), (2, 10, '678', ',,;abc')]
expected_data_csv = '''\
id,A,B,C
1,2,abc,"xy,""z"
2,10,"678",",,;abc"
'''
tmp_csv_file = tempfile.NamedTemporaryFile()
with open(tmp_csv_file.name, 'w') as f:
    f.write(expected_data_csv)


class DummyFileWrapper(luigi.Task):
    def output(self):
        return luigi.LocalTarget(tmp_csv_file.name)


class DummyWriteCsvToDb(CsvToDb):

    def __init__(self, table_name, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.__class__.table = table_name

        # By default luigi assigns the same task_id to the objects of
        # this class.
        # That leads to errors when updating the marker table (table_updates).
        self.task_id = f"{self.task_id}_{str(dt.datetime.now())}"
        time.sleep(1e-6)  # to guarantee uniqueness of dt string above

    columns = [
        ('id', 'INT'),
        ('A', 'INT'),
        ('B', 'TEXT'),
        ('C', 'TEXT')
    ]
    primary_key = 'id'

    table = None  # value set in __init__

    def requires(self):
        return DummyFileWrapper()


def get_temp_table():
    tmp = f'tmp_{time.time()}'.replace('.', '')
    time.sleep(1e-6)  # to guarantee uniqueness of tmp name
    return tmp


# -------- TESTS START HERE -------

class TestCsvToDb(DatabaseTaskTest):

    def setUp(self):
        super().setUp()

        self.table_name = get_temp_table()

        '''
        Insert manually calculated dummy_date because otherwise,
        luigi may not create a new DummyWriteCsvToDb Task

        This should be kept in mind in case the behaviour is seen elsewhere
        as well
        '''
        self.dummy = DummyWriteCsvToDb(
            self.table_name,
            dummy_date=mmh3.hash(self.table_name, 666))

    def tearDown(self):
        # TODO: Do we need this? Tests do pass without it.
        #self.db_connector.connection.set_isolation_level(0)
        self.db_connector.execute(f'DROP TABLE {self.table_name};')

        super().tearDown()

    def test_adding_data_to_database_new_table(self):

        self.dummy.run()
        actual_data = self.db_connector.query(
            f'SELECT * FROM {self.table_name};')
        self.assertEqual(actual_data, expected_data)

    def test_adding_data_to_database_existing_table(self):

        # ----- Set up database -----
        self.db_connector.execute(
            f'''CREATE TABLE {self.table_name} (
                id int,
                A int,
                B text,
                C text);
            ''',
            f'''
                ALTER TABLE {self.table_name}
                ADD CONSTRAINT {self.table_name}_primkey PRIMARY KEY (id);
            ''',
            f'''
                INSERT INTO {self.table_name}
                VALUES (0, 1, 'a', 'b');
            '''
        )

        # ----- Execute code under test ----
        self.dummy.run()

        # ----- Inspect result ------
        actual_data = self.db_connector.query(
            f'SELECT * FROM {self.table_name};')
        self.assertEqual(actual_data, [(0, 1, 'a', 'b'), *expected_data])

    def test_no_duplicates_are_inserted(self):

        # ----- Set up database -----
        self.db_connector.execute(
            f'''CREATE TABLE {self.table_name} (
                id int,
                A int,
                B text,
                C text);
            ''',
            f'''
                ALTER TABLE {self.table_name}
                ADD CONSTRAINT {self.table_name}_primkey PRIMARY KEY (id);
            ''',
            f'''
                INSERT INTO {self.table_name}
                VALUES (1, 2, 'i-am-a-deprecated-value', 'xy,"z');
            '''
        )

        # ----- Execute code under test ----
        self.dummy.run()

        # ----- Inspect result ------
        actual_data = self.db_connector.query(
            f'SELECT * FROM {self.table_name};')
        self.assertEqual(actual_data, expected_data)
