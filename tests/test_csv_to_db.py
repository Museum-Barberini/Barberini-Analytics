import datetime
import os
import tempfile
import time
import unittest
from unittest.mock import patch

import luigi
import pandas as pd
import psycopg2

from src._utils.csv_to_db import CsvToDb
from task_test import DatabaseTaskTest

# Initialize test and write it to a csv file
expected_data = [(1, 2, "abc", "xy,\"z"), (2, 10, "678", ",,;abc")]
expected_data_csv = "id,A,B,C\n1,2,abc,\"xy,\"\"z\"\n2,10,\"678\",\",,;abc\"\n"
tmp_csv_file = tempfile.NamedTemporaryFile()
with open(tmp_csv_file.name, 'w') as fp:
    fp.write(expected_data_csv)


class DummyFileWrapper(luigi.Task):
    def output(self):
        return luigi.LocalTarget(tmp_csv_file.name)


class DummyWriteCsvToDb(CsvToDb):

    def __init__(self, table_name):
        super().__init__()
        self.__class__.table = table_name

        # By default luigi assigns the same task_id to the objects of this class.
        # That leads to errors when updating the marker table (tablue_updates).
        self.task_id = f"{self.task_id}_{str(datetime.datetime.now())}"

    columns = [
        ("id", "INT"),
        ("A", "INT"),
        ("B", "TEXT"),
        ("C", "TEXT")
    ]
    primary_key = "id"

    host = os.environ['POSTGRES_HOST']
    database = "barberini_test"
    user = os.environ['POSTGRES_USER']
    password = os.environ['POSTGRES_PASSWORD']

    table = None  # value set in __init__

    def requires(self):
        return DummyFileWrapper()


def get_temp_table():
    return f"tmp_{time.time()}".replace(".", "")


# -------- TESTS START HERE -------

class TestCsvToDb(DatabaseTaskTest):

    @patch("src._utils.csv_to_db.set_db_connection_options")
    def setUp(self, mock):

        super().setUp()
        self.table_name = get_temp_table()
        self.dummy = DummyWriteCsvToDb(self.table_name)
        # Store mock object to make assertions about it later on
        self.mock_set_db_conn_options = mock

    def tearDown(self):

        self.mock_set_db_conn_options.assert_called_once()
        self.db.connection.set_isolation_level(0)
        self.db.commit(f"DROP TABLE {self.table_name};")
        # Make absolutely sure that each test gets fresh params
        self.table_name = None
        self.dummy = None
        super().tearDown()

    def test_adding_data_to_database_new_table(self):

        self.dummy.run()
        actual_data = self.db.request(f"select * from {self.table_name};")
        self.assertEqual(actual_data, expected_data)

    def test_adding_data_to_database_existing_table(self):

        # ----- Set up database -----
        self.db.commit(
            f"CREATE TABLE {self.table_name} (id int, A int, B text, C text);",
            f"""
                ALTER TABLE {self.table_name}
                    ADD CONSTRAINT {self.table_name}_the_primary_key_constraint PRIMARY KEY (id);
            """,
            f"INSERT INTO {self.table_name} VALUES (0, 1, 'a', 'b');")

        # ----- Execute code under test ----
        self.dummy.run()

        # ----- Inspect result ------
        actual_data = self.db.request(f"select * from {self.table_name};")
        self.assertEqual(actual_data, [(0, 1, "a", "b"), *expected_data])

    def test_no_duplicates_are_inserted(self):

        # ----- Set up database -----
        self.db.commit(
            f"CREATE TABLE {self.table_name} (id int, A int, B text, C text);",
            f"""
                ALTER TABLE {self.table_name}
                    ADD CONSTRAINT {self.table_name}_the_primary_key_constraint PRIMARY KEY (id);
            """,
            f"INSERT INTO {self.table_name} VALUES (1, 2, 'i-am-a-deprecated-value', 'xy,\"z');")

        # ----- Execute code under test ----
        self.dummy.run()

        # ----- Inspect result ------
        actual_data = self.db.request(f"select * from {self.table_name};")
        self.assertEqual(actual_data, expected_data)
