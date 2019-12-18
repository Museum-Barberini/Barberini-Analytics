import unittest
import psycopg2
import luigi
import time
import tempfile
import pandas as pd
from unittest.mock import patch

from src._utils.csv_to_db import CsvToDb


class TestCsvToDb(unittest.TestCase):
    """ IMPORTANT: to be able to run this test you need to have a
            database barberini_test running"""

    # To test (by implementing subclasses):
    # - adding data to database works
    # - no duplicates are added
    # - column types are set as defined in the subclass
    # - what happens if table/database does not exist
    # - what happens if user/password combination does not work
    
    @patch("src._utils.csv_to_db.set_db_connection_options")
    def test_adding_data_to_database_new_table(self, mock):

        # ----- Set up -------

        # initialize data
        tmp_csv_file = tempfile.NamedTemporaryFile()
        with open(tmp_csv_file.name, "w") as fp:
            fp.write("id,A,B,C\n1,2,abc,\"xy,\"\"z\"\n2,10,\"678\",\",,;abc\"\n")

        class DummyFileWrapper(luigi.Task):
            def output(self):
                return luigi.LocalTarget(tmp_csv_file.name)

        table_name = f"tmp_{time.time()}".replace(".", "")
        class DummyWriteCsvToDb(CsvToDb):

            table = table_name
            columns = [
                    ("id", "INT"),
                    ("A", "INT"),
                    ("B", "TEXT"),
                    ("C", "TEXT")
            ]
            primary_key = "id"

            host = "host.docker.internal"
            database = "barberini_test"
            user = "postgres"
            password = "docker"

            def requires(self):
                return DummyFileWrapper()

        # ----- Execute code under test -----

        dummy = DummyWriteCsvToDb()
        dummy.run()

        # ----- Inspect result -----

        mock.assert_called_once()

        con = psycopg2.connect(host="host.docker.internal", dbname="barberini_test",
                               user="postgres", password="docker")
        cur = con.cursor()
        cur.execute(f"select * from {table_name};")
        actual_data = cur.fetchall()
        cur.close()

        self.assertEqual(actual_data, [(1, 2, "abc", "xy,\"z"), (2, 10, "678", ",,;abc")])


        # ----- Delete the temporary table (if the test was successful) -------
        con.set_isolation_level(0)
        cur = con.cursor()
        cur.execute(f"DROP TABLE {table_name};")
        cur.close()
        con.close()

