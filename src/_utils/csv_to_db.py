import csv
import datetime as dt

import luigi
import psycopg2
from luigi.contrib.postgres import CopyToTable

from set_db_connection_options import set_db_connection_options


class CsvToDb(CopyToTable):
    """
    Copies a depended csv file into the a central database.
    Subclasses have to override columns, primary_key and requires().
    NOTE that you will need to drop or manually alter an existing table if
    you change its schema.
    """

    @property
    def primary_key(self):
        raise NotImplementedError

    sql_file_path_pattern = luigi.Parameter(
        default='src/_utils/sql_scripts/{0}.sql')
    schema_only = luigi.BoolParameter(
        default=False,
        description=("If True, the table will be only created "
                     "but not actually filled with the input data."))

    # Don't delete this! This parameter assures that every (sub)instance of me
    # is treated as an individual and will be re-run.
    dummy_date = luigi.FloatParameter(
        default=dt.datetime.timestamp(dt.datetime.now()))

    # These attributes are set in __init__. They need to be defined
    # here because they are abstract methods in CopyToTable.
    host = None
    database = None
    user = None
    password = None

    # override the default column separator (tab)
    column_separator = ','

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        set_db_connection_options(self)
        if self.schema_only:
            self.requires = lambda: []
        self.seed = 666

    def init_copy(self, connection):
        if not self.check_existence(connection):
            raise UndefinedTableError()
        super().init_copy(connection)

    def copy(self, cursor, file):
        if self.schema_only:
            return
        query = self.load_sql_script('copy', self.table, ','.join(
            [f'{col[0]} = EXCLUDED.{col[0]}' for col in self.columns]))
        cursor.copy_expert(query, file)

    def rows(self):
        if self.schema_only:
            return []
        rows = super().rows()
        next(rows)
        return rows

    def check_existence(self, connection):
        cursor = connection.cursor()
        cursor.execute(self.load_sql_script('check_existence', self.table))
        existence_boolean = cursor.fetchone()[0]
        return existence_boolean

    def create_table(self, connection):
        super().create_table(connection)
        print("INFO: Create table " + self.table)
        self.create_primary_key(connection)

    def create_primary_key(self, connection):
        connection.cursor().execute(
            self.load_sql_script(
                'set_primary_key',
                self.table,
                self.tuple_like_string(self.primary_key)
            )
        )

    def load_sql_script(self, name, *args):
        with open(self.sql_file_path_pattern.format(name)) as sql_file:
            return sql_file.read().format(*args)

    def tuple_like_string(self, value):
        string = value
        if isinstance(value, tuple):
            string = ','.join(value)
        return f'({string})'


class UndefinedTableError(psycopg2.ProgrammingError):
    pgcode = psycopg2.errorcodes.UNDEFINED_TABLE
