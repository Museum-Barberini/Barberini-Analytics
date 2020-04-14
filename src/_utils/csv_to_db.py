import datetime as dt
import logging
import os

import luigi
import psycopg2
from luigi.contrib.postgres import CopyToTable

logger = logging.getLogger('luigi-interface')


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

    @property
    def foreign_keys(self):
        # Default: no foreign key definitions
        return []

    schema_only = luigi.BoolParameter(
        default=False,
        description=("If True, the table will be only created "
                     "but not actually filled with the input data."))

    # Don't delete this! This parameter assures that every (sub)instance of me
    # is treated as an individual and will be re-run.
    dummy_date = luigi.FloatParameter(
        default=dt.datetime.timestamp(dt.datetime.now()))

    # Set db connection parameters using env vars
    host = os.environ['POSTGRES_HOST']
    database = os.environ['POSTGRES_DB']
    user = os.environ['POSTGRES_USER']
    password = os.environ['POSTGRES_PASSWORD']

    # override the default column separator (tab)
    column_separator = ','

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if self.schema_only:
            self.requires = lambda: []
        self.seed = 666

        self.sql_file_path_pattern = 'src/_utils/sql_scripts/{0}.sql'

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
        logger.info("Create table " + self.table)
        self.create_primary_key(connection)
        self.create_foreign_key(connection)

    def create_primary_key(self, connection):
        connection.cursor().execute(
            self.load_sql_script(
                'set_primary_key',
                self.table,
                self.tuple_like_string(self.primary_key)
            )
        )

    def create_foreign_key(self, connection):
        for key in self.foreign_keys:
            connection.cursor().execute(
                self.load_sql_script(
                    'set_foreign_key',
                    self.table,
                    key['origin_column'],
                    key['target_table'],
                    key['target_column']
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
