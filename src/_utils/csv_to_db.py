from ast import literal_eval
import datetime as dt
from io import StringIO
import logging

import luigi
from luigi.contrib.postgres import CopyToTable
import numpy as np
import pandas as pd
from psycopg2.errors import UndefinedTable

import db_connector
from data_preparation import minimal_mode

logger = logging.getLogger('luigi-interface')


class CsvToDb(CopyToTable):
    """
    Copies a depended csv file into the a central database.
    Subclasses have to override columns and requires().
    Don't forget to write a migration script if you change the table schema.
    """

    minimal_mode = luigi.parameter.BoolParameter(
        default=minimal_mode,
        description="If True, only a minimal amount of data will be prepared"
                    "in order to test the pipeline for structural problems")

    """
    Don't delete this or make it private! This parameter assures that every
    (sub)instance of me is treated as an individual and will be re-run.
    """
    dummy_date = luigi.FloatParameter(
        default=dt.datetime.timestamp(dt.datetime.now()))

    # override the default column separator (tab)
    column_separator = ','

    host = database = user = password = None

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Set db connection parameters using env vars
        self.db_connector = db_connector.db_connector()
        self.host = self.db_connector.host
        self.database = self.db_connector.database
        self.user = self.db_connector.user
        self.password = self.db_connector.password
        self._columns = None  # lazy property

    seed = 666
    sql_file_path_pattern = 'src/_utils/sql_scripts/{0}.sql'

    converters_in = {
        'ARRAY': literal_eval
    }

    converters_out = {
        'ARRAY': lambda iterable:
            # TODO: This might break when we pass certain objects into the
            # array. In case of this event, we will want to consider
            # information_schema.element_types as well ...
            f'''{{{','.join(
                f'"{item}"' for item in iterable
            )}}}''' if iterable else '{}',
        'integer': lambda value:
            None if np.isnan(value) else str(int(value))
    }

    @property
    def columns(self):
        if not self._columns:
            table_path = self.table_path
            self._columns = self.db_connector.query(f'''
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE (table_schema, table_name)
                        = ('{table_path[0]}', '{table_path[1]}')
                    AND is_generated = 'NEVER'
                    AND column_default IS NULL
                ORDER BY ordinal_position
            ''')
            if not self._columns:
                raise UndefinedTable(self.table)

        return self._columns

    def copy(self, cursor, file):
        query = self.load_sql_script(
            'copy',
            *self.table_path,
            ', '.join([col[0] for col in self.columns]),
            ', '.join(
                [f'{col[0]} = EXCLUDED.{col[0]}' for col in self.columns]))
        logger.debug(f"{self.__class__}: Executing query: {query}")
        cursor.copy_expert(query, file)

    def create_table(self):
        """
        Overridden from superclass to forbid dynamical schema changes.
        """

        raise Exception(
            "CsvToDb does not support dynamical schema modifications."
            "To change the schema, create and run a migration script.")

    def rows(self):
        """
        Completely throw away super's stupid implementation because there are
        some inconsistencies between pandas's and postgres's interpretations
        of CSV files. Mainly, arrays are treated differently. This requires us
        to do the conversion ourself ...
        """
        # TODO: This keeps getting muddy and more muddy. Consider using
        # something like pandas.io.sql and override copy?

        df = self.read_csv(self.input())

        for i, (col_name, col_type) in enumerate(self.columns):
            col_name = df.columns[i]
            try:
                converter = self.converters_out[col_type]
            except KeyError:
                continue
            df[col_name] = df[col_name].apply(converter)
        csv = df.to_csv(index=False, header=False)

        for line in filter(None, csv.split('\n')):
            yield (line,)

    def read_csv(self, input):
        with input.open('r') as file:
            csv_columns = pd.read_csv(StringIO(next(file))).columns
        converters = {
            csv_name: self.converters_in[sql_type]
            for csv_name, (sql_name, sql_type)
            in zip(csv_columns, self.columns)
            if sql_type in self.converters_in
        }
        with input.open('r') as file:
            return pd.read_csv(file, converters=converters)

    @property
    def table_path(self):
        """
        Split up self.table into schema and table name.
        """

        table = self.table
        segments = table.split('.')
        return (
            '.'.join(segments[:-1]) if segments[:-1] else 'public',
            segments[-1]
        )

    def load_sql_script(self, name, *args):
        with open(self.sql_file_path_pattern.format(name)) as sql_file:
            return sql_file.read().format(*args)
