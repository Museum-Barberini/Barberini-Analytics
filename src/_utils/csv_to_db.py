from ast import literal_eval
import datetime as dt
from io import StringIO
import logging

import luigi
from luigi.contrib.postgres import CopyToTable
from luigi.format import UTF8
import numpy as np
import pandas as pd
from psycopg2.errors import UndefinedTable

import db_connector
from data_preparation import DataPreparationTask, minimal_mode

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

        # lazy properties
        self._columns = None
        self._primary_constraint_name = None

    seed = 666
    sql_file_path_pattern = 'src/_utils/sql_scripts/{0}.sql'

    """
    Conversion functions to be applied by pandas while loading the input CSV.
    """
    converters_in = {
        # Pandas stores arrays as "(1,2,3)", but must be specified the
        # following function explicitly in order not to load them as plain
        # string.
        'ARRAY': literal_eval
    }

    """
    Conversion functions to be applied before generating the ouput CSV for
    postgres.
    """
    converters_out = {
        # Unless pandas, postgres parses arrays according to the following
        # syntax: '{42,"some text"}'
        'ARRAY': lambda iterable:
            # TODO: This might break when we pass certain objects into the
            # array. In case of this event, we will want to consider
            # information_schema.element_types as well ...
            f'''{{{','.join(
                f'"{item}"' for item in iterable
            )}}}''' if iterable else '{}',
        # This is necessary due to pandas storing data as numpy types whenever
        # possible. In some cases we don't want this because it introduces
        # float representations of large numbers and NaN instead of None.
        # Convert them to strings always, and postgres will parse them itself,
        # anyway.
        'integer': lambda value:
            None if np.isnan(value) else str(int(value))
    }

    replace_content = False

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

    @property
    def primary_constraint_name(self):
        if not self._primary_constraint_name:
            table_path = self.table_path
            self._primary_constraint_name = self.db_connector.query(f'''
                SELECT
                    constraint_name
                FROM
                    information_schema.table_constraints
                WHERE
                    constraint_type = 'PRIMARY KEY'
                    AND (table_schema, table_name)
                        = ('{table_path[0]}', '{table_path[1]}')
            ''', only_first=True)[0]
            if not self._primary_constraint_name:
                raise UndefinedTable(self.table)

        return self._primary_constraint_name

    def copy(self, cursor, file):

        if self.replace_content:
            # delete existing rows to avoid cache invalidation
            cursor.execute(f'TRUNCATE {self.table}')

        query = self.load_sql_script(
            'copy',
            *self.table_path,
            self.primary_constraint_name,
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
        Completely throw away super's implementation because there are some
        inconsistencies between pandas's and postgres's interpretations of CSV
        files. Mainly, arrays are encoded differently. This requires us to do
        the conversion ourself ...
        """
        # NOTE: This keeps getting muddy and more muddy. Consider using
        # something like pandas.io.sql and override copy?

        df = self.read_csv(self.input())

        for i, (col_name, col_type) in enumerate(self.columns):
            csv_col_name = df.columns[i]
            try:
                converter = self.converters_out[col_type]
            except KeyError:
                continue
            df[csv_col_name] = df[csv_col_name].apply(converter)
        csv = df.to_csv(index=False, header=False)

        for line in filter(None, csv.split('\n')):
            yield (line,)

    def read_csv(self, input_csv):
        with input_csv.open('r') as file:
            # Optimization. We're only interested in the column names, no
            # need to read the whole file.
            header_stream = StringIO(next(file))
            csv_columns = pd.read_csv(header_stream).columns
        converters = {
            csv_name: self.converters_in[sql_type]
            for csv_name, (sql_name, sql_type)
            in zip(csv_columns, self.columns)
            if sql_type in self.converters_in
        }
        with input_csv.open('r') as file:
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


class QueryCacheToDb(DataPreparationTask):

    def output(self):

        return luigi.LocalTarget(
            f'{self.output_dir}/{self.task_id}.csv',
            format=UTF8
        )

    def run(self):

        self.db_connector.execute(
            f'TRUNCATE TABLE {self.table}',
            f'INSERT INTO {self.table} {self.query}'
        )

        count = self.db_connector.query(f'SELECT COUNT(*) FROM {self.table}')

        with self.output().open('w') as file:
            file.write(f"Cache of {count} rows")
