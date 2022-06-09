from ast import literal_eval
import datetime as dt
from typing import TypeVar

import luigi
from luigi.contrib.postgres import CopyToTable
from luigi.format import UTF8
import numpy as np
import pandas as pd
from psycopg2.errors import UndefinedTable

import _utils

logger = _utils.logger

T = TypeVar('T')


class CsvToDb(CopyToTable):
    """
    Copies a depended CSV file into the database.

    Subclasses have to override columns and requires().
    Don't forget to write a migration script if you change the table schema.
    """

    minimal_mode = luigi.parameter.BoolParameter(
        default=_utils.minimal_mode(),
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
        self.db_connector = _utils.db_connector()
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
        'ARRAY': literal_eval,
        'text': str
    }

    """
    Conversion functions to be applied before generating the output CSV for
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

    """
    If True, the table will be truncated before copying the new values.
    """
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
            ''')  # nosec B608
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
            ''', only_first=True)[0]  # nosec B608
            if not self._primary_constraint_name:
                raise UndefinedTable(self.table)

        return self._primary_constraint_name

    def copy(self, cursor, file):

        table = '.'.join(self.table_path)
        tmp_table = f'{"_".join(self.table_path)}_tmp'
        columns = ', '.join([col[0] for col in self.columns])
        query = f'''
            BEGIN;
                CREATE TEMPORARY TABLE {tmp_table} (
                    LIKE {table} INCLUDING ALL);
                COPY {tmp_table} ({columns}) FROM stdin WITH (FORMAT CSV);

                INSERT INTO {table} ({columns})
                    SELECT {columns} FROM {tmp_table}
                ON CONFLICT ON CONSTRAINT {self.primary_constraint_name}
                    DO UPDATE SET {', '.join(
                        f'{column[0]} = EXCLUDED.{column[0]}'
                        for column in self.columns
                    )};
                {f"""
                DELETE FROM {table}
                    WHERE NOT EXISTS (
                        SELECT NULL
                        FROM {tmp_table}
                        WHERE ({', '.join(
                            f'{self.table}.{column[0]}'
                            for column in self.columns
                        )}) = ({', '.join(
                            f'{tmp_table}.{column[0]}'
                            for column in self.columns
                        )}));
                """ * self.replace_content}
            COMMIT;
        '''  # nosec B608
        logger.debug(f"{self.__class__}: Executing query: {query}")
        cursor.copy_expert(query, file)

    def create_table(self):
        """Overridden from superclass to forbid dynamical schema changes."""
        raise Exception(
            "CsvToDb does not support dynamical schema modifications."
            "To change the schema, create and run a migration script.")

    def rows(self):
        """
        Return the rows to be stored into the database.

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
            csv_columns = pd.read_csv(file, nrows=0).columns
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
        """Split up self.table into schema and table name."""
        table = self.table
        segments = table.split('.')
        return (
            '.'.join(segments[:-1]) if segments[:-1] else 'public',
            segments[-1]
        )


class QueryDb(_utils.DataPreparationTask):
    """Make an SQL query and store the results into an output file."""

    query = luigi.Parameter(
        description="The SQL query to perform on the DB"
    )

    args = _utils.ObjectParameter(
        default=(),
        description="The SQL query's positional arguments"
    )

    kwargs = _utils.ObjectParameter(
        default={},
        description="The SQL query's named arguments"
    )

    limit = luigi.parameter.IntParameter(
        default=-1,
        description="The maximum number of rows to fetch. Optional. If -1, "
                    "all rows will be fetched.")

    shuffle = luigi.BoolParameter(
        default=False,
        description="If True, all rows will be shuffled. For debugging and "
                    "exploration purposes. Might impact performance.")

    def output(self):

        return luigi.LocalTarget(
            f'{self.output_dir}/{self.task_id}.csv',
            format=UTF8
        )

    def run(self):

        query = self.build_query()
        rows, columns = self.db_connector.query_with_header(
            query, *self.args, **self.kwargs)
        df = pd.DataFrame(rows, columns=columns)

        df = self.transform(df)

        self.write_output(df)

    def build_query(self):

        query = self.query
        if self.shuffle:
            query += ' ORDER BY RANDOM()'
        if self.minimal_mode and self.limit == -1:
            self.limit = 50
        if self.limit and self.limit > -1:
            query += f' LIMIT {self.limit}'
        return query

    def transform(self, df):
        """Provide a hook for subclasses."""
        return df

    def write_output(self, df):

        with self.output().open('w') as output_stream:
            df.to_csv(output_stream, index=False, header=True)


class QueryCacheToDb(QueryDb):
    """
    Make an SQL query and store the results into a cache table.

    This task is optimized. The query results won't leave the DBMS at any
    point during updating the cache table.
    """

    def run(self):

        assert not self.args or not self.kwargs, \
            "cannot combine args and kwargs"
        all_args = next(filter(bool, [self.args, self.kwargs]), None)

        query = self.build_query()
        self.db_connector.execute(
            f'TRUNCATE TABLE {self.table}',
            (f'INSERT INTO {self.table} {query}', all_args)
        )

        count = self.db_connector.query(
            f'SELECT COUNT(*) FROM {self.table}')  # nosec B608

        # Write dummy output for sake of complete()
        with self.output().open('w') as file:
            file.write(f"Cache of {count} rows")
