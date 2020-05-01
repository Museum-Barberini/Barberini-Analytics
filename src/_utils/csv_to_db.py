import datetime as dt
import logging
import os

import luigi
from luigi.contrib.postgres import CopyToTable

logger = logging.getLogger('luigi-interface')


class CsvToDb(CopyToTable):
    """
    Copies a depended csv file into the a central database.
    Subclasses have to override columns and requires().
    Don't forget to write a migration script if you change the table schema.
    """

    # TODO: Deprecate
    schema_only = luigi.BoolParameter(default=False, description=(
            "If True, the table will be only created but not actually filled"
            "with the input data."))

    """
    Don't delete this! This parameter assures that every (sub)instance of me
    is treated as an individual and will be re-run.
    """
    dummy_date = luigi.FloatParameter(
        default=dt.datetime.timestamp(dt.datetime.now()),
        visibility=luigi.parameter.ParameterVisibility.PRIVATE)

    # override the default column separator (tab)
    column_separator = ','

    host = database = user = password = None

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if self.schema_only:
            self.requires = lambda: []

        # Set db connection parameters using env vars
        self.host = os.environ['POSTGRES_HOST']
        self.database = os.environ['POSTGRES_DB']
        self.user = os.environ['POSTGRES_USER']
        self.password = os.environ['POSTGRES_PASSWORD']

    seed = 666
    sql_file_path_pattern = 'src/_utils/sql_scripts/{0}.sql'

    """
    CsvToDb does not support dynamical schema changes.
    To change the schema, create and run a migration script.
    """
    create_table = None

    def copy(self, cursor, file):
        if self.schema_only:
            return
        query = self.load_sql_script('copy', self.table, ','.join(
            [f'{col[0]} = EXCLUDED.{col[0]}' for col in self.columns]))
        logger.debug(f"{self.__class__}: Executing query: {query}")
        cursor.copy_expert(query, file)

    def rows(self):
        if self.schema_only:
            return []
        rows = super().rows()
        next(rows)
        return rows

    def load_sql_script(self, name, *args):
        with open(self.sql_file_path_pattern.format(name)) as sql_file:
            return sql_file.read().format(*args)
