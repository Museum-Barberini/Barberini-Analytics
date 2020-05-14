import logging
import pandas as pd
import operator
import os
import sys
from functools import partial, reduce
from typing import Callable, Dict, List, Tuple

import luigi
import psycopg2.extensions

import db_connector

logger = logging.getLogger('luigi-interface')

minimal_mode = os.getenv('MINIMAL') == 'True'
OUTPUT_DIR = os.environ['OUTPUT_DIR']

db_connector.register_array_type('SQL_IDENTIFIER', 'information_schema')


class DataPreparationTask(luigi.Task):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.db_connector = db_connector.db_connector()

    table = luigi.parameter.Parameter(
        description="The name of the table the data should be prepared for",
        default=None)

    minimal_mode = luigi.parameter.BoolParameter(
        default=minimal_mode,
        description="If True, only a minimal amount of data will be prepared"
                    "in order to test the pipeline for structural problems")

    @property
    def output_dir(self):
        return OUTPUT_DIR

    def ensure_foreign_keys(
                self,
                df: pd.DataFrame,
                invalid_values_handler: Callable[[
                        pd.DataFrame,
                        Tuple[List[str], Tuple[str, List[str]]],
                        pd.DataFrame
                    ], None] = None
            ) -> pd.DataFrame:
        """
        Note that this currently only works with lower case identifiers.
        """
        def log_invalid_values(
                invalid_values, foreign_key, original_values):
            logger.warning(
                f"Skipped {len(invalid_values)} out of {len(df)} rows "
                f"due to foreign key violation: {foreign_key}")
            print(
                f"Following values were invalid:\n{invalid_values}"
                if sys.stdout.isatty() else
                "Values not printed for privacy reasons")

        def filter_invalid_values(df, foreign_key):
            if df.empty:
                return df

            columns, (foreign_table, foreign_columns) = foreign_key
            # Convert first component (columns) from tuple to list
            columns, foreign_columns = list(columns), list(foreign_columns)
            foreign_key = columns, (foreign_table, foreign_columns)

            foreign_values = [
                # cast values to string uniformly to prevent mismatching
                # due to wrong data types
                str(value)
                for values in self.db_connector.query(f'''
                    SELECT {', '.join(foreign_columns)}
                    FROM {foreign_table}
                ''')
                for value in values]
            for column in columns:
                if not isinstance(df[column][0], str):
                    df[column] = df[column].apply(str)

            # Remove all rows from the df where the value does not match any
            # value from the referenced table
            validities = df[columns] \
                .isin(foreign_values) \
                .apply(partial(reduce, operator.and_), axis=1) \
                .squeeze()
            valid_values, invalid_values = df[validities], df[~validities]
            if not invalid_values.empty:
                log_invalid_values(invalid_values, foreign_key, df)
                if invalid_values_handler:
                    invalid_values_handler(invalid_values, foreign_key, df)

            return valid_values

        return reduce(filter_invalid_values, self.foreign_keys().items(), df)

    def foreign_keys(self) -> Dict[List[str], Tuple[str, List[str]]]:
        if not self.table:
            return {}

        return {
            tuple(columns): (foreign_table, tuple(foreign_columns))
            for [columns, foreign_table, foreign_columns]
            in self.db_connector.query(f'''
                --- CREDITS: https://stackoverflow.com/a/1152321
                SELECT
                    array_agg(kcu.column_name) AS columns,
                    ccu.table_name AS foreign_table_name,
                    array_agg(ccu.column_name) AS foreign_columns
                FROM
                    information_schema.table_constraints AS tc
                JOIN information_schema.key_column_usage AS kcu
                    ON tc.constraint_name = kcu.constraint_name
                    AND tc.table_schema = kcu.table_schema
                JOIN information_schema.constraint_column_usage AS ccu
                    ON ccu.constraint_name = tc.constraint_name
                    AND ccu.table_schema = tc.table_schema
                WHERE
                    tc.constraint_type = 'FOREIGN KEY'
                    AND tc.table_name = '{self.table}'
                GROUP BY
                    ccu.table_name, tc.constraint_name;
            ''')
        }
