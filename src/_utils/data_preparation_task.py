import logging
import pandas as pd
import sys
from functools import reduce
from typing import Callable, Dict, Tuple

import luigi
import os

import db_connector

logger = logging.getLogger('luigi-interface')

minimal_mode = os.getenv('MINIMAL') == 'True'


class DataPreparationTask(luigi.Task):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.db_connector = db_connector.db_connector()

    minimal_mode = luigi.parameter.BoolParameter(
        default=minimal_mode,
        description="If True, only a minimal amount of data will be prepared"
                    "in order to test the pipeline for structural problems")

    table = luigi.parameter.Parameter(
        description="The name of the table the data should be prepared for",
        default=None)

    def ensure_foreign_keys(
                self,
                df: pd.DataFrame,
                invalid_values_handler: Callable[
                        [pd.DataFrame, Tuple[str, str], pd.DataFrame], None
                    ] = None
            ) -> pd.DataFrame:
        """
        Note that this currently only works with lower case identifiers.
        """
        if not invalid_values_handler:
            def log_invalid_values(
                    invalid_values, foreign_key, original_values):
                column, _ = foreign_key
                original_count = df[column].count()
                logger.warning(
                    f"Skipped {invalid_values.count()} out of "
                    f"{original_count} data sets due to foreign key "
                    f" violation: {foreign_key}")
                print(
                    f"Following values were invalid:\n{invalid_values}"
                    if sys.stdin.isatty()
                    else
                    "Values not printed for privacy reasons")
            return self.ensure_foreign_keys(df, log_invalid_values)

        if df.empty:
            return df  # optimization

        def filter_invalid_values(df, foreign_key):
            column, (foreign_table, foreign_column) = foreign_key

            foreign_values = [
                # cast values to string uniformly to prevent
                # mismatching due to wrong data types
                str(value) for [value] in self.db_connector.query(f'''
                    SELECT {foreign_column}
                    FROM {foreign_table}
                ''')]
            if not isinstance(df[column][0], str):
                df[column] = df[column].apply(str)

            # Remove all rows from the df where the value does not match any
            # value from the referenced table
            filtered_df = df[df[column].isin(foreign_values)]
            invalid_values = df[~df[column].isin(foreign_values)]
            if not invalid_values.empty:
                invalid_values_handler(invalid_values, foreign_key, df)

            return filtered_df

        return reduce(filter_invalid_values, self.foreign_keys().items(), df)

    def foreign_keys(self) -> Dict[str, Tuple[str, str]]:
        if not self.table:
            return {}

        return {
            column: (foreign_table, foreign_column)
            for [column, foreign_table, foreign_column]
            in self.db_connector.query(f'''
                --- CREDITS: https://stackoverflow.com/a/1152321
                SELECT
                    kcu.column_name,
                    ccu.table_name AS foreign_table_name,
                    ccu.column_name AS foreign_column_name
                FROM
                    information_schema.table_constraints AS tc
                    JOIN information_schema.key_column_usage AS kcu
                    ON tc.constraint_name = kcu.constraint_name
                    AND tc.table_schema = kcu.table_schema
                    JOIN information_schema.constraint_column_usage AS ccu
                    ON ccu.constraint_name = tc.constraint_name
                    AND ccu.table_schema = tc.table_schema
                WHERE tc.constraint_type = 'FOREIGN KEY'
                    AND tc.table_name = '{self.table}';
            ''')
        }
