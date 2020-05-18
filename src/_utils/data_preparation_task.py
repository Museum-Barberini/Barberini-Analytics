import logging
import pandas as pd
import os
import sys
from functools import reduce
from typing import Callable, Dict, Tuple

import luigi

import db_connector

logger = logging.getLogger('luigi-interface')

minimal_mode = os.getenv('MINIMAL') == 'True'
OUTPUT_DIR = os.environ['OUTPUT_DIR']


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
                invalid_values_handler: Callable[
                        [pd.DataFrame, Tuple[str, str], pd.DataFrame], None
                    ] = None
            ) -> pd.DataFrame:
        """
        Note that this currently only works with lower case identifiers.
        """
        def log_invalid_values(
                invalid_values, foreign_key, original_values):
            column, _ = foreign_key
            original_count = len(df[column])
            logger.warning(
                f"Skipped {len(invalid_values)} out of {original_count} rows "
                f"due to foreign key violation: {foreign_key}")
            print(
                f"Following values were invalid:\n{invalid_values}"
                if sys.stdout.isatty() else
                "Values not printed for privacy reasons")

        def filter_invalid_values(df, foreign_key):
            if df.empty:
                return df

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
                log_invalid_values(invalid_values, foreign_key, df)
                if invalid_values_handler:
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
                WHERE
                    tc.constraint_type = 'FOREIGN KEY'
                    AND tc.table_name = '{self.table}';
            ''')
        }

    def iter_verbose(self, iterable, msg, size=None, index_fun=None):
        if not sys.stdout.isatty():
            yield from iterable
            return
        if isinstance(msg, str):
            yield from self.iter_verbose(
                iterable,
                msg=lambda: msg, size=size, index_fun=index_fun
            )
            return
        if size is None:
            size = len(iterable) if hasattr(iterable, '__len__') else None
        format_args = {
            'item': str(None),
            'index': 0
        }
        if size:
            format_args['size'] = size
        print()
        try:
            for index, item in enumerate(iterable, start=1):
                if index_fun:
                    index = index_fun() + 1
                format_args['index'] = index
                format_args['item'] = item
                message = msg().format(**format_args)
                message = f'\r{message} ... '
                if size:
                    message += f'({(index - 1) / size :2.1%})'
                print(message, end=' ', flush=True)
                yield item
            print(f'\r{msg().format(**format_args)} ... done.', flush=True)
        except Exception:
            print(flush=True)
            raise

    def loop_verbose(
            self, while_fun, item_fun, msg, size=None, index_fun=None):
        def loop():
            while while_fun():
                yield item_fun()
        return self.iter_verbose(loop(), msg, size=size, index_fun=index_fun)
