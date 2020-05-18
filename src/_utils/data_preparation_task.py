import logging
import pandas as pd
import operator
import os
import sys
from functools import partial, reduce
from typing import Callable, Dict, List, Tuple

import luigi

import db_connector

logger = logging.getLogger('luigi-interface')

minimal_mode = os.getenv('MINIMAL') == 'True'
OUTPUT_DIR = os.environ['OUTPUT_DIR']

db_connector.register_array_type('SQL_IDENTIFIER', 'information_schema')


class DataPreparationTask(luigi.Task):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.db_connector = db_connector.db_connector()

    table = luigi.parameter.OptionalParameter(
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
        def log_invalid_values(invalid_values, foreign_key):
            logger.warning(
                f"Skipped {len(invalid_values)} out of {len(df)} rows "
                f"due to foreign key violation: {foreign_key}")
            print(
                f"Following values were invalid:\n{invalid_values}"
                if sys.stdout.isatty() else
                "Values not printed for privacy reasons")

        def filter_invalid_values(values, foreign_key):
            if values.empty:
                return values

            columns, (foreign_table, foreign_columns) = foreign_key
            # Convert first component (columns) from tuple to list
            columns, foreign_columns = list(columns), list(foreign_columns)
            foreign_key = columns, (foreign_table, foreign_columns)

            _foreign_columns = [
                f'{foreign_table}.{column}'
                for column
                in foreign_columns
            ]
            foreign_values = pd.DataFrame(
                self.db_connector.query(f'''
                    SELECT {', '.join(foreign_columns)}
                    FROM {foreign_table}
                '''),
                columns=_foreign_columns
            ).astype(dict(zip(_foreign_columns, values.dtypes[columns])))

            # Remove all rows from the df where the value does not match any
            # value from the referenced table
            invalid = pd.merge(
                    values, foreign_values,
                    left_on=columns, right_on=_foreign_columns,
                    how='left'
                )[_foreign_columns] \
                .isnull() \
                .apply(partial(reduce, operator.or_), axis=1)
            valid_values, invalid_values = values[~invalid], values[invalid]
            if not invalid_values.empty:
                log_invalid_values(invalid_values, foreign_key)
                if invalid_values_handler:
                    invalid_values_handler(invalid_values, foreign_key)

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
                    array_agg(DISTINCT kcu.column_name) AS columns,
                    ccu.table_name AS foreign_table_name,
                    array_agg(DISTINCT ccu.column_name) AS foreign_columns
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
