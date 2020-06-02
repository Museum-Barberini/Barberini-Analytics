import logging
import os
import sys
from functools import reduce
from typing import Callable, Dict, List, Tuple

import luigi
import pandas as pd

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

    def condense_performance_values(
            self,
            df,
            timestamp_column='timestamp'):

        def get_key_columns():
            primary_key_columns = self.db_connector.query(
                f'''
                SELECT a.attname, format_type(a.atttypid, a.atttypmod)
                    AS data_type
                FROM   pg_index i
                JOIN   pg_attribute a ON a.attrelid = i.indrelid
                                    AND a.attnum = ANY(i.indkey)
                WHERE  i.indrelid = '{self.table}'::regclass
                AND    i.indisprimary
                ''')
            return [
                row[0]
                for row in primary_key_columns
                if row[0] != timestamp_column]

        def get_performance_columns():
            nonlocal key_columns
            # This simply returns all columns except the key
            # and timestamp columns of the target table
            db_columns = self.db_connector.query(
                f'''
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = \'{self.table}\'
                ''')
            return [
                row[0]
                for row in db_columns
                if row[0] not in key_columns and row[0] != timestamp_column]

        if not self.table:
            raise RuntimeError("Table not set in condense_performance_values")

        # Read latest performance data from DB
        key_columns = get_key_columns()
        performance_columns = get_performance_columns()
        query_keys = ','.join(key_columns)
        latest_performances = self.db_connector.query(
            f'''
            SELECT {query_keys},{','.join(performance_columns)}
            FROM {self.table} AS p1
                NATURAL JOIN (
                    SELECT {query_keys}, MAX({timestamp_column})
                        AS {timestamp_column}
                    FROM {self.table}
                    GROUP BY {query_keys}
                ) AS p2
            '''
        )

        # For each new entry, check against most recent
        # performance data -> drop if it didn't change
        latest_performance_df = pd.DataFrame(
            latest_performances, columns=[*key_columns, *performance_columns])

        new_suffix = '_new'
        old_suffix = '_old'
        merge_result = pd.merge(
            df,
            latest_performance_df,
            how='left',  # keep all new data + preserve index
            on=key_columns,
            suffixes=(new_suffix, old_suffix))

        org_count = df[key_columns[0]].count()
        to_drop = []
        new_values = merge_result[[
            f'{perf_col}{new_suffix}'
            for perf_col in performance_columns]]
        old_values = merge_result[[
            f'{perf_col}{old_suffix}'
            for perf_col in performance_columns]]

        # Cut off suffixes to enable Series comparison
        new_values.columns = [
            label[:-len(new_suffix)]
            for label in new_values.columns]
        old_values.columns = [
            label[:-len(old_suffix)]
            for label in old_values.columns]

        for i, new_row in new_values.iterrows():
            # The dtypes of the DataFrames get messed up
            # sometimes, so we cast to object for safety
            new_row = new_row.astype(object)
            old_row = old_values.loc[i].astype(object)
            if new_row.equals(old_row):
                to_drop.append(i)

        logger.info(f"Discard {len(to_drop)} unchanged performance "
                    f"values out of {org_count} for {self.table}")
        return df.drop(index=to_drop).reset_index(drop=True)

    def filter_fkey_violations(
                self,
                df: pd.DataFrame,
                invalid_values_handler: Callable[[
                        pd.DataFrame,
                        Tuple[str, Tuple[List[str], str, List[str]]],
                        pd.DataFrame
                    ], None] = None
            ) -> pd.DataFrame:

        if not self.table:
            raise RuntimeError("Table not set in filter_fkey_violations")

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

        def filter_invalid_values(values, constraint):
            if values.empty:
                return values

            _, (columns, foreign_table, foreign_columns) = constraint

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
            if foreign_table == self.table:
                new_foreign_values = values[foreign_columns].rename(
                    columns=dict(zip(foreign_columns, _foreign_columns)))
                new_foreign_values.reset_index(inplace=True, drop=True)
                foreign_values = foreign_values.append(new_foreign_values)

            # Remove all rows from the df where the value does not match any
            # value from the referenced table
            valid = pd.merge(
                    values.reset_index(),
                    foreign_values,
                    left_on=columns, right_on=_foreign_columns,
                    how='left'
                ).drop_duplicates(
                ).set_index(
                    values.index
                ).apply(
                    lambda row:
                        # Null reference
                        row[columns].isnull().any() or
                        # Left merge successful
                        not row[_foreign_columns].isnull().all(),
                    axis=1
                )
            valid_values, invalid_values = values[valid], values[~valid]
            if not invalid_values.empty:
                log_invalid_values(invalid_values, constraint)
                if invalid_values_handler:
                    invalid_values_handler(invalid_values, constraint[1])

            return valid_values

        return reduce(
            filter_invalid_values,
            self.foreign_key_constraints().items(),
            df)

    def foreign_key_constraints(self) -> Dict[
                str,
                Tuple[List[str], str, List[str]]
            ]:
        return {
            constraint_name: (columns, foreign_table, foreign_columns)
            for [constraint_name, columns, foreign_table, foreign_columns]
            in self.db_connector.query(f'''
                WITH foreign_keys AS (
                    SELECT
                        rc.constraint_name,
                        kcu.column_name,
                        kcu_foreign.table_name AS foreign_table_name,
                        kcu_foreign.column_name AS foreign_column_name
                    FROM
                        information_schema.referential_constraints rc
                    JOIN information_schema.key_column_usage kcu
                        ON kcu.constraint_name = rc.constraint_name
                    JOIN information_schema.key_column_usage kcu_foreign
                        ON kcu_foreign.constraint_name
                            = rc.unique_constraint_name
                        AND kcu_foreign.ordinal_position
                            = kcu.position_in_unique_constraint
                    WHERE kcu.table_name = '{self.table}'
                    GROUP BY
                        rc.constraint_name,
                        kcu_foreign.table_name,
                        kcu.ordinal_position,
                        kcu.column_name,
                        kcu_foreign.column_name
                    ORDER BY kcu.ordinal_position
                )
                SELECT
                    constraint_name,
                    array_agg(column_name),
                    foreign_table_name,
                    array_agg(foreign_column_name)
                FROM foreign_keys
                GROUP BY constraint_name, foreign_table_name;
            ''')
        }

    def iter_verbose(self, iterable, msg, size=None, index_fun=None):
        """
        Iterate over an iterable, but as a side effect, print progress updates
        to the console if appropriate.
        Neglecting any outputs, this method is equivalent to:
            def iter_verbose(self, iterable):
                return iterable
        """
        if not sys.stdout.isatty():
            yield from iterable
            return
        if isinstance(msg, str):
            yield from self.iter_verbose(
                iterable,
                msg=lambda: msg, size=size, index_fun=index_fun
            )
            return
        size = size if size \
            else len(iterable) if hasattr(iterable, '__len__') \
            else None
        format_args = {
            'item': str(None),
            'index': 0
        }
        if size:
            format_args['size'] = size
        print()
        try:
            iterable = iter(iterable)
            index = 0
            item = next(iterable)
            while True:
                index = index_fun() + 1 if index_fun else index + 1
                forth = yield item
                item = iterable.send(forth) \
                    if hasattr(iterable, 'send') \
                    else next(iterable)

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

    def loop_verbose(self, msg, size=None):
        """
        Stream an infinite generator that prints progress updates to the
        console if appropriate. Use next() to increment the progress or
        .send() to specify the current index.
        """
        index = 0

        def loop():
            nonlocal index
            while True:
                next_index = yield index
                index = next_index if next_index else index + 1
        return self.iter_verbose(
            loop(), msg, size=size, index_fun=lambda: index)
