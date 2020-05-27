import logging
import pandas as pd
import os
import sys
from functools import reduce
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

    def ensure_performance_change(
            self,
            df,
            key_column,
            performance_columns,
            timestamp_column='timestamp'):

        def find_latest_performance(latest_performances, search_id):
            # Assumption: 'latest_performances' is ordered
            # according to the id -> binary search
            if not latest_performances:
                return None
            no_of_entries = len(latest_performances)
            if no_of_entries == 1:
                if latest_performances[0][0] == search_id:
                    return latest_performances[0][1:]
                else:  # search_id was not found
                    return None

            half_index = no_of_entries // 2
            half_id = latest_performances[half_index][0]
            if half_id == search_id:
                return latest_performances[half_index][1:]
            if half_id > search_id:
                return find_latest_performance(
                    latest_performances[:half_index], search_id)
            else:  # half_id < search_id
                return find_latest_performance(
                    latest_performances[half_index + 1:], search_id)

        if not self.table:
            # This is the case for tests
            return df

        # Read latest performance data from DB
        latest_performances = self.db_connector.query(
            f'''
            SELECT {key_column},{','.join(performance_columns)}
            FROM {self.table} AS p1
                NATURAL JOIN (
                    SELECT {key_column}, MAX({timestamp_column}) AS timestamp
                    FROM {self.table}
                    GROUP BY {key_column}
                ) AS p2
            ORDER BY {key_column} ASC
            '''
        )

        # For each new entry, check against most recent
        # performance data -> only keep if it changed
        to_keep = pd.DataFrame(columns=df.columns)
        for i, row in df.iterrows():
            latest_performance = find_latest_performance(
                latest_performances, str(row[key_column]))
            if not latest_performance or \
                    latest_performance != tuple(
                        row[performance_columns].tolist()):
                to_keep.loc[i] = row
        org_count = df[key_column].count()
        logger.info(f"Discarded {org_count - to_keep[key_column].count()} "
                    f"unchanged performance values out of {org_count} "
                    f"for {self.table}")
        return to_keep.reset_index(drop=True)

    def ensure_foreign_keys(
                self,
                df: pd.DataFrame,
                invalid_values_handler: Callable[[
                        pd.DataFrame,
                        Tuple[str, Tuple[List[str], str, List[str]]],
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
        if not self.table:
            # This is the case for tests
            return {}

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
