import sys
from functools import reduce
from typing import Callable, Dict, List, Tuple

import luigi
import pandas as pd
from tqdm import tqdm

from ._database import register_array_type
import _utils

logger = _utils.logger

register_array_type('SQL_IDENTIFIER', 'information_schema')


class DataPreparationTask(luigi.Task):
    """The abstract superclass of all tasks preparing data for a DB table."""

    def __init__(self, *args, **kwargs):

        super().__init__(*args, **kwargs)
        self.db_connector = _utils.db_connector()

    table = luigi.parameter.OptionalParameter(
        description="The name of the table the data should be prepared for",
        default=None)

    minimal_mode = luigi.parameter.BoolParameter(
        default=_utils.minimal_mode(),
        description="If True, only a minimal amount of data will be prepared"
                    "in order to test the pipeline for structural problems")

    @property
    def output_dir(self):

        return _utils.output_dir()

    def condense_performance_values(
            self,
            df,
            timestamp_column='timestamp',
            delta_function: Callable[[pd.Series, pd.Series], object] = None):

        if not self.table:
            raise RuntimeError("Table not set in condense_performance_values")

        condenser = PerformanceValueCondenser(
            self.db_connector,
            self.table,
            timestamp_column
        )
        condenser.delta_function = delta_function
        return condenser.condense_performance_values(df)

    def encode_strings(self, df):
        r"""
        Apply necessary encodings to all string columns of the dataframe.

        NOTE that for the most encodings, pandas' to_csv() implementation is
        definitively sufficient and should not be levered out here. In this
        place, we are only handling edge cases that are treated specially by
        luigi. Concretely, this only affects carriage returns (\a) at the
        moment, which otherwise rigorously will be replaced with line feeds
        (\n) by luigi.Task, causing the output CSV file to contain illegal
        line breaks. For more information, see #384 and:
        https://groups.google.com/g/luigi-user/c/tePPe6oJrjk

        Returns an encoded copy of the dataframe.
        """
        df = df.copy()
        for column in df.select_dtypes(include=['object']).columns:
            df[column] = df[column].str.replace(r'\r\n|\r', '\n')
        return df

    def filter_fkey_violations(
            self,
            df: pd.DataFrame,
            invalid_values_handler: Callable[[
                pd.DataFrame,
                Tuple[str, Tuple[List[str], str, List[str]]],
                pd.DataFrame
            ], None] = None
            ) -> pd.DataFrame:
        """
        Filter out values violation any foreign key of the target table.

        NOTE that this currently only works with lower case identifiers.
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
                    row[columns].isnull().any()
                    # Left merge successful
                    or not row[_foreign_columns].isnull().all(),
                axis=1
            )
            valid_values, invalid_values = values[valid], values[~valid]
            if not invalid_values.empty:
                log_invalid_values(invalid_values, constraint)
                if valid_values.empty and not self.minimal_mode:
                    # all data has been skipped, something is fishy
                    # TODO: the check for minimal_mode is a temporary
                    # workaround, see issue #191
                    raise ValueError("All values have been discarded "
                                     "due to foreign key violation!")
                if invalid_values_handler:
                    invalid_values_handler(invalid_values, constraint[1])

            return valid_values

        return reduce(
            filter_invalid_values,
            self.foreign_key_constraints().items(),
            df)

    def foreign_key_constraints(self) -> Dict[
            str, Tuple[List[str], str, List[str]]]:

        if not self.table:
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

    def tqdm(self, iterable, **kwargs):
        """
        Iterate over an iterable, printing progress updates to the console.

        Completely transparent wrapper function for the popular status-
        reporting library tqdm.
        Usage example that loops through an iterable using tqdm:
            for i in tqdm(range(1, 5), desc="Processing list"):
                print(i)
        """
        desc = kwargs.get('desc', None)
        if desc:
            logger.info(desc)
        return tqdm(iterable, **kwargs)


class ConcatCsvs(DataPreparationTask):
    """Concatenate all input CSV files into a single output CSV file."""

    def run(self):

        dfs = [
            self.read_csv(input)
            for input in luigi.task.flatten(self.input())
        ]
        df = pd.concat(dfs)
        with self.output().open('w') as output:
            df.to_csv(output, index=False)

    def read_csv(self, target: luigi.Target):

        with target.open('r') as input:
            return pd.read_csv(input)


class PerformanceValueCondenser():

    def __init__(self, db_connector, table, timestamp_column='timestamp'):
        super().__init__()
        self.db_connector = db_connector
        self.table = table
        self.timestamp_column = timestamp_column

    delta_function = None
    delta_prefix = 'delta_'

    def condense_performance_values(self, df):

        # Read latest performance data from DB
        key_columns = self.get_key_columns()
        performance_columns = self.get_performance_columns(key_columns)
        query_keys = ','.join(key_columns)
        latest_performances = self.db_connector.query(f'''
            SELECT {query_keys}, {', '.join(performance_columns)}
            FROM {self.table} AS p1
                NATURAL JOIN (
                    SELECT {query_keys}, MAX({self.timestamp_column})
                        AS {self.timestamp_column}
                    FROM {self.table}
                    GROUP BY {query_keys}
                ) AS p2
        ''')

        # For each new entry, check against most recent performance data
        # -> drop if it didn't change
        latest_performance_df = pd.DataFrame(
            latest_performances, columns=[*key_columns, *performance_columns])

        new_suffix, old_suffix = '_new', '_old'
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

        deltas = None
        if self.delta_function:
            deltas = []

        # Cut off suffixes to enable Series comparison
        new_values.columns = [
            label[:-len(new_suffix)]
            for label in new_values.columns]
        old_values.columns = [
            label[:-len(old_suffix)]
            for label in old_values.columns]

        for i, new_row in new_values.iterrows():
            # The dtypes of the DataFrames get messed up sometimes, so we cast
            # to object for safety
            new_row = new_row.astype(object)
            old_row = old_values.loc[i].astype(object)
            if new_row.equals(old_row):
                to_drop.append(i)
            elif deltas is not None:
                deltas.append(self.delta_function(old_row, new_row))

        logger.info(f"Discard {len(to_drop)} unchanged performance "
                    f"values out of {org_count} for {self.table}")
        df = df.drop(index=to_drop).reset_index(drop=True)

        if deltas is not None:
            delta_columns = [
                f'{self.delta_prefix}{perf_col}'
                for perf_col in performance_columns
            ]
            df[delta_columns] = pd.DataFrame([
                dict(zip(delta_columns, delta))
                if isinstance(delta, pd.Series)
                else delta
                for delta in deltas
            ])

        return df

    def get_key_columns(self):
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
            if row[0] != self.timestamp_column]

    def get_performance_columns(self, key_columns):
        # This simply returns all columns except the key
        # and timestamp columns of the target table
        db_columns = self.db_connector.query(
            f'''
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = '{self.table}'
            ''')
        return [
            column for (column,) in db_columns
            if column not in key_columns
            and column != self.timestamp_column
            and not column.startswith(self.delta_prefix)]

    @staticmethod
    def linear_delta(old_row, new_row):
        return (new_row - old_row).fillna(0).astype(int)
