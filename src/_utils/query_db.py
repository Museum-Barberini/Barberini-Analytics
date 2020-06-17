from ast import literal_eval
import logging
import os
import re
import signal
from time import sleep

import luigi
import pandas as pd

from data_preparation import DataPreparationTask

logger = logging.getLogger('luigi-interface')


class QueryDb(DataPreparationTask):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.args = str(self.args)

    query = luigi.Parameter(
        description="The SQL query to perform on the DB"
    )

    # Don't use a ListParameter here to preserve free typing of arguments
    # TODO: Fix warnings
    args = luigi.Parameter(
        default=(),
        description="The SQL query's parameters"
    )

    limit = luigi.parameter.IntParameter(
        default=-1,
        description="The maximum number posts to fetch. Optional. If -1, "
                    "all posts will be fetched.")

    shuffle = luigi.BoolParameter(
        default=False,
        description="If True, all posts will be shuffled. For debugging and "
                    "exploration purposes. Might impact performance.")

    report_progress_function_pattern = re.compile(
        r'\/\*<REPORT_PROGRESS>\*\/(["\w\.]+)'
    )

    report_progress_row_interval = 1000

    def build_query(self):
        query = self.query
        if self.shuffle:
            query += ' ORDER BY RANDOM()'
        if self.minimal_mode and self.limit == -1:
            self.limit = 50
        if self.limit and self.limit != -1:
            query += f' LIMIT {self.limit}'
        return query

    def output(self):
        return luigi.LocalTarget(
            f'{self.output_dir}/{self.task_id}.csv',
            format=luigi.format.UTF8
        )

    def run(self):

        query = self.build_query()
        self.report_progress(self._run, query)

    def _run(self, query):

        # Unpack luigi-serialized parameter
        args = literal_eval(self.args)

        rows, columns = self.db_connector.query_with_header(query, *args)
        df = pd.DataFrame(rows, columns=columns)
        with self.output().open('w') as output_stream:
            return df.to_csv(output_stream, index=False, header=True)

    # TODO: Support message string parameter (save in list and increment
    # sequence index). When printing, start new line if value if message has
    # changed.
    def report_progress(self, fun, query):

        has_reporter = False
        progress_names = []
        cleanups = []

        def compile_progress(match):

            nonlocal has_reporter
            has_reporter = True

            table_name = match.group(1)
            progress_name = f'progress_{len(progress_names)}_{id(self)}'[:58]
            progress_names.append(progress_name)

            for sequence in [progress_name, f'max_{progress_name}']:
                self.db_connector.execute(f'''
                    CREATE SEQUENCE {sequence} START 1
                ''')
                cleanups.append((
                    lambda sequence_closure:
                    # See https://stackoverflow.com/q/2295290
                    lambda: self.db_connector.execute(f'''
                        DROP SEQUENCE {sequence_closure}
                    '''))(sequence))

            return rf'''(
                SELECT * FROM (
                    SELECT _table.* FROM (
                        SELECT _table.*, ROW_NUMBER() OVER() _row_number
                        FROM {table_name} _table
                    ) _table
                    WHERE CASE
                        WHEN _row_number = 1 THEN SETVAL(
                            'max_{progress_name}',
                            (SELECT COUNT(*) FROM {table_name}) + 1,
                            true
                        ) <> 0
                        WHEN MOD(
                            _row_number,
                            {self.progress_auto_update_interval}
                        ) = 0 THEN SETVAL(
                            '{progress_name}',
                            NEXTVAL('{progress_name}') +
                                {self.report_progress_row_interval - 1},
                            true
                        ) <> 0
                        ELSE TRUE
                    END
                ) _table
            ) "{table_name}"'''

        try:
            complex_query = self.report_progress_function_pattern.sub(
                compile_progress,
                query
            )
            if not has_reporter:
                return fun(complex_query)

            pid = os.fork()
            # No zombie children
            signal.signal(signal.SIGCHLD, signal.SIG_IGN)

            if pid:  # we are parent
                try:
                    result = fun(complex_query)
                    print("\nDone.", flush=True)
                    return result
                finally:
                    os.kill(pid, signal.SIGTERM)

            # we are child
            print()
            while True:
                progresses = [
                    self.db_connector.query(
                        f'''
                            SELECT
                                CAST(current.last_value AS int),
                                CAST(max.last_value AS int) - 1
                            FROM
                                {progress_name} current,
                                max_{progress_name} max
                        ''',
                        only_first=True
                    )
                    for progress_name
                    in progress_names
                ]
                progress_strings = [
                    f"{current}/{max}, {current / max :2.1%}"
                    for current, max in progresses
                    if max
                ]
                print(
                    f"\rExecuting query ..." + (
                        f' ({" | ".join(progress_strings)})'
                        if progress_strings
                        else ""),
                    end=''
                )
                sleep(1)

        finally:
            while cleanups:
                cleanups.pop()()
