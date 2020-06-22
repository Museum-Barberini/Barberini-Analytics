from ast import literal_eval

import luigi
import pandas as pd

from data_preparation import DataPreparationTask


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

    def output(self):

        return luigi.LocalTarget(
            f'{self.output_dir}/{self.task_id}.csv',
            format=luigi.format.UTF8
        )

    def run(self):

        # Unpack luigi-serialized parameter
        args = literal_eval(self.args)

        query = self.build_query()
        rows, columns = self.db_connector.query_with_header(query, *args)
        df = pd.DataFrame(rows, columns=columns)
        with self.output().open('w') as output_stream:
            df.to_csv(output_stream, index=False, header=True)

    def build_query(self):

        query = self.query
        if self.shuffle:
            query += ' ORDER BY RANDOM()'
        if self.minimal_mode and self.limit == -1:
            self.limit = 50
        if self.limit and self.limit != -1:
            query += f' LIMIT {self.limit}'
        return query
