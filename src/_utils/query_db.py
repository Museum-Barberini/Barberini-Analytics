from ast import literal_eval

import luigi
import pandas as pd

from data_preparation import DataPreparationTask


class QueryDb(DataPreparationTask):

    def __init__(self, *args, **kwargs):

        super().__init__(*args, **kwargs)

        try:
            self.args = str(self.args)
        except AttributeError:
            pass  # args was overridden and does not need conversion
        try:
            self.kwargs = str(self.kwargs)
        except AttributeError:
            pass  # kwargs was overridden and does not need conversion

    query = luigi.Parameter(
        description="The SQL query to perform on the DB"
    )

    # Don't use a ListParameter here to preserve free typing of arguments
    # TODO: Fix warnings
    args = luigi.Parameter(
        default=(),
        description="The SQL query's positional arguments"
    )

    # Don't use a DictParameter here to preserve free typing of arguments
    # TODO: Fix warnings
    kwargs = luigi.Parameter(
        default={},
        description="The SQL query's named arguments"
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

        args, kwargs = self.args, self.kwargs
        if isinstance(args, str):
            # Unpack luigi-serialized parameter
            args = literal_eval(args)
        if isinstance(kwargs, str):
            # Unpack luigi-serialized parameter
            kwargs = literal_eval(kwargs)

        query = self.build_query()
        rows, columns = self.db_connector.query_with_header(
            query, *args, **kwargs
        )
        df = pd.DataFrame(rows, columns=columns)

        df = self.transform(df)

        self.write_output(df)

    def build_query(self):

        query = self.query
        if self.shuffle:
            query += ' ORDER BY RANDOM()'
        if self.minimal_mode and self.limit == -1:
            self.limit = 50
        if self.limit and self.limit > -1:
            query += f' LIMIT {self.limit}'
        return query

    def transform(self, df):
        """
        Hook for subclasses.
        """

        return df

    def write_output(self, df):

        with self.output().open('w') as output_stream:
            df.to_csv(output_stream, index=False, header=True)
