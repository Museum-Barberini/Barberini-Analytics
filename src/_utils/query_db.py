import luigi
import pandas as pd

from data_preparation_task import DataPreparationTask
from db_connector import db_connector


class QueryDb(DataPreparationTask):

    limit = luigi.parameter.IntParameter(
        default=None,
        description="The maximum number posts to fetch. Optional. If None, "
                    "all posts will be fetched.")

    shuffle = luigi.BoolParameter(
        default=False,
        description="If True, all posts will be shuffled. For debugging and "
                    "exploration purposes. Might impact performance.")

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.db_connector = db_connector
        self.host = self.db_connector.host
        self.database = self.db_connector.database
        self.user = self.db_connector.user
        self.password = self.db_connector.password

    host = database = user = password = None

    def build_query(self):
        query = self.query
        if self.shuffle:
            query += 'ORDER BY RANDOM() '
        if self.limit:
            query += f'LIMIT {self.limit} '
        return query

    def run(self):
        query = self.build_query()
        rows, columns = self.db_connector.query_with_header(query)
        df = pd.DataFrame(rows, columns=columns)
        with self.output().open('w') as output_stream:
            df.to_csv(output_stream, index=False, header=True)
