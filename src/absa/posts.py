import luigi
import luigi.format
import pandas as pd

from data_preparation_task import DataPreparationTask
from db_connector import db_connector


class FetchPosts(DataPreparationTask):

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

    table = 'post'
    host = database = user = password = None

    def output(self):
        return luigi.LocalTarget('output/posts.csv', format=luigi.format.UTF8)

    @property
    def query(self):
        query = f'''
            SELECT source, post_id, text
            FROM post
            WHERE text <> ''
        '''
        if self.shuffle:
            query += 'ORDER BY RANDOM() '
        if self.limit:
            query += f'LIMIT {self.limit} '
        return query

    def run(self):
        results = pd.DataFrame(
            self.db_connector.query(self.query),
            columns=['source', 'post_id', 'text'])
        with self.output().open('w') as output_stream:
            results.to_csv(output_stream, index=False, header=True)
