import luigi
import luigi.format

from query_db import QueryDb


class FetchPosts(QueryDb):

    def output(self):
        return luigi.LocalTarget('output/posts.csv', format=luigi.format.UTF8)

    query = f'''
        SELECT source, post_id, text
        FROM post
        WHERE text <> ''
    '''
