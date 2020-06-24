""" from csv_to_db import CsvToDb
from data_preparation import DataPreparationTask
from query_db import QueryDb
from .post_aspects import PostAspectsToDb
from .post_sentiments import PostSentimentsToDb


class PostAspectSentimentsToDb(CsvToDb):

    table = 'absa.post_aspect_sentiment'

    def requires(self):

        yield CollectPostAspectSentiments(table=self.table)


class CollectPostAspectSentiments(QueryDb):

    def requires(self):

        yield PostAspectsToDb()
        yield PostSentimentsToDb()

    @property
    def query(self):

        return f'''
            SELECT
                source, post_id,
                aspect_id,
                avg(sentiment) AS sentiment,
                count(DISTINCT word_index) AS count,
                dataset,
                post_aspect.match_algorithm AS aspect_match_algorithm,
                post_sentiment.match_algorithm AS sentiment_match_algorithm,
                sentiment_model
            FROM absa.post_sentiment
                JOIN absa.post_aspect USING (source, post_id)
            GROUP BY
                source, post_id, aspect_id,
                dataset, aspect_match_algorithm,
                sentiment_match_algorithm, sentiment_model;
        '''
 """
