import validators

from db_test import DatabaseTestCase


class CheckAbsa(DatabaseTestCase):
    """
    Make quick assertions about the schema. Meant to be run after the database
    has been filled, e. g. after a minimal pipeline run has been executed.
    Requires $POSTGRES_DB_TEMPLATE to point to a prepared database.
    NOTE that if these tests get too slow, we could override setup_database to
    prevent copying the database for each read-only access. However, at the
    moment YAGNI.
    """

    def test_post_aspect_sentiment(self):

        counts = {
            count for [count] in self.db_connector.query('''
                SELECT DISTINCT count(*)
                FROM absa.post_aspect_sentiment
                GROUP BY (
                    source, post_id, aspect_id,
                    aspect_match_algorithm,
                    dataset, sentiment_match_algorithm,
                    sentiment_model
                )
            ''')
        }

        self.assertCountEqual(
            {1},
            counts,
            msg="post_aspect_sentiment contains duplicate rows"
        )
