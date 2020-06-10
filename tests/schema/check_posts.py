from db_test import DatabaseTestCase


class CheckPosts(DatabaseTestCase):
    """
    Make quick assertions about the schema. Meant to be run after the database
    has been filled, e. g. after a minimal pipeline run has been executed.
    Requires $POSTGRES_DB_TEMPLATE to point to a prepared database.
    NOTE that if these tests get too slow, we could override setup_database to
    prevent copying the database for each read-only access. However, at the
    moment YAGNI.
    """

    def test_post_sources(self):

        sources = [
            source for [source] in self.db_connector.query('''
                SELECT DISTINCT(source)
                FROM post
            ''')]

        self.assertCountEqual(
            {
                "Facebook Post", "Facebook Comment",
                "Google Maps",
                "Instagram",
                "Google Play", "Apple Appstore",
                "Twitter"
            },
            sources,
            msg="Expected sources are not met by post view"
        )

    def test_post_unique(self):

        invalid_sources = {
            source for [source, post_id, count] in self.db_connector.query('''
                SELECT source, post_id, COUNT(*)
                FROM post
                GROUP BY source, post_id
                HAVING COUNT(*) <> 1
            ''')}

        self.assertFalse(
            invalid_sources,
            msg=f"Key (source, post_id) is a duplicate for the following "
                f"sources: {invalid_sources}"
        )

    def test_permalink(self):

        invalid_sources = self.db_connector.query('''
            SELECT source, COUNT(post_id)
            FROM post
            WHERE permalink IS NULL
            GROUP BY source
        ''')

        self.assertFalse(
            invalid_sources,
            msg=f"Permalinks are missing (partially?) for the following "
                f"sources: {invalid_sources}"
        )
