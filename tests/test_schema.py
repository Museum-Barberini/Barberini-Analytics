import logging
import os
import subprocess as sp

from db_test import DatabaseTestCase, _perform_query

logger = logging.getLogger('luigi-interface')


class TestSchema(DatabaseTestCase):

    db_name = 'barberini_test_schema'

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.setup_minimal_database()

    @classmethod
    def tearDownClass(cls):
        try:
            os.environ['POSTGRES_DB_TEMPLATE'] = cls.outer_template_name
            _perform_query(f'DROP DATABASE {cls.db_name}')
        finally:
            result = super().tearDownClass()
        return result

    @classmethod
    def setup_minimal_database(cls):
        cls.outer_template_name = os.getenv('POSTGRES_DB_TEMPLATE')
        os.environ.update(POSTGRES_DB_TEMPLATE=cls.db_name)
        _perform_query(f'''
            CREATE DATABASE {os.environ['POSTGRES_DB_TEMPLATE']}
            TEMPLATE {cls.outer_template_name}
        ''')
        logger.info("Fetching posts in minimal pipeline mode")
        sp.run(
            check=True,
            args='''make
                luigi-restart-scheduler
                luigi-clean
                luigi-task LMODULE=posts LTASK=PostsToDb
                luigi-clean
            '''.split(),
            env=dict(
                os.environ,
                POSTGRES_DB=cls.db_name,
                MINIMAL=str(True),
                OUTPUT_DIR=f'output_{cls.db_name}'
            )
        )
        _perform_query(f'''
            ALTER DATABASE {cls.db_name}
            SET default_transaction_read_only = true;
        ''')

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
                "Google Play", "Apple Appstore"
            },
            sources
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
            msg=f"The columns (source, post_id) is no unique key for the "
                f"following sources: {invalid_sources}"
        )
