from contextlib import contextmanager
import logging
import os

from db_test import DatabaseTestCase, _perform_query
from posts import PostsToDb

logger = logging.getLogger('luigi-interface')


class TestSchema(DatabaseTestCase):

    db_name = 'barberini_test_schema'

    @classmethod
    def setUpClass(cls):

        super().setUpClass()
        cls.setup_minimal_database()

    @classmethod
    def tearDownClass(cls):
        """
        Revert changes from setup_minimal_database().
        (Maybe we should implement a class-wise cleanUp system in suitable 🤓)
        """

        try:
            os.environ['POSTGRES_DB_TEMPLATE'] = cls.outer_template_name
            _perform_query(f'DROP DATABASE {cls.db_name}')
        finally:
            result = super().tearDownClass()
        return result

    @classmethod
    def setup_minimal_database(cls):
        """
        All schema tests will perform read-only queries on a non-empty post
        database. For performance reasons, fill this database once and
        store a reference to it into POSTGRES_DB_TEMPLATE which is used by
        super().setUp(). This method has been copied and adapted from
        DatabaseTestSuite.setup_minimal_database().
        """

        cls.outer_template_name = os.getenv('POSTGRES_DB_TEMPLATE')
        os.environ['POSTGRES_DB_TEMPLATE'] = cls.db_name
        _perform_query(f'''
            CREATE DATABASE {os.environ['POSTGRES_DB_TEMPLATE']}
            TEMPLATE {cls.outer_template_name}
        ''')
        logger.info("Fetching posts in minimal pipeline mode")
        with _push_env(
                POSTGRES_DB=cls.db_name,
                MINIMAL=str(True),
                OUTPUT_DIR=f'output_{cls.db_name}'):
            cls.run_task_externally(PostsToDb)
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


@contextmanager
def _push_env(**kwargs):
    """
    Temporarily add the specified variables to the environment.
    """

    env_backup = os.environ
    os.environ.update(kwargs)
    try:
        yield
    finally:
        for key in kwargs:
            if key in env_backup:
                os.environ[key] = env_backup[key]
            else:
                os.environ.pop(key)
