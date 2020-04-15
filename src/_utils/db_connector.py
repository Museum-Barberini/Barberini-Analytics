import logging
import os
import psycopg2

from typing import Callable, List, Tuple

logger = logging.getLogger('luigi-interface')


class DbConnector:

    host = os.environ['POSTGRES_HOST']
    database = os.environ['POSTGRES_DB']
    user = os.environ['POSTGRES_USER']
    password = os.environ['POSTGRES_PASSWORD']

    @classmethod
    def query(cls, query: str, only_first: bool = False) -> List[Tuple]:
        """
        Execute a query and return a list of results.
        If only_first is set to True, only return the
        first result as a tuple.
        """
        def result_function(cursor):
            nonlocal only_first
            if only_first:
                return cursor.fetchone()
            return cursor.fetchall()

        return cls._execute_query(
            query=query,
            result_function=result_function
        )

    @classmethod
    def execute(cls, query: str) -> None:
        """
        Execute a query. Use this function when you don't
        care about the result of the query, e.g. for DELETE.
        """
        cls._execute_query(
            query=query,
            result_function=lambda cur: None
        )

    @classmethod
    def exists(cls, query: str) -> bool:
        """
        Check if the given query returns any results. Return
        True if the query returns results, otherwise False.
        Note that the given query should absolutely not end on a semicolon.
        """
        return cls._execute_query(
            query=f'SELECT EXISTS({query})',
            result_function=lambda cur: cur.fetchone()[0] is True
        )

    @classmethod
    def _execute_query(cls, query: str, result_function: Callable):

        conn = psycopg2.connect(
            host=cls.host, database=cls.database,
            user=cls.user, password=cls.password
        )
        try:
            with conn:
                with conn.cursor() as cur:
                    logger.debug(f'Executing query: {query}')
                    cur.execute(query)
                    return result_function(cur)
        finally:
            conn.close()
