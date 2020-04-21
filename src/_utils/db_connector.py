import logging
import os
import psycopg2

from typing import Callable, List, Tuple

logger = logging.getLogger('luigi-interface')


class DbConnector:

    def __init__(self, host, user, database, password):
        super().__init__()
        self.host = host
        self.user = user
        self.database = database
        self.password = password

    def execute(self, *queries: List[str]) -> None:
        """
        Execute one or multiple queries. Use this function when you don't
        care about the result of the query, e.g. for DELETE.
        """
        list(self._execute_queries(
            queries=queries,
            result_function=lambda cur: None
        ))

    def exists(self, query: str) -> bool:
        """
        Check if the given query returns any results. Return
        True if the query returns results, otherwise False.
        Note that the given query should absolutely not end on a semicolon.
        """
        return bool(self.query(
            query=f'SELECT EXISTS({query})',
            only_first=True)[0])

    def query(self, query: str, only_first: bool = False) -> List[Tuple]:
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

        results = self._execute_query(
            query=query,
            result_function=result_function
        )
        result = next(results)
        if next(results, result) is not result:
            raise AssertionError(
                "DB access with just one query should only return one result")
        return result

    def _execute_queries(
                self,
                queries: List[str],
                result_function: Callable
            ) -> None:
        """
        Note that this is a generator function!
        """
        conn = psycopg2.connect(
            host=self.host,
            database=self.database,
            user=self.user,
            password=self.password
        )
        try:
            with conn:
                with conn.cursor() as cur:
                    for query in queries:
                        logger.debug(f'Executing query: {query}')
                        cur.execute(query)
                        yield result_function(cur)
        finally:
            conn.commit()

    def _execute_query(self, query: str, result_function: Callable) -> None:
        """
        Note that this is a generator function!
        """
        return self._execute_queries([query], result_function)


db_connector = DbConnector(
    host=os.environ['POSTGRES_HOST'],
    database=os.environ['POSTGRES_DB'],
    user=os.environ['POSTGRES_USER'],
    password=os.environ['POSTGRES_PASSWORD'])
