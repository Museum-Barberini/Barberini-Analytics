import logging
import os
import psycopg2

from typing import Callable, List, Tuple

logger = logging.getLogger('luigi-interface')


class DbConnector:

    host     = os.environ['POSTGRES_HOST']
    database = os.environ['POSTGRES_DB']
    user     = os.environ['POSTGRES_USER']
    password = os.environ['POSTGRES_PASSWORD']

    @classmethod
    def query(cls, query) -> List[Tuple]:
        """
        Execute a query and return a list of results. 
        """
        return cls._execute_query(
            query=query, 
            result_function=lambda curs: curs.fetchall()
        )

    @classmethod
    def execute(cls, query) -> None:
        """
        Execute a query. Use this function when you don't 
        care about the result of the query, e.g. for DELETE.
        """
        cls._execute_query(
            query=query,
            result_function=lambda curs: None
        )

    @classmethod
    def exists(cls, query) -> bool:
        """
        Check if the given query returns any results. True,
        if the query returns results, False otherwise.
        Note that the given query should not end on a semicolon.
        """
        return cls._execute_query(
            query=f'SELECT EXISTS({query})',
            result_function=lambda curs: curs.fetchone()[0] is True
        )
    
    @classmethod
    def _execute_query(cls, query: str, result_function: Callable):
        
        conn = psycopg2.connect(
            host=cls.host, database=cls.database,
            user=cls.user, password=cls.password
        )
        try:
            with conn:
                with conn.cursor() as curs:
                    logger.debug(f'Executing query: {query}')
                    curs.execute(query)
                    return result_function(curs)
        finally:
            conn.close()
