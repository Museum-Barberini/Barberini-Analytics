import loggin
import os
import psycopg2

from typing import List, Tuple

logger = logging.getLogger('luigi-interface')


class DbConnector:

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.host     = os.environ['POSTGRES_HOST']
        self.database = os.environ['POSTGRES_DB']
        self.user     = os.environ['POSTGRES_USER']
        self.password = os.environ['POSTGRES_PASSWORD']

    def query(self, query) -> List[Tuple]:
        """
        Execute a query and return a list of results. 
        """
        self._execute_query(
            query=query, 
            result_function=lambda curs: curs.fetchall()
        )

    def execute(self, query) -> None:
        """
        Execute a query. Use this function when you don't 
        care about the result of the query, e.g. for DELETE.
        """
        try:
            self._execute_query(
                query=query,
                result_function=lambda curs: None
            )
        except psycopg2.errors.UndefinedTable:
            # table does not exist
            logger.warning(
                'You are trying to perfom an operation on a table '
                'that does not exist. The query you tried to execute: '
                f'{query}'
            )

    def exists(self, query) -> bool:
        """
        Check if the given query returns any results. True,
        if the query returns results, False otherwise.
        """
        self._execute_query(
            query=f'SELECT EXISTS({query})',
            result_function=lambda curs: curs.fetchone()[0] is True
        )
    
    def _execute_query(self, query: str, result_function: Callable):
        
        conn = self._open_connection()
        try:
            with conn:
                with conn.cursor() as curs:
                    logger.debug(f'Executing query: {query}')
                    curs.execute(query)
                    return result_function(curs)
        finally:
            conn.close()

    def _open_connection(self):

        return psycopg2.connect(
            host=self.host, database=self.database,
            user=self.user, password=self.password
        )
