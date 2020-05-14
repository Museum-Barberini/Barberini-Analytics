import copy
import logging
import os
import psycopg2

from typing import Callable, List, Tuple, TypeVar

logger = logging.getLogger('luigi-interface')

T = TypeVar('T')


class DbConnector:

    def __init__(self, host, user, database, password):
        super().__init__()
        # crucial to avoid unintended access to default postgres database
        assert database, "Database was not specified"
        self.host = host
        self.user = user
        self.database = database
        self.password = password

    def execute(self, *queries: List[str]) -> List[Tuple]:
        """
        Execute one or multiple queries as one atomic operation and returns
        the results of all queries. If any query fails, all will be reverted
        and an error will be raised.
        """
        return list(self._execute_queries(
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

    def _create_connection(self):
        return psycopg2.connect(
            host=self.host,
            database=self.database,
            user=self.user,
            password=self.password
        )

    def _execute_queries(
                self,
                queries: List[str],
                result_function: Callable[[psycopg2.extensions.cursor], T]
            ) -> List[T]:
        """
        Executes all passed queries as one atomic operation and yields the
        results of each query. If any query fails, all will be reverted and an
        error will be raised.
        Note that this is a generator function so the operation will be only
        commited once the generator has been enumerated.
        """
        conn = self._create_connection()
        try:
            with conn:
                with conn.cursor() as cur:
                    for query in queries:
                        logger.debug(f"DbConnector: Executing query: {query}")
                        cur.execute(query)
                        yield result_function(cur)
        finally:
            conn.close()

    def _execute_query(self, query: str, result_function: Callable) -> None:
        """
        Executes the passed query and returns the results.
        Note that this is a generator function so the operation will be only
        commited once the generator has been enumerated.
        """
        return self._execute_queries([query], result_function)


def db_connector():
    connector = copy.copy(default_connector())
    connector.database = os.environ['POSTGRES_DB']
    return connector


def default_connector():
    return DbConnector(
        host=os.environ['POSTGRES_HOST'],
        database='postgres',
        user=os.environ['POSTGRES_USER'],
        password=os.environ['POSTGRES_PASSWORD'])


def register_array_type(type_name, namespace_name):
    """
    Register the specified postgres type manually, allowing psycopg2 to parse
    arrays of that type correctly.
    If custom types are not configured, queries such as
        c.query("select array ['pg_type'::information_schema.sql_identifier]")
    will be answered with strings like '{pg_type}' rather than with a true
    array of objects.
    """
    connector = default_connector()
    typarray, typcategory = connector.query(f'''
            SELECT typarray, typcategory
            FROM pg_type
            JOIN pg_namespace
                ON typnamespace = pg_namespace.oid
            WHERE typname ILIKE '{type_name}'
                AND nspname ILIKE '{namespace_name}'
        ''', only_first=True)
    psycopg2.extensions.register_type(
        psycopg2.extensions.new_array_type(
            (typarray,),
            f'{type_name}[]',
            {'S': psycopg2.STRING}[typcategory]
        ))
