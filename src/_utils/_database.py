"""Provides helper classes for connecting to databases."""

import os
from typing import Callable, Dict, Iterable, List, Tuple, TypeVar, Union

import psycopg2

import _utils
logger = _utils.logger

T = TypeVar('T')
TQueryAndArgs = Tuple[
    str,
    Union[Iterable[object], Dict[str, object]]
]


class DbConnector:

    def __init__(self, host, user, database, password):

        super().__init__()
        self.host = host
        self.user = user
        self.database = database
        self.password = password

    @property
    def database(self):

        return self.__database

    @database.setter
    def database(self, database):

        # crucial to avoid unintended access to default postgres database
        assert database, "Database was not specified"
        assert (
            database == 'postgres'
            or os.getenv('BARBERINI_ANALYTICS_CONTEXT') == 'PRODUCTION'
            or 'test' in database
        ), (
            "Unexpected access to production database was blocked!\n To "
            "modify production database manually, set "
            "BARBERINI_ANALYTICS_CONTEXT to the PRODUCTION constant."
        )

        self.__database = database

    def __repr__(self):
        """Compute formal string representation of the receiver."""
        return (
            f'{type(self).__name__}('
            f'host={self.host}, '
            f'user={self.user}, '
            f'database={self.database})'
        )

    def __str__(self):
        """Compute familiar string representation of the receiver."""
        return f'{type(self).__name__}(db={self.database})'

    def execute(
            self,
            *queries: List[Union[str, TQueryAndArgs]]
            ) -> List[Tuple]:
        """
        Execute all queries as one transaction and return their results.

        If any query fails, all queries will be reverted before an error is
        raised.
        """
        return list(self._execute_queries(
            queries_and_args=queries,
            result_function=lambda cur: None
        ))

    def exists(self, query: str) -> bool:
        """
        Check if the given query returns any results.

        Note that the given query should absolutely not end on a semicolon.
        """
        return bool(self.query(
            query=f'SELECT EXISTS({query})',
            only_first=True)[0])

    def exists_table(self, table: str) -> bool:
        """Check if the given table is present in the database."""
        return self.exists(f'''
                SELECT * FROM information_schema.tables
                WHERE LOWER(table_name) = LOWER('{table}')
            ''')  # nosec B608

    def query(
            self,
            query: str,
            *args: Iterable[object],
            only_first: bool = False,
            **kwargs: Dict[str, object]
            ) -> List[Tuple]:
        """
        Execute a query and return a list of results.

        If only_first is set to True, only return the first result as a tuple.
        """
        def result_function(cursor):
            nonlocal only_first
            if only_first:
                return cursor.fetchone()
            return cursor.fetchall()

        results = self._execute_query(
            query=query,
            result_function=result_function,
            args=args,
            kwargs=kwargs
        )
        result = next(results)
        if next(results, result) is not result:
            raise AssertionError(
                "DB access with just one query should only return one result")
        return result

    def query_with_header(
            self,
            query: str,
            *args: Iterable[object],
            **kwargs: Dict[str, object]
            ) -> List[Tuple]:
        """Execute a query and return all rows and their column names."""
        all_results = self._execute_query(
            query=query,
            result_function=lambda cursor:
                (cursor.fetchall(), [desc[0] for desc in cursor.description]),
            args=args,
            kwargs=kwargs
        )
        results = next(all_results)
        if next(all_results, results) is not results:
            raise AssertionError(
                "DB access with just one query should only return one table")
        return results

    def _create_connection(self):

        return psycopg2.connect(
            host=self.host,
            database=self.database,
            user=self.user,
            password=self.password
        )

    def _execute_queries(
            self,
            queries_and_args: List[Union[str, TQueryAndArgs]],
            result_function: Callable[[psycopg2.extensions.cursor], T]
            ) -> List[T]:
        """
        Execute all queries as one transaction and yield their results.

        If any query fails, all queries will be reverted before an error is
        raised. Note that this is a generator function so the operation will
        be only commited once the generator has been enumerated completely.
        """
        conn = self._create_connection()
        try:
            with conn:
                try:
                    with conn.cursor() as cur:
                        for query_and_args in queries_and_args:
                            query, args = \
                                query_and_args \
                                if isinstance(query_and_args, tuple) \
                                else (query_and_args, ())
                            logger.debug(
                                "DbConnector: Executing query '''%s''' "
                                "with args: %s",
                                query,
                                args
                            )
                            try:
                                cur.execute(query, args)
                            except psycopg2.Error:
                                print(query, args)
                                raise
                            yield result_function(cur)
                finally:
                    for notice in conn.notices:
                        logger.warning(notice.strip())
        finally:
            conn.close()

    def _execute_query(
            self,
            query: str,
            result_function: Callable[[psycopg2.extensions.cursor], T],
            args: Iterable[object] = (),
            kwargs: Dict[str, object] = {}
            ) -> None:
        """
        Execute the passed query and returns the results.

        Note that this is a generator function so the operation will be only
        commited once the generator has been enumerated.
        """
        assert not args or not kwargs, "cannot combine args and kwargs"
        all_args = next(
            filter(bool, [args, kwargs]),
            # always pass any args for consistent resolution of percent
            # escapings
            None
        )

        return self._execute_queries([(query, all_args)], result_function)


def db_connector(database=None):
    """Create a connector to the default production database."""
    connector = default_connector()
    if database is None:
        database = os.environ['POSTGRES_DB']
    connector.database = database
    return connector


def default_connector():
    """Create a connector to the default postgres database."""
    return DbConnector(
        host=os.environ['POSTGRES_HOST'],
        database='postgres',
        user=os.environ['POSTGRES_USER'],
        password=os.environ['POSTGRES_PASSWORD'])


def register_array_type(type_name, namespace_name):
    """
    Register the postgres type manually to parse parse arrays correctly.

    If custom types are not configured, queries such as
        c.query("select array ['pg_type'::information_schema.sql_identifier]")
    will be answered with strings like '{pg_type}' rather than with a true
    array of objects.
    """
    connector = default_connector()
    typarray, typcategory = connector.query(
        '''
            SELECT typarray, typcategory
            FROM pg_type
            JOIN pg_namespace
                ON typnamespace = pg_namespace.oid
            WHERE typname ILIKE %(type_name)s
                AND nspname ILIKE %(namespace_name)s
        ''',
        only_first=True,
        type_name=type_name,
        namespace_name=namespace_name
    )
    psycopg2.extensions.register_type(
        psycopg2.extensions.new_array_type(
            (typarray,),
            f'{type_name}[]',
            {'S': psycopg2.STRING}[typcategory]
        ))
