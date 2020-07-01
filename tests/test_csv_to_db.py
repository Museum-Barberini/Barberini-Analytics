import luigi
from luigi.mock import MockTarget

from csv_to_db import CsvToDb
from db_test import DatabaseTestCase


EXPECTED_DATA = [
    (1, 2, 'abc', 'xy,"z'),
    (3, 42, "i have a\nlinebreak", "and i have some strange \x1E 0x1e char"),
    (2, 10, '678', ',,;abc')
]
EXPECTED_CSV = '''\
id,A,B,C
1,2,abc,"xy,""z"
3,42,"i have a\nlinebreak","and i have some strange \x1e 0x1e char"
2,10,"678",",,;abc"
'''


class TestCsvToDb(DatabaseTestCase):

    def setUp(self):
        super().setUp()

        self.table_name = 'tmp_csv_table'
        self.dummy = DummyWriteCsvToDb(
            table=self.table_name,
            csv=EXPECTED_CSV
        )

        # Set up database
        self.db_connector.execute(
            f'''CREATE TABLE {self.table_name} (
                id int,
                A int,
                B text,
                C text
            )''',
            f'''
                ALTER TABLE {self.table_name}
                ADD PRIMARY KEY (id);
            ''')

    def test_adding_data_to_database_existing_table(self):

        # Set up database samples
        self.db_connector.execute(f'''
                INSERT INTO {self.table_name}
                VALUES (0, 1, 'a', 'b');
            ''')

        # Execute code under test
        self.run_task(self.dummy)

        # Inspect result
        actual_data = self.db_connector.query(
            f'SELECT * FROM {self.table_name}'
        )
        self.assertEqual([(0, 1, 'a', 'b'), *EXPECTED_DATA], actual_data)

    def test_no_duplicates_are_inserted(self):

        # Set up database samples
        self.db_connector.execute(f'''
                INSERT INTO {self.table_name}
                VALUES (1, 2, 'i-am-a-deprecated-value', 'xy,"z')
            ''')

        # Execute code under test
        self.run_task(self.dummy)

        # Inspect result
        actual_data = self.db_connector.query(
            f'SELECT * FROM {self.table_name}')
        self.assertEqual(EXPECTED_DATA, actual_data)

    def test_columns(self):

        self.run_task(self.dummy)

        expected_columns = [
            ('id', 'integer'),
            ('a', 'integer'),
            ('b', 'text'),
            ('c', 'text')
        ]
        actual_columns = list(self.dummy.columns)
        self.assertListEqual(expected_columns, actual_columns)

    def test_primary_key(self):

        # Set up database samples
        constraint_name = 'custom_primary_name'
        self.db_connector.execute(
            f'''
                INSERT INTO {self.table_name}
                VALUES (1, 2, 'i-am-a-deprecated-value', 'xy,"z')
            ''',
            f'''
                ALTER INDEX {self.table_name}_pkey
                RENAME TO {constraint_name}
            '''
        )

        self.assertEqual(constraint_name, self.dummy.primary_constraint_name)

        # Execute code under test
        self.run_task(self.dummy)

        # Inspect result
        actual_data = self.db_connector.query(
            f'SELECT * FROM {self.table_name}')
        self.assertEqual(EXPECTED_DATA, actual_data)

    def test_array_columns(self):

        COMPLEX_DATA = [
            ((1, 2, 3), ['a', 'b', 'c'], "'quoted str'"),
            ((), [], '[bracketed str]')
        ]
        COMPLEX_CSV = '''\
tuple,array,str
"(1,2,3)","['a','b','c']",'quoted str'
(),[],[bracketed str]
'''

        # Set up database
        self.db_connector.execute(
            f'DROP TABLE {self.table_name}',
            f'''CREATE TABLE {self.table_name} (
                ints int[],
                texts text[],
                text text PRIMARY KEY
            )''')

        self.dummy.csv = COMPLEX_CSV

        # Execute code under test
        self.run_task(self.dummy)

        # Inspect result
        actual_data = self.db_connector.query(
            f'SELECT * FROM {self.table_name}'
        )
        self.assertEqual(
            [(list(row[0]), *row[1:]) for row in COMPLEX_DATA],
            actual_data
        )


class DummyFileWrapper(luigi.Task):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.mock_target = MockTarget(
            f'DummyFileWrapperMock{hash(self)}',
            format=luigi.format.UTF8)

    csv = luigi.Parameter()

    def run(self):
        with self.mock_target.open('w') as input_file:
            input_file.write(self.csv)

    def output(self):
        return self.mock_target


class DummyWriteCsvToDb(CsvToDb):

    table = luigi.Parameter()

    csv = luigi.Parameter()

    def requires(self):
        return DummyFileWrapper(csv=self.csv)
