from unittest.mock import MagicMock

from luigi.format import UTF8
from luigi.mock import MockTarget
import pandas as pd
from unittest.mock import patch

from data_preparation import ConcatCsvs, DataPreparationTask
from db_test import DatabaseTestCase

TABLE_NAME = 'test_table'
COLUMN_NAME = 'test_column'
COLUMN_NAME_2 = 'test_column_2'

TABLE_NAME_FOREIGN = 'test_table_foreign'
TABLE_NAME_FOREIGN_2 = 'test_table_foreign_2'
COLUMN_NAME_FOREIGN = 'test_column_foreign'
COLUMN_NAME_FOREIGN_2 = 'test_column_foreign_2'


class TestDataPreparationTask(DatabaseTestCase):

    def test_filter_fkey_violations_one_column(self):
        self.db_connector.execute(
            f'''CREATE TABLE {TABLE_NAME_FOREIGN} (
                {COLUMN_NAME_FOREIGN} INT PRIMARY KEY,
                {COLUMN_NAME_FOREIGN_2} TEXT
            )''',
            f'''CREATE TABLE {TABLE_NAME} (
                {COLUMN_NAME} INT
                    REFERENCES {TABLE_NAME_FOREIGN}
                    ({COLUMN_NAME_FOREIGN}),
                {COLUMN_NAME_2} TEXT
            )''',
            f'''INSERT INTO {TABLE_NAME_FOREIGN} VALUES (
                0, 'z'
            )'''
        )
        self.assertFilterFkeyViolationsOnce(
            df=pd.DataFrame(
                [[0, 'a'], [0, 'b'], [1, 'a'], [1, 'b']],
                columns=[COLUMN_NAME, COLUMN_NAME_2]),
            expected_valid=pd.DataFrame(
                [[0, 'a'], [0, 'b']],
                columns=[COLUMN_NAME, COLUMN_NAME_2],
                index=[0, 1]),
            expected_invalid=pd.DataFrame(
                [[1, 'a'], [1, 'b']],
                columns=[COLUMN_NAME, COLUMN_NAME_2],
                index=[2, 3]),
            expected_foreign_key=(
                [COLUMN_NAME], TABLE_NAME_FOREIGN, [COLUMN_NAME_FOREIGN]
            )
        )

    def test_filter_fkey_violations_multiple_columns(self):
        self.db_connector.execute(
            f'''CREATE TABLE {TABLE_NAME_FOREIGN} (
                {COLUMN_NAME_FOREIGN_2} TEXT,
                {COLUMN_NAME_FOREIGN} INT,
                PRIMARY KEY ({COLUMN_NAME_FOREIGN_2}, {COLUMN_NAME_FOREIGN})
            )''',
            f'''CREATE TABLE {TABLE_NAME} (
                {COLUMN_NAME} INT,
                {COLUMN_NAME_2} TEXT,
                FOREIGN KEY ({COLUMN_NAME}, {COLUMN_NAME_2})
                    REFERENCES {TABLE_NAME_FOREIGN}
                    ({COLUMN_NAME_FOREIGN}, {COLUMN_NAME_FOREIGN_2})
            )''',
            f'''INSERT INTO {TABLE_NAME_FOREIGN} VALUES
                ('a', 0),
                ('b', 1)
            '''
        )
        self.assertFilterFkeyViolationsOnce(
            df=pd.DataFrame(
                [[0, 'a'], [0, 'b'], [1, 'a'], [1, 'b']],
                columns=[COLUMN_NAME, COLUMN_NAME_2]),
            expected_valid=pd.DataFrame(
                [[0, 'a'], [1, 'b']],
                columns=[COLUMN_NAME, COLUMN_NAME_2],
                index=[0, 3]),
            expected_invalid=pd.DataFrame(
                [[0, 'b'], [1, 'a']],
                columns=[COLUMN_NAME, COLUMN_NAME_2],
                index=[1, 2]),
            expected_foreign_key=(
                [COLUMN_NAME, COLUMN_NAME_2],
                TABLE_NAME_FOREIGN,
                [COLUMN_NAME_FOREIGN, COLUMN_NAME_FOREIGN_2]
            )
        )

    def test_filter_fkey_violations_multiple_constraints(self):
        self.db_connector.execute(
            f'''CREATE TABLE {TABLE_NAME_FOREIGN} (
                {COLUMN_NAME_FOREIGN} INT PRIMARY KEY
            )''',
            f'''CREATE TABLE {TABLE_NAME_FOREIGN_2} (
                {COLUMN_NAME_FOREIGN_2} TEXT PRIMARY KEY
            )''',
            f'''CREATE TABLE {TABLE_NAME} (
                {COLUMN_NAME} INT
                    REFERENCES {TABLE_NAME_FOREIGN} ({COLUMN_NAME_FOREIGN}),
                {COLUMN_NAME_2} TEXT
                    REFERENCES {TABLE_NAME_FOREIGN_2}
                    ({COLUMN_NAME_FOREIGN_2})
            )''',
            f'''INSERT INTO {TABLE_NAME_FOREIGN} VALUES (0)''',
            f'''INSERT INTO {TABLE_NAME_FOREIGN_2} VALUES ('a')'''
        )
        self.assertFilterFkeyViolations(
            df=pd.DataFrame(
                [[0, 'a'], [0, 'b'], [1, 'a'], [1, 'b']],
                columns=[COLUMN_NAME, COLUMN_NAME_2]),
            expected_valid=pd.DataFrame(
                [[0, 'a']],
                columns=[COLUMN_NAME, COLUMN_NAME_2],
                index=[0]),
            expected_foreign_keys=[
                (
                    [COLUMN_NAME],
                    TABLE_NAME_FOREIGN,
                    [COLUMN_NAME_FOREIGN]
                ),
                (
                    [COLUMN_NAME_2],
                    TABLE_NAME_FOREIGN_2,
                    [COLUMN_NAME_FOREIGN_2]
                )
            ]
        )

    def test_filter_fkey_violations_one_of_multiple_constraints(self):
        self.db_connector.execute(
            f'''CREATE TABLE {TABLE_NAME_FOREIGN} (
                {COLUMN_NAME_FOREIGN} INT PRIMARY KEY
            )''',
            f'''CREATE TABLE {TABLE_NAME_FOREIGN_2} (
                {COLUMN_NAME_FOREIGN_2} TEXT PRIMARY KEY
            )''',
            f'''CREATE TABLE {TABLE_NAME} (
                {COLUMN_NAME} INT
                    REFERENCES {TABLE_NAME_FOREIGN} ({COLUMN_NAME_FOREIGN}),
                {COLUMN_NAME_2} TEXT
                    REFERENCES {TABLE_NAME_FOREIGN_2}
                    ({COLUMN_NAME_FOREIGN_2})
            )''',
            f'''INSERT INTO {TABLE_NAME_FOREIGN} VALUES (0)''',
            f'''INSERT INTO {TABLE_NAME_FOREIGN_2} VALUES
                ('a'),
                ('b')
            '''
        )
        self.assertFilterFkeyViolationsOnce(
            df=pd.DataFrame(
                [[0, 'a'], [0, 'b'], [1, 'a'], [1, 'b']],
                columns=[COLUMN_NAME, COLUMN_NAME_2]),
            expected_valid=pd.DataFrame(
                [[0, 'a'], [0, 'b']],
                columns=[COLUMN_NAME, COLUMN_NAME_2],
                index=[0, 1]),
            expected_invalid=pd.DataFrame(
                [[1, 'a'], [1, 'b']],
                columns=[COLUMN_NAME, COLUMN_NAME_2],
                index=[2, 3]),
            expected_foreign_key=(
                [COLUMN_NAME], TABLE_NAME_FOREIGN, [COLUMN_NAME_FOREIGN]
            )
        )

    def test_filter_fkey_violations_self_reference(self):
        self.db_connector.execute(
            f'''CREATE TABLE {TABLE_NAME} (
                {COLUMN_NAME} INT PRIMARY KEY,
                {COLUMN_NAME_2} INT REFERENCES {TABLE_NAME} ({COLUMN_NAME})
            )''',
            f'''INSERT INTO {TABLE_NAME} VALUES (0, NULL)'''
        )
        self.assertFilterFkeyViolationsOnce(
            df=pd.DataFrame(
                [[2, 1], [4, 3], [1, 0]],
                columns=[COLUMN_NAME, COLUMN_NAME_2]),
            expected_valid=pd.DataFrame(
                [[2, 1], [1, 0]],
                columns=[COLUMN_NAME, COLUMN_NAME_2],
                index=[0, 2]),
            expected_invalid=pd.DataFrame(
                [[4, 3]],
                columns=[COLUMN_NAME, COLUMN_NAME_2],
                index=[1]),
            expected_foreign_key=(
                [COLUMN_NAME_2], TABLE_NAME, [COLUMN_NAME]
            )
        )

    def test_filter_fkey_violations_no_foreign_keys(self):
        self.db_connector.execute(
            f'''CREATE TABLE {TABLE_NAME_FOREIGN} (
                {COLUMN_NAME_FOREIGN} INT PRIMARY KEY
            )''',
            f'''CREATE TABLE {TABLE_NAME} (
                {COLUMN_NAME} INT
            )''',
            f'INSERT INTO {TABLE_NAME_FOREIGN} VALUES (0)'
        )
        self.denyEnsureForeignKeys(
            df=pd.DataFrame([[0], [1]], columns=[COLUMN_NAME]),
            expected_valid=pd.DataFrame(
                [[0], [1]],
                columns=[COLUMN_NAME],
                index=[0, 1])
        )

    def test_filter_fkey_violations_empty_df(self):
        self.db_connector.execute(
            f'''CREATE TABLE {TABLE_NAME_FOREIGN} (
                {COLUMN_NAME_FOREIGN} INT PRIMARY KEY
            )''',
            f'''CREATE TABLE {TABLE_NAME} (
                {COLUMN_NAME} INT
                    REFERENCES {TABLE_NAME_FOREIGN} ({COLUMN_NAME_FOREIGN})
            )''',
            f'INSERT INTO {TABLE_NAME_FOREIGN} VALUES (0)'
        )
        self.denyEnsureForeignKeys(
            df=pd.DataFrame([], columns=[COLUMN_NAME]),
            expected_valid=pd.DataFrame([], columns=[COLUMN_NAME])
        )

    def test_filter_fkey_violations_null_reference(self):
        self.db_connector.execute(
            f'''CREATE TABLE {TABLE_NAME_FOREIGN} (
                {COLUMN_NAME_FOREIGN} TEXT,
                {COLUMN_NAME_FOREIGN_2} TEXT,
                PRIMARY KEY ({COLUMN_NAME_FOREIGN}, {COLUMN_NAME_FOREIGN_2})
            )''',
            f'''CREATE TABLE {TABLE_NAME} (
                {COLUMN_NAME} TEXT,
                {COLUMN_NAME_2} TEXT,
                FOREIGN KEY ({COLUMN_NAME}, {COLUMN_NAME_2})
                    REFERENCES {TABLE_NAME_FOREIGN}
                    ({COLUMN_NAME_FOREIGN}, {COLUMN_NAME_FOREIGN_2})
            )''',
            f'''INSERT INTO {TABLE_NAME_FOREIGN} VALUES (
                'a', 'A'
            )'''
        )
        self.assertFilterFkeyViolationsOnce(
            df=pd.DataFrame(
                [
                    ['a', 'A'], ['a', 'b'], [None, 'A'], [None, 'B'],
                    ['a', None], ['b', None], [None, None]
                ],
                columns=[COLUMN_NAME, COLUMN_NAME_2]),
            expected_valid=pd.DataFrame(
                [
                    ['a', 'A'], [None, 'A'], [None, 'B'],
                    ['a', None], ['b', None], [None, None]
                ],
                columns=[COLUMN_NAME, COLUMN_NAME_2],
                index=[0, 2, 3, 4, 5, 6]),
            expected_invalid=pd.DataFrame(
                [['a', 'b']],
                columns=[COLUMN_NAME, COLUMN_NAME_2],
                index=[1]),
            expected_foreign_key=(
                [COLUMN_NAME, COLUMN_NAME_2],
                TABLE_NAME_FOREIGN,
                [COLUMN_NAME_FOREIGN, COLUMN_NAME_FOREIGN_2]
            )
        )

    def test_filter_fkey_violations_error_if_all_values_discarded(self):
        self.task = DataPreparationTask(table=TABLE_NAME)
        self.db_connector.execute(
            f'''CREATE TABLE {TABLE_NAME_FOREIGN} (
                {COLUMN_NAME_FOREIGN} INT PRIMARY KEY
            )''',
            f'''CREATE TABLE {TABLE_NAME} (
                {COLUMN_NAME} INT
                    REFERENCES {TABLE_NAME_FOREIGN} ({COLUMN_NAME_FOREIGN})
            )''')
        # no values are inserted into DB prior
        print(self.db_connector.query(f"SELECT * FROM {TABLE_NAME_FOREIGN}"))
        with self.assertRaisesRegex(
                ValueError,
                "All values have been discarded "
                "due to foreign key violation!"):
            self.task.filter_fkey_violations(
                pd.DataFrame([[0], [1]], columns=[COLUMN_NAME])
            )

    def assertFilterFkeyViolations(
            self, df, expected_valid, expected_foreign_keys):
        self.task = DataPreparationTask(table=TABLE_NAME)

        actual_foreign_keys = []

        def handle_invalid_values(invalid, foreign_key):
            actual_foreign_keys.append(foreign_key)
        handle_invalid_values = MagicMock(side_effect=handle_invalid_values)

        actual_df = self.task.filter_fkey_violations(df, handle_invalid_values)

        pd.testing.assert_frame_equal(expected_valid, actual_df)
        self.assertEqual(
            len(expected_foreign_keys),
            handle_invalid_values.call_count)
        self.assertCountEqual(expected_foreign_keys, actual_foreign_keys)

    def assertFilterFkeyViolationsOnce(
            self, df, expected_valid, expected_invalid, expected_foreign_key):
        self.task = DataPreparationTask(table=TABLE_NAME)

        def handle_invalid_values(invalid, foreign_key):
            pd.testing.assert_frame_equal(expected_invalid, invalid)
            self.assertEqual(expected_foreign_key, foreign_key)
        handle_invalid_values = MagicMock(side_effect=handle_invalid_values)

        actual_df = self.task.filter_fkey_violations(df, handle_invalid_values)

        pd.testing.assert_frame_equal(expected_valid, actual_df)
        handle_invalid_values.assert_called_once()

    def denyEnsureForeignKeys(self, df, expected_valid):
        self.task = DataPreparationTask(table=TABLE_NAME)
        handle_invalid_values = MagicMock()

        actual_df = self.task.filter_fkey_violations(df, handle_invalid_values)

        pd.testing.assert_frame_equal(expected_valid, actual_df)
        handle_invalid_values.assert_not_called()

    def test_condense_performance_values(self):
        timestamp_column = 'timestamp_name'
        self.db_connector.execute(
            f'''CREATE TABLE {TABLE_NAME} (
                {COLUMN_NAME} TEXT,
                {COLUMN_NAME_2} INT,
                {timestamp_column} TIMESTAMP,
                PRIMARY KEY ({COLUMN_NAME}, {timestamp_column})
            )''',
            f'''INSERT INTO {TABLE_NAME} VALUES
                ('0', 1, '2020-05-01 00:00:00'),
                ('0', 2, '2020-05-02 00:00:00'),
                ('1', 2, '2020-05-01 00:00:00'),
                ('1', 2, '2020-05-02 00:00:00'),
                ('2', 4, '2020-05-01 00:00:00')
            '''
        )

        df = pd.DataFrame([
            ['0', 3, '2020-05-03 00:00:00'],
            ['1', 2, '2020-05-03 00:00:00'],
            ['2', 3, '2020-05-03 00:00:00']],
            columns=[COLUMN_NAME, COLUMN_NAME_2, timestamp_column])

        expected_df = pd.DataFrame([
            ['0', 3, '2020-05-03 00:00:00'],
            ['2', 3, '2020-05-03 00:00:00']],
            columns=[COLUMN_NAME, COLUMN_NAME_2, timestamp_column])

        self.task = DataPreparationTask(table=TABLE_NAME)
        actual_df = self.task.condense_performance_values(
            df,
            timestamp_column=timestamp_column)

        pd.testing.assert_frame_equal(expected_df, actual_df)

    def test_input_df_is_unchanged_filter_fkey_violations(self):
        self.db_connector.execute(
            f'''CREATE TABLE {TABLE_NAME_FOREIGN} (
                {COLUMN_NAME_FOREIGN} INT PRIMARY KEY,
                {COLUMN_NAME_FOREIGN_2} TEXT
            )''',
            f'''CREATE TABLE {TABLE_NAME} (
                {COLUMN_NAME} INT
                    REFERENCES {TABLE_NAME_FOREIGN}
                    ({COLUMN_NAME_FOREIGN}),
                {COLUMN_NAME_2} TEXT
            )''',
            f'''INSERT INTO {TABLE_NAME_FOREIGN} VALUES (
                0, 'z'
            )'''
        )

        df = pd.DataFrame(
            [[0, 'a'], [0, 'b'], [1, 'a'], [1, 'b']],
            columns=[COLUMN_NAME, COLUMN_NAME_2])
        df_copy = df.copy()

        self.task = DataPreparationTask(table=TABLE_NAME)
        self.task.filter_fkey_violations(df)
        pd.testing.assert_frame_equal(df, df_copy)

    def test_input_df_is_unchanged_condense_performance_values(self):
        timestamp_column = 'timestamp'
        self.db_connector.execute(
            f'''CREATE TABLE {TABLE_NAME} (
                {COLUMN_NAME} TEXT,
                {COLUMN_NAME_2} INT,
                {timestamp_column} TIMESTAMP,
                PRIMARY KEY ({COLUMN_NAME}, timestamp)
            )''',
            f'''INSERT INTO {TABLE_NAME} VALUES
                ('0', 1, '2020-05-01 00:00:00'),
                ('0', 2, '2020-05-02 00:00:00'),
                ('1', 2, '2020-05-01 00:00:00'),
                ('1', 2, '2020-05-02 00:00:00'),
                ('2', 4, '2020-05-01 00:00:00')
            '''
        )

        df = pd.DataFrame([
            ['0', 3, '2020-05-03 00:00:00'],
            ['1', 2, '2020-05-03 00:00:00'],
            ['2', 3, '2020-05-03 00:00:00']],
            columns=[COLUMN_NAME, COLUMN_NAME_2, timestamp_column])

        df_copy = df.copy()
        self.task = DataPreparationTask(table=TABLE_NAME)
        self.task.condense_performance_values(df)
        pd.testing.assert_frame_equal(df, df_copy)


class TestConcatCsvs(DatabaseTestCase):

    @patch.object(ConcatCsvs, 'output')
    @patch.object(ConcatCsvs, 'input')
    def test_one(self, input_mock, output_mock):

        df_in = pd.DataFrame([[1, 'foo'], [2, 'bar']], columns=['a', 'b'])
        self.install_mock_target(
            input_mock,
            lambda file: df_in.to_csv(file, index=False)
        )
        output_target = MockTarget(str(self))
        output_mock.return_value = output_target

        self.task = ConcatCsvs()
        self.run_task(self.task)
        with output_target.open('r') as output:
            df_out = pd.read_csv(output)

        pd.testing.assert_frame_equal(df_in, df_out)

    @patch.object(ConcatCsvs, 'output')
    @patch.object(ConcatCsvs, 'input')
    def test_two(self, input_mock, output_mock):

        df_in0 = pd.DataFrame(
            [[1, 'foo'], [2, 'bar']],
            columns=['a', 'b']
        )
        df_in1 = pd.DataFrame(
            [[42, 'spam'], [1337, 'häm']],
            columns=['a', 'b']
        )
        input_mock.return_value = [
            self.install_mock_target(
                MagicMock(),
                lambda file: df.to_csv(file, index=False)
            ) for df in [df_in0, df_in1]]
        output_target = MockTarget(str(self), format=UTF8)
        output_mock.return_value = output_target

        self.task = ConcatCsvs()
        self.run_task(self.task)
        with output_target.open('r') as output:
            df_out = pd.read_csv(output)

        df_expected = pd.DataFrame(
            [
                [1, 'foo'], [2, 'bar'],
                [42, 'spam'], [1337, 'häm'],
            ],
            columns=['a', 'b']
        )
        pd.testing.assert_frame_equal(df_expected, df_out)
