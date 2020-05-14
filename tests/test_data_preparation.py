import unittest
from unittest.mock import patch, PropertyMock

import pandas as pd

from data_preparation_task import DataPreparationTask
from db_test import DatabaseTestCase

TABLE_NAME = 'test_table'
COLUMN_NAME = 'test_column'
COLUMN_NAME_2 = 'test_column_2'
TABLE_NAME_FOREIGN = 'test_table_foreign'
COLUMN_NAME_FOREIGN = 'test_column_foreign'
COLUMN_NAME_2_FOREIGN = 'test_column_foreign_2'


# TODO: Refactor tests
# TODO: Write test for multiple foreign keys
class TestDataPreparationTask(DatabaseTestCase):

    @patch.object(DataPreparationTask, 'table', new_callable=PropertyMock)
    def test_ensure_foreign_keys_self_reference(self, table_name_mock):
        table_name_mock.return_value = TABLE_NAME
        self.db_connector.execute(
            f'''CREATE TABLE {TABLE_NAME} (
                {COLUMN_NAME} INT PRIMARY KEY
                    REFERENCES {TABLE_NAME} ({COLUMN_NAME})
            )''',
            f'INSERT INTO {TABLE_NAME} VALUES (0)'
        )

        df = pd.DataFrame([[0], [1]], columns=[COLUMN_NAME])

        # Expected behavior: 1 is removed because it is not found in the DB
        expected_df = pd.DataFrame(
            [['0']],
            columns=[COLUMN_NAME],
            index=[0])
        expected_invalid_values = pd.DataFrame(
            [['1']],
            columns=[COLUMN_NAME],
            index=[1])

        def handle_invalid_values(invalid, foreign_key, valid):
            pd.testing.assert_frame_equal(expected_invalid_values, invalid)
            self.assertEqual(
                (COLUMN_NAME, (TABLE_NAME, COLUMN_NAME)),
                foreign_key)
            pd.testing.assert_frame_equal(expected_df, valid)
        handle_invalid_values = unittest.mock.MagicMock(handle_invalid_values)

        self.task = DataPreparationTask()
        actual_df = self.task.ensure_foreign_keys(df, handle_invalid_values)

        pd.testing.assert_frame_equal(expected_df, actual_df)
        handle_invalid_values.assert_called_once()

    @patch.object(DataPreparationTask, 'table', new_callable=PropertyMock)
    def test_ensure_foreign_keys_one_column(self, table_name_mock):
        table_name_mock.return_value = TABLE_NAME
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

        df = pd.DataFrame([[0], [1]], columns=[COLUMN_NAME])

        # Expected behavior: 1 is removed because it is not found in the DB
        expected_df = pd.DataFrame(
            [['0']],
            columns=[COLUMN_NAME],
            index=[0])
        expected_invalid_values = pd.DataFrame(
            [['1']],
            columns=[COLUMN_NAME],
            index=[1])

        def handle_invalid_values(invalid, foreign_key, valid):
            pd.testing.assert_frame_equal(expected_invalid_values, invalid)
            self.assertEqual(
                ([COLUMN_NAME], (TABLE_NAME_FOREIGN, [COLUMN_NAME_FOREIGN])),
                foreign_key
            )
            pd.testing.assert_frame_equal(expected_df, valid)
        handle_invalid_values = unittest.mock.MagicMock(handle_invalid_values)

        self.task = DataPreparationTask()
        actual_df = self.task.ensure_foreign_keys(df, handle_invalid_values)

        pd.testing.assert_frame_equal(expected_df, actual_df)
        handle_invalid_values.assert_called_once()

    @patch.object(DataPreparationTask, 'table', new_callable=PropertyMock)
    def test_ensure_foreign_keys_multiple_columns(self, table_name_mock):
        table_name_mock.return_value = TABLE_NAME
        self.db_connector.execute(
            f'''CREATE TABLE {TABLE_NAME_FOREIGN} (
                {COLUMN_NAME_FOREIGN} INT,
                {COLUMN_NAME_2_FOREIGN} TEXT,
                PRIMARY KEY ({COLUMN_NAME_FOREIGN}, {COLUMN_NAME_2_FOREIGN})
            )''',
            f'''CREATE TABLE {TABLE_NAME} (
                {COLUMN_NAME} INT,
                {COLUMN_NAME_2} TEXT,
                FOREIGN KEY ({COLUMN_NAME}, {COLUMN_NAME_2})
                    REFERENCES {TABLE_NAME_FOREIGN}
                    ({COLUMN_NAME_FOREIGN}, {COLUMN_NAME_2_FOREIGN})
            )''',
            f'''INSERT INTO {TABLE_NAME_FOREIGN} VALUES (0, 'a')'''
        )

        df = pd.DataFrame(
            [[0, 'a'], [0, 'b'], [1, 'a'], [1, 'b']],
            columns=[COLUMN_NAME, COLUMN_NAME_2])

        # Expected behavior: Everything but (0, 'a') is removed
        # because it is not found in the DB
        expected_df = pd.DataFrame(
            [['0', 'a']],
            columns=[COLUMN_NAME, COLUMN_NAME_2],
            index=[0])
        expected_invalid_values = pd.DataFrame(
            [['0', 'b'], ['1', 'a'], ['1', 'b']],
            columns=[COLUMN_NAME, COLUMN_NAME_2],
            index=[1, 2, 3])

        def handle_invalid_values(invalid, foreign_key, valid):
            pd.testing.assert_frame_equal(expected_invalid_values, invalid)
            self.assertEqual(
                ([COLUMN_NAME, COLUMN_NAME_2], (TABLE_NAME_FOREIGN, [
                    COLUMN_NAME_FOREIGN, COLUMN_NAME_2_FOREIGN
                ])),
                foreign_key
            )
            pd.testing.assert_frame_equal(expected_df, valid)
        handle_invalid_values = unittest.mock.MagicMock(handle_invalid_values)

        self.task = DataPreparationTask()
        actual_df = self.task.ensure_foreign_keys(df, handle_invalid_values)

        pd.testing.assert_frame_equal(expected_df, actual_df)
        handle_invalid_values.assert_called_once()
