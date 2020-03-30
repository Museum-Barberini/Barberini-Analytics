import unittest
from unittest.mock import patch, PropertyMock

import pandas as pd

from data_preparation_task import DataPreparationTask
from task_test import DatabaseHelper


class TestDataPreparationTask(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.db_helper = DatabaseHelper()
        self.created_tables = []

        self.test_db_name = 'barberini_test'

    def setUp(self):
        self.db_helper.setUp()

    def tearDown(self):
        for table in self.created_tables:
            self.db_helper.commit(f'DROP TABLE {table}')
        self.created_tables = []
        self.db_helper.tearDown()

    @patch.object(
        DataPreparationTask,
        'foreign_keys',
        new_callable=PropertyMock)
    @patch.object(
        DataPreparationTask,
        'database',
        new_callable=PropertyMock)
    def test_ensure_foreign_keys(self, database_mock, fkeys_mock):
        database_mock.return_value = self.test_db_name
        test_table = 'test_table'
        test_column = 'test_column'

        self.db_helper.commit(
            f'CREATE TABLE {test_table} ({test_column} INT)',
            f'INSERT INTO {test_table} VALUES (0)'
        )
        self.created_tables.append(test_table)

        fkeys_mock.return_value = [
            {
                'origin_column': f'{test_column}',
                'target_table': f'{test_table}',
                'target_column': f'{test_column}'
            }
        ]

        df = pd.DataFrame([[0], [1]], columns=[test_column])

        # Expected behaviour: 1 is removed because it is not found in the DB
        expected_df = pd.DataFrame([[0]], columns=[test_column])
        expected_invalid_values = pd.DataFrame([[1]], columns=[test_column])

        actual_df, actual_invalid_values = \
            DataPreparationTask().ensure_foreign_keys(df)

        self.assertTrue(expected_df.equals(actual_df))
        self.assertEqual(expected_invalid_values.equals(actual_invalid_values))
