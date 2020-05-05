from unittest.mock import patch, PropertyMock

import pandas as pd

from data_preparation_task import DataPreparationTask
from task_test import DatabaseTaskTest


class TestDataPreparationTask(DatabaseTaskTest):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.created_tables = []

    def tearDown(self):
        for table in self.created_tables:
            self.db_connector.execute(f'DROP TABLE {table}')
        super().tearDown()

    @patch.object(
        DataPreparationTask,
        'foreign_keys',
        new_callable=PropertyMock)
    def test_ensure_foreign_keys(self, fkeys_mock):
        test_table = 'test_table'
        test_column = 'test_column'

        self.db_connector.execute(
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
        expected_df = pd.DataFrame(
            [['0']],
            columns=[test_column],
            index=[0])
        expected_invalid_values = pd.DataFrame(
            [['1']],
            columns=[test_column],
            index=[1])

        actual_df, actual_invalid_values = \
            DataPreparationTask().ensure_foreign_keys(df)

        pd.testing.assert_frame_equal(expected_df, actual_df)
        pd.testing.assert_frame_equal(
            expected_invalid_values, actual_invalid_values)
