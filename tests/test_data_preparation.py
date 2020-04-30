import unittest
from unittest.mock import patch, PropertyMock

import pandas as pd
from unittest.mock import patch, PropertyMock

from data_preparation_task import DataPreparationTask
from task_test import DatabaseTaskTest

TABLE_NAME = 'test_table'
COLUMN_NAME = 'test_column'


class TestDataPreparationTask(DatabaseTaskTest):
    created_tables = []

    def tearDown(self):
        try:
            for table in self.created_tables:
                self.db_connector.execute(f'DROP TABLE {table}')
        finally:
            super().tearDown()

    @patch.object(DataPreparationTask, 'table', new_callable=PropertyMock)
    def test_ensure_foreign_keys(self, table_name_mock):
        table_name_mock.return_value = TABLE_NAME
        self.db_connector.execute(
            query=f'CREATE TABLE {TABLE_NAME} ({COLUMN_NAME} INT)')
        self.created_tables.append(TABLE_NAME)
        self.db_connector.execute(
            f'''
            ALTER TABLE {TABLE_NAME}
                ADD CONSTRAINT {TABLE_NAME}_primkey
                PRIMARY KEY ({COLUMN_NAME});
            ''',
            f'''
            ALTER TABLE {TABLE_NAME}
                ADD CONSTRAINT {TABLE_NAME}_{COLUMN_NAME}_fkey
                FOREIGN KEY ({COLUMN_NAME})
                REFERENCES {TABLE_NAME} ({COLUMN_NAME})
            ''',
            f'INSERT INTO {TABLE_NAME} VALUES (0)')

        df = pd.DataFrame([[0], [1]], columns=[COLUMN_NAME])

        # Expected behaviour: 1 is removed because it is not found in the DB
        expected_df = pd.DataFrame(
            [[0]],
            columns=[COLUMN_NAME],
            index=[0])
        expected_invalid_values = pd.DataFrame(
            [[1]],
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
