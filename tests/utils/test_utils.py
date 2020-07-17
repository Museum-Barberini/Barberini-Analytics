from unittest.mock import MagicMock, patch

from luigi.format import UTF8
from luigi.mock import MockTarget
import pandas as pd

from _utils.utils import ConcatCsvs
from db_test import DatabaseTestCase


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
