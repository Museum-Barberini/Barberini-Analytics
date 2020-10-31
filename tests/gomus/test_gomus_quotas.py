from unittest.mock import patch

import luigi
from luigi.format import UTF8
from luigi.mock import MockTarget
import pandas as pd

from db_test import DatabaseTestCase
from gomus.quotas import ExtractQuotas, FetchQuotas
from gomus._utils.fetch_htmls import FailableTarget, FetchGomusHTML


class TestExtractQuotas(DatabaseTestCase):
    """Tests the gomus ExtractQuotas task."""

    @patch.object(ExtractQuotas, 'input')
    def test_extract_quotas(self, input_mock):

        self.task = ExtractQuotas()
        input_mock.return_value = luigi.LocalTarget(
            'tests/test_data/gomus/quotas/quotas_in.csv',
            format=UTF8)

        self.task.run()

        expected_quotas = pd.read_csv(
            'tests/test_data/gomus/quotas/quotas_out.csv')
        with self.task.output().open() as output:
            actual_quotas = pd.read_csv(output)
        pd.testing.assert_frame_equal(expected_quotas, actual_quotas)


class TestFetchQuotas(DatabaseTestCase):
    """Tests the gomus FetchQuotas task."""

    def test_fetch_quotas(self):

        self.task = FetchQuotas()
        self.task.max_missing_ids = 3
        mock_codes = [
            404, 200, 200, 404, 404, 404, 200, 200, 200, 200, 404, 404, 404,
            404, 200]

        self.iter_task(mock_codes, max_index=13)

        with self.task.output().open() as output:
            output_df = pd.read_csv(output)

        pd.testing.assert_frame_equal(
            pd.DataFrame([
                {'file_path': f'quota_{i}.html'}
                for i in [1, 2, 6, 7, 8, 9]]),
            output_df)

    def test_http_error(self):

        self.task = FetchQuotas()
        self.task.max_missing_ids = 3
        mock_codes = [404, 200, 200, 404, 404, 404, 200, 500, 200]

        with self.assertRaises(ValueError):
            self.iter_task(mock_codes, max_index=7)

        self.assertFalse(self.task.complete())

    def iter_task(self, mock_codes, max_index):

        gen = self.task.run()
        dep = next(gen)
        for i, code in enumerate(mock_codes):
            self.assertIsInstance(dep, FetchGomusHTML)
            self.assertLessEqual(i, max_index)

            if 200 <= code < 300:
                target = MockTarget(f'quota_{i}.html')
            elif code in dep.ignored_status_codes:
                target = MockTarget(f'quota_{i}.html.error')
            else:
                raise ValueError("Unhandled status code")
            with target.open('w'):
                pass

            try:
                dep = gen.send(FailableTarget(target))
            except StopIteration:
                dep = None
                break
        self.assertFalse(dep)
