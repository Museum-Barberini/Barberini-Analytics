from unittest.mock import patch, MagicMock

import luigi
from luigi.format import UTF8
import pandas as pd

from db_test import DatabaseTestCase
from gomus._utils.fetch_htmls import FetchGomusHTML
from gomus.quotas import ExtractQuotas, FetchQuotaIds


class TestExtractQuotas(DatabaseTestCase):
    """Tests the gomus ExtractQuotas task."""

    @patch.object(ExtractQuotas, 'input')
    def test_extract_mock(self, input_mock):
        """Give the task some mock data and test how it parses them."""
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

    @patch.object(ExtractQuotas, 'input')
    def test_extract_production(self, input_mock):
        """Give the task some production data and test how it parses them."""
        self.task = ExtractQuotas()
        html_task = FetchGomusHTML(url='/admin/quotas/26')
        self.run_task(html_task)
        self.install_mock_target(
            input_mock,
            lambda stream:
                pd.DataFrame([
                    {'file_path': html_task.output().path}
                ]).to_csv(stream))

        self.task.run()

        with self.task.output().open() as output:
            actual_quotas = pd.read_csv(output)
        self.assertEqual(1, len(actual_quotas))
        quota = actual_quotas.iloc[0]
        self.assertEqual(26, quota['quota_id'])
        self.assertEqual("Barberini - Kontingent 1 - Werktags", quota['name'])
        self.assertEqual('2020-08-17 19:08:00', quota['creation_date'])


class TestFetchQuotaIds(DatabaseTestCase):
    """Tests the gomus FetchQuotaIds task."""

    def test_fetch_mock(self):
        """Give the task some mock data and test how it parses them."""
        self.task = FetchQuotaIds()
        self._mock_html_targets(self.task.run(), {
            '/admin/quotas?per_page=100': 'quotas/page=1.html',
            '/admin/quotas?page=2.html': 'quotas/page=2.html'
        })

        expected_quota_ids = pd.read_csv(
            'tests/test_data/gomus/quotas/quota_ids.csv')
        with self.task.output().open() as output:
            actual_quota_ids = pd.read_csv(output)
        pd.testing.assert_frame_equal(expected_quota_ids, actual_quota_ids)

    def test_fetch_production(self):
        """Give the task some production data and test how it parses them."""
        self.task = FetchQuotaIds()
        # make sure to test pagination
        self.task.url = '/admin/quotas?per_page=5'

        self.run_task(self.task)

        with self.task.output().open() as output:
            actual_quota_ids = pd.read_csv(output)
        self.assertGreater(len(actual_quota_ids), 0)
        self.assertTrue((actual_quota_ids['quota_id'] > 0).all())
        self.assertTrue((actual_quota_ids['quota_id'] < 1000).all())

    def _mock_html_targets(self, gen, mock_files):
        try:
            dep = next(gen)
            while True:
                if isinstance(dep, FetchGomusHTML):
                    dep.output = MagicMock()
                    mock_file = mock_files[dep.url]
                    with open('tests/test_data/gomus/quotas/' + mock_file)\
                            as input_file:
                        mock_content = input_file.read()
                    self.install_mock_target(
                        dep.output,
                        lambda stream: stream.write(mock_content))
                dep = gen.send(dep.output())
        except StopIteration:
            pass
