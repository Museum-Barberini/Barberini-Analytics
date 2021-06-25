import datetime as dt
import itertools as it
from unittest.mock import patch

import luigi
from luigi.format import UTF8
from luigi.mock import MockTarget
import pandas as pd
import regex

from db_test import DatabaseTestCase
from _utils import QueryDb
from gomus.capacities import ExtractCapacities, FetchCapacities
from gomus._utils.fetch_htmls import FetchGomusHTML


class TestExtractCapacities(DatabaseTestCase):
    """Tests the gomus ExtractCapacities task."""

    @patch.object(ExtractCapacities, 'input')
    def test_extract_mock(self, input_mock):
        """Give the task some mock data and test how it parses them."""
        # See comments in test HTML files.

        self.task = ExtractCapacities(today=dt.date(2020, 10, 29))
        input_mock.return_value = luigi.LocalTarget(
            'tests/test_data/gomus/capacities/capacities_in.csv',
            format=UTF8)

        self.task.run()

        expected_capacities = pd.read_csv(
            'tests/test_data/gomus/capacities/capacities_out.csv')
        with self.task.output().open() as output:
            actual_capacities = pd.read_csv(output)
        pd.testing.assert_frame_equal(expected_capacities, actual_capacities)

    @patch.object(ExtractCapacities, 'input')
    def test_extract_gomus_error(self, input_mock):
        """Give the task some erroneous data that should cause an error."""
        # See comments in test HTML files.

        self.task = ExtractCapacities(today=dt.date(2021, 4, 14))
        input_mock.return_value = luigi.LocalTarget(
            'tests/test_data/gomus/capacities/capacities_error.csv',
            format=UTF8)

        with self.assertRaises(ValueError) as context:
            self.task.run()
        self.assertTrue("extract" in str(context.exception))

    @patch.object(ExtractCapacities, 'input')
    def test_extract_production(self, input_mock):
        """Give the task some production data and test how it parses them."""
        self.task = ExtractCapacities(today=dt.date(2021, 4, 5))
        html_task = FetchGomusHTML(
            url='/admin/quotas/26/capacities?start_at=2021-04-05')
        self.run_task(html_task)
        self.install_mock_target(
            input_mock, lambda stream:
                pd.DataFrame([
                    {'file_path': html_task.output().path}
                ]).to_csv(stream))

        self.task.run()
        with self.task.output().open() as output:
            actual_capacities = pd.read_csv(output)

        # Make some plausibility checks here.

        # Data format
        self.assertEqual(7, len(actual_capacities.groupby('date')))
        self.assertEqual(4 * 24, len(actual_capacities.groupby('time')))

        # Value range
        for column in ['max', 'sold', 'reserved', 'available']:
            self.assertTrue(all(0 <= value <= 1000 for value in actual_capacities[column]))
            sums = actual_capacities.groupby('date')[column].sum()
            self.assertTrue(all(0 <= value <= 10000 for value in sums))

        # Consistency
        pd.testing.assert_series_equal(actual_capacities['available'], actual_capacities['max'] - actual_capacities['sold'] - actual_capacities['reserved'], check_names=False)

        # Anything
        self.assertGreater((actual_capacities['max'] > 0).sum(), 1)

class TestFetchCapacities(DatabaseTestCase):
    """Tests the gomus FetchCapacities task."""

    url_pattern = regex.compile(
        r'''
            \/admin\/quotas\/(?<quota_id>\d+)\/capacities
            \?start_at=(?<start_at>.*)
        ''',
        flags=regex.X
    )

    def setUp(self):

        super().setUp()

        self.db_connector.execute('''
            INSERT INTO gomus_quota VALUES
                (1, 'spam', '2020-10-01 10:01', '2020-10-14 14:41'),
                (2, 'ham', '2020-10-01 10:01', '2020-10-14 14:41'),
                (3, 'eggs', '2020-10-01 10:01', '2020-10-14 14:41'),
                (4, 'eg9z', '2020-10-01 10:01', '2020-10-14 14:41')
        ''')

    def test_date_interval(self):

        today = dt.datetime(2021, 1, 8, 12, 34, 56)
        weeks_back, weeks_ahead = 1, 3
        self.task = FetchCapacities(
            today=today, weeks_back=weeks_back, weeks_ahead=weeks_ahead
        )

        requested_datas = self.iter_task()

        self.assertCountEqual(
            it.product(
                range(1, 4 + 1),  # id
                map(dt.date.fromisoformat, [
                    '2020-12-28', '2021-01-04', '2021-01-11', '2021-01-18',
                    '2021-01-25'
                ])),
            requested_datas
        )

    @patch('gomus.capacities.SLOT_LENGTH_MINUTES', 60 * 4)
    def test_cache(self):

        self.task = FetchCapacities(
            today=dt.datetime(2021, 1, 4, 1, 0, 0),
            weeks_back=0,
            weeks_ahead=0
        )
        self.db_connector.execute(
            # 1: complete but outdated
            '''
                INSERT INTO gomus_capacity
                SELECT 1, date, time, 6, 3, 2, 1, '2020-10-12 21:12'
                FROM
                    generate_series(
                        '2021-01-04', '2021-01-10', INTERVAL '1 day') date,
                    (
                        SELECT make_interval(hours=>hours) AS time
                        FROM generate_series(0, 24 - 4, 4) hours
                    ) time
            ''',
            # 2: complete and up to date
            '''
                INSERT INTO gomus_capacity
                SELECT 2, date, time, 6, 3, 2, 1, '2020-10-14 14:45'
                FROM
                    generate_series(
                        '2021-01-04', '2021-01-10', INTERVAL '1 day') date,
                    (
                        SELECT make_interval(hours=>hours) AS time
                        FROM generate_series(0, 24 - 4, 4) hours
                    ) time
            ''',
            # 3: incomplete
            '''INSERT INTO gomus_capacity VALUES
                (3, '2021-01-04', '08:00', 6, 3, 2, 1, '2020-10-14 14:45'),
                (3, '2021-01-04', '12:00', 4, 2, 2, 0, '2020-10-14 14:45')''',
            # 4: empty 😉
            '''
                INSERT INTO gomus_capacity
                SELECT *
                FROM gomus_capacity
                WHERE false
            '''
        )

        requested_datas = self.iter_task()

        self.assertCountEqual(
            it.product(
                [1, 3, 4],  # id
                [dt.date(2021, 1, 4)]),
            requested_datas
        )

    def iter_task(self):

        gen = self.task.run()
        dep = next(gen)
        try:
            while True:
                if isinstance(dep, QueryDb):
                    dep.run()
                    dep = gen.send(dep.output())
                    continue
                url_match = self.url_pattern.match(dep.url)
                dep = gen.send(MockTarget(
                    f"capacities_{url_match['quota_id']}_"
                    f"{url_match['start_at']}.html"))
        except StopIteration:
            pass

        with self.task.output().open() as output:
            paths = pd.read_csv(output)

        for path in paths['file_path']:
            *_, id_, date = path.split('_')
            yield (
                int(id_),
                dt.date.fromisoformat(date[:-len('.html')])
            )
