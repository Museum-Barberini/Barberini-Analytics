import datetime as dt
import json
import re
from shutil import copyfile
from unittest.mock import MagicMock, patch

from freezegun import freeze_time
import instaloader
from luigi.format import UTF8
from luigi.mock import MockTarget
import pandas as pd

from db_test import DatabaseTestCase
from .utils.test_database import DummyWriteCsvToDb
import instagram

IG_TEST_DATA = 'tests/test_data/instagram'


class TestInstagram(DatabaseTestCase):
    """Tests the instagram module."""

    @patch('instagram.try_request_multiple_times')
    @patch.object(instagram.FetchIgPosts, 'output')
    @patch.object(instagram.MuseumFacts, 'output')
    def test_post_transformation(self, fact_mock, output_mock, request_mock):
        fact_target = MockTarget('facts_in', format=UTF8)
        fact_mock.return_value = fact_target
        output_target = MockTarget('post_out', format=UTF8)
        output_mock.return_value = output_target

        with open(f'{IG_TEST_DATA}/post_actual.json',
                  'r',
                  encoding='utf-8') as data_in:
            input_data = data_in.read()
        expected_data = pd.read_csv(f'{IG_TEST_DATA}/post_expected.csv')

        request_mock.side_effect = lambda url: \
            MagicMock(ok=True, json=lambda: json.loads(input_data))

        self.run_task(instagram.FetchIgPosts())

        with output_target.open('r') as output_data:
            actual_data = pd.read_csv(output_data)
        pd.testing.assert_frame_equal(actual_data, expected_data)

    @patch('instagram.try_request_multiple_times')
    @patch.object(instagram.FetchIgPosts, 'output')
    @patch.object(instagram.MuseumFacts, 'output')
    def test_pagination(self, fact_mock, output_mock, request_mock):
        # This is very similar to test_facebook.test_pagination

        fact_target = MockTarget('facts_in', format=UTF8)
        fact_mock.return_value = fact_target
        output_target = MockTarget('post_out', format=UTF8)
        output_mock.return_value = output_target

        with open(f'{IG_TEST_DATA}/post_next.json', 'r') \
                as next_data_in:
            next_data = next_data_in.read()
        with open(f'{IG_TEST_DATA}/post_previous.json', 'r') \
                as previous_data_in:
            previous_data = previous_data_in.read()

        def next_json():
            return json.loads(next_data)

        def previous_json():
            return json.loads(previous_data)

        next_response = MagicMock(ok=True, json=next_json)
        previous_response = MagicMock(ok=True, json=previous_json)

        request_mock.side_effect = [
            next_response,
            next_response,
            previous_response
        ]

        self.run_task(instagram.FetchIgPosts())

        self.assertEqual(request_mock.call_count, 3)

    @patch.object(instagram.IgPostsToDb, 'complete')
    @patch.object(instagram.FetchIgPostThumbnails, 'get_thumbnail_uri')
    @patch.object(instagram.FetchIgPostThumbnails, 'output')
    def test_thumbnails_to_db(self, output_mock, uri_mock, to_db_mock):
        thumbnails = pd.read_csv(f'{IG_TEST_DATA}/post_thumbnails.csv')
        post_data = pd.read_csv(f'{IG_TEST_DATA}/post_expected.csv')

        # Prepare database with posts (some with, others without a thumbnail)
        merged = thumbnails.merge(post_data, on='permalink')
        post_data['thumbnail_uri'] = merged['thumbnail_uri']
        post_data.loc[
            post_data.index == len(post_data) - 1, 'thumbnail_uri'
        ] = None
        input_task = DummyWriteCsvToDb(
            table=instagram.IgPostsToDb.table,
            csv=post_data.to_csv(index=False))
        self.run_task(input_task)

        # Mock get_thumbnail_uri() to answer mocked URIs directly
        output_target = MockTarget('post_out')
        output_mock.return_value = output_target
        uri_mock.side_effect = lambda permalink: \
            thumbnails[thumbnails['permalink'] == permalink][
                'thumbnail_uri'].values[0]
        to_db_mock.return_value = True

        # Let's go!
        self.run_task(instagram.IgPostThumbnailsToDb())

        actual_data = pd.DataFrame(self.db_connector.query(f'''
            SELECT permalink, thumbnail_uri
            FROM {instagram.IgPostsToDb.table}  -- # nosec - constant
        '''), columns=['permalink', 'thumbnail_uri'])
        pd.testing.assert_frame_equal(
            actual_data,
            thumbnails[['permalink', 'thumbnail_uri']])
        self.assertEquals(
            uri_mock.call_count,
            post_data['thumbnail_uri'].isna().sum())

    @patch.object(instagram.FetchIgPostThumbnails, 'create_instaloader')
    @patch.object(instagram.FetchIgPostThumbnails, 'get_thumbnail_url')
    def test_get_thumbnail_uri(self, url_mock, instaloader_mock):
        thumbnails = pd.read_csv(
            f'{IG_TEST_DATA}/post_thumbnails.csv', keep_default_na=False)

        url_mock.side_effect = lambda permalink: \
            thumbnails[thumbnails['permalink'] == permalink][
                'thumbnail_url'].values[0]

        # Mock download calls and copy local files instead
        def mocked_download(download_path, url, mtime):
            url_match = re.match(
                r'^test://(?P<path>.*\.(?P<ext>[^.]+))(?:&ext=\w+)$', url)
            self.assertTrue(url_match)
            local_path = f"{IG_TEST_DATA}/thumbnails/{url_match['path']}"
            copyfile(local_path, f'{download_path}.jpg')
            return True
        instaloader_mock.return_value = MagicMock(download_pic=mocked_download)

        # Let's go!
        self.task = instagram.FetchIgPostThumbnails()
        actual_thumbnails = thumbnails['permalink'].apply(
            self.task.get_thumbnail_uri)

        pd.testing.assert_series_equal(
            actual_thumbnails, thumbnails['thumbnail_uri'],
            check_names=False)

    @patch('instagram.try_request_multiple_times')
    @patch.object(instagram.FetchIgPostPerformance, 'output')
    @patch.object(instagram.FetchIgPostPerformance, 'input')
    def test_post_performance_transformation(
            self, input_mock, output_mock, request_mock):
        self.db_connector.execute(
            '''INSERT INTO ig_post (ig_post_id) VALUES
                (0123456789),
                (9876543210)''',
            '''INSERT INTO ig_post_performance VALUES
                (0123456789, '2019-10-04', 5, 4, 3, 2, 1, 2, 1, 0, 1, 0)'''
        )
        input_target = MockTarget('posts_in', format=UTF8)
        input_mock.return_value = input_target
        output_target = MockTarget('insights_out', format=UTF8)
        output_mock.return_value = output_target

        with input_target.open('w') as posts_target:
            with open(f'{IG_TEST_DATA}/post_expected.csv',
                      'r',
                      encoding='utf-8') as posts_input:
                posts_target.write(posts_input.read())

        with open(f'{IG_TEST_DATA}/post_insights_video_actual.json',
                  'r',
                  encoding='utf-8') as json_video_in:
            input_video_insights = json_video_in.read()

        with open(f'{IG_TEST_DATA}/post_insights_no_video_actual.json',
                  'r',
                  encoding='utf-8') as json_no_video_in:
            input_no_video_insights = json_no_video_in.read()

        with open(f'{IG_TEST_DATA}/post_insights_expected.csv',
                  'r',
                  encoding='utf-8') as expected_data_in:
            expected_df = pd.read_csv(expected_data_in)

        def mock_video_json():
            return json.loads(input_video_insights)

        def mock_no_video_json():
            return json.loads(input_no_video_insights)

        mock_video_response = MagicMock(ok=True, json=mock_video_json)
        mock_no_video_response = MagicMock(ok=True, json=mock_no_video_json)
        request_mock.side_effect = [
            mock_video_response,
            mock_no_video_response,
            mock_no_video_response]

        with freeze_time('2020-01-01 00:00:05'):
            self.task = instagram.FetchIgPostPerformance(
                columns=[
                    column[0]
                    for column
                    in instagram.IgPostPerformanceToDb().columns],
                timespan=dt.timedelta(days=100000),
                table='ig_post_performance')
            self.task.run()

        with output_target.open('r') as output_data:
            output_df = pd.read_csv(output_data)
        pd.testing.assert_frame_equal(expected_df, output_df)

    @patch('instagram.try_request_multiple_times')
    @patch.object(instagram.FetchIgProfileMetricsDevelopment, 'output')
    @patch.object(instagram.MuseumFacts, 'output')
    def test_fetch_profile_metrics_development(
            self, fact_mock, output_mock, request_mock):
        fact_target = MockTarget('facts_in', format=UTF8)
        fact_mock.return_value = fact_target
        output_target = MockTarget('post_out', format=UTF8)
        output_mock.return_value = output_target

        with open(f'{IG_TEST_DATA}/profile_metrics_development_actual.json',
                  'r',
                  encoding='utf-8') as data_in:
            input_data = data_in.read()

        with open(f'{IG_TEST_DATA}/profile_metrics_development_expected.csv',
                  'r',
                  encoding='utf-8') as data_out:
            expected_data = data_out.read()

        def mock_json():
            return json.loads(input_data)
        mock_response = MagicMock(ok=True, json=mock_json)
        request_mock.return_value = mock_response

        self.run_task(instagram.FetchIgProfileMetricsDevelopment(
            columns=[col[0] for col in
                     instagram.IgProfileMetricsDevelopmentToDb().columns]))

        with output_target.open('r') as output_data:
            self.assertEqual(output_data.read(), expected_data)

    @patch('instagram.try_request_multiple_times')
    @patch.object(instagram.FetchIgTotalProfileMetrics, 'output')
    @patch.object(instagram.MuseumFacts, 'output')
    def test_fetch_total_profile_metrics(
            self, fact_mock, output_mock, request_mock):
        fact_target = MockTarget('facts_in', format=UTF8)
        fact_mock.return_value = fact_target
        output_target = MockTarget('post_out', format=UTF8)
        output_mock.return_value = output_target

        with open(f'{IG_TEST_DATA}/total_profile_metrics_actual.json',
                  'r',
                  encoding='utf-8') as data_in:
            input_data = data_in.read()

        with open(f'{IG_TEST_DATA}/total_profile_metrics_expected.csv',
                  'r',
                  encoding='utf-8') as data_out:
            expected_data = data_out.read()

        def mock_json():
            return json.loads(input_data)
        mock_response = MagicMock(ok=True, json=mock_json)
        request_mock.return_value = mock_response

        with freeze_time('2020-01-01 00:00:05'):
            self.run_task(instagram.FetchIgTotalProfileMetrics(
                columns=[col[0] for col in
                         instagram.IgTotalProfileMetricsToDb().columns]))

        with output_target.open('r') as output_data:
            self.assertEqual(output_data.read(), expected_data)

    @patch('instagram.try_request_multiple_times')
    @patch.object(instagram.FetchIgAudienceOrigin, 'output')
    @patch.object(instagram.MuseumFacts, 'output')
    def test_audience_origin_transformation(
            self, input_mock, output_mock, request_mock):
        fact_target = MockTarget('facts_in', format=UTF8)
        input_mock.return_value = fact_target
        output_target = MockTarget('post_out', format=UTF8)
        output_mock.return_value = output_target

        with open(f'{IG_TEST_DATA}/audience_origin_actual.json',
                  'r',
                  encoding='utf-8') as data_in:
            input_data = data_in.read()

        with open(f'{IG_TEST_DATA}/audience_origin_expected.csv',
                  'r',
                  encoding='utf-8') as data_out:
            expected_data = data_out.read()

        def mock_json():
            return json.loads(input_data)
        mock_response = MagicMock(ok=True, json=mock_json)
        request_mock.return_value = mock_response

        instagram.MuseumFacts().run()

        with freeze_time('2020-01-01 00:00:05'):
            # Use city mode for testing, though the
            # transformation is the same for countries
            # The only difference between the two is the received,
            # data, which cannot be tested here
            instagram.FetchIgAudienceOrigin(
                columns=[col[0] for col in
                         instagram.IgAudienceCityToDb().columns],
                country_mode=False
            ).run()

        with output_target.open('r') as output_data:
            self.assertEqual(output_data.read(), expected_data)

    @patch('instagram.try_request_multiple_times')
    @patch.object(instagram.FetchIgAudienceGenderAge, 'output')
    @patch.object(instagram.MuseumFacts, 'output')
    def test_audience_gender_age_transformation(
            self, input_mock, output_mock, request_mock):
        fact_target = MockTarget('facts_in', format=UTF8)
        input_mock.return_value = fact_target
        output_target = MockTarget('post_out', format=UTF8)
        output_mock.return_value = output_target

        with open(f'{IG_TEST_DATA}/audience_gender_age_actual.json',
                  'r',
                  encoding='utf-8') as data_in:
            input_data = data_in.read()

        with open(f'{IG_TEST_DATA}/audience_gender_age_expected.csv',
                  'r',
                  encoding='utf-8') as data_out:
            expected_data = data_out.read()

        def mock_json():
            return json.loads(input_data)
        mock_response = MagicMock(ok=True, json=mock_json)
        request_mock.return_value = mock_response

        instagram.MuseumFacts().run()
        with freeze_time('2020-01-01 00:00:05'):
            instagram.FetchIgAudienceGenderAge(
                columns=[col[0] for col in
                         instagram.IgAudienceGenderAgeToDb().columns]
            ).run()

        with output_target.open('r') as output_data:
            self.assertEqual(output_data.read(), expected_data)
