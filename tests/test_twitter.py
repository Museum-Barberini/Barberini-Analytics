from unittest.mock import patch
import datetime as dt
import pandas as pd

from luigi.format import UTF8
from luigi.mock import MockTarget
from twitter import FetchTwitter, ExtractTweets, ExtractTweetPerformance
from task_test import DatabaseTaskTest


class TextFetchTwitter(DatabaseTaskTest):

    @patch.object(FetchTwitter, 'output')
    def test_fetch_twitter(self, output_mock):
        output_target = MockTarget('raw_out', format=UTF8)
        output_mock.return_value = output_target

        desired_attributes = [
            'user_id',
            'tweet_id',
            'text',
            'parent_tweet_id',
            'timestamp']

        class MockDate(dt.date):
            @classmethod
            def today(cls):
                # on this day or team's account had sent a related tweet
                return cls(2020, 2, 6)

        tmp_date = dt.date

        # Ensure dt.date is reset in any case
        try:
            dt.date = MockDate
            FetchTwitter(timespan=dt.timedelta(days=1)).run()

        finally:
            dt.date = tmp_date

        with output_target.open('r') as output_file:
            output_df = pd.read_csv(output_file)
        for attr in desired_attributes:
            self.assertTrue(
                attr in output_df.columns,
                msg=f'{attr} is not part of the fetched attributes')
        self.assertTrue(len(output_df.index) >= 1)
        # guaranteed to be true as long as we don't delete our tweet


class TestExtractTweets(DatabaseTaskTest):

    @patch.object(FetchTwitter, 'output')
    @patch.object(ExtractTweets, 'museum_user_id')
    @patch.object(ExtractTweets, 'output')
    def test_extract_tweets(self, output_mock, user_id_mock, raw_tweets_mock):
        output_target = MockTarget('extracted_out', format=UTF8)
        output_mock.return_value = output_target

        with open(
                'tests/test_data/twitter/raw_tweets.csv',
                'r',
                encoding='utf-8') as data_in:
            raw_tweets = data_in.read()

        with open(
                'tests/test_data/twitter/expected_extracted_tweets.csv',
                'r',
                encoding='utf-8') as data_out:
            extracted_tweets = data_out.read()

        user_id = '42'

        self.install_mock_target(
            raw_tweets_mock,
            lambda file: file.write(raw_tweets))
        user_id_mock.return_value = user_id

        task = ExtractTweets()
        task.run()

        with output_target.open('r') as output_file:
            output = output_file.read()
        self.assertEqual(output, extracted_tweets)


class TestExtractTweetPerformance(DatabaseTaskTest):

    @patch.object(FetchTwitter, 'output')
    @patch.object(ExtractTweetPerformance, 'output')
    def test_extract_tweet_performance(self, output_mock, raw_tweets_mock):
        output_target = MockTarget('perform_extracted_out', format=UTF8)
        output_mock.return_value = output_target

        with open(
                'tests/test_data/twitter/raw_tweets.csv',
                'r',
                encoding='utf-8') as data_in:
            raw_tweets = data_in.read()

        with open(
                'tests/test_data/twitter/expected_tweet_performance.csv',
                'r',
                encoding='utf-8') as data_out:
            extracted_performance = data_out.read()

        self.install_mock_target(
            raw_tweets_mock,
            lambda file: file.write(raw_tweets))

        task = ExtractTweetPerformance()
        task.run()

        with output_target.open('r') as output_file:
            output = output_file.read()
        self.assertEqual(
            output.split('\n')[0],
            extracted_performance.split('\n')[0])
        for i in range(1, 3):
            self.assertEqual(
                output.split('\n')[i][0:-14],
                extracted_performance.split('\n')[i][0:-14])
            # cutting away the timestamp
