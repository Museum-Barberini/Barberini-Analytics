from unittest.mock import patch
import datetime as dt
import pandas as pd

from twitter import FetchTwitter, ExtractTweets, ExtractTweetPerformance
from task_test import DatabaseTaskTest


class TextFetchTwitter(DatabaseTaskTest):

    def test_fetch_twitter(self):
        desired_attributes = [
            'user_id',
            'tweet_id',
            'text',
            'parent_tweet_id',
            'timestamp']
        timespan_start = dt.date(2020, 2, 6)
        timespan_end = dt.date(2020, 2, 7)
        # in this timespan out team account had
        # sent one tweet with #MuseumBarberini
        task = FetchTwitter(
            min_timestamp=timespan_start,
            max_timestamp=timespan_end)

        task.run()

        with task.output().open('r') as output_file:
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
    def test_extract_tweets(self, user_id_mock, raw_tweets_mock):

        with open('tests/test_data/twitter/raw_tweets.csv', 'r') as data_in:
            raw_tweets = data_in.read()

        with open(
            'tests/test_data/twitter/expected_extracted_tweets.csv',
                'r') as data_out:
            extracted_tweets = data_out.read()

        user_id = '42'

        self.install_mock_target(
            raw_tweets_mock,
            lambda file: file.write(raw_tweets))
        user_id_mock.return_value = user_id

        task = ExtractTweets()
        task.run()

        with task.output().open('r') as output_file:
            output = output_file.read()
        self.assertEquals(output, extracted_tweets)


class TestExtractTweetPerformance(DatabaseTaskTest):

    def setUp(self):
        super().setUp()
        self.task = ExtractTweetPerformance()

    @patch.object(FetchTwitter, 'output')
    def test_extract_tweets(self, raw_tweets_mock):

        with open('tests/test_data/twitter/raw_tweets.csv', 'r') as data_in:
            raw_tweets = data_in.read()

        with open(
            'tests/test_data/twitter/expected_tweet_performance.csv',
                'r') as data_out:
            extracted_performance = data_out.read()

        self.install_mock_target(
            raw_tweets_mock,
            lambda file: file.write(raw_tweets))

        self.task.run()

        with self.task.output().open('r') as output_file:
            output = output_file.read()
        self.assertEquals(
            output.split('\n')[0],
            extracted_performance.split('\n')[0])
        for i in range(1, 3):
            self.assertEquals(
                output.split('\n')[i][0:11],
                extracted_performance.split('\n')[i][0:11])
            # cutting away the timestamp
