import datetime as dt
import pandas as pd
from unittest.mock import patch

from freezegun import freeze_time
from luigi.format import UTF8
from luigi.mock import MockTarget

from twitter import FetchTwitter, ExtractTweets, ExtractTweetPerformance
from db_test import DatabaseTestCase


class TextFetchTwitter(DatabaseTestCase):

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

        with freeze_time('2020-02-06'):
            # on this day our team's account had sent a related tweet
            FetchTwitter(timespan=dt.timedelta(days=1)).run()

        with output_target.open('r') as output_file:
            output_df = pd.read_csv(output_file)
        for attr in desired_attributes:
            self.assertTrue(
                attr in output_df.columns,
                msg=f'{attr} is not part of the fetched attributes')
        self.assertTrue(len(output_df.index) >= 1)
        # guaranteed to be true as long as we don't delete our tweet

        # check if timezone is correct on known example
        got_the_right_value = False
        for timestamp in output_df['timestamp']:
            if "2020-02-06 16:05" in timestamp:  # the post time shown online
                got_the_right_value = True
        self.assertTrue(
            got_the_right_value,
            msg="timestamp of our tweet not found, wrong timezone?")


class TestExtractTweets(DatabaseTestCase):

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

        user_id = 10000000

        self.install_mock_target(
            raw_tweets_mock,
            lambda file: file.write(raw_tweets))
        user_id_mock.return_value = user_id

        task = ExtractTweets()
        task.run()

        with output_target.open('r') as output_file:
            output = output_file.read()
        self.assertEqual(output, extracted_tweets)

    @patch.object(FetchTwitter, 'output')
    @patch.object(ExtractTweets, 'output')
    def test_extract_empty_tweets(self, output_mock, raw_tweets_mock):
        output_target = MockTarget('extracted_out', format=UTF8)
        output_mock.return_value = output_target

        with open(
                'tests/test_data/twitter/empty_raw_tweets.csv',
                'r',
                encoding='utf-8') as data_in:
            raw_tweets = data_in.read()

        with open(
                'tests/test_data/twitter/empty_extracted_tweets.csv',
                'r',
                encoding='utf-8') as data_out:
            extracted_tweets = data_out.read()

        self.install_mock_target(
            raw_tweets_mock,
            lambda file: file.write(raw_tweets))

        task = ExtractTweets()
        task.run()

        with output_target.open('r') as output_file:
            output = output_file.read()
        self.assertEqual(output, extracted_tweets)


class TestExtractTweetPerformance(DatabaseTestCase):

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
            self.assertEqual(  # cutting away the timestamp
                output.split('\n')[i].split(';')[:-1],
                extracted_performance.split('\n')[i].split(';')[:-1])

    @patch.object(FetchTwitter, 'output')
    @patch.object(ExtractTweetPerformance, 'output')
    def test_empty_tweet_performance(self, output_mock, raw_tweets_mock):
        output_target = MockTarget('perform_extracted_out', format=UTF8)
        output_mock.return_value = output_target

        with open(
                'tests/test_data/twitter/empty_raw_tweets.csv',
                'r',
                encoding='utf-8') as data_in:
            raw_tweets = data_in.read()

        with open(
                'tests/test_data/twitter/empty_tweet_performance.csv',
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
        self.assertEqual(output, extracted_performance)
