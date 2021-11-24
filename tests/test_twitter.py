import datetime as dt
import os
from random import random
import re
import time
from unittest.mock import patch

from dateutil import parser as date_parser
from luigi.format import UTF8
from luigi.mock import MockTarget
import pandas as pd

from twitter import FetchTwitter, ExtractTweets, ExtractTweetPerformance
from db_test import DatabaseTestCase, logger


class TestFetchTwitter(DatabaseTestCase):
    """Tests the FetchTwitter task."""

    def generate_random_hex_string(self, length):
        return''.join(   # nosec - this is just a random string
            hex(int(16 * (random() % 1)))[2:].zfill(2)
            for _ in range(length)
        )

    def post_tweet(self, text):
        from twython import Twython
        # HOW TO GET THESE KEYS:
        # 1. Register app on https://developer.twitter.com/en/portal/projects
        # 2. Under settings, give your app permissions to read and write posts
        # 3. Under "Consumer Keys", generate the "API Key" and "API Secret"
        # 4. Under "Authentication Tokens", generate the "Access Token" and
        #    "Access Token Secret"
        app_key = os.getenv('TWYTHON_APP_KEY')
        app_secret = os.getenv('TWYTHON_APP_SECRET')
        access_token = os.getenv('TWYTHON_ACCESS_TOKEN')
        access_secret = os.getenv('TWYTHON_ACCESS_SECRET')
        account = Twython(app_key, app_secret, access_token, access_secret)
        info = account.verify_credentials()
        logger.info(f"Logged in as '{info['name']}'")

        tweet = account.update_status(status=text)
        tweet_id = tweet['id']
        tweet_date = date_parser.parse(tweet['created_at'])

        logger.info(f"Tweeted: '{tweet_id}'")
        return {
            'id': tweet_id,
            'user_id': tweet['user']['id'],
            'text': tweet['text'],
            'created_at': tweet_date
        }

    @patch.object(FetchTwitter, 'output')
    def test_fetch_twitter(self, output_mock):
        """Integration test! We post a real tweet and then try to fetch it."""
        output_target = MockTarget('raw_out', format=UTF8)
        output_mock.return_value = output_target

        sample = "TestBarberiniAnalyticsFetchTwitter" \
            + self.generate_random_hex_string(12)
        text = (
            f"This is an automated random tweet for integration testing of "
            "BarberiniAnalytics.\n\n"
            f"{sample}\n\n"
            "For more information, see: https://github.com/Museum-Barberini/"
            "Barberini-Analytics/blob/master/tests/test_twitter.py"
        )

        # ARRANGE
        tweet = self.post_tweet(text)
        time.sleep(3)  # Wait for the tweet to be processed

        # ACT
        FetchTwitter(
            query=sample,
            timespan=dt.timedelta(days=1)
        ).run()

        with output_target.open('r') as output_file:
            output_df = pd.read_csv(output_file)
        # Dirty workaround for pandas's peculiarities regarding default values
        none = object()
        output_df = output_df.fillna(none)
        output_df['text'] = output_df['text'].apply(
            lambda text: re.sub(r'\s+', ' ', text)
        )
        output_df['timestamp'] = output_df['timestamp'].apply(
            date_parser.parse)

        # ASSERT
        expected_tweet = {
            'tweet_id': tweet['id'],
            'user_id': tweet['user_id'],
            'parent_tweet_id': none,
            'timestamp': tweet['created_at']
        }
        expected_text = re.sub(r'\s+', ' ', tweet['text'])
        text_predicates = [
            lambda text: text.startswith(expected_text.split('…')[0]),
            lambda text: sample in text
        ]

        filtered_df = output_df
        for key, value in expected_tweet.items():
            previous_df = filtered_df
            filtered_df = filtered_df[filtered_df[key] == value]
            self.assertTrue(
                len(filtered_df) >= 1,
                f"Did not find any tweet with {key} = {value}, "
                f"values are: {previous_df[key]}")
        for index, predicate in enumerate(text_predicates):
            previous_df = filtered_df
            filtered_df = filtered_df[filtered_df['text'].apply(predicate)]
            self.assertTrue(
                len(filtered_df) >= 1,
                f"Did not find any tweet with text matching predicate {index}"
                f"\n\nValues are: {previous_df['text']}")


class TestExtractTweets(DatabaseTestCase):
    """Tests the ExtractTweet task."""

    @patch.object(FetchTwitter, 'output')
    @patch.object(ExtractTweets, 'output')
    def test_extract_tweets(self, output_mock, raw_tweets_mock):
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

        self.install_mock_target(
            raw_tweets_mock,
            lambda file: file.write(raw_tweets))

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
    """Tests the ExtractTweetPerformance task."""

    @patch.object(FetchTwitter, 'output')
    @patch.object(ExtractTweetPerformance, 'output')
    def test_extract_tweet_performance(self, output_mock, raw_tweets_mock):
        self.db_connector.execute(
            '''
            INSERT INTO tweet (tweet_id) VALUES
                ('1234567890123456789'),
                ('111111111111111111'),
                ('2222222222222222222')
            '''
        )
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
            expected_performance = data_out.read()

        self.install_mock_target(
            raw_tweets_mock,
            lambda file: file.write(raw_tweets))

        task = ExtractTweetPerformance(table='tweet_performance')
        task.run()

        with output_target.open('r') as output_file:
            output = output_file.read()
        self.assertEqual(
            output.split('\n')[0],
            expected_performance.split('\n')[0])
        for i in range(1, 3):
            self.assertEqual(  # cutting away the timestamp
                output.split('\n')[i].split(';')[:-1],
                expected_performance.split('\n')[i].split(';')[:-1])

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
            expected_performance = data_out.read()

        self.install_mock_target(
            raw_tweets_mock,
            lambda file: file.write(raw_tweets))

        task = ExtractTweetPerformance(table='tweet_performance')
        task.run()

        with output_target.open('r') as output_file:
            output = output_file.read()
        self.assertEqual(output, expected_performance)
