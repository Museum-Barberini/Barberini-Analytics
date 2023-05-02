from unittest.mock import patch

from luigi.format import UTF8
from luigi.mock import MockTarget

from twitter import FetchTwitter, ExtractTweets, ExtractTweetPerformance
from db_test import DatabaseTestCase


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
