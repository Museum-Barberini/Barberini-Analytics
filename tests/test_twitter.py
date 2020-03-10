from unittest.mock import MagicMock, patch
from unittest import mock

import pandas as pd

from twitter import ExtractTweets, ExtractPerformanceTweets
from task_test import DatabaseTaskTest


class TestExtractTweets(DatabaseTaskTest):

    def __init__(self, methodName):
        super().__init__(methodName)
        self.task = self.isolate(ExtractTweets())

    @patch.object(ExtractTweets, 'museum_user_id')
    def test_extract_tweets(self, user_id_mock):

        raw_tweets = pd.DataFrame(data={
            'timestamp': ['2020-01-01 23:59:59,5', '2020-01-02 00:00:00,5'],
            'user_id': ['42', '43'],
            'tweet_id': ['1337', '9999'],
            'text': ['Welcome to the exhibition!', 'I am so exited!'],
            'parent_tweet_id': ['', '1337'],
            'value_we_dont_care_about': ['foo', 'foooo']
        })
        extracted_tweets = pd.DataFrame(data={
            'user_id': ['42', '43'],
            'tweet_id': ['1337', '9999'],
            'text': ['Welcome to the exhibition!', 'I am so exited!'],
            'response_to': ['', '1337'],
            'post_date': ['2020-01-01 23:59:59,5', '2020-01-02 00:00:00,5'],
            'is_from_barberini': [True, False]
        })
        user_id = '42'

        raw_tweets_mock = MagicMock()
        raw_tweets_mock.return_value = raw_tweets
        user_id_mock.return_value = user_id

        with mock.patch('twitter.pd.read_csv', new=raw_tweets_mock):
            self.task.run()

        with self.task.output().open('r') as output_file:
            output = pd.read_csv(output_file)
        pd.util.testing.assert_frame_equal(output, extracted_tweets)
