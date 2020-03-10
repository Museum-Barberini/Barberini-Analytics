import unittest
from unittest.mock import MagicMock, patch

import pandas as pd

from twitter import ExtractTweets, ExtractPerformanceTweets
from task_test import DatabaseTaskTest


class TestExtractTweets(DatabaseTaskTest):

    def __init__(self, methodName):
        super().__init__(methodName)
        self.task = self.isolate(ExtractTweets())

    @patch.object(ExtractTweets, 'museum_user_id')
    @patch('twitter.pd.read_csv')
    def test_extract_tweets(self, raw_tweets_mock, user_id_mock):

        raw_tweets = pd.DataFrame(data={
            'timestamp': ['2020-01-01 23:59:59,1583798399', '2020-01-02 00:00:00,1583798399'],
            'user_id': ['42', '43'],
            'tweet_id': ['1337', '9999'],
            'text': ['Welcome to the exhibition!', 'I am so exited!'],
            'parent_tweet_id': [None, '1337'],
            'value_we_dont_care_about': ['foo', 'foooo']
        })
        extracted_tweets = pd.DataFrame(data={
            'user_id': ['42', '43'],
            'tweet_id': ['1337', '9999'],
            'text': ['Welcome to the exhibition!', 'I am so exited!'],
            'response_to': [None, '1337'],
            'post_date': ['2020-01-01 23:59:59,1583798399', '2020-01-02 00:00:00,1583798399'],
            'is_from_barberini': [True, False]
        })
        user_id = '42'

        raw_tweets_mock.return_value = raw_tweets
        user_id_mock.return_value = user_id

        self.task.run()

        with self.task.output().open('r') as output_file:
            output = pd.read_csv(output_file)
            print(output)
            print(raw_tweets)
        self.assertEquals(output, extracted_tweets)
