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

        raw_tweets = '''timestamp,user_id,tweet_id,text,parent_tweet_id,likes,retweets,replies,some_other_value
2020-01-01 23:59:59,42,1337,Welcome to the exhibition!,,5,4,1,foo
2020-01-02 00:00:00,43,9999,I am so exited!,1337,1,0,0,fooooo
'''
        extracted_tweets = '''user_id,tweet_id,text,response_to,post_date,is_from_barberini
42,1337,Welcome to the exhibition!,,2020-01-01 23:59:59,True
43,9999,I am so exited!,1337,2020-01-02 00:00:00,False
'''
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


class TestExtractTweetPerfromance(DatabaseTaskTest):

    def setUp(self):
        super().setUp()
        self.task = ExtractTweetPerformance()

    @patch.object(FetchTwitter, 'output')
    def test_extract_tweets(self, raw_tweets_mock):

        raw_tweets = '''timestamp,user_id,tweet_id,text,parent_tweet_id,likes,retweets,replies,some_other_value
2020-01-01 23:59:59,42,1337,Welcome to the exhibition!,,5,4,1,foo
2020-01-02 00:00:00,43,9999,I am so exited!,1337,1,0,0,fooooo
'''
        extracted_performance = '''tweet_id,likes,retweets,replies,timestamp
1337,5,4,1,SOME_TIMESTAMP
9999,1,0,0,SOME_TIMESTAMP
'''
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
