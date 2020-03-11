from unittest.mock import patch

from twitter import FetchTwitter, ExtractTweets, ExtractPerformanceTweets
from task_test import DatabaseTaskTest


class TestExtractTweets(DatabaseTaskTest):

    def setUp(self):
        super().setUp()
        self.task = ExtractTweets()

    @patch.object(FetchTwitter, 'output')
    @patch.object(ExtractTweets, 'museum_user_id')
    def test_extract_tweets(self, user_id_mock, raw_tweets_mock):

        raw_tweets = '''timestamp,user_id,tweet_id,text,parent_tweet_id,some_other_value
2020-01-01 23:59:59,42,1337,Welcome to the exhibition!,,foo
2020-01-02 00:00:00,43,9999,I am so exited!,1337,fooooo
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

        self.task.run()

        with self.task.output().open('r') as output_file:
            output = output_file.read()
        self.assertEquals(output, extracted_tweets)
