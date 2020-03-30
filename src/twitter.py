import datetime as dt
import json

import luigi
import pandas as pd
import twitterscraper as ts
from luigi.format import UTF8

from csv_to_db import CsvToDb
from data_preparation_task import DataPreparationTask
from museum_facts import MuseumFacts


class FetchTwitter(luigi.Task):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # Default for min_timestamp, requires the other values to be set
        # already, so this can't be done via 'default='
        if not self.min_timestamp:
            self.min_timestamp = (self.max_timestamp
                                  - dt.timedelta(days=self.timespan))

    query = luigi.Parameter(default="museumbarberini")
    timespan = luigi.Parameter(
        default=60,
        description="For how many days tweets should be "
                    "fetched if min_timestamp is not explicitly set")

    min_timestamp = luigi.DateParameter(default=None)
    max_timestamp = luigi.DateParameter(
        default=dt.date.today() + dt.timedelta(days=1))

    def output(self):
        return luigi.LocalTarget(
            (f'output/twitter/raw_tweets_{self.min_timestamp}_to_'
             f'{self.max_timestamp}.csv'),
            format=UTF8)

    def run(self):
        tweets = ts.query_tweets(
            self.query,
            begindate=self.min_timestamp,
            enddate=self.max_timestamp)
        df = pd.DataFrame([tweet.__dict__ for tweet in tweets])
        df = df.drop_duplicates(subset=["tweet_id"])
        with self.output().open('w') as output_file:
            df.to_csv(output_file, index=False, header=True)


class ExtractTweets(DataPreparationTask):

    def requires(self):
        yield MuseumFacts()
        yield FetchTwitter(timespan=7)

    def run(self):
        with self.input()[1].open('r') as input_file:
            df = pd.read_csv(
                input_file,
                dtype={
                    'user_id': str,
                    'tweet_id': str,
                    'parent_tweet_id': str
                    })
        # pandas would by default store them as int64 or float64
        df = df.filter([
            'user_id',
            'tweet_id',
            'text',
            'parent_tweet_id',
            'timestamp'])
        df.columns = [
            'user_id',
            'tweet_id',
            'text',
            'response_to',
            'post_date']
        df['is_from_barberini'] = df['user_id'] == self.museum_user_id()
        df = df.drop_duplicates()
        with self.output().open('w') as output_file:
            df.to_csv(output_file, index=False, header=True)

    def output(self):
        return luigi.LocalTarget("output/twitter/tweets.csv", format=UTF8)

    def museum_user_id(self):
        with self.input()[0].open('r') as facts_file:
            facts = json.load(facts_file)
        return facts['ids']['twitter']['userId']


class ExtractTweetPerformance(DataPreparationTask):

    def _requires(self):
        return luigi.task.flatten([
            TweetsToDB(),
            super()._requires()
        ])

    def requires(self):
        return FetchTwitter()

    def run(self):
        with self.input().open('r') as input_file:
            df = pd.read_csv(input_file)
        df = df.filter(['tweet_id', 'likes', 'retweets', 'replies'])
        current_timestamp = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        df['timestamp'] = current_timestamp

        df = self.ensure_foreign_keys(df)

        with self.output().open('w') as output_file:
            df.to_csv(output_file, index=False, header=True)

    def output(self):
        return luigi.LocalTarget(
            "output/twitter/tweet_performance.csv", format=UTF8)


class TweetsToDB(CsvToDb):

    table = "tweet"

    columns = [
        ("user_id", "TEXT"),
        ("tweet_id", "TEXT"),
        ("text", "TEXT"),
        ("response_to", "TEXT"),
        ("post_date", "DATE"),
        ("is_from_barberini", "BOOL")
    ]

    primary_key = 'tweet_id'

    def requires(self):
        return ExtractTweets()


class TweetPerformanceToDB(CsvToDb):

    table = "tweet_performance"

    columns = [
        ("tweet_id", "TEXT"),
        ("likes", "INT"),
        ("retweets", "INT"),
        ("replies", "INT"),
        ("timestamp", "TIMESTAMP")
    ]

    primary_key = ('tweet_id', 'timestamp')

    foreign_keys = [
        {
            "origin_column": "tweet_id",
            "target_table": "tweet",
            "target_column": "tweet_id"
        }
    ]

    def requires(self):
        return ExtractTweetPerformance(
            foreign_keys=self.foreign_keys)
