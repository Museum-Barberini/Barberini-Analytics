import datetime as dt
import json

import luigi
import pandas as pd
import twitterscraper as ts
from luigi.format import UTF8

from csv_to_db import CsvToDb
from data_preparation_task import DataPreparationTask
from museum_facts import MuseumFacts
from set_db_connection_options import set_db_connection_options


class TweetsToDB(CsvToDb):
    minimal = luigi.parameter.BoolParameter(default=False)

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
        return ExtractTweets(minimal=self.minimal)


class TweetPerformanceToDB(CsvToDb):
    minimal = luigi.parameter.BoolParameter(default=False)

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
            foreign_keys=self.foreign_keys,
            minimal=self.minimal)


class FetchTwitter(luigi.Task):
    minimal = luigi.parameter.BoolParameter(default=False)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        set_db_connection_options(self)

    query = luigi.Parameter(default="museumbarberini")
    min_timestamp = luigi.DateParameter(default=dt.date(2015, 1, 1))
    max_timestamp = luigi.DateParameter(
        default=dt.date.today() + dt.timedelta(days=1))

    def output(self):
        return luigi.LocalTarget("output/twitter/raw_tweets.csv", format=UTF8)

    def run(self):
        if self.minimal:
            self.min_timestamp = dt.date.today()
            self.max_timestamp = dt.date.today() + dt.timedelta(days=1)

        print("---------", self.min_timestamp, self.max_timestamp)
        tweets = ts.query_tweets(
            self.query,
            begindate=self.min_timestamp,
            enddate=self.max_timestamp)
        df = pd.DataFrame([tweet.__dict__ for tweet in tweets])
        df = df.drop_duplicates(subset=["tweet_id"])
        with self.output().open('w') as output_file:
            df.to_csv(output_file, index=False, header=True)


class ExtractTweets(DataPreparationTask):
    minimal = luigi.parameter.BoolParameter(default=False)

    def requires(self):
        yield MuseumFacts()
        yield FetchTwitter(minimal=self.minimal)

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
    minimal = luigi.parameter.BoolParameter(default=False)

    def _requires(self):
        return luigi.task.flatten([
            TweetsToDB(minimal=self.minimal),
            super()._requires()
        ])

    def requires(self):
        return FetchTwitter(minimal=self.minimal)

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
            "output/twitter/performance_tweets.csv", format=UTF8)
