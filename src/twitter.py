import datetime as dt
import json

import luigi
import pandas as pd
import twitterscraper as ts
from luigi.format import UTF8

from csv_to_db import CsvToDb
from data_preparation_task import DataPreparationTask
from museum_facts import MuseumFacts


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
        return ExtractTweetPerformance(foreign_keys=self.foreign_keys)


class TweetAuthorsToDB(CsvToDb):

    table = "tweet_author"

    columns = [
        ("user_id", "TEXT"),
        ("user_name", "TEXT")
    ]

    primary_key = "user_id"

    def requires(self):
        return LoadTweetAuthors()


class ExtractTweets(DataPreparationTask):

    def requires(self):
        yield MuseumFacts()
        yield FetchTwitter()

    def run(self):

        with self.input()[1].open('r') as input_file:
            df = pd.read_csv(
                input_file, keep_default_na=False)
            # parent_tweet_id can be empty,
            # which pandas would turn into NaN by default
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

        df, _ = self.ensure_foreign_keys(df)

        with self.output().open('w') as output_file:
            df.to_csv(output_file, index=False, header=True)

    def output(self):
        return luigi.LocalTarget(
            "output/twitter/tweet_performance.csv", format=UTF8)


class FetchTwitter(DataPreparationTask):

    query = luigi.Parameter(default="museumbarberini")
    timespan = luigi.parameter.TimeDeltaParameter(
        default=dt.timedelta(days=60),
        description="For how many days tweets should be fetched")

    def output(self):
        return luigi.LocalTarget(
            (f'output/twitter/raw_tweets.csv'),
            format=UTF8)

    def run(self):
        timespan = self.timespan
        if self.minimal_mode:
            timespan = dt.timedelta(days=0)

        tweets = ts.query_tweets(
            self.query,
            begindate=dt.date.today() - timespan,
            enddate=dt.date.today() + dt.timedelta(days=1))
        if tweets:
            df = pd.DataFrame([tweet.__dict__ for tweet in tweets])
        else:  # no tweets returned, ensure schema
            df = pd.DataFrame(columns=[
                'user_id',
                'tweet_id',
                'text',
                'parent_tweet_id',
                'timestamp',
                'likes',
                'retweets',
                'replies'])
        df = df.drop_duplicates(subset=["tweet_id"])

        with self.output().open('w') as output_file:
            df.to_csv(output_file, index=False, header=True)


class LoadTweetAuthors(DataPreparationTask):

    def output(self):
        return luigi.LocalTarget("data/tweet_authors.csv", format=UTF8)
