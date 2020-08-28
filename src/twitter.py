"""Provides tasks for downloading tweets related to the museum."""

import datetime as dt
from pytz import utc

import luigi
import pandas as pd
import twitterscraper as ts
from luigi.format import UTF8
import tzlocal

from _utils import CsvToDb, DataPreparationTask, MuseumFacts


class TweetsToDb(CsvToDb):
    """Store extracted tweets about the museum into the database."""

    table = 'tweet'

    def requires(self):
        return ExtractTweets()


class TweetPerformanceToDb(CsvToDb):
    """Store extracted tweet performance values into the database."""

    table = 'tweet_performance'

    def requires(self):
        return ExtractTweetPerformance(table=self.table)


class TweetAuthorsToDb(CsvToDb):
    """Store hard-coded tweet authors into the database."""

    table = 'tweet_author'

    def requires(self):
        return LoadTweetAuthors()


class ExtractTweets(DataPreparationTask):
    """Extract tweets downloaded from Twitter."""

    def requires(self):
        yield MuseumFacts()
        yield FetchTwitter()

    def run(self):

        with self.input()[1].open('r') as input_file:
            df = pd.read_csv(input_file, keep_default_na=False, dtype={
                'tweet_id': str, 'parent_tweet_id': str
            })
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
        df = df.drop_duplicates()
        with self.output().open('w') as output_file:
            df.to_csv(output_file, index=False, header=True)

    def output(self):
        return luigi.LocalTarget(
            f'{self.output_dir}/twitter/tweets.csv',
            format=UTF8
        )


class ExtractTweetPerformance(DataPreparationTask):
    """Extract performance values from the fetched tweets."""

    def _requires(self):
        return luigi.task.flatten([
            TweetsToDb(),
            super()._requires()
        ])

    def requires(self):
        return FetchTwitter()

    def run(self):
        with self.input().open('r') as input_file:
            df = pd.read_csv(input_file, dtype={
                'tweet_id': str, 'parent_tweet_id': str
            })

        df = df.filter(['tweet_id', 'likes', 'retweets', 'replies'])
        current_timestamp = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        df['timestamp'] = current_timestamp
        df['tweet_id'] = df['tweet_id'].apply(str)
        df = self.filter_fkey_violations(df)
        df = self.condense_performance_values(df)

        with self.output().open('w') as output_file:
            df.to_csv(output_file, index=False, header=True)

    def output(self):
        return luigi.LocalTarget(
            f'{self.output_dir}/twitter/tweet_performance.csv',
            format=UTF8
        )


class FetchTwitter(DataPreparationTask):
    """Fetch tweets related to the museum using the twitterscraper."""

    query = luigi.Parameter(default="museumbarberini")
    timespan = luigi.parameter.TimeDeltaParameter(
        default=dt.timedelta(days=60),
        description="For how many days tweets should be fetched")

    def output(self):
        return luigi.LocalTarget(
            f'{self.output_dir}/twitter/raw_tweets.csv',
            format=UTF8
        )

    def run(self):
        timespan = self.timespan
        if self.minimal_mode:
            timespan = dt.timedelta(days=5)

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

        # timestamp is utc by default
        df['timestamp'] = df['timestamp'].apply(
            lambda utc_dt:
            utc.localize(utc_dt, is_dst=None).astimezone(
                tzlocal.get_localzone()))

        with self.output().open('w') as output_file:
            df.to_csv(output_file, index=False, header=True)


class LoadTweetAuthors(DataPreparationTask):
    """Load information about hard-coded tweet authors."""

    def output(self):
        return luigi.LocalTarget("data/tweet_authors.csv", format=UTF8)
