import pandas as pd
import luigi
from luigi.format import UTF8
import datetime as dt
import twint
import sys
import os

from _utils import DataPreparationTask, CsvToDb
from extended_twitter_collection.keyword_intervals import KeywordIntervalsToDB


class ExtendedTwitterDatasetToDB(CsvToDb):

    table = 'twitter_extended_candidates'

    def requires(self):
        return CollectExtendedTwitterDataset()


class CollectExtendedTwitterDataset(DataPreparationTask):

    def requires(self):
        return KeywordIntervalsToDB()

    def output(self):
        return luigi.LocalTarget(
            f'{self.output_dir}/twitter/extended_candidates.csv',
            format=UTF8
        )

    def run(self):

        active_intervals = self.db_connector.query("""
            SELECT term, count_interval
            FROM twitter_keyword_intervals
            WHERE end_date >= CURRENT_DATE
        """)

        seven_days_ago = (
            dt.datetime.today() - dt.timedelta(days=7)
        ).strftime("%Y-%m-%d")

        tweet_dfs = []
        for term, count in active_intervals:
            tweet_dfs.append(self.fetch_tweets(
                query=term,
                start_date=seven_days_ago,
                limit=count * 50
            ))

        # always query 'museumbarberini'
        tweet_dfs.append(self.fetch_tweets(
            query="museumbarberini",
            start_date=seven_days_ago,
            limit=2000
        ))

        tweet_df = pd.concat(tweet_dfs)
        tweet_df = tweet_df.drop_duplicates(subset=["term", "tweet_id"])

        with self.output().open('w') as output_file:
            tweet_df.to_csv(output_file, index=False, header=True)

    def fetch_tweets(self, query, start_date, limit):
        """
        All searches are limited to German tweets (twitter lang code de)
        """

        print(f"Querying Tweets. term \"{query}\" "
              "limit: {limit}, start_date: {start_date}")
        tweets = []  # tweets go in this list

        c = twint.Config()
        c.Limit = limit
        c.Search = query
        c.Store_object = True
        c.Since = f"{start_date} 00:00:00"
        c.Lang = "de"
        c.Store_object_tweets_list = tweets

        # suppress twint output
        with HiddenPrints():
            twint.run.Search(c)

        tweets_df = pd.DataFrame([
            {
                "term": query,
                "user_id": t.user_id,
                "tweet_id": t.id,
                "text": t.tweet,
                "response_to": "",
                "post_date": t.datestamp,
                "permalink": t.link
            }
            for t in tweets
        ])
        # insert space before links to match hashtags correctly
        tweets_df["text"] = tweets_df["text"]\
            .replace("pic.", " pic.", regex=False)\
            .replace("https", " https", regex=False)\
            .replace("http", " http", regex=False)

        return tweets_df


class HiddenPrints:
    def __enter__(self):
        self._original_stdout = sys.stdout
        sys.stdout = open(os.devnull, 'w')

    def __exit__(self, exc_type, exc_val, exc_tb):
        sys.stdout.close()
        sys.stdout = self._original_stdout
