import pandas as pd
import luigi
from luigi.format import UTF8
import datetime as dt
import twint
import sys
import os
import csv

from _utils import DataPreparationTask, CsvToDb
from extended_twitter_collection.keyword_intervals import KeywordIntervalsToDB


class TwitterExtendedDatasetToDB(CsvToDb):

    table = "twitter_extended_dataset"

    def requires(self):
        return TwitterExtendedDataset()


class TwitterExtendedDataset(DataPreparationTask):

    def requires(self):
        return TwitterCandidateTweetsToDB()

    def output(self):
        return luigi.LocalTarget(
            f"{self.output_dir}/twitter/extended_dataset.csv",
            format=UTF8
        )
    
    def run(self):

        r_thresh = 50
        ranking_thresh = 0.8

        # apply thresholds
        extended_dataset = self.db_connector.query(f"""
            SELECT user_id, tweet_id::text, text, response_to, post_date, permalink
            FROM
            -- keyword-intervals enriched with interval-based R value
            (
                SELECT * FROM (
                    SELECT ki.term, ki.start_date, ki.end_date, ki.count_interval, count(*)::float/ki.count_interval::float AS R_interval
                    FROM twitter_keyword_intervals ki INNER JOIN twitter_extended_candidates ec
                    ON ki.term = ec.term AND ec.post_date BETWEEN ki.start_date AND ki.end_date
                    WHERE ki.count_interval > 0
                    GROUP BY ki.term, ki.start_date, ki.end_date, ki.count_interval
                ) as temp
                -- only consider keyword-intervals with an R value below 50
                WHERE R_interval <= {r_thresh}
            ) AS ki_r
            INNER JOIN twitter_extended_candidates ec
            ON
                -- match tweets to intervals based on the post date
                ec.post_date between ki_r.start_date and ki_r.end_date
            AND
                -- tweet should contain the term as a whole word
                ec.text ~* ('\m' || ki_r.term || '\M')
            GROUP BY user_id, tweet_id, text, response_to, post_date, permalink
            -- Only keep top-ranked tweets
            HAVING sum(1 / r_interval) >= {ranking_thresh} 
        """)

        extended_dataset = pd.DataFrame(extended_dataset, columns=["user_id", "tweet_id", "text", "response_to", "post_date", "permalink"], dtype=str)
        extended_dataset = extended_dataset.drop_duplicates(subset=["tweet_id"])

        with self.output().open("w") as output_file:
            extended_dataset.to_csv(output_file, index=False, quoting=csv.QUOTE_NONNUMERIC)

class TwitterCandidateTweetsToDB(CsvToDb):

    table = 'twitter_extended_candidates'

    def requires(self):
        return TwitterCollectCandidateTweets()


class TwitterCollectCandidateTweets(DataPreparationTask):

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
        if not tweets_df.empty:
            tweets_df["text"] = tweets_df["text"]\
                .str.replace("pic.", " pic.", regex=False)\
                .str.replace("https", " https", regex=False)\
                .str.replace("http", " http", regex=False)

        return tweets_df


class HiddenPrints:
    def __enter__(self):
        self._original_stdout = sys.stdout
        sys.stdout = open(os.devnull, 'w')

    def __exit__(self, exc_type, exc_val, exc_tb):
        sys.stdout.close()
        sys.stdout = self._original_stdout
