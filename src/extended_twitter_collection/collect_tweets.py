"""
Extended twitter collection.

TODO: Refine docstrings!
"""

import csv
import datetime as dt

import pandas as pd
import luigi
from luigi.format import UTF8
import twint

from _utils import CsvToDb, DataPreparationTask, logger
from .keyword_intervals import KeywordIntervalsToDB


class TwitterExtendedDatasetToDB(CsvToDb):

    table = 'twitter_extended_dataset'

    def requires(self):
        return TwitterExtendedDataset()


class TwitterExtendedDataset(DataPreparationTask):

    # Define thresholds for filtering algorithm.
    r_thresh = luigi.IntParameter(default=50)
    ranking_thresh = luigi.FloatParameter(default=0.8)

    def requires(self):
        return TwitterCandidateTweetsToDB()

    def output(self):
        return luigi.LocalTarget(
            f'{self.output_dir}/twitter/extended_dataset.csv',
            format=UTF8
        )

    def run(self):

        # Apply filtering based on thresholds
        extended_dataset = self.db_connector.query(fr'''
            SELECT user_id, tweet_id::text, text, response_to,
                   post_date, permalink, likes, retweets, replies
            FROM
            -- keyword-intervals enriched with interval-based R value
            (
                SELECT * FROM (
                    SELECT ki.term, ki.start_date, ki.end_date,
                           ki.count_interval,
                        count(*)::float/ki.count_interval::float AS R_interval
                    FROM
                        twitter_keyword_intervals ki
                    INNER JOIN
                        twitter_extended_candidates ec
                    ON
                        ki.term = ec.term
                    AND
                        ec.post_date BETWEEN ki.start_date AND ki.end_date
                    WHERE ki.count_interval > 0
                    GROUP BY ki.term, ki.start_date,
                             ki.end_date, ki.count_interval
                ) as temp
                -- only consider keyword-intervals with
                -- an R value below the threshold
                WHERE R_interval <= {self.r_thresh}
            ) AS ki_r
            INNER JOIN twitter_extended_candidates AS ec
            ON
                -- match tweets to intervals based on the post date
                ec.post_date between ki_r.start_date and ki_r.end_date
            AND
                -- tweet should contain the term as a whole word
                ec.text ~* ('\m' || ki_r.term || '\M')
            GROUP BY user_id, tweet_id, text,
                     response_to, post_date, permalink,
                     likes, retweets, replies
            -- Only keep top-ranked tweets
            HAVING sum(1 / r_interval) >= {self.ranking_thresh}
        ''')

        # create a dataframe from the filtering results
        extended_dataset = pd.DataFrame(
            extended_dataset, columns=[
                'user_id', 'tweet_id', 'text',
                'response_to', 'post_date', 'permalink',
                'likes', 'retweets', 'replies'
            ], dtype=str)
        extended_dataset = extended_dataset.drop_duplicates(
            subset=['tweet_id'])

        # Output results. Use restrictive quoting to
        # make database import less error prone.
        with self.output().open('w') as output_file:
            extended_dataset.to_csv(
                output_file, index=False, quoting=csv.QUOTE_NONNUMERIC)


class TwitterCandidateTweetsToDB(CsvToDb):

    table = 'twitter_extended_candidates'

    def requires(self):
        return TwitterCollectCandidateTweets()


class TwitterCollectCandidateTweets(DataPreparationTask):

    # only fetch the first count * collection_r_limit tweets
    # for a given keyword-interval
    collection_r_limit = luigi.IntParameter(default=50)

    def requires(self):
        return KeywordIntervalsToDB()

    def output(self):
        return luigi.LocalTarget(
            f'{self.output_dir}/twitter/extended_candidates.csv',
            format=UTF8
        )

    def run(self):

        # We will fetch all tweets from the last 7 days
        # for the keywords that have active intervals.
        active_intervals = self.db_connector.query(f'''
            SELECT term, count_interval
            FROM twitter_keyword_intervals
            WHERE end_date >= CURRENT_DATE
            {'LIMIT 500' if self.minimal_mode else ''}
        ''')

        seven_days_ago = (
            dt.datetime.today() - dt.timedelta(days=7)
        ).strftime('%Y-%m-%d')

        # fetch the tweets
        tweet_dfs = []  # list of dataframes
        for term, count in self.tqdm(
                active_intervals, desc="Extending tweet collection"):
            tweet_dfs.append(self.fetch_tweets(
                query=term,
                start_date=seven_days_ago,
                limit=count * self.collection_r_limit
            ))

        # always query 'museumbarberini'
        # TODO: Don't hardcode this.
        tweet_dfs.append(self.fetch_tweets(
            query="museumbarberini",
            start_date=seven_days_ago,
            limit=10000
        ))

        # create dataframe with all the fetched tweets
        tweet_df = pd.concat(tweet_dfs)
        tweet_df = tweet_df.drop_duplicates(subset=['term', 'tweet_id'])
        tweet_df = self.encode_strings(tweet_df)

        with self.output().open('w') as output_file:
            tweet_df.to_csv(output_file, index=False, header=True)

    def fetch_tweets(self, query, start_date, limit):
        """All searches are limited to German tweets (twitter lang code de)."""
        logger.debug(
            f"Querying Tweets. term \"{query}\", "
            f"limit: {limit}, start_date: {start_date}"
        )

        tweets = []  # tweets go in this list

        # set config options for twint
        c = twint.Config()
        c.Limit = limit
        c.Search = query
        c.Store_object = True
        c.Since = f'{start_date} 00:00:00'
        c.Lang = 'de'
        c.Hide_output = True
        c.Store_object_tweets_list = tweets

        # execute the twitter search
        twint.run.Search(c)

        # create dataframe from search results
        tweets_df = pd.DataFrame([
            {
                'term': query,
                'user_id': t.user_id,
                'tweet_id': t.id,
                'text': t.tweet,
                'response_to': '',
                'post_date': t.datestamp,
                'permalink': t.link,
                'likes': t.likes_count,
                'retweets': t.retweets_count,
                'replies': t.replies_count
            }
            for t in tweets
        ])

        # insert space before links to match hashtags correctly
        if not tweets_df.empty:
            tweets_df['text'] = tweets_df['text']\
                .str.replace('pic.', ' pic.', regex=False)\
                .str.replace('https', ' https', regex=False)\
                .str.replace('http', ' http', regex=False)

        return tweets_df
