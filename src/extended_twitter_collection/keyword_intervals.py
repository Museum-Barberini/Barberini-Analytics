import pandas as pd
import luigi
from stop_words import get_stop_words
from collections import defaultdict
import re
from luigi.format import UTF8
import datetime as dt
import numpy as np

from _utils import DataPreparationTask, CsvToDb
from twitter import TweetsToDb


class KeywordIntervalsToDB(CsvToDb):

    table = 'twitter_keyword_intervals'

    def requires(self):
        return KeywordIntervals()


class KeywordIntervals(DataPreparationTask):

    offset = luigi.IntParameter(default=7)

    def requires(self):
        return TermCounts()

    def run(self):

        with self.input().open("r") as input_file:
            terms_df = pd.read_csv(input_file)

        # fetch initial dataset (table tweet)
        initial_dataset = self.db_connector.query(
            """
            SELECT tweet_id, text, post_date
            FROM tweet
            """
        )
        initial_dataset = pd.DataFrame(
            initial_dataset, columns=["tweet_id", "text", "date"])

        # insert space before links to match hashtags correctly
        initial_dataset["text"] = initial_dataset["text"]\
            .str.replace("pic.", " pic.", regex=False)\
            .str.replace("https", " https", regex=False)\
            .str.replace("http", " http", regex=False)

        intervals_and_post_dates = terms_df.term.apply(
            self.get_relevance_timespan, args=(initial_dataset,))
        terms_df["relevance_timespan"] = [
            e[0] if not pd.isnull(e) else np.nan
            for e in intervals_and_post_dates
        ]
        terms_df["post_dates"] = [
            e[1] if not pd.isnull(e) else np.nan
            for e in intervals_and_post_dates
        ]
        terms_df = terms_df.dropna(subset=["post_dates"])
        terms_df["count"] = [len(x) for x in terms_df["post_dates"]]

        intervals = []
        for i, row in terms_df.iterrows():
            for x, y in row["relevance_timespan"]:
                start = min(x, y)
                end = max(x, y)
                count_interval = len([
                    date for date in row["post_dates"]
                    if date >= start and date <= end
                ])
                intervals.append(
                    (row["term"], row["count"], count_interval, start, end))
        intervals_df = pd.DataFrame(
            intervals, columns=["term", "count_overall", "count_interval",
                                "start_date", "end_date"]
        )

        with self.output().open('w') as output_file:
            intervals_df.to_csv(output_file, index=False, header=True)

    def output(self):
        return luigi.LocalTarget(
            f'{self.output_dir}/twitter/term_counts_with_intervals.csv',
            format=UTF8
        )

    def get_relevance_timespan(self, term, initial_dataset):

        print(" " * 60, end="\r")  # delete previous print
        print(f"current term: {term}", end="\r")

        if not isinstance(term, str):
            return np.nan

        # find all dates on which the term was used
        # exclude tweets where the term only appears as the domain
        # name in an url -> this favours terms such as maz-online or pnn
        regex = rf"(?<![(www\.)(http://)])\b{term}\b"

        relevant_tweets = initial_dataset[[
            True if re.findall(regex, tweet) else False
            for tweet in initial_dataset.text.str.lower()
        ]]
        post_dates = pd.to_datetime(
            relevant_tweets.date, infer_datetime_format=True)
        post_dates = post_dates.sort_values()
        post_dates_output = [str(date) for date in post_dates]
        if post_dates.empty:
            return np.nan

        # date range is date +/- 1 weeks (7 days)
        offset_dt = dt.timedelta(days=self.offset)

        intervals = []
        cur_start = post_dates.iloc[0]
        prev_date = post_dates.iloc[0]

        for date in post_dates[1:]:
            if abs((date-prev_date).days) <= self.offset*2:
                prev_date = date
            else:
                intervals.append(
                    (cur_start + offset_dt, prev_date - offset_dt))
                cur_start = date
                prev_date = date

        # handle last date
        last_date = post_dates.iloc[-1]
        intervals.append((cur_start + offset_dt, last_date - offset_dt))

        # convert to string
        intervals = [(str(a), str(b)) for a, b in intervals]

        return intervals, post_dates_output


class TermCounts(DataPreparationTask):

    def requires(self):
        return TweetsToDb()

    def run(self):

        # fetch initial dataset (table tweet)
        initial_dataset = self.db_connector.query(
            """
            SELECT text
            FROM tweet
            """
        )
        initial_dataset = pd.DataFrame(initial_dataset, columns=["text"])

        # insert space before links to match hashtags correctly
        initial_dataset["text"] = initial_dataset["text"]\
            .str.replace("pic.", " pic.", regex=False)\
            .str.replace("https", " https", regex=False)\
            .str.replace("http", " http", regex=False)

        # extract hashtags
        hashtags = []
        for text in initial_dataset["text"].tolist():
            for token in text.split():
                if not token.startswith("#"):
                    continue
                # lowercase
                token = token.lower()
                # remove '#'
                token = token.strip("#")
                # remove punctuation
                if not re.match(r"[a-z0-9öäüß_\-]+", token):
                    continue
                token = re.findall(r"[a-z0-9öäüß_\-]+", token)[0]
                # drop terms with 2 or less characters
                if len(token) <= 2:
                    continue
                # remove stop words
                if token in [*get_stop_words("de"), "twitter", "www"]:
                    continue
                hashtags.append(token.lower())

        # drop duplicates
        hashtags = list(set(hashtags))

        # count occurrences. Only count max one occurrence per tweet.
        term_counts = defaultdict(lambda: 0)
        for term in hashtags:
            print(term, end="\r")
            regex = rf"\b{term}\b"
            for tweet in initial_dataset["text"].tolist():
                if not tweet:
                    continue
                tweet = tweet.lower()
                term_counts[term] += 1 if re.findall(regex, tweet) else 0

        terms_ordered = sorted(
            term_counts.items(), key=lambda x: x[1], reverse=True)
        terms_df = pd.DataFrame(terms_ordered, columns=["term", "count"])

        with self.output().open('w') as output_file:
            terms_df.to_csv(output_file, index=False, header=True)

    def output(self):
        return luigi.LocalTarget(
            f'{self.output_dir}/twitter/term_counts.csv',
            format=UTF8
        )
