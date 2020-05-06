from functools import reduce

import luigi
import pandas as pd
import regex

from csv_to_db import CsvToDb
from data_preparation_task import DataPreparationTask
from .posts import FetchPosts


regex_type = type(regex.compile(''))


def regex_compile(pattern: str) -> regex_type:
    return regex.compile(pattern, flags=regex.VERBOSE | regex.VERSION1)


def regex_compile_outermost(pattern: str, flags=0) -> regex_type:
    return regex_compile(rf'''
        ^
        {pattern}
        |
        {pattern}
        $''')


def regex_compile_greedy_lookaround(pattern: str, flags=0) -> regex_type:
    return regex_compile(rf'''
        (?<=
            (?!{pattern})
            {pattern}+
        ) | (?=
            (?<!{pattern})
            {pattern}+
        )''')


class PostWordsToDB(CsvToDb):

    limit = luigi.IntParameter(
        default=-1,
        description="The maximum number posts to fetch. Optional. If -1, "
                    "all posts will be fetched.")

    shuffle = luigi.BoolParameter(
        default=False,
        description="If True, all posts will be shuffled. For debugging and "
                    "exploration purposes. Might impact performance.")

    table = 'post_word'

    columns = [
        ('source', 'TEXT'),
        ('post_id', 'TEXT'),
        ('word_index', 'INT'),
        ('word', 'TEXT')
    ]

    primary_key = 'source', 'post_id', 'word_index'

    def requires(self):
        return CollectPostWords(
            limit=self.limit,
            shuffle=self.shuffle)


class CollectPostWords(DataPreparationTask):

    limit = luigi.IntParameter(
        default=-1,
        description="The maximum number posts to fetch. Optional. If -1, "
                    "all posts will be fetched.")

    shuffle = luigi.BoolParameter(
        default=False,
        description="If True, all posts will be shuffled. For debugging and "
                    "exploration purposes. Might impact performance.")

    def requires(self):
        yield FetchPosts(
            limit=self.limit,
            shuffle=self.shuffle)

    def output(self):
        return luigi.LocalTarget(
            'output/post_words.csv',
            format=luigi.format.UTF8)

    def run(self):
        with self.input()[0].open('r') as posts_stream:
            posts = pd.read_csv(posts_stream)

        tokens_per_post = {
            (source, post_id): self.tokenize(text)
            for (source, post_id, text)
            in posts.itertuples(index=False)
        }
        words_per_post = {
            (source, post_id, word_index): token
            for ((source, post_id), tokens) in tokens_per_post.items()
            for (word_index, token) in enumerate(tokens)
        }
        post_words = pd.Series(words_per_post) \
            .rename_axis(['source', 'post_id', 'word_index']) \
            .reset_index(name='word')

        with self.output().open('w') as words_stream:
            post_words.to_csv(words_stream, index=False, header=True)

    # Patterns to split text, removing the matching chars
    split = regex_compile(r'''
            \s+                 # Whitespace
            |
            (?<!\/\S*)\/(?!\/)  # Slashes (but not in URLs)
        ''')
    # Patterns to isolate a separate word
    separate = regex_compile_greedy_lookaround(r'''
            \p{So}+  # Symbols and emojis
        ''')
    # Patterns to strip from beginning and end of each word
    strip = regex_compile_outermost(r'''
            # every punctuation, but not:
            # hashtags and mentions (twitter)
            [
                \p{P}
                --[
                    \@\#
                ]
            ]+
        ''')
    # Patterns to ignore words
    ignore = regex_compile(r'''
            ^\w$  # Single character as word
        ''')
    # Patterns to compress the remaining words
    compressions = {
        # Treat repeated emoji as one occurence
        regex_compile(r'(\p{So}){2,}'): r'\1'
    }

    def tokenize(self, text):
        text = self.separate.sub(' ', text)
        tokens = self.split.split(text)
        tokens = [
            self.strip.sub('', token)
            for token
            in tokens
            if token
        ]
        tokens = [
            token.lower()
            for token
            in tokens
            if token
        ]
        tokens = [
            token
            for token
            in tokens
            if not self.ignore.match(token)
        ]
        tokens = [
            reduce(
                lambda _token, compression:
                    compression[0].sub(compression[1], _token),
                self.compressions.items(),
                token)
            for token
            in tokens
        ]
        return tokens
