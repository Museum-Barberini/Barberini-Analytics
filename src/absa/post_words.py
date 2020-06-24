from functools import reduce

import luigi
import pandas as pd
import regex

from csv_to_db import CsvToDb
from data_preparation import DataPreparationTask
from posts import PostsToDb
from query_db import QueryDb


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


class PostWordsToDb(CsvToDb):

    table = 'absa.post_word'

    limit = luigi.IntParameter(
        default=-1,
        description="The maximum number posts to fetch. Optional. If -1, "
                    "all posts will be fetched.")

    shuffle = luigi.BoolParameter(
        default=False,
        description="If True, all posts will be shuffled. For debugging and "
                    "exploration purposes. Might impact performance.")

    standalone = luigi.BoolParameter(
        default=False,
        description="If False, the post database will be updated before the"
                    "posts will be collected. If True, only posts already in"
                    "the database will be respected.")

    def requires(self):
        return CollectPostWords(
            table=self.table,
            limit=self.limit,
            shuffle=self.shuffle,
            standalone=self.standalone)


class CollectPostWords(DataPreparationTask):

    limit = luigi.IntParameter(
        default=-1,
        description="The maximum number posts to fetch. Optional. If -1, "
                    "all posts will be fetched.")

    shuffle = luigi.BoolParameter(
        default=False,
        description="If True, all posts will be shuffled. For debugging and "
                    "exploration purposes. Might impact performance.")

    standalone = luigi.BoolParameter(
        default=False,
        description="If False, the post database will be updated before the"
                    "posts will be collected. If True, only posts already in"
                    "the database will be respected.")

    post_table = 'post'

    def output(self):
        return luigi.LocalTarget(
            f'{self.output_dir}/absa/post_words.csv',
            format=luigi.format.UTF8
        )

    def run(self):
        if not self.standalone:
            yield PostsToDb()

        posts_target = yield QueryDb(
            query=f'''
                WITH known_post_ids AS (SELECT post_id FROM {self.table})
                SELECT source, post_id, text
                FROM {self.post_table}
                WHERE text <> ''
                AND post_id NOT IN (SELECT * FROM known_post_ids)
            ''',
            limit=self.limit,
            shuffle=self.shuffle)
        with posts_target.open('r') as posts_stream:
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
            .reset_index(name='word') \
            if words_per_post else \
            pd.DataFrame(columns=['word', 'source', 'post_id', 'word_index'])

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
