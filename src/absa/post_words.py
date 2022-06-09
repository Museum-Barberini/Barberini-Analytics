"""Provides tasks for splitting up every post into relevant words."""

from functools import reduce
from typing import Iterable, Tuple

import luigi
import pandas as pd
import regex

from _utils import CsvToDb, DataPreparationTask, QueryDb
from _posts import PostsToDb


regex_type = type(regex.compile(''))


def regex_compile(pattern: str) -> regex_type:
    """Compile the regex pattern in the preferred flavour."""
    return regex.compile(pattern, flags=regex.VERBOSE | regex.VERSION1)


def regex_compile_outermost(pattern: str, flags=0) -> regex_type:
    """Create a regex that begins or ends with the given pattern."""
    return regex_compile(rf'''
        ^
        {pattern}
        |
        {pattern}
        $''')


def regex_compile_greedy_lookaround(pattern: str, flags=0) -> regex_type:
    """Create a regex with a "greedy lookaround" of the given pattern."""
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
                WITH new_post_id AS (
                    SELECT post_id
                    FROM post
                    WHERE post_date > ANY(
                        SELECT max(post_date)
                        FROM {self.table}
                        NATURAL JOIN post
                    ) IS NOT FALSE
                )
                SELECT source, post_id, text
                FROM {self.post_table}
                NATURAL JOIN new_post_id
                WHERE text <> ''
            ''',  # nosec B608
            limit=self.limit,
            shuffle=self.shuffle)
        with posts_target.open('r') as posts_stream:
            posts = pd.read_csv(posts_stream)

        tokens_per_post = {
            (source, post_id): self.tokenize(text)
            for source, post_id, text
            in posts.itertuples(index=False)
        }
        words_per_post = {
            (source, post_id, word_index + 1): token
            for (source, post_id), tokens in tokens_per_post.items()
            for word_index, token in enumerate(tokens)
        }

        post_words = pd.DataFrame(columns=[
            'source', 'post_id', 'word_index', 'word', 'sentence_index'
        ])
        post_words.set_index(list(post_words.columns[:3]), inplace=True)
        post_words = post_words.append(
            pd.DataFrame(words_per_post).T.rename(
                columns=dict(enumerate(post_words.columns))
            )
        )

        with self.output().open('w') as words_stream:
            post_words.to_csv(words_stream, header=True)

    # Patterns to isolate a separate word
    separate = regex_compile_greedy_lookaround(r'''
            \p{So}+  # Symbols and emojis
        ''')

    split_sentence = regex_compile(r'''
            [\.\?\!]+ \s+
            |
            [\r\n]+
        ''')

    # Patterns to split text, removing the matching chars
    split = regex_compile(r'''
            \s+                 # Whitespace
            |
            (?<!\/\S*) \/ (?!\/)  # Slashes (but not in URLs)
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
        regex_compile(r'(\p{So}) {2,}'): r'\1'
    }

    def tokenize(self, text) -> Iterable[Tuple[int, str]]:

        sentences = self.split_sentence.split(text)

        index = 0
        for sentence in sentences:
            tokens = self.tokenize_sentence(sentence)
            if not tokens:
                continue
            index += 1
            for token in tokens:
                yield token, index

    def tokenize_sentence(self, text) -> Iterable[str]:

        text = self.separate.sub(' ', text)
        tokens = self.split.split(text)
        tokens = [
            self.strip.sub('', token)
            for token in tokens
            if token
        ]
        tokens = [
            token.lower()
            for token in tokens
            if token
        ]
        tokens = [
            token
            for token in tokens
            if not self.ignore.match(token)
        ]
        tokens = [
            reduce(
                lambda _token, compression:
                    compression[0].sub(compression[1], _token),
                self.compressions.items(),
                token)
            for token in tokens
        ]
        return tokens
