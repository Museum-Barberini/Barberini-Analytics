import json

import luigi
from luigi.format import UTF8
import pandas as pd
import spacy
from spacy.tokens import Doc, Token
from tqdm import tqdm

from query_db import QueryDb
from json_converters import JsoncToJson
from data_preparation import DataPreparationTask
from posts import PostsToDb


"""
NEXT STEPS:
- [x] one task that matches patterns and outputs aspect-sentiment-pairs
- [ ] one task that reads aspect-sentiment-pairs, matches sentiment weights and groups them by aspect
    - where to put dbscan?

- rename aspect_sentiment to opinion globally? rather no, but refine namings. what about opinion_phrase here?
"""

class CollectPostOpinions(DataPreparationTask):

    spacy_model = 'de_core_news_sm'

    def _requires(self):

        return luigi.task.flatten([
            PostsToDb(),
            super()._requires()
        ])

    def requires(self):

        yield LoadOpinionPatterns()
        yield QueryDb(query='''
            SELECT source, post_id, text
            FROM post
            WHERE NOT is_from_museum
            AND text IS NOT NULL
        ''')

    def output(self):

        return luigi.LocalTarget(
            f'{self.output_dir}/absa/post_opinions.csv',
            format=UTF8
        )

    def run(self):

        inputs = self.input()
        with inputs[0].open() as stream:
            patterns = json.load(stream)
        with inputs[1].open() as stream:
            post_df = pd.read_csv(stream)

        pattern_df = pd.DataFrame(
            patterns.items(),
            columns=['pattern_name', 'pattern']
        )
        if self.minimal_mode:
            post_df = post_df.head(25)
        tqdm.pandas()

        nlp = spacy.load(self.spacy_model)

        post_df['doc'] = post_df['text'].progress_apply(nlp)
        post_df['pos_tags'] = post_df['doc'].apply(lambda doc:
            [token.pos_ for token in doc]
        )

        post_pattern_df = cross_join(post_df, pattern_df)
        post_pattern_df['tokens_list'] = post_pattern_df.apply(
            lambda row: list(self.extract_opinions(row.pattern, row)),
            axis=1
        )
        post_pattern_df = post_pattern_df.explode('tokens_list').dropna(
            subset=['tokens_list']
        ).rename(
            columns={'tokens_list': 'tokens'}
        )
        post_pattern_df['aspect_word'] = post_pattern_df.apply(
            lambda row: self.extract_segment(row.pattern, row, 'isAspect'),
            axis=1
        )
        post_pattern_df['sentiment_word'] = post_pattern_df.apply(
            lambda row: self.extract_segment(row.pattern, row, 'isSentiment'),
            axis=1
        )
        post_pattern_df = post_pattern_df[[
            'source', 'post_id', 'pattern_name',
            'aspect_word', 'sentiment_word'
        ]]

        with self.output().open('w') as output:
            post_pattern_df.to_csv(output, index=False)

    def extract_opinions(self, pattern, post):

        pattern_tags = [segment['pos'] for segment in pattern]
        for index in find_subseqs(post.pos_tags, pattern_tags):
            yield post.doc[index:index + len(pattern)]

    def extract_segment(self, pattern, post, property_):

        aspects = (
            token
            for token, segment
            in zip(post.tokens, pattern)
            if segment.get(property_, False)
        )

        aspect = next(aspects)
        try:
            next(aspects)
        except StopIteration:
            return aspect
        raise AssertionError("Multiple aspect words in pattern")


class LoadOpinionPatterns(JsoncToJson):

    def input(self):

        return luigi.LocalTarget(
            'data/absa/opinion_patterns.jsonc',
            format=UTF8
        )

    def output(self):

        return luigi.LocalTarget(
            f'{self.output_dir}/absa/opinion_patterns.json',
            format=UTF8
        )


def cross_join(df1, df2):
    """
    Return the cross product of two DataFrames.
    """

    magic_name = 'cross_join_side'
    return pd.merge(
        df1.assign(**{magic_name: None}),
        df2.assign(**{magic_name: None}),
        on=magic_name
    ).drop(magic_name, axis=1)

def find_subseqs(seq, sub):
    """
    Find all subsequences of seq that equal sub. Yield the index of each
    match.
    """

    if not isinstance(seq, list):
        return find_subseqs(list(seq), sub)
    n = len(sub)
    for i in range(len(seq) - n + 1):
        if sub == seq[i:i + n]:
            yield i
