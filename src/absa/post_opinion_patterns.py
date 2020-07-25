import json
import logging

import gensim
import luigi
from luigi.format import UTF8
import numpy as np
import pandas as pd
from sklearn.cluster import DBSCAN
import spacy
from tqdm import tqdm

from _posts import PostsToDb
from _utils import ConcatCsvs, CsvToDb, DataPreparationTask, JsoncToJson, QueryDb
from .german_word_embeddings import FetchGermanWordEmbeddings
from .phrase_polarity import PhrasePolaritiesToDb


logger = logging.getLogger('luigi-interface')

tqdm.pandas()


class PostOpinionSentimentsToDb(CsvToDb):

    table = 'absa.post_opinion_sentiment'

    replace_content = True

    def requires(self):

        return ConcatPostOpinionSentiments()


class ConcatPostOpinionSentiments(ConcatCsvs):

    sentiment_match_algorithms = ['identity', 'inflected']

    def requires(self):

        for match_algorithm in self.sentiment_match_algorithms:
            yield GroupPostOpinionSentiments(
                sentiment_match_algorithm=match_algorithm
            )

    def output(self):

        return luigi.LocalTarget(
            f'{self.output_dir}/absa/post_opinion_sentiments_grouped.csv',
            format=UTF8
        )

    def collect_csvs(self):

        for match_algorithm, target in zip(
                    self.sentiment_match_algorithms,
                    self.input()
                ):
            yield self.read_csv(target).assign(
                sentiment_match_algorithm=match_algorithm
            )[[
                'source', 'post_id', 'target_aspect_words',
                'count', 'sentiment', 'aspect_phrase',
                'dataset', 'sentiment_match_algorithm'
            ]]


class GroupPostOpinionSentiments(DataPreparationTask):

    sentiment_match_algorithm = luigi.Parameter()

    def requires(self):

        yield CollectPostOpinionSentiments(
            sentiment_match_algorithm=self.sentiment_match_algorithm
        )
        yield FetchGermanWordEmbeddings()

    def output(self):

        return luigi.LocalTarget(
            f'{self.output_dir}/absa/post_opinion_sentiments_'
            f'{self.sentiment_match_algorithm}_grouped.csv',
            format=UTF8
        )

    def run(self):

        logger.info("Loading input ...")
        df, self.model = self.load_input()

        logger.info("Computing word vectors ...")
        df['vec'] = df['aspect_phrase'].progress_apply(self.word2vec)
        df = df.dropna(subset=['vec'])

        logger.info("Clustering aspect phrases ...")
        df = self.cluster_aspects(df)

        logger.info("Labelling clusters ...")
        df = self.label_clusters(df)

        df = df.groupby([
            'source', 'post_id', 'dataset', 'target_aspect_words'
        ]).agg({
            'aspect_phrase': list,
            'sentiment': 'mean',
            'count': 'sum'
        }).rename({'aspect_phrases': 'aspect_phrase'})

        logger.info("Storing ...")
        with self.output().open('w') as stream:
            df.to_csv(stream, index=True)
        logger.info("Done.")

    def load_input(self):

        input_ = self.input()
        with input_[0].open() as stream:
            df = pd.read_csv(stream)
        model = gensim.models.KeyedVectors.load_word2vec_format(
            input_[1].path,
            binary=True
        )
        return df, model

    word2vec_trans = str.maketrans({
        " ": '_',
        "ä": 'ae',
        "ö": 'oe',
        "ü": 'ue',
        "Ä": 'Ae',
        "Ö": 'Oe',
        "Ü": 'Ue',
        "ß": 'ss'
    })

    def word2vec(self, word):

        word = word.translate(self.word2vec_trans)
        try:
            return self.model.get_vector(word)
        except KeyError:
            return None

    dbscan_eps, dbscan_minsamples = 0.37, 2

    def cluster_aspects(self, df):

        dbscan = DBSCAN(metric='cosine', eps=self.dbscan_eps, min_samples=self.dbscan_minsamples)
        df['bin'] = dbscan.fit(list(df['vec'])).labels_

        is_noise = df['bin'] == -1
        noise, nonnoise = df[is_noise], df[~is_noise]
        logger.info(
            f"DBSCAN: Created {len(nonnoise)} bins, "
            f"{len(noise) / len(df) * 100 :.2f}% noise."
        )

        return nonnoise

    cluster_label_topn = 3

    def label_clusters(self, df):

        bins = df.groupby('bin')[['vec']].agg(
            lambda vecs: np.array(list(np.average(
                np.array(vecs.apply(tuple), dtype=object),
                axis=0
            )))
        )
        bins['target_aspect_words'] = bins['vec'].apply(
            lambda vec: next(zip(*self.model.similar_by_vector(
                vec, topn=self.cluster_label_topn
            )))
        )

        return df.merge(bins, on='bin')


class CollectPostOpinionSentiments(DataPreparationTask):

    polarity_table = 'absa.phrase_polarity'

    sentiment_match_algorithm = luigi.Parameter()

    def _requires(self):

        return luigi.task.flatten([
            PhrasePolaritiesToDb(),
            super()._requires()
        ])

    def requires(self):

        return CollectPostOpinionPhrases()

    def output(self):

        return luigi.LocalTarget(
            f'{self.output_dir}/absa/'
            f'post_opinion_sentiments_{self.sentiment_match_algorithm}.csv',
            format=UTF8
        )

    def run(self):

        with self.input().open() as stream:
            opinion_phrases = pd.read_csv(stream)

        polarities = yield QueryDb(
            query={
                'identity': f'''
                    SELECT dataset, phrase, weight
                    FROM {self.polarity_table}
                ''',
                'inflected': f'''
                    SELECT pp.dataset, inflected AS phrase, weight
                    FROM {self.polarity_table} AS pp
                    JOIN absa.inflection ON (
                        pp.dataset, phrase
                    ) = (
                        inflection.dataset, word
                    )
                '''
            }[self.sentiment_match_algorithm],
            limit=-2
        )
        with polarities.open() as stream:
            polarity_phrases = pd.read_csv(stream)

        opinion_sentiments = pd.merge(
            opinion_phrases,
            polarity_phrases,
            left_on='sentiment_phrase',
            right_on='phrase'
        ).groupby(by=['source', 'post_id', 'dataset', 'aspect_phrase'])[
            'weight'
        ].agg(sentiment='mean', count='count')

        with self.output().open('w') as stream:
            opinion_sentiments.to_csv(stream, index=True)


class CollectPostOpinionPhrases(DataPreparationTask):

    spacy_model = 'de_core_news_sm'

    post_table = 'post'

    def _requires(self):

        return luigi.task.flatten([
            PostsToDb(),
            super()._requires()
        ])

    def requires(self):

        yield LoadOpinionPatterns()
        yield QueryDb(query=f'''
            SELECT source, post_id, text
            FROM {self.post_table}
            WHERE NOT is_from_museum
            AND text IS NOT NULL
        ''')

    def output(self):

        return luigi.LocalTarget(
            f'{self.output_dir}/absa/post_opinion_phrases.csv',
            format=UTF8
        )

    def run(self):

        pattern_df, post_df = self.load_input()

        logger.info("Analyzing posts ...")
        post_df = self.analyze_grammar(post_df)
        logger.info("Collecting opinion phrases ...")
        post_pattern_df = self.collect_phrases(pattern_df, post_df)

        logger.info("Storing ...")
        post_pattern_df = post_pattern_df[[
            'source', 'post_id', 'pattern_name',
            'aspect_phrase', 'sentiment_phrase'
        ]]
        with self.output().open('w') as output:
            post_pattern_df.to_csv(output, index=False)

        logger.info("Done.")

    def load_input(self):

        input_ = self.input()
        with input_[0].open() as stream:
            patterns = json.load(stream)
        with input_[1].open() as stream:
            post_df = pd.read_csv(stream)

        pattern_df = pd.DataFrame(
            patterns.items(),
            columns=['pattern_name', 'pattern']
        )

        return pattern_df, post_df

    def analyze_grammar(self, post_df):

        nlp = spacy.load(self.spacy_model)

        return post_df.assign(
            doc=lambda post:
                post.text.progress_apply(nlp)
        ).assign(
            pos_tags=lambda post:
                post.doc.apply(lambda doc: [token.pos_ for token in doc])
        )

    def collect_phrases(self, pattern_df, post_df):

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

        post_pattern_df['aspect_phrase'] = post_pattern_df.apply(
            lambda row: self.extract_segment(row.pattern, row, 'isAspect'),
            axis=1
        )
        post_pattern_df['sentiment_phrase'] = post_pattern_df.apply(
            lambda row: self.extract_segment(row.pattern, row, 'isSentiment'),
            axis=1
        )

        post_pattern_df.drop('tokens', axis=1)
        return post_pattern_df

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
