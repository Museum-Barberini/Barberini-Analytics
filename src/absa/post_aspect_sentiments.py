"""Provides tasks for determining the aspect-wise sentiments of user posts."""

import luigi

from _utils import QueryCacheToDb
from .post_aspects import PostAspectsToDb
from .post_ngrams import PostNgramsToDb
from .post_sentiments import PostPhrasePolaritiesToDb


class PostAspectSentimentsToDb(luigi.WrapperTask):

    def requires(self):

        # For post_aspect_sentiment_max
        yield PostPhraseAspectPolaritiesToDb()

        yield PostAspectSentimentsLinearDistanceToDb()


class PostPhraseAspectPolaritiesToDb(QueryCacheToDb):

    table = 'absa.post_phrase_aspect_polarity'

    aspect_table = 'absa.post_aspect'

    polarity_table = 'absa.post_phrase_polarity'

    phrase_table = 'absa.post_ngram'

    def requires(self):

        yield PostAspectsToDb()
        yield PostPhrasePolaritiesToDb()
        yield PostNgramsToDb()

    @property
    def query(self):

        return f'''
            SELECT
                polarity_phrase.source, polarity_phrase.post_id,
                aspect_id,
                aspect_phrase.n AS aspect_phrase_n,
                aspect_phrase.word_index AS aspect_word_index,
                aspect_phrase.sentence_index AS aspect_sentence_index,
                polarity_phrase.n AS polarity_phrase_n,
                polarity_phrase.word_index AS polarity_word_index,
                polarity_phrase.sentence_index AS polarity_sentence_index,
                CASE
                    WHEN sum(polarity) = 0 THEN NULL
                    ELSE sum(polarity ^ 2) / sum(polarity)
                END AS sentiment,
                count(DISTINCT {self.aspect_table}.word_index) AS count,
                dataset,
                {self.aspect_table}.match_algorithm AS aspect_match_algorithm,
                post_phrase_polarity.match_algorithm
                    AS sentiment_match_algorithm
            FROM {self.polarity_table} AS post_phrase_polarity
                JOIN {self.phrase_table} AS polarity_phrase
                    USING (source, post_id, n, word_index)
                JOIN {self.aspect_table}
                    USING (source, post_id)
                JOIN {self.phrase_table} AS aspect_phrase ON
                    (
                        aspect_phrase.source,
                        aspect_phrase.post_id,
                        aspect_phrase.word_index
                    ) = (
                        {self.aspect_table}.source,
                        {self.aspect_table}.post_id,
                        {self.aspect_table}.word_index
                    )
            GROUP BY
                polarity_phrase.source, polarity_phrase.post_id,
                aspect_id,
                aspect_phrase.n, aspect_phrase.word_index,
                polarity_phrase.n, polarity_phrase.word_index,
                dataset, aspect_match_algorithm,
                sentiment_match_algorithm,
                -- Pseudo joins:
                aspect_phrase.sentence_index, polarity_phrase.sentence_index
        '''


class PostAspectSentimentsLinearDistanceToDb(luigi.WrapperTask):

    def requires(self):

        yield PostAspectSentimentsLinearDistanceLimitToDb()
        yield PostAspectSentimentsLinearDistanceWeightToDb()


class PostAspectSentimentsLinearDistanceLimitToDb(QueryCacheToDb):

    table = 'absa.post_aspect_sentiment_linear_distance_limit'

    phrase_aspect_table = 'absa.post_phrase_aspect_polarity_linear_distance'

    def requires(self):

        return PostPhraseAspectPolaritiesLinearDistanceToDb()

    @property
    def kwargs(self):

        return dict(threshold=self.threshold)

    threshold = 4

    @property
    def query(self):

        return f'''
            WITH linear_distance AS (
                SELECT *
                FROM {self.phrase_aspect_table}
                WHERE linear_distance <= %(threshold)s
            )
            SELECT
                source, post_id,
                aspect_id,
                avg(linear_distance) AS linear_distance,
                CASE
                    WHEN sum(polarity) = 0 THEN NULL
                    ELSE sum(polarity ^ 2) / sum(polarity)
                END AS sentiment,
                count(DISTINCT aspect_word_index) AS aspect_count,
                count(DISTINCT polarity_word_index) AS polarity_count,
                dataset,
                aspect_match_algorithm,
                sentiment_match_algorithm
            FROM linear_distance
            GROUP BY
                source, post_id, aspect_id,
                dataset, aspect_match_algorithm,
                sentiment_match_algorithm
        '''


class PostAspectSentimentsLinearDistanceWeightToDb(QueryCacheToDb):

    table = 'absa.post_aspect_sentiment_linear_distance_weight'

    phrase_aspect_table = 'absa.post_phrase_aspect_polarity_linear_distance'

    def requires(self):

        return PostPhraseAspectPolaritiesLinearDistanceToDb()

    @property
    def kwargs(self):

        return dict(weight_alpha=self.weight_alpha)

    weight_alpha = 5

    # TODO: Decide whether we want to square polarity
    # (See commit 99193460503243ae893d837a58470e7c93995448)
    @property
    def query(self):

        return f'''
            WITH linear_weight AS (
                SELECT
                    *,
                    (CASE WHEN
                        linear_distance <= %(weight_alpha)s * sqrt(-ln(1e-6))
                        THEN round(
                            exp(-(
                                (linear_distance::numeric / %(weight_alpha)s) ^ 2
                            )),
                            6
                        )
                        ELSE 0
                    END) AS linear_weight
                FROM {self.phrase_aspect_table}
            )
            SELECT
                source, post_id,
                aspect_id,
                avg(linear_distance) AS linear_distance,
                CASE
                    WHEN sum(polarity) = 0 THEN NULL
                    ELSE sum(polarity ^ 2 * linear_weight)
                        / sum(polarity * linear_weight)
                END AS sentiment,
                count(DISTINCT aspect_word_index) AS aspect_count,
                count(DISTINCT polarity_word_index) AS polarity_count,
                dataset,
                aspect_match_algorithm,
                sentiment_match_algorithm
            FROM linear_weight
            WHERE linear_weight > 0
            GROUP BY
                source, post_id, aspect_id,
                dataset, aspect_match_algorithm,
                sentiment_match_algorithm
        '''


class PostPhraseAspectPolaritiesLinearDistanceToDb(QueryCacheToDb):

    table = 'absa.post_phrase_aspect_polarity_linear_distance'

    phrase_aspect_table = 'absa.post_phrase_aspect_polarity'

    def requires(self):

        return PostPhraseAspectPolaritiesToDb()

    @property
    def query(self):

        return f'''
            WITH linear_distance AS (
                SELECT
                    post_phrase_aspect_polarity.source,
                    post_phrase_aspect_polarity.post_id,
                    aspect_id,
                    aspect_word_index,  -- TODO: Add missing n
                    polarity_phrase_n, polarity_word_index,
                    CASE
                        WHEN sum(polarity) = 0 THEN NULL
                        ELSE sum(polarity ^ 2) / sum(polarity)
                    END AS sentiment,
                    least(
                        min(abs(polarity_word_index - (
                            aspect_word_index + aspect_phrase_n - 1)
                        )),
                        min(abs(aspect_word_index - (
                            polarity_word_index + polarity_phrase_n - 1)
                        ))
                    ) AS linear_distance,
                    sum(count) AS sum,
                    dataset,
                    aspect_match_algorithm,
                    sentiment_match_algorithm
                FROM
                    {self.phrase_aspect_table}
                GROUP BY
                    post_phrase_aspect_polarity.source,
                    post_phrase_aspect_polarity.post_id,
                    aspect_id,
                    aspect_word_index,
                    polarity_phrase_n, polarity_word_index,
                    dataset, aspect_match_algorithm,
                    sentiment_match_algorithm
            )
            SELECT *
            FROM linear_distance
        '''
