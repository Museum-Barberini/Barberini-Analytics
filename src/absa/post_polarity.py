import luigi
from luigi.format import UTF8

from csv_to_db import CsvToDb
from data_preparation import ConcatCsvs
from query_db import QueryDb
from .phrase_matching import FuzzyJoinPhrases, FuzzyMatchPhrases
from .phrase_polarity import PhrasePolaritiesToDb
from .post_ngrams import PostNgramsToDb


class PostPolaritiesToDb(CsvToDb):

    table = 'absa.post_polarity'

    def requires(self):
        return CollectPostPolarities(table=self.table)


class FuzzyJoinPostPolarities(FuzzyJoinPhrases):

    reference_table = 'absa.phrase_polarity'
    reference_phrase = 'phrase'
    reference_key = 'phrase, dataset'

    def requires(self):

        yield from super().requires()
        yield PhrasePolaritiesToDb()

    def known_post_ids_query(self):

        if not self.minimal_mode:
            return super().known_post_ids_query()

        return f'''
            {super().known_post_ids_query()}
            UNION (
                SELECT post_id
                FROM post
                WHERE post_date <= NOW() - INTERVAL '3 days'
            )
        '''

    def final_query(self):

        return f'''
            WITH post_phrase_polarity AS (
                SELECT
                    phrase_match.source, phrase_match.post_id,
                    phrase_match.word_index, phrase_match.n,
                    AVG(weight) AS polarity, STDDEV(weight),
                    dataset, '{self.algorithm.name}' AS match_algorithm
                FROM
                    /*<REPORT_PROGRESS>*/best_phrase_match
                        NATURAL JOIN phrase_match
                        JOIN {self.reference_table} AS reference
                            USING ({self.reference_key})
                GROUP BY
                    phrase_match.source, phrase_match.post_id,
                    phrase_match.word_index, phrase_match.n,
                    dataset
            )
            SELECT
                source, post_id,
                AVG(polarity) AS polarity, STDDEV(polarity),
                COUNT((word_index, n)) AS count,
                dataset, match_algorithm
            FROM post_phrase_polarity
            GROUP BY source, post_id, dataset, match_algorithm
        '''


class CollectPostPolarities(ConcatCsvs):

    def requires(self):

        yield CollectFuzzyPostPolarities(table=self.table)
        yield CollectIdentityPostPolarities(table=self.table)
        yield CollectInflectedPostPolarities(table=self.table)

    def output(self):

        return luigi.LocalTarget(
            f'{self.output_dir}/absa/post_polarity.csv',
            format=UTF8
        )


class CollectFuzzyPostPolarities(FuzzyMatchPhrases):

    join = FuzzyJoinPostPolarities

    def output(self):
        return luigi.LocalTarget(
            f'{self.output_dir}/absa/fuzzy_post_polarity.csv',
            format=UTF8
        )


class CollectPostPolaritiesAbstract(QueryDb):

    def requires(self):

        yield PostNgramsToDb()
        yield PhrasePolaritiesToDb()

    @property
    def query(self):

        return f'''
            WITH
                word_count AS (
                    SELECT source, post_id, count(word_index)
                    FROM absa.post_word
                    GROUP BY source, post_id
                ),
                post_phrase_polarity AS (
                    SELECT
                        source, post_id, word_index, post_ngram.n,
                        avg(weight) AS polarity, stddev(weight),
                        count(weight),
                        phrase_polarity.dataset,
                        '{self.algorithm}' AS match_algorithm
                    FROM
                        {self.query_source}
                    GROUP BY
                        source, post_id, word_index, post_ngram.n,
                        phrase_polarity.dataset
                )
            SELECT
                source, post_id,
                avg(polarity) AS polarity, stddev(polarity),
                CASE
                    WHEN word_count.count > 0
                    THEN post_phrase_polarity.count::real / word_count.count
                    ELSE NULL
                END AS subjectivity,
                count((word_index, n)),
                dataset, match_algorithm
            FROM post_phrase_polarity
                JOIN word_count USING (source, post_id)
            GROUP BY source, post_id, dataset, match_algorithm
        '''

    @property
    def query_source(self):

        return f'''
            absa.post_ngram AS post_ngram
        '''


class CollectIdentityPostPolarities(CollectPostPolaritiesAbstract):

    algorithm = 'identity'

    @property
    def query_source(self):

        return f'''
            {super().query_source}
            JOIN absa.phrase_polarity USING (phrase)
        '''


class CollectInflectedPostPolarities(CollectPostPolaritiesAbstract):

    algorithm = 'inflected'

    @property
    def query_source(self):

        return f'''
            {super().query_source}
            JOIN absa.inflection
                ON lower(inflection.inflected) = lower(post_ngram.phrase)
            JOIN absa.phrase_polarity
                ON  phrase_polarity.phrase = inflection.word
                AND phrase_polarity.dataset = inflection.dataset
        '''
