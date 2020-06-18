from typing import Callable

import luigi

from .post_ngrams import PostNgramsToDb
from data_preparation import ConcatCsvs
from query_db import QueryDb


class FuzzyJoinPhrases(QueryDb):

    def requires(self):

        yield PostNgramsToDb()

    algorithm = luigi.TaskParameter(description="Match algorithm to apply")

    primary_table = 'absa.post_ngram'
    primary_phrase = 'phrase'

    @property
    def query(self):

        return f'''
            CREATE TEMPORARY TABLE phrase_match AS (
                WITH
                    known_post_id AS ({self.known_post_ids_query()}),
                    phrase AS (
                        SELECT
                            *
                        FROM
                            /*<REPORT_PROGRESS>*/{self.primary_table}
                        WHERE
                            post_id NOT IN (SELECT * FROM known_post_id)
                            AND {self.algorithm.pre_filter_query(
                                self.primary_phrase
                            )}
                    )
                SELECT
                    phrase.source, phrase.post_id, phrase.word_index, phrase.n,
                    reference.{self.reference_key},
                    {self.algorithm.value_query(
                            f'phrase.{self.primary_phrase}',
                            f'reference.{self.reference_phrase}'
                        )} AS match_value
                FROM
                    phrase,
                    {self.reference_table} AS reference
            );

            CREATE TEMPORARY TABLE best_phrase_match (
                source TEXT,
                post_id TEXT,
                word_index INTEGER,
                n INTEGER,
                match_value REAL,
                PRIMARY KEY (source, post_id, word_index, n)
            );
            INSERT INTO best_phrase_match
            SELECT
                source, post_id, word_index, n,
                {self.algorithm.aggregate_query('match_value')} AS match_value
            FROM
                /*<REPORT_PROGRESS>*/phrase_match
                    NATURAL JOIN {self.primary_table} AS phrase
            WHERE
                {self.algorithm.post_filter_query(
                    'match_value',
                    f'phrase.{self.primary_phrase}'
                )}
            GROUP BY
                source, post_id, word_index, n;

            {self.final_query()}
        '''

    def known_post_ids_query(self):

        return f'SELECT post_id FROM {self.table}'

    def final_query(self):

        return f'''
            SELECT DISTINCT
                source, post_id, word_index, reference.{self.reference_key},
                -- just any word
                MIN(reference.{self.reference_phrase}) AS reference_word,
                '{self.algorithm.name}' AS match_algorithm
            FROM
                /*<REPORT_PROGRESS>*/best_phrase_match
                    NATURAL JOIN phrase_match
                    JOIN {self.reference_table} AS reference
                        USING ({self.reference_key})
            GROUP BY
                source, post_id, word_index, reference.{self.reference_key}
        '''


class FuzzyMatchPhrases(ConcatCsvs):

    join: Callable[[], FuzzyJoinPhrases]

    def requires(self):

        for algorithm in self.algorithms():
            yield self.join(table=self.table, algorithm=algorithm)

    def algorithms(self):

        yield FuzzyMatchIdentity()
        yield FuzzyMatchLevenshtein()
        yield FuzzyMatchTrigram()


class FuzzyMatch(luigi.Task):

    def pre_filter_query(self, post_word):
        """
        Query to filter post words before applying the algorithm.
        """

        return 'TRUE'

    run = None  # Not a task to be executed, just a strategy object


class FuzzyMatchIdentity(FuzzyMatch):

    name = 'identity'

    def aggregate_query(self, match):
        return f'''
            MAX({match})
        '''

    def value_query(self, post_word, target_word):
        return f'''
            (lower({post_word}) = lower({target_word}))::int
        '''

    def post_filter_query(self, match, post_word):
        return f'''
            {match}::bool
        '''


class FuzzyMatchLevenshtein(FuzzyMatch):

    name = 'levenshtein'

    threshold = 0.19

    def aggregate_query(self, match):
        return f'''
            MIN({match})
        '''

    def value_query(self, post_word, target_word):
        return f'''
            CAST(levenshtein(
                LOWER({post_word}),
                LOWER({target_word}
            )) AS real)
            / length({post_word})
        '''

    def pre_filter_query(self, post_word):

        return f'''
            LENGTH({post_word}) <= 255
        '''

    def post_filter_query(self, match, post_word):
        return f'''
            {match} <= {self.threshold}
        '''


class FuzzyMatchTrigram(FuzzyMatch):

    name = 'trigram'

    threshold = 0.65

    def aggregate_query(self, match):
        return f'''
            MAX({match})
        '''

    def value_query(self, post_word, target_word):
        return f'''
            similarity({post_word}, {target_word})
        '''

    def post_filter_query(self, match, post_word):
        return f'''
            {match} >= {self.threshold}
        '''
