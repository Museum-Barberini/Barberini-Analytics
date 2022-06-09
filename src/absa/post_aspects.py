"""Provides tasks to match all posts to predefined target aspects."""

import luigi
from luigi.format import UTF8

from _utils import CsvToDb, ConcatCsvs, QueryDb
from .post_ngrams import PostNgramsToDb
from .target_aspects import TargetAspectsToDb


class PostAspectsToDb(CsvToDb):

    table = 'absa.post_aspect'

    def requires(self):
        return CollectPostAspects(table=self.table)


class CollectPostAspects(ConcatCsvs):
    """
    Find all known aspects in posts, applying different matching algorithms.

    TODO: Drop topological ancestors ("Ausstellungen" if there is
    also "Ausstellungen/van Gogh")
    TODO: Add column "n" to table post_aspect
    """

    def requires(self):
        yield CollectPostAspectsEquality(table=self.table)
        yield CollectPostAspectsTrigram(table=self.table)
        yield CollectPostAspectsLevenshtein(table=self.table)

    def output(self):
        return luigi.LocalTarget(
            f'{self.output_dir}/absa/post_aspects.csv',
            format=UTF8
        )


class CollectPostAspectsAlgorithm(QueryDb):
    """
    The abstract superclass for all post-search query algorithms.

    Note regarding time consumption: 2020-06-15 each subinstance took less
    than 5 minutes when running the first time.
    """

    def _requires(self):
        return luigi.task.flatten([
            PostNgramsToDb(),
            TargetAspectsToDb(),
            super()._requires()
        ])

    match_name = 'match_value'

    @property
    def query(self):
        return f'''
            CREATE TEMPORARY TABLE aspect_match AS (
                WITH
                    new_post_id AS (
                        SELECT post_id
                        FROM post
                        WHERE post_date > ANY(
                            SELECT max(post_date)
                            FROM {self.table}
                            NATURAL JOIN post
                        ) IS NOT FALSE
                    ),
                    post_ngram AS (
                        SELECT
                            *
                        FROM
                            absa.post_ngram
                                NATURAL JOIN new_post_id
                        WHERE
                            {self.pre_filter_query('phrase')}
                    )
                SELECT
                    source, post_id, word_index, aspect_id,
                    {self.value_query('phrase', 'target_aspect_word.word')}
                        AS {self.match_name}
                FROM
                    post_ngram,
                    absa.target_aspect_word
            );

            CREATE TEMPORARY TABLE best_aspect_match (
                source TEXT,
                post_id TEXT,
                word_index INTEGER,
                {self.match_name} REAL,
                PRIMARY KEY (source, post_id, word_index)
            );
            INSERT INTO best_aspect_match
            SELECT
                source, post_id, word_index,
                {self.aggregate_query} AS {self.match_name}
            FROM
                aspect_match
                    NATURAL JOIN absa.post_ngram
            WHERE
                {self.post_filter_query('phrase')}
            GROUP BY
                source, post_id, word_index;

            SELECT DISTINCT
                source, post_id, word_index, aspect_id,
                MIN(target_aspect_word.word) AS aspect_word,  -- just any word
                '{self.algorithm}' AS match_algorithm
            FROM
                best_aspect_match
                    NATURAL JOIN aspect_match
                    JOIN absa.target_aspect_word USING (aspect_id)
            GROUP BY
                source, post_id, word_index, aspect_id
        '''  # nosec B608

    def pre_filter_query(self, post_word_name):
        """Query to filter post words before applying the algorithm."""
        return 'TRUE'


class CollectPostAspectsEquality(CollectPostAspectsAlgorithm):

    algorithm = 'equality'

    @property
    def aggregate_query(self):
        return f'''
            MAX({self.match_name})
        '''

    def value_query(self, post_word_name, target_word_name):
        return f'''
            (lower({post_word_name}) = lower({target_word_name}))::int
        '''

    def post_filter_query(self, post_word_name):
        return f'''
            {self.match_name}::bool
        '''


class CollectPostAspectsTrigram(CollectPostAspectsAlgorithm):

    algorithm = 'trigram'

    threshold = 0.65

    @property
    def aggregate_query(self):
        return f'''
            MAX({self.match_name})
        '''

    def value_query(self, post_word_name, target_word_name):
        return f'''
            similarity({post_word_name}, {target_word_name})
        '''

    def post_filter_query(self, post_word_name):
        return f'''
            {self.match_name} >= {self.threshold}
        '''


class CollectPostAspectsLevenshtein(CollectPostAspectsAlgorithm):

    algorithm = 'levenshtein'

    threshold = 0.19

    @property
    def aggregate_query(self):
        return f'''
            MIN({self.match_name})
        '''

    def value_query(self, post_word_name, target_word_name):
        return f'''
            CAST(levenshtein(
                LOWER({post_word_name}),
                LOWER({target_word_name}
            )) AS real)
            / length({post_word_name})
        '''

    def pre_filter_query(self, post_word_name):

        return f'''
            LENGTH({post_word_name}) <= 255
        '''

    def post_filter_query(self, post_word_name):
        return f'''
            {self.match_name} <= {self.threshold}
        '''
