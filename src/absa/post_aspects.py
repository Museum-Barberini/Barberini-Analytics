import luigi
from luigi.format import UTF8

from csv_to_db import CsvToDb
from data_preparation import MergeCsv
from query_db import QueryDb
from .post_words import PostWordsToDb
from .target_aspects import TargetAspectsToDb


class PostAspectsToDb(CsvToDb):

    table = 'absa.post_aspect'

    def requires(self):
        return CollectPostAspects()


class CollectPostAspects(MergeCsv):
    """
    TODO: Drop topological ancestors ("Ausstellungen" if there is
    also "Ausstellungen/van Gogh")
    TODO: Check ngrams ("van Gogh")
    """

    def requires(self):
        yield CollectPostAspectsTrigram()
        yield CollectPostAspectsLevenshtein()

    def output(self):
        return luigi.LocalTarget(
            f'{self.output_dir}/absa/post_aspects.csv',
            format=UTF8
        )


class CollectPostAspectsAlgorithm(QueryDb):

    def _requires(self):
        return luigi.task.flatten([
            PostWordsToDb(),
            TargetAspectsToDb(),
            super()._requires()
        ])

    match_name = 'match_value'

    @property
    def query(self):
        return f'''
            WITH best_matches AS (
                SELECT
                    source, post_id, word_index,
                    {self.aggregate_query} {self.match_name}
                FROM
                    absa.post_word
                        NATURAL JOIN post,
                    absa.target_aspect_word,
                    {self.value_query} {self.match_name}
                WHERE
                    {self.filter_query}
                GROUP BY
                    source, post_id, word_index
            )
            SELECT DISTINCT
                source, post_id, word_index, aspect_id,
                MIN(target_aspect_word.word) AS aspect_word,  -- just any
                '{self.algorithm}' AS algorithm
            FROM
                best_matches
                    NATURAL JOIN absa.post_word
                    NATURAL JOIN post,
                absa.target_aspect_word,
                {self.value_query} {self.match_name}_2
            WHERE {self.match_name} = {self.match_name}_2
            GROUP BY
                source, post_id, word_index, aspect_id
        '''


class CollectPostAspectsTrigram(CollectPostAspectsAlgorithm):

    algorithm = 'trigram'

    threshold = 0.65

    @property
    def aggregate_query(self):
        return f'''
            MAX({self.match_name})
        '''

    @property
    def value_query(self):
        return '''
            similarity(post_word.word, target_aspect_word.word)
        '''

    @property
    def filter_query(self):
        return f'''
            {self.match_name} >= {self.threshold}
        '''


class CollectPostAspectsLevenshtein(CollectPostAspectsAlgorithm):

    algorithm = 'levenshtein'

    threshold = 2 / 9

    @property
    def aggregate_query(self):
        return f'''
            MIN({self.match_name})
        '''

    @property
    def value_query(self):
        return '''
            levenshtein(post_word.word, target_aspect_word.word)
        '''

    @property
    def filter_query(self):
        return f'''
            {self.match_name}::real / length(post_word.word)
            <= {self.threshold}
        '''
