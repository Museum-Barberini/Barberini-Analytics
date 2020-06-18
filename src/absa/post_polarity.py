import luigi
from luigi.format import UTF8

from csv_to_db import CsvToDb
from .phrase_matching import JoinPhrases, MergePhrases
from .phrase_polarity import PhrasePolaritiesToDb


class PostPolaritiesToDb(CsvToDb):

    table = 'absa.post_polarity'

    def requires(self):
        return CollectPostPolarities(table=self.table)


class JoinPostPolarities(JoinPhrases):

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
            WITH phrase_polarity AS (
                SELECT
                    source, post_id, word_index, n,
                    AVG(weight) AS polarity, STDDEV(weight),
                    dataset, '{self.algorithm.name}' AS match_algorithm
                FROM
                    /*<REPORT_PROGRESS>*/best_phrase_match
                        NATURAL JOIN phrase_match
                        JOIN {self.reference_table} AS reference
                            USING ({self.reference_key})
                GROUP BY
                    source, post_id, word_index, n, dataset
            )
            SELECT
                source, post_id,
                AVG(polarity) AS polarity, STDDEV(polarity),
                COUNT((word_index, n)) AS count,
                dataset, match_algorithm
            FROM phrase_polarity
            GROUP BY source, post_id, dataset, match_algorithm
        '''


class CollectPostPolarities(MergePhrases):

    join = JoinPostPolarities

    def output(self):
        return luigi.LocalTarget(
            f'{self.output_dir}/absa/post_polarities.csv',
            format=UTF8
        )
