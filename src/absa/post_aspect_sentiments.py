import luigi
from luigi.format import UTF8

from csv_to_db import CsvToDb
from data_preparation import DataPreparationTask
from query_db import QueryDb
from .post_aspects import PostAspectsToDb
from .post_ngrams import PostNgramsToDb
from .post_sentiments import PostPhrasePolaritiesToDb
from .post_sentiments import PostSentimentsToDb


#TODO: Use
class QueryCacheToDb(DataPreparationTask):

    def output(self):

        return luigi.LocalTarget(
            f'{self.output_dir}/{self.task_id}.csv',
            format=UTF8
        )

    def run(self):

        self.db_connector.execute(
            f'TRUNCATE TABLE {self.table}',
            f'INSERT INTO {self.table} {self.query}'
        )

        count = self.db_connector.query(f'SELECT COUNT(*) FROM {self.table}')

        with self.output().open('w') as file:
            file.write(f"Cache of {count} rows")


class PostAspectSentimentsLinearDistanceToDb(CsvToDb):

    table = 'absa.post_aspect_sentiment_linear_distance'

    replace_content = True

    def requires(self):

        return CollectPostAspectSentimentsLinearDistance()


class CollectPostAspectSentimentsLinearDistance(QueryDb):

    phrase_aspect_table = 'absa.post_phrase_aspect_polarity_linear_distance'

    def requires(self):

        return PostPhraseAspectPolaritiesLinearDistanceToDb()

    @property
    def query(self):

        return f'''
            SELECT
                source, post_id,
                aspect_id,
                avg(linear_distance) AS linear_distance,
                avg(polarity) AS sentiment,
                count(DISTINCT aspect_word_index) AS aspect_count,
                count(DISTINCT polarity_word_index) AS polarity_count,
                dataset,
                aspect_match_algorithm,
                sentiment_match_algorithm
            FROM {self.phrase_aspect_table}
            GROUP BY
                source, post_id, aspect_id,
                dataset, aspect_match_algorithm,
                sentiment_match_algorithm
        '''


class PostPhraseAspectPolaritiesLinearDistanceToDb(CsvToDb):

    table = 'absa.post_phrase_aspect_polarity_linear_distance'

    replace_content = True

    def requires(self):

        return CollectPostPhraseAspectPolaritiesLinearDistance()


class CollectPostPhraseAspectPolaritiesLinearDistance(QueryDb):

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
                    avg(polarity) AS polarity,
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
            WHERE linear_distance <= 4
        '''


class PostPhraseAspectPolaritiesToDb(CsvToDb):

    table = 'absa.post_phrase_aspect_polarity'

    replace_content = True

    def requires(self):

        return CollectPostPhraseAspectPolarities()


class CollectPostPhraseAspectPolarities(QueryDb):

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
                avg(polarity) AS polarity,
                count(DISTINCT {self.aspect_table}.word_index) AS count,
                dataset,
                {self.aspect_table}.match_algorithm AS aspect_match_algorithm,
                post_phrase_polarity.match_algorithm AS sentiment_match_algorithm
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
