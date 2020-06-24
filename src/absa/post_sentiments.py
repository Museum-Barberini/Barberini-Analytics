import luigi
from luigi.format import UTF8

from csv_to_db import CsvToDb
from data_preparation import ConcatCsvs
from query_db import QueryDb
from .phrase_matching import FuzzyJoinPhrases, FuzzyMatchPhrases
from .phrase_polarity import PhrasePolaritiesToDb
from .post_ngrams import PostNgramsToDb


class PostSentimentsToDb(luigi.WrapperTask):

    def requires(self):

        yield PostSentimentsDocumentToDb()
        yield PostSentimentsSentenceToDb()


class PostSentimentsDocumentToDb(CsvToDb):

    table = 'absa.post_sentiment_document'

    def requires(self):

        return CollectPostSentimentsDocument(table=self.table)


class PostSentimentsSentenceToDb(CsvToDb):

    table = 'absa.post_sentiment_sentence'

    def requires(self):

        return CollectPostSentimentsSentence(table=self.table)


class FuzzyJoinPostSentiments(FuzzyJoinPhrases):

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
                    best_phrase_match
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


class CollectFuzzyPostSentiments(FuzzyMatchPhrases):

    join = FuzzyJoinPostSentiments

    def output(self):
        return luigi.LocalTarget(
            f'{self.output_dir}/absa/fuzzy_post_sentiment.csv',
            format=UTF8
        )


class CollectPostSentimentsAbstract(QueryDb):

    # TODO: Ideally, we could read this information from requires
    post_phrase_polarity_table = 'absa.post_phrase_polarity'

    def requires(self):

        yield PostPhrasePolaritiesToDb()

    @property
    def query(self):

        return f'''
            WITH
                word_count AS (
                    SELECT source, post_id, count(word_index)
                    FROM absa.post_word
                    GROUP BY source, post_id
                )
            SELECT
                {self.query_select()},
                avg(polarity) AS sentiment, stddev(polarity),
                CASE
                    WHEN word_count.count > 0
                    THEN count(DISTINCT post_phrase_polarity.word_index)::real
                        / word_count.count
                    ELSE NULL
                END AS subjectivity,
                count((word_index, n)),
                dataset, match_algorithm
            FROM {self.query_from()}
                JOIN word_count USING (source, post_id)
            GROUP BY
                {self.query_select()},
                dataset, match_algorithm,
                word_count.count
        '''

    def query_from(self):

        return self.post_phrase_polarity_table

    def query_select(self):

        return 'source, post_id'


class CollectPostSentimentsDocument(CollectPostSentimentsAbstract):

    model_name = 'same_document'


class CollectPostSentimentsSentence(CollectPostSentimentsAbstract):

    model_name = 'same_sentence'

    def query_from(self):

        return f'''{super().query_from()}
            JOIN absa.post_ngram USING (source, post_id, n, word_index)
        '''

    def query_select(self):

        return f'''
            {super().query_select()},
            sentence_index
        '''


class MatchPostSentiments(luigi.Task):

    def requires(self):

        yield PostNgramsToDb()
        yield PhrasePolaritiesToDb()

    run = None  # not a real task, just a strategy object

    def query_source(self):

        return '''
            absa.post_ngram AS post_ngram
        '''


class MatchIdentityPostSentiments(MatchPostSentiments):

    name = 'identity'

    def query_source(self):

        return f'''
            {super().query_source()}
            JOIN absa.phrase_polarity USING (phrase)
        '''


class MatchInflectedPostSentiments(MatchPostSentiments):

    name = 'inflected'

    def query_source(self):

        return f'''
            {super().query_source()}
            JOIN absa.inflection
                ON lower(inflection.inflected) = lower(post_ngram.phrase)
            JOIN absa.phrase_polarity
                ON  phrase_polarity.phrase = inflection.word
                AND phrase_polarity.dataset = inflection.dataset
        '''


# TODO: Much too slow. Make it faster! Disabled for now.
# yield CollectFuzzyPostSentiments(table=self.table)


# Refactoring TODO: Align schema of post_phrase_polarity and post_aspect?

class PostPhrasePolaritiesToDb(CsvToDb):

    table = 'absa.post_phrase_polarity'

    def requires(self):

        return CollectAllPostPhrasePolarities(table=self.table)


class CollectAllPostPhrasePolarities(ConcatCsvs):

    match_algorithms = [
        MatchIdentityPostSentiments,
        MatchInflectedPostSentiments
    ]

    def output(self):

        return luigi.LocalTarget(
            f'{self.output_dir}/absa/post_phrase_polarities.csv',
            format=UTF8
        )

    def requires(self):

        for algorithm in self.match_algorithms:
            yield CollectPostPhrasePolarities(
                table=self.table,
                match_algorithm=algorithm()
            )


class CollectPostPhrasePolarities(QueryDb):

    match_algorithm = luigi.TaskParameter()

    @property
    def kwargs(self):

        return {
            'match_algorithm': self.match_algorithm.name
        }

    def requires(self):

        yield from self.match_algorithm._requires()

    @property
    def query(self):

        return f'''
            SELECT
                source, post_id,
                post_ngram.n, word_index,
                avg(weight) AS polarity, stddev(weight),
                phrase_polarity.dataset,
                %(match_algorithm)s AS match_algorithm
            FROM
                {self.match_algorithm.query_source()}
            GROUP BY
                source, post_id, post_ngram.n, word_index,
                phrase_polarity.dataset
        '''
