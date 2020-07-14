import logging
import luigi
import luigi.format
import pandas as pd

from _utils import CsvToDb, DataPreparationTask, QueryDb, logger
from .post_words import PostWordsToDb
from .stopwords import StopwordsToDb


class PostNgramsToDb(CsvToDb):

    table = 'absa.post_ngram'

    n_min = luigi.IntParameter(
        default=1,
        description="Minimum length of n-grams to collect")

    n_max = luigi.IntParameter(
        default=4,
        description="Maximum length of n-grams to collect")

    limit = luigi.IntParameter(
        default=-1,
        description="The maximum number posts to fetch. Optional. If -1, "
                    "all posts will be fetched.")

    shuffle = luigi.BoolParameter(
        default=False,
        description="If True, all posts will be shuffled. For debugging and "
                    "exploration purposes. Might impact performance.")

    standalone = luigi.BoolParameter(
        default=False,
        description="If False, the post database will be updated before the"
                    "posts will be collected. If True, only posts already in"
                    "the database will be respected.")

    def requires(self):
        return CollectPostNgrams(
            n_min=self.n_min,
            n_max=self.n_max,
            table=self.table,
            limit=self.limit,
            shuffle=self.shuffle,
            standalone=self.standalone)


class CollectPostNgrams(DataPreparationTask):

    n_min = luigi.IntParameter(
        default=1,
        description="Minimum length of collected n-grams")

    n_max = luigi.IntParameter(
        default=4,
        description="Maximum length of collected n-grams")

    limit = luigi.IntParameter(
        default=-1,
        description="The maximum number posts to fetch. Optional. If -1, "
                    "all posts will be fetched.")

    shuffle = luigi.BoolParameter(
        default=False,
        description="If True, all posts will be shuffled. For debugging and "
                    "exploration purposes. Might impact performance.")

    standalone = luigi.BoolParameter(
        default=False,
        description="If False, the post database will be updated before the"
                    "posts will be collected. If True, only posts already in"
                    "the database will be respected.")

    word_table = 'absa.post_word'
    stopword_table = 'absa.stopword'

    def _requires(self):
        return luigi.task.flatten([
            PostWordsToDb(
                limit=self.limit,
                shuffle=self.shuffle,
                standalone=self.standalone),
            StopwordsToDb(),
            super()._requires()
        ])

    def output(self):
        return luigi.LocalTarget(
            f'{self.output_dir}/absa/post_ngrams.csv',
            format=luigi.format.UTF8)

    def run(self):
        dfs = []
        for n in range(self.n_min, self.n_max + 1):
            logger.info(f"Collecting n={n}-grams ...")
            ngram_file = yield QueryDb(query=self._build_query(n))
            with ngram_file.open('r') as ngram_stream:
                dfs.append(pd.read_csv(ngram_stream))

        ngrams = pd.concat(dfs)

        with self.output().open('w') as output_stream:
            ngrams.to_csv(output_stream, index=False, header=True)

    def _build_query(self, n):

        def mult_exp(template):
            return ', '.join([
                template.format(i=i)
                for i in range(n)])

        def mult_join(template):
            if n < 2:
                return 'TRUE'
            return ' AND '.join([
                template.format(i=i, j=i + 1)
                for i in range(n - 1)])

        return f'''
            WITH
                word_relevant AS (
                    SELECT  *
                    FROM    {self.word_table}
                    WHERE   word NOT IN (
                        SELECT word
                        FROM {self.stopword_table}
                    )
                ),
                /* TODO Discuss: Do we really want to drop ngrams such as
                "van Gogh" that include stopwords ("in") at any place? */
                new_post_id AS (
                    SELECT post_id
                    FROM post
                    WHERE post_date > ANY(
                        SELECT max(post_date)
                        FROM {self.table}
                        NATURAL JOIN post
                    ) IS NOT FALSE
                )
            SELECT  word{0}.source AS source,
                    word{0}.post_id AS post_id,
                    {n} AS n,
                    word{0}.word_index AS word_index,
                    CONCAT_WS(' ', {mult_exp('word{i}.word')}) AS ngram
            FROM    {mult_exp('word_relevant AS word{i}')}
            WHERE   {mult_join('(word{i}.source, word{i}.post_id) ='
                               '(word{j}.source, word{j}.post_id)')}
            AND     {mult_join('word{i}.word_index + 1 = word{j}.word_index')}
            AND     word{0}.post_id IN (SELECT * FROM new_post_id)
        '''
