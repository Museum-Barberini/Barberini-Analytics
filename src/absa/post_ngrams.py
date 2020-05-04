import luigi
import luigi.format
import pandas as pd

from csv_to_db import CsvToDb
from data_preparation_task import DataPreparationTask
from db_connector import db_connector
from .post_words import PostWordsToDB
from .stopwords import StopwordsToDb


class PostNgramsToDB(CsvToDb):

    limit = luigi.IntParameter(
        default=None,
        description="The maximum number posts to fetch. Optional. If None, "
                    "all posts will be fetched.")

    shuffle = luigi.BoolParameter(
        default=False,
        description="If True, all posts will be shuffled. For debugging and "
                    "exploration purposes. Might impact performance.")

    n_min = luigi.IntParameter(
        default=1,
        description="Minimum length of collected n-grams")

    n_max = luigi.IntParameter(
        default=4,
        description="Maximum length of collected n-grams")

    table = 'post_ngram'

    columns = [
        ('source', 'TEXT'),
        ('post_id', 'TEXT'),
        ('ngram', 'TEXT')
    ]

    primary_key = 'source', 'post_id'

    def requires(self):
        return CollectPostNgrams(
            limit=self.limit,
            shuffle=self.shuffle,
            n_min=self.n_min,
            n_max=self.n_max)


class CollectPostNgrams(DataPreparationTask):

    limit = luigi.IntParameter(
        default=None,
        description="The maximum number posts to fetch. Optional. If None, "
                    "all posts will be fetched.")

    shuffle = luigi.BoolParameter(
        default=False,
        description="If True, all posts will be shuffled. For debugging and "
                    "exploration purposes. Might impact performance.")

    n_min = luigi.IntParameter(
        default=1,
        description="Minimum length of collected n-grams")

    n_max = luigi.IntParameter(
        default=4,
        description="Maximum length of collected n-grams")

    word_table = 'post_word'
    stopword_table = 'stopword'

    def output(self):
        return luigi.LocalTarget(
            'output/post_ngrams.csv',
            format=luigi.format.UTF8)

    def run(self):
        if not db_connector.exists_table(self.word_table):
            yield PostWordsToDB()
        if not db_connector.exists_table(self.stopword_table):
            yield StopwordsToDb()

        ngrams = []
        for n in range(self.n_min, self.n_max + 1):
            query = self._build_query(n)
            print(query)
            result = db_connector.query(query)
            ngrams.extend(result)

        df = pd.DataFrame(ngrams, columns=['source', 'post_id', 'ngram'])
        with self.output().open('w') as output_stream:
            df.to_csv(output_stream, index=False, header=True)

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
            CREATE TEMPORARY VIEW relevant_{self.word_table} AS
                SELECT  *
                FROM    post_word
                WHERE   word NOT IN (
                    SELECT word
                    FROM stopword
                );
            -- TODO Discuss:
            -- Do we really want to drop ngrams such as "Museum in Potsdam"?
            SELECT  word{0}.source AS source,
                    word{0}.post_id AS post_id,
                    CONCAT_WS(' ', {mult_exp('word{i}.word')}) AS ngram
            FROM    {mult_exp(f'relevant_{self.word_table} AS word{{i}}')}
            WHERE   {mult_join('(word{i}.source, word{i}.post_id) ='
                               '(word{j}.source, word{j}.post_id)')}
            AND     {mult_join('word{i}.word_index + 1 = word{j}.word_index')}
        '''
