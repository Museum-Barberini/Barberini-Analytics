import luigi
import luigi.format

from csv_to_db import CsvToDb
from data_preparation import DataPreparationTask


# TODO: Introduce a column for the language?

class StopwordsToDb(CsvToDb):

    def requires(self):
        return LoadStopwords()

    table = 'absa.stopword'

    columns = [
        ('word', 'TEXT'),
    ]

    primary_key = 'word'


class LoadStopwords(DataPreparationTask):

    def output(self):
        return luigi.LocalTarget(
            "data/stopwords.csv",
            format=luigi.format.UTF8)
