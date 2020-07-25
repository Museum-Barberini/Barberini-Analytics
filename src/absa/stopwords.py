import luigi
import luigi.format

from _utils import CsvToDb, DataPreparationTask


# TODO: Introduce a column for the language?

class StopwordsToDb(CsvToDb):

    def requires(self):
        return LoadStopwords()

    table = 'absa.stopword'


class LoadStopwords(DataPreparationTask):

    def output(self):
        return luigi.LocalTarget(
            "data/stopwords.csv",
            format=luigi.format.UTF8)
