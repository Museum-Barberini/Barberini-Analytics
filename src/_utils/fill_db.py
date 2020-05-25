import luigi

from posts import PostsToDb, PostPerformanceToDb
from gomus.gomus import GomusToDb
from google_trends.gtrends_values import GtrendsValuesToDB
# from absa.post_ngrams import PostNgramsToDb


class FillDB(luigi.WrapperTask):

    def requires(self):
        yield FillDBDaily()
        yield FillDBHourly()


class FillDBDaily(luigi.WrapperTask):

    def requires(self):
        # Public sources
        yield GtrendsValuesToDB()
        yield PostsToDb()

        # Internal sources
        yield GomusToDb()

        # Analysis tasks
        # yield PostNgramsToDb()


class FillDBHourly(luigi.WrapperTask):

    def requires(self):
        # Public sources
        yield PostPerformanceToDb()
