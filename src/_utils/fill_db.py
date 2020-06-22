import luigi

from posts import PostsToDb, PostPerformanceToDb
from gomus.gomus import GomusToDb
from google_trends.gtrends_values import GtrendsValuesToDb
from absa.post_ngrams import PostNgramsToDb
from topic_modeling import TopicModeling


class FillDb(luigi.WrapperTask):

    def requires(self):
        yield FillDbDaily()
        yield FillDbHourly()


class FillDbDaily(luigi.WrapperTask):

    def requires(self):
        # Public sources
        yield GtrendsValuesToDb()
        yield PostsToDb()

        # Internal sources
        yield GomusToDb()

        # Analysis tasks
        yield PostNgramsToDb()
        yield TopicModeling()


class FillDbHourly(luigi.WrapperTask):

    def requires(self):
        # Public sources
        yield PostPerformanceToDb()
