import luigi

from posts import PostsToDb, PostPerformanceToDb
from gomus import GomusToDb
from absa import AspectBasedSentimentAnalysis
from topic_modeling import TopicModeling


class FillDb(luigi.WrapperTask):

    def requires(self):
        yield FillDbDaily()
        yield FillDbHourly()


class FillDbDaily(luigi.WrapperTask):

    def requires(self):
        # Public sources
        yield PostsToDb()

        # Internal sources
        yield GomusToDb()

        # Analysis tasks
        yield AspectBasedSentimentAnalysis()
        yield TopicModeling()


class FillDbHourly(luigi.WrapperTask):

    def requires(self):
        # Public sources
        yield PostPerformanceToDb()
