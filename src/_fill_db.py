import luigi

from _posts import PostsToDb, PostPerformanceToDb
from absa import AspectBasedSentimentAnalysis
from diagnostics import Diagnostics
from gomus import GomusToDb
from topic_modeling import TopicModeling
from visitor_prediction import PredictionsToDb


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
        yield PredictionsToDb()

        # Diagnostics
        yield Diagnostics()


class FillDbHourly(luigi.WrapperTask):

    def requires(self):

        # Public sources
        yield PostPerformanceToDb()
