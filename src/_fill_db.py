import luigi

from _posts import PostsToDb, PostPerformanceToDb
from absa import AspectBasedSentimentAnalysis
from diagnostics import Diagnostics
from gomus import GomusToDb
from topic_modeling import TopicModeling
from visitor_prediction.predict import PredictionsToDb
from extended_twitter_collection.keyword_intervals import KeywordIntervalsToDB


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

        # Extended Tweet Gathering
        yield KeywordIntervalsToDB()


class FillDbHourly(luigi.WrapperTask):

    def requires(self):

        # Public sources
        yield PostPerformanceToDb()
