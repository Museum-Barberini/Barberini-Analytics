"""Provides a high-level organization of all data pipeline tasks."""

import luigi

from _posts import PostsToDb, PostPerformanceToDb
from absa import AspectBasedSentimentAnalysis
from diagnostics import Diagnostics
from extended_twitter_collection import TwitterExtendedDatasetToDB
from gomus import GomusToDb
from topic_modeling import TopicModeling
from visitor_prediction.predict import PredictionsToDb


class FillDb(luigi.WrapperTask):
    """The proto-root of the complete data pipeline."""

    def requires(self):

        yield FillDbDaily()
        yield FillDbHourly()


class FillDbDaily(luigi.WrapperTask):
    """Runs all tasks that are relevant for the daily pipeline execution."""

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
        # ---
        # NOTE: We are disabling Twitter temporarily, see !369.
        # TODO: Uncomment and fix this later!
        # yield TwitterExtendedDatasetToDB()
        # ---


class FillDbHourly(luigi.WrapperTask):
    """Runs all tasks that are relevant for the hourly pipeline execution."""

    def requires(self):

        # Public sources
        yield PostPerformanceToDb()
