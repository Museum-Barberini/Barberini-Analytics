import luigi

from posts import PostsToDb, PostPerformanceToDb
from gomus.gomus import GomusToDb
from google_trends.gtrends_values import GtrendsValuesToDB
from absa.post_ngrams import PostNgramsToDb
from absa.post_aspects import PostAspectsToDb
from topic_modeling import TopicModeling


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
        yield PostAspectsToDb()
        yield PostNgramsToDb()
        yield TopicModeling()


class FillDBHourly(luigi.WrapperTask):

    def requires(self):
        # Public sources
        yield PostPerformanceToDb()
