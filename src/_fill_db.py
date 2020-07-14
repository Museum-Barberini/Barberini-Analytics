import luigi

from _posts import PostsToDb, PostPerformanceToDb
from gomus.gomus import GomusToDb
from google_trends.gtrends_values import GtrendsValuesToDb
from absa.post_aspects import PostAspectsToDb
from absa.post_ngrams import PostNgramsToDb
from absa.phrase_polarity import PhrasePolaritiesToDb
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
        yield PostAspectsToDb()
        yield PostNgramsToDb()
        yield PhrasePolaritiesToDb()

        yield TopicModeling()


class FillDbHourly(luigi.WrapperTask):

    def requires(self):

        # Public sources
        yield PostPerformanceToDb()
