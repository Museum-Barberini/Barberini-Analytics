import luigi

from apple_appstore import AppstoreReviewsToDB
from facebook import FbPostsToDB, FbPostCommentsToDB, FbPostPerformanceToDB
from google_maps import GoogleMapsReviewsToDB
from gplay.gplay_reviews import GooglePlaystoreReviewsToDB
from instagram import IgToDBWrapper, IgPostPerformanceToDB
# TODO: disabled while twitterscraper is broken
# from twitter import TweetsToDB, TweetPerformanceToDB, TweetAuthorsToDB


class PostsToDb(luigi.WrapperTask):

    fetch_performance = luigi.BoolParameter(
        description="If enabled, performance data will be also fetched now.",
        default=False
    )

    def requires(self):
        #yield AppstoreReviewsToDB()
        yield FbPostsToDB()
        yield FbPostCommentsToDB()
        yield GoogleMapsReviewsToDB()
        yield GooglePlaystoreReviewsToDB()
        yield IgToDBWrapper()
        # TODO: disabled while twitterscraper is broken
        # yield TweetAuthorsToDB()
        # yield TweetsToDB()

        if self.fetch_performance:
            yield PostPerformanceToDb()


class PostPerformanceToDb(luigi.WrapperTask):

    def requires(self):
        yield FbPostPerformanceToDB()
        yield IgPostPerformanceToDB()
        # TODO: disabled while twitterscraper is broken
        # yield TweetPerformanceToDB()
