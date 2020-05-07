import luigi

from apple_appstore import AppstoreReviewsToDB
from facebook import FbPostsToDB, FbPostPerformanceToDB
from google_maps import GoogleMapsReviewsToDB
from gplay.gplay_reviews import GooglePlaystoreReviewsToDB
from instagram import IgToDBWrapper, IgPostPerformanceToDB
from twitter import TweetsToDB, TweetPerformanceToDB, TweetAuthorsToDB


class PostsToDb(luigi.WrapperTask):

    def requires(self):
        yield AppstoreReviewsToDB()
        yield FbPostsToDB()
        yield GoogleMapsReviewsToDB()
        yield GooglePlaystoreReviewsToDB()
        yield IgToDBWrapper()
        yield TweetAuthorsToDB()
        yield TweetsToDB()


class PostPerformanceToDb(luigi.WrapperTask):

    def requires(self):
        yield FbPostPerformanceToDB()
        yield IgPostPerformanceToDB()
        yield TweetPerformanceToDB()
