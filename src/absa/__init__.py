import luigi

from .phrase_polarity import PhrasePolaritiesToDb
from .post_aspects import PostAspectsToDb
from .post_ngrams import PostNgramsToDb
from .post_sentiments import PostSentimentsToDb


class AspectBasedSentimentAnalysis(luigi.WrapperTask):

    def requires(self):

        yield PhrasePolaritiesToDb()
        yield PostAspectsToDb()
        yield PostNgramsToDb()
        yield PostSentimentsToDb()
