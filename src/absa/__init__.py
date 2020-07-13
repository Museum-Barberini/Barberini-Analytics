import luigi

from .phrase_polarity import PhrasePolaritiesToDb
from .post_aspects import PostAspectsToDb
from .post_aspect_sentiments import PostAspectSentimentsToDb
from .post_ngrams import PostNgramsToDb
from .post_sentiments import PostSentimentsToDb


class AspectBasedSentimentAnalysis(luigi.WrapperTask):

    def requires(self):

        # The actual ABSA process
        yield PostAspectSentimentsToDb()

        # Explicitely interesting tasks (to be referenced directly from PBI)
        yield PhrasePolaritiesToDb()
        yield PostAspectsToDb()
        yield PostNgramsToDb()
        yield PostSentimentsToDb()  # TODO: Do we need this at all???
                                    # Currently not referenced by
                                    # PostAspectSentimentsToDb.
                                    # Used by https://gitlab.hpi.de/
                                    # bp-barberini/thesis-absa/
                                    # absa-experiments/-/blob/master/
                                    # compare_subjectivity.ipynb, which again
                                    # is not referenced in https://
                                    # gitlab.hpi.de/bp-barberini/thesis-absa/
                                    # thesis-absa/-/blob/thesis/chapters/
                                    # 05_evaluation/evaluation.tex ... 🤯
