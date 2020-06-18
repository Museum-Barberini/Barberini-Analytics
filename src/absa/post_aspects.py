import luigi
from luigi.format import UTF8

from csv_to_db import CsvToDb
from .phrase_matching import JoinPhrases, MergePhrases
from .target_aspects import TargetAspectsToDb


class PostAspectsToDb(CsvToDb):

    table = 'absa.post_aspect'

    def requires(self):
        return CollectPostAspects(table=self.table)


class JoinPostAspects(JoinPhrases):
    """
    Note regarding time consumption: 2020-06-15 each subinstance took less
    than 5 minutes when running the first time.
    """

    reference_table = 'absa.target_aspect_word'
    reference_phrase = 'word'
    reference_key = 'aspect_id'

    def requires(self):

        yield from super().requires()
        yield TargetAspectsToDb()


class CollectPostAspects(MergePhrases):
    """
    TODO: Drop topological ancestors ("Ausstellungen" if there is
    also "Ausstellungen/van Gogh")
    """

    join = JoinPostAspects

    def output(self):
        return luigi.LocalTarget(
            f'{self.output_dir}/absa/post_aspects.csv',
            format=UTF8
        )
