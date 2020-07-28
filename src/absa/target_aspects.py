from ast import literal_eval

import json
import luigi
import pandas as pd
from luigi.format import UTF8

from _utils import CsvToDb, DataPreparationTask, JsoncToJson

# TODO: Respect exhibitions table here?


class TargetAspectsToDb(luigi.WrapperTask):

    def requires(self):
        yield TargetAspectLabelsToDb()
        yield TargetAspectWordsToDb()


class TargetAspectLabelsToDb(CsvToDb):

    table = 'absa.target_aspect'

    def requires(self):
        return ConvertTargetAspectLabels()


class TargetAspectWordsToDb(CsvToDb):

    table = 'absa.target_aspect_word'

    def requires(self):
        return ConvertTargetAspectWords()


class ConvertTargetAspectWords(DataPreparationTask):

    label_table = 'absa.target_aspect'

    def _requires(self):
        return luigi.task.flatten([
            TargetAspectLabelsToDb(),
            super()._requires()
        ])

    def requires(self):
        return ConvertTargetAspects()

    def output(self):
        return luigi.LocalTarget(
            f'{self.output_dir}/absa/target_aspect_words.csv',
            format=UTF8
        )

    def run(self):
        with self.input().open('r') as csv_stream:
            df = pd.read_csv(csv_stream, converters={'aspect': literal_eval})

        aspect_ids = self.fetch_aspect_ids(self.db_connector, df['aspect'])
        df['aspect_id'] = df['aspect'].apply(aspect_ids.get)
        df = df[['aspect_id', 'word']]

        with self.output().open('w') as csv_stream:
            df.to_csv(csv_stream, index=False, header=True)

    @classmethod
    def fetch_aspect_ids(cls, db_connector, aspect_names):

        def fetch_aspect_id(aspect):
            response = db_connector.query(
                f'''
                    SELECT aspect_id
                    FROM {cls.label_table}
                    WHERE aspect = %s
                ''',
                list(aspect),
                only_first=True
            )
            return response[0]

        return {
            aspect_name: fetch_aspect_id(aspect_name)
            for aspect_name
            in set(aspect_names)
        }


class ConvertTargetAspectLabels(DataPreparationTask):

    def requires(self):
        return ConvertTargetAspects()

    def output(self):
        return luigi.LocalTarget(
            f'{self.output_dir}/absa/target_aspect_labels.csv',
            format=UTF8
        )

    def run(self):
        with self.input().open('r') as csv_stream:
            df = pd.read_csv(csv_stream, converters={'aspect': literal_eval})
        df = df[['aspect']].drop_duplicates()
        with self.output().open('w') as csv_stream:
            df.to_csv(csv_stream, header=True, index=False)


class ConvertTargetAspects(DataPreparationTask):

    def requires(self):
        return LoadTargetAspects()

    def output(self):
        return luigi.LocalTarget(
            f'{self.output_dir}/absa/target_aspects.csv',
            format=UTF8
        )

    def run(self):
        with self.input().open('r') as json_stream:
            aspects = json.load(json_stream)

        flattened = dict(self.flatten(aspects))
        expanded = self.expand(flattened)
        df = pd.DataFrame(expanded, columns=['aspect', 'word'])

        with self.output().open('w') as csv_stream:
            df.to_csv(csv_stream, header=True, index=False)

    def flatten(self, aspects):
        for aspect, words in aspects.items():
            if hasattr(words, 'items'):
                for subaspect, words in self.flatten(words):
                    yield (aspect, *subaspect), words
            _aspect = aspect
            if aspect[0] == "'" == aspect[-1]:
                _aspect = aspect[1:-1]
            else:
                words = [*words, aspect]
            yield (_aspect,), words
            continue

    def expand(self, flattened):
        for aspect, words in flattened.items():
            for word in words:
                yield aspect, word


class LoadTargetAspects(JsoncToJson):

    file = luigi.Parameter(default='data/target_aspects.jsonc')

    def input(self):
        return luigi.LocalTarget(self.file, format=UTF8)

    def output(self):
        return luigi.LocalTarget(
            f'{self.output_dir}/absa/target_aspects.json', format=UTF8
        )
