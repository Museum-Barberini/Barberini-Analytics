from ast import literal_eval

import json
import luigi
import pandas as pd
from luigi.format import UTF8

from csv_to_db import CsvToDb
from data_preparation_task import DataPreparationTask
from json_converters import JsoncToJson
from query_db import QueryDb


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

        aspect_ids = {}

        def register_id_for_aspect(aspect):
            response_file = yield QueryDb(query=f'''
                SELECT aspect_id
                FROM {self.label_table}
                WHERE aspect = '{{{','.join([
                    f'"{part}"' for part in aspect
                ])}}}'::text[]
            ''')
            with response_file.open('r') as response_stream:
                response = pd.read_csv(response_stream)
            aspect_ids[aspect] = response.iloc[0][0]

        for aspect in df['aspect'].drop_duplicates():
            yield from register_id_for_aspect(aspect)
        df['aspect_id'] = df['aspect'].apply(aspect_ids.get)
        df = df[['aspect_id', 'word']]

        with self.output().open('w') as csv_stream:
            df.to_csv(csv_stream, index=False, header=True)


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
        df['aspect'] = df['aspect'].apply(
            lambda aspect:
                f'''{{{','.join([f'{part}' for part in aspect])}}}''')
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
