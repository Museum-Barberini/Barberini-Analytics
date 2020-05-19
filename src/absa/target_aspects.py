import json
import luigi
import pandas as pd
from luigi.format import UTF8

from data_preparation_task import DataPreparationTask
from json_converters import JsoncToJson


class LoadAspects(JsoncToJson):

    file = luigi.Parameter(default='data/target_aspects.jsonc')

    def input(self):
        return luigi.LocalTarget(self.file, format=UTF8)

    def output(self):
        return luigi.LocalTarget(
            f'{self.output_dir}/absa/target_aspects.json', format=UTF8
        )


class ConvertAspects(DataPreparationTask):

    def requires(self):
        return LoadAspects()

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
        print(aspects)
        for aspect, words in aspects.items():
            if not hasattr(words, 'items'):
                yield (aspect,), words
                continue
            for subaspect, words in self.flatten(words):
                yield (aspect, *subaspect), words

    def expand(self, flattened):
        for aspect, words in flattened.items():
            for word in words:
                yield aspect, word
