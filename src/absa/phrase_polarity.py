from io import BytesIO, TextIOWrapper
import luigi
from luigi.format import UTF8
import pandas as pd
from urllib.request import urlopen
from zipfile import ZipFile

import regex

from .post_words import regex_compile
from csv_to_db import CsvToDb
from data_preparation import DataPreparationTask


class PhrasePolaritiesToDb(luigi.WrapperTask):

    def requires(self):

        yield SentiWsToDb()
        yield SeplToDb()


class SentiWsToDb(CsvToDb):

    table = 'absa.phrase_polarity_sentiws'

    def requires(self):

        return FetchSentiWs()


class SeplToDb(CsvToDb):

    table = 'absa.phrase_polarity_sepl'

    def requires(self):

        return FetchSepl()


class FetchSentiWs(DataPreparationTask):

    url = luigi.Parameter(
        'http://pcai056.informatik.uni-leipzig.de/downloads/etc/'
        'SentiWS/SentiWS_v2.0.zip'
    )

    format_float = regex.compile(r'[-+]?[0-9]*\.?[0-9]+([eE][-+]?[0-9]+)?')

    format = regex_compile(rf'''
            ^
            (?<word>\p{{L}}+)
            \|
            (?<pos_tag>[A-Z]+)
            \t
            (?<weight>{format_float.pattern})
            \t?
            ((?<=\t)
                (?<inflection>\p{{L}}+)
                (\s*,\s*
                    (?<inflection>\p{{L}}+)
                )*
            )?
            $
        ''')

    def output(self):
        return luigi.LocalTarget(
            f'{self.output_dir}/absa/sentiws.csv',
            format=UTF8
        )

    def run(self):

        response = urlopen(self.url)
        archive = ZipFile(BytesIO(response.read()))
        rows = self.load_polarities(archive)
        df = pd.DataFrame(
            rows,
            columns=['word', 'pos_tag', 'weight', 'inflections']
        )

        with self.output().open('w') as output:
            df.to_csv(output, index=False)

    def load_polarities(self, archive):

        for way in ['Positive', 'Negative']:
            with archive.open(f'SentiWS_v2.0_{way}.txt') as input:
                input_text = TextIOWrapper(input, encoding='utf-8')
                for line in input_text:
                    yield self.load_polarity(line)

    def load_polarity(self, line):

        match = self.format.search(line)
        return dict(
            word=match.group('word'),
            pos_tag=match.group('pos_tag'),
            weight=float(match.group('weight')),
            inflections=match.captures('inflection')
        )


class FetchSepl(DataPreparationTask):
    """
    Sentiment Phrase List. URL: http://www.opinion-mining.org
    """

    def input(self):

        return luigi.LocalTarget(
            'secret_files/absa/SePL-german-v1.1.csv',
            format=UTF8
        )

    def output(self):

        return luigi.LocalTarget(
            f'{self.output_dir}/absa/sepl.csv',
            format=UTF8
        )

    def run(self):

        with self.input().open() as input:
            df = pd.read_csv(
                input,
                header=None,
                sep=';',
                comment='#',
                keep_default_na=False
            )
        df.columns = [
            'phrase',
            'opinion_value',
            'standard_deviation',
            'standard_error',
            'phrase_type',
            'manual_correction'
        ]

        df['manual_correction'] = df['manual_correction'].apply(
            self.parse_manual_correction)

        with self.output().open('w') as output:
            df.to_csv(output, index=False)

    def parse_manual_correction(self, value):

        if not value:
            return False
        if value == 'm':
            return True
        raise ValueError(f"Unknow manual_correction: {value}")
