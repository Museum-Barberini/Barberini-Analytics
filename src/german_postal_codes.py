"""Provides list of German postal codes."""

import luigi
import pandas as pd
from luigi.format import UTF8

from _utils import DataPreparationTask


class LoadGermanPostalCodes(DataPreparationTask):
    """Load German postal codes from disk."""

    def output(self):
        return luigi.LocalTarget(
            f'{self.output_dir}/german_postal_codes.csv',
            format=UTF8
        )

    def run(self):
        with open('data/German-Zip-Codes.csv',
                  'r', encoding='utf-8') as input_file:
            df = pd.read_csv(input_file,
                             encoding='utf-8',
                             error_bad_lines=False,
                             sep=';',
                             dtype=str)
            with self.output().open('w') as output_file:
                df.to_csv(output_file, encoding='utf-8')
