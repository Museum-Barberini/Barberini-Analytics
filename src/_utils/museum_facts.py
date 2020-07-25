import luigi
from luigi.format import UTF8

from .json_converters import JsoncToJson


class MuseumFacts(JsoncToJson):

    facts_file = luigi.Parameter(default='data/barberini_facts.jsonc')

    def input(self):
        return luigi.LocalTarget(self.facts_file)

    def output(self):
        return luigi.LocalTarget(
            f'{self.output_dir}/museum_facts.json',
            format=UTF8
        )
