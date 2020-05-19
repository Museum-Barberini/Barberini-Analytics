import luigi
import os

from json_converters import JsoncToJson


class MuseumFacts(JsoncToJson):

    facts_file = luigi.Parameter(default='data/barberini_facts.jsonc')

    def input(self):
        return luigi.LocalTarget(self.facts_file)

    def output(self):
        return luigi.LocalTarget(
            f'{os.environ["OUTPUT_DIR"]}/museum_facts.json'
        )
