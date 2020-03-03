import luigi
import jstyleson
import json


class MuseumFacts(luigi.Task):

    facts_file = luigi.Parameter(default='data/barberini_facts.jsonc')

    def output(self):
        return luigi.LocalTarget('output/museum_facts.json')

    def run(self):
        with open(self.facts_file, 'r') as input_file:
            facts = jstyleson.load(input_file)
            with self.output().open('w') as output_file:
                json.dump(facts, output_file)
