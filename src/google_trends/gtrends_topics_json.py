import json

import luigi
from luigi.format import UTF8


class GTrendsTopicsJson(luigi.Task):

    input_file = luigi.Parameter(default='data/barberini-facts.json')

    def output(self):
        return luigi.LocalTarget(
            'output/google-trends/topics.json', format=UTF8)

    def run(self):
        topics = self.collect_topics()
        with self.output().open('w') as output_file:
            output_file.write(json.dumps(dict(enumerate(topics))))

    def collect_topics(self):
        with open(self.input_file, encoding='utf-8') as json_file:
            barberini_facts = json.load(json_file)
            barberini_topic = barberini_facts['ids']['google']['keyword']
            exhibitions_topics = [
                'barberini ' +
                exhibition for exhibition in barberini_facts['exhibitions']]

            return [barberini_topic] + exhibitions_topics
