import luigi
import json

from museum_facts import MuseumFacts


class GtrendsTopics(luigi.Task):

    def requires(self):
        return MuseumFacts()

    def output(self):
        return luigi.LocalTarget('output/google_trends/topics.json')

    def run(self):
        topics = self.collect_topics()
        with self.output().open('w') as output_file:
            json.dump(topics, output_file)

    def collect_topics(self):
        with self.input().open('r') as facts_file:
            facts = json.load(facts_file)

        museum_topic = facts['ids']['google']['knowledgeId']
        gtrends_facts = facts['gtrends']
        museum_names = gtrends_facts['museumNames']
        extra_topics = gtrends_facts['topics']
        complex_topics = [
            ' '.join([museum_name, extra_topic])
            for extra_topic in extra_topics
            for museum_name in museum_names]

        return [museum_topic] + complex_topics