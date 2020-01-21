import luigi
import json

class GTrendsTopicsJson(luigi.Task):
    
    input_file = luigi.Parameter(default='data/barberini-facts.json')
    
    def output(self):
        return luigi.LocalTarget("output/google-trends/topics.json")
    
    def run(self):
        topics = self.collect_topics()
        with self.output().open("w") as output_file:
            output_file.write(json.dumps(dict(enumerate(topics))))
    
    
    def collect_topics(self):
        with open(self.input_file) as json_top_10_facts_about_barberini_you_didnt_know_number_8_will_shock_you:
            barberini_facts = \
                json.load(json_top_10_facts_about_barberini_you_didnt_know_number_8_will_shock_you)
            import pdb; pdb.set_trace()
            barberini_topic = barberini_facts['ids']['google']['keyword']
            exhibitions_topics = ['barberini ' + exhibition for exhibition in barberini_facts['exhibitions']]
            
            return [barberini_topic] + exhibitions_topics
