import os

import luigi
from luigi.format import UTF8

from google_trends.gtrends_topics_json import GTrendsTopicsJson


class GTrendsInterestJson(luigi.contrib.external_program.ExternalProgramTask):
    js_path = "./src/google_trends/trends_interest.js"

    def requires(self):
        return GTrendsTopicsJson()

    def output(self):
        return luigi.LocalTarget(
            "output/google-trends/interests.json", format=UTF8)

    def program_args(self):
        return ['node', self.js_path] + \
            [os.path.realpath(path)
             for path in [self.input().path, self.output().path]]
