import csv
import json
import jstyleson

from .data_preparation import DataPreparationTask


class JsonToCsv(DataPreparationTask):
    def run(self):
        my_json = self.get_json()

        with self.output().open('w') as csv_output:
            csvwriter = csv.writer(csv_output)
            csvwriter.writerow(my_json[0].keys())
            for json_row in my_json:
                csvwriter.writerow(json_row.values())

    def get_json(self):
        with self.input().open('r') as json_file:
            return json.load(json_file)


class JsoncToJson(DataPreparationTask):
    def run(self):
        with self.input().open('r') as input_file:
            facts = jstyleson.load(input_file)
        with self.output().open('w') as output_file:
            json.dump(facts, output_file)
