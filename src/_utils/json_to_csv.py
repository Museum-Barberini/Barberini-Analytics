import luigi
import json, csv

class JsonToCsv(luigi.Task): 
    def run(self):
        my_json = self.getJson()
        
        with self.output().open('w') as csv_output:
            csvwriter = csv.writer(csv_output)
            count = 0
            for json_row in my_json:
                if count == 0:
                    header = json_row.keys()
                    csvwriter.writerow(header)
                    count += 1
                csvwriter.writerow(json_row.values())
    
    def getJson(self):
        with self.input().open('r') as json_file:
            return json.loads(json_file.read())
