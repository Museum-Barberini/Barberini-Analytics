from google_trends.gtrends_topics_json import *
from task_test import DatabaseTaskTest

class GtrendsTopicsJsonTest(DatabaseTaskTest):
	def __init__(self, methodName):
		super().__init__(methodName)
		self.task = self.isolate(GTrendsTopicsJson())
	
	def testGtrendsTopicsJson(self):
		self.task.run()
		with open('output/google-trends/topics.json', 'r') as file:
			json_string = file.read()
		self.assertTrue(json_string) # not empty
		json_dict = json.loads(json_string)
		for key, value in json_dict.items():
			self.assertTrue(isinstance(key, str))
			self.assertTrue(isinstance(value, str))
