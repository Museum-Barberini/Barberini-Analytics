import unittest
from unittest.mock import MagicMock
from unittest.mock import *
from google_trends.gtrends_topics_table import *
import pandas as pd
import io


class TaskMock:
	def __init__(self, task):
		self.__init__()
		self.task = task
	
	def input(self):
		return [path.replace('output/', 'tests/') for path in task.input()]

class TestGtrendsTopicsTable(unittest.TestCase):
	#def setup(self):
	#	super().setup()
	task = GTrendsTopicsTable()
	task.complete = lambda: True
	# LATEST TODO: Change task to use our "wrap-mocking" TaskMock.input()
	# This might help: https://stackoverflow.com/questions/30336828/is-there-a-way-to-access-the-original-function-in-a-mocked-method-function-such
	# And write the json into the file.
	
	@patch.object(JsonToCsvTask, 'input', io.BytesIO(b'''{
			1: "one",
			2: "two"
		}'''))
	def test_getJson(self):
		#mock.getJson = 'anunexecutable'
		#lambda: {
		#	1: "one",
		#	2: "two"
		#}
		
		task = GTrendsTopicsTable()
		actual = task.getJson()
		
		expected = [
			{'topic_id': 1, 'name': "one"},
			{'topic_id': 2, 'name': "two"}
		]
		self.assertEqual(actual, expected)
		# LATEST TODO: Why does mocking not work?


if __name__ == '__main__':
	unittest.main()
