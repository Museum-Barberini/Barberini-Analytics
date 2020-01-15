import unittest
import luigi
from unittest.mock import patch
import yaml
import tempfile

from set_db_connection_options import set_db_connection_options


class TestSetDbConnectionOptions(unittest.TestCase):

	@patch("set_db_connection_options.open")
	def test_setting_db_connection_attributes(self, mock):

		# initialize mock config file
		tmp_file = tempfile.NamedTemporaryFile()
		with open(tmp_file.name, "w") as fp:
			yaml.dump(
				{"host": "h", "database": "d", "user": "u", "password": "p"},
				fp
			)
		mock.return_value = open(tmp_file.name)
		
		# call function under test
		task = luigi.Task()
		set_db_connection_options(task, "some_config.yaml")
		
		# inspect result
		self.assertEqual(task.host, "h")
		self.assertEqual(task.database, "d")
		self.assertEqual(task.user, "u")
		self.assertEqual(task.password, "p")
		mock.assert_called_with("some_config.yaml")
