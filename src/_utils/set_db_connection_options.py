import os
import yaml
import luigi


def set_db_connection_options(task: luigi.Task, db_config: str = None) -> None:
	""" 
	Set the attributes host, database, user, and password to
	the values given in the db_config file for the given task.
	Modifies the task in place.

	Usage:
		To use this method for a luigi.Task add the following __init__
		method to the task:

			def __init__(self, *args, **kwargs):
		        super().__init__(*args, **kwargs)
		        set_db_connection_options(self)

		Please note that if the class you are modifying is a subclass
		of luigi.contrib.postgres.CopyToTable you need to declare the 
		attributes host, database, user, and password outside of __init__.
		You can do so with the following piece of code:

			host     = None
			database = None
			user     = None
			password = None

			def __init__(...):
				...
	"""
	
	if db_config is None:
		db_config = './db_config.yaml'
		if not os.path.isfile(db_config):
			db_config = './db_config_default.yaml'
	with open(db_config) as config_file:
		db_config_options = yaml.load(config_file)

	task.host 	  = db_config_options["host"]
	task.database = db_config_options["database"]
	task.user 	  = db_config_options["user"]
	task.password = db_config_options["password"]
