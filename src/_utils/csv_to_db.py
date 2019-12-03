from luigi.contrib.postgres import CopyToTable
import luigi
from datetime import datetime
from abc import abstractmethod

from set_db_connection_options import set_db_connection_options

class CsvToDb(CopyToTable):

	# These attributes are set in __init__. They need to be defined
	# here because they are abstract methods in CopyToTable.
	host     = None
	database = None
	user     = None
	password = None

	def __init__(self, *args, **kwargs):
		super().__init__(*args, **kwargs)
		set_db_connection_options(self)

	# This parameter should be unique for every run forcing the task to re-run.
	date = luigi.Parameter(default=datetime.timestamp(datetime.now()))
	
	column_separator = ","
	
	def copy(self, cursor, file):
		cursor.copy_expert(load_sql_script("copy", self.table, self.table + '_tmp'), file)
	
	def rows(self):
		rows = super().rows()
		next(rows) # skip csv header
		return rows
	
	def create_table(self, connection):
		super.create_table(connection)
		self.create_primary_key(connection)
	
	def create_primary_key(self, connection):
		# LATEST TODO: Hope that exists work like we expect. Next put queries to create table with named constraints\
		# as declared in subclasses. Rename sql files. Maybe a subfolder?
		connection.cursor().execute(load_sql_script("set_primary_key", self.table, self.primary_key()))
	
	@property
	@abstractmethod
	# To be overridden, should return a tuple of column names
	def primary_key(self):
		pass


sql_file_path_pattern = 'src/_utils/csv_to_db.{0}.sql'
def load_sql_script(name, *args):
		with open(sql_file_path_pattern.format(name)) as sql_file:
			return sql_file.read().format(args)
