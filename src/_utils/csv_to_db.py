from luigi.contrib.postgres import CopyToTable
import luigi
from datetime import datetime

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
		self.assure_schema(cursor) # Hacked because there is no better hook ¯\_(ツ)_/¯
		cursor.copy_expert(load_sql_script("copy").format(self.table, self.table + '_tmp'), file)
	
	def rows(self):
		rows = super().rows()
		next(rows) # skip csv header
		return rows
	
	def assure_schema(self, cursor):
		exists = cursor.execute(load_sql_script("check_existence").format(self.table)).fetchone()[0]
		if exists:
			return
		# LATEST TODO: Hope that exists work like we expect. Next put queries to create table with named constraints\
		# as declared in subclasses. Rename sql files. Maybe a subfolder?


sql_file_path_pattern = 'src/_utils/csv_to_db.{0}.sql'
def load_sql_script(name):
		with open(sql_file_path_pattern.format(name)) as sql_file:
			return sql_file.read()
