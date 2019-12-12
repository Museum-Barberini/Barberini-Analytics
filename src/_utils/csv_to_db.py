import csv
from luigi.contrib.postgres import CopyToTable
import luigi
import psycopg2
from datetime import datetime
from set_db_connection_options import set_db_connection_options


class CsvToDb(CopyToTable):

	@property
	def primary_key(self):
		raise NotImplemented()
	
	sql_file_path_pattern = luigi.Parameter(default='src/_utils/csv_to_db.sql/{0}.sql')
	
	# Don't delete this! This parameter assures that every (sub)instance of me is treated as an individual and will be re-run.
	dummy_date = luigi.Parameter(default=datetime.timestamp(datetime.now()))

	# These attributes are set in __init__. They need to be defined
	# here because they are abstract methods in CopyToTable.
	host	 = None
	database = None
	user	 = None
	password = None

	def __init__(self, *args, **kwargs):
		super().__init__(*args, **kwargs)
		set_db_connection_options(self)
		self.seed = 666

	def init_copy(self, connection):
		if not self.check_existence(connection):
			raise UndefinedTableError()
		super().init_copy(connection)
	
	def copy(self, cursor, file):
		query = self.load_sql_script('copy', self.table, ','.join(
			[f'{col[0]} = EXCLUDED.{col[0]}' for col in self.columns]))
		## DEBUG
		print(query)
		print(file.read())
		## GUBED
		# LATEST TODO: Only run gomus to find out why it fails.
		# The bug we originally wanted to analyze was that it tried to write a whole row into the first column.
		cursor.copy_expert(query, file)
	
	def rows(self):
		with self.input().open('r') as csvfile:
			reader = csv.reader(csvfile)
			next(reader) # skip header
			for row in reader:
				yield row
	
	def check_existence(self, connection):
		cursor = connection.cursor()
		cursor.execute(self.load_sql_script('check_existence', self.table))
		existence_boolean = cursor.fetchone()[0]
		return existence_boolean
	
	def create_table(self, connection):
		super().create_table(connection)
		self.create_primary_key(connection)
	
	def create_primary_key(self, connection):
		connection.cursor().execute(self.load_sql_script(
			'set_primary_key',
			self.table,
			self.tuple_like_string(self.primary_key)))
	
	def load_sql_script(self, name, *args):
		with open(self.sql_file_path_pattern.format(name)) as sql_file:
			return sql_file.read().format(*args)
	
	def tuple_like_string(self, value):
		string = value
		if isinstance(value, tuple):
			string = ','.join(value)
		return f'({string})'

class UndefinedTableError(psycopg2.ProgrammingError):
	pgcode = psycopg2.errorcodes.UNDEFINED_TABLE
