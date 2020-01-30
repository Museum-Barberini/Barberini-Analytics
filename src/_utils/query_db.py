import luigi
import psycopg2

from fill_db import FillDB
from set_db_connection_options import set_db_connection_options


class QueryDB(luigi.Task):
	
	def __init__(self, *args, **kwargs):
		super().__init__(*args, **kwargs)
		set_db_connection_options(self)
	
	tables = luigi.ListParameter(default=None) # If None, I will fetch all tables
	
	def requires(self):
		return FillDB()
	
	def output(self):
		return luigi.LocalTarget('output/DB.snapshot.txt')
	
	def run(self):
		with self.output().open('w') as output_file:
			try:
				connection = psycopg2.connect(
					host=self.host, database=self.database,
					user=self.user, password=self.password
				)
				cursor = connection.cursor()
				snapshotter = Snapshotter(cursor, output_file, self.tables)
				snapshotter.print_all()
				cursor.close()
			except psycopg2.DatabaseError as error:
				print(error)
			finally:
				if connection is not None:
					connection.close()

class Snapshotter:
	cursor = None
	output_file = None
	tables = None
	
	def __init__(self, cursor, output_file, tables):
		super().__init__()
		self.cursor = cursor
		self.output_file = output_file
		self.tables = tables
		
	def print_all(self):
		tables = self.tables
		if tables is None:
			tables = self.fetch_verbose("""
				SELECT table_name
				FROM information_schema.tables
				WHERE table_schema = 'public'
				ORDER BY table_name
				""")
			self.print_table(tables)
		for name in tables:
			table = self.fetch_verbose("""SELECT * FROM %s""" % name)
			self.print_table(table)
	
	def fetch_verbose(self, query):
		print(query, file=self.output_file)
		self.cursor.execute(query)
		return self.cursor.fetchall()
	
	def print_table(self, table):
		for row in table:
			print([value.encode("utf-8") if isinstance(value, str) else value
					for value in row],
				file=self.output_file)
		print("++++++++", file=self.output_file)
	
