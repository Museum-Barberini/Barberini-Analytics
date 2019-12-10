import luigi
import psycopg2

from fill_db import FillDB
from set_db_connection_options import set_db_connection_options


class QueryDB(luigi.Task):
	
	def __init__(self, *args, **kwargs):
		super().__init__(*args, **kwargs)
		set_db_connection_options(self)
	
	tables = luigi.Parameter(
		default=["tweets", "gtrends_topics", "gtrends_interests"]
	)
	# TODO: Parse tables from metaquery below
	
	def requires(self):
		return FillDB()
	
	def output(self):
		return luigi.LocalTarget('output/DB.snapshot.txt')
	
	def run(self):
		with self.output().open('w') as output_file:
			try:
				conn = psycopg2.connect(
					host=self.host, database=self.database,
					user=self.user, password=self.password
				)
				cur = conn.cursor()
				for query in ["SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' ORDER BY table_name"] + \
						["SELECT * FROM %s" % table for table in self.tables]:
					print(query, file=output_file)
					cur.execute(query)
					
					# For now just print the query result
					row = cur.fetchone()
					while(row is not None):
						print([x.encode("utf-8") if isinstance(x, str) else x for x in row], file=output_file)
						row = cur.fetchone()
					print("++++++++", file=output_file)
				
				cur.close()
			
			except psycopg2.DatabaseError as error:
				print(error)
			
			finally:
				if conn is not None:
					conn.close()
