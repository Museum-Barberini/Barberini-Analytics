from luigi.contrib.postgres import CopyToTable
import luigi
import psycopg2

from set_db_connection_options import set_db_connection_options


class CsvToDb(CopyToTable):

	# These attributes are set in __init__. They need to be defined
	# here because they are abstract methods in CopyToTable.
	host	 = None
	database = None
	user	 = None
	password = None

	def __init__(self, *args, **kwargs):
		super().__init__(*args, **kwargs)
		set_db_connection_options(self)

	
	def init_copy(self, connection):
		if not self.check_existence(connection):
			raise UndefinedTableError()
		super().init_copy(connection)
	
	def copy(self, cursor, file):
		query = load_sql_script("copy", self.table, ",".join([f"{col[0]} = EXCLUDED.{col[0]}" for col in self.columns]))
		cursor.copy_expert(query, file)
	
	def rows(self):
		rows = super().rows()
		next(rows) # skip csv header
		return rows
	
	def check_existence(self, connection):
		cursor = connection.cursor()
		cursor.execute(load_sql_script("check_existence", self.table))
		return cursor.fetchone()[0]
	
	def create_table(self, connection):
		super().create_table(connection)
		self.create_primary_key(connection)
	
	def create_primary_key(self, connection):
		prim_key = self.primary_key
		if isinstance(prim_key, tuple):
			prim_key = ",".join(prim_key)
		prim_key = f"({prim_key})"
		connection.cursor().execute(load_sql_script("set_primary_key", self.table, prim_key))
	
	@property
	def primary_key(self):
		raise NotImplemented()


sql_file_path_pattern = 'src/_utils/csv_to_db.{0}.sql'
def load_sql_script(name, *args):
	with open(sql_file_path_pattern.format(name)) as sql_file:
		return sql_file.read().format(*args)

class UndefinedTableError(psycopg2.ProgrammingError):
	pgcode = psycopg2.errorcodes.UNDEFINED_TABLE
