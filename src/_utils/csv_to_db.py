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
		cursor.copy_expert("""COPY {0} FROM STDIN WITH (FORMAT CSV)""".format(self.table), file)

	def rows(self):
            rows = super().rows()
            next(rows)  # skip csv header
            return rows

