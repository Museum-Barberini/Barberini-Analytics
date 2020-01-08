""" 
IMPORTANT NOTE:
To be able to run tests that use this helper, you will need
* a running postgres database server
* a database named 'barberini_test'.
"""

import psycopg2

# ------ CREATE DATABASE IF NECESSARY -------
try:
    conn = psycopg2.connect(host="db", user="postgres", password="docker")
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute("CREATE DATABASE barberini_test;")
except psycopg2.DatabaseError as error:
    print(error)
finally:
    cur.close()
    conn.close()


class DatabaseHelper:
	def setUp(self):
		self.connection = psycopg2.connect(
			host="db",
			dbname="barberini_test",
			user="postgres",
			password="docker")
	
	def tearDown(self):
		self.connection.close()
	
	def request(self, query):
		self.cursor = self.connection.cursor()
		self.cursor.execute(query)
		self.result = cursor.fetchall()
		self.cursor.close()
		return result
	
	def commit(self, *queries):
		self.cursor = self.connection.cursor()
		for query in queries:
			cursor.execute(query)
		self.cursor.close()
		self.connection.commit()
