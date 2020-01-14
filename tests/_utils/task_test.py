import unittest
import psycopg2
import os

""" 
IMPORTANT NOTE:
To be able to run tests that use this helper, you will need
* a running postgres database server
* a database named 'barberini_test'.
"""
# ------ CREATE DATABASE IF NECESSARY -------
conn = psycopg2.connect(
	host=os.environ['POSTGRES_HOST'],
	user=os.environ['POSTGRES_USER'],
	password=os.environ['POSTGRES_PASSWORD'])
try:
	conn.autocommit = True
	cur = conn.cursor()
	try:
		cur.execute("CREATE DATABASE barberini_test;")
	finally:
		cur.close()
finally:
	conn.close()


class DatabaseHelper:
	def setUp(self):
		
		self.connection = psycopg2.connect(
			host=os.environ['POSTGRES_HOST'],
			dbname="barberini_test",
            user=os.environ['POSTGRES_USER'],
			password=os.environ['POSTGRES_PASSWORD'])
	
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


# DOCUMENTATION: This is kind of 💩💩💩
# Don't like it, don't keep it, if you can.
# This should be very easy by using unittest.mock the right way, but I failed ...

class DatabaseTaskTest(unittest.TestCase):
	db = DatabaseHelper()
	
	def setUp(self):
		super().setUp()
		self.db.setUp()
	
	def tearDown(self):
		super().tearDown()
		self.db.tearDown()
	
	def isolate(self, task):
		task.complete = lambda: True
		return task
