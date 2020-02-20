import unittest
from luigi.mock import MockTarget
from luigi.format import UTF8
import json
import os
import subprocess
import unittest

import psycopg2

from museum_facts import MuseumFacts


""" 
IMPORTANT NOTE:
To be able to run tests that use this helper, you will need
- a running postgres database server,
- a database named 'barberini_test'.
"""

def create_database_if_necessary():
    cur = conn = None
    try:
        conn = psycopg2.connect(
            host=os.environ['POSTGRES_HOST'],
            user=os.environ['POSTGRES_USER'],
            password=os.environ['POSTGRES_PASSWORD'],
            database='postgres') # barberini_test may not yet exist
        conn.autocommit = True
        cur = conn.cursor()
        database = os.environ['POSTGRES_DB']
        assert 'test' in database, 'Tests cannot be run on production database. Use GitLab Runner or make test.'
        try:
            cur.execute(f"DROP DATABASE {database};") # each test execution should get a fresh database
        except psycopg2.errors.InvalidCatalogName:
            pass # did not exist ¯\_(ツ)_/¯
        cur.execute(f"CREATE DATABASE {database};")
    finally:
        if cur is not None:
            cur.close()
        if conn is not None:
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
        result = self.cursor.fetchall()
        self.column_names = [desc[0] for desc in self.cursor.description]
        self.cursor.close()
        return result
    
    def commit(self, *queries):
        self.cursor = self.connection.cursor()
        for query in queries:
            self.cursor.execute(query)
        self.cursor.close()
        self.connection.commit()


class DatabaseTaskTest(unittest.TestCase):
    db = DatabaseHelper()
    
    def setUp(self):
        super().setUp()
        create_database_if_necessary()
        self.db.setUp()
        
        facts_task = MuseumFacts()
        facts_task.run()
        with facts_task.output().open('r') as facts_file:
            self.facts = json.load(facts_file)
    
    def tearDown(self):
        super().tearDown()
        self.db.tearDown()
    
    def isolate(self, task):
        task.complete = True
        return task
    
    def install_mock_target(self, mock_object, store_function):
        mock_target = MockTarget(f'mock{hash(mock_object.hash())}', format=UTF8)
        with mock_target.open('w') as input_file:
            store_function(input_file)
        mock_object.return_value = mock_target
        return mock_target
