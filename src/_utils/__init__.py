import logging
import os

logger = logging.getLogger('luigi-interface')

minimal_mode = os.getenv('MINIMAL') == 'True'
OUTPUT_DIR = os.environ['OUTPUT_DIR']

from data_preparation import DataPreparationTask
from _database import DbConnector
from database import CsvToDb, QueryDb
from json_converters import JsonToCsv, JsoncToJson
from museum_facts import MuseumFacts
from utils import ConcatCsvs, StreamToLogger

# Backwards compatibility
from _database import db_connector

__all__ = [
	DataPreparationTask,
	CsvToDb, DbConnector, QueryDb,
	JsonToCsv, JsoncToJson,
	MuseumFacts,
	ConcatCsvs, StreamToLogger,

	db_connector
]
