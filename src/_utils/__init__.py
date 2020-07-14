import logging
import os

logger = logging.getLogger('luigi-interface')

minimal_mode = os.getenv('MINIMAL') == 'True'
OUTPUT_DIR = os.environ['OUTPUT_DIR']

from data_preparation import DataPreparationTask    # noqa: E402
from _database import DbConnector                   # noqa: E402
from database import CsvToDb, QueryDb               # noqa: E402
from json_converters import JsonToCsv, JsoncToJson  # noqa: E402
from museum_facts import MuseumFacts                # noqa: E402
from utils import ConcatCsvs, StreamToLogger        # noqa: E402

# Backwards compatibility
from _database import db_connector                  # noqa: E402
# TODO: At some time, we might want to use flake8-per-file-ignores or so ...


__all__ = [
    DataPreparationTask,
    CsvToDb, DbConnector, QueryDb,
    JsonToCsv, JsoncToJson,
    MuseumFacts,
    ConcatCsvs, StreamToLogger,

    db_connector
]
