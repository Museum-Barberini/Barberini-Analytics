import logging
import os

logger = logging.getLogger('luigi-interface')

minimal_mode = os.getenv('MINIMAL') == 'True'


def output_dir():
    """Answer the path to the root of the output directory for all tasks."""
    return os.environ['OUTPUT_DIR']


from .utils import ObjectParameter, StreamToLogger             # noqa: E402
from .data_preparation import ConcatCsvs, DataPreparationTask  # noqa: E402
from ._database import DbConnector                             # noqa: E402
from .database import CsvToDb, QueryDb, QueryCacheToDb         # noqa: E402
from .json_converters import JsonToCsv, JsoncToJson            # noqa: E402
from .museum_facts import MuseumFacts                          # noqa: E402

# Backwards compatibility
from ._database import db_connector                            # noqa: E402
# TODO: At some time, we might want to use flake8-per-file-ignores or so ...


__all__ = [
    DataPreparationTask,
    ConcatCsvs, CsvToDb, DbConnector, QueryCacheToDb, QueryDb,
    JsonToCsv, JsoncToJson,
    MuseumFacts,
    ObjectParameter, StreamToLogger,

    db_connector, minimal_mode, output_dir
]
