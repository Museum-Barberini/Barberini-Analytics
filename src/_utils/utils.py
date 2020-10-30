from contextlib import contextmanager
import logging
import os
import sys
from typing import Union

import jsonpickle
import luigi
import pandas as pd

from .data_preparation import DataPreparationTask


class ConcatCsvs(DataPreparationTask):
    """Concatenate all input CSV files into a single output CSV file."""

    def run(self):

        dfs = [
            self.read_csv(input)
            for input in luigi.task.flatten(self.input())
        ]
        df = pd.concat(dfs)
        with self.output().open('w') as output:
            df.to_csv(output, index=False)

    def read_csv(self, target: luigi.Target):

        with target.open('r') as input:
            return pd.read_csv(input)


class ObjectParameter(luigi.Parameter):
    """A luigi parameter that takes an arbitrary object."""

    def parse(self, input):

        return jsonpickle.loads(input)

    def serialize(self, obj):

        return jsonpickle.dumps(obj)


class StreamToLogger:
    """Fake file-like stream that redirects writes to a logger instance."""

    def __init__(
        self,
        logger: logging.Logger = logging.getLogger(),
        log_level=logging.INFO
            ):

        self.logger = logger
        self.log_level = log_level
        self.linebuf = ''  # Disable buffering and flush()

    def write(self, message):

        for line in message.rstrip().splitlines():
            self.logger.log(self.log_level, line.rstrip())

    @contextmanager
    def activate(self):

        outer_stream = sys.stdout
        sys.stdout = self
        try:
            yield
        finally:
            sys.stdout = outer_stream


def load_django_renderer():
    """Load Django's renderer for generating HTML files."""
    import django
    from django.conf import settings
    from django.template.loader import render_to_string

    if not settings.configured:
        settings.configure(TEMPLATES=[{
            'BACKEND': 'django.template.backends.django.DjangoTemplates',
            'DIRS': [os.getcwd()],
        }])
    django.setup()
    return render_to_string


@contextmanager
def set_log_level_temporarily(logger: logging.Logger, level: Union[int, str]):
    """Context manager to change the log level of the logger temporarily."""
    old_level = logger.level
    logger.setLevel(level)
    try:
        yield
    finally:
        logger.setLevel(old_level)
