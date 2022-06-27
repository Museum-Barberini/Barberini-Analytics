from contextlib import contextmanager
import logging
import os
import sys
from typing import Union

import jsonpickle
import luigi
from varname import nameof

from backports.datetime_fromisoformat import MonkeyPatch
MonkeyPatch.patch_fromisoformat()


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


@contextmanager
def enforce_luigi_notifications(format):
    """Encorce sending luigi notification mails."""
    email = luigi.notifications.email()
    original = email.force_send, email.format
    luigi.notifications.email().format = format
    try:
        yield
    finally:
        email.force_send, email.format = original


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


def strcoord(string: str, index: int):
    """Convert the index in the string into a cooordinate tuple (row, col)."""
    if index > len(string):
        raise IndexError(f"{nameof(index)} out of range")
    lines = string[:index + 1].splitlines(keepends=True)
    return len(lines), len(lines[-1])
