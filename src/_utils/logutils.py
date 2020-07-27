from contextlib import contextmanager
import logging
import sys


class StreamToLogger:
    """
    Fake file-like stream object that redirects writes to a logger instance.
    """

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
