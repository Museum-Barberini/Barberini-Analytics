#!/usr/bin/env python3
"""
Supports sending a notification email about a critical production error.

The intention of this script is to send an error mail even outside of
Luigi's default capability, so that external errors are noticed (make
startup is still assumed to work, since luigi's send_error_mail is used).
"""

import logging
import socket
import sys

import luigi.notifications

logging.basicConfig(level=logging.INFO)


def send_error_mail(error_source, details=None):
    """Send an error email informing that the data pipeline has failed."""
    if details:
        import django.utils.html
        details = django.utils.html.escape(details).replace('\n', '<br>')
    luigi.notifications.send_error_email(
        subject=("External error in production pipeline. "
                 f"Host: {socket.gethostname()}"),
        message=("An external error has occured while trying to run the "
                 "pipeline. For details see the according log.<br>"
                 f"Error source: {error_source}"
                 + (f"<br>Details: {details}" if details else ""))
    )


if __name__ == '__main__':
    if len(sys.argv) < 2:
        raise ValueError(f"Usage: {sys.argv[0]} <error source> [<details>]")
    send_error_mail(sys.argv[1], sys.argv[2] if len(sys.argv) > 2 else None)
