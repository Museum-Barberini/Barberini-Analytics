#!/usr/bin/env python3
"""
Supports sending a notification email about a failing CI pipeline.

The intention of this script is to send an error mail even outside of
Luigi's default capability, so that external errors are noticed (make
startup is still assumed to work, since luigi's send_error_mail is used).
"""
import logging
import sys

import luigi.notifications

from _utils.utils import enforce_luigi_notifications

logging.basicConfig(level=logging.INFO)


def send_failure_mail(url):
    """Send an error email informing that the CI pipeline has failed."""
    with enforce_luigi_notifications(format='html'):
        luigi.notifications.send_error_email(
            subject=("Scheduled pipeline failed"),
            message=("A scheduled pipeline has failed. For details, visit: "
                     f"{url}")
        )


if __name__ == '__main__':
    if len(sys.argv) < 2:
        raise ValueError(f"Usage: {sys.argv[0]} <url>")
    send_failure_mail(sys.argv[1])
