#!/usr/bin/env python3
import logging
import sys

import luigi.notifications

from _utils.utils import enforce_luigi_notifications

logging.basicConfig(level=logging.INFO)


def send_failure_mail(url):
    with enforce_luigi_notifications(format='html'):
        luigi.notifications.send_error_email(
            subject=("Scheduled pipeline failed"),
            message=("A scheduled pipeline has failed. For details, see: "
                     f"{url}.")
        )


if __name__ == '__main__':
    if len(sys.argv) < 2:
        raise ValueError(f"Usage: {sys.argv[0]} <url>")
    send_failure_mail(sys.argv[1])
