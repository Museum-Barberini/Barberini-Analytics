#!/usr/bin/env python3
# The intention of this script is to send an error mail even outside of
# Luigi's default capability, so that external errors are noticed (make
# startup is still assumed to work, since luigi's send_error_mail is used)
import sys
import luigi.notifications


def send_error_mail(error_source):
    luigi.notifications.send_error_email(
        subject="[BP] External error in production pipeline",
        message=("An external error has occured while trying to run the "
                 "pipeline. For details see the according log.\n"
                 f"Host: {error_source}")
    )


if __name__ == '__main__':
    if len(sys.argv) < 2:
        raise ValueError(f"Usage: {sys.argv[0]} <error source>")
    send_error_mail(sys.argv[1])
