#!/bin/bash
set -e
cd "$(dirname "$0")"
(crontab -l ; cat .crontab) | crontab -
