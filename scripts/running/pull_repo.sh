#!/bin/bash
set -e

cd "$(dirname "$0")"
git pull

make -C ../.. apply-pending-migrations
