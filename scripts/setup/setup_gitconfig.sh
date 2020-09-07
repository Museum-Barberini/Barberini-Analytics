#!/bin/bash
set -e

cd "$(dirname "$0")/../.."
cat .gitconfig >> .git/config
