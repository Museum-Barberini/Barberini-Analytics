#!/bin/bash
set -e

# apt version is too old

scversion="stable" # or "v0.4.7", or "latest"
wget -O- "https://github.com/koalaman/shellcheck/releases/download/${scversion?}/shellcheck-${scversion?}.linux.x86_64.tar.xz" | tar -xJv
cp "shellcheck-${scversion}/shellcheck" /usr/bin/
rm -rf "shellcheck-${scversion?}"
shellcheck --version
