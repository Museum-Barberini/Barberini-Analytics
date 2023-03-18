#!/bin/bash
set -e

# apt version is too old

scversion="stable" # or "v0.4.7", or "latest"
echo foo > /dev/stderr # debug
wget -qO- "https://github.com/koalaman/shellcheck/releases/download/${scversion?}/shellcheck-${scversion?}.linux.x86_64.tar.xz" > download # debug
echo bar > /dev/stderr # debug
# print size of file 'download'
wc -c download > /dev/stderr # debug
cat download | /dev/stderr # debug
wget -qO- "https://github.com/koalaman/shellcheck/releases/download/${scversion?}/shellcheck-${scversion?}.linux.x86_64.tar.xz" | tar -xJv
cp "shellcheck-${scversion}/shellcheck" /usr/bin/
rm -rf "shellcheck-${scversion?}"
shellcheck --version
