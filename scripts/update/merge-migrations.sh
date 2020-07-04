#!/bin/bash
# This is a merge driver for git that automatically resolves conflicts between
# migration scripts. To install it, run scripts/setup/setup_gitconfig.sh. For
# more information, read the MR description of !157.
# $@ = %O %A %B %P = ancestor theirs ours path

echo "merge-migrations $4"
set -e

o=$(pwd)
cd $(dirname "$4")
[[ "$4" =~ .*\/migration_([[:alnum:]]+)(\..+)? ]]
i="${BASH_REMATCH[1]}"
ext="${BASH_REMATCH[2]}"

# Find available migration number
j="$i"
while : ; do
    ((j++)) && i=$(printf "%0${#i}d\n" "$j")
    n="migration_$i"
    [ "$(echo "$(find . -maxdepth 1 -name 'migration_*.*' -exec bash -c 'printf "%s.\n" "${@%.*}"' _ {} + | cut -c3-)"$'\n'"$n" | sort | tail -n1)" = "$n" ] \
        && break
done

cd "$o"
mv "$2" "$(dirname "$4")/migration_$i$ext"
mv "$3" "$2"

echo "merge-migrations: Resolved"
trap '>&2 echo -e "☝ Please try again: \n $ git add \"migration_$i$ext\""' ERR
git add "migration_$i$ext"    
