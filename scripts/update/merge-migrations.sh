#!/bin/bash
# This is a merge driver for git that automatically resolves conflicts between
# migration scripts. To install it, run scripts/setup/setup_gitconfig.sh. For
# more information, read the MR description of !157.
# $@ = %O %A %B %P = ancestor theirs ours path

echo "merge-migrations $4"
set -e

o=$(pwd)
t=$(dirname "$4")
cd "$t"
[[ "$4" =~ .*\/migration_([[:alnum:]]+)(\..+)? ]]
i="${BASH_REMATCH[1]}"
ext="${BASH_REMATCH[2]}"

# Find available migration number
j=${i#0}
while : ; do
    ((j++)) ; i=$(printf "%0${#i}d\n" "$j")
    n="migration_$i"
    [ "$(echo "$(find . -maxdepth 1 -name 'migration_*.*' -exec \
        bash -c 'printf "%s.\n" "${@%.*}"' _ {} + \
        | cut -c3-)"$'\n'"$n" \
        | sort \
        | tail -n1)" = "$n" ] \
        && break
    if (( j > 1024 )); then
        echo "merge-migrations: Suspicious migration number detected"
        echo "My developers have had some very bad experience with infinite"
        echo "loops, so they decided not to support migration numbers > 1024"
        echo "at the moment. If and only you if you are sure you need this,"
        echo "please update this constant. But be warned about the danger! 💀"
        exit 42
    fi
done

cd "$o"
mv "$2" "$t/migration_$i$ext"
mv "$3" "$2"

echo "merge-migrations: Resolved"
commit="git add \"$t/migration_$i$ext\""
trap '>&2 echo -e "☝ Please try again: \n $ $commit"' ERR
eval $commit
