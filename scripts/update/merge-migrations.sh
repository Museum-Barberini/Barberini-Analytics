#!/bin/bash
echo "merge-migrations $4"

set -e

o=$(pwd)
# $4 is %P
cd $(dirname "$4")
 echo "cd'ed"
[[ "$4" =~ .*\/migration_([[:alnum:]]+)(\..+)? ]]
 echo "matched"
i="${BASH_REMATCH[1]}"
ext="${BASH_REMATCH[2]}"
 echo "i=$i, ext=$ext"

j="$i"
while : ; do
     echo "i=$i"
    ((j++))
    i=$(printf "%0${#i}d\n" "$j")
    n="migration_$i"
    [ "$(echo "$(find . -maxdepth 1 -name 'migration_*.*' -exec bash -c 'printf "%s.\n" "${@%.*}"' _ {} + | cut -c3-)"$'\n'"$n" | sort | tail -n1)" = "$n" ] \
        && break
done

cd "$o"
echo "now moving"
mv "$2" "$(dirname "$4")/migration_$i$ext"
mv "$3" "$2"

echo "merge-migrations: Resolved"
trap '>&2 echo -e "☝ Please try again: \n $ git add \"migration_$i$ext\""' ERR
git add "migration_$i$ext"    
