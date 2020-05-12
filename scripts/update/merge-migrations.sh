echo "merge-migrations"

mv "$2" "$4+1"
echo "$3" > "$2"

echo "merge-migrations: Resolved"

exit 0
