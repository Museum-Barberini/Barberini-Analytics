# TODO: This does not respect db_config.yaml
# TODO: I have no clue why this does need sudo
cd output && sudo java -jar ../schemaspy.jar \
	-t pgsql -dp ../postgresql.jar \
	-host localhost -s public -db barberini -u postgres -p docker \
	-o schemaspy
