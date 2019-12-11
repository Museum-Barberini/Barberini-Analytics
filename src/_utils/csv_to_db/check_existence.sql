-- {0} -- table name

SELECT EXISTS (
	SELECT 42
	FROM   information_schema.tables 
	WHERE  table_schema = 'public'
	AND    table_name = '{0}'
);
