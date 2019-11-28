SELECT EXISTS (
   SELECT 42
   FROM   information_schema.tables 
   WHERE  table_schema = 'schema_name'
   AND    table_name = '{0}'
   );
