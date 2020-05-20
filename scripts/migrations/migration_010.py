#!/usr/bin/env python3
# Rename constraints (#193)
import db_connector
db_connector = db_connector.db_connector()


# 1. Rename primary keys
primkey_changes = [
    f'''
        ALTER TABLE {table_schema}.{table_name}
        RENAME CONSTRAINT {constraint_name}
        TO {table_name}_pkey;
    '''
    for constraint_name, table_schema, table_name
    in db_connector.query('''
        SELECT DISTINCT(tco.constraint_name),
            tco.table_schema,
            kcu.table_name
        FROM information_schema.table_constraints tco
        JOIN information_schema.key_column_usage kcu
            ON	kcu.constraint_name = tco.constraint_name
            AND	kcu.constraint_schema = tco.constraint_schema
            AND	kcu.constraint_name = tco.constraint_name
        WHERE tco.constraint_type = 'PRIMARY KEY'
            AND tco.constraint_name LIKE '%_primkey'
    ''')
]

# 2. Rename foreign keys
fkey_changes = [
    f'''
        ALTER TABLE {table_schema}.{table_name}
        RENAME CONSTRAINT {constraint_name}
        TO {table_name}_{column_name}_fkey
    '''
    for constraint_name, table_schema, table_name, column_name
    in db_connector.query('''
        SELECT DISTINCT(tco.constraint_name),
            tco.table_schema,
            tco.table_name,
            kcu.column_name
        FROM information_schema.table_constraints AS tco
            JOIN information_schema.key_column_usage AS kcu
            ON tco.constraint_name = kcu.constraint_name
            AND tco.table_schema = kcu.table_schema
            JOIN information_schema.constraint_column_usage AS ccu
            ON ccu.constraint_name = tco.constraint_name
            AND ccu.table_schema = tco.table_schema
        WHERE tco.constraint_type = 'FOREIGN KEY'
            AND tco.constraint_name NOT LIKE tco.table_name || '%_'
    ''')
]

# 3. Commit
db_connector.execute(*primkey_changes, *fkey_changes)
