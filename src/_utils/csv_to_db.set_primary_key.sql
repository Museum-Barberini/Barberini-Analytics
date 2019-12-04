-- {0} -- table name
-- {1} -- primary key

BEGIN;
ALTER TABLE {0}
	DROP CONSTRAINT IF EXISTS the_primary_key_constraint;
ALTER TABLE {0}
	ADD PRIMARY KEY {1};
COMMIT;
