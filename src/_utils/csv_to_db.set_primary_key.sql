-- {0} -- table name
-- {1} -- primary key

BEGIN;
ALTER TABLE {0}
	DROP CONSTRAINT IF EXISTS {0}_the_primary_key_constraint;
ALTER TABLE {0}
	ADD CONSTRAINT {0}_the_primary_key_constraint PRIMARY KEY {1};
COMMIT;
