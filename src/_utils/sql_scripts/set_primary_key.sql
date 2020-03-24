-- {0} -- table name
-- {1} -- primary key

BEGIN;
ALTER TABLE {0}
	DROP CONSTRAINT IF EXISTS {0}_primkey;
ALTER TABLE {0}
	ADD CONSTRAINT {0}_primkey PRIMARY KEY {1};
COMMIT;
