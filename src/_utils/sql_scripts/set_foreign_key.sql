-- {0} -- local table name
-- {1} -- local attribute
-- {2} -- foreign table name
-- {3} -- foreign attribute

BEGIN;
ALTER TABLE {0}
    ADD CONSTRAINT {1}_fkey FOREIGN KEY ({1}) REFERENCES {2} ({3});
COMMIT;
