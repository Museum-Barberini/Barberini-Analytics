-- Nuke Google Trends right out of this plane of existence (#289)

BEGIN;

    DROP TABLE gtrends_value;

COMMIT;
