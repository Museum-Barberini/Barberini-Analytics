-- nuke gtrends right out of this plane of existence (#289)

BEGIN;

    DROP TABLE IF EXISTS gtrends_values;
    DROP TABLE IF EXISTS gtrends_topics;

COMMIT;
