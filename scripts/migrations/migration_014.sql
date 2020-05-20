BEGIN;

    ALTER TABLE tweet_author
        ADD COLUMN role TEXT;

COMMIT;
