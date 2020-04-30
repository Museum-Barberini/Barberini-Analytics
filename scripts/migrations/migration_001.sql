-- Cleanse tweets and unify social media tables (!101)

BEGIN;

    ALTER TABLE appstore_review RENAME COLUMN date TO post_date;
    ALTER TABLE google_maps_review RENAME COLUMN date TO post_date;
    ALTER TABLE gplay_review RENAME COLUMN date TO post_date;

    CREATE TABLE tweet_author (
        user_id TEXT PRIMARY KEY, 
	    user_name TEXT NOT NULL
    );

    -- we want to re-fetch all the tweets because we can now handle emojis
    TRUNCATE tweet, tweet_performance RESTART IDENTITY;

    ALTER TABLE tweet ALTER COLUMN post_date TYPE TIMESTAMP;

COMMIT;
