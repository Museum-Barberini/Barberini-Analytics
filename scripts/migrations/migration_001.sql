BEGIN;

    ALTER TABLE appstore_review RENAME COLUMN date TO post_date;
    ALTER TABLE google_maps_review RENAME COLUMN date TO post_date;
    ALTER TABLE gplay_review RENAME COLUMN date TO post_date;

    CREATE TABLE tweet_author (
        user_id TEXT, 
	    user_name TEXT NOT NULL
    );

    ALTER TABLE tweet_author
        ADD CONSTRAINT tweet_author_primkey PRIMARY KEY (user_id);

    -- we want to re-fetch all the tweets because we can now handle emojis
    TRUNCATE tweet;

    ALTER TABLE tweet ALTER COLUMN post_date TYPE TIMESTAMP;

COMMIT;
