BEGIN;

    ALTER TABLE appstore_review RENAME COLUMN date TO post_date;
    ALTER TABLE google_maps_review RENAME COLUMN date TO post_date;
    ALTER TABLE gplay_review RENAME COLUMN date TO post_date;

    CREATE TABLE tweet_author (
        user_id TEXT PRIMARY KEY, 
	user_name TEXT NOT NULL
    );

COMMIT;
