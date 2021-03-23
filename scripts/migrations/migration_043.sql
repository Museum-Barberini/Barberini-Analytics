-- Twitter expansion: Extended tweet collection (!345)

BEGIN;

    CREATE TABLE twitter_extended_candidates (
        term VARCHAR(255),
        user_id TEXT,
        tweet_id TEXT,
        text TEXT,
        response_to TEXT,
        post_date TIMESTAMP,
        permalink TEXT,
        PRIMARY KEY (term, tweet_id)
    );

COMMIT;
