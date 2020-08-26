BEGIN;

    DROP TABLE twitter_extended_candidates;
    DROP TABLE twitter_extended_dataset;

    CREATE TABLE twitter_extended_candidates (
        term TEXT,
        user_id TEXT,
        tweet_id TEXT,
        text TEXT,
        response_to TEXT,
        post_date TIMESTAMP,
        permalink TEXT,
        likes INT,
        retweets INT,
        replies INT,
        PRIMARY KEY (term, tweet_id)
    );
    CREATE TABLE twitter_extended_dataset (
        user_id TEXT,
        tweet_id TEXT,
        text TEXT,
        response_to TEXT,
        post_date TIMESTAMP,
        permalink TEXT,
        likes INT,
        retweets INT,
        replies INT,
        PRIMARY KEY (tweet_id)
    );
COMMIT;
