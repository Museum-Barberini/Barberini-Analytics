-- Clean up false positive tweets (!354)

BEGIN;

    ALTER TABLE tweet_performance
        DROP CONSTRAINT tweet_performance_tweet_id_fkey,
        ADD CONSTRAINT tweet_performance_tweet_id_fkey
            FOREIGN KEY (tweet_id) REFERENCES tweet
            ON DELETE CASCADE;
    DELETE FROM tweet
        USING post
        WHERE post.source = 'Twitter' AND post.post_id = tweet.tweet_id
            AND NOT (is_from_museum
                OR tweet.text ILIKE '%museumbarberini%'
                OR is_response);

COMMIT;
