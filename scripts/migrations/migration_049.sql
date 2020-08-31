-- Clean up false positive tweets (!354)

BEGIN;

    DELETE FROM tweet
        USING post
        WHERE post.source = 'Twitter' AND post.post_id = tweet.tweet_id
            AND NOT (is_from_museum
                OR tweet.text ILIKE '%museumbarberini%'
                OR is_response);

COMMIT;
