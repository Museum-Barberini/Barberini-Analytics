-- Revise social media views (!149)

BEGIN;

    -- 0. Drop old views
    DROP VIEW post;
    DROP VIEW app_review;

    -- 1. Add app_id column to app store tables
    -- Recreate tables because they are not optimized anyway and psql does not
    -- allow to insert a column at a different position than the end
    DROP TABLE appstore_review;
    CREATE TABLE appstore_review (
        app_id text,
        appstore_review_id text NOT NULL,
        text text,
        rating integer,
        app_version text,
        vote_count integer,
        vote_sum integer,
        title text,
        post_date timestamp,
        country_code text
    );
    ALTER TABLE appstore_review
        ADD CONSTRAINT appstore_review_primkey
        PRIMARY KEY (app_id, appstore_review_id);

    DROP TABLE gplay_review;
    CREATE TABLE public.gplay_review (
        app_id text,
        playstore_review_id text NOT NULL,
        text text,
        rating integer,
        app_version text,
        thumbs_up integer,
        title text,
        post_date timestamp
    );
    ALTER TABLE gplay_review
        ADD CONSTRAINT gplay_review_primkey
        PRIMARY KEY (app_id, playstore_review_id);

    ALTER TABLE fb_post
        ADD COLUMN page_id TEXT;


    -- 2. Add permalink column to post tables
    ALTER TABLE appstore_review
        ADD permalink TEXT
        GENERATED ALWAYS AS (
            'https://apps.apple.com/de/app/museum-barberini/id'
                || app_id
                || '#see-all/reviews'
        ) STORED;
    ALTER TABLE gplay_review
        ADD permalink TEXT
        GENERATED ALWAYS AS (
            'https://play.google.com/store/apps/details'
                || '?id=com.barberini.museum.barberinidigital&reviewId='
                || playstore_review_id
        ) STORED;
    ALTER TABLE tweet
        ADD permalink TEXT
        GENERATED ALWAYS AS (
            'https://twitter.com/user/status/' || tweet_id
        ) STORED;

    -- TODO: It is not nice to hard-code the musem data here.
    /*CREATE VIEW app_review AS
    (
        SELECT
            'Apple Appstore' AS source,
            appstore_review_id AS review_id,
            NULLIF(CONCAT_WS(E'\n', title, text), '') AS text,
            NULL AS post_date,
            rating,
            app_version,
            NULL AS likes,
            title,
            'https://apps.apple.com/de/app/museum-barberini/id1150432552'
                || '#see-all/reviews'
                -- Apple seems not to support review-specific URLs
                AS permalink
        FROM appstore_review
    ) UNION (
        SELECT
            'Google Play' AS source,
            playstore_review_id AS review_id,
            text,
            post_date,
            rating,
            app_version,
            thumbs_up AS likes,
            NULL as title,
            'https://play.google.com/store/apps/details'
                || '?id=com.barberini.museum.barberinidigital&reviewId='
                || playstore_review_id
                AS permalink
        FROM gplay_review
    );

    CREATE VIEW social_media_post AS (
        SELECT
            'Facebook' AS source,
            fb_post_id AS post_id,
            text,
            post_date,
            NULL as media_type,
            NULL as response_to,
            NULL as user_id,
            TRUE as is_promotion,
            likes,
            comments,
            shares,
            
    ) UNION (
        SELECT
            'Twitter' AS source,
            tweet_id AS post_id,
            text,
            post_date,
            NULL as media_type,
            response_to,
            user_id,
            is_promotion,
            likes,
            replies AS comments,
            retweets AS shares,
            permalink
        FROM tweet
        NATURAL JOIN (
            SELECT tweet_id, likes, replies, retweets
            FROM tweet_performance tp1
            WHERE EXISTS(
                SELECT tp2.tweet_id
                FROM tweet_performance tp2
                WHERE tp2.tweet_id = tp1.tweet_id
                GROUP BY tp2.tweet_id, tp2.timestamp
                HAVING MAX(tp2.timestamp) = tp1.timestamp)
        ) AS performance
    )*/

COMMIT;