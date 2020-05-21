BEGIN;

    ALTER TABLE tweet_author
        ADD COLUMN role TEXT;


    DROP VIEW tweet_rich, social_media_post, post;

    ALTER TABLE tweet
        DROP COLUMN is_from_barberini;

    CREATE VIEW tweet_rich AS (
        SELECT *, (author_role ILIKE 'official%') IS true AS is_from_museum
        FROM tweet_performance AS p1
        NATURAL JOIN (
            SELECT tweet_id, MAX(timestamp) AS timestamp
            FROM tweet_performance
            GROUP BY tweet_id) AS p2
        NATURAL RIGHT JOIN tweet
        NATURAL LEFT JOIN (
			SELECT user_id, user_name, role AS author_role
			FROM tweet_author
		) tweet_author
    );

    CREATE OR REPLACE VIEW fb_post_rich AS (
        SELECT *
        FROM fb_post_performance AS p1
        NATURAL JOIN (
            SELECT page_id, post_id, MAX(timestamp) AS timestamp
            FROM fb_post_performance
            GROUP BY page_id, post_id) AS p2
        NATURAL RIGHT JOIN fb_post
    );
    CREATE OR REPLACE VIEW ig_post_rich AS (
        SELECT *
        FROM ig_post_performance AS p1
        NATURAL JOIN (
            SELECT ig_post_id, MAX(timestamp) AS timestamp
            FROM ig_post_performance
            GROUP BY ig_post_id) AS p2
        NATURAL RIGHT JOIN ig_post
    );

    CREATE VIEW social_media_post AS
    (
        SELECT
            'Facebook' AS source,
            fb_post_id AS post_id,
            text,
            post_date,
            NULL AS media_type,
            NULL AS response_to,
            NULL AS user_id,
            TRUE AS is_from_museum,
            likes,
            comments,
            shares,
            permalink
        FROM fb_post_rich
    ) UNION (
        SELECT
            'Instagram' AS source,
            ig_post_id AS review_id,
            text,
            post_date,
            media_type,
            NULL AS response_to,
            NULL AS user_id,
            TRUE AS is_from_museum,
            likes,
            comments,
            NULL AS shares,
            permalink
        FROM ig_post_rich
    ) UNION (
        SELECT
            'Twitter' AS source,
            tweet_id AS post_id,
            text,
            post_date,
            NULL as media_type,
            response_to,
            user_id,
            is_from_museum,
            likes,
            replies AS comments,
            retweets AS shares,
            permalink
        FROM tweet_rich
    );

    CREATE VIEW post AS
    (
        SELECT
            source,
            review_id AS post_id,
            'App Review' AS context,
            text,
            post_date,
            rating,
            FALSE AS is_from_museum,
            likes,
            CAST(NULL AS int) AS comments,
            CAST(NULL AS int) AS shares,
            permalink
        FROM app_review
    ) UNION (
        SELECT
            source,
            review_id AS post_id,
            'Museum Review' AS context,
            text,
            post_date,
            rating,
            FALSE AS is_from_museum,
            NULL AS likes,
            NULL AS comments,
            NULL AS shares,
            permalink
        FROM museum_review
    ) UNION (
        SELECT
            source,
            post_id,
            'Social Media' AS context,
            text,
            post_date,
            NULL AS rating,
            is_from_museum,
            likes,
            comments,
            shares,
            permalink
        FROM social_media_post
    );

COMMIT;
