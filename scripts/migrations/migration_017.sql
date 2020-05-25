/** Revise posts schema (!186)
  * Add column role to tweet_author (#236)
  * Revise and unify is_from_barberini (#236)
  * Integrate facebook comments into views (#239)
  * Fix bugs in performance views (#223, #238)
  */

BEGIN;

    -- Drop old views
    DROP VIEW fb_post_all, social_media_post, post;

    -- Create new views
    CREATE VIEW fb_post_all AS
    (
        SELECT
            fb_post_id AS post_id,
            page_id,
            post_date,
            text,
            TRUE AS is_from_museum,
            NULL AS response_to,
	    false as is_comment
        FROM fb_post
    ) UNION (
        SELECT
            fb_post_comment_id AS post_id,
            page_id,
            post_date,
            text,
            is_from_museum,
            response_to,
	    true as is_comment
        FROM fb_post_comment
    );

    CREATE VIEW social_media_post AS (
        WITH _social_media_post AS (
            (
                SELECT
                    CASE WHEN is_comment THEN 'Facebook Comment'
		         ELSE 'Facebook Post'
		    END AS source,
                    fb_post_id AS post_id,
                    text,
                    post_date,
                    NULL AS media_type,
                    response_to,
                    NULL AS user_id,
                    is_from_museum,
                    likes,
                    comments,
                    shares,
                    permalink
                FROM fb_post_all
                NATURAL LEFT JOIN fb_post_rich

            ) UNION (
                SELECT
                    'Instagram' AS source,
                    ig_post_id AS post_id,
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
            )
        )
        SELECT *, (response_to IS NOT NULL) is_response
        FROM _social_media_post
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
            FALSE AS is_response,
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
            FALSE AS is_response,
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
            is_response,
            likes,
            comments,
            shares,
            permalink
        FROM social_media_post
    );


COMMIT;
