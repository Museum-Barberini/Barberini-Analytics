-- Create unions of social media tables (!121)

BEGIN;

    CREATE VIEW post AS (
        (SELECT 
            'facebook' as source,
            fb_post.fb_post_id as post_id,
            text,
            post_date,
            likes,
            shares,
            comments,
            '' as language,
            0 as rating
        FROM fb_post jOIN ( 
            SELECT 
                max(likes) as likes,
                max(shares) as shares,
                max(comments) as comments,
                fb_post_id
            FROM fb_post_performance
            GROUP BY fb_post_id) as performance
        ON fb_post.fb_post_id = performance.fb_post_id)
    UNION
        (SELECT 
            'twitter' as source,
            tweet.tweet_id as post_id,
            text,
            post_date,
            likes,
            null as shares,
            null as comments,
            '' as language,
            0 as rating
        FROM tweet jOIN ( 
            SELECT max(likes) as likes, tweet_id
            FROM tweet_performance
            GROUP BY tweet_id) as performance
        ON tweet.tweet_id = performance.tweet_id)
    UNION
        (SELECT 
            'google_maps' as source,
            google_maps_review_id as post_id,
            text,
            post_date,
            null as likes,
            null as shares,
            null as comments,
            language,
            rating
        FROM google_maps_review)
    );

    CREATE VIEW app_review AS (
        (SELECT 
            'appstore' as source,
            appstore_review_id as review_id,
            title,
            text,
            rating,
            post_date,
            app_version,
            vote_count,
            vote_sum,
            0 as thumbs_up
        FROM appstore_review)
    UNION 
        (SELECT 
            'gplay' as source,
            playstore_review_id as review_id,
            title,
            text,
            rating,
            post_date,
            app_version,
            0 as vote_count,
            0 as vote_sum,
            thumbs_up
        FROM gplay_review)
    );

COMMIT;
