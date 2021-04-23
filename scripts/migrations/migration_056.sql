-- Update Instagram views with thumbnails (!407)

BEGIN;

    CREATE OR REPLACE VIEW ig_post_rich AS (
        SELECT ig_post.ig_post_id,
            p1."timestamp",
            p1.impressions,
            p1.reach,
            p1.engagement,
            p1.saved,
            p1.video_views,
            ig_post.text,
            ig_post.post_date,
            ig_post.media_type,
            ig_post.likes,
            ig_post.comments,
            ig_post.permalink,
            ig_post.thumbnail_uri
        FROM ig_post_performance AS p1
        NATURAL JOIN (
            SELECT ig_post_id, MAX(timestamp) AS timestamp
            FROM ig_post_performance
            GROUP BY ig_post_id) AS p2
        NATURAL RIGHT JOIN ig_post
    );

COMMIT;
