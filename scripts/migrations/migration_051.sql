/** Accelerate instagram report (!373)
  * Because computation of performance delta values in Power BI is too slow,
    we need to precompute them in the database.
  */

BEGIN;

    -- 1. Add delta columns to schema
    ALTER TABLE ig_post_performance
        ADD COLUMN delta_impressions INT,
        ADD COLUMN delta_reach INT,
        ADD COLUMN delta_engagement INT,
        ADD COLUMN delta_saved INT,
        ADD COLUMN delta_video_views INT;

    -- 2. Fill historical delta columns (slow!)
    UPDATE ig_post_performance pnow
    SET (
        delta_impressions, delta_reach, delta_engagement, delta_saved,
        delta_video_views
    ) = (
        SELECT
            pnow.impressions - coalesce(pprev.impressions, 0),
            pnow.reach - coalesce(pprev.reach, 0),
            pnow.engagement - coalesce(pprev.engagement, 0),
            pnow.saved - coalesce(pprev.saved, 0),
            pnow.video_views - coalesce(pprev.video_views, 0)
        FROM
            ig_post_performance pprev
            NATURAL JOIN (
                SELECT ig_post_id, MAX(timestamp) AS timestamp
                FROM ig_post_performance
                WHERE timestamp < pnow.timestamp
                GROUP BY ig_post_id
            ) AS p2
            NATURAL RIGHT JOIN ig_post
        WHERE pnow.ig_post_id = ig_post.ig_post_id
    );

COMMIT;
