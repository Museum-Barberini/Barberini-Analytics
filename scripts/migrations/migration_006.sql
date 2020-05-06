-- Add impressions and reach (unique impressions) for each post (!143)

BEGIN;

    ALTER TABLE fb_post_performance ADD COLUMN post_impressions INT;
    ALTER TABLE fb_post_performance ADD COLUMN post_impressions_unique INT;

COMMIT;
