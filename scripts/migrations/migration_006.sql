BEGIN;

ALTER TABLE fb_post_performance ADD COLUMN post_impressions INT;

ALTER TABLE fb_post_performance ADD COLUMN post_impressions_unique INT;

COMMIT;
