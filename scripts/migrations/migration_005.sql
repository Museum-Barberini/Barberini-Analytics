-- Create tables for Instagram
BEGIN;
CREATE TABLE ig_post (
    ig_post_id TEXT,
    caption TEXT,
    timestamp TIMESTAMP,
    media_type TEXT,
    like_count INT,
    comments_count INT,
    permalink TEXT
);

ALTER TABLE ig_post
    ADD CONSTRAINT ig_post_primkey PRIMARY KEY (ig_post_id);

COMMIT;
