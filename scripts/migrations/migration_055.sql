-- Integrate Instagram thumbnails (!406)

BEGIN;

    ALTER TABLE ig_post
        ADD COLUMN thumbnail_uri TEXT;

END;
