-- Migrate Google Maps reviews/fix permalinks (!442)

BEGIN;

    -- Convert permalink into regular column
    ALTER TABLE google_maps_review
        ALTER COLUMN permalink
        DROP EXPRESSION;

    -- Migrate all permalinks based on well known place_id
    CREATE FUNCTION migrate_permalink(id TEXT, place_id TEXT) RETURNS TEXT
        LANGUAGE plpgsql AS
    $$BEGIN
        IF place_id = 'ChIJyV9mg0lfqEcRnbhJji6c17E' THEN
            -- Museum Barberini
            RETURN 'https://maps.google.com/maps?cid=12814882988475660445';
        END IF;

        RAISE NOTICE 'Unknown place_id for review (%), unable to correct permalink, setting it to NULL', place_id;
        RETURN NULL;
    END;$$;

    UPDATE google_maps_review
    SET permalink = migrate_permalink(google_maps_review_id, place_id);

COMMIT;
