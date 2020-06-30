-- Fix Google Maps permalinks (see #279)

BEGIN;

    -- 1. Divert referencing views
    CREATE OR REPLACE VIEW museum_review AS (
        SELECT *
        FROM museum_review
    );

    -- 2. Replace generated column
    ALTER TABLE google_maps_review
        DROP COLUMN permalink,
        ADD COLUMN permalink TEXT GENERATED ALWAYS AS (
            'https://www.google.com/maps/place/?q=place_id:' || place_id
            /** GMB API does not provide option to create permalink for
              * google_maps_review_id. See also:
              * https://support.google.com/business/thread/11131183
              */
        ) STORED;

    -- 3. Recreate referencing views
    CREATE OR REPLACE VIEW museum_review AS (
        SELECT
            'Google Maps' AS source,
            google_maps_review_id AS review_id,
            rating,
            text,
            post_date,
            permalink
        FROM google_maps_review
        WHERE place_id = 'ChIJyV9mg0lfqEcRnbhJji6c17E'  -- Museum Barberini
    );

COMMIT;
