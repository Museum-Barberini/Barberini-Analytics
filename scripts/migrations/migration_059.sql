-- Fix Apple Appstore reviews permalink (!448)

BEGIN;

	ALTER TABLE appstore_review
        DROP COLUMN permalink,
        ADD COLUMN permalink TEXT GENERATED ALWAYS AS (
                'https://apps.apple.com/de/app/museum-barberini/id'
                    || app_id
                    || '?see-all=reviews'
                -- Apple seems not to support review-specific URLs
            ) STORED;

COMMIT;
