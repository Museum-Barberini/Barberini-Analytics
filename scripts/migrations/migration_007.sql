BEGIN;

ALTER TABLE gomus_customer ADD COLUMN cleansed_postal_code TEXT;

ALTER TABLE gomus_customer ADD COLUMN cleansed_country TEXT;

COMMIT;