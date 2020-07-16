-- Gomus: Add columns latitude, longitude for postal code analysis

BEGIN;

    ALTER TABLE gomus_customer ADD COLUMN latitude FLOAT;
    ALTER TABLE gomus_customer ADD COLUMN longitude FLOAT;

COMMIT;
