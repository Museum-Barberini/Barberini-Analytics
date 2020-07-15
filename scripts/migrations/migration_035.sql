-- Gomus: Add columns longitude, latitude for postal code analysis

BEGIN;

    ALTER TABLE gomus_customer ADD COLUMN latitude FLOAT;
    ALTER TABLE gomus_customer ADD COLUMN longitude FLOAT;

COMMIT;
