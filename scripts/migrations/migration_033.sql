-- Add attribute for whether customer is assumed to work in tourism

BEGIN;

    ALTER TABLE gomus_customer ADD COLUMN tourism_specialist BOOLEAN;

COMMIT;