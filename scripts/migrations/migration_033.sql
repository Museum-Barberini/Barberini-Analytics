-- add column is_cancelled (!283)

BEGIN;

    ALTER TABLE gomus_order_contains ADD COLUMN is_cancelled TEXT;

COMMIT;
