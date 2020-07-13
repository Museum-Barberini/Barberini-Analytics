-- add column is_cancelled (!281)

BEGIN;

    ALTER TABLE gomus_order_contains ADD COLUMN is_cancelled TEXT;

COMMIT;
