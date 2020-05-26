BEGIN;
-- The primary key was wrongly (article_id) before,
-- but it must be (article_id, article_type)
ALTER TABLE gomus_order_contains
    DROP CONSTRAINT IF EXISTS gomus_order_contains_pkey;

ALTER TABLE gomus_order_contains
    ADD PRIMARY KEY (article_id, article_type);

COMMIT;
