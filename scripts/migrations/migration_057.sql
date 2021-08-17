-- Fix fetch_updated_mail(): Make customer ID mutable

BEGIN;

    ALTER TABLE gomus_event
        DROP CONSTRAINT gomus_event_customer_id_fkey,
        ADD CONSTRAINT gomus_event_customer_id_fkey
        FOREIGN KEY (customer_id)
        REFERENCES gomus_customer
        ON UPDATE CASCADE;

COMMIT;
