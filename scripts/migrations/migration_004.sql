-- Required for enabling updates to the customer_id in gomus_customer
BEGIN;
ALTER TABLE gomus_order DROP CONSTRAINT customer_id_fkey;
ALTER TABLE gomus_order ADD CONSTRAINT customer_id_fkey FOREIGN KEY (customer_id) REFERENCES gomus_customer (customer_id) ON UPDATE CASCADE;

-- This was forgotten in earlier versions, it should be in place already. Adding it now for the future
ALTER TABLE gomus_to_customer_mapping ADD CONSTRAINT customer_id_fkey FOREIGN KEY (customer_id) REFERENCES gomus_customer (customer_id) ON UPDATE CASCADE;
COMMIT;
