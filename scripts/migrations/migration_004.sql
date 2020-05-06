-- Required for enabling updates to the customer_id in gomus_customer

BEGIN;

	ALTER TABLE gomus_order
		DROP CONSTRAINT customer_id_fkey;
	ALTER TABLE gomus_order
		ADD CONSTRAINT	customer_id_fkey
		FOREIGN KEY 	(customer_id)
		REFERENCES 		gomus_customer (customer_id)
		ON UPDATE 		CASCADE;

COMMIT;
