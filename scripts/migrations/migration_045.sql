-- Revise twitter_keyword table types (!347)

BEGIN;

	ALTER TABLE twitter_extended_candidates ALTER COLUMN term TYPE TEXT;
	ALTER TABLE twitter_keyword_intervals ALTER COLUMN term TYPE TEXT;

COMMIT;
