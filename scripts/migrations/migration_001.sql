BEGIN;

	ALTER TABLE appstore_review RENAME COLUMN date TO post_timestamp;
	ALTER TABLE google_maps_review RENAME COLUMN date TO post_date;
	ALTER TABLE gplay_review RENAME COLUMN date TO post_timestamp;
	ALTER TABLE fb_post RENAME COLUMN post_date TO post_timestamp;

COMMIT;
