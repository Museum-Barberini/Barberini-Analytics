BEGIN;

	DROP VIEW twitter_extended_dataset;

	CREATE TABLE twitter_extended_dataset (
		user_id TEXT, 
		tweet_id TEXT,
		text TEXT,
		response_to TEXT,
		post_date TIMESTAMP,
		permalink TEXT,
		PRIMARY KEY (tweet_id)
	)

COMMIT;
