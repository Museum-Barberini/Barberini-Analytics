-- This migration introduces the tables 
-- topic_modeling_texts and topic_modeling_topics 
-- that are used to store topic modeling results.

BEGIN;

	CREATE TABLE topic_modeling_texts (
		id INTEGER PRIMARY KEY,
		post_id TEXT,
		text TEXT,
		source TEXT,
		post_date TIMESTAMP,
		topic TEXT,
		model_name TEXT
	);

	CREATE TABLE topic_modeling_topics (
		id INTEGER PRIMARY KEY,
		topic TEXT,
		term TEXT,
		count INTEGER,
		model TEXT
	);

COMMIT;
