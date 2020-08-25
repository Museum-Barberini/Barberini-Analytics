BEGIN;

	CREATE TABLE twitter_keyword_intervals (
		term VARCHAR(255),
		count_overall INT,
		count_interval INT,
		start_date DATE,
		end_date DATE,
		PRIMARY KEY (term, start_date)
	);

COMMIT;
