-- Revise twitter_keyword table types (!347)

BEGIN;

	-- drop view to be able to change col types
	DROP VIEW twitter_extended_dataset;

	ALTER TABLE twitter_extended_candidates ALTER COLUMN term TYPE TEXT;
	ALTER TABLE twitter_keyword_intervals ALTER COLUMN term TYPE TEXT;

	-- re-create view
	CREATE VIEW twitter_extended_dataset AS
	SELECT user_id, tweet_id, text, response_to, post_date, permalink
	FROM
		-- keyword-intervals enriched with interval-based R value
		(
		SELECT ki.term, ki.start_date, ki.end_date, ki.count_interval, ki.count_interval::float/count(*)::float AS R_interval
		FROM twitter_keyword_intervals ki INNER JOIN twitter_extended_candidates ec
		ON ki.term = ec.term AND ec.post_date BETWEEN ki.start_date AND ki.end_date
		GROUP BY ki.term, ki.start_date, ki.end_date, ki.count_interval
		) AS ki_r
	INNER JOIN twitter_extended_candidates ec
	ON
	-- match tweets to intervals based on the post date
		ec.post_date between ki_r.start_date and ki_r.end_date
	AND
	-- only consider keyword-intervals with an R value below 50
		ki_r.count_interval <= 50
	AND
	-- tweet should contain the term as a whole word
		ec.text ~* ('\m' || ki_r.term || '\M')
	GROUP BY user_id, tweet_id, text, response_to, post_date, permalink
	-- Only keep top-ranked tweets
	HAVING sum(r_interval) > 0.8;

COMMIT;
