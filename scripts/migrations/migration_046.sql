BEGIN;

	DROP VIEW twitter_extended_dataset;

	-- re-create view
	CREATE VIEW twitter_extended_dataset AS
	SELECT user_id, tweet_id, text, response_to, post_date, permalink
	FROM
		-- keyword-intervals enriched with interval-based R value
		(
		SELECT * FROM (
			SELECT ki.term, ki.start_date, ki.end_date, ki.count_interval, count(*)::float/ki.count_interval::float AS R_interval
			FROM twitter_keyword_intervals ki INNER JOIN twitter_extended_candidates ec
			ON ki.term = ec.term AND ec.post_date BETWEEN ki.start_date AND ki.end_date
			WHERE ki.count_interval > 0
			GROUP BY ki.term, ki.start_date, ki.end_date, ki.count_interval
			) as temp
		-- only consider keyword-intervals with an R value below 50
		WHERE R_interval <= 50
		) AS ki_r
	INNER JOIN twitter_extended_candidates ec
	ON
	-- match tweets to intervals based on the post date
		ec.post_date between ki_r.start_date and ki_r.end_date
	AND
	-- tweet should contain the term as a whole word
		ec.text ~* ('\m' || ki_r.term || '\M')
	GROUP BY user_id, tweet_id, text, response_to, post_date, permalink
	-- Only keep top-ranked tweets
	HAVING sum(r_interval) > 0.8;

COMMIT;
