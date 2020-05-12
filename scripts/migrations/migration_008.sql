-- removes entries from tweet_performance and fb_post_performance
-- deletes entries that are 60 days older than the post date, 
-- but keeps at least one entry per post

BEGIN;

DELETE FROM fb_post_performance AS perf
WHERE 
	DATE_PART('day', perf.time_stamp -
		(SELECT MAX(post_date)
		FROM fb_post
		WHERE fb_post.fb_post_id = perf.fb_post_id)
	) > 60
AND (fb_post_id, perf.time_stamp) NOT IN 
	(SELECT perf2.fb_post_id, MAX(perf2.time_stamp)
	FROM fb_post_performance AS perf2
	group by perf2.fb_post_id
	having perf2.fb_post_id = perf.fb_post_id);

DELETE FROM tweet_performance AS perf
WHERE 
	DATE_PART('day', perf.timestamp -
		(SELECT MAX(post_date)
		FROM tweet
		WHERE tweet.tweet_id = perf.tweet_id)
	) > 60
AND (tweet_id, perf.timestamp) NOT IN 
	(SELECT perf2.tweet_id, MAX(perf2.timestamp)
	FROM tweet_performance AS perf2
	group by perf2.tweet_id
	having perf2.tweet_id = perf.tweet_id);
