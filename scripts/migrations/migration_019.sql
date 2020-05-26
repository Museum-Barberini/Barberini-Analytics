BEGIN;

    DROP VIEW exhibition_day;
   
    CREATE VIEW exhibition_day AS (
        SELECT DATE(date),  
        CASE 
            WHEN special IS NULL 
                THEN CONCAT(EXTRACT(YEAR FROM start_date), ' ', short_title)
            ELSE title
        END AS title
        FROM generate_series(
            (SELECT MIN(start_date) FROM exhibition_time),
            now(),
            '1 day'::interval
        ) date
        LEFT JOIN (SELECT * FROM exhibition_time NATURAL JOIN exhibition) as exhib
	    ON date BETWEEN start_date AND end_date
    );

COMMIT;
